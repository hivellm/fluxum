//! SPEC-027 (PGW-001..004) end-to-end: a real TCP client speaks the Postgres
//! v3 wire protocol to the read-only endpoint — startup + cleartext-token
//! auth, a `SELECT` streamed as RowDescription/DataRow, `information_schema`
//! discovery, a write rejected as read-only, and a session `SET` accepted as a
//! harmless no-op. Not a mock: the endpoint is bound on an ephemeral port and
//! driven over a socket, so the framing and handshake are exercised for real.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, ServerPeerRegistry, TokenProvider};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerDef, ReducerEngine,
    ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_server::ShardContext;
use fluxum_server::pgwire::{self, PgOptions};

const SHARD: u32 = 0;

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
];
static ITEM: TableSchema = TableSchema {
    name: "Item",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct ItemRow {
    id: u64,
    name: String,
}
impl Table for ItemRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &ITEM;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.name)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(name)] => Ok(Self {
                id: *id,
                name: name.clone(),
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn add_item(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    match args {
        [FluxValue::I64(id), FluxValue::Str(name)] => {
            ctx.tx.insert(ItemRow {
                id: *id as u64,
                name: name.clone(),
            })?;
            Ok(())
        }
        _ => Err(fluxum_core::FluxumError::Reducer(
            "add_item(id, name)".into(),
        )),
    }
}
fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("add_item", args, 2)
}
static ADD_ITEM: ReducerDef = ReducerDef {
    name: "add_item",
    handler: add_item,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

/// A running endpoint plus the valid token to authenticate with.
struct Harness {
    addr: std::net::SocketAddr,
    token: Vec<u8>,
    _server: pgwire::PgServer,
    _ctx: Arc<ShardContext>,
}

async fn start() -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&ITEM]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&ADD_ITEM]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("pgwire-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let provider = TokenProvider::new(b"analytics-secret".to_vec());
    let token = provider.mint(b"analytics").unwrap();
    let auth = Authenticator::with_provider(Arc::new(provider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);

    // Two committed rows to stream back.
    for (id, name) in [(1i64, "apple"), (2, "banana")] {
        ctx.engine
            .call(
                ReducerCaller {
                    identity: Identity::from_token("seed"),
                    connection_id: ConnectionId::new(1),
                    timestamp: Timestamp::now(),
                    shard_id: SHARD,
                },
                "add_item",
                vec![FluxValue::I64(id), FluxValue::Str(name.into())],
            )
            .await
            .unwrap();
    }

    let server = pgwire::serve(Arc::clone(&ctx), "127.0.0.1:0", PgOptions::default())
        .await
        .unwrap();
    Harness {
        addr: server.local_addr,
        token,
        _server: server,
        _ctx: ctx,
    }
}

/// A minimal Postgres v3 client for the test.
struct PgClient {
    stream: TcpStream,
}

impl PgClient {
    async fn connect_and_auth(h: &Harness, token: &[u8]) -> Self {
        let mut stream = TcpStream::connect(h.addr).await.unwrap();
        // StartupMessage: len | protocol 3.0 | user\0..\0 database\0..\0 \0.
        let mut body = 196_608i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0analytics\0database\0fluxum\0\0");
        let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        stream.write_all(&packet).await.unwrap();
        // Expect AuthenticationCleartextPassword ('R', code 3).
        let (tag, body) = read_message(&mut stream).await;
        assert_eq!(tag, b'R');
        assert_eq!(i32::from_be_bytes(body[..4].try_into().unwrap()), 3);
        // PasswordMessage.
        let mut pw = vec![b'p'];
        pw.extend_from_slice(&((token.len() + 5) as i32).to_be_bytes());
        pw.extend_from_slice(token);
        pw.push(0);
        stream.write_all(&pw).await.unwrap();
        Self { stream }
    }

    async fn query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.stream.write_all(&msg).await.unwrap();
        self.read_until_ready().await
    }

    /// The extended-query flow for a parameterless statement:
    /// Parse → Bind → Describe(portal) → Execute → Sync.
    async fn extended_query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut buf = Vec::new();
        // Parse: unnamed statement, no declared param types.
        let mut parse = Vec::new();
        parse.push(0u8); // statement name ""
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0i16.to_be_bytes());
        frame(&mut buf, b'P', &parse);
        // Bind: unnamed portal ← unnamed statement, no formats/params.
        let mut bind = Vec::new();
        bind.push(0); // portal ""
        bind.push(0); // statement ""
        bind.extend_from_slice(&0i16.to_be_bytes()); // format codes
        bind.extend_from_slice(&0i16.to_be_bytes()); // params
        bind.extend_from_slice(&0i16.to_be_bytes()); // result formats
        frame(&mut buf, b'B', &bind);
        // Describe the portal.
        frame(&mut buf, b'D', b"P\0");
        // Execute the portal, unlimited rows.
        let mut exec = Vec::new();
        exec.push(0); // portal ""
        exec.extend_from_slice(&0i32.to_be_bytes());
        frame(&mut buf, b'E', &exec);
        // Sync.
        frame(&mut buf, b'S', b"");
        self.stream.write_all(&buf).await.unwrap();
        self.read_until_ready().await
    }

    async fn read_until_ready(&mut self) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        loop {
            let (tag, body) = read_message(&mut self.stream).await;
            let done = tag == b'Z';
            out.push((tag, body));
            if done {
                return out;
            }
        }
    }
}

/// Append a tagged frontend message (`tag | i32 len-inclusive | body`).
fn frame(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
    buf.push(tag);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(body);
}

async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = stream.read_u8().await.unwrap();
    let len = stream.read_i32().await.unwrap();
    let mut body = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut body).await.unwrap();
    (tag, body)
}

fn tags(msgs: &[(u8, Vec<u8>)]) -> Vec<u8> {
    msgs.iter().map(|(t, _)| *t).collect()
}

fn body_contains(msgs: &[(u8, Vec<u8>)], tag: u8, needle: &[u8]) -> bool {
    msgs.iter()
        .filter(|(t, _)| *t == tag)
        .any(|(_, b)| b.windows(needle.len()).any(|w| w == needle))
}

#[tokio::test]
async fn a_select_streams_rows_after_the_handshake() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, &h.token).await;
    // The handshake completion (AuthenticationOk … ReadyForQuery).
    let hello = client.read_until_ready().await;
    assert!(tags(&hello).contains(&b'R'), "AuthenticationOk");
    assert_eq!(tags(&hello).last(), Some(&b'Z'), "ReadyForQuery");

    let reply = client.query("SELECT * FROM Item").await;
    let t = tags(&reply);
    assert!(t.contains(&b'T'), "RowDescription present: {t:?}");
    assert_eq!(t.iter().filter(|&&x| x == b'D').count(), 2, "two DataRows");
    assert!(body_contains(&reply, b'D', b"apple"));
    assert!(body_contains(&reply, b'D', b"banana"));
    assert!(body_contains(&reply, b'C', b"SELECT 2"), "command tag");
}

#[tokio::test]
async fn information_schema_lists_the_tables() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, &h.token).await;
    client.read_until_ready().await;
    let reply = client
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        .await;
    assert!(tags(&reply).contains(&b'T'));
    assert!(body_contains(&reply, b'D', b"Item"), "Item discoverable");
}

#[tokio::test]
async fn writes_and_ddl_are_rejected_read_only() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, &h.token).await;
    client.read_until_ready().await;
    for write in [
        "INSERT INTO Item VALUES (3, 'x')",
        "UPDATE Item SET name='y' WHERE id=1",
        "DELETE FROM Item",
        "DROP TABLE Item",
    ] {
        let reply = client.query(write).await;
        assert!(
            body_contains(&reply, b'E', b"25006"),
            "`{write}` must be rejected read-only, got {:?}",
            tags(&reply)
        );
    }
    // The connection is still usable after a rejection.
    let ok = client.query("SELECT * FROM Item").await;
    assert!(tags(&ok).contains(&b'T'));
}

#[tokio::test]
async fn session_set_and_begin_are_accepted_no_ops() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, &h.token).await;
    client.read_until_ready().await;
    let set = client.query("SET client_encoding = 'UTF8'").await;
    assert!(body_contains(&set, b'C', b"SET"), "SET is a no-op");
    let begin = client.query("BEGIN").await;
    assert!(body_contains(&begin, b'C', b"BEGIN"));
}

#[tokio::test]
async fn the_extended_query_protocol_parses_binds_describes_and_executes() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, &h.token).await;
    client.read_until_ready().await;
    let reply = client.extended_query("SELECT * FROM Item").await;
    let t = tags(&reply);
    assert!(t.contains(&b'1'), "ParseComplete: {t:?}");
    assert!(t.contains(&b'2'), "BindComplete");
    assert!(t.contains(&b'T'), "RowDescription from Describe");
    assert_eq!(t.iter().filter(|&&x| x == b'D').count(), 2, "two DataRows");
    assert!(body_contains(&reply, b'C', b"SELECT 2"));
    assert_eq!(t.last(), Some(&b'Z'), "ReadyForQuery from Sync");
}

#[tokio::test]
async fn a_bad_token_is_rejected_at_auth() {
    let h = start().await;
    let mut client = PgClient::connect_and_auth(&h, b"not-a-valid-token").await;
    // The server answers a FATAL error and closes; no ReadyForQuery arrives.
    let (tag, body) = read_message(&mut client.stream).await;
    assert_eq!(tag, b'E', "an ErrorResponse for the bad token");
    assert!(
        body.windows(5).any(|w| w == b"28P01"),
        "invalid_password SQLSTATE"
    );
}
