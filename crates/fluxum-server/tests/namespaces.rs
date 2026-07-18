//! SPEC-025 §6 (OPS-050/051) — database namespaces: one binary hosting
//! several independent databases. A client authenticated into `acme` sees
//! only acme's rows, its commits never reach globex's subscribers, an
//! unknown namespace refuses the handshake, a connection cannot switch
//! database mid-life, and `/metrics` attributes each namespace.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{
    Authenticate, ClientMessage, FrameCodec, OneOffQuery, ReducerCall, ServerMessage, Subscribe,
};
use fluxum_server::ShardContext;
use fluxum_server::namespace::Namespace;
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 9;

// --- Note table + a reducer that writes one -----------------------------------

static NOTE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static NOTE: TableSchema = TableSchema {
    name: "Note",
    columns: NOTE_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Note {
    id: u64,
    text: String,
}

impl Table for Note {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &NOTE;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.text)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(text)] => Ok(Self {
                id: *id,
                text: text.clone(),
            }),
            other => Err(fluxum_core::FluxumError::Storage(format!(
                "Note: {other:?}"
            ))),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn write_note(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("write_note(text)".into())),
    };
    ctx.tx.insert(Note { id: 0, text })?;
    Ok(())
}

fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("write_note", args, 1)
}

static WRITE_NOTE: ReducerDef = ReducerDef {
    name: "write_note",
    handler: write_note,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

// --- Harness: one process, a default database + acme + globex -------------------

struct Harness {
    server: tcp::TcpServer,
    ctx: Arc<ShardContext>,
}

async fn start() -> Harness {
    let (engine, subs) = build_db("default");
    let authenticator =
        Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, authenticator, SHARD, 256);

    for name in ["acme", "globex"] {
        let (engine, subs) = build_db(name);
        ctx.register_namespace(Namespace::new(name, engine, subs, 256))
            .unwrap();
    }

    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, ctx }
}

fn build_db(name: &str) -> (ReducerEngine, SubscriptionManager) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&NOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join(format!("log-{name}")),
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
        Arc::new(ReducerRegistry::from_defs([&WRITE_NOTE]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity(name),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    (engine, subs)
}

// --- Minimal framed client -------------------------------------------------------

struct Client {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, message: ClientMessage) {
        let body = message.encode().unwrap();
        let framed = self.codec.encode(&body).unwrap();
        self.stream.write_all(&framed).await.unwrap();
    }

    async fn recv(&mut self) -> Option<ServerMessage> {
        loop {
            if let Ok(Some((frame, consumed))) = self.codec.decode(&self.buf) {
                let msg = match frame {
                    fluxum_protocol::Frame::Body(body) => {
                        Some(ServerMessage::decode(body).unwrap())
                    }
                    fluxum_protocol::Frame::KeepAlive => None,
                };
                self.buf.drain(..consumed);
                if let Some(msg) = msg {
                    return Some(msg);
                }
                continue;
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await {
                Ok(0) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }

    async fn recv_timeout(&mut self, dur: Duration) -> Option<ServerMessage> {
        tokio::time::timeout(dur, self.recv()).await.ok().flatten()
    }

    /// Authenticate into `namespace` (`None` = the default database).
    async fn authenticate(&mut self, namespace: Option<&str>) -> ServerMessage {
        self.send(ClientMessage::Authenticate(Authenticate {
            id: 1,
            token: b"user".to_vec(),
            compression: None,
            tx_updates: None,
            namespace: namespace.map(ToOwned::to_owned),
        }))
        .await;
        self.recv().await.unwrap()
    }

    async fn write_note(&mut self, text: &str) {
        self.send(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "write_note".into(),
            version: None,
            args: vec![FluxValue::Str(text.into())],
            idempotency_key: None,
        }))
        .await;
        match self.recv().await.unwrap() {
            ServerMessage::ReducerResult(r) => assert!(r.outcome.is_ok(), "{r:?}"),
            other => panic!("expected ReducerResult, got {other:?}"),
        }
    }

    async fn notes(&mut self) -> Vec<String> {
        self.send(ClientMessage::OneOffQuery(OneOffQuery {
            id: 3,
            sql: "SELECT * FROM Note".into(),
        }))
        .await;
        match self.recv().await.unwrap() {
            // Rows arrive FluxBIN-encoded; the note text is embedded as UTF-8,
            // so a lossy decode is enough to identify which tenant's row it is.
            ServerMessage::InitialData(init) => init
                .tables
                .iter()
                .flat_map(|t| t.inserts.iter())
                .map(|row| String::from_utf8_lossy(row).into_owned())
                .collect(),
            other => panic!("expected InitialData, got {other:?}"),
        }
    }
}

// --- OPS-050 acceptance: two tenants, one binary ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_sees_only_its_own_database() {
    let h = start().await;

    let mut acme = Client::connect(h.server.local_addr).await;
    assert!(matches!(
        acme.authenticate(Some("acme")).await,
        ServerMessage::AuthResult(_)
    ));
    acme.write_note("acme-only").await;

    let mut globex = Client::connect(h.server.local_addr).await;
    assert!(matches!(
        globex.authenticate(Some("globex")).await,
        ServerMessage::AuthResult(_)
    ));
    globex.write_note("globex-only").await;

    // Each sees exactly its own row — never the sibling's.
    let acme_rows = acme.notes().await;
    assert_eq!(acme_rows.len(), 1, "{acme_rows:?}");
    assert!(acme_rows[0].contains("acme-only"), "{acme_rows:?}");

    let globex_rows = globex.notes().await;
    assert_eq!(globex_rows.len(), 1, "{globex_rows:?}");
    assert!(globex_rows[0].contains("globex-only"), "{globex_rows:?}");

    // The default database saw neither write.
    let mut default = Client::connect(h.server.local_addr).await;
    default.authenticate(None).await;
    assert!(default.notes().await.is_empty(), "default DB is untouched");

    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_never_reaches_another_namespaces_subscribers() {
    let h = start().await;

    // A globex subscriber waits for live updates.
    let mut globex = Client::connect(h.server.local_addr).await;
    globex.authenticate(Some("globex")).await;
    globex
        .send(ClientMessage::Subscribe(Subscribe {
            id: 4,
            queries: vec!["SELECT * FROM Note".into()],
        }))
        .await;
    assert!(matches!(
        globex.recv().await.unwrap(),
        ServerMessage::InitialData(_)
    ));

    // acme commits. globex must hear nothing.
    let mut acme = Client::connect(h.server.local_addr).await;
    acme.authenticate(Some("acme")).await;
    acme.write_note("for acme eyes only").await;
    assert!(
        globex
            .recv_timeout(Duration::from_millis(400))
            .await
            .is_none(),
        "a tenant's commit must not fan out to another tenant"
    );

    // A commit in globex's own database does reach it.
    let mut globex_writer = Client::connect(h.server.local_addr).await;
    globex_writer.authenticate(Some("globex")).await;
    globex_writer.write_note("globex news").await;
    assert!(
        matches!(
            globex.recv_timeout(Duration::from_secs(2)).await,
            Some(ServerMessage::TxUpdate(_))
        ),
        "the tenant's own commit does fan out to it"
    );

    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_namespace_refuses_the_handshake() {
    let h = start().await;
    let mut client = Client::connect(h.server.local_addr).await;
    match client.authenticate(Some("nope")).await {
        ServerMessage::Error(e) => {
            assert_eq!(e.code, fluxum_protocol::codes::AUTH_FAILED);
            assert!(e.message.contains("nope"), "{e:?}");
        }
        other => panic!("expected an error, got {other:?}"),
    }
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_cannot_switch_database_mid_life() {
    let h = start().await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(Some("acme")).await;

    // A re-Authenticate naming another database is refused; the binding is
    // for the connection's lifetime.
    client
        .send(ClientMessage::Authenticate(Authenticate {
            id: 7,
            token: b"user".to_vec(),
            compression: None,
            tx_updates: None,
            namespace: Some("globex".into()),
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::Error(e) => assert_eq!(e.code, fluxum_protocol::codes::AUTH_FAILED),
        other => panic!("expected a refusal, got {other:?}"),
    }

    // Re-authenticating into the *same* database is still fine.
    assert!(matches!(
        client.authenticate(Some("acme")).await,
        ServerMessage::AuthResult(_)
    ));
    h.server.shutdown();
}

// --- OPS-051 attribution ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn metrics_carry_a_namespace_label_per_database() {
    let h = start().await;
    let mut acme = Client::connect(h.server.local_addr).await;
    acme.authenticate(Some("acme")).await;
    acme.write_note("counted").await;

    let resp = fluxum_server::admin::dispatch(&h.ctx, "GET", "/metrics", &[]).await;
    let text = match &resp.body {
        serde_json::Value::String(text) => text.clone(),
        other => panic!("expected metrics text, got {other:?}"),
    };
    assert!(
        text.contains("namespace=\"acme\""),
        "acme's series must be attributable"
    );
    assert!(
        text.contains("namespace=\"globex\""),
        "every registered namespace is exposed"
    );
    // The default database's series stay unlabelled (backward compatible).
    assert!(
        text.contains(&format!("fluxum_connections_total{{shard=\"{SHARD}\"}}")),
        "the default database keeps its original label set"
    );
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn registering_a_duplicate_or_reserved_namespace_is_refused() {
    let h = start().await;
    let (engine, subs) = build_db("acme");
    assert!(
        h.ctx
            .register_namespace(Namespace::new("acme", engine, subs, 8))
            .is_err(),
        "a name must resolve to exactly one database"
    );
    let (engine, subs) = build_db("default");
    assert!(
        h.ctx
            .register_namespace(Namespace::new("default", engine, subs, 8))
            .is_err(),
        "`default` is the implicit database and is reserved"
    );
    h.server.shutdown();
}
