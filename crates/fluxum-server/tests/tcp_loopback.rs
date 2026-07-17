//! T5.1 FluxRPC/TCP loopback integration suite (SPEC-006 §3–4; FR-40/42/45;
//! DAG exit test): the auth handshake, the pre-auth 401 gate, reducer-call
//! routing with id multiplexing, subscribe → InitialData → live TxUpdate
//! push, unsubscribe, one-off query, the idle-timeout (408) and
//! frame-too-large (413) enforcement, and reconnect resync via the tx_id
//! gap — all over a real loopback TCP socket.
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
    SubscribeSingle, Unsubscribe,
};
use fluxum_server::ShardContext;
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 1;

// --- Chat table + send_chat reducer --------------------------------------------

static CHAT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static CHAT: TableSchema = TableSchema {
    name: "Chat",
    columns: CHAT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct ChatRow {
    id: u64,
    text: String,
}

impl Table for ChatRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &CHAT;
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
                "ChatRow: unexpected shape {other:?}"
            ))),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn send_chat(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => {
            return Err(fluxum_core::FluxumError::Reducer(
                "send_chat(text) required".into(),
            ));
        }
    };
    if text.is_empty() {
        return Err(fluxum_core::FluxumError::Reducer("empty text".into()));
    }
    ctx.tx.insert(ChatRow { id: 0, text })?;
    Ok(())
}

fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("send_chat", args, 1)?;
    let _ = fluxum_core::reducer::args::decode_arg::<String>("send_chat", args, 0, "text")?;
    Ok(())
}

static SEND_CHAT: ReducerDef = ReducerDef {
    name: "send_chat",
    handler: send_chat,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

// --- Server harness ------------------------------------------------------------

struct Harness {
    server: tcp::TcpServer,
    store: Arc<MemStore>,
    ctx: Arc<ShardContext>,
}

async fn start(options: TcpOptions) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    // Leak the tempdir so the log path survives the test (in-memory store
    // only needs the log open).
    let dir = Box::leak(Box::new(dir));
    let schema = Schema::from_tables([&CHAT]).unwrap();
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
    let registry = Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap());
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("tcp-test"),
    );
    let subscriptions = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let authenticator =
        Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subscriptions, authenticator, SHARD, 256);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", options)
        .await
        .unwrap();
    Harness { server, store, ctx }
}

// --- A minimal framed test client ----------------------------------------------

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

    /// Send raw pre-framed bytes (for the oversized-frame test).
    async fn send_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
    }

    /// Read the next server message, or `None` on clean close.
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

    async fn authenticate(&mut self, token: &[u8], id: u32) -> ServerMessage {
        self.send(ClientMessage::Authenticate(Authenticate {
            id,
            token: token.to_vec(),
            compression: None,
            tx_updates: None,
        }))
        .await;
        self.recv().await.unwrap()
    }
}

// --- AUTH-020/021: handshake ----------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn authenticate_returns_authresult_echoing_the_id() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    let reply = client.authenticate(b"alice-token", 7).await;
    match reply {
        ServerMessage::AuthResult(ar) => {
            assert_eq!(ar.id, 7, "id echoed (RPC-002)");
            assert_eq!(ar.identity.len(), 32);
        }
        other => panic!("expected AuthResult, got {other:?}"),
    }
    h.server.shutdown();
}

// --- AUTH-020: pre-auth 401 gate ------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn pre_auth_messages_are_401_and_keep_the_connection_open() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    // A ReducerCall before Authenticate → 401.
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 1,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("hi".into())],
            idempotency_key: None,
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::Error(e) => {
            assert_eq!(e.code, fluxum_protocol::codes::AUTH_REQUIRED);
            assert_eq!(e.id, Some(1));
        }
        other => panic!("expected 401 Error, got {other:?}"),
    }
    // The connection stays open: a following Authenticate succeeds.
    let reply = client.authenticate(b"alice", 2).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    h.server.shutdown();
}

// --- RPC-021: reducer call + commit --------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_call_commits_and_returns_reducerresult() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 1).await;
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 42,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("hello".into())],
            idempotency_key: None,
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::ReducerResult(r) => {
            assert_eq!(r.id, 42);
            assert!(r.outcome.is_ok());
        }
        other => panic!("expected ReducerResult, got {other:?}"),
    }
    // A business Err(String) comes back as ReducerResult { Err }, not Error.
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 43,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str(String::new())],
            idempotency_key: None,
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::ReducerResult(r) => {
            assert_eq!(r.id, 43);
            assert_eq!(r.outcome.unwrap_err().message, "empty text");
        }
        other => panic!("expected ReducerResult Err, got {other:?}"),
    }
    // Exactly one row committed.
    let table = h.store.table_id("Chat").unwrap();
    assert_eq!(h.store.snapshot().scan(table).unwrap().count(), 1);
    h.server.shutdown();
}

// --- RPC-002: id multiplexing ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_calls_are_answered_with_matching_ids() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 0).await;
    // Fire five calls without waiting.
    for id in 10..15 {
        client
            .send(ClientMessage::ReducerCall(ReducerCall {
                id,
                reducer: "send_chat".into(),
                version: None,
                args: vec![FluxValue::Str(format!("m{id}"))],
                idempotency_key: None,
            }))
            .await;
    }
    // Collect five responses; the id set must match (order may vary).
    let mut ids = Vec::new();
    for _ in 0..5 {
        match client.recv().await.unwrap() {
            ServerMessage::ReducerResult(r) => ids.push(r.id),
            other => panic!("expected ReducerResult, got {other:?}"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![10, 11, 12, 13, 14]);
    h.server.shutdown();
}

// --- SUB-001/002/021: subscribe + live TxUpdate push ----------------------------

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_returns_initialdata_then_pushes_txupdate_on_commit() {
    let h = start(TcpOptions::default()).await;
    let mut sub = Client::connect(h.server.local_addr).await;
    let mut writer = Client::connect(h.server.local_addr).await;
    sub.authenticate(b"subscriber", 1).await;
    writer.authenticate(b"writer", 1).await;

    sub.send(ClientMessage::SubscribeSingle(SubscribeSingle {
        id: 5,
        query: "SELECT * FROM Chat".into(),
    }))
    .await;
    let query_id = match sub.recv().await.unwrap() {
        ServerMessage::InitialData(data) => {
            assert_eq!(data.id, 5);
            assert!(data.tables[0].inserts.is_empty(), "empty initial state");
            data.tables[0].query_id
        }
        other => panic!("expected InitialData, got {other:?}"),
    };

    // Another client commits a Chat row → the subscriber gets a TxUpdate.
    writer
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 1,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("live".into())],
            idempotency_key: None,
        }))
        .await;
    assert!(matches!(
        writer.recv().await.unwrap(),
        ServerMessage::ReducerResult(_)
    ));

    match sub.recv_timeout(Duration::from_secs(2)).await {
        Some(ServerMessage::TxUpdate(update)) => {
            assert_eq!(update.tables[0].inserts.len(), 1, "the new row");
            assert!(update.tx_id >= 1);
        }
        other => panic!("expected a pushed TxUpdate, got {other:?}"),
    }

    // Unsubscribe stops delivery: a second commit yields no TxUpdate.
    sub.send(ClientMessage::Unsubscribe(Unsubscribe {
        id: 6,
        query_ids: vec![query_id],
    }))
    .await;
    writer
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("after".into())],
            idempotency_key: None,
        }))
        .await;
    assert!(matches!(
        writer.recv().await.unwrap(),
        ServerMessage::ReducerResult(_)
    ));
    assert!(
        sub.recv_timeout(Duration::from_millis(400)).await.is_none(),
        "no TxUpdate after unsubscribe"
    );
    h.server.shutdown();
}

// --- RPC-025: one-off query -----------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn one_off_query_returns_current_state_without_subscribing() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 1).await;
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 1,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("one".into())],
            idempotency_key: None,
        }))
        .await;
    client.recv().await.unwrap();

    client
        .send(ClientMessage::OneOffQuery(OneOffQuery {
            id: 9,
            sql: "SELECT * FROM Chat".into(),
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::InitialData(data) => {
            assert_eq!(data.id, 9);
            assert_eq!(data.tables[0].inserts.len(), 1);
        }
        other => panic!("expected InitialData, got {other:?}"),
    }
    h.server.shutdown();
}

// --- RPC-060: idle timeout ------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn idle_connection_gets_408_then_closes() {
    let options = TcpOptions {
        idle_timeout: Some(Duration::from_millis(300)),
        ..TcpOptions::default()
    };
    let h = start(options).await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 1).await;
    // Send nothing; expect a 408 then close.
    match client.recv_timeout(Duration::from_secs(2)).await {
        Some(ServerMessage::Error(e)) => {
            assert_eq!(e.code, fluxum_protocol::codes::PROTO_IDLE_TIMEOUT)
        }
        other => panic!("expected 408, got {other:?}"),
    }
    assert!(client.recv().await.is_none(), "connection closed after 408");
    h.server.shutdown();
}

// --- RPC-061: frame too large ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn oversized_frame_gets_413_then_closes() {
    let options = TcpOptions {
        max_frame_bytes: 1024,
        ..TcpOptions::default()
    };
    let h = start(options).await;
    let mut client = Client::connect(h.server.local_addr).await;
    // A 4-byte header declaring 2 MB — rejected from the header alone.
    let header = 2_000_000u32.to_le_bytes();
    client.send_raw(&header).await;
    match client.recv_timeout(Duration::from_secs(2)).await {
        Some(ServerMessage::Error(e)) => {
            assert_eq!(e.code, fluxum_protocol::codes::PROTO_FRAME_TOO_LARGE)
        }
        other => panic!("expected 413, got {other:?}"),
    }
    assert!(client.recv().await.is_none(), "connection closed after 413");
    h.server.shutdown();
}

// --- SPEC-006 acceptance 14: reconnect resync via tx_id gap ---------------------

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_resubscribe_gets_fresh_initialdata_reflecting_missed_commits() {
    let h = start(TcpOptions::default()).await;

    // A writer commits one row while the subscriber is connected.
    let mut writer = Client::connect(h.server.local_addr).await;
    writer.authenticate(b"writer", 1).await;
    let mut sub = Client::connect(h.server.local_addr).await;
    sub.authenticate(b"subscriber", 1).await;
    sub.send(ClientMessage::Subscribe(Subscribe {
        id: 1,
        queries: vec!["SELECT * FROM Chat".into()],
    }))
    .await;
    assert!(matches!(
        sub.recv().await.unwrap(),
        ServerMessage::InitialData(_)
    ));

    // Subscriber disconnects (drop the stream); writer commits two rows in
    // the gap.
    drop(sub);
    for text in ["gap-1", "gap-2"] {
        writer
            .send(ClientMessage::ReducerCall(ReducerCall {
                id: 1,
                reducer: "send_chat".into(),
                version: None,
                args: vec![FluxValue::Str(text.into())],
                idempotency_key: None,
            }))
            .await;
        writer.recv().await.unwrap();
    }

    // Reconnect + re-subscribe: fresh InitialData reflects all committed
    // rows, and the tx_id has advanced past what the client last saw.
    let mut sub2 = Client::connect(h.server.local_addr).await;
    sub2.authenticate(b"subscriber", 1).await;
    sub2.send(ClientMessage::Subscribe(Subscribe {
        id: 2,
        queries: vec!["SELECT * FROM Chat".into()],
    }))
    .await;
    match sub2.recv().await.unwrap() {
        ServerMessage::InitialData(data) => {
            assert_eq!(data.id, 2);
            // The two gap commits are present in the fresh snapshot.
            assert!(data.tables[0].inserts.len() >= 2, "missed rows recovered");
        }
        other => panic!("expected fresh InitialData, got {other:?}"),
    }
    h.server.shutdown();
}

// --- Lifecycle hooks over the transport (RED-011/012, UC-1 presence) ------------

static ONLINE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "conn",
        ty: FluxType::ConnectionId,
    },
    ColumnSchema {
        name: "who",
        ty: FluxType::Identity,
    },
];
static ONLINE: TableSchema = TableSchema {
    name: "OnlineUser",
    columns: ONLINE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

struct OnlineUser {
    conn: fluxum_core::types::ConnectionId,
    who: fluxum_core::types::Identity,
}

impl Table for OnlineUser {
    type Pk = fluxum_core::types::ConnectionId;
    const SCHEMA: &'static TableSchema = &ONLINE;
    fn primary_key(&self) -> Self::Pk {
        self.conn
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::ConnectionId(self.conn),
            RowValue::Identity(self.who),
        ]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::ConnectionId(conn), RowValue::Identity(who)] => Ok(Self {
                conn: *conn,
                who: *who,
            }),
            other => Err(fluxum_core::FluxumError::Storage(format!(
                "OnlineUser: unexpected shape {other:?}"
            ))),
        }
    }
    fn pk_values(pk: &Self::Pk) -> Vec<RowValue> {
        vec![RowValue::ConnectionId(*pk)]
    }
}

fn presence_connect(ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    ctx.tx.insert(OnlineUser {
        conn: ctx.connection_id,
        who: ctx.identity,
    })?;
    Ok(())
}

fn presence_disconnect(ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    ctx.tx.delete::<OnlineUser>(ctx.connection_id)?;
    Ok(())
}

static PRESENCE_CONNECT: fluxum_core::reducer::LifecycleDef = fluxum_core::reducer::LifecycleDef {
    kind: fluxum_core::reducer::LifecycleKind::OnConnect,
    name: "presence_connect",
    handler: presence_connect,
};
static PRESENCE_DISCONNECT: fluxum_core::reducer::LifecycleDef =
    fluxum_core::reducer::LifecycleDef {
        kind: fluxum_core::reducer::LifecycleKind::OnDisconnect,
        name: "presence_disconnect",
        handler: presence_disconnect,
    };

async fn start_presence() -> Harness {
    start_hooked(LifecycleHooks::from_defs([
        &PRESENCE_CONNECT,
        &PRESENCE_DISCONNECT,
    ]))
    .await
}

async fn start_hooked(hooks: LifecycleHooks) -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT, &ONLINE]).unwrap();
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
    let registry = Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap());
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        hooks,
        SHARD,
        fluxum_core::auth::server_identity("tcp-test"),
    );
    let subscriptions = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let authenticator =
        Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subscriptions, authenticator, SHARD, 256);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

async fn wait_for_rows(store: &MemStore, table: &str, want: usize) -> bool {
    let tid = store.table_id(table).unwrap();
    for _ in 0..200 {
        if store.snapshot().row_count(tid).unwrap() == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_hooks_fire_and_fan_out_over_the_transport() {
    let h = start_presence().await;

    // Connect + authenticate → the `on_connect` hook inserts a presence row
    // and its diff is published to the fan-out (RED-011).
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 1).await;
    assert!(
        wait_for_rows(&h.store, "OnlineUser", 1).await,
        "on_connect did not insert the presence row"
    );

    // Drop the socket → the read loop sees EOF and runs `on_disconnect`,
    // deleting the presence row (RED-012).
    drop(client);
    assert!(
        wait_for_rows(&h.store, "OnlineUser", 0).await,
        "on_disconnect did not clean up the presence row"
    );

    h.server.shutdown();
}

// --- Failing lifecycle hooks must not break the transport --------------------------

fn failing_hook(_ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    Err(fluxum_core::FluxumError::Reducer("hook boom".into()))
}
static FAILING_CONNECT: fluxum_core::reducer::LifecycleDef = fluxum_core::reducer::LifecycleDef {
    kind: fluxum_core::reducer::LifecycleKind::OnConnect,
    name: "failing_connect",
    handler: failing_hook,
};
static FAILING_DISCONNECT: fluxum_core::reducer::LifecycleDef =
    fluxum_core::reducer::LifecycleDef {
        kind: fluxum_core::reducer::LifecycleKind::OnDisconnect,
        name: "failing_disconnect",
        handler: failing_hook,
    };

#[tokio::test(flavor = "multi_thread")]
async fn failing_lifecycle_hooks_do_not_break_the_tcp_transport() {
    let h = start_hooked(LifecycleHooks::from_defs([
        &FAILING_CONNECT,
        &FAILING_DISCONNECT,
    ]))
    .await;

    // on_connect fails (warn only): the AuthResult still lands and the
    // session is fully usable.
    let mut client = Client::connect(h.server.local_addr).await;
    let reply = client.authenticate(b"alice", 1).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("survives".into())],
            idempotency_key: None,
        }))
        .await;
    assert!(matches!(
        client.recv().await.unwrap(),
        ServerMessage::ReducerResult(_)
    ));

    // on_disconnect fails on EOF (warn only): the server keeps serving.
    drop(client);
    let mut next = Client::connect(h.server.local_addr).await;
    let reply = next.authenticate(b"bob", 3).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    h.server.shutdown();
}

// --- RPC-001: keep-alive frames and malformed envelopes ---------------------------

#[tokio::test(flavor = "multi_thread")]
async fn client_keepalive_frames_are_ignored() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    // A zero-length keep-alive before and between real frames is a no-op.
    client.send_raw(&FrameCodec::keepalive()).await;
    let reply = client.authenticate(b"alice", 1).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_envelope_is_400_and_keeps_the_connection() {
    let h = start(TcpOptions::default()).await;
    let mut client = Client::connect(h.server.local_addr).await;
    // A well-framed body that is not a decodable ClientMessage.
    let garbage = client.codec.encode(&[0xC1, 0xFF, 0x00]).unwrap();
    client.send_raw(&garbage).await;
    match client.recv().await.unwrap() {
        ServerMessage::Error(e) => {
            assert_eq!(
                e.code,
                fluxum_protocol::codes::PROTO_MALFORMED,
                "RPC-001 malformed envelope"
            );
            assert_eq!(e.id, None, "no id to echo");
        }
        other => panic!("expected 400 Error, got {other:?}"),
    }
    // The connection stays open: a following Authenticate succeeds.
    let reply = client.authenticate(b"alice", 2).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    h.server.shutdown();
}

// --- RPC-060: idle timeout disabled ------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
// SO_LINGER(0) is exactly what this test wants: an abortive RST close. The
// deprecation concern (drop blocking the thread) is harmless on loopback
// with empty buffers.
#[allow(deprecated)]
async fn idle_timeout_disabled_reads_without_expiry_and_survives_a_reset() {
    let options = TcpOptions {
        idle_timeout: None,
        ..TcpOptions::default()
    };
    let h = start(options).await;
    let mut client = Client::connect(h.server.local_addr).await;
    client.authenticate(b"alice", 1).await;
    // With expiry disabled, a quiet gap does not 408 the connection.
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .send(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "send_chat".into(),
            version: None,
            args: vec![FluxValue::Str("after the gap".into())],
            idempotency_key: None,
        }))
        .await;
    assert!(matches!(
        client.recv().await.unwrap(),
        ServerMessage::ReducerResult(_)
    ));

    // An abortive close (RST via SO_LINGER 0) surfaces a read error; the
    // server logs it and keeps serving new connections.
    client.stream.set_linger(Some(Duration::ZERO)).unwrap();
    drop(client);
    let mut next = Client::connect(h.server.local_addr).await;
    let reply = next.authenticate(b"bob", 3).await;
    assert!(matches!(reply, ServerMessage::AuthResult(_)));
    h.server.shutdown();
}

// --- SPEC-023 DMX-011: the ephemeral TTL sweeper over the transport ----------------

static EPH_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "note",
        ty: FluxType::Str,
    },
];
static EPH: TableSchema = TableSchema {
    name: "EphNote",
    columns: EPH_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Ephemeral,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fluxum_core::schema::inventory::submit! {
    fluxum_core::schema::EphemeralDef {
        table: "EphNote",
        owner: None,
        expire_after_us: Some(300_000), // 300 ms TTL → 100 ms sweep cadence
    }
}

async fn start_ephemeral() -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT, &EPH]).unwrap();
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
    let registry = Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap());
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("tcp-test"),
    );
    let subscriptions = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let authenticator =
        Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subscriptions, authenticator, SHARD, 256);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_rows_expire_and_the_sweep_fans_out_deletes() {
    let h = start_ephemeral().await;
    // Requesting the sweeper again is a no-op (both transports call it).
    h.ctx.start_ephemeral_sweeper();

    // Seed one ephemeral row directly in the committed store.
    let eph = h.store.table_id("EphNote").unwrap();
    let mut tx = h.store.begin();
    tx.insert(
        eph,
        vec![RowValue::U64(1), RowValue::Str("fleeting".into())],
    )
    .unwrap();
    let diff = tx.commit().unwrap();
    h.ctx.publish_commit(diff);

    // A subscriber sees the row in its InitialData…
    let mut sub = Client::connect(h.server.local_addr).await;
    sub.authenticate(b"watcher", 1).await;
    sub.send(ClientMessage::SubscribeSingle(SubscribeSingle {
        id: 5,
        query: "SELECT * FROM EphNote".into(),
    }))
    .await;
    match sub.recv().await.unwrap() {
        ServerMessage::InitialData(data) => {
            assert_eq!(data.tables[0].inserts.len(), 1, "the seeded row");
        }
        other => panic!("expected InitialData, got {other:?}"),
    }

    // …then the DMX-011 sweep deletes it after the TTL and the delete diff
    // fans out as a TxUpdate.
    match sub.recv_timeout(Duration::from_secs(5)).await {
        Some(ServerMessage::TxUpdate(update)) => {
            assert_eq!(update.tables[0].deletes.len(), 1, "the swept row");
            assert!(update.tables[0].inserts.is_empty());
        }
        other => panic!("expected the sweep TxUpdate, got {other:?}"),
    }
    assert!(
        wait_for_rows(&h.store, "EphNote", 0).await,
        "the expired row is gone from the store"
    );
    h.server.shutdown();
}
