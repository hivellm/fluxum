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
    let server = tcp::serve(ctx, "127.0.0.1:0", options).await.unwrap();
    Harness { server, store }
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
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::Error(e) => {
            assert_eq!(e.code, 401);
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
        }))
        .await;
    match client.recv().await.unwrap() {
        ServerMessage::ReducerResult(r) => {
            assert_eq!(r.id, 43);
            assert_eq!(r.outcome.unwrap_err(), "empty text");
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
        Some(ServerMessage::Error(e)) => assert_eq!(e.code, 408),
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
        Some(ServerMessage::Error(e)) => assert_eq!(e.code, 413),
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
