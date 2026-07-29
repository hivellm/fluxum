//! F-005 framing (phase9_fanout-event-batching 1.5): one commit matching
//! K of a connection's queries travels as ONE `TxUpdate` carrying K
//! `TableUpdate` lanes — and a connection with a different match set gets
//! its own frame (the equivalence-class partition), never someone else's.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage, SubscribeSingle};
use fluxum_server::ShardContext;
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 1;

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
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

struct Harness {
    server: tcp::TcpServer,
    store: Arc<MemStore>,
    ctx: Arc<ShardContext>,
}

async fn start() -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
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
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("merge-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 64);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

fn commit_row(store: &MemStore, id: u64, text: &str) -> fluxum_core::store::TxDiff {
    let tid = store.table_id("Chat").unwrap();
    let mut tx = store.begin();
    tx.insert(tid, vec![RowValue::U64(id), RowValue::Str(text.into())])
        .unwrap();
    tx.commit().unwrap()
}

struct Client {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr, token: &str) -> Self {
        let mut client = Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
        };
        client
            .send(ClientMessage::Authenticate(Authenticate {
                id: 1,
                token: token.as_bytes().to_vec(),
                compression: None,
                tx_updates: None,
                namespace: None,
            }))
            .await;
        assert!(matches!(
            client.recv().await.unwrap(),
            ServerMessage::AuthResult(_)
        ));
        client
    }

    /// Subscribe one query; returns its server-assigned query id.
    async fn subscribe(&mut self, id: u32, query: &str) -> u32 {
        self.send(ClientMessage::SubscribeSingle(SubscribeSingle {
            id,
            query: query.into(),
        }))
        .await;
        match self.recv().await.unwrap() {
            ServerMessage::InitialData(initial) => initial.tables.first().map_or(0, |t| t.query_id),
            other => panic!("expected InitialData, got {other:?}"),
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

    async fn recv_update_within(&mut self, timeout: Duration) -> Option<fluxum_protocol::TxUpdate> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let msg = tokio::time::timeout_at(deadline, self.recv())
                .await
                .ok()??;
            if let ServerMessage::TxUpdate(update) = msg {
                return Some(update);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_commit_matching_two_queries_is_one_merged_frame() {
    let h = start().await;

    // Two overlapping queries on one connection; a single-query bystander
    // proves the class partition delivers per match set.
    let mut both = Client::connect(h.server.local_addr, "both").await;
    let q_all = both.subscribe(2, "SELECT * FROM Chat").await;
    let q_pos = both.subscribe(3, "SELECT * FROM Chat WHERE id > 0").await;
    assert_ne!(q_all, q_pos, "distinct queries get distinct ids");

    let mut single = Client::connect(h.server.local_addr, "single").await;
    let q_single = single.subscribe(2, "SELECT * FROM Chat").await;

    h.ctx.publish_commit(commit_row(&h.store, 7, "merged"));

    // The two-query connection gets ONE frame carrying BOTH lanes.
    let update = both
        .recv_update_within(Duration::from_secs(3))
        .await
        .expect("the merged update");
    assert_eq!(
        update.tables.len(),
        2,
        "one commit, two matched queries, ONE frame (F-005)"
    );
    let mut ids: Vec<u32> = update.tables.iter().map(|t| t.query_id).collect();
    ids.sort_unstable();
    let mut wanted = vec![q_all, q_pos];
    wanted.sort_unstable();
    assert_eq!(ids, wanted, "each lane is stamped with its own query_id");
    assert_eq!(
        update.tables[0].inserts.rows_data, update.tables[1].inserts.rows_data,
        "both lanes carry the same committed row"
    );
    // And no second frame follows for the same commit.
    assert!(
        both.recv_update_within(Duration::from_millis(300))
            .await
            .is_none(),
        "the old per-query path would have sent a second frame"
    );

    // The bystander's frame carries exactly its own lane.
    let update = single
        .recv_update_within(Duration::from_secs(3))
        .await
        .expect("the single-query update");
    assert_eq!(update.tables.len(), 1);
    assert_eq!(update.tables[0].query_id, q_single);

    // OBS-024: the writers recorded their coalesced writes.
    let exposition = h.ctx.metrics().prometheus(u64::from(SHARD));
    assert!(
        exposition.contains("fluxum_writer_writes_total"),
        "the OBS-024 families are exported"
    );
    h.server.shutdown();
}
