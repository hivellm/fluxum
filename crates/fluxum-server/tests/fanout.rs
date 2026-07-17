//! Commit fan-out edge coverage (SUB-021/024/042; SPEC-006 acceptance 14):
//! broadcast lag recovery, an unevaluable diff, a `TxUpdate` too large to
//! frame, and the slow-consumer (Full) / closed-sink eviction tiers — all
//! against the real fan-out task spawned by the TCP transport.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, Row, RowValue, TableDiff, TxDiff};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::Identity;
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage, SubscribeSingle};
use fluxum_server::tcp::{self, TcpOptions};
use fluxum_server::{ConnHandle, OutFrame, ShardContext};

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

static MARKER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static MARKER: TableSchema = TableSchema {
    name: "Marker",
    columns: MARKER_COLS,
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

async fn start(commit_capacity: usize) -> Harness {
    start_on_shard(commit_capacity, SHARD).await
}

async fn start_on_shard(commit_capacity: usize, shard: u32) -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT, &MARKER]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            shard,
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
        shard,
        fluxum_core::auth::server_identity("fanout-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, shard, commit_capacity);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

/// Commit one row directly to the committed store and return its diff.
fn commit_row(store: &MemStore, table: &str, id: u64, text: &str) -> TxDiff {
    let tid = store.table_id(table).unwrap();
    let mut tx = store.begin();
    tx.insert(tid, vec![RowValue::U64(id), RowValue::Str(text.into())])
        .unwrap();
    tx.commit().unwrap()
}

/// Delete one row directly and return its diff (source of a real `PkBytes`).
fn delete_row(store: &MemStore, table: &str, id: u64) -> TxDiff {
    let tid = store.table_id(table).unwrap();
    let mut tx = store.begin();
    assert!(tx.delete(tid, &[RowValue::U64(id)]).unwrap());
    tx.commit().unwrap()
}

// --- A minimal framed subscriber client ------------------------------------------

struct Client {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl Client {
    async fn subscribed(addr: std::net::SocketAddr, query: &str) -> Self {
        let mut client = Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
        };
        client
            .send(ClientMessage::Authenticate(Authenticate {
                id: 1,
                token: b"subscriber".to_vec(),
                compression: None,
                tx_updates: None,
            }))
            .await;
        assert!(matches!(
            client.recv().await.unwrap(),
            ServerMessage::AuthResult(_)
        ));
        client
            .send(ClientMessage::SubscribeSingle(SubscribeSingle {
                id: 2,
                query: query.into(),
            }))
            .await;
        assert!(matches!(
            client.recv().await.unwrap(),
            ServerMessage::InitialData(_)
        ));
        client
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

    async fn recv_update(&mut self, timeout: Duration) -> Option<fluxum_protocol::TxUpdate> {
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

// --- SPEC-006 acceptance 14: broadcast lag never wedges the fan-out ---------------

#[tokio::test(flavor = "multi_thread")]
async fn fanout_recovers_from_broadcast_lag() {
    let h = start(4).await; // tiny commit backlog → easy to overflow
    let mut sub = Client::subscribed(h.server.local_addr, "SELECT * FROM Marker").await;

    // Stall the fan-out (it needs the subscription mutex per diff) and
    // overflow the 4-slot broadcast with unrelated Chat commits.
    {
        let _guard = h.ctx.subscriptions.lock().await;
        for i in 0..100u64 {
            let diff = commit_row(&h.store, "Chat", 1_000 + i, "flood");
            h.ctx.publish_commit(diff);
        }
    }

    // After the lag the fan-out must still deliver fresh commits.
    let diff = commit_row(&h.store, "Marker", 1, "post-lag");
    let marker_tx = diff.tx_id;
    h.ctx.publish_commit(diff);
    let update = sub
        .recv_update(Duration::from_secs(3))
        .await
        .expect("the fan-out survived the lag");
    assert_eq!(update.tx_id, marker_tx);
    assert_eq!(update.tables[0].inserts.len(), 1);
    h.server.shutdown();
}

// --- An unevaluable diff is logged and skipped -------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn fanout_skips_a_diff_that_fails_evaluation() {
    let h = start(64).await;
    let mut sub = Client::subscribed(h.server.local_addr, "SELECT * FROM Marker").await;

    // Harvest a real PkBytes from a genuine delete…
    let _ = commit_row(&h.store, "Marker", 7, "victim");
    let real_delete = delete_row(&h.store, "Marker", 7);
    let pk = real_delete.tables[0].deletes[0].0.clone();

    // …and publish a corrupt diff whose deleted row has no columns: PK
    // re-encoding fails inside evaluation, and the fan-out must skip it.
    let marker = h.store.table_id("Marker").unwrap();
    h.ctx.publish_commit(TxDiff {
        tx_id: real_delete.tx_id + 1_000,
        tables: vec![TableDiff {
            table_id: marker,
            inserts: vec![],
            deletes: vec![(pk, Row::new(vec![]))],
        }],
        auto_inc: vec![],
    });

    // The next good commit still reaches the subscriber.
    let diff = commit_row(&h.store, "Marker", 8, "good");
    let good_tx = diff.tx_id;
    h.ctx.publish_commit(diff);
    let update = sub
        .recv_update(Duration::from_secs(3))
        .await
        .expect("the fan-out skipped the corrupt diff");
    assert_eq!(update.tx_id, good_tx, "the corrupt diff produced no update");
    h.server.shutdown();
}

// --- A TxUpdate too large for one frame is skipped ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn fanout_skips_an_update_that_exceeds_the_frame_limit() {
    let h = start(64).await;
    let mut sub = Client::subscribed(h.server.local_addr, "SELECT * FROM Marker").await;

    // A row bigger than DEFAULT_MAX_FRAME_BYTES (16 MiB) cannot be framed.
    let giant = "x".repeat(17 * 1024 * 1024);
    let diff = commit_row(&h.store, "Marker", 20, &giant);
    h.ctx.publish_commit(diff);

    // The following small commit is the first update the subscriber sees.
    let diff = commit_row(&h.store, "Marker", 21, "small");
    let small_tx = diff.tx_id;
    h.ctx.publish_commit(diff);
    let update = sub
        .recv_update(Duration::from_secs(5))
        .await
        .expect("the fan-out skipped the oversized update");
    assert_eq!(update.tx_id, small_tx, "the giant update was dropped");
    h.server.shutdown();
}

// --- SUB-042: slow-consumer (Full) and closed-sink eviction ------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_full_send_buffer_drops_the_slow_subscriber_and_spares_the_healthy_one() {
    let h = start(64).await;
    let mut healthy = Client::subscribed(h.server.local_addr, "SELECT * FROM Marker").await;

    // A fake connection with a 1-frame queue that is never drained.
    let (slow_tx, _slow_rx) = mpsc::channel::<OutFrame>(1);
    let slow_shutdown = Arc::new(Notify::new());
    h.ctx
        .connections
        .insert(
            999,
            ConnHandle {
                sink: slow_tx,
                shutdown: Arc::clone(&slow_shutdown),
            },
        )
        .await;
    {
        let snapshot = h.store.snapshot();
        let mut manager = h.ctx.subscriptions.lock().await;
        manager
            .subscribe(
                999,
                Subscriber::client(Identity::from_bytes([9u8; 32])),
                "SELECT * FROM Marker",
                &snapshot,
            )
            .unwrap();
    }

    let dropped = slow_shutdown.notified();
    tokio::pin!(dropped);

    // Two commits: the first fills the 1-slot queue, the second trips the
    // SUB-042 Full tier — shutdown is notified and the handle removed.
    for id in [30, 31] {
        let diff = commit_row(&h.store, "Marker", id, "burst");
        h.ctx.publish_commit(diff);
    }
    tokio::time::timeout(Duration::from_secs(3), &mut dropped)
        .await
        .expect("the slow consumer was shut down (SUB-042 Full)");

    // The healthy subscriber received both updates.
    assert!(healthy.recv_update(Duration::from_secs(3)).await.is_some());
    assert!(healthy.recv_update(Duration::from_secs(3)).await.is_some());
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_closed_sink_is_evicted_without_disturbing_the_healthy_subscriber() {
    let h = start(64).await;
    let mut healthy = Client::subscribed(h.server.local_addr, "SELECT * FROM Marker").await;

    // A fake connection whose receiver is already gone.
    let (dead_tx, dead_rx) = mpsc::channel::<OutFrame>(1);
    drop(dead_rx);
    h.ctx
        .connections
        .insert(
            998,
            ConnHandle {
                sink: dead_tx,
                shutdown: Arc::new(Notify::new()),
            },
        )
        .await;
    {
        let snapshot = h.store.snapshot();
        let mut manager = h.ctx.subscriptions.lock().await;
        manager
            .subscribe(
                998,
                Subscriber::client(Identity::from_bytes([8u8; 32])),
                "SELECT * FROM Marker",
                &snapshot,
            )
            .unwrap();
    }

    let diff = commit_row(&h.store, "Marker", 40, "after the ghost");
    let tx_id = diff.tx_id;
    h.ctx.publish_commit(diff);
    let update = healthy
        .recv_update(Duration::from_secs(3))
        .await
        .expect("delivery continues past the closed sink");
    assert_eq!(update.tx_id, tx_id);
    h.server.shutdown();
}

// --- SPEC-007 SHD-051 (T5.5 exit 1.4): cross-shard subscription aggregation --------

#[tokio::test(flavor = "multi_thread")]
async fn tx_updates_carry_the_originating_shard_for_cross_shard_aggregation() {
    // A query spanning shards = one subscription per shard; the client
    // aggregates the streams. Every TxUpdate must carry its originating
    // shard so attribution (and per-shard ordering) is unambiguous.
    let a = start_on_shard(64, 3).await;
    let b = start_on_shard(64, 7).await;
    let mut sub_a = Client::subscribed(a.server.local_addr, "SELECT * FROM Chat").await;
    let mut sub_b = Client::subscribed(b.server.local_addr, "SELECT * FROM Chat").await;

    let diff = commit_row(&a.store, "Chat", 1, "from shard 3");
    a.ctx.publish_commit(diff);
    let diff = commit_row(&b.store, "Chat", 2, "from shard 7");
    b.ctx.publish_commit(diff);

    let from_a = sub_a
        .recv_update(Duration::from_secs(3))
        .await
        .expect("shard 3 update");
    let from_b = sub_b
        .recv_update(Duration::from_secs(3))
        .await
        .expect("shard 7 update");
    assert_eq!(from_a.shard_id, 3, "SHD-051 shard tag");
    assert_eq!(from_b.shard_id, 7, "SHD-051 shard tag");

    // Aggregation key: (shard_id, tx_id) — per-shard order is preserved,
    // and a second commit on one shard advances only that shard's stream.
    let diff = commit_row(&a.store, "Chat", 5, "again from 3");
    let second_tx = diff.tx_id;
    a.ctx.publish_commit(diff);
    let second = sub_a
        .recv_update(Duration::from_secs(3))
        .await
        .expect("second shard 3 update");
    assert_eq!(second.shard_id, 3);
    assert!(second.tx_id > from_a.tx_id);
    assert_eq!(second.tx_id, second_tx);

    a.server.shutdown();
    b.server.shutdown();
}
