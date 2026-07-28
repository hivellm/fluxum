//! phase9_delta-compression 1.6: wire bytes per position update, per layer,
//! on the MMO-shape workload — an update travels as delete(pk) + full row of
//! a `Player`-like table, exactly what `move_player` fans out.
//!
//! Four sessions subscribe to the same query and the same commits stream to
//! all of them; what differs is the negotiated wire: full/none (the
//! baseline), light/none (RPC-035), full/gzip and light/gzip (RPC-008
//! stream-deflate). The test asserts the layering pays in order and prints
//! the measured table — the numbers quoted in the task's before/after
//! record come from this run, reproducible with
//! `cargo test -p fluxum-server --test wire_layers_measure -- --nocapture`.
//!
//! Latency guard: per-frame e2e (commit publish → client decode) is
//! measured on the same run; the compressed sessions must stay in the same
//! order of magnitude as the baseline (loopback, in-process — a relative
//! guard, not an absolute SLO).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

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
use fluxum_protocol::{
    Authenticate, ClientMessage, FrameCodec, ServerMessage, StreamDecompressor, SubscribeSingle,
    TAG_GZIP_STREAM, TAG_UNCOMPRESSED,
};
use fluxum_server::ShardContext;
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 1;

// The MMO row: identity-sized name + integer coords, like the demo Player.
static PLAYER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "hue",
        ty: FluxType::U64,
    },
];
static PLAYER: TableSchema = TableSchema {
    name: "Player",
    columns: PLAYER_COLS,
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
    let schema = Schema::from_tables([&PLAYER]).unwrap();
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
        fluxum_core::auth::server_identity("wire-measure"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

fn spawn_player(store: &MemStore, id: u64) {
    let tid = store.table_id("Player").unwrap();
    let mut tx = store.begin();
    tx.insert(
        tid,
        vec![
            RowValue::U64(id),
            RowValue::Str(format!("p-{id:06x}")),
            RowValue::I64(1000),
            RowValue::I64(600),
            RowValue::U64(id % 360),
        ],
    )
    .unwrap();
    let diff = tx.commit().unwrap();
    drop(diff);
}

/// One position update, the MMO wire shape: delete(pk) + insert(new row)
/// in a single commit.
fn move_player(store: &MemStore, id: u64, x: i64, y: i64) -> fluxum_core::store::TxDiff {
    let tid = store.table_id("Player").unwrap();
    let mut tx = store.begin();
    assert!(tx.delete(tid, &[RowValue::U64(id)]).unwrap());
    tx.insert(
        tid,
        vec![
            RowValue::U64(id),
            RowValue::Str(format!("p-{id:06x}")),
            RowValue::I64(x),
            RowValue::I64(y),
            RowValue::U64(id % 360),
        ],
    )
    .unwrap();
    tx.commit().unwrap()
}

struct MeasuredClient {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
    inflate: Option<StreamDecompressor>,
    armed: bool,
    label: &'static str,
    /// Wire bytes (frame prefix + tag + payload) per received update.
    update_bytes: Vec<usize>,
    /// Publish→decode latency per received update, µs.
    latency_us: Vec<u64>,
}

impl MeasuredClient {
    async fn connect(
        addr: std::net::SocketAddr,
        label: &'static str,
        compression: Option<&str>,
        tx_updates: Option<&str>,
    ) -> Self {
        let mut client = Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
            inflate: None,
            armed: false,
            label,
            update_bytes: Vec::new(),
            latency_us: Vec::new(),
        };
        client
            .send(ClientMessage::Authenticate(Authenticate {
                id: 1,
                token: label.as_bytes().to_vec(),
                compression: compression.map(str::to_string),
                tx_updates: tx_updates.map(str::to_string),
                namespace: None,
            }))
            .await;
        let (first, _) = client.recv().await.unwrap();
        if let ServerMessage::AuthResult(auth) = &first
            && auth.compression.as_deref() == Some("gzip")
        {
            client.armed = true;
            client.inflate = Some(StreamDecompressor::new());
        }
        client
            .send(ClientMessage::SubscribeSingle(SubscribeSingle {
                id: 2,
                query: "SELECT * FROM Player".into(),
            }))
            .await;
        let (initial, _) = client.recv().await.unwrap();
        assert!(matches!(initial, ServerMessage::InitialData(_)));
        client
    }

    async fn send(&mut self, message: ClientMessage) {
        let body = message.encode().unwrap();
        let framed = self.codec.encode(&body).unwrap();
        self.stream.write_all(&framed).await.unwrap();
    }

    /// Next message + its on-the-wire size (length prefix included).
    async fn recv(&mut self) -> Option<(ServerMessage, usize)> {
        loop {
            if let Ok(Some((frame, consumed))) = self.codec.decode(&self.buf) {
                let out = match frame {
                    fluxum_protocol::Frame::Body(body) => {
                        let message = if self.armed {
                            match body[0] {
                                TAG_GZIP_STREAM => {
                                    let inflated = self
                                        .inflate
                                        .as_mut()
                                        .unwrap()
                                        .inflate_chunk(&body[1..], 16 << 20)
                                        .unwrap();
                                    ServerMessage::decode(&inflated).unwrap()
                                }
                                TAG_UNCOMPRESSED => ServerMessage::decode(&body[1..]).unwrap(),
                                other => panic!("unexpected tag {other:#x}"),
                            }
                        } else {
                            ServerMessage::decode(body).unwrap()
                        };
                        Some((message, consumed))
                    }
                    fluxum_protocol::Frame::KeepAlive => None,
                };
                self.buf.drain(..consumed);
                if let Some(out) = out {
                    return Some(out);
                }
                continue;
            }
            let mut chunk = [0u8; 8192];
            match self.stream.read(&mut chunk).await {
                Ok(0) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }

    /// Await the next broadcast update and record its size + latency.
    async fn record_update(&mut self, published: Instant) {
        loop {
            let (message, wire) = tokio::time::timeout(Duration::from_secs(5), self.recv())
                .await
                .expect("update within 5s")
                .expect("stream alive");
            match message {
                ServerMessage::TxUpdate(_) | ServerMessage::TxUpdateLight(_) => {
                    self.update_bytes.push(wire);
                    self.latency_us
                        .push(u64::try_from(published.elapsed().as_micros()).unwrap_or(u64::MAX));
                    return;
                }
                _ => {}
            }
        }
    }

    fn mean_bytes(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let total: f64 = self.update_bytes.iter().sum::<usize>() as f64;
        #[allow(clippy::cast_precision_loss)]
        let n = self.update_bytes.len() as f64;
        total / n.max(1.0)
    }

    fn latency_p(&self, q: f64) -> u64 {
        let mut sorted = self.latency_us.clone();
        sorted.sort_unstable();
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let ix = ((sorted.len() as f64 - 1.0) * q).round() as usize;
        sorted.get(ix).copied().unwrap_or(0)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wire_layers_pay_in_order_on_the_mmo_shape() {
    const PLAYERS: u64 = 20;
    const MOVES: u64 = 400;

    let h = start().await;
    for id in 0..PLAYERS {
        spawn_player(&h.store, id);
    }

    let addr = h.server.local_addr;
    let mut sessions = vec![
        MeasuredClient::connect(addr, "full/none", None, Some("full")).await,
        MeasuredClient::connect(addr, "light/none", None, Some("light")).await,
        MeasuredClient::connect(addr, "full/gzip", Some("gzip"), Some("full")).await,
        MeasuredClient::connect(addr, "light/gzip", Some("gzip"), Some("light")).await,
    ];

    // The workload: players move round-robin, one commit per move — the
    // fan-out delivers every commit to all four sessions. Provenance is
    // REAL (a distinct identity per player + the reducer name), which is
    // what makes the comparison honest: 32 caller bytes rotating across 20
    // identities are exactly what a deflate window cannot amortize, and
    // stripping them (RPC-035) is the layer gzip cannot replace.
    let identities: Vec<fluxum_core::types::Identity> = (0..PLAYERS)
        .map(|id| fluxum_core::auth::server_identity(&format!("player-{id}")))
        .collect();
    for i in 0..MOVES {
        let id = i % PLAYERS;
        #[allow(clippy::cast_possible_wrap)]
        let (x, y) = (
            ((i * 7) % 2000).cast_signed(),
            ((i * 13) % 1200).cast_signed(),
        );
        let diff = move_player(&h.store, id, x, y);
        let published = Instant::now();
        h.ctx.publish_commit_meta(
            diff,
            fluxum_core::txn::CommitMeta {
                caller: identities[usize::try_from(id).unwrap()],
                reducer_name: "move_player".into(),
            },
        );
        for session in &mut sessions {
            session.record_update(published).await;
        }
    }

    let mut report =
        String::from("\nwire bytes per MMO position update (delete+insert of one Player row)\n");
    for s in &sessions {
        report.push_str(&format!(
            "  {:<11} mean {:>6.1} B/update   e2e p50 {:>5} µs  p99 {:>5} µs\n",
            s.label,
            s.mean_bytes(),
            s.latency_p(0.50),
            s.latency_p(0.99),
        ));
    }
    // The compressor's own cost, from the server's counters (RPC-008).
    let exposition = h.ctx.metrics().prometheus(u64::from(SHARD));
    for line in exposition.lines() {
        if line.starts_with("fluxum_wire_compression_") {
            report.push_str("  ");
            report.push_str(line);
            report.push('\n');
        }
    }
    println!("{report}");

    let full = sessions[0].mean_bytes();
    let light = sessions[1].mean_bytes();
    let full_gz = sessions[2].mean_bytes();
    let light_gz = sessions[3].mean_bytes();

    // The layering must pay in order: light strips the envelope, the stream
    // window then compresses what remains — and the composed stack beats
    // every single layer.
    assert!(light < full, "light {light:.1} !< full {full:.1}");
    assert!(full_gz < full, "full/gzip {full_gz:.1} !< full {full:.1}");
    assert!(
        light_gz < light,
        "light/gzip {light_gz:.1} !< light {light:.1}"
    );
    assert!(
        light_gz < full_gz,
        "light/gzip {light_gz:.1} !< full/gzip {full_gz:.1}"
    );
    // The compounded win the proposal promised: at least 3x off the
    // baseline on this workload (measured ~5x; the assert keeps margin).
    assert!(
        light_gz * 3.0 < full,
        "compounded stack only {full:.1} -> {light_gz:.1}"
    );
    // Latency guard (loopback, relative): compression must not move e2e
    // delivery out of its order of magnitude.
    let base_p99 = sessions[0].latency_p(0.99).max(1);
    let gz_p99 = sessions[3].latency_p(0.99);
    assert!(
        gz_p99 < base_p99 * 10,
        "gzip p99 {gz_p99} µs vs baseline p99 {base_p99} µs"
    );
    h.server.shutdown();
}
