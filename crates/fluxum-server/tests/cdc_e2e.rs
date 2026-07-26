//! SPEC-020 §6 (PLG-050) end-to-end: the server-side CDC wiring — build the
//! plugin registry from a manifest, spawn a pump per `stream_sink` binding,
//! and prove that every committed delta reaches a real out-of-process sink
//! at least once, driven only by the durable-log watch (never the write
//! path). The sink is a real TCP sidecar on an ephemeral port, exercising the
//! [`SidecarProxy`](fluxum_core::plugin::SidecarProxy) `StreamSink` wire in a
//! live pump — not a mock.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::{Config, PluginDecl, PluginHost, PluginScope};
use fluxum_core::plugin::PluginRegistry;
use fluxum_core::reducer::{LifecycleHooks, ReducerDef, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::row::Row;
use fluxum_core::store::{MemStore, RowValue, TableDiff, TableId, TxDiff};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::Timestamp;
use fluxum_protocol::frame::{Frame, FrameCodec};
use fluxum_protocol::plugin_rpc::{
    Committed, PLUGIN_RPC_VERSION, PluginRequest, PluginResponse, Ready,
};
use fluxum_server::ShardContext;

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

// --- A real TCP sidecar that records the tx ids it is fed and acks ----------------

fn spawn_stub_sidecar(seen: Arc<Mutex<Vec<u64>>>, stop: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
        for stream in listener.incoming() {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let Ok(stream) = stream else { return };
            let seen = Arc::clone(&seen);
            thread::spawn(move || serve_sink(stream, &seen));
        }
    });
    addr
}

fn serve_sink(mut stream: TcpStream, seen: &Mutex<Vec<u64>>) {
    let codec = FrameCodec::default();
    let mut buf = Vec::new();
    loop {
        let Some(body) = read_frame(&mut stream, &mut buf, &codec) else {
            return;
        };
        let request: PluginRequest = match rmp_serde::from_slice(&body) {
            Ok(request) => request,
            Err(_) => return,
        };
        let response = match request {
            PluginRequest::Hello(_) => PluginResponse::Ready(Ready {
                version: PLUGIN_RPC_VERSION,
                name: "stub-ingest".into(),
            }),
            PluginRequest::Commit(commit) => {
                seen.lock()
                    .unwrap()
                    .extend(commit.txs.iter().map(|tx| tx.tx_id));
                PluginResponse::Committed(Committed {
                    call_id: commit.call_id,
                })
            }
            _ => return,
        };
        let bytes = rmp_serde::to_vec(&response).unwrap();
        if stream.write_all(&codec.encode(&bytes).unwrap()).is_err() {
            return;
        }
    }
}

fn read_frame(stream: &mut TcpStream, buf: &mut Vec<u8>, codec: &FrameCodec) -> Option<Vec<u8>> {
    loop {
        match codec.decode(buf) {
            Ok(Some((Frame::Body(body), consumed))) => {
                let out = body.to_vec();
                buf.drain(..consumed);
                return Some(out);
            }
            Ok(Some((Frame::KeepAlive, consumed))) => {
                buf.drain(..consumed);
            }
            Ok(None) => {
                let mut chunk = [0u8; 4096];
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => return None,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            }
            Err(_) => return None,
        }
    }
}

fn item_diff(tx_id: u64) -> TxDiff {
    TxDiff {
        tx_id,
        tables: vec![TableDiff {
            table_id: TableId::of("Item"),
            inserts: vec![Row::new(vec![
                RowValue::U64(tx_id),
                RowValue::Str(format!("item-{tx_id}")),
            ])],
            deletes: vec![],
        }],
        auto_inc: vec![],
    }
}

async fn wait_until(mut done: impl FnMut() -> bool, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    done()
}

#[tokio::test]
async fn a_sidecar_cdc_sink_receives_every_committed_delta_at_least_once() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let endpoint = spawn_stub_sidecar(Arc::clone(&seen), Arc::clone(&stop));

    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let ckpt_dir = dir.path().join("cdc");
    let schema = Schema::from_tables([&ITEM]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(CommitLog::open(&log_dir, SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) = TxPipeline::new(
        Arc::clone(&store),
        Arc::clone(&log),
        TxPipelineOptions::default(),
    )
    .unwrap();
    tokio::spawn(worker.run());
    let no_reducers: [&'static ReducerDef; 0] = [];
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs(no_reducers).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("cdc-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema.clone()), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);

    // A `stream_sink` bound to the stub over Plugin RPC, the assembly-time
    // PLG-032 flow — exactly what `boot::assemble` runs.
    let config = Config {
        plugins: vec![PluginDecl {
            name: "vectorizer_ingest".into(),
            capability: "stream_sink".into(),
            host: PluginHost::Sidecar {
                endpoint: endpoint.clone(),
                timeout_ms: 2_000,
                token: None,
            },
            applies_to: PluginScope::default(),
        }],
        ..Config::default()
    };
    let registry = Arc::new(PluginRegistry::build(&schema, &config).unwrap());
    let pumps = fluxum_server::cdc::spawn_sinks(&ctx, &registry, log_dir, ckpt_dir);
    assert_eq!(pumps.len(), 1, "one pump per stream_sink binding");

    // Commit five durable transactions straight to the pump's log; the durable
    // watch wakes the pump, which streams them to the sidecar off the log.
    for i in 1..=5u64 {
        log.append_diff(&item_diff(i), Timestamp::now())
            .await
            .unwrap();
    }
    log.wait_durable(5).await.unwrap();

    assert!(
        wait_until(|| seen.lock().unwrap().len() >= 5, Duration::from_secs(10)).await,
        "the sidecar sink received the committed deltas"
    );
    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    got.dedup();
    assert_eq!(
        got,
        vec![1, 2, 3, 4, 5],
        "every committed delta reached the sink at least once (PLG-050)"
    );
    // The sink's lag settled back to zero once caught up.
    assert!(
        wait_until(
            || ctx.metrics().sink_lag("vectorizer_ingest") == 0,
            Duration::from_secs(2)
        )
        .await
    );

    stop.store(true, Ordering::Relaxed);
    for pump in &pumps {
        pump.stop();
    }
}
