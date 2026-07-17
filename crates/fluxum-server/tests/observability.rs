//! SPEC-012 (T5.6 exit) — observability: the `fluxum_*` Prometheus
//! catalogue over `/metrics`, reducer outcome + tx counters, the pinned
//! latency histogram buckets, `/health` status semantics (200 ok / 503
//! degraded), and the structured slow-reducer WARN + panic ERROR lines.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::ShardState;
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
use fluxum_server::http::{self, HttpOptions};
use serde_json::Value;

const SHARD: u32 = 3;

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
struct NoteRow {
    id: u64,
    text: String,
}
impl Table for NoteRow {
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
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn add_note(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("add_note(text)".into())),
    };
    ctx.tx.insert(NoteRow { id: 0, text })?;
    Ok(())
}
fn fail_note(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    Err(fluxum_core::FluxumError::Reducer("always fails".into()))
}
fn boom_note(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    panic!("boom in a reducer");
}
fn slow_note(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    std::thread::sleep(Duration::from_millis(3));
    Ok(())
}
fn nop_check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static ADD_NOTE: ReducerDef = ReducerDef {
    name: "add_note",
    handler: add_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static FAIL_NOTE: ReducerDef = ReducerDef {
    name: "fail_note",
    handler: fail_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static BOOM_NOTE: ReducerDef = ReducerDef {
    name: "boom_note",
    handler: boom_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static SLOW_NOTE: ReducerDef = ReducerDef {
    name: "slow_note",
    handler: slow_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn build_ctx() -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&NOTE]).unwrap();
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
        Arc::new(
            ReducerRegistry::from_defs([&ADD_NOTE, &FAIL_NOTE, &BOOM_NOTE, &SLOW_NOTE]).unwrap(),
        ),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("obs-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 256)
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("obs"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: SHARD,
    }
}

// --- Acceptance 1/2/3: catalogue, outcome counters, histogram buckets -------------

#[tokio::test]
async fn metrics_catalogue_is_complete_with_outcomes_and_histogram_buckets() {
    let ctx = build_ctx();
    // One commit, one business-error rollback (drive them through the engine).
    ctx.engine
        .call(caller(), "add_note", vec![FluxValue::Str("hello".into())])
        .await
        .unwrap();
    let _ = ctx
        .engine
        .call(caller(), "fail_note", vec![])
        .await
        .unwrap_err();

    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let resp = request(server.local_addr, "GET", "/metrics", None).await;
    assert_eq!(resp.status, 200);
    assert!(
        resp.content_type.contains("text/plain"),
        "Prometheus exposition is text/plain, got {}",
        resp.content_type
    );
    let text = resp.text();

    // OBS-010: outcome-labelled reducer counters (acceptance 2).
    assert!(
        text.contains(
            "fluxum_reducer_calls_total{shard=\"3\",reducer=\"add_note\",outcome=\"ok\"} 1"
        )
    );
    assert!(text.contains(
        "fluxum_reducer_calls_total{shard=\"3\",reducer=\"fail_note\",outcome=\"err\"} 1"
    ));
    // OBS-013: one commit, one rollback.
    assert!(text.contains("fluxum_tx_commits_total{shard=\"3\"} 1"));
    assert!(text.contains("fluxum_tx_rollbacks_total{shard=\"3\"} 1"));

    // OBS-011: every pinned histogram bucket boundary is present (acceptance 3).
    for bound in fluxum_core::metrics::REDUCER_DURATION_BUCKETS_US {
        assert!(
            text.contains(&format!("le=\"{bound}\"")),
            "missing histogram bucket le={bound}"
        );
    }
    assert!(text.contains(
        "fluxum_reducer_duration_us_bucket{shard=\"3\",reducer=\"add_note\",le=\"+Inf\"}"
    ));
    assert!(text.contains("fluxum_reducer_duration_us_count{shard=\"3\",reducer=\"add_note\"}"));

    // OBS-001: the P0 catalogue series required by 1.1 all appear.
    for series in [
        "fluxum_up{shard=\"3\"}",
        "fluxum_shard_state{shard=\"3\"}",
        "fluxum_reducer_queue_depth{shard=\"3\"}",
        "fluxum_fanout_messages_total{shard=\"3\"}",
        "fluxum_fanout_rows_total{shard=\"3\"}",
        "fluxum_subscriber_drops_total{shard=\"3\",reason=\"buffer_full\"}",
        "fluxum_subscriptions_active{shard=\"3\"}",
        "fluxum_connections_active{shard=\"3\"}",
        "fluxum_auth_success_total{shard=\"3\"}",
        "fluxum_table_rows{shard=\"3\",table=\"Note\"}",
        "fluxum_memstore_bytes{shard=\"3\"}",
    ] {
        assert!(text.contains(series), "missing catalogue series: {series}");
    }
    // The committed row is reflected in the row gauge.
    assert!(text.contains("fluxum_table_rows{shard=\"3\",table=\"Note\"} 1"));
    server.shutdown();
}

// --- Acceptance 4: /health status semantics ---------------------------------------

#[tokio::test]
async fn health_reports_ready_then_degrades_on_recovery() {
    let ctx = build_ctx();
    let receipt = ctx
        .engine
        .call(caller(), "add_note", vec![FluxValue::Str("x".into())])
        .await
        .unwrap();
    // Publish like the transport would, so the lock-free health tx_id advances.
    ctx.publish_commit(receipt.diff);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    // Ready → 200 ok, with per-shard id/state/tx_id/queue_depth.
    let resp = request(server.local_addr, "GET", "/health", None).await;
    assert_eq!(resp.status, 200);
    let body = resp.json();
    assert_eq!(body["status"], "ok");
    let shard = &body["shards"][0];
    assert_eq!(shard["id"], "3");
    assert_eq!(shard["state"], "ready");
    assert_eq!(shard["tx_id"], 1);
    assert_eq!(shard["queue_depth"], 0);

    // Force recovery → 503 degraded (OBS-060).
    ctx.metrics().set_shard_state(ShardState::Recovering);
    let resp = request(server.local_addr, "GET", "/health", None).await;
    assert_eq!(resp.status, 503);
    assert_eq!(resp.json()["status"], "degraded");

    // A starting/shutting-down shard is an error (503).
    ctx.metrics().set_shard_state(ShardState::ShuttingDown);
    let resp = request(server.local_addr, "GET", "/health", None).await;
    assert_eq!(resp.status, 503);
    assert_eq!(resp.json()["status"], "error");
    server.shutdown();
}

// --- HWA-013: /health exposes the effective configuration -------------------------

#[tokio::test]
async fn health_exposes_the_effective_configuration_when_installed() {
    let ctx = build_ctx();
    // Without an install, `/health` simply omits the key.
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let body = request(server.local_addr, "GET", "/health", None)
        .await
        .json();
    assert!(body.get("config").is_none(), "not installed → key absent");

    // The real boot path: probe the host, derive, install (HWA-012/013).
    let hardware = fluxum_core::hw::HardwareProfile::probe();
    let effective =
        fluxum_core::hw::derive(&hardware, &fluxum_core::config::Config::default()).unwrap();
    ctx.set_effective_config(&effective);

    let body = request(server.local_addr, "GET", "/health", None)
        .await
        .json();
    let config = &body["config"];
    // Probe inputs (HWA-013).
    assert!(config["hardware"]["logical_cores"].as_u64().unwrap() >= 1);
    assert!(config["hardware"]["total_ram_bytes"].as_u64().unwrap() > 0);
    // Derived values carry their provenance...
    assert!(config["worker_threads"]["value"].as_u64().unwrap() >= 1);
    assert!(config["worker_threads"]["source"].as_str().is_some());
    assert!(config["memory_budget_bytes"]["source"].as_str().is_some());
    // ...and the per-kernel SIMD selection is resolved (HWA-033).
    assert!(config["simd_kernels"]["value"].is_object());
    server.shutdown();
}

// --- Acceptance 8: a panicking reducer rolls back; the shard keeps serving ---------

#[tokio::test]
async fn reducer_panic_rolls_back_and_shard_stays_ready() {
    let ctx = build_ctx();
    let err = ctx
        .engine
        .call(caller(), "boom_note", vec![])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("boom"), "{err}");

    // The panic counted as an err outcome + a rollback, and the shard is
    // still Ready (fluxum_shard_state == 2) — it kept serving.
    let m = ctx.metrics();
    assert_eq!(m.shard_state(), ShardState::Ready);
    let text = m.prometheus(0);
    assert!(text.contains(
        "fluxum_reducer_calls_total{shard=\"3\",reducer=\"boom_note\",outcome=\"err\"} 1"
    ));
    assert!(text.contains("fluxum_tx_rollbacks_total{shard=\"3\"} 1"));
    assert!(text.contains("fluxum_shard_state{shard=\"3\"} 2"));

    // The shard still serves the next call.
    ctx.engine
        .call(caller(), "add_note", vec![FluxValue::Str("after".into())])
        .await
        .unwrap();
}

// --- Acceptance 6/7: structured slow-reducer WARN captured from tracing ------------

#[tokio::test(flavor = "current_thread")]
async fn slow_reducer_emits_a_structured_warn_line() {
    use tracing_subscriber::prelude::*;

    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = SharedWriter(Arc::clone(&buffer));
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(move || writer.clone()),
    );
    let _guard = tracing::subscriber::set_default(subscriber);

    let ctx = build_ctx();
    // Threshold 1µs → the 3ms slow_note reducer trips the WARN (OBS-072).
    ctx.metrics().set_slow_reducer_threshold_us(1);
    ctx.engine
        .call(caller(), "slow_note", vec![])
        .await
        .unwrap();

    let logged = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    // Every emitted line is a JSON object with a `level` field (acceptance 6),
    // and one carries the slow_reducer event with shard/reducer/duration_us.
    let mut saw_slow = false;
    for line in logged.lines().filter(|l| !l.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).expect("each log line is JSON");
        assert!(value.get("level").is_some(), "line has a level: {line}");
        let fields = &value["fields"];
        if fields.get("event").and_then(Value::as_str) == Some("slow_reducer") {
            saw_slow = true;
            assert_eq!(value["level"], "WARN");
            assert_eq!(fields["reducer"], "slow_note");
            assert_eq!(fields["shard"], 3);
            assert!(fields["duration_us"].as_u64().unwrap() >= 1);
        }
    }
    assert!(saw_slow, "expected a slow_reducer WARN line in:\n{logged}");
}

/// A `MakeWriter` sink that appends to a shared buffer (for capturing
/// `tracing` JSON output in-process).
#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// --- Minimal HTTP/1.1 request helper ----------------------------------------------

struct Resp {
    status: u16,
    content_type: String,
    body: Vec<u8>,
}
impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

async fn request(addr: std::net::SocketAddr, method: &str, path: &str, body: Option<&str>) -> Resp {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    let headers_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break buf.len();
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut content_length = 0usize;
    let mut content_type = String::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_owned();
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            } else if k == "content-type" {
                content_type = v;
            }
        }
    }
    let mut body: Vec<u8> = buf[headers_end + 4..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Resp {
        status,
        content_type,
        body,
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
