//! SPEC-025 OPS-040/041 (config hot reload, task 1.7 exit) — reloading a
//! *running* shard: the level change takes effect with no restart and
//! `/health` reflects it; a changed port is rejected with a clear error and
//! nothing is applied.
//!
//! The core reload logic (which keys are frozen, what "changed" means) is
//! unit-tested in `fluxum_core::config::reload_tests`. What can only be
//! tested here is the part that touches a live shard: that a reload actually
//! *reaches* the running knobs (the rate limiter, the metrics threshold, the
//! `/health` view) and that a rejected reload leaves every one of them alone.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::Config;
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, HttpOptions};
use serde_json::Value;

const SHARD: u32 = 7;

static NOTE_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
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
        Arc::new(ReducerRegistry::from_defs([]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("reload-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 256)
}

/// A config file under the `development` profile (the default `production`
/// profile demands an auth secret, which is orthogonal to reloading).
fn write_config(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("fluxum.yaml");
    let mut file = std::fs::File::create(&path).unwrap();
    write!(file, "profile: development\n{body}").unwrap();
    path
}

fn load(path: &std::path::Path) -> Config {
    Config::load(Some(path)).unwrap()
}

// --- Acceptance 1: level info→debug takes effect live, /health reflects it -------

#[tokio::test]
async fn a_reload_reaches_the_running_shard_and_health_reports_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "logging:\n  level: info\nobservability:\n  slow_reducer_threshold_us: 5000\n",
    );
    let ctx = build_ctx();
    // No logging handle: this test process shares one global subscriber with
    // every other test, so installing one here is a coin flip. The remaining
    // keys still publish — that independence is deliberate.
    ctx.install_config(Some(path.clone()), load(&path), None);

    // The boot publish already reached the live metrics registry.
    assert_eq!(ctx.metrics().slow_reducer_threshold_us(), 5_000);

    write_config(
        dir.path(),
        "logging:\n  level: debug\nobservability:\n  slow_reducer_threshold_us: 250\n",
    );
    let changed = ctx.reload_config().expect("a reloadable-only change");
    assert_eq!(
        changed,
        vec![
            "logging.level".to_owned(),
            "observability.slow_reducer_threshold_us".to_owned()
        ],
        "the reload reports exactly what an operator changed"
    );
    // The knob moved on the *running* shard — no restart involved.
    assert_eq!(ctx.metrics().slow_reducer_threshold_us(), 250);

    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let health = request(server.local_addr, "GET", "/health", None).await;
    assert_eq!(health.status, 200);
    let reloadable = &health.json()["reloadable"];
    assert_eq!(reloadable["logging.level"]["value"], "debug");
    assert_eq!(
        reloadable["logging.level"]["source"], "file",
        "/health explains *why* a value is what it is, not just what it is"
    );
    assert_eq!(
        reloadable["observability.slow_reducer_threshold_us"]["value"],
        250
    );
}

// --- Acceptance 2: a changed port is rejected, nothing is applied ----------------

#[tokio::test]
async fn a_changed_port_is_rejected_and_no_reloadable_key_moves() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "server:\n  http_port: 15800\nobservability:\n  slow_reducer_threshold_us: 5000\n",
    );
    let ctx = build_ctx();
    ctx.install_config(Some(path.clone()), load(&path), None);
    assert_eq!(ctx.metrics().slow_reducer_threshold_us(), 5_000);

    // A frozen key *and* a reloadable one change together: the reloadable
    // half must not sneak through. That is what "all-or-nothing" buys.
    write_config(
        dir.path(),
        "server:\n  http_port: 19999\nobservability:\n  slow_reducer_threshold_us: 111\n",
    );
    let err = ctx.reload_config().expect_err("a frozen key changed");
    assert!(
        err.contains("server.http_port"),
        "names the offender: {err}"
    );
    assert!(
        err.contains("Restart to apply"),
        "tells the operator what to do: {err}"
    );
    assert_eq!(
        ctx.metrics().slow_reducer_threshold_us(),
        5_000,
        "a rejected reload applies nothing at all (OPS-041)"
    );

    // The shard is still reloadable afterwards: a rejection is not a
    // latch. Fix the file, reload again, and it lands.
    write_config(
        dir.path(),
        "server:\n  http_port: 15800\nobservability:\n  slow_reducer_threshold_us: 111\n",
    );
    let changed = ctx.reload_config().expect("port restored");
    assert_eq!(changed, vec!["observability.slow_reducer_threshold_us"]);
    assert_eq!(ctx.metrics().slow_reducer_threshold_us(), 111);
}

// --- The reload reaches every reloadable knob, not just the observable one -------

#[tokio::test]
async fn every_reloadable_key_reaches_its_live_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "reducer:\n  shard_max_reducers_per_sec: 1000\nsubscriptions:\n  send_buffer_bytes: 2MiB\n",
    );
    let ctx = build_ctx();
    ctx.install_config(Some(path.clone()), load(&path), None);
    assert_eq!(
        ctx.engine.rate_limiter().shard_max_reducers_per_sec(),
        Some(1_000),
        "boot publishes through the same path a reload does"
    );
    assert_eq!(ctx.send_buffer_bytes(), 2 * 1024 * 1024);

    // SEC-046: the RED-052 guard is mandatory-on — a reload that tries to
    // disable it is rejected by validation and nothing is applied.
    write_config(
        dir.path(),
        "reducer:\n  shard_max_reducers_per_sec: 0\nsubscriptions:\n  send_buffer_bytes: 8MiB\n",
    );
    let err = ctx.reload_config().expect_err("0 would disable the guard");
    assert!(err.contains("shard_max_reducers_per_sec"), "{err}");
    assert_eq!(
        ctx.engine.rate_limiter().shard_max_reducers_per_sec(),
        Some(1_000),
        "a rejected reload applies nothing (OPS-041)"
    );

    // Retuning it (and the SEC-045/046/047 bounds) lands without a restart.
    write_config(
        dir.path(),
        "reducer:\n  shard_max_reducers_per_sec: 2000\n  max_execution_ms: 1234\n  \
         max_tx_bytes: 1MiB\nsubscriptions:\n  send_buffer_bytes: 8MiB\nquery:\n  \
         max_queries_per_sec_per_identity: 7\n  max_queries_per_sec_per_source: 9\n",
    );
    ctx.reload_config().unwrap();
    assert_eq!(
        ctx.engine.rate_limiter().shard_max_reducers_per_sec(),
        Some(2_000)
    );
    assert_eq!(
        ctx.engine.bounds().get(),
        (1_234, 1024 * 1024),
        "the SEC-046 reducer bounds reach the engine"
    );
    assert_eq!(
        ctx.query_limiter().rates(),
        (7, 9),
        "the SEC-047 admission rates reach the limiter"
    );
    assert_eq!(ctx.send_buffer_bytes(), 8 * 1024 * 1024);
}

// --- POST /config/reload (OPS-040) ----------------------------------------------

#[tokio::test]
async fn the_admin_endpoint_reloads_and_reports_what_changed() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        dir.path(),
        "observability:\n  slow_reducer_threshold_us: 5000\n",
    );
    let ctx = build_ctx();
    ctx.install_config(Some(path.clone()), load(&path), None);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    write_config(
        dir.path(),
        "observability:\n  slow_reducer_threshold_us: 42\n",
    );
    let resp = request(server.local_addr, "POST", "/config/reload", Some("{}")).await;
    assert_eq!(resp.status, 200);
    let body = resp.json();
    assert_eq!(body["success"], true, "RPC-052 envelope");
    let payload = &body["payload"];
    assert_eq!(payload["reloaded"], true);
    assert_eq!(
        payload["changed"],
        serde_json::json!(["observability.slow_reducer_threshold_us"])
    );
    assert_eq!(
        payload["reloadable"]["observability.slow_reducer_threshold_us"]["value"],
        42
    );
    assert_eq!(ctx.metrics().slow_reducer_threshold_us(), 42);
}

#[tokio::test]
async fn the_admin_endpoint_refuses_a_frozen_change_with_400() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), "sharding:\n  shards: 2\n");
    let ctx = build_ctx();
    ctx.install_config(Some(path.clone()), load(&path), None);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    write_config(dir.path(), "sharding:\n  shards: 4\n");
    let resp = request(server.local_addr, "POST", "/config/reload", Some("{}")).await;
    assert_eq!(resp.status, 400, "a rejected reload is the caller's fault");
    assert!(
        resp.text().contains("sharding.shards"),
        "the HTTP error names the frozen key too: {}",
        resp.text()
    );
}

#[tokio::test]
async fn a_shard_with_no_installed_config_refuses_to_guess() {
    let ctx = build_ctx();
    let err = ctx.reload_config().expect_err("nothing to reload from");
    assert!(err.contains("no configuration installed"), "{err}");
    assert!(
        ctx.reloadable_config().is_none(),
        "/health omits the section rather than inventing defaults"
    );
}

// --- Minimal HTTP/1.1 request helper ----------------------------------------------

struct Resp {
    status: u16,
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
    for line in lines {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = v.trim().parse().unwrap_or(0);
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
    Resp { status, body }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
