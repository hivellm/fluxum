//! T5.3 HTTP/JSON admin API suite (SPEC-006 §7, RPC-050..053; FR-44/FR-91;
//! DAG exit: curl tests for all endpoints): `/health` (lock-free, < 50 ms),
//! `/metrics` (Prometheus text), `/schema` (tables + reducers + views),
//! `POST /reducer/:name`, `POST /query`, `GET /view/:name` — each with the
//! RPC-051/052 JSON envelopes on the same :15800 port as the binary `/rpc`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
    ViewContext, ViewDef, ViewRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, HttpOptions};
use serde_json::Value;

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
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn send_chat(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("send_chat(text)".into())),
    };
    if text.is_empty() {
        return Err(fluxum_core::FluxumError::Reducer("empty text".into()));
    }
    ctx.tx.insert(ChatRow { id: 0, text })?;
    Ok(())
}
fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("send_chat", args, 1)
}
static SEND_CHAT: ReducerDef = ReducerDef {
    name: "send_chat",
    handler: send_chat,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

/// A `#[fluxum::view]` returning the Chat row count.
fn chat_count(ctx: &ViewContext<'_>, _args: &[FluxValue]) -> Result<Value> {
    let count = ctx.tx.scan::<ChatRow>()?.len();
    Ok(serde_json::json!({ "count": count }))
}
static CHAT_COUNT: ViewDef = ViewDef {
    name: "chat_count",
    handler: chat_count,
};

async fn start() -> http::HttpServer {
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
        Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("admin-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let views = ViewRegistry::from_defs([&CHAT_COUNT]).unwrap();
    let ctx = ShardContext::with_views(engine, subs, auth, views, SHARD, 256);
    http::serve(ctx, "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap()
}

// --- A curl-like HTTP client ---------------------------------------------------

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

// --- RPC-053: /health lock-free, < 50 ms ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn health_reports_status_and_is_fast() {
    let server = start().await;
    let start_t = Instant::now();
    let resp = request(server.local_addr, "GET", "/health", None).await;
    let elapsed = start_t.elapsed();
    assert_eq!(resp.status, 200);
    let json = resp.json();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["shards"], 1);
    assert_eq!(json["shard"]["id"], SHARD);
    assert!(
        elapsed < Duration::from_millis(50),
        "health took {elapsed:?}"
    );
    server.shutdown();
}

// --- /metrics: Prometheus text -------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn metrics_serves_prometheus_text() {
    let server = start().await;
    let resp = request(server.local_addr, "GET", "/metrics", None).await;
    assert_eq!(resp.status, 200);
    assert!(
        resp.content_type.starts_with("text/plain"),
        "{}",
        resp.content_type
    );
    let text = resp.text();
    assert!(text.contains("fluxum_up"), "{text}");
    assert!(
        text.contains("# TYPE fluxum_shard_last_tx_id gauge"),
        "{text}"
    );
    server.shutdown();
}

// --- /schema: tables + reducers + views ----------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schema_lists_tables_reducers_and_views() {
    let server = start().await;
    let resp = request(server.local_addr, "GET", "/schema", None).await;
    assert_eq!(resp.status, 200);
    let json = resp.json();
    assert_eq!(json["success"], true);
    let payload = &json["payload"];
    assert_eq!(payload["tables"][0]["name"], "Chat");
    assert_eq!(payload["tables"][0]["columns"][0]["name"], "id");
    assert_eq!(payload["reducers"], serde_json::json!(["send_chat"]));
    assert_eq!(payload["views"], serde_json::json!(["chat_count"]));
    server.shutdown();
}

// --- POST /reducer/:name -------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_endpoint_calls_and_commits() {
    let server = start().await;
    let addr = server.local_addr;

    // RPC-051 envelope: payload is the argument array.
    let resp = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"request_id":"abc","payload":["hello"]}"#),
    )
    .await;
    assert_eq!(resp.status, 200);
    let json = resp.json();
    assert_eq!(json["success"], true);
    assert_eq!(json["request_id"], "abc");
    assert_eq!(json["payload"]["committed"], true);

    // A business error (RED-060) is a well-formed failure envelope.
    let resp = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":[""]}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
    let json = resp.json();
    assert_eq!(json["success"], false);
    assert_eq!(json["error"], "empty text");

    // An unknown reducer is a 404.
    let resp = request(addr, "POST", "/reducer/nope", Some(r#"{"payload":[]}"#)).await;
    assert_eq!(resp.status, 404);
    server.shutdown();
}

// --- POST /query: one-off SQL → JSON rows --------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_returns_json_rows() {
    let server = start().await;
    let addr = server.local_addr;
    // Seed two rows via the reducer endpoint.
    for text in ["a", "b"] {
        request(
            addr,
            "POST",
            "/reducer/send_chat",
            Some(&format!(r#"{{"payload":["{text}"]}}"#)),
        )
        .await;
    }

    let resp = request(
        addr,
        "POST",
        "/query",
        Some(r#"{"payload":{"sql":"SELECT * FROM Chat"}}"#),
    )
    .await;
    assert_eq!(resp.status, 200);
    let json = resp.json();
    assert_eq!(json["success"], true);
    let payload = &json["payload"];
    assert_eq!(payload["table"], "Chat");
    assert_eq!(payload["rows"].as_array().unwrap().len(), 2);
    // The row is a JSON object keyed by column name.
    let texts: Vec<&str> = payload["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["text"].as_str().unwrap())
        .collect();
    assert!(texts.contains(&"a") && texts.contains(&"b"));

    // An unknown table → 400.
    let resp = request(
        addr,
        "POST",
        "/query",
        Some(r#"{"payload":{"sql":"SELECT * FROM Ghost"}}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
    server.shutdown();
}

// --- GET /view/:name -----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn view_endpoint_dispatches_and_returns_json() {
    let server = start().await;
    let addr = server.local_addr;
    request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":["one"]}"#),
    )
    .await;

    let resp = request(addr, "GET", "/view/chat_count", None).await;
    assert_eq!(resp.status, 200);
    let json = resp.json();
    assert_eq!(json["success"], true);
    assert_eq!(json["payload"]["count"], 1);

    let resp = request(addr, "GET", "/view/nope", None).await;
    assert_eq!(resp.status, 404);
    server.shutdown();
}
