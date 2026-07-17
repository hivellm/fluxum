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

// --- POST /query/explain (SPEC-018 QP-051) ---------------------------------------

#[tokio::test]
async fn query_explain_reports_the_access_path() {
    let server = start().await;
    let resp = request(
        server.local_addr,
        "POST",
        "/query/explain",
        Some(r#"{"sql": "SELECT * FROM Chat WHERE text = 'hi' LIMIT 3"}"#),
    )
    .await;
    assert_eq!(resp.status, 200);
    let body = resp.json();
    assert_eq!(body["success"], true);
    let report = &body["payload"];
    assert_eq!(report["table"], "Chat");
    assert_eq!(report["access"]["kind"], "full_scan", "Chat has no index");
    assert_eq!(report["limit"], 3);

    // Compile failures surface as 400s, not executions.
    let resp = request(
        server.local_addr,
        "POST",
        "/query/explain",
        Some(r#"{"sql": "SELECT * FROM Nope"}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
}

// --- GET /plugins + hot disable (SPEC-020 PLG-060/061) --------------------------

#[tokio::test]
async fn plugins_endpoint_reports_and_hot_disables() {
    use fluxum_core::config::{Config, PluginDecl, PluginHost, PluginScope};
    use fluxum_core::plugin::PluginRegistry;

    // Assemble a shard with a validated registry installed: one sidecar
    // retriever scoped to Chat (the assembly-time PLG-032 flow).
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
    let subs = SubscriptionManager::new(Arc::new(schema.clone()), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);

    let config = Config {
        plugins: vec![PluginDecl {
            name: "vec_hybrid".into(),
            capability: "retriever".into(),
            host: PluginHost::Sidecar {
                endpoint: "127.0.0.1:15811".into(),
                timeout_ms: 60,
            },
            applies_to: PluginScope {
                tables: vec!["Chat".into()],
                columns: vec![],
            },
        }],
        ..Config::default()
    };
    let registry = Arc::new(PluginRegistry::build(&schema, &config).unwrap());
    ctx.set_plugins(registry);
    let server = http::serve(ctx, "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    // PLG-060: name, capability, host, placement, health, scope — listed.
    let resp = request(server.local_addr, "GET", "/plugins", None).await;
    assert_eq!(resp.status, 200);
    let body = resp.json();
    let plugins = body["payload"]["plugins"].as_array().unwrap();
    let hybrid = plugins
        .iter()
        .find(|p| p["name"] == "vec_hybrid")
        .expect("manifest plugin listed");
    assert_eq!(hybrid["capability"], "retriever");
    assert_eq!(hybrid["placement"], "read_path");
    assert_eq!(hybrid["health"], "active");
    assert!(hybrid["host"].as_str().unwrap().contains("127.0.0.1:15811"));
    assert_eq!(hybrid["tables"][0], "Chat");
    // The adopted auth seam appears too (PLG-002).
    assert!(
        plugins.iter().any(|p| p["capability"] == "auth" && p["host"] == "builtin"),
        "{plugins:?}"
    );

    // PLG-061: hot disable without a core restart, then re-enable.
    let resp = request(server.local_addr, "POST", "/plugins/vec_hybrid/disable", None).await;
    assert_eq!(resp.status, 200);
    let resp = request(server.local_addr, "GET", "/plugins", None).await;
    let body = resp.json();
    let health = body["payload"]["plugins"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "vec_hybrid")
        .unwrap()["health"]
        .clone();
    assert_eq!(health, "disabled");
    let resp = request(server.local_addr, "POST", "/plugins/vec_hybrid/enable", None).await;
    assert_eq!(resp.status, 200);
    let resp = request(server.local_addr, "POST", "/plugins/ghost/disable", None).await;
    assert_eq!(resp.status, 404, "unknown plugin");

    // PLG-030 meters ride /metrics.
    let resp = request(server.local_addr, "GET", "/metrics", None).await;
    let text = resp.text();
    assert!(
        text.contains("fluxum_plugin_panics_total{plugin=\"vec_hybrid\"} 0"),
        "{text}"
    );
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

// --- Admin routing edges (RPC-050) ----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_route_with_wrong_method_is_404_and_unknown_paths_map_to_http_codes() {
    let server = start().await;
    let addr = server.local_addr;

    // An admin path with the wrong method falls through dispatch → 404.
    let resp = request(addr, "DELETE", "/health", None).await;
    assert_eq!(resp.status, 404);
    let json = resp.json();
    assert_eq!(json["success"], false);
    assert_eq!(json["error"], "not found");

    // A GET/POST outside every route is a plain 404.
    let resp = request(addr, "GET", "/nope", None).await;
    assert_eq!(resp.status, 404);

    // A non-GET/POST method outside the admin surface is 405.
    let resp = request(addr, "DELETE", "/nope", None).await;
    assert_eq!(resp.status, 405);
    server.shutdown();
}

// --- POST /reducer error paths (RPC-051) -----------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_endpoint_rejects_malformed_bodies() {
    let server = start().await;
    let addr = server.local_addr;

    // Invalid JSON → 400.
    let resp = request(addr, "POST", "/reducer/send_chat", Some("{nope")).await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("invalid JSON")
    );

    // Empty body → empty argument list → arity failure (a 400 envelope).
    let resp = request(addr, "POST", "/reducer/send_chat", None).await;
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["success"], false);

    // An argument outside the FluxValue universe (a JSON object).
    let resp = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":[{"k":1}]}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("FluxValue universe")
    );

    // A non-array payload.
    let resp = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":"text"}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("argument array")
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn reducer_endpoint_accepts_bare_json_and_every_flux_json_shape() {
    let server = start().await;
    let addr = server.local_addr;

    // A bare (non-enveloped) JSON array body is taken as the payload.
    let resp = request(addr, "POST", "/reducer/send_chat", Some(r#"["bare"]"#)).await;
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["payload"]["committed"], true);

    // Every JSON shape inside the FluxValue universe converts: null, bool,
    // integer, float, string, nested array. send_chat then rejects the
    // arity, proving conversion ran before dispatch.
    let resp = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":[null, true, 3, 2.5, "s", [1, "x"]]}"#),
    )
    .await;
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["success"], false);
    server.shutdown();
}

// --- POST /query error paths ------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn query_endpoint_rejects_bad_bodies() {
    let server = start().await;
    let addr = server.local_addr;

    // Invalid JSON → 400.
    let resp = request(addr, "POST", "/query", Some("{nope")).await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("invalid JSON")
    );

    // Missing payload.sql → 400.
    let resp = request(addr, "POST", "/query", Some(r#"{"payload":{}}"#)).await;
    assert_eq!(resp.status, 400);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("payload.sql")
    );
    server.shutdown();
}

// --- Transformed schema + error-status harness (CT-050/052; RPC-052) ------------

static VAULT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "amount",
        ty: FluxType::Decimal,
    },
    ColumnSchema {
        name: "at",
        ty: FluxType::Timestamp,
    },
    ColumnSchema {
        name: "memo",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "label",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "card",
        ty: FluxType::Bytes,
    },
    ColumnSchema {
        name: "total",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "subtotal",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "pub_note",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "own_note",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "srv_note",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "hint",
        ty: FluxType::Str,
    },
];
static VAULT: TableSchema = TableSchema {
    name: "Vault",
    columns: VAULT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

use fluxum_core::transform::{
    CaseFold, ColumnTransformDef, CryptoScheme, GrantScope, MaskStrategy, SignScheme, SignedBy,
    StringForm, TransformDescriptor,
};

fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "amount",
        transforms: &[TransformDescriptor::NormalizeMoney { scale: 2, currency: Some("USD") }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "at",
        transforms: &[TransformDescriptor::NormalizeDatetime],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "memo",
        transforms: &[TransformDescriptor::NormalizeString {
            form: StringForm::Nfc,
            case: CaseFold::None,
            trim: false,
        }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "title",
        transforms: &[TransformDescriptor::NormalizeString {
            form: StringForm::Nfkc,
            case: CaseFold::Fold,
            trim: true,
        }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "label",
        transforms: &[TransformDescriptor::NormalizeString {
            form: StringForm::Nfc,
            case: CaseFold::Lower,
            trim: false,
        }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "card",
        transforms: &[
            TransformDescriptor::Encrypted { scheme: CryptoScheme::Ecies, key: "vault_key" },
            TransformDescriptor::Masked { strategy: MaskStrategy::Ciphertext },
            TransformDescriptor::Grant { select: GrantScope::Role("auditor") },
        ],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "total",
        transforms: &[TransformDescriptor::Signed {
            scheme: SignScheme::Ed25519,
            by: SignedBy::Server,
        }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "subtotal",
        transforms: &[TransformDescriptor::Signed {
            scheme: SignScheme::Ed25519,
            by: SignedBy::IdentityColumn(1),
        }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "pub_note",
        transforms: &[TransformDescriptor::Grant { select: GrantScope::Public }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "own_note",
        transforms: &[TransformDescriptor::Grant { select: GrantScope::Owner }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "srv_note",
        transforms: &[TransformDescriptor::Grant { select: GrantScope::ServerPeer }],
    }
}
fluxum_core::schema::inventory::submit! {
    ColumnTransformDef {
        table: "Vault",
        column: "hint",
        transforms: &[TransformDescriptor::Masked { strategy: MaskStrategy::Hash }],
    }
}

/// A view whose handler fails with a non-`Query` error → HTTP 500.
fn boom_view(_ctx: &ViewContext<'_>, _args: &[FluxValue]) -> Result<Value> {
    Err(fluxum_core::FluxumError::Storage("view exploded".into()))
}
static BOOM_VIEW: ViewDef = ViewDef {
    name: "boom",
    handler: boom_view,
};

/// A schedule-only reducer: a client (or admin) call answers 403 (RED-025).
static SCHED_ONLY: ReducerDef = ReducerDef {
    name: "sched_only",
    handler: |_, _| Ok(()),
    check_args: |_| Ok(()),
    client_callable: false,
    max_rate_per_sec: 0,
};

/// A rate-limited reducer: the second call within a second answers 429.
static LIMITED: ReducerDef = ReducerDef {
    name: "limited_chat",
    handler: send_chat,
    check_args,
    client_callable: true,
    max_rate_per_sec: 1,
};

async fn start_hardened(shard_max_reducers_per_sec: u64) -> http::HttpServer {
    use fluxum_core::reducer::{RateLimiter, RateLimiterOptions};

    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT, &VAULT]).unwrap();
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
        Arc::new(ReducerRegistry::from_defs([&SEND_CHAT, &SCHED_ONLY, &LIMITED]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("admin-test"),
    )
    .with_rate_limiter(RateLimiter::new(
        RateLimiterOptions {
            shard_max_reducers_per_sec,
        },
        [],
    ));
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let views = ViewRegistry::from_defs([&CHAT_COUNT, &BOOM_VIEW]).unwrap();
    let ctx = ShardContext::with_views(engine, subs, auth, views, SHARD, 256);
    http::serve(ctx, "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap()
}

// --- CT-050/052: /schema surfaces every transform descriptor kind ---------------

#[tokio::test(flavor = "multi_thread")]
async fn schema_surfaces_column_transform_descriptors() {
    let server = start_hardened(0).await;
    let resp = request(server.local_addr, "GET", "/schema", None).await;
    assert_eq!(resp.status, 200);
    let json = resp.json();
    let tables = json["payload"]["tables"].as_array().unwrap();
    let vault = tables.iter().find(|t| t["name"] == "Vault").unwrap();
    let column = |name: &str| -> &Value {
        vault["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap()
    };

    // Untransformed columns carry no `transforms` key.
    assert!(column("id").get("transforms").is_none());

    assert_eq!(
        column("amount")["transforms"][0],
        serde_json::json!({ "kind": "normalize.money", "scale": 2, "currency": "USD" })
    );
    assert_eq!(
        column("at")["transforms"][0],
        serde_json::json!({ "kind": "normalize.datetime" })
    );
    // The three case folds and both Unicode forms.
    assert_eq!(column("memo")["transforms"][0]["form"], "nfc");
    assert_eq!(column("memo")["transforms"][0]["case"], "none");
    assert_eq!(column("memo")["transforms"][0]["trim"], false);
    assert_eq!(column("title")["transforms"][0]["form"], "nfkc");
    assert_eq!(column("title")["transforms"][0]["case"], "fold");
    assert_eq!(column("title")["transforms"][0]["trim"], true);
    assert_eq!(column("label")["transforms"][0]["case"], "lower");

    // The encrypted → masked → grant pipeline, key NAME only (CT-052).
    let card = column("card")["transforms"].as_array().unwrap();
    assert_eq!(
        card[0],
        serde_json::json!({ "kind": "encrypted", "scheme": "ecies", "key": "vault_key" })
    );
    assert_eq!(
        card[1],
        serde_json::json!({ "kind": "masked", "strategy": "ciphertext" })
    );
    assert_eq!(
        card[2],
        serde_json::json!({ "kind": "column_grant", "select": { "role": "auditor" } })
    );

    // Both signing authorities.
    assert_eq!(column("total")["transforms"][0]["by"], "server");
    assert_eq!(
        column("subtotal")["transforms"][0]["by"],
        serde_json::json!({ "column": 1 })
    );

    // The remaining grant scopes and the hash mask.
    assert_eq!(column("pub_note")["transforms"][0]["select"], "public");
    assert_eq!(column("own_note")["transforms"][0]["select"], "owner");
    assert_eq!(column("srv_note")["transforms"][0]["select"], "server_peer");
    assert_eq!(column("hint")["transforms"][0]["strategy"], "hash");
    server.shutdown();
}

// --- RPC-052: error statuses map onto HTTP codes ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn admin_errors_carry_403_429_500_statuses() {
    let server = start_hardened(0).await;
    let addr = server.local_addr;

    // A schedule-only reducer answers 403 (RED-025).
    let resp = request(
        addr,
        "POST",
        "/reducer/sched_only",
        Some(r#"{"payload":[]}"#),
    )
    .await;
    assert_eq!(resp.status, 403);
    assert_eq!(resp.json()["success"], false);

    // A rate-limited reducer answers 429 on the burst excess (RED-050).
    let first = request(
        addr,
        "POST",
        "/reducer/limited_chat",
        Some(r#"{"payload":["a"]}"#),
    )
    .await;
    assert_eq!(first.status, 200);
    let second = request(
        addr,
        "POST",
        "/reducer/limited_chat",
        Some(r#"{"payload":["b"]}"#),
    )
    .await;
    assert_eq!(second.status, 429);

    // A view failing with a non-Query error is a 500.
    let resp = request(addr, "GET", "/view/boom", None).await;
    assert_eq!(resp.status, 500);
    assert!(
        resp.json()["error"]
            .as_str()
            .unwrap()
            .contains("view exploded")
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn shard_cap_answers_503_on_the_excess_call() {
    let server = start_hardened(1).await;
    let addr = server.local_addr;
    let first = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":["a"]}"#),
    )
    .await;
    assert_eq!(first.status, 200);
    let second = request(
        addr,
        "POST",
        "/reducer/send_chat",
        Some(r#"{"payload":["b"]}"#),
    )
    .await;
    assert_eq!(second.status, 503, "RED-052 shard overload");
    server.shutdown();
}
