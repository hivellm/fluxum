//! Admin-surface error/edge arms (phase8 coverage floor): the refusal and
//! conversion paths the happy-path suites never walk — audit pk coercion
//! across every supported key type, checkpoint/backup/sessions/bans
//! refusals, reducer argument policing, and the `/schema` full-text index
//! rendering. All through [`admin::dispatch`] with loopback requests, so
//! each assertion pins the served JSON, not an internal.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::ServerPeer;
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, FullTextLanguage, IndexSchema, Schema, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::admin::{self, AdminRequest};
use serde_json::{Value, json};

const SHARD: u32 = 7;
const PEER_TOKEN: &str = "arms-peer-token";

// One composite primary key covering every supported audit pk type in a
// single conversion pass (Bool..U64, Str), plus a Timestamp-keyed table for
// the deliberate "unsupported" arm.
static PK10_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "b",
        ty: FluxType::Bool,
    },
    ColumnSchema {
        name: "i1",
        ty: FluxType::I8,
    },
    ColumnSchema {
        name: "i2",
        ty: FluxType::I16,
    },
    ColumnSchema {
        name: "i4",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "i8c",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "u1",
        ty: FluxType::U8,
    },
    ColumnSchema {
        name: "u2",
        ty: FluxType::U16,
    },
    ColumnSchema {
        name: "u4",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "u8c",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "s",
        ty: FluxType::Str,
    },
];
static PK10: TableSchema = TableSchema {
    name: "Pk10",
    columns: PK10_COLS,
    primary_key: &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static PKTIME_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "at",
    ty: FluxType::Timestamp,
}];
static PKTIME: TableSchema = TableSchema {
    name: "PkTime",
    columns: PKTIME_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

// A full-text-indexed table, so `/schema` renders the FTS index block.
static DOC_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "body",
        ty: FluxType::Str,
    },
];
static DOC: TableSchema = TableSchema {
    name: "Doc",
    columns: DOC_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::FullText {
        column: 1,
        language: FullTextLanguage::English,
        stop_words: true,
        stemming: true,
    }],
    visibility: VisibilityRule::PublicAll,
};

fn noop(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    Ok(())
}
fn any_args(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}
/// Registered but not client-callable — the F-004 refusal arm.
static SCHED_ONLY: ReducerDef = ReducerDef {
    name: "sched_only",
    handler: noop,
    check_args: any_args,
    client_callable: false,
    max_rate_per_sec: 0,
};

fn make_ctx() -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&PK10, &PKTIME, &DOC]).unwrap();
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
        Arc::new(ReducerRegistry::from_defs([&SCHED_ONLY]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("arms-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let peers = ServerPeerRegistry::from_config(&[ServerPeer {
        name: "ops".into(),
        token: PEER_TOKEN.into(),
    }])
    .unwrap();
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), peers);
    ShardContext::new(engine, subs, auth, SHARD, 64)
}

async fn call(ctx: &Arc<ShardContext>, method: &str, path: &str, body: Value) -> (u16, Value) {
    let bytes = serde_json::to_vec(&body).unwrap();
    let resp = admin::dispatch(ctx, AdminRequest::local(method, path, &bytes)).await;
    (resp.status, resp.body)
}

fn error_of(body: &Value) -> String {
    body["error"].as_str().unwrap_or_default().to_owned()
}

// --- POST /audit: pk coercion across every supported key type ---------------------

#[tokio::test(flavor = "multi_thread")]
async fn audit_pk_coercion_covers_every_supported_type() {
    let ctx = make_ctx();
    let good = json!([
        true,
        -1,
        -300,
        70000,
        -9,
        200,
        60000,
        4000000,
        18_446_744_073_709_551_615_u64,
        "s"
    ]);
    let (status, body) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": PEER_TOKEN, "table": "Pk10", "pk": good, "limit": 5 }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["payload"]["count"], 0, "empty log, clean coercion");

    // Each type's refusal names the column (declaration order).
    let bads: &[(usize, Value, &str)] = &[
        (0, json!(1), "b"),         // Bool from number
        (1, json!(200), "i1"),      // i8 overflow
        (2, json!(70000), "i2"),    // i16 overflow
        (3, json!(i64::MAX), "i4"), // i32 overflow
        (4, json!("x"), "i8c"),     // i64 from string
        (5, json!(300), "u1"),      // u8 overflow
        (6, json!(70000), "u2"),    // u16 overflow
        (7, json!(-1), "u4"),       // u32 from negative
        (8, json!(-1), "u8c"),      // u64 from negative
        (9, json!(5), "s"),         // Str from number
    ];
    for (idx, bad, column) in bads {
        let mut values = good.as_array().unwrap().clone();
        values[*idx] = bad.clone();
        let (status, body) = call(
            &ctx,
            "POST",
            "/audit",
            json!({ "token": PEER_TOKEN, "table": "Pk10", "pk": values }),
        )
        .await;
        assert_eq!(status, 400, "position {idx}: {body}");
        assert!(
            error_of(&body).contains(&format!("column `{column}`")),
            "names the column: {body}"
        );
    }

    // Arity mismatch and the deliberately unsupported key type.
    let (status, body) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": PEER_TOKEN, "table": "Pk10", "pk": [true] }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("10-column primary key"), "{body}");

    let (status, body) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": PEER_TOKEN, "table": "PkTime", "pk": [1] }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(
        error_of(&body).contains("does not support"),
        "Timestamp keys refuse row matching: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn audit_refusal_arms() {
    let ctx = make_ctx();
    // No token → 401; a plain (non-peer) identity → 403.
    let (status, _) = call(&ctx, "POST", "/audit", json!({ "table": "Pk10" })).await;
    assert_eq!(status, 401);
    let (status, body) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": "plain-client", "table": "Pk10" }),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    // Unknown table → 404; pk of the wrong JSON shape → 400.
    let (status, _) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": PEER_TOKEN, "table": "Nope" }),
    )
    .await;
    assert_eq!(status, 404);
    let (status, body) = call(
        &ctx,
        "POST",
        "/audit",
        json!({ "token": PEER_TOKEN, "table": "Pk10", "pk": "not-an-array" }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("array"), "{body}");
}

// --- ops refusals: checkpoint / backup / config reload ----------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ops_routes_refuse_cleanly_when_unassembled() {
    let ctx = make_ctx();
    // No checkpoint worker in this assembly.
    let (status, body) = call(&ctx, "POST", "/checkpoint", json!({})).await;
    assert_eq!(status, 404, "{body}");
    assert!(error_of(&body).contains("no checkpoint worker"), "{body}");
    // No installed config → the backup source is unknown.
    let (status, body) = call(&ctx, "POST", "/backup", json!({ "out": "x" })).await;
    assert_eq!(status, 500, "{body}");
    assert!(
        error_of(&body).contains("no configuration installed"),
        "{body}"
    );
    // Malformed backup requests.
    let (status, body) = call(&ctx, "POST", "/backup", json!({})).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("payload.out"), "{body}");
    let (status, body) = call(&ctx, "POST", "/backup/verify", json!({})).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("payload.dir"), "{body}");
    // Verifying a directory that is not a backup reports the failure
    // per-file (REP-064's report shape), not as a transport error.
    let (status, body) = call(
        &ctx,
        "POST",
        "/backup/verify",
        json!({ "dir": "Z:/definitely/not/a/backup" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["payload"]["ok"], false, "{body}");
    assert!(
        !body["payload"]["errors"].as_array().unwrap().is_empty(),
        "{body}"
    );
    // A reload without an installed config names the refusal.
    let (status, body) = call(&ctx, "POST", "/config/reload", json!({})).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("cannot reload"), "{body}");
}

// --- sessions without an HTTP directory -------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn session_routes_without_a_directory() {
    let ctx = make_ctx();
    let (status, body) = call(&ctx, "GET", "/sessions", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(body["payload"]["sessions"], json!([]));
    let (status, body) = call(&ctx, "DELETE", "/sessions/abc", json!({})).await;
    assert_eq!(status, 404);
    assert!(
        error_of(&body).contains("no HTTP session directory"),
        "{body}"
    );
    let (status, _) = call(&ctx, "DELETE", "/sessions?identity=00", json!({})).await;
    assert_eq!(status, 404);
}

// --- bans refusal arms -------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn ban_refusal_arms() {
    let ctx = make_ctx();
    let resp = admin::dispatch(&ctx, AdminRequest::local("POST", "/bans", b"not json")).await;
    assert_eq!(resp.status, 400);
    assert!(
        error_of(&resp.body).contains("bad ban request"),
        "{}",
        resp.body
    );
    let (status, body) = call(&ctx, "POST", "/bans", json!({ "ttl_secs": 5 })).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("needs an `entry`"), "{body}");
    let (status, _) = call(&ctx, "POST", "/bans", json!({ "entry": "not an ip" })).await;
    assert_eq!(status, 400);
    let (status, body) = call(&ctx, "DELETE", "/bans/198.51.100.9", json!({})).await;
    assert_eq!(status, 404);
    assert!(error_of(&body).contains("no runtime ban"), "{body}");
}

// --- reducer argument policing ------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_call_polices_arguments_and_callability() {
    let ctx = make_ctx();
    // A payload that is not an argument array (an unregistered name passes
    // the callability check — absent means callable — so the shape arm runs).
    let (status, body) = call(&ctx, "POST", "/reducer/ghost", json!({"payload": "x"})).await;
    assert_eq!(status, 400, "{body}");
    // An argument outside the FluxValue universe (an object).
    let (status, body) = call(&ctx, "POST", "/reducer/x", json!([{ "nested": 1 }])).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("FluxValue universe"), "{body}");
    // Schedule-only reducers are not invocable over HTTP (F-004).
    let (status, body) = call(&ctx, "POST", "/reducer/sched_only", json!([])).await;
    assert_eq!(status, 403);
    assert!(error_of(&body).contains("not client-callable"), "{body}");
    // Draining refuses new reducer work with a retryable signal (OPS-030).
    ctx.begin_drain();
    let (status, body) = call(&ctx, "POST", "/reducer/sched_only", json!([])).await;
    assert_eq!(status, 503);
    assert!(error_of(&body).contains("draining"), "{body}");
}

// --- /schema renders the full-text index block (FTS-050) ---------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schema_renders_fulltext_indexes() {
    let ctx = make_ctx();
    let (status, body) = call(&ctx, "GET", "/schema", json!({})).await;
    assert_eq!(status, 200);
    let tables = body["payload"]["tables"].as_array().unwrap();
    let doc = tables.iter().find(|t| t["name"] == "Doc").unwrap();
    let index = &doc["indexes"][0];
    assert_eq!(index["kind"], "fulltext", "{doc}");
    assert_eq!(index["columns"], json!(["body"]), "{doc}");
    assert_eq!(index["language"], "english", "{doc}");
    assert_eq!(index["stemming"], true, "{doc}");
    let fulltext = &doc["fulltext"][0];
    assert_eq!(fulltext["column"], "body", "{doc}");
    assert!(fulltext["bm25"]["k1"].is_number(), "{doc}");
}

// --- POST /rows request-shape refusals ----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn row_edit_refuses_malformed_requests() {
    let ctx = make_ctx();
    let resp = admin::dispatch(&ctx, AdminRequest::local("POST", "/rows", b"not json")).await;
    assert_eq!(resp.status, 400, "{}", resp.body);
    let (status, body) = call(&ctx, "POST", "/rows", json!({ "op": "upsert" })).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("payload.table"), "{body}");
    let (status, body) = call(&ctx, "POST", "/rows", json!({ "table": "Doc" })).await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("payload.row"), "{body}");
    let (status, body) = call(
        &ctx,
        "POST",
        "/rows",
        json!({ "table": "Ghost", "row": {} }),
    )
    .await;
    assert_eq!(status, 404);
    assert!(error_of(&body).contains("unknown table"), "{body}");
    let (status, body) = call(
        &ctx,
        "POST",
        "/rows",
        json!({ "table": "Doc", "op": "replace", "row": { "id": 1, "body": "x" } }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(error_of(&body).contains("`upsert` or `delete`"), "{body}");
}

// --- /health mirrors the lifecycle state (OBS-060) ----------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn health_status_follows_the_shard_lifecycle() {
    use fluxum_core::metrics::ShardState;
    let ctx = make_ctx();
    ctx.metrics().set_shard_state(ShardState::Ready);
    let (status, body) = call(&ctx, "GET", "/health", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok", "{body}");
    // Recovering serves 503/degraded — a load balancer pulls the shard but
    // an operator still reads the document.
    ctx.metrics().set_shard_state(ShardState::Recovering);
    let (status, body) = call(&ctx, "GET", "/health", json!({})).await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "degraded", "{body}");
    ctx.metrics().set_shard_state(ShardState::ShuttingDown);
    let (status, body) = call(&ctx, "GET", "/health", json!({})).await;
    assert_eq!(status, 503);
    assert_eq!(body["status"], "error", "{body}");
}
