//! T6.1 — the **module API freeze** gate (SPEC-011, FR-81).
//!
//! A fixture module declared with the real `#[fluxum::*]` macros is rendered
//! through `GET /schema`, canonicalized exactly as `fluxum schema export`
//! does, and compared **byte for byte** against the committed
//! `tests/golden/schema.json`. Any change to the schema document — a renamed
//! key, a dropped field, a reordered list — fails here.
//!
//! That is the point: after T6.1 the `#[fluxum::*]` surface and this document
//! may only change *additively*. A diff is not automatically wrong, but it is
//! automatically a decision: update the golden deliberately, and bump
//! `SCHEMA_DOCUMENT_VERSION` if the change is not additive.
//!
//! The fixture stands in for the demo-app module (DAG T6.5), which does not
//! exist yet; it is deliberately broad — every index kind, a visibility rule,
//! auto-inc, unique constraints, and reducers with real signatures — so the
//! gate covers the whole document rather than a corner of it.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerContext, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{Identity, Timestamp};
use fluxum_macros as fluxum;
use fluxum_server::ShardContext;
use fluxum_server::admin;

const SHARD: u32 = 21;
const GOLDEN: &str = include_str!("golden/schema.json");

// --- The fixture module ----------------------------------------------------------

/// A chat message: auto-inc pk, a full-text body, a btree index.
#[fluxum::table(public)]
#[index(btree(channel, sent_at))]
#[fulltext(body)]
pub struct ChatMessage {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub channel: u32,
    pub body: String,
    pub sent_at: Timestamp,
}

/// Per-user rows: owner-only visibility, a unique constraint.
#[fluxum::table(public)]
#[visibility(owner_only(owner))]
#[unique(slug)]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub slug: String,
    pub done: bool,
}

/// Composite pk + a spatial index.
#[fluxum::table(public, primary_key(grid_x, grid_y))]
#[spatial(quadtree(x, y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    pub x: f32,
    pub y: f32,
    pub reading: f64,
}

#[fluxum::reducer]
fn send_chat(ctx: &ReducerContext, channel: u32, body: String) -> Result<(), String> {
    ctx.tx
        .insert(ChatMessage {
            id: 0,
            channel,
            body,
            sent_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::reducer(max_rate = "5/s")]
fn complete_task(ctx: &ReducerContext, task_id: u64) -> Result<(), String> {
    let _ = (ctx, task_id);
    Ok(())
}

// --- Harness ----------------------------------------------------------------------

async fn ctx() -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::assemble().unwrap();
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
        Arc::new(ReducerRegistry::from_registered().unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("golden"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 64)
}

/// The schema document exactly as `fluxum schema export` writes it.
async fn exported_schema(ctx: &Arc<ShardContext>) -> String {
    let resp = admin::dispatch(ctx, admin::AdminRequest::local("GET", "/schema", &[])).await;
    assert_eq!(resp.status, 200);
    let body = serde_json::to_string(&resp.body).unwrap();
    fluxum_cli::canonical_schema(&body).unwrap()
}

// --- The freeze gate --------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_document_matches_its_golden_byte_for_byte() {
    let ctx = ctx().await;
    let actual = exported_schema(&ctx).await;

    if actual != GOLDEN {
        // Point at the first differing line: a wall of JSON is useless when
        // the whole question is "what moved?".
        let (mut line, mut expected_line, mut actual_line) = (0, "", "");
        for (i, (a, b)) in GOLDEN.lines().zip(actual.lines()).enumerate() {
            if a != b {
                line = i + 1;
                expected_line = a;
                actual_line = b;
                break;
            }
        }
        panic!(
            "the /schema document changed — this is the T6.1 MODULE API FREEZE gate.\n\
             First difference at line {line}:\n  golden: {expected_line}\n  actual: {actual_line}\n\n\
             If the change is intended AND additive, update \
             crates/fluxum-server/tests/golden/schema.json.\n\
             If it removes or repurposes a key it is breaking: bump \
             admin::SCHEMA_DOCUMENT_VERSION too.\n\n\
             --- actual ---\n{actual}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_export_is_canonical_and_stable() {
    let ctx = ctx().await;
    // Two renderings of the same schema are the same bytes — the property
    // that makes a golden file a usable gate at all (sorted keys, sorted
    // reducer list, no map-iteration order leaking in).
    assert_eq!(exported_schema(&ctx).await, exported_schema(&ctx).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_document_carries_its_version_and_every_declared_surface() {
    let ctx = ctx().await;
    let resp = admin::dispatch(&ctx, admin::AdminRequest::local("GET", "/schema", &[])).await;
    let doc = &resp.body["payload"];

    assert_eq!(doc["document_version"], admin::SCHEMA_DOCUMENT_VERSION);
    // SDK-002: the module's own version, not the document shape's.
    assert_eq!(doc["schema_version"], 1);
    assert!(
        doc["procedures"].is_array(),
        "the key exists even while empty"
    );

    // Reducers carry their call signature, which is what SDK codegen needs.
    let send = doc["reducers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "send_chat")
        .expect("send_chat is registered");
    assert_eq!(send["params"][0]["name"], "channel");
    assert_eq!(send["params"][0]["type"], "u32");
    assert_eq!(send["params"][1]["type"], "String");
    assert_eq!(send["return_type"], "Result < (), String >");
    assert_eq!(send["max_rate_per_sec"], 0);

    let complete = doc["reducers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "complete_task")
        .unwrap();
    assert_eq!(
        complete["max_rate_per_sec"], 5,
        "the RED-050 rate is published"
    );

    // Tables carry the whole contract: auto-inc, unique, visibility, indexes.
    let table = |name: &str| {
        doc["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == name)
            .unwrap_or_else(|| panic!("{name} missing"))
            .clone()
    };
    let chat = table("ChatMessage");
    assert_eq!(chat["auto_inc"], "id");
    assert_eq!(chat["visibility"]["kind"], "public_all");
    assert!(
        chat["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["kind"] == "fulltext"),
        "{chat}"
    );

    let task = table("Task");
    assert_eq!(task["visibility"]["kind"], "owner_only");
    assert_eq!(task["visibility"]["column"], "owner");
    assert_eq!(task["unique"][0][0], "slug");

    let sensor = table("Sensor");
    assert_eq!(sensor["primary_key"].as_array().unwrap().len(), 2);
    assert!(
        sensor["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["kind"] == "quadtree"),
        "{sensor}"
    );
}
