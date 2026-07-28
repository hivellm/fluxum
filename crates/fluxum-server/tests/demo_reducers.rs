//! The demo module's reducer behavior through the admin surface — the arms
//! the golden (shape) test cannot see: `move_player` spawn/move/clamp
//! semantics and the chat/task validation refusals. Links the REAL demo
//! module (`fluxum_demo::link()`), so this pins what the served binary does.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::admin::{self, AdminRequest};
use serde_json::{Value, json};

const SHARD: u32 = 23;

fn make_ctx() -> Arc<ShardContext> {
    fluxum_demo::link();
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
        fluxum_core::auth::server_identity("demo-reducers"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 64)
}

async fn call(ctx: &Arc<ShardContext>, method: &str, path: &str, body: Value) -> (u16, Value) {
    let bytes = serde_json::to_vec(&body).unwrap();
    let resp = admin::dispatch(ctx, AdminRequest::local(method, path, &bytes)).await;
    (resp.status, resp.body)
}

async fn players(ctx: &Arc<ShardContext>) -> Vec<Value> {
    let (status, body) = call(
        ctx,
        "POST",
        "/query",
        json!({ "sql": "SELECT * FROM Player" }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["payload"]["rows"].as_array().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn move_player_spawns_moves_and_clamps() {
    let ctx = make_ctx();

    // First call spawns: name + hue derive from the caller identity, and an
    // out-of-world position clamps to the arena instead of erroring.
    let (status, body) = call(&ctx, "POST", "/reducer/move_player", json!([30000, -5])).await;
    assert_eq!(status, 200, "{body}");
    let rows = players(&ctx).await;
    assert_eq!(rows.len(), 1, "one avatar per connection");
    let spawned = rows[0].clone();
    assert_eq!(spawned["x"], 2000, "x clamps to WORLD_W");
    assert_eq!(spawned["y"], 0, "y clamps to 0");
    let name = spawned["name"].as_str().unwrap().to_owned();
    assert!(
        name.starts_with("p-") && name.len() == 8,
        "derived name: {name}"
    );
    let hue = spawned["hue"].as_u64().unwrap();
    assert!(hue < 360, "hue is a degree: {hue}");

    // Later calls move — same avatar, same derived name and hue.
    let (status, body) = call(&ctx, "POST", "/reducer/move_player", json!([50, 60])).await;
    assert_eq!(status, 200, "{body}");
    let rows = players(&ctx).await;
    assert_eq!(rows.len(), 1, "moved, not respawned");
    assert_eq!(rows[0]["x"], 50);
    assert_eq!(rows[0]["y"], 60);
    assert_eq!(
        rows[0]["name"],
        Value::String(name),
        "spawn identity is stable"
    );
    assert_eq!(rows[0]["hue"].as_u64().unwrap(), hue);
}

#[tokio::test(flavor = "multi_thread")]
async fn chat_and_task_validations_refuse_with_named_reasons() {
    let ctx = make_ctx();
    let err = |body: &Value| body["error"].as_str().unwrap_or_default().to_owned();

    let (status, body) = call(&ctx, "POST", "/reducer/send_chat", json!([1, ""])).await;
    assert_eq!(status, 400);
    assert!(err(&body).contains("message is empty"), "{body}");

    let long = "x".repeat(4097);
    let (status, body) = call(&ctx, "POST", "/reducer/send_chat", json!([1, long])).await;
    assert_eq!(status, 400);
    assert!(err(&body).contains("too long"), "{body}");

    let (status, body) = call(&ctx, "POST", "/reducer/add_task", json!([""])).await;
    assert_eq!(status, 400);
    assert!(err(&body).contains("title is empty"), "{body}");

    let (status, body) = call(&ctx, "POST", "/reducer/complete_task", json!([999])).await;
    assert_eq!(status, 400);
    assert!(err(&body).contains("no task 999"), "{body}");

    // Completing an existing task twice is idempotent, not an error.
    let (status, _) = call(&ctx, "POST", "/reducer/add_task", json!(["ship phase8"])).await;
    assert_eq!(status, 200);
    let (status, _) = call(&ctx, "POST", "/reducer/complete_task", json!([1])).await;
    assert_eq!(status, 200);
    let (status, body) = call(&ctx, "POST", "/reducer/complete_task", json!([1])).await;
    assert_eq!(status, 200, "idempotent: {body}");
}
