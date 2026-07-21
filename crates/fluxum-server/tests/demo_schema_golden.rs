//! The DEMO module's `/schema`, pinned (T6.5, FR-82).
//!
//! `tests/golden/schema.json` freezes the *document shape* against a broad
//! fixture (T6.1); THIS golden pins the schema of the actual demo module the
//! served binary links (`crates/fluxum-demo`) — the document the committed
//! TypeScript bindings under `sdks/typescript/tests/generated/` are generated
//! from. The chain is: demo module → this golden → generated bindings → the
//! `generated.e2e` test that drives the live server through them. A demo
//! schema change fails here first, and the fix is deliberate: regenerate this
//! golden (`FLUXUM_REGEN=1`), then the bindings
//! (`FLUXUM_REGEN=1 cargo test -p fluxum-cli --test typescript_generated_golden`).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::Arc;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::admin;

const SHARD: u32 = 22;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/demo-schema.json")
}

/// The demo module rendered through `GET /schema`, canonicalized exactly as
/// `fluxum schema export` writes it. This test binary links ONLY the demo
/// module, so `Schema::assemble` sees exactly what the served binary serves.
async fn exported_demo_schema() -> String {
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
        fluxum_core::auth::server_identity("demo-golden"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 64);

    let resp = admin::dispatch(&ctx, admin::AdminRequest::local("GET", "/schema", &[])).await;
    assert_eq!(resp.status, 200);
    let body = serde_json::to_string(&resp.body).unwrap();
    fluxum_cli::canonical_schema(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_demo_module_schema_matches_its_golden() {
    let actual = exported_demo_schema().await;
    let path = golden_path();

    if std::env::var_os("FLUXUM_REGEN").is_some() {
        std::fs::write(&path, &actual).unwrap();
        return;
    }

    let committed = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert_eq!(
        committed, actual,
        "the demo module's schema drifted from tests/golden/demo-schema.json — the committed \
         TypeScript bindings were generated from that golden. Regenerate BOTH, deliberately:\n\
         FLUXUM_REGEN=1 cargo test -p fluxum-server --test demo_schema_golden\n\
         FLUXUM_REGEN=1 cargo test -p fluxum-cli --test typescript_generated_golden"
    );
}
