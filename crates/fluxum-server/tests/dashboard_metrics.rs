//! PRD §12.1 dashboard criterion: the committed Grafana dashboard
//! (`ops/grafana/fluxum-overview.json`) must cover **every P0 metric family**
//! the server exports on `/metrics`. This test scrapes a live server, pulls
//! the `fluxum_*` family names out of the exposition, and asserts each one is
//! referenced by the dashboard JSON — so a new metric without a panel fails
//! here instead of going silently unmonitored.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;

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
        fluxum_core::auth::server_identity("dash-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 256)
}

/// Every declared metric family, from the `# TYPE fluxum_xxx <kind>` header
/// lines — the complete exported surface regardless of whether traffic has
/// produced a data series yet (a per-reducer counter has no data line on a
/// fresh server, but its TYPE header is always present).
fn declared_families(exposition: &str) -> BTreeSet<String> {
    exposition
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("# TYPE ")?;
            let name = rest.split_whitespace().next()?;
            name.starts_with("fluxum_").then(|| name.to_owned())
        })
        .collect()
}

#[tokio::test]
async fn the_dashboard_covers_every_exported_p0_metric_family() {
    let ctx = build_ctx();
    let exposition = ctx.metrics().prometheus(0);
    let exported = declared_families(&exposition);
    assert!(
        exported.contains("fluxum_reducer_calls_total"),
        "sanity: the exposition declares the reducer counter family"
    );

    let dashboard = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/grafana/fluxum-overview.json"),
    )
    .expect("the committed dashboard exists");

    let missing: Vec<&String> = exported
        .iter()
        .filter(|family| !dashboard.contains(family.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "these exported P0 metric families have no dashboard panel: {missing:?}"
    );
}
