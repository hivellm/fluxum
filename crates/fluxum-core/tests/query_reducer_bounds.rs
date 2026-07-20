//! SPEC-026 SEC-045/046 — query and reducer execution bounds: the effective
//! `LIMIT` (implicit default, clamp-or-reject over the maximum), the
//! per-query row-scan budget and wall-clock deadline, and the reducer's
//! cooperative deadline + per-transaction write ceiling (breach → rollback,
//! counted).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::Result;
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::{Metrics, QueryAbortReason, ReducerAbortReason};
use fluxum_core::reducer::{
    ExecBounds, FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerDef,
    ReducerEngine, ReducerRegistry, with_context_bounded,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::subscription::{
    QueryBounds, Subscriber, SubscriptionLimits, SubscriptionManager,
};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_protocol::codes;

const SHARD: u32 = 0;

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "note",
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

#[derive(Debug, Clone, PartialEq)]
struct Item {
    id: u64,
    note: String,
}
impl Table for Item {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &ITEM;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.note)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(note)] => Ok(Self {
                id: *id,
                note: note.clone(),
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn seeded_store(rows: u64) -> (Arc<Schema>, MemStore) {
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let item = store.table_id("Item").unwrap();
    let mut tx = store.begin();
    for id in 1..=rows {
        tx.insert(
            item,
            vec![RowValue::U64(id), RowValue::Str(format!("note {id}"))],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    (schema, store)
}

fn manager_with_bounds(
    schema: &Arc<Schema>,
) -> (SubscriptionManager, Arc<QueryBounds>, Arc<Metrics>) {
    let mut manager = SubscriptionManager::new(Arc::clone(schema), SubscriptionLimits::default());
    let bounds = Arc::new(QueryBounds::default());
    let metrics = Metrics::new(SHARD);
    manager.set_query_bounds(Arc::clone(&bounds));
    manager.set_metrics(Arc::clone(&metrics));
    (manager, bounds, metrics)
}

fn viewer() -> Subscriber {
    Subscriber::client(Identity::from_bytes([7; 32]))
}

fn rows_of(sql: &str, manager: &SubscriptionManager, store: &MemStore) -> usize {
    manager
        .snapshot_result(viewer(), sql, &store.snapshot())
        .unwrap()
        .tables[0]
        .inserts
        .len()
}

// --- SEC-045: LIMIT bounds --------------------------------------------------------

#[test]
fn a_query_without_a_limit_gets_the_configured_default() {
    let (schema, store) = seeded_store(50);
    let (manager, bounds, _) = manager_with_bounds(&schema);
    // Unbounded (the built-in default): all rows come back.
    assert_eq!(rows_of("SELECT * FROM Item", &manager, &store), 50);
    bounds.set(5, 0, false, 0, 0);
    assert_eq!(rows_of("SELECT * FROM Item", &manager, &store), 5);
    // An explicit LIMIT below the default is untouched.
    assert_eq!(rows_of("SELECT * FROM Item LIMIT 3", &manager, &store), 3);
}

#[test]
fn an_over_max_limit_is_clamped_by_default_and_rejected_in_reject_mode() {
    let (schema, store) = seeded_store(50);
    let (manager, bounds, metrics) = manager_with_bounds(&schema);
    bounds.set(0, 10, false, 0, 0);
    assert_eq!(
        rows_of("SELECT * FROM Item LIMIT 500", &manager, &store),
        10,
        "clamp mode caps the page silently"
    );
    bounds.set(0, 10, true, 0, 0);
    let err = manager
        .snapshot_result(viewer(), "SELECT * FROM Item LIMIT 500", &store.snapshot())
        .unwrap_err();
    assert_eq!(err.query_code(), Some(codes::SQL_LIMIT_REJECTED), "{err}");
    assert_eq!(metrics.query_aborted(QueryAbortReason::Limit), 1);
    // Under the maximum, reject mode admits normally.
    assert_eq!(rows_of("SELECT * FROM Item LIMIT 10", &manager, &store), 10);
}

#[test]
fn a_rejected_subscription_registers_nothing() {
    let (schema, store) = seeded_store(10);
    let (mut manager, bounds, _) = manager_with_bounds(&schema);
    bounds.set(0, 5, true, 0, 0);
    let err = manager
        .subscribe(1, viewer(), "SELECT * FROM Item LIMIT 100", &store.snapshot())
        .unwrap_err();
    assert_eq!(err.query_code(), Some(codes::SQL_LIMIT_REJECTED), "{err}");
    assert_eq!(manager.plan_count(), 0, "no plan was registered");
    assert_eq!(manager.subscription_count(1), 0);
}

// --- SEC-045: row-scan budget and deadline ----------------------------------------

#[test]
fn a_runaway_scan_hits_the_row_scan_budget() {
    let (schema, store) = seeded_store(1_000);
    let (manager, bounds, metrics) = manager_with_bounds(&schema);
    bounds.set(0, 0, false, 100, 0);
    let err = manager
        .snapshot_result(viewer(), "SELECT * FROM Item", &store.snapshot())
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(codes::SQL_SCAN_BUDGET_EXCEEDED),
        "{err}"
    );
    assert_eq!(metrics.query_aborted(QueryAbortReason::ScanBudget), 1);
    // A query inside the budget still serves.
    bounds.set(0, 0, false, 5_000, 0);
    assert_eq!(rows_of("SELECT * FROM Item", &manager, &store), 1_000);
}

#[test]
fn a_slow_query_is_aborted_at_the_deadline() {
    // 30k rows through the full-scan filter takes well over 1 ms in a debug
    // build; the final deadline poll in `finish` catches even a scan whose
    // per-row cadence missed it.
    let (schema, store) = seeded_store(30_000);
    let (manager, bounds, metrics) = manager_with_bounds(&schema);
    bounds.set(0, 0, false, 0, 1);
    let err = manager
        .snapshot_result(viewer(), "SELECT * FROM Item", &store.snapshot())
        .unwrap_err();
    assert_eq!(err.query_code(), Some(codes::SQL_DEADLINE_EXCEEDED), "{err}");
    assert!(metrics.query_aborted(QueryAbortReason::Deadline) >= 1);
}

// --- SEC-046: reducer bounds (direct, via with_context_bounded) --------------------

fn registry() -> ReducerRegistry {
    ReducerRegistry::new()
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([1; 32]),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: SHARD,
    }
}

#[test]
fn a_reducer_past_its_deadline_is_aborted_at_the_next_host_call() {
    let (_, store) = seeded_store(1);
    let registry = registry();
    let mut tx = store.begin();
    let bounds = ExecBounds {
        deadline: Some(std::time::Instant::now()),
        max_tx_bytes: 0,
    };
    let err = with_context_bounded(&registry, caller(), &mut tx, bounds, |ctx| {
        std::thread::sleep(std::time::Duration::from_millis(2));
        ctx.tx.scan::<Item>()?; // the host-call boundary polls the deadline
        Ok(())
    })
    .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(codes::REDUCER_DEADLINE_EXCEEDED),
        "{err}"
    );
}

#[test]
fn a_breached_bound_is_latched_even_if_the_reducer_swallows_the_error() {
    // SEC-046: catching the typed error and returning Ok must not smuggle a
    // partial write set into a commit.
    let (_, store) = seeded_store(0);
    let registry = registry();
    let mut tx = store.begin();
    let bounds = ExecBounds {
        deadline: None,
        max_tx_bytes: 64,
    };
    let err = with_context_bounded(&registry, caller(), &mut tx, bounds, |ctx| {
        let result = ctx.tx.insert(Item {
            id: 1,
            note: "x".repeat(500),
        });
        assert!(result.is_err(), "the ceiling fired");
        Ok(()) // swallowed — the latch must still roll the call back
    })
    .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(codes::REDUCER_TX_BUDGET_EXCEEDED),
        "{err}"
    );
    drop(tx); // rollback
    assert_eq!(
        store.snapshot().scan(TableId::of("Item")).unwrap().count(),
        0,
        "nothing committed"
    );
}

// --- SEC-046: engine end-to-end (rollback + counters) ------------------------------

fn fill_notes(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    // ~100 KiB across 100 rows — far over a 1 KiB ceiling.
    for id in 1..=100u64 {
        ctx.tx.insert(Item {
            id,
            note: "y".repeat(1_024),
        })?;
    }
    Ok(())
}
fn slow_poke(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(5));
    ctx.tx.scan::<Item>()?;
    Ok(())
}
static FILL_NOTES: ReducerDef = ReducerDef {
    name: "fill_notes",
    handler: fill_notes,
    check_args: |_| Ok(()),
    client_callable: true,
    max_rate_per_sec: 0,
};
static SLOW_POKE: ReducerDef = ReducerDef {
    name: "slow_poke",
    handler: slow_poke,
    check_args: |_| Ok(()),
    client_callable: true,
    max_rate_per_sec: 0,
};

fn engine(dir: &std::path::Path) -> (ReducerEngine, Arc<MemStore>) {
    let schema = Schema::from_tables([&ITEM]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&FILL_NOTES, &SLOW_POKE]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("bounds-test"),
    );
    (engine, store)
}

#[tokio::test]
async fn the_engine_rolls_back_a_write_ceiling_breach_and_counts_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, store) = engine(dir.path());
    engine.bounds().set(0, 1_024);
    let err = engine
        .call(caller(), "fill_notes", vec![])
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(codes::REDUCER_TX_BUDGET_EXCEEDED),
        "{err}"
    );
    assert_eq!(
        store.snapshot().scan(TableId::of("Item")).unwrap().count(),
        0,
        "the transaction rolled back"
    );
    assert_eq!(engine.metrics().reducer_aborted(ReducerAbortReason::Alloc), 1);

    // Raised (or disabled) bounds admit the same reducer.
    engine.bounds().set(0, 0);
    engine.call(caller(), "fill_notes", vec![]).await.unwrap();
    assert_eq!(
        store.snapshot().scan(TableId::of("Item")).unwrap().count(),
        100
    );
}

#[tokio::test]
async fn the_engine_aborts_a_reducer_past_its_deadline_and_counts_it() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = engine(dir.path());
    engine.bounds().set(1, 0); // 1 ms; the body sleeps 5 ms before its host call
    let err = engine.call(caller(), "slow_poke", vec![]).await.unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(codes::REDUCER_DEADLINE_EXCEEDED),
        "{err}"
    );
    assert_eq!(
        engine
            .metrics()
            .reducer_aborted(ReducerAbortReason::Deadline),
        1
    );

    // Unbounded again: the same body commits.
    engine.bounds().set(0, 0);
    engine.call(caller(), "slow_poke", vec![]).await.unwrap();
}
