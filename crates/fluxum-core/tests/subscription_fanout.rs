//! T4.2 subscription fan-out suite (SPEC-005 SUB-001..006/013/020..024/040/
//! 044; FR-30/31/34; DAG exit test): lifecycle round-trip, InitialData equal
//! to a direct committed query, incremental diffs on commit, QueryHash dedup
//! (one plan + one encode for N identical subscribers), value-level pruning
//! and the table-watchers fast path (cost O(matching plans), not O(clients)),
//! ORDER BY/LIMIT on InitialData only, and admission caps (429).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId, Tx};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

// --- Schema --------------------------------------------------------------------

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "channel",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F64,
    },
];

static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[3, 4],
    }],
    visibility: VisibilityRule::PublicAll,
};

static OTHER_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];

static OTHER: TableSchema = TableSchema {
    name: "Other",
    columns: OTHER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&SENSOR, &OTHER]).unwrap())
}

fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([&SENSOR, &OTHER]).unwrap()).unwrap()
}

fn manager() -> SubscriptionManager {
    SubscriptionManager::new(schema(), SubscriptionLimits::default())
}

fn sensor(id: u64, channel: u32, reading: i64, x: f64, y: f64) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::U32(channel),
        RowValue::I64(reading),
        RowValue::F64(x),
        RowValue::F64(y),
    ]
}

fn client(seed: u8) -> Identity {
    Identity::from_bytes([seed; 32])
}

/// Commit a write and return the diff (for `on_commit`).
fn commit(store: &MemStore, write: impl FnOnce(&mut Tx<'_>)) -> fluxum_core::store::TxDiff {
    let mut tx = store.begin();
    write(&mut tx);
    tx.commit().unwrap()
}

fn rowlist_len(list: &fluxum_protocol::RowList) -> usize {
    list.len()
}

// --- SUB-001/002: lifecycle + InitialData ---------------------------------------

#[test]
fn subscribe_returns_initialdata_matching_a_direct_query() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(2, 8, 20, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(3, 7, 30, 0.0, 0.0)).unwrap();
    });

    let mut mgr = manager();
    let sub = mgr
        .subscribe(
            1,
            &client(1),
            "SELECT * FROM Sensor WHERE channel = 7",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(sub.query_id, 1);

    // InitialData equals a direct committed query of channel = 7 (ids 1, 3).
    let direct = store
        .snapshot()
        .scan(sensor_id)
        .unwrap()
        .filter(|r| r.value(1) == Some(&RowValue::U32(7)))
        .count();
    assert_eq!(direct, 2);
    assert_eq!(sub.initial.tables.len(), 1);
    assert_eq!(rowlist_len(&sub.initial.tables[0].inserts), 2);
    assert_eq!(sub.initial.tables[0].table_name, "Sensor");
    assert!(sub.initial.tables[0].deletes.is_empty());
}

// --- SUB-021: incremental diffs on commit ---------------------------------------

#[test]
fn commit_produces_incremental_insert_and_delete_diffs() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 0.0, 0.0)).unwrap();
    });

    let mut mgr = manager();
    mgr.subscribe(
        1,
        &client(1),
        "SELECT * FROM Sensor WHERE channel = 7",
        &store.snapshot(),
    )
    .unwrap();

    // A matching insert plus a non-matching insert: only the match appears.
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(2, 7, 20, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(3, 9, 30, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(deltas.len(), 1, "one matched query");
    assert_eq!(rowlist_len(&deltas[0].update.inserts), 1, "only channel 7");
    assert_eq!(deltas[0].subscribers, vec![1]);

    // Deleting the matching row shows up as a delete diff (PK-only).
    let diff = commit(&store, |tx| {
        assert!(tx.delete(sensor_id, &[RowValue::U64(2)]).unwrap());
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(rowlist_len(&deltas[0].update.deletes), 1);
    assert_eq!(rowlist_len(&deltas[0].update.inserts), 0);

    // A commit touching only an unmatched channel produces nothing.
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(4, 9, 40, 0.0, 0.0)).unwrap();
    });
    assert!(mgr.on_commit(&diff).unwrap().is_empty());
}

// --- SUB-004/005: unsubscribe + disconnect stop delivery ------------------------

#[test]
fn unsubscribe_and_disconnect_stop_delivery() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    let mut mgr = manager();

    let a = mgr
        .subscribe(1, &client(1), "SELECT * FROM Sensor", &store.snapshot())
        .unwrap();
    mgr.subscribe(2, &client(2), "SELECT * FROM Sensor", &store.snapshot())
        .unwrap();
    assert_eq!(mgr.plan_count(), 1, "dedup: one shared plan");

    // Unsubscribe connection 1: its query_id stops, connection 2 remains.
    assert!(mgr.unsubscribe(1, a.query_id));
    assert!(
        !mgr.unsubscribe(1, a.query_id),
        "second unsubscribe is a no-op"
    );
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(deltas[0].subscribers, vec![2], "only connection 2 remains");

    // Disconnect connection 2: the plan is evicted entirely.
    mgr.disconnect(2);
    assert_eq!(mgr.plan_count(), 0);
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(2, 7, 20, 0.0, 0.0)).unwrap();
    });
    assert!(mgr.on_commit(&diff).unwrap().is_empty());
}

// --- SUB-020: QueryHash dedup — one plan + one encode for N subscribers ---------

#[test]
fn identical_queries_share_one_plan_and_one_encoding() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    let mut mgr = manager();

    // 1,000 connections, same normalized query (varied whitespace/case).
    for conn in 0..1_000u128 {
        let sql = if conn % 2 == 0 {
            "SELECT * FROM Sensor WHERE channel = 7"
        } else {
            "select  *  from Sensor where channel=7"
        };
        mgr.subscribe(conn, &client(1), sql, &store.snapshot())
            .unwrap();
    }
    assert_eq!(mgr.plan_count(), 1, "one shared CompiledPlan for all 1,000");

    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    // Exactly ONE delta (one evaluation + one encoding), fanned to 1,000
    // subscribers who each share the same Arc bytes (SUB-024).
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].subscribers.len(), 1_000);
    let update = Arc::clone(&deltas[0].update);
    assert_eq!(rowlist_len(&update.inserts), 1);
    assert_eq!(
        Arc::strong_count(&deltas[0].update),
        2,
        "shared, not copied"
    );
}

// --- SUB-023: value-level pruning selects O(1) plans, not O(clients) ------------

#[test]
fn value_level_pruning_selects_only_the_matching_value_plan() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    let mut mgr = manager();

    // 1,000 clients each on a DISTINCT id value.
    for id in 0..1_000u128 {
        let sql = format!("SELECT * FROM Sensor WHERE id = {id}");
        mgr.subscribe(id, &client(1), &sql, &store.snapshot())
            .unwrap();
    }
    assert_eq!(mgr.plan_count(), 1_000, "1,000 distinct plans");

    // A 1-row commit at id = 42 selects and evaluates exactly ONE plan.
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(42, 7, 10, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(
        deltas.len(),
        1,
        "exactly one plan matched (O(1), not O(clients))"
    );
    assert_eq!(deltas[0].subscribers, vec![42]);
    assert_eq!(rowlist_len(&deltas[0].update.inserts), 1);
}

#[test]
fn table_watchers_fast_path_skips_untouched_tables() {
    let store = store();
    let other_id = store.table_id("Other").unwrap();
    let mut mgr = manager();

    // A no-equality plan lands in the table_watchers tier.
    mgr.subscribe(1, &client(1), "SELECT * FROM Sensor", &store.snapshot())
        .unwrap();

    // A commit touching only `Other` produces no Sensor delta (fast path).
    let diff = commit(&store, |tx| {
        tx.insert(other_id, vec![RowValue::U64(1)]).unwrap();
    });
    assert!(mgr.on_commit(&diff).unwrap().is_empty());
}

// --- SUB-013: ORDER BY / LIMIT on InitialData only ------------------------------

#[test]
fn order_by_limit_apply_to_initialdata_not_diffs() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    for id in 1..=5u64 {
        commit(&store, |tx| {
            tx.insert(sensor_id, sensor(id, 7, (id as i64) * 10, 0.0, 0.0))
                .unwrap();
        });
    }

    let mut mgr = manager();
    let sub = mgr
        .subscribe(
            1,
            &client(1),
            "SELECT * FROM Sensor WHERE channel = 7 ORDER BY reading DESC LIMIT 2",
            &store.snapshot(),
        )
        .unwrap();
    // InitialData: top-2 by reading DESC → ids 5, 4.
    assert_eq!(rowlist_len(&sub.initial.tables[0].inserts), 2);

    // A commit adds 3 more channel-7 rows: the diff is unordered AND
    // unlimited (all 3, no LIMIT 2 applied).
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(6, 7, 5, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(7, 7, 6, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(8, 7, 7, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(
        rowlist_len(&deltas[0].update.inserts),
        3,
        "diffs are unlimited (SUB-013)"
    );
}

// --- SUB-011: spatial InitialData through the index -----------------------------

#[test]
fn spatial_initialdata_uses_the_index() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 5.0, 5.0)).unwrap();
        tx.insert(sensor_id, sensor(2, 7, 20, 500.0, 500.0))
            .unwrap();
    });

    let mut mgr = manager();
    let sub = mgr
        .subscribe(
            1,
            &client(1),
            "SELECT * FROM Sensor IN REGION (0, 0, 100, 100)",
            &store.snapshot(),
        )
        .unwrap();
    // Only the row inside the region (id 1) is in InitialData.
    assert_eq!(rowlist_len(&sub.initial.tables[0].inserts), 1);
}

// --- SUB-044: admission caps ----------------------------------------------------

#[test]
fn admission_caps_reject_with_429_leaving_existing_subscriptions_intact() {
    let store = store();
    let limits = SubscriptionLimits {
        max_subscriptions_per_connection: 2,
        max_compiled_plans: 3,
    };
    let mut mgr = SubscriptionManager::new(schema(), limits);

    // Per-connection cap: 2 subscriptions on connection 1, third is 429.
    mgr.subscribe(
        1,
        &client(1),
        "SELECT * FROM Sensor WHERE id = 1",
        &store.snapshot(),
    )
    .unwrap();
    mgr.subscribe(
        1,
        &client(1),
        "SELECT * FROM Sensor WHERE id = 2",
        &store.snapshot(),
    )
    .unwrap();
    let err = mgr
        .subscribe(
            1,
            &client(1),
            "SELECT * FROM Sensor WHERE id = 3",
            &store.snapshot(),
        )
        .unwrap_err();
    assert_eq!(err.query_code(), Some(429), "{err}");
    assert!(
        err.to_string().contains("max_subscriptions_per_connection"),
        "{err}"
    );
    assert_eq!(mgr.subscription_count(1), 2, "existing subs intact");

    // Global plan cap: connections 2 and 3 add plans 3 (id=1 shared? no —
    // distinct). id=1 and id=2 already exist as plans; a distinct new plan
    // pushes past max_compiled_plans = 3.
    mgr.subscribe(
        2,
        &client(1),
        "SELECT * FROM Sensor WHERE id = 4",
        &store.snapshot(),
    )
    .unwrap(); // plan 3
    let err = mgr
        .subscribe(
            3,
            &client(1),
            "SELECT * FROM Sensor WHERE id = 5",
            &store.snapshot(),
        )
        .unwrap_err();
    assert_eq!(err.query_code(), Some(429), "{err}");
    assert!(err.to_string().contains("max_compiled_plans"), "{err}");

    // Subscribing to an EXISTING plan does not count against the plan cap.
    mgr.subscribe(
        2,
        &client(1),
        "SELECT * FROM Sensor WHERE id = 1",
        &store.snapshot(),
    )
    .unwrap();
    assert_eq!(mgr.plan_count(), 3);
}

// --- Multi-value dedup witness: two different plans on the same table ------------

#[test]
fn distinct_plans_on_one_table_fan_out_independently() {
    let store = store();
    let sensor_id = store.table_id("Sensor").unwrap();
    let mut mgr = manager();

    mgr.subscribe(
        1,
        &client(1),
        "SELECT * FROM Sensor WHERE channel = 7",
        &store.snapshot(),
    )
    .unwrap();
    mgr.subscribe(
        2,
        &client(2),
        "SELECT * FROM Sensor WHERE channel = 8",
        &store.snapshot(),
    )
    .unwrap();
    assert_eq!(mgr.plan_count(), 2);

    // A commit with one row per channel yields one delta per plan.
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 7, 10, 0.0, 0.0)).unwrap();
        tx.insert(sensor_id, sensor(2, 8, 20, 0.0, 0.0)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert_eq!(deltas.len(), 2);
    for delta in &deltas {
        assert_eq!(
            rowlist_len(&delta.update.inserts),
            1,
            "each plan its own row"
        );
        assert_eq!(delta.subscribers.len(), 1);
    }

    // tx_update carries the shard tx id and the shared bytes.
    let update = SubscriptionManager::tx_update(&diff, &deltas[0]);
    assert_eq!(update.tx_id, diff.tx_id);
    assert_eq!(update.tables.len(), 1);
    let _ = TableId::of("Sensor");
}
