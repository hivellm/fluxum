//! T2.5 spatial-index tests (SPEC-008 SPX-001..004/020..022/030/032/040,
//! FR-60): QuadTree region/radius/point queries through the store equal a
//! brute-force full-scan oracle; bucket size is configurable with identical
//! results (SPX-003); coordinate moves leave no stale entries (SPX-032);
//! rollback leaves the spatial index bit-identical to a fresh rebuild
//! (STG-007 rule 2); the SPX-040 event-stream advisory fires on log-like
//! table names only.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;

use fluxum_core::index::Rect;
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule, spatial_stream_advisory,
};
use fluxum_core::store::{MemStore, Row, RowValue, StoreOptions, TableId};

// --- Hand-built static schemas (macro output stand-ins, like index_scans) ---

static VEHICLE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
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

static VEHICLE: TableSchema = TableSchema {
    name: "Vehicle",
    columns: VEHICLE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[1, 2],
    }],
    visibility: VisibilityRule::PublicAll,
};

static SENSOR32_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F32,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F32,
    },
];

static SENSOR32: TableSchema = TableSchema {
    name: "Sensor32",
    columns: SENSOR32_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[1, 2],
    }],
    visibility: VisibilityRule::PublicAll,
};

static PLAIN_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];

static PLAIN: TableSchema = TableSchema {
    name: "Plain",
    columns: PLAIN_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// Test root bounds: [0, 100] × [0, 100] (points outside land in the
/// SPX-004 overflow bucket and must still be found).
fn options(bucket: usize) -> StoreOptions {
    StoreOptions {
        spatial_bucket_size: bucket,
        spatial_bounds: Rect::new(0.0, 0.0, 100.0, 100.0),
        ..StoreOptions::default()
    }
}

fn store_with_bucket(bucket: usize) -> (MemStore, TableId) {
    let schema = Schema::from_tables([&VEHICLE, &PLAIN]).expect("schema assembles");
    let store = MemStore::with_options(&schema, options(bucket)).expect("store builds");
    let table = store.table_id("Vehicle").unwrap();
    (store, table)
}

fn store() -> (MemStore, TableId) {
    store_with_bucket(8)
}

fn vehicle(id: u64, x: f64, y: f64) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::F64(x), RowValue::F64(y)]
}

fn triple(row: &Row) -> (u64, f64, f64) {
    match (row.value(0), row.value(1), row.value(2)) {
        (Some(RowValue::U64(id)), Some(RowValue::F64(x)), Some(RowValue::F64(y))) => (*id, *x, *y),
        other => panic!("malformed Vehicle row: {other:?}"),
    }
}

fn insert_all(store: &MemStore, table: TableId, rows: &[(u64, f64, f64)]) {
    let mut tx = store.begin();
    for &(id, x, y) in rows {
        tx.insert(table, vehicle(id, x, y)).unwrap();
    }
    tx.commit().unwrap();
}

fn ids(mut rows: Vec<(u64, f64, f64)>) -> Vec<u64> {
    rows.sort_by_key(|&(id, _, _)| id);
    rows.into_iter().map(|(id, _, _)| id).collect()
}

// --- Queries vs the full-scan oracle (SPX-020/021, task 1.1/1.5) ---

#[test]
fn region_radius_and_point_queries_match_the_full_scan_oracle() {
    let (store, table) = store();
    // Boundary-heavy layout: corners, edges, midlines, coincident points,
    // and out-of-bounds rows.
    insert_all(
        &store,
        table,
        &[
            (1, 0.0, 0.0),     // root corner
            (2, 100.0, 100.0), // opposite corner
            (3, 50.0, 50.0),   // dead centre (quadrant midlines)
            (4, 50.0, 50.0),   // coincident with row 3 (distinct PK)
            (5, 25.0, 75.0),
            (6, 30.0, 40.0),
            (7, 33.0, 44.0),   // distance 5 from (30, 40)
            (8, -10.0, 50.0),  // out of bounds (overflow bucket)
            (9, 110.0, 110.0), // out of bounds
        ],
    );
    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();

    let regions = [
        Rect::new(0.0, 0.0, 100.0, 100.0),
        Rect::new(0.0, 0.0, 50.0, 50.0), // edge ends exactly on midline
        Rect::new(50.0, 50.0, 50.0, 50.0), // starts exactly on midline
        Rect::new(25.0, 75.0, 0.0, 0.0), // degenerate point box
        Rect::new(-20.0, -20.0, 200.0, 200.0), // covers the overflow rows
        Rect::new(60.0, 0.0, -10.0, 10.0), // negative extent: empty
    ];
    for region in regions {
        let got = ids(snap
            .spatial_region(table, region)
            .unwrap()
            .iter()
            .map(triple)
            .collect());
        let want = ids(snap
            .scan(table)
            .unwrap()
            .map(triple)
            .filter(|&(_, x, y)| region.contains_point(x, y))
            .collect());
        assert_eq!(got, want, "region {region:?}");
    }

    let radii = [
        (30.0, 40.0, 5.0), // row 7 at distance exactly 5 must be included
        (50.0, 50.0, 0.0),
        (0.0, 0.0, 150.0),
        (-10.0, 50.0, 1.0), // centred on an overflow row
        (50.0, 50.0, -1.0), // negative radius: empty
    ];
    for (cx, cy, r) in radii {
        let got = ids(snap
            .spatial_radius(table, cx, cy, r)
            .unwrap()
            .iter()
            .map(triple)
            .collect());
        let want = ids(snap
            .scan(table)
            .unwrap()
            .map(triple)
            .filter(|&(_, x, y)| {
                let (dx, dy) = (x - cx, y - cy);
                r >= 0.0 && dx * dx + dy * dy <= r * r
            })
            .collect());
        assert_eq!(got, want, "radius ({cx}, {cy}, {r})");
    }

    assert_eq!(
        ids(snap
            .spatial_point(table, 50.0, 50.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [3, 4]
    );
    assert!(snap.spatial_point(table, 51.0, 50.0).unwrap().is_empty());
}

// --- Update coherence (SPX-032, task 1.3) ---

#[test]
fn coordinate_move_leaves_no_stale_entries() {
    let (store, table) = store();
    insert_all(&store, table, &[(1, 10.0, 10.0), (2, 90.0, 90.0)]);

    // Upsert moving row 1 from (10, 10) to (80, 20): delete + reinsert with
    // different content merges as PendingOp::Update — old entry out, new
    // entry in, atomically with the commit.
    let mut tx = store.begin();
    assert!(tx.delete(table, &[RowValue::U64(1)]).unwrap());
    tx.insert(table, vehicle(1, 80.0, 20.0)).unwrap();
    tx.commit().unwrap();

    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();
    // Old-location queries no longer return the row…
    assert!(
        snap.spatial_region(table, Rect::new(5.0, 5.0, 10.0, 10.0))
            .unwrap()
            .is_empty()
    );
    assert!(snap.spatial_point(table, 10.0, 10.0).unwrap().is_empty());
    assert!(
        snap.spatial_radius(table, 10.0, 10.0, 1.0)
            .unwrap()
            .is_empty()
    );
    // …and new-location queries do.
    assert_eq!(
        ids(snap
            .spatial_region(table, Rect::new(75.0, 15.0, 10.0, 10.0))
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1]
    );
    // A move into and out of the overflow bucket stays coherent too.
    let mut tx = store.begin();
    assert!(tx.delete(table, &[RowValue::U64(1)]).unwrap());
    tx.insert(table, vehicle(1, -50.0, -50.0)).unwrap();
    tx.commit().unwrap();
    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();
    assert!(snap.spatial_point(table, 80.0, 20.0).unwrap().is_empty());
    assert_eq!(
        ids(snap
            .spatial_point(table, -50.0, -50.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1]
    );
}

// --- Rollback and MVCC (SPX-030, STG-007 rule 2) ---

#[test]
fn rollback_leaves_the_spatial_index_untouched() {
    let (store, table) = store();
    insert_all(&store, table, &[(1, 10.0, 10.0), (2, 20.0, 20.0)]);
    let before = store.snapshot();

    let mut tx = store.begin();
    tx.insert(table, vehicle(3, 30.0, 30.0)).unwrap();
    assert!(tx.delete(table, &[RowValue::U64(1)]).unwrap());
    tx.insert(table, vehicle(1, 99.0, 99.0)).unwrap(); // reinsert-different
    tx.rollback();

    let after = store.snapshot();
    assert!(before.same_state(&after)); // the exact prior state
    after.verify_index_integrity(table).unwrap();
    assert_eq!(
        ids(after
            .spatial_region(table, Rect::new(0.0, 0.0, 100.0, 100.0))
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1, 2]
    );
    assert!(after.spatial_point(table, 30.0, 30.0).unwrap().is_empty());
    assert_eq!(
        ids(after
            .spatial_point(table, 10.0, 10.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1]
    );
}

#[test]
fn spatial_reads_in_a_tx_see_committed_state_only() {
    let (store, table) = store();
    insert_all(&store, table, &[(1, 10.0, 10.0)]);

    let mut tx = store.begin();
    tx.insert(table, vehicle(2, 10.0, 10.0)).unwrap();
    assert!(tx.delete(table, &[RowValue::U64(1)]).unwrap());
    // Pending insert invisible; pending delete not yet applied (STG-004).
    assert_eq!(
        ids(tx
            .spatial_point(table, 10.0, 10.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1]
    );
    let pinned = store.snapshot();
    tx.commit().unwrap();

    // The pre-commit snapshot still answers from its pinned state.
    assert_eq!(
        ids(pinned
            .spatial_point(table, 10.0, 10.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [1]
    );
    assert_eq!(
        ids(store
            .snapshot()
            .spatial_point(table, 10.0, 10.0)
            .unwrap()
            .iter()
            .map(triple)
            .collect()),
        [2]
    );
}

// --- Bucket size configuration (SPX-003, task 1.2) ---

#[test]
fn bucket_size_is_configurable_and_never_changes_results() {
    let rows: Vec<(u64, f64, f64)> = (0u32..40)
        .map(|i| {
            let f = f64::from(i);
            (u64::from(i), (f * 7.3) % 100.0, (f * 13.7) % 100.0)
        })
        .collect();
    let region = Rect::new(10.0, 10.0, 55.0, 65.0);

    let mut answers: Vec<(Vec<u64>, Vec<u64>)> = Vec::new();
    for bucket in [1usize, 2, 8, 64] {
        let (store, table) = store_with_bucket(bucket);
        insert_all(&store, table, &rows);
        let snap = store.snapshot();
        snap.verify_index_integrity(table).unwrap();
        answers.push((
            ids(snap
                .spatial_region(table, region)
                .unwrap()
                .iter()
                .map(triple)
                .collect()),
            ids(snap
                .spatial_radius(table, 50.0, 50.0, 30.0)
                .unwrap()
                .iter()
                .map(triple)
                .collect()),
        ));
    }
    // The default (8) and every non-default bucket size agree exactly.
    assert!(answers.windows(2).all(|w| w[0] == w[1]), "{answers:?}");

    // Default is 8; zero is rejected.
    assert_eq!(StoreOptions::default().spatial_bucket_size, 8);
    let schema = Schema::from_tables([&VEHICLE]).expect("schema assembles");
    let err = MemStore::with_options(&schema, options(0))
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("spatial_bucket_size"), "{err}");
}

#[test]
fn invalid_spatial_bounds_are_rejected() {
    let schema = Schema::from_tables([&VEHICLE]).expect("schema assembles");
    for bounds in [
        Rect::new(0.0, 0.0, -1.0, 10.0),
        Rect::new(0.0, 0.0, 10.0, 0.0),
        Rect::new(f64::NAN, 0.0, 10.0, 10.0),
        Rect::new(0.0, 0.0, f64::INFINITY, 10.0),
    ] {
        let err = MemStore::with_options(
            &schema,
            StoreOptions {
                spatial_bounds: bounds,
                ..StoreOptions::default()
            },
        )
        .map(|_| ())
        .unwrap_err();
        assert!(err.to_string().contains("spatial_bounds"), "{err}");
    }
}

// --- f32 coordinate columns widen losslessly (SPX-001) ---

#[test]
fn f32_coordinate_columns_are_widened_to_f64() {
    let schema = Schema::from_tables([&SENSOR32]).expect("schema assembles");
    let store = MemStore::with_options(&schema, options(8)).expect("store builds");
    let table = store.table_id("Sensor32").unwrap();

    let mut tx = store.begin();
    tx.insert(
        table,
        vec![RowValue::U64(1), RowValue::F32(12.5), RowValue::F32(87.5)],
    )
    .unwrap();
    tx.commit().unwrap();

    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();
    assert_eq!(snap.spatial_point(table, 12.5, 87.5).unwrap().len(), 1);
    assert_eq!(
        snap.spatial_region(table, Rect::new(12.5, 87.5, 0.0, 0.0))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        snap.spatial_radius(table, 12.5, 87.5, 0.0).unwrap().len(),
        1
    );
}

// --- Error paths (SPX-022 surface; coded 400/503 land with T2.6) ---

#[test]
fn spatial_queries_on_a_non_spatial_table_are_rejected() {
    let (store, _) = store();
    let plain = store.table_id("Plain").unwrap();
    let snap = store.snapshot();
    for err in [
        snap.spatial_region(plain, Rect::new(0.0, 0.0, 1.0, 1.0))
            .map(|_| ())
            .unwrap_err(),
        snap.spatial_radius(plain, 0.0, 0.0, 1.0)
            .map(|_| ())
            .unwrap_err(),
        snap.spatial_point(plain, 0.0, 0.0).map(|_| ()).unwrap_err(),
    ] {
        assert!(
            err.to_string()
                .contains("table 'Plain' has no spatial index"),
            "{err}"
        );
    }
}

// --- SPX-040 advisory lint (task 1.4) ---

#[test]
fn event_stream_advisory_fires_on_log_like_spatial_tables_only() {
    static TICKLOG_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "x",
            ty: FluxType::F32,
        },
        ColumnSchema {
            name: "y",
            ty: FluxType::F32,
        },
    ];
    static SPATIAL_QT: &[IndexSchema] = &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[1, 2],
    }];
    let named = |name: &'static str, indexes: &'static [IndexSchema]| TableSchema {
        name,
        columns: TICKLOG_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes,
        visibility: VisibilityRule::PublicAll,
    };

    // Every documented event-stream suffix fires the advisory…
    for name in [
        "GpsTickLog",
        "PositionStream",
        "MoveTick",
        "RouteTrace",
        "LocationHistory",
    ] {
        let table = named(name, SPATIAL_QT);
        let advisory = spatial_stream_advisory(&table)
            .unwrap_or_else(|| panic!("expected advisory for {name}"));
        assert!(advisory.contains("SPX-040"), "{advisory}");
        assert!(advisory.contains(name), "{advisory}");
    }
    // …persistent geo-state names do not…
    for name in ["Vehicle", "Sensor", "Zone"] {
        assert_eq!(spatial_stream_advisory(&named(name, SPATIAL_QT)), None);
    }
    // …and a log-like name without a spatial index does not.
    assert_eq!(spatial_stream_advisory(&named("AuditLog", &[])), None);

    // The advisory is non-fatal: the schema still assembles and stores.
    static GPS_TICK_LOG: TableSchema = TableSchema {
        name: "GpsTickLog",
        columns: TICKLOG_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: SPATIAL_QT,
        visibility: VisibilityRule::PublicAll,
    };
    let schema = Schema::from_tables([&GPS_TICK_LOG]).expect("advisory must not reject");
    assert!(MemStore::with_options(&schema, options(8)).is_ok());
}

// --- Property suite (task 1.5, DAG exit test): spatial queries ≡ full-scan
// --- oracle under randomized insert/delete/move workloads with commits and
// --- rollbacks; index always bit-identical to a fresh rebuild (STG-007).

/// Grid coordinates: in-bounds lattice points (including bounds edges and
/// quadrant midlines) plus out-of-bounds values, forcing coincident points,
/// midline routing, and the overflow bucket.
fn coord() -> impl Strategy<Value = f64> {
    prop_oneof![
        8 => (0u8..=8).prop_map(|i| f64::from(i) * 12.5),
        1 => Just(-5.0),
        1 => Just(105.0),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    Insert {
        id: u64,
        x: f64,
        y: f64,
    },
    /// Upsert to new coordinates (SPX-032 move) when the row exists.
    Move {
        id: u64,
        x: f64,
        y: f64,
    },
    Delete {
        id: u64,
    },
    Commit,
    Rollback,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u64..=10, coord(), coord()).prop_map(|(id, x, y)| Op::Insert { id, x, y }),
        3 => (1u64..=10, coord(), coord()).prop_map(|(id, x, y)| Op::Move { id, x, y }),
        2 => (1u64..=10).prop_map(|id| Op::Delete { id }),
        1 => Just(Op::Commit),
        1 => Just(Op::Rollback),
    ]
}

type Model = BTreeMap<u64, (f64, f64)>;

/// Spatial answers over the committed snapshot must equal the full-scan
/// oracle, and the index must be bit-identical to a fresh rebuild.
fn check_against_model(store: &MemStore, table: TableId, model: &Model) {
    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();

    let expected: Vec<(u64, f64, f64)> = model.iter().map(|(&id, &(x, y))| (id, x, y)).collect();

    let regions = [
        Rect::new(0.0, 0.0, 100.0, 100.0),
        Rect::new(12.5, 25.0, 37.5, 25.0),
        Rect::new(50.0, 50.0, 0.0, 0.0),
        Rect::new(-10.0, -10.0, 200.0, 200.0),
        Rect::new(62.5, 62.5, 37.5, 37.5),
    ];
    for region in regions {
        let got = ids(snap
            .spatial_region(table, region)
            .unwrap()
            .iter()
            .map(triple)
            .collect());
        let want: Vec<u64> = expected
            .iter()
            .filter(|&&(_, x, y)| region.contains_point(x, y))
            .map(|&(id, _, _)| id)
            .collect();
        assert_eq!(got, want, "region {region:?}");
    }

    let radii = [(50.0, 50.0, 12.5), (0.0, 0.0, 25.0), (100.0, 100.0, 0.0)];
    for (cx, cy, r) in radii {
        let got = ids(snap
            .spatial_radius(table, cx, cy, r)
            .unwrap()
            .iter()
            .map(triple)
            .collect());
        let want: Vec<u64> = expected
            .iter()
            .filter(|&&(_, x, y)| {
                let (dx, dy) = (x - cx, y - cy);
                dx * dx + dy * dy <= r * r
            })
            .map(|&(id, _, _)| id)
            .collect();
        assert_eq!(got, want, "radius ({cx}, {cy}, {r})");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn spatial_queries_equal_the_full_scan_oracle_under_random_ops(
        ops in prop::collection::vec(op_strategy(), 1..70),
        bucket in prop_oneof![Just(1usize), Just(2), Just(8)],
    ) {
        let (store, table) = store_with_bucket(bucket);
        let mut committed: Model = BTreeMap::new();
        let mut pending: Model = BTreeMap::new();
        let mut tx = Some(store.begin());

        for op in ops {
            match op {
                Op::Insert { id, x, y } => {
                    let result = tx.as_mut().unwrap().insert(table, vehicle(id, x, y));
                    if let std::collections::btree_map::Entry::Vacant(slot) = pending.entry(id) {
                        prop_assert!(result.is_ok(), "{result:?}");
                        slot.insert((x, y));
                    } else {
                        prop_assert!(result.is_err()); // PK conflict
                    }
                }
                Op::Move { id, x, y } => {
                    if let std::collections::btree_map::Entry::Occupied(mut slot) =
                        pending.entry(id)
                    {
                        let t = tx.as_mut().unwrap();
                        prop_assert!(t.delete(table, &[RowValue::U64(id)]).unwrap());
                        t.insert(table, vehicle(id, x, y)).unwrap();
                        slot.insert((x, y));
                    }
                }
                Op::Delete { id } => {
                    let existed = pending.remove(&id).is_some();
                    let deleted = tx
                        .as_mut()
                        .unwrap()
                        .delete(table, &[RowValue::U64(id)])
                        .unwrap();
                    prop_assert_eq!(deleted, existed);
                }
                Op::Commit => {
                    tx.take().unwrap().commit().unwrap();
                    committed.clone_from(&pending);
                    check_against_model(&store, table, &committed);
                    tx = Some(store.begin());
                }
                Op::Rollback => {
                    tx.take().unwrap().rollback();
                    pending.clone_from(&committed);
                    check_against_model(&store, table, &committed);
                    tx = Some(store.begin());
                }
            }
        }

        // Dropping the trailing transaction rolls it back (STG-006).
        drop(tx);
        check_against_model(&store, table, &committed);
    }
}
