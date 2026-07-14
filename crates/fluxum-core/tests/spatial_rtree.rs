//! T2.6 R-tree and spatial-predicate tests (SPEC-008 SPX-010/020..023/
//! 030..032, FR-61/FR-62): R-tree region/radius queries through the store
//! equal a brute-force full-scan oracle (intersection semantics, degenerate
//! boxes, shared edges); the `min <= max` insert constraint is enforced
//! eagerly; `IN REGION` / `WITHIN RADIUS` predicates resolve via the spatial
//! index with the SPEC-008 error contract — 400 for invalid parameters or a
//! non-spatial table, 503 while the post-recovery rebuild is pending — and
//! rebuild restores results identical to the pre-rebuild committed state.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use proptest::prelude::*;

use fluxum_core::FluxumError;
use fluxum_core::index::{Aabb, Rect, SpatialPredicate};
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, Row, RowValue, StoreOptions, TableId};

// --- Hand-built static schemas (macro output stand-ins) ---

static ZONE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "min_x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "min_y",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "max_x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "max_y",
        ty: FluxType::F64,
    },
];

static ZONE: TableSchema = TableSchema {
    name: "Zone",
    columns: ZONE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::RTree,
        columns: &[1, 2, 3, 4],
    }],
    visibility: VisibilityRule::PublicAll,
};

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

struct Ids {
    zone: TableId,
    vehicle: TableId,
    plain: TableId,
}

fn store_with_capacity(capacity: usize) -> (MemStore, Ids) {
    let schema = Schema::from_tables([&ZONE, &VEHICLE, &PLAIN]).expect("schema assembles");
    let store = MemStore::with_options(
        &schema,
        StoreOptions {
            spatial_bucket_size: capacity,
            spatial_bounds: Rect::new(0.0, 0.0, 100.0, 100.0),
            ..StoreOptions::default()
        },
    )
    .expect("store builds");
    let ids = Ids {
        zone: store.table_id("Zone").unwrap(),
        vehicle: store.table_id("Vehicle").unwrap(),
        plain: store.table_id("Plain").unwrap(),
    };
    (store, ids)
}

fn store() -> (MemStore, Ids) {
    store_with_capacity(8)
}

fn zone(id: u64, b: Aabb) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::F64(b.min_x),
        RowValue::F64(b.min_y),
        RowValue::F64(b.max_x),
        RowValue::F64(b.max_y),
    ]
}

fn vehicle(id: u64, x: f64, y: f64) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::F64(x), RowValue::F64(y)]
}

fn row_id(row: &Row) -> u64 {
    match row.value(0) {
        Some(RowValue::U64(id)) => *id,
        other => panic!("malformed row id: {other:?}"),
    }
}

fn zone_box(row: &Row) -> Aabb {
    match (row.value(1), row.value(2), row.value(3), row.value(4)) {
        (
            Some(RowValue::F64(a)),
            Some(RowValue::F64(b)),
            Some(RowValue::F64(c)),
            Some(RowValue::F64(d)),
        ) => Aabb::new(*a, *b, *c, *d),
        other => panic!("malformed Zone row: {other:?}"),
    }
}

fn ids_of(rows: &[Row]) -> Vec<u64> {
    let mut ids: Vec<u64> = rows.iter().map(row_id).collect();
    ids.sort_unstable();
    ids
}

fn insert_zones(store: &MemStore, table: TableId, rows: &[(u64, Aabb)]) {
    let mut tx = store.begin();
    for &(id, b) in rows {
        tx.insert(table, zone(id, b)).unwrap();
    }
    tx.commit().unwrap();
}

// --- R-tree via the store: intersection semantics vs the oracle (1.1) ---

#[test]
fn region_queries_use_intersection_semantics_for_boxes() {
    let (store, ids) = store();
    insert_zones(
        &store,
        ids.zone,
        &[
            (1, Aabb::new(0.0, 0.0, 10.0, 10.0)),
            (2, Aabb::new(10.0, 10.0, 20.0, 20.0)), // touches 1 at a corner
            (3, Aabb::new(40.0, 40.0, 60.0, 60.0)),
            (4, Aabb::new(50.0, 50.0, 50.0, 50.0)), // degenerate point box
            (5, Aabb::new(50.0, 0.0, 50.0, 100.0)), // degenerate segment
            (6, Aabb::new(-10.0, -10.0, -5.0, -5.0)), // outside quadtree-style bounds
        ],
    );
    let snap = store.snapshot();
    snap.verify_index_integrity(ids.zone).unwrap();

    let regions = [
        Rect::new(0.0, 0.0, 100.0, 100.0),
        Rect::new(10.0, 10.0, 0.0, 0.0), // point probe on the shared corner
        Rect::new(45.0, 45.0, 10.0, 10.0),
        Rect::new(-20.0, -20.0, 12.0, 12.0),
        Rect::new(70.0, 70.0, 5.0, 5.0),
    ];
    for region in regions {
        let got = ids_of(&snap.spatial_region(ids.zone, region).unwrap());
        let query = Aabb::new(region.x, region.y, region.x + region.w, region.y + region.h);
        let mut want: Vec<u64> = snap
            .scan(ids.zone)
            .unwrap()
            .filter(|row| zone_box(row).intersects(&query))
            .map(row_id)
            .collect();
        want.sort_unstable();
        assert_eq!(got, want, "region {region:?}");
    }

    // Radius: minimum box distance, exactly-r inclusive (SPX-021).
    let got = ids_of(&snap.spatial_radius(ids.zone, 30.0, 30.0, 14.15).unwrap());
    // Box 2's corner (20, 20) is 10√2 ≈ 14.142 away; box 3's corner
    // (40, 40) likewise; box 5 (x = 50) is 20 away.
    assert_eq!(got, [2, 3]);
    assert_eq!(
        ids_of(&snap.spatial_radius(ids.zone, 30.0, 30.0, 14.1).unwrap()),
        [] as [u64; 0]
    );
    // Centre inside a box: distance zero.
    assert_eq!(
        ids_of(&snap.spatial_radius(ids.zone, 5.0, 5.0, 0.0).unwrap()),
        [1]
    );
}

// --- SPX-010 constraint: min <= max enforced eagerly at insert ---

#[test]
fn rtree_min_max_constraint_fails_the_insert() {
    let (store, ids) = store();
    let mut tx = store.begin();
    for bad in [
        Aabb::new(10.0, 0.0, 5.0, 10.0),      // min_x > max_x
        Aabb::new(0.0, 10.0, 10.0, 5.0),      // min_y > max_y
        Aabb::new(f64::NAN, 0.0, 10.0, 10.0), // NaN never satisfies min <= max
    ] {
        let err = tx.insert(ids.zone, zone(1, bad)).unwrap_err();
        assert!(err.to_string().contains("SPX-010"), "{err}");
    }
    // The failed inserts left nothing behind; a valid row still works.
    tx.insert(ids.zone, zone(1, Aabb::new(0.0, 0.0, 1.0, 1.0)))
        .unwrap();
    tx.commit().unwrap();
    let snap = store.snapshot();
    snap.verify_index_integrity(ids.zone).unwrap();
    assert_eq!(snap.row_count(ids.zone).unwrap(), 1);
}

// --- Update coherence and rollback (SPX-030/032) ---

#[test]
fn box_moves_leave_no_stale_entries_and_rollback_is_exact() {
    let (store, ids) = store();
    insert_zones(&store, ids.zone, &[(1, Aabb::new(0.0, 0.0, 10.0, 10.0))]);

    // Move the box to the opposite corner.
    let mut tx = store.begin();
    assert!(tx.delete(ids.zone, &[RowValue::U64(1)]).unwrap());
    tx.insert(ids.zone, zone(1, Aabb::new(80.0, 80.0, 90.0, 90.0)))
        .unwrap();
    tx.commit().unwrap();

    let snap = store.snapshot();
    snap.verify_index_integrity(ids.zone).unwrap();
    assert!(
        snap.spatial_region(ids.zone, Rect::new(0.0, 0.0, 20.0, 20.0))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        ids_of(
            &snap
                .spatial_region(ids.zone, Rect::new(85.0, 85.0, 1.0, 1.0))
                .unwrap()
        ),
        [1]
    );

    // Rollback restores the exact prior state.
    let before = store.snapshot();
    let mut tx = store.begin();
    assert!(tx.delete(ids.zone, &[RowValue::U64(1)]).unwrap());
    tx.insert(ids.zone, zone(2, Aabb::new(1.0, 1.0, 2.0, 2.0)))
        .unwrap();
    tx.rollback();
    let after = store.snapshot();
    assert!(before.same_state(&after));
    after.verify_index_integrity(ids.zone).unwrap();
}

// --- Predicate evaluation (1.2): IN REGION / WITHIN RADIUS via the index ---

#[test]
fn predicates_resolve_via_the_index_on_both_spatial_families() {
    let (store, ids) = store();
    insert_zones(
        &store,
        ids.zone,
        &[
            (1, Aabb::new(0.0, 0.0, 10.0, 10.0)),
            (2, Aabb::new(30.0, 30.0, 45.0, 45.0)),
        ],
    );
    let mut tx = store.begin();
    for (id, x, y) in [(1u64, 5.0, 5.0), (2, 40.0, 40.0), (3, 95.0, 95.0)] {
        tx.insert(ids.vehicle, vehicle(id, x, y)).unwrap();
    }
    tx.commit().unwrap();
    let snap = store.snapshot();

    // QuadTree table: points inside the closed box.
    let got = snap
        .eval_spatial(
            ids.vehicle,
            &SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: 40.0,
                h: 40.0,
            },
        )
        .unwrap();
    assert_eq!(ids_of(&got), [1, 2]); // (40, 40) on the closed edge

    // R-tree table: stored boxes intersecting the query box.
    let got = snap
        .eval_spatial(
            ids.zone,
            &SpatialPredicate::InRegion {
                x: 10.0,
                y: 10.0,
                w: 5.0,
                h: 5.0,
            },
        )
        .unwrap();
    assert_eq!(ids_of(&got), [1]); // zone 1 touches (10, 10)

    // WITHIN RADIUS, both families, exactly-r inclusive.
    let got = snap
        .eval_spatial(
            ids.vehicle,
            &SpatialPredicate::WithinRadius {
                x: 0.0,
                y: 5.0,
                r: 5.0,
            },
        )
        .unwrap();
    assert_eq!(ids_of(&got), [1]); // vehicle 1 at distance exactly 5
    let got = snap
        .eval_spatial(
            ids.zone,
            &SpatialPredicate::WithinRadius {
                x: 20.0,
                y: 20.0,
                r: 14.15,
            },
        )
        .unwrap();
    assert_eq!(ids_of(&got), [1, 2]); // both corners at 10√2 ≈ 14.142

    // Tx-level evaluation sees the committed snapshot only.
    let mut tx = store.begin();
    tx.insert(ids.vehicle, vehicle(9, 5.0, 5.0)).unwrap();
    let got = tx
        .eval_spatial(
            ids.vehicle,
            &SpatialPredicate::WithinRadius {
                x: 5.0,
                y: 5.0,
                r: 0.0,
            },
        )
        .unwrap();
    assert_eq!(ids_of(&got), [1]);
    tx.rollback();
}

// --- Error paths (1.3): 400 invalid parameters / no spatial index ---

#[test]
fn invalid_predicates_and_non_spatial_tables_return_code_400() {
    let (store, ids) = store();
    let snap = store.snapshot();

    let cases = [
        (
            ids.vehicle,
            SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: -1.0,
                h: 10.0,
            },
            "width",
        ),
        (
            ids.zone,
            SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: 10.0,
                h: -0.5,
            },
            "height",
        ),
        (
            ids.vehicle,
            SpatialPredicate::WithinRadius {
                x: 0.0,
                y: 0.0,
                r: -2.0,
            },
            "radius",
        ),
        (
            ids.zone,
            SpatialPredicate::WithinRadius {
                x: 0.0,
                y: 0.0,
                r: f64::NAN,
            },
            "radius",
        ),
    ];
    for (table, predicate, needle) in cases {
        let err = snap
            .eval_spatial(table, &predicate)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.query_code(), Some(400), "{err}");
        assert!(err.to_string().contains(needle), "{err}");
        assert!(err.to_string().contains("non-negative"), "{err}");
    }

    // SPX-022: spatial predicate on a table without #[spatial].
    let err = snap
        .eval_spatial(
            ids.plain,
            &SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: 1.0,
                h: 1.0,
            },
        )
        .map(|_| ())
        .unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");
    assert!(
        err.to_string()
            .contains("table 'Plain' has no spatial index"),
        "{err}"
    );
    assert!(matches!(err, FluxumError::Query { .. }));
}

// --- Error paths (1.3): 503 during the post-recovery rebuild (SPX-031) ---

#[test]
fn rebuild_gate_returns_503_then_identical_results() {
    let (store, ids) = store();
    insert_zones(
        &store,
        ids.zone,
        &[
            (1, Aabb::new(0.0, 0.0, 10.0, 10.0)),
            (2, Aabb::new(20.0, 20.0, 30.0, 30.0)),
        ],
    );
    let mut tx = store.begin();
    tx.insert(ids.vehicle, vehicle(1, 5.0, 5.0)).unwrap();
    tx.commit().unwrap();

    let region = SpatialPredicate::InRegion {
        x: 0.0,
        y: 0.0,
        w: 100.0,
        h: 100.0,
    };
    let pre_zone = ids_of(&store.snapshot().eval_spatial(ids.zone, &region).unwrap());
    let pre_vehicle = ids_of(&store.snapshot().eval_spatial(ids.vehicle, &region).unwrap());
    assert!(store.spatial_ready());

    // Enter the rebuilding state (what recovery does for unpersisted
    // spatial indexes, SPX-031).
    store.mark_spatial_rebuilding();
    assert!(!store.spatial_ready()); // ReducerCall admission gate input
    let snap = store.snapshot();
    for table in [ids.zone, ids.vehicle] {
        assert!(!snap.spatial_ready(table).unwrap());
        let err = snap.eval_spatial(table, &region).map(|_| ()).unwrap_err();
        assert_eq!(err.query_code(), Some(503), "{err}");
        assert!(err.to_string().contains("spatial index not ready"), "{err}");
        // Direct spatial reads hit the same gate — no scan fallback exists.
        let err = snap
            .spatial_radius(table, 0.0, 0.0, 1000.0)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(err.query_code(), Some(503), "{err}");
    }
    // Non-spatial access is unaffected while rebuilding.
    assert_eq!(snap.row_count(ids.zone).unwrap(), 2);
    assert!(snap.spatial_ready(ids.plain).unwrap());

    // Rebuild completes: results identical to the pre-rebuild state.
    store.rebuild_spatial_indexes().unwrap();
    assert!(store.spatial_ready());
    let snap = store.snapshot();
    snap.verify_index_integrity(ids.zone).unwrap();
    snap.verify_index_integrity(ids.vehicle).unwrap();
    assert_eq!(
        ids_of(&snap.eval_spatial(ids.zone, &region).unwrap()),
        pre_zone
    );
    assert_eq!(
        ids_of(&snap.eval_spatial(ids.vehicle, &region).unwrap()),
        pre_vehicle
    );
}

// --- Property suite (tail 2.2): store-level R-tree ≡ full-scan oracle ---

/// Grid-aligned boxes, degenerate ones included.
fn small_box() -> impl Strategy<Value = Aabb> {
    ((0u8..=8), (0u8..=8), (0u8..=3), (0u8..=3)).prop_map(|(x, y, w, h)| {
        let (x, y) = (f64::from(x) * 12.5, f64::from(y) * 12.5);
        Aabb::new(x, y, x + f64::from(w) * 10.0, y + f64::from(h) * 10.0)
    })
}

#[derive(Debug, Clone)]
enum Op {
    Insert { id: u64, aabb: Aabb },
    Move { id: u64, aabb: Aabb },
    Delete { id: u64 },
    Commit,
    Rollback,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u64..=10, small_box()).prop_map(|(id, aabb)| Op::Insert { id, aabb }),
        3 => (1u64..=10, small_box()).prop_map(|(id, aabb)| Op::Move { id, aabb }),
        2 => (1u64..=10).prop_map(|id| Op::Delete { id }),
        1 => Just(Op::Commit),
        1 => Just(Op::Rollback),
    ]
}

type Model = BTreeMap<u64, Aabb>;

fn check_against_model(store: &MemStore, table: TableId, model: &Model) {
    let snap = store.snapshot();
    snap.verify_index_integrity(table).unwrap();

    let regions = [
        Aabb::new(0.0, 0.0, 130.0, 130.0),
        Aabb::new(25.0, 25.0, 60.0, 60.0),
        Aabb::new(50.0, 50.0, 50.0, 50.0),
        Aabb::new(100.0, 0.0, 110.0, 40.0),
    ];
    for q in regions {
        let got = ids_of(
            &snap
                .spatial_region(
                    table,
                    Rect::new(q.min_x, q.min_y, q.max_x - q.min_x, q.max_y - q.min_y),
                )
                .unwrap(),
        );
        let mut want: Vec<u64> = model
            .iter()
            .filter(|(_, b)| b.intersects(&q))
            .map(|(&id, _)| id)
            .collect();
        want.sort_unstable();
        assert_eq!(got, want, "region {q:?}");
    }
    for (x, y, r) in [(50.0, 50.0, 12.5), (0.0, 0.0, 30.0), (110.0, 110.0, 0.0)] {
        let got = ids_of(&snap.spatial_radius(table, x, y, r).unwrap());
        let mut want: Vec<u64> = model
            .iter()
            .filter(|(_, b)| b.min_dist2(x, y) <= r * r)
            .map(|(&id, _)| id)
            .collect();
        want.sort_unstable();
        assert_eq!(got, want, "radius ({x}, {y}, {r})");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn rtree_store_queries_equal_the_full_scan_oracle_under_random_ops(
        ops in prop::collection::vec(op_strategy(), 1..70),
        capacity in prop_oneof![Just(2usize), Just(3), Just(8)],
    ) {
        let (store, ids) = store_with_capacity(capacity);
        let mut committed: Model = BTreeMap::new();
        let mut pending: Model = BTreeMap::new();
        let mut tx = Some(store.begin());

        for op in ops {
            match op {
                Op::Insert { id, aabb } => {
                    let result = tx.as_mut().unwrap().insert(ids.zone, zone(id, aabb));
                    if let std::collections::btree_map::Entry::Vacant(slot) = pending.entry(id) {
                        prop_assert!(result.is_ok(), "{result:?}");
                        slot.insert(aabb);
                    } else {
                        prop_assert!(result.is_err()); // PK conflict
                    }
                }
                Op::Move { id, aabb } => {
                    if let std::collections::btree_map::Entry::Occupied(mut slot) =
                        pending.entry(id)
                    {
                        let t = tx.as_mut().unwrap();
                        prop_assert!(t.delete(ids.zone, &[RowValue::U64(id)]).unwrap());
                        t.insert(ids.zone, zone(id, aabb)).unwrap();
                        slot.insert(aabb);
                    }
                }
                Op::Delete { id } => {
                    let existed = pending.remove(&id).is_some();
                    let deleted = tx
                        .as_mut()
                        .unwrap()
                        .delete(ids.zone, &[RowValue::U64(id)])
                        .unwrap();
                    prop_assert_eq!(deleted, existed);
                }
                Op::Commit => {
                    tx.take().unwrap().commit().unwrap();
                    committed.clone_from(&pending);
                    check_against_model(&store, ids.zone, &committed);
                    tx = Some(store.begin());
                }
                Op::Rollback => {
                    tx.take().unwrap().rollback();
                    pending.clone_from(&committed);
                    check_against_model(&store, ids.zone, &committed);
                    tx = Some(store.begin());
                }
            }
        }

        drop(tx); // trailing rollback (STG-006)
        check_against_model(&store, ids.zone, &committed);
    }
}
