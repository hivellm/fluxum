//! T2.6 spatial benchmark (SPEC-008 acceptance 2, FR-61/FR-62): with
//! 1,000,000 indexed rows, an `IN REGION` query selecting on the order of
//! 1,000 rows must run **at least 10× faster** through the spatial index
//! than an O(n) full scan evaluating the same predicate, and latency must
//! scale consistently with O(log n + k) as n grows (compare the `n=100k`
//! and `n=1M` groups: ~constant-factor growth, not the 10× of a scan).
//!
//! Run: `cargo bench -p fluxum-core --bench spatial`
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use fluxum_core::index::{Aabb, Rect, SpatialPredicate};
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, StoreOptions, TableId};

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

const WORLD: f64 = 1000.0;

/// Deterministic pseudo-uniform coordinate stream (splitmix64).
struct Coords(u64);

impl Coords {
    fn next_f64(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64 * WORLD
    }
}

/// A store with `n` uniformly placed Vehicle points.
fn point_store(n: u64) -> (MemStore, TableId) {
    let schema = Schema::from_tables([&VEHICLE]).expect("schema assembles");
    let store = MemStore::with_options(
        &schema,
        StoreOptions {
            spatial_bounds: Rect::new(0.0, 0.0, WORLD, WORLD),
            ..StoreOptions::default()
        },
    )
    .expect("store builds");
    let table = store.table_id("Vehicle").expect("table registered");
    let mut coords = Coords(7);
    let mut tx = store.begin();
    for id in 0..n {
        let (x, y) = (coords.next_f64(), coords.next_f64());
        tx.insert(
            table,
            vec![RowValue::U64(id), RowValue::F64(x), RowValue::F64(y)],
        )
        .expect("insert");
    }
    tx.commit().expect("commit");
    (store, table)
}

/// A store with `n` small Zone boxes (side ≤ 1.0) at uniform positions.
fn box_store(n: u64) -> (MemStore, TableId) {
    let schema = Schema::from_tables([&ZONE]).expect("schema assembles");
    let store = MemStore::new(&schema).expect("store builds");
    let table = store.table_id("Zone").expect("table registered");
    let mut coords = Coords(11);
    let mut tx = store.begin();
    for id in 0..n {
        let (x, y) = (coords.next_f64(), coords.next_f64());
        let (w, h) = (coords.next_f64() / WORLD, coords.next_f64() / WORLD);
        tx.insert(
            table,
            vec![
                RowValue::U64(id),
                RowValue::F64(x),
                RowValue::F64(y),
                RowValue::F64(x + w),
                RowValue::F64(y + h),
            ],
        )
        .expect("insert");
    }
    tx.commit().expect("commit");
    (store, table)
}

/// `IN REGION` box sized to select ~1000 of `n` uniform rows.
fn region_for(n: u64) -> SpatialPredicate {
    let side = (1000.0 / n as f64).sqrt() * WORLD;
    SpatialPredicate::InRegion {
        x: (WORLD - side) / 2.0,
        y: (WORLD - side) / 2.0,
        w: side,
        h: side,
    }
}

fn bench_in_region(c: &mut Criterion) {
    for n in [100_000u64, 1_000_000] {
        let (store, table) = point_store(n);
        let snap = store.snapshot();
        let predicate = region_for(n);
        let SpatialPredicate::InRegion { x, y, w, h } = predicate else {
            unreachable!()
        };
        let hits = snap.eval_spatial(table, &predicate).expect("query").len();
        assert!(
            (200..5000).contains(&hits),
            "region should select ~1000 rows, got {hits}"
        );

        let mut group = c.benchmark_group(format!("in_region_quadtree_n{n}"));
        group.sample_size(20);
        // The SPX-023 path: resolved via the spatial index.
        group.bench_function("index", |b| {
            b.iter(|| {
                let rows = snap
                    .eval_spatial(table, black_box(&predicate))
                    .expect("query");
                black_box(rows.len())
            });
        });
        // The forbidden baseline an O(n) engine would run (what SpacetimeDB
        // does today): full scan + per-row predicate.
        group.bench_function("full_scan", |b| {
            b.iter(|| {
                let count = snap
                    .scan(table)
                    .expect("scan")
                    .filter(|row| {
                        let (Some(RowValue::F64(px)), Some(RowValue::F64(py))) =
                            (row.value(1), row.value(2))
                        else {
                            return false;
                        };
                        *px >= x && *px <= x + w && *py >= y && *py <= y + h
                    })
                    .count();
                black_box(count)
            });
        });
        group.finish();
    }
}

fn bench_in_region_rtree(c: &mut Criterion) {
    let n = 1_000_000u64;
    let (store, table) = box_store(n);
    let snap = store.snapshot();
    let predicate = region_for(n);
    let SpatialPredicate::InRegion { x, y, w, h } = predicate else {
        unreachable!()
    };
    let query = Aabb::new(x, y, x + w, y + h);
    let hits = snap.eval_spatial(table, &predicate).expect("query").len();
    assert!(
        (200..5000).contains(&hits),
        "region should select ~1000 boxes, got {hits}"
    );

    let mut group = c.benchmark_group(format!("in_region_rtree_n{n}"));
    group.sample_size(20);
    group.bench_function("index", |b| {
        b.iter(|| {
            let rows = snap
                .eval_spatial(table, black_box(&predicate))
                .expect("query");
            black_box(rows.len())
        });
    });
    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let count = snap
                .scan(table)
                .expect("scan")
                .filter(|row| {
                    let (
                        Some(RowValue::F64(a)),
                        Some(RowValue::F64(bb)),
                        Some(RowValue::F64(cc)),
                        Some(RowValue::F64(d)),
                    ) = (row.value(1), row.value(2), row.value(3), row.value(4))
                    else {
                        return false;
                    };
                    Aabb::new(*a, *bb, *cc, *d).intersects(&query)
                })
                .count();
            black_box(count)
        });
    });
    group.finish();
}

fn bench_within_radius(c: &mut Criterion) {
    let n = 1_000_000u64;
    let (store, table) = point_store(n);
    let snap = store.snapshot();
    // Radius selecting ~1000 rows: π r² / WORLD² ≈ 1000 / n.
    let r = (1000.0 / n as f64 / std::f64::consts::PI).sqrt() * WORLD;
    let predicate = SpatialPredicate::WithinRadius {
        x: WORLD / 2.0,
        y: WORLD / 2.0,
        r,
    };
    let mut group = c.benchmark_group(format!("within_radius_quadtree_n{n}"));
    group.sample_size(20);
    group.bench_function("index", |b| {
        b.iter(|| {
            let rows = snap
                .eval_spatial(table, black_box(&predicate))
                .expect("query");
            black_box(rows.len())
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_in_region,
    bench_in_region_rtree,
    bench_within_radius
);
criterion_main!(benches);
