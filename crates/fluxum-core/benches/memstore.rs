//! NFR-02 benchmark (T2.1 item 1.5): committed-state point lookup on a hot
//! row must complete in < 1 µs. In T2.1 every committed row is hot (the
//! buffer pool / cold tier land in T2.4+ per SPEC-015), so this measures the
//! logical `CommittedState` lookup path: lock-free snapshot load + FluxBIN
//! PK encode + `BTreeMap` probe.
#![allow(clippy::unwrap_used)]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
];

static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

const ROWS: u64 = 100_000;

fn committed_point_lookup(c: &mut Criterion) {
    let schema = Schema::from_tables([&USER]).unwrap();
    let store = MemStore::new(&schema).unwrap();
    let uid = store.table_id("User").unwrap();

    let mut tx = store.begin();
    for i in 0..ROWS {
        tx.insert(
            uid,
            vec![RowValue::U64(0), RowValue::Str(format!("user-{i}"))],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    // Hot-path shape: reader holds a snapshot and probes by PK.
    let snap = store.snapshot();
    let mut key = 1u64;
    c.bench_function("committed_point_lookup_hot_100k", |b| {
        b.iter(|| {
            key = key % ROWS + 1; // walk the keyspace; ids are 1..=ROWS
            let row = snap
                .query_pk(uid, &[RowValue::U64(black_box(key))])
                .unwrap();
            debug_assert!(row.is_some());
            black_box(row)
        });
    });

    // Including the wait-free snapshot load per lookup (the shape a view
    // without a held snapshot would use).
    c.bench_function("committed_point_lookup_hot_100k_with_snapshot_load", |b| {
        b.iter(|| {
            key = key % ROWS + 1;
            let snap = store.snapshot();
            black_box(
                snap.query_pk(uid, &[RowValue::U64(black_box(key))])
                    .unwrap(),
            )
        });
    });
}

criterion_group!(benches, committed_point_lookup);
criterion_main!(benches);
