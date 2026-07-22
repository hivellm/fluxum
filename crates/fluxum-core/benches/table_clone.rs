//! Decision bench for `phase6_memstore-structural-sharing` (item 1.1).
//!
//! Question 1 — where does the O(table) commit cost live? Clones of the
//! exact key/value shapes `TableState` holds, at growing sizes:
//! `rows: BTreeMap<PkBytes, Row>`, a secondary index's
//! `BTreeMap<Vec<u8>, BTreeSet<PkBytes>>`, a unique constraint's
//! `BTreeMap<Vec<u8>, PkBytes>`.
//!
//! Question 2 — does a persistent map (imbl::OrdMap, path-copying with
//! Arc-shared chunks) pay its way? Its clone must be O(1), its writes
//! O(log n) *on a shared map* (the commit-merge case), and its reads close
//! enough to `std::BTreeMap` that the hot read path does not regress.
//!
//! Run: `cargo bench -p fluxum-core --bench table_clone`
//! The numbers recorded in the task decision came from this bench.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fluxum_core::store::{PkBytes, Row, RowValue};
use std::hint::black_box;

/// A `Task`-shaped row: (id u64, owner 32B identity-ish, title str, done).
fn row(id: u64) -> (PkBytes, Row) {
    let pk = PkBytes::from_bytes(id.to_be_bytes().to_vec());
    let row = Row::new(vec![
        RowValue::U64(id),
        RowValue::Bytes(vec![0xAB; 32]),
        RowValue::Str(format!("bench task {id}")),
        RowValue::Bool(false),
    ]);
    (pk, row)
}

fn std_rows(n: u64) -> BTreeMap<PkBytes, Row> {
    (0..n).map(row).collect()
}

fn imbl_rows(n: u64) -> imbl::OrdMap<PkBytes, Row> {
    (0..n).map(row).collect()
}

/// The three committed shapes cloned by `Arc::make_mut` today (question 1).
fn clone_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("std_clone");
    for n in [10_000u64, 100_000, 1_000_000] {
        let rows = std_rows(n);
        group.bench_with_input(BenchmarkId::new("rows", n), &rows, |b, rows| {
            b.iter(|| black_box(rows.clone()))
        });
    }
    // Secondary index and unique-map shapes at one representative size.
    let n = 100_000u64;
    let index: BTreeMap<Vec<u8>, BTreeSet<PkBytes>> = (0..n)
        .map(|i| {
            let (pk, _) = row(i);
            (i.to_be_bytes().to_vec(), BTreeSet::from([pk]))
        })
        .collect();
    group.bench_with_input(BenchmarkId::new("btree_index", n), &index, |b, index| {
        b.iter(|| black_box(index.clone()))
    });
    let unique: BTreeMap<Vec<u8>, PkBytes> = (0..n)
        .map(|i| (i.to_be_bytes().to_vec(), row(i).0))
        .collect();
    group.bench_with_input(BenchmarkId::new("unique", n), &unique, |b, unique| {
        b.iter(|| black_box(unique.clone()))
    });
    group.finish();
}

/// The candidate's costs (question 2).
fn imbl_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("imbl");
    for n in [100_000u64, 1_000_000] {
        let rows = imbl_rows(n);
        group.bench_with_input(BenchmarkId::new("clone", n), &rows, |b, rows| {
            b.iter(|| black_box(rows.clone()))
        });
        // The commit-merge case: insert into a map whose chunks are shared
        // with another live snapshot — every touched path must copy.
        group.bench_with_input(BenchmarkId::new("shared_insert", n), &rows, |b, rows| {
            let snapshot = rows.clone(); // keeps every chunk shared
            let (pk, new_row) = row(n + 1);
            b.iter_batched(
                || rows.clone(),
                |mut map| {
                    map.insert(pk.clone(), new_row.clone());
                    black_box(map)
                },
                criterion::BatchSize::SmallInput,
            );
            drop(snapshot);
        });
    }

    // Read-path parity vs std at 100k.
    let n = 100_000u64;
    let std_map = std_rows(n);
    let imbl_map = imbl_rows(n);
    let probe = row(n / 2).0;
    group.bench_function("std_get_100k", |b| {
        b.iter(|| black_box(std_map.get(&probe)))
    });
    group.bench_function("imbl_get_100k", |b| {
        b.iter(|| black_box(imbl_map.get(&probe)))
    });
    let lo = row(1_000).0;
    group.bench_function("std_range100_100k", |b| {
        b.iter(|| {
            black_box(
                std_map
                    .range(lo.clone()..)
                    .take(100)
                    .map(|(_, r)| r.values().len())
                    .sum::<usize>(),
            )
        })
    });
    group.bench_function("imbl_range100_100k", |b| {
        b.iter(|| {
            black_box(
                imbl_map
                    .range(lo.clone()..)
                    .take(100)
                    .map(|(_, r)| r.values().len())
                    .sum::<usize>(),
            )
        })
    });
    group.bench_function("std_iter_100k", |b| {
        b.iter(|| black_box(std_map.values().map(|r| r.values().len()).sum::<usize>()))
    });
    group.bench_function("imbl_iter_100k", |b| {
        b.iter(|| {
            black_box(
                imbl_map
                    .iter()
                    .map(|(_, r)| r.values().len())
                    .sum::<usize>(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, clone_cost, imbl_cost);
criterion_main!(benches);
