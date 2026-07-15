//! NFR-03 benchmark (T3.1 item 1.6, SPEC-003 acceptance 10): commit p99 of
//! the `update_reading` small-write reducer must be < 1 ms with async log
//! writes enabled (the default: `CommitLog::append` hands off to the
//! group-commit flush actor and never waits for fsync).
//!
//! The measured path is the full pipeline cycle: submit → begin → upsert →
//! commit merge (copy-on-write of the touched table) → commit-log enqueue →
//! respond. A p50/p99/max summary over individual commits is printed before
//! the criterion run, since criterion reports mean/median only.
#![allow(clippy::unwrap_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

static READING_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "sensor_id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "updated_at",
        ty: FluxType::I64,
    },
];

static READING: TableSchema = TableSchema {
    name: "Reading",
    columns: READING_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// Committed rows in the touched table. The T2.1 commit merge deep-clones
/// the touched table's key map (the documented Phase-2 copy-on-write
/// trade-off), so this sizes the per-commit merge cost.
const ROWS: u64 = 1_000;
/// Individual samples for the p99 summary.
const SAMPLES: usize = 2_000;

fn reading(sensor_id: u64, value: f64) -> Vec<RowValue> {
    vec![
        RowValue::U64(sensor_id),
        RowValue::F64(value),
        RowValue::I64(1_720_000_000),
    ]
}

fn txn_commit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::new(&Schema::from_tables([&READING]).unwrap()).unwrap());
    let log = Arc::new(CommitLog::open(dir.path(), 0, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let _worker = rt.spawn(worker.run());
    let rid = store.table_id("Reading").unwrap();

    // Prepopulate the sensor rows.
    rt.block_on(pipeline.call(Box::new(move |tx| {
        for id in 0..ROWS {
            tx.insert(rid, reading(id, 20.0))?;
        }
        Ok(())
    })))
    .unwrap();

    // `update_reading`: the canonical small-write reducer — one committed
    // row replaced by primary key.
    let commit_once = |i: u64| {
        rt.block_on(pipeline.call(Box::new(move |tx| {
            tx.upsert(rid, reading(i % ROWS, 20.0 + (i % 80) as f64 / 8.0))?;
            Ok(())
        })))
        .unwrap()
    };

    // NFR-03 summary: individual commit latencies, p50/p99/max.
    let mut latencies = Vec::with_capacity(SAMPLES);
    for i in 0..SAMPLES as u64 {
        let start = Instant::now();
        black_box(commit_once(i));
        latencies.push(start.elapsed());
    }
    latencies.sort_unstable();
    let pct = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let p50 = pct(0.50);
    let p99 = pct(0.99);
    let max = latencies[latencies.len() - 1];
    println!(
        "NFR-03 update_reading commit latency over {SAMPLES} commits \
         ({ROWS}-row table, async log writes): p50={p50:?} p99={p99:?} max={max:?} \
         — target p99 < 1 ms"
    );
    assert!(
        p99 < Duration::from_millis(1),
        "NFR-03 violated: commit p99 {p99:?} >= 1 ms"
    );

    let mut i = SAMPLES as u64;
    c.bench_function("txn_commit_update_reading_1k", |b| {
        b.iter(|| {
            i += 1;
            black_box(commit_once(i))
        });
    });
}

criterion_group!(benches, txn_commit);
criterion_main!(benches);
