//! T3.1 verification suite (SPEC-003 acceptance 2/3/4/5/6-lite/9; DAG exit
//! test): the validate → merge → append → respond pipeline, rollback and
//! panic isolation, gap-free `tx_id` across commits/rollbacks and recovery,
//! PK/`#[unique]`/auto-inc constraints with upsert semantics, immediate
//! `503 "shard busy"` backpressure, and the concurrent-read /
//! sequential-write harness.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, TxRecord, replay};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

const SHARD: u32 = 5;

// --- Hand-built static schemas (macro output stand-ins, as in store_acid) --

static ACCOUNT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "email",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "balance",
        ty: FluxType::I64,
    },
];

/// Auto-inc PK + a single-column `#[unique]` constraint on `email`.
static ACCOUNT: TableSchema = TableSchema {
    name: "Account",
    columns: ACCOUNT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[&[1]],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static SLOT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "room",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "number",
        ty: FluxType::U32,
    },
];

/// Multi-column `#[unique]` constraint on `(room, number)` (DM-006).
static SLOT: TableSchema = TableSchema {
    name: "Slot",
    columns: SLOT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[&[1, 2]],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F64,
    },
];

/// Composite PK (TXN-040 single + composite coverage).
static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0, 1],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static COUNTER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::U64,
    },
];

static COUNTER: TableSchema = TableSchema {
    name: "Counter",
    columns: COUNTER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static EVENT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "client",
        ty: FluxType::U32,
    },
];

static EVENT: TableSchema = TableSchema {
    name: "Event",
    columns: EVENT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn mem_store() -> Arc<MemStore> {
    let schema = Schema::from_tables([&ACCOUNT, &SLOT, &SENSOR, &COUNTER, &EVENT]).unwrap();
    Arc::new(MemStore::new(&schema).unwrap())
}

/// A running pipeline over a fresh store and a log in `dir`.
fn pipeline_in(
    dir: &std::path::Path,
    options: TxPipelineOptions,
) -> (Arc<MemStore>, TxPipeline, tokio::task::JoinHandle<()>) {
    let store = mem_store();
    let log = Arc::new(CommitLog::open(dir, SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) = TxPipeline::new(Arc::clone(&store), log, options).unwrap();
    let worker = tokio::spawn(worker.run());
    (store, pipeline, worker)
}

fn account(email: &str, balance: i64) -> Vec<RowValue> {
    vec![
        RowValue::U64(0),
        RowValue::Str(email.into()),
        RowValue::I64(balance),
    ]
}

fn account_with_id(id: u64, email: &str, balance: i64) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Str(email.into()),
        RowValue::I64(balance),
    ]
}

fn slot(id: u64, room: u32, number: u32) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::U32(room),
        RowValue::U32(number),
    ]
}

fn sensor(x: i32, y: i32, reading: f64) -> Vec<RowValue> {
    vec![RowValue::I32(x), RowValue::I32(y), RowValue::F64(reading)]
}

/// Every logged record of shard `SHARD` in `dir`, in order.
fn logged_records(dir: &std::path::Path) -> Vec<TxRecord> {
    let mut records = Vec::new();
    let report = replay(dir, SHARD, |_, record| {
        records.push(record);
        Ok(())
    })
    .unwrap();
    assert!(report.corruption.is_none());
    records
}

// --- The pipeline: validate → merge → append → respond (TXN-021) ----------

#[tokio::test(flavor = "multi_thread")]
async fn commit_pipeline_merges_appends_and_responds() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();

    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("ana@example.com", 100))?;
            tx.insert(aid, account("bo@example.com", 250))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Respond: the receipt carries the tx id and the full diff (the
    // SPEC-005 seam).
    assert_eq!(receipt.tx_id, 1);
    assert_eq!(receipt.diff.tables.len(), 1);
    assert_eq!(receipt.diff.tables[0].inserts.len(), 2);

    // Merge: committed and visible to lock-free readers.
    let snap = store.snapshot();
    assert_eq!(snap.row_count(aid).unwrap(), 2);
    let ana = snap.query_pk(aid, &[RowValue::U64(1)]).unwrap().unwrap();
    assert_eq!(ana.value(1), Some(&RowValue::Str("ana@example.com".into())));

    // Append: durable in the commit log (TXN-004), gap-free from tx 1.
    pipeline.log().wait_durable(receipt.tx_id).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let records = logged_records(dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tx_id, 1);
    assert_eq!(records[0].shard_id, SHARD);
    assert_eq!(records[0].mutations.len(), 1);
    assert_eq!(records[0].mutations[0].inserts.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_reducer_rolls_back_and_writes_no_log_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();
    let before = store.snapshot();

    // TXN-001 scenario: rows inserted before the error must not survive.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("ghost@example.com", 1))?;
            Err(fluxum_core::FluxumError::Storage("business rule".into()))
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("business rule"), "{err}");

    // Rollback is pure TxState discard: the published state is the *same*
    // state (pointer identity), not an equal reconstruction (TXN-022).
    assert!(before.same_state(&store.snapshot()));

    // The rolled-back call consumed no tx id (TXN-030)...
    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("real@example.com", 1))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, 1);

    // ...and left no trace in the log (TXN-022 step 4).
    pipeline.log().wait_durable(1).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let records = logged_records(dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tx_id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn panicking_reducer_rolls_back_and_the_shard_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();
    let before = store.snapshot();

    // Silence the default panic hook for the deliberate panic below; this
    // test binary's other tests do not panic on purpose.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("doomed@example.com", 1))?;
            panic!("deliberate reducer bug");
        }))
        .await
        .unwrap_err();
    std::panic::set_hook(hook);

    // Same rollback as an Err (TXN-022), reported as a wire-ready 500.
    assert_eq!(err.query_code(), Some(500));
    assert!(err.to_string().contains("reducer panicked"), "{err}");
    assert!(err.to_string().contains("deliberate reducer bug"), "{err}");
    assert!(before.same_state(&store.snapshot()));

    // The worker survived and processes subsequent calls (acceptance 9).
    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("alive@example.com", 1))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, 1);
    assert_eq!(store.snapshot().row_count(aid).unwrap(), 1);
}

// --- tx_id: gap-free, monotonic, recovery-resumed (TXN-030) ---------------

#[tokio::test(flavor = "multi_thread")]
async fn tx_ids_are_gap_free_across_commits_and_rollbacks() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let sid = store.table_id("Sensor").unwrap();

    let mut receipts = Vec::new();
    for i in 0..5i32 {
        // A rollback between every pair of commits.
        let bad = pipeline
            .call(Box::new(move |tx| {
                tx.insert(sid, sensor(i, -1, 0.0))?;
                Err(fluxum_core::FluxumError::Storage("nope".into()))
            }))
            .await;
        assert!(bad.is_err());
        let receipt = pipeline
            .call(Box::new(move |tx| {
                tx.insert(sid, sensor(i, i, f64::from(i)))?;
                Ok(())
            }))
            .await
            .unwrap();
        receipts.push(receipt.tx_id);
    }
    assert_eq!(receipts, vec![1, 2, 3, 4, 5]);

    pipeline.log().wait_durable(5).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let logged: Vec<u64> = logged_records(dir.path()).iter().map(|r| r.tx_id).collect();
    assert_eq!(logged, vec![1, 2, 3, 4, 5], "gap-free, strictly increasing");
}

#[tokio::test(flavor = "multi_thread")]
async fn tx_id_resumes_at_last_replayed_plus_one_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let snap_dir = dir.path().join("snapshots");

    // Session 1: three commits, durable, then shut down.
    {
        let (store, pipeline, worker) = pipeline_in(&log_dir, TxPipelineOptions::default());
        let sid = store.table_id("Sensor").unwrap();
        for i in 1..=3i32 {
            pipeline
                .call(Box::new(move |tx| {
                    tx.insert(sid, sensor(i, i, f64::from(i)))?;
                    Ok(())
                }))
                .await
                .unwrap();
        }
        pipeline.log().wait_durable(3).await.unwrap();
        drop(pipeline);
        worker.await.unwrap();
    }

    // Session 2: recover (checkpoint-less: full log replay) and continue.
    let store = mem_store();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    let outcome = recover(&store, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.last_tx_id, Some(3));
    assert_eq!(outcome.next_tx_id, 4, "TXN-030: last_replayed_tx_id + 1");

    let log = Arc::new(CommitLog::open(&log_dir, SHARD, 1, CommitLogOptions::default()).unwrap());
    assert_eq!(log.recovery().last_tx_id, Some(3));
    let (pipeline, worker) = TxPipeline::new(store, log, TxPipelineOptions::default()).unwrap();
    let worker = tokio::spawn(worker.run());
    let sid = pipeline.store().table_id("Sensor").unwrap();
    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.insert(sid, sensor(9, 9, 9.0))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, 4);

    pipeline.log().wait_durable(4).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let logged: Vec<u64> = logged_records(&log_dir).iter().map(|r| r.tx_id).collect();
    assert_eq!(logged, vec![1, 2, 3, 4]);
}

// --- Constraints (TXN-040, TXN-041, TXN-042; acceptance 2/3) ---------------

#[tokio::test(flavor = "multi_thread")]
async fn pk_conflicts_roll_back_with_descriptive_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();
    let sid = store.table_id("Sensor").unwrap();

    pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account_with_id(7, "a@example.com", 0))?;
            tx.insert(sid, sensor(-2, 9, 1.0))?;
            Ok(())
        }))
        .await
        .unwrap();
    let before = store.snapshot();

    // Single-column PK conflict (TXN-040 error shape).
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account_with_id(7, "other@example.com", 0))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "storage error: primary key conflict: table=Account pk=(7)"
    );

    // Composite PK conflict.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(sid, sensor(-2, 9, 2.0))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "storage error: primary key conflict: table=Sensor pk=(-2, 9)"
    );

    assert!(before.same_state(&store.snapshot()));
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_constraints_are_enforced_single_and_composite() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();
    let lid = store.table_id("Slot").unwrap();

    pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("taken@example.com", 0))?;
            tx.insert(lid, slot(1, 10, 4))?;
            Ok(())
        }))
        .await
        .unwrap();
    let before = store.snapshot();

    // Single-column violation against a committed row (TXN-041).
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("taken@example.com", 99))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "storage error: unique constraint violation: table=Account columns=(email) \
         value=(\"taken@example.com\")"
    );

    // Composite violation; a row differing in one column is fine.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(lid, slot(2, 10, 4))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("unique constraint violation: table=Slot columns=(room, number)"),
        "{err}"
    );
    assert!(before.same_state(&store.snapshot()));
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(lid, slot(2, 10, 5))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Same-transaction pending conflict: the whole call rolls back, so the
    // first (valid) insert vanishes too (TXN-001).
    let mid = store.snapshot();
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("dup@example.com", 1))?;
            tx.insert(aid, account("dup@example.com", 2))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unique constraint violation"),
        "{err}"
    );
    assert!(mid.same_state(&store.snapshot()));
}

#[tokio::test(flavor = "multi_thread")]
async fn unique_values_can_move_within_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();

    pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account_with_id(1, "left@example.com", 0))?;
            tx.insert(aid, account_with_id(2, "right@example.com", 0))?;
            Ok(())
        }))
        .await
        .unwrap();

    // A value freed by a tx-delete is claimable in the same transaction.
    pipeline
        .call(Box::new(move |tx| {
            tx.delete(aid, &[RowValue::U64(1)])?;
            tx.insert(aid, account_with_id(3, "left@example.com", 5))?;
            Ok(())
        }))
        .await
        .unwrap();

    // ...but while the owner is still visible, it is a violation.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account_with_id(4, "right@example.com", 5))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unique constraint violation"),
        "{err}"
    );

    // Swap two rows' unique values in one transaction (delete both, then
    // reinsert crossed) — exercises the two-pass merge maintenance.
    pipeline
        .call(Box::new(move |tx| {
            tx.delete(aid, &[RowValue::U64(2)])?;
            tx.delete(aid, &[RowValue::U64(3)])?;
            tx.insert(aid, account_with_id(2, "left@example.com", 0))?;
            tx.insert(aid, account_with_id(3, "right@example.com", 5))?;
            Ok(())
        }))
        .await
        .unwrap();
    let snap = store.snapshot();
    let two = snap.query_pk(aid, &[RowValue::U64(2)]).unwrap().unwrap();
    assert_eq!(
        two.value(1),
        Some(&RowValue::Str("left@example.com".into()))
    );
    snap.verify_index_integrity(aid).unwrap();

    // An upsert that moves a value frees the old one for another row in the
    // same transaction.
    pipeline
        .call(Box::new(move |tx| {
            tx.upsert(aid, account_with_id(3, "moved@example.com", 5))?;
            tx.insert(aid, account_with_id(9, "right@example.com", 1))?;
            Ok(())
        }))
        .await
        .unwrap();
    let snap = store.snapshot();
    assert!(snap.query_pk(aid, &[RowValue::U64(9)]).unwrap().is_some());
    snap.verify_index_integrity(aid).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_replaces_instead_of_erroring() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let lid = store.table_id("Slot").unwrap();

    pipeline
        .call(Box::new(move |tx| {
            tx.insert(lid, slot(1, 10, 4))?;
            Ok(())
        }))
        .await
        .unwrap();

    // TXN-040 exception: an occupied PK replaces the row.
    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.upsert(lid, slot(1, 20, 6))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(receipt.diff.tables[0].inserts.len(), 1);
    assert_eq!(receipt.diff.tables[0].deletes.len(), 1);
    let row = store
        .snapshot()
        .query_pk(lid, &[RowValue::U64(1)])
        .unwrap()
        .unwrap();
    assert_eq!(row.value(1), Some(&RowValue::U32(20)));

    // Upsert with identical content is a structural no-op (empty diff).
    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.upsert(lid, slot(1, 20, 6))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert!(receipt.diff.is_empty(), "{:?}", receipt.diff);

    // Upsert over this transaction's own pending insert replaces it.
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(lid, slot(2, 30, 1))?;
            tx.upsert(lid, slot(2, 30, 2))?;
            Ok(())
        }))
        .await
        .unwrap();
    let row = store
        .snapshot()
        .query_pk(lid, &[RowValue::U64(2)])
        .unwrap()
        .unwrap();
    assert_eq!(row.value(2), Some(&RowValue::U32(2)));

    // Upsert still enforces #[unique] against other rows (TXN-041).
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.upsert(lid, slot(3, 20, 6))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unique constraint violation"),
        "{err}"
    );

    // Plain insert on an occupied PK still conflicts.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(lid, slot(1, 40, 1))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("primary key conflict"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_inc_ids_are_visible_pre_commit_and_never_reused_after_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path(), TxPipelineOptions::default());
    let aid = store.table_id("Account").unwrap();

    // TXN-042: the id is observable inside the reducer, before commit.
    let seen = Arc::new(AtomicU64::new(0));
    let seen_in_reducer = Arc::clone(&seen);
    pipeline
        .call(Box::new(move |tx| {
            let row = tx.insert(aid, account("first@example.com", 0))?;
            let Some(&RowValue::U64(id)) = row.value(0) else {
                return Err(fluxum_core::FluxumError::Storage("no id".into()));
            };
            seen_in_reducer.store(id, Ordering::SeqCst);
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    // A rolled-back transaction consumes ids 2 and 3; they are not reused.
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(aid, account("gone@example.com", 0))?;
            tx.insert(aid, account("gone2@example.com", 0))?;
            Err(fluxum_core::FluxumError::Storage("rolled back".into()))
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rolled back"), "{err}");

    let seen_next = Arc::new(AtomicU64::new(0));
    let seen_in_reducer = Arc::clone(&seen_next);
    pipeline
        .call(Box::new(move |tx| {
            let row = tx.insert(aid, account("second@example.com", 0))?;
            let Some(&RowValue::U64(id)) = row.value(0) else {
                return Err(fluxum_core::FluxumError::Storage("no id".into()));
            };
            seen_in_reducer.store(id, Ordering::SeqCst);
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(
        seen_next.load(Ordering::SeqCst),
        4,
        "ids 2 and 3 were consumed by the rollback and never reused (TXN-042)"
    );
    let snap = store.snapshot();
    assert!(snap.query_pk(aid, &[RowValue::U64(2)]).unwrap().is_none());
    assert!(snap.query_pk(aid, &[RowValue::U64(3)]).unwrap().is_none());
}

// --- Backpressure (TXN-011; acceptance 4) ----------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_queue_answers_503_shard_busy_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) =
        pipeline_in(dir.path(), TxPipelineOptions { queue_capacity: 1 });
    let sid = store.table_id("Sensor").unwrap();

    // A reducer that parks the single writer until released.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let blocker = pipeline
        .submit(Box::new(move |_tx| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        }))
        .unwrap();
    // Wait until the writer is inside the blocker: the queue is now empty
    // and the worker busy.
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();

    // Fill the queue to capacity (1)...
    let queued = pipeline
        .submit(Box::new(move |tx| {
            tx.insert(sid, sensor(1, 1, 1.0))?;
            Ok(())
        }))
        .unwrap();

    // ...and the next submission is rejected immediately, without blocking.
    let start = std::time::Instant::now();
    let err = pipeline
        .submit(Box::new(move |tx| {
            tx.insert(sid, sensor(2, 2, 2.0))?;
            Ok(())
        }))
        .unwrap_err();
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "rejection must be immediate, took {:?}",
        start.elapsed()
    );
    assert_eq!(err.query_code(), Some(503));
    assert_eq!(err.to_string(), "query error 503: shard busy");

    // Release the writer: everything accepted still commits, in order.
    release_tx.send(()).unwrap();
    let first = blocker.await.unwrap().unwrap();
    let second = queued.await.unwrap().unwrap();
    assert_eq!(first.tx_id, 1);
    assert_eq!(second.tx_id, 2);
}

// --- DAG exit test: concurrent reads / sequential writes (acceptance 4) ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_never_block_while_writes_serialize() {
    const CLIENTS: u32 = 8;
    const CALLS_PER_CLIENT: u32 = 25;
    const TOTAL: u64 = (CLIENTS * CALLS_PER_CLIENT) as u64;

    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(
        dir.path(),
        TxPipelineOptions {
            queue_capacity: 4 * CLIENTS as usize * CALLS_PER_CLIENT as usize,
        },
    );
    let cid = store.table_id("Counter").unwrap();
    let eid = store.table_id("Event").unwrap();

    // Seed the counter row.
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(cid, vec![RowValue::U32(0), RowValue::U64(0)])?;
            Ok(())
        }))
        .await
        .unwrap();

    // Concurrent readers on plain OS threads: lock-free snapshots, always a
    // consistent state — counter == committed increments == event rows
    // (each write transaction changes both together, atomically).
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let ready = ready_tx.clone();
            thread::spawn(move || {
                let mut observed = 0u64;
                let mut last_value = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let snap = store.snapshot();
                    let row = snap.query_pk(cid, &[RowValue::U32(0)]).unwrap();
                    let value = match row.as_ref().and_then(|r| r.value(1)) {
                        Some(&RowValue::U64(v)) => v,
                        _ => 0,
                    };
                    let events = snap.row_count(eid).unwrap() as u64;
                    assert_eq!(
                        value, events,
                        "reader observed a partial transaction: counter {value} vs \
                         {events} events"
                    );
                    assert!(value >= last_value, "committed history went backwards");
                    last_value = value;
                    observed += 1;
                    if observed == 1 {
                        ready.send(()).expect("main task is waiting");
                    }
                }
                observed
            })
        })
        .collect();
    drop(ready_tx);
    // Don't start the clients until every reader has observed at least one
    // state — otherwise a fast writer can finish the whole workload before
    // a reader thread is even scheduled (seen on Windows CI; same guard as
    // the store_acid harness).
    tokio::task::spawn_blocking(move || {
        for _ in 0..4 {
            ready_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("all readers observe the initial state");
        }
    })
    .await
    .unwrap();

    // N concurrent clients firing read-modify-write reducers at the shard.
    let clients: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let pipeline = pipeline.clone();
            tokio::spawn(async move {
                let mut tx_ids = Vec::with_capacity(CALLS_PER_CLIENT as usize);
                for _ in 0..CALLS_PER_CLIENT {
                    let receipt = pipeline
                        .call(Box::new(move |tx| {
                            // Default reads see the committed snapshot at
                            // begin (TXN-050); the single writer makes this
                            // read-modify-write safe with no lost updates.
                            let row = tx.query_pk(cid, &[RowValue::U32(0)])?;
                            let value = match row.as_ref().and_then(|r| r.value(1)) {
                                Some(&RowValue::U64(v)) => v,
                                _ => {
                                    return Err(fluxum_core::FluxumError::Storage(
                                        "counter row missing".into(),
                                    ));
                                }
                            };
                            tx.upsert(cid, vec![RowValue::U32(0), RowValue::U64(value + 1)])?;
                            tx.insert(eid, vec![RowValue::U64(0), RowValue::U32(client)])?;
                            Ok(())
                        }))
                        .await
                        .unwrap();
                    tx_ids.push(receipt.tx_id);
                }
                tx_ids
            })
        })
        .collect();

    let mut all_tx_ids = Vec::new();
    for client in clients {
        let tx_ids = client.await.unwrap();
        // Each client's own receipts arrive in submission order.
        assert!(tx_ids.windows(2).all(|w| w[0] < w[1]));
        all_tx_ids.extend(tx_ids);
    }
    stop.store(true, Ordering::Relaxed);

    // Serial commit history: the union of receipts is exactly tx 2..=TOTAL+1
    // (tx 1 seeded the counter) with no gaps or duplicates (TXN-010/030).
    all_tx_ids.sort_unstable();
    let expected: Vec<u64> = (2..=TOTAL + 1).collect();
    assert_eq!(all_tx_ids, expected);

    // No lost updates: every increment landed.
    let snap = store.snapshot();
    let row = snap.query_pk(cid, &[RowValue::U32(0)]).unwrap().unwrap();
    assert_eq!(row.value(1), Some(&RowValue::U64(TOTAL)));
    assert_eq!(snap.row_count(eid).unwrap() as u64, TOTAL);

    // Readers ran throughout without ever blocking the writer (or being
    // blocked): every thread made progress and saw only consistent states.
    for reader in readers {
        let observed = reader.join().expect("reader thread panicked");
        assert!(observed > 0);
    }

    // And the log agrees: gap-free 1..=TOTAL+1 (acceptance 5).
    pipeline.log().wait_durable(TOTAL + 1).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let logged: Vec<u64> = logged_records(dir.path()).iter().map(|r| r.tx_id).collect();
    let expected: Vec<u64> = (1..=TOTAL + 1).collect();
    assert_eq!(logged, expected);
}
