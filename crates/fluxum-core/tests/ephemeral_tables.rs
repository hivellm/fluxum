//! SPEC-023 DMX-010/012 — ephemeral (memory-only) tables: writes fan out to
//! subscribers but never reach the commit log or a checkpoint, so an ephemeral
//! table starts empty after a restart. `expire_after` / owner-disconnect
//! cleanup (DMX-011) is a separate increment.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::checkpoint::{CheckpointRepo, SnapshotWorker, WorkerOptions, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, TxRecord, replay};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

const SHARD: u32 = 7;

// A durable, client-visible table.
static ROOM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
];
static ROOM: TableSchema = TableSchema {
    name: "Room",
    columns: ROOM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

// An ephemeral table (memory-only, client-visible) — a live cursor keyed by
// the owner's connection id.
static CURSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "conn",
        ty: FluxType::ConnectionId,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::I32,
    },
];
static CURSOR: TableSchema = TableSchema {
    name: "Cursor",
    columns: CURSOR_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Ephemeral,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn mem_store() -> Arc<MemStore> {
    let schema = Schema::from_tables([&ROOM, &CURSOR]).unwrap();
    Arc::new(MemStore::new(&schema).unwrap())
}

fn pipeline_in(dir: &std::path::Path) -> (Arc<MemStore>, TxPipeline, tokio::task::JoinHandle<()>) {
    let store = mem_store();
    let log = Arc::new(CommitLog::open(dir, SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    (store, pipeline, tokio::spawn(worker.run()))
}

fn room(id: u64, name: &str) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::Str(name.into())]
}
fn cursor(conn: u128, x: i32, y: i32) -> Vec<RowValue> {
    vec![
        RowValue::ConnectionId(fluxum_core::types::ConnectionId::new(conn)),
        RowValue::I32(x),
        RowValue::I32(y),
    ]
}

// DMX-011 cleanup metadata for Cursor: owner-bound to `conn` (ordinal 0),
// rows expire 300 s after their last write (time injected in tests).
fluxum_core::schema::inventory::submit! {
    fluxum_core::schema::EphemeralDef {
        table: "Cursor",
        owner: Some(0),
        expire_after_us: Some(300_000_000),
    }
}

#[test]
fn table_access_helpers_classify_ephemeral_and_visibility() {
    assert!(CURSOR.is_ephemeral());
    assert!(!ROOM.is_ephemeral());
    // Ephemeral tables are client-visible (they fan out); Private/Global are not.
    assert!(TableAccess::Ephemeral.is_client_visible());
    assert!(TableAccess::Public.is_client_visible());
    assert!(!TableAccess::Private.is_client_visible());
    assert!(!TableAccess::Global.is_client_visible());
    assert!(TableAccess::Ephemeral.is_ephemeral());
    assert!(!TableAccess::Public.is_ephemeral());
}

fn logged_records(dir: &std::path::Path) -> Vec<TxRecord> {
    let mut records = Vec::new();
    replay(dir, SHARD, |_, record| {
        records.push(record);
        Ok(())
    })
    .unwrap();
    records
}

/// DMX-010: an ephemeral write is applied to committed state and rides the
/// fan-out diff, but the commit-log record carries only the durable table.
#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_writes_fan_out_but_skip_the_commit_log() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(dir.path());
    let room_id = store.table_id("Room").unwrap();
    let cursor_id = store.table_id("Cursor").unwrap();

    let receipt = pipeline
        .call(Box::new(move |tx| {
            tx.insert(room_id, room(1, "lobby"))?;
            tx.insert(cursor_id, cursor(42, 10, 20))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Fan-out diff (the SPEC-005 seam) carries BOTH tables — ephemeral rows are
    // delivered to subscribers exactly like durable ones (DMX-010).
    assert_eq!(receipt.diff.tables.len(), 2);

    // Committed state holds the ephemeral row in memory (visible to readers).
    let snap = store.snapshot();
    assert_eq!(snap.row_count(room_id).unwrap(), 1);
    assert_eq!(snap.row_count(cursor_id).unwrap(), 1);

    // The commit log records only the durable table (Room) — never Cursor.
    pipeline.log().wait_durable(receipt.tx_id).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let records = logged_records(dir.path());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tx_id, 1);
    let logged_tables: Vec<u32> = records[0].mutations.iter().map(|m| m.table_id).collect();
    assert_eq!(
        logged_tables,
        vec![room_id.as_u32()],
        "only the durable table is logged"
    );
}

/// DMX-010 + TXN-030: an ephemeral-only transaction writes no row data to the
/// log yet still consumes its `tx_id`, so the durable sequence stays gap-free.
#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_only_tx_logs_no_row_data_but_keeps_tx_id_gap_free() {
    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, worker) = pipeline_in(dir.path());
    let room_id = store.table_id("Room").unwrap();
    let cursor_id = store.table_id("Cursor").unwrap();

    // tx 1: ephemeral only.
    let r1 = pipeline
        .call(Box::new(move |tx| {
            tx.insert(cursor_id, cursor(42, 1, 1))?;
            Ok(())
        }))
        .await
        .unwrap();
    // tx 2: durable.
    let r2 = pipeline
        .call(Box::new(move |tx| {
            tx.insert(room_id, room(1, "lobby"))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!((r1.tx_id, r2.tx_id), (1, 2));

    pipeline.log().wait_durable(2).await.unwrap();
    drop(pipeline);
    worker.await.unwrap();
    let records = logged_records(dir.path());
    let ids: Vec<u64> = records.iter().map(|r| r.tx_id).collect();
    assert_eq!(ids, vec![1, 2], "gap-free tx_id sequence");
    // The ephemeral-only tx logged a record with no row mutations.
    assert!(
        records[0].mutations.is_empty(),
        "ephemeral-only tx wrote row data to the WAL"
    );
    assert_eq!(records[1].mutations.len(), 1);
}

/// DMX-012: neither the commit log nor a checkpoint carries ephemeral rows, so
/// after a restart the durable table is restored and the ephemeral table is
/// empty.
#[tokio::test(flavor = "multi_thread")]
async fn ephemeral_table_is_empty_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let snap_dir = dir.path().join("snapshots");

    // Session 1: durable + ephemeral writes, then a checkpoint and shutdown.
    {
        let (store, pipeline, worker) = pipeline_in(&log_dir);
        let room_id = store.table_id("Room").unwrap();
        let cursor_id = store.table_id("Cursor").unwrap();
        let receipt = pipeline
            .call(Box::new(move |tx| {
                tx.insert(room_id, room(1, "lobby"))?;
                tx.insert(cursor_id, cursor(42, 5, 6))?;
                Ok(())
            }))
            .await
            .unwrap();
        pipeline.log().wait_durable(receipt.tx_id).await.unwrap();

        // Force a checkpoint: it must exclude the ephemeral table (repo.write).
        let repo = Arc::new(CheckpointRepo::open(&snap_dir).unwrap());
        let cp = SnapshotWorker::spawn(
            Arc::clone(&store),
            Arc::clone(&repo),
            SHARD,
            WorkerOptions::default(),
        )
        .unwrap();
        cp.observe_commit(receipt.tx_id);
        cp.checkpoint_now().unwrap();
        cp.close().unwrap();

        drop(pipeline);
        worker.await.unwrap();
    }

    // Session 2: recover from checkpoint + log into a fresh store.
    let store = mem_store();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    recover(&store, &repo, &log_dir, SHARD).unwrap();

    let snap = store.snapshot();
    let room_id = store.table_id("Room").unwrap();
    let cursor_id = store.table_id("Cursor").unwrap();
    assert_eq!(snap.row_count(room_id).unwrap(), 1, "durable row restored");
    assert_eq!(
        snap.row_count(cursor_id).unwrap(),
        0,
        "ephemeral table is empty after restart (DMX-012)"
    );
}

// --- DMX-011: owner-bound disconnect cleanup + expire_after sweeper -------------

/// DMX-011: on disconnect, the engine deletes exactly the dropped
/// connection's rows from owner-bound ephemeral tables — in the hook
/// transaction, whose receipt fans the deletes out.
#[tokio::test(flavor = "multi_thread")]
async fn disconnect_cleanup_deletes_only_the_owners_rows() {
    use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};

    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path());
    let cursor_id = store.table_id("Cursor").unwrap();
    let engine = ReducerEngine::new(
        pipeline.clone(),
        Arc::new(ReducerRegistry::from_defs([]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("ephemeral-test"),
    );

    // Two connections, three rows.
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(cursor_id, cursor(42, 1, 1))?;
            tx.insert(cursor_id, cursor(43, 2, 2))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Connection 42 drops: its row is deleted, 43's row survives.
    let receipt = engine
        .client_disconnected(
            fluxum_core::types::Identity::from_bytes([9u8; 32]),
            fluxum_core::types::ConnectionId::new(42),
        )
        .await
        .unwrap()
        .expect("cleanup transaction expected");
    assert_eq!(receipt.diff.tables.len(), 1);
    assert_eq!(receipt.diff.tables[0].deletes.len(), 1);
    let snap = store.snapshot();
    assert_eq!(snap.row_count(cursor_id).unwrap(), 1);

    // A connection with no rows commits an empty cleanup (no hook, no rows).
    let receipt = engine
        .client_disconnected(
            fluxum_core::types::Identity::from_bytes([9u8; 32]),
            fluxum_core::types::ConnectionId::new(99),
        )
        .await
        .unwrap()
        .expect("cleanup transaction expected");
    assert!(receipt.diff.tables.is_empty());
}

/// DMX-011: the sweeper expires rows `expire_after` after their last write
/// (identity-witness semantics) — an actively rewritten row never expires,
/// idle sweeps run no transaction, and deletes fan out as ordinary diffs.
#[tokio::test(flavor = "multi_thread")]
async fn sweeper_expires_stale_rows_and_refreshes_rewritten_ones() {
    use fluxum_core::scheduler::EphemeralSweeper;
    use fluxum_core::types::Timestamp;

    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path());
    let cursor_id = store.table_id("Cursor").unwrap();
    let sweeper = EphemeralSweeper::from_registered(pipeline.clone()).expect("Cursor has a TTL");
    assert!(sweeper.cadence() >= std::time::Duration::from_millis(100));

    let t0 = Timestamp::from_micros(1_000_000_000);
    let ttl = 300_000_000; // Cursor's registered expire_after_us

    pipeline
        .call(Box::new(move |tx| {
            tx.insert(cursor_id, cursor(42, 1, 1))?;
            tx.insert(cursor_id, cursor(43, 2, 2))?;
            Ok(())
        }))
        .await
        .unwrap();

    // First observation registers witnesses; nothing is due → no transaction.
    assert!(sweeper.sweep_once_at(t0).await.unwrap().is_none());

    // Connection 42 keeps writing; 43 goes idle.
    pipeline
        .call(Box::new(move |tx| {
            tx.upsert(cursor_id, cursor(42, 5, 5))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Past 43's TTL: 43 expires, 42 was rewritten → witness refreshes.
    let t1 = Timestamp::from_micros(t0.as_micros() + ttl + 1_000_000);
    let receipt = sweeper
        .sweep_once_at(t1)
        .await
        .unwrap()
        .expect("one expiry due");
    assert_eq!(receipt.diff.tables.len(), 1);
    assert_eq!(receipt.diff.tables[0].deletes.len(), 1);
    assert_eq!(store.snapshot().row_count(cursor_id).unwrap(), 1);

    // 42 now idles past its TTL (measured from the refresh at t1).
    let t2 = Timestamp::from_micros(t1.as_micros() + ttl + 1_000_000);
    let receipt = sweeper
        .sweep_once_at(t2)
        .await
        .unwrap()
        .expect("last row expires");
    assert_eq!(receipt.diff.tables[0].deletes.len(), 1);
    assert_eq!(store.snapshot().row_count(cursor_id).unwrap(), 0);

    // Fully empty table: idle sweep, no transaction.
    let t3 = Timestamp::from_micros(t2.as_micros() + ttl);
    assert!(sweeper.sweep_once_at(t3).await.unwrap().is_none());
}

/// The wall-clock sweep wrapper: a fresh sweeper's first pass only registers
/// witnesses (nothing is older than its TTL), so no transaction runs.
#[tokio::test(flavor = "multi_thread")]
async fn wall_clock_sweep_registers_witnesses_without_a_transaction() {
    use fluxum_core::scheduler::EphemeralSweeper;

    let dir = tempfile::tempdir().unwrap();
    let (store, pipeline, _worker) = pipeline_in(dir.path());
    let cursor_id = store.table_id("Cursor").unwrap();
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(cursor_id, cursor(42, 1, 1))?;
            Ok(())
        }))
        .await
        .unwrap();

    let sweeper = EphemeralSweeper::from_registered(pipeline.clone()).expect("Cursor has a TTL");
    assert!(sweeper.sweep_once().await.unwrap().is_none());
    assert_eq!(store.snapshot().row_count(cursor_id).unwrap(), 1);
}

/// DMX-011 backstop: a registered EphemeralDef whose table is not part of
/// the assembled schema is skipped (the registry is process-global; schemas
/// may be subsets).
#[test]
fn ephemeral_defs_for_absent_tables_are_skipped_at_assembly() {
    // This binary registers a def for `Cursor`; a schema without Cursor
    // still assembles.
    let schema = Schema::from_tables([&ROOM]).unwrap();
    assert!(schema.table("Cursor").is_none());
    // And the def registry is queryable by name.
    assert!(fluxum_core::schema::ephemeral_def("Cursor").is_some());
    assert!(fluxum_core::schema::ephemeral_def("Ghost").is_none());
}
