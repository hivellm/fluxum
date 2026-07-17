//! SPEC-023 DMX-020/021 — row TTL: a background sweep deletes expired rows in
//! ordinary transactions that emit delete diffs, is at-least-once/idempotent,
//! and bounds each pass so a large backlog drains without one giant delete.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::scheduler::TtlSweeper;
use fluxum_core::schema::TtlKind;
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::Timestamp;

const SHARD: u32 = 23;

// A `Session` table whose rows expire at an absolute `expires_at` Timestamp
// (the DMX-020 `#[ttl(field)]` form). The TTL descriptor is registered by the
// macro in real code; the tests inject it via the schedule sweeper directly.
static SESSION_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "expires_at",
        ty: FluxType::Timestamp,
    },
];
static SESSION: TableSchema = TableSchema {
    name: "Session",
    columns: SESSION_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn session(id: u64, expires_at_us: i64) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Timestamp(Timestamp::from_micros(expires_at_us)),
    ]
}

struct Harness {
    store: Arc<MemStore>,
    pipeline: TxPipeline,
    table: TableId,
    _worker: tokio::task::JoinHandle<()>,
}

fn harness(dir: &std::path::Path) -> Harness {
    let schema = Schema::from_tables([&SESSION]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let table = store.table_id("Session").unwrap();
    Harness {
        store,
        pipeline,
        table,
        _worker: tokio::spawn(worker.run()),
    }
}

fn row_count(h: &Harness) -> usize {
    h.store.snapshot().scan(h.table).unwrap().count()
}

/// DMX-020 field mode: rows whose `expires_at` is at or before the sweep time
/// are deleted; rows with a future expiry survive; the sweep emits a delete
/// diff for the removed rows.
#[tokio::test(flavor = "multi_thread")]
async fn absolute_expiry_deletes_past_rows_only() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;

    // Three rows: two already expired, one in the future.
    h.pipeline
        .call(Box::new(move |tx| {
            tx.insert(table, session(1, 1_000))?;
            tx.insert(table, session(2, 2_000))?;
            tx.insert(table, session(3, 9_999_999))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(row_count(&h), 3);

    // Register the field-mode sweeper by hand (the macro registers it in real
    // builds; here we drive it directly).
    let sweeper = ttl_field_sweeper(&h, 1);

    // Sweep at t = 5_000: rows 1 and 2 are past, row 3 survives.
    let (receipt, more) = sweeper
        .sweep_once_at(Timestamp::from_micros(5_000))
        .await
        .unwrap();
    assert!(!more, "small backlog fits one pass");
    let receipt = receipt.expect("two rows expired");
    let deletes: usize = receipt.diff.tables.iter().map(|t| t.deletes.len()).sum();
    assert_eq!(deletes, 2, "the delete diff carries both removals");
    assert_eq!(row_count(&h), 1, "only the future-dated row remains");

    // A second sweep at the same time is an idempotent no-op (nothing due).
    let (again, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(5_000))
        .await
        .unwrap();
    assert!(
        again.is_none(),
        "re-sweep of already-expired rows is a no-op"
    );
}

/// DMX-020 field mode: a row rewritten with a future expiry between the
/// snapshot scan and the delete survives (the delete re-verifies).
#[tokio::test(flavor = "multi_thread")]
async fn a_refreshed_row_is_not_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;
    h.pipeline
        .call(Box::new(move |tx| {
            tx.insert(table, session(1, 1_000))?;
            Ok(())
        }))
        .await
        .unwrap();

    // Refresh row 1 to a far-future expiry, then sweep "now".
    h.pipeline
        .call(Box::new(move |tx| {
            tx.upsert(table, session(1, 100_000_000))?;
            Ok(())
        }))
        .await
        .unwrap();
    let sweeper = ttl_field_sweeper(&h, 1);
    let (receipt, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(5_000))
        .await
        .unwrap();
    assert!(receipt.is_none(), "the refreshed row is not expired");
    assert_eq!(row_count(&h), 1);
}

/// DMX-021: a backlog larger than the batch cap drains across passes, each
/// pass bounded, without a single giant delete.
#[tokio::test(flavor = "multi_thread")]
async fn large_backlog_drains_in_bounded_batches() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;
    // 2500 already-expired rows.
    h.pipeline
        .call(Box::new(move |tx| {
            for id in 1..=2_500u64 {
                tx.insert(table, session(id, 1_000))?;
            }
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(row_count(&h), 2_500);

    // A small batch cap so we can observe multiple bounded passes.
    let sweeper = ttl_field_sweeper_capped(&h, 1, 1_000);
    let mut passes = 0;
    loop {
        let (receipt, more) = sweeper
            .sweep_once_at(Timestamp::from_micros(5_000))
            .await
            .unwrap();
        if let Some(receipt) = &receipt {
            let deletes: usize = receipt.diff.tables.iter().map(|t| t.deletes.len()).sum();
            assert!(deletes <= 1_000, "each pass is bounded to the cap");
        }
        passes += 1;
        if !more {
            break;
        }
        assert!(passes < 10, "must terminate");
    }
    assert!(passes >= 3, "2500 rows / 1000 cap ⇒ at least 3 passes");
    assert_eq!(row_count(&h), 0, "the whole backlog drained");
}

/// DMX-020 sliding (`after`) mode: a row expires `after_us` after its last
/// observed write; the first sweep registers the witness, a later sweep past
/// the window deletes it, and a rewrite in between refreshes the window.
#[tokio::test(flavor = "multi_thread")]
async fn sliding_ttl_expires_since_last_write() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;
    let after_us = 10_000i64;
    let sweeper = TtlSweeper::for_tables_test(
        h.pipeline.clone(),
        vec![(h.table, &SESSION, TtlKind::After { after_us })],
        1024,
    );

    h.pipeline
        .call(Box::new(move |tx| {
            tx.insert(table, session(1, 0))?;
            Ok(())
        }))
        .await
        .unwrap();

    // First sweep registers the witness (age 0) — nothing due.
    let (r, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(1_000))
        .await
        .unwrap();
    assert!(r.is_none(), "just-seen row is not expired");

    // A sweep before the window elapses is still a no-op.
    let (r, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(1_000 + after_us))
        .await
        .unwrap();
    assert!(r.is_none(), "at exactly the window it is not yet past");

    // A rewrite with changed data refreshes the witness: the age clock
    // restarts (an identical no-op upsert legitimately would not).
    h.pipeline
        .call(Box::new(move |tx| {
            tx.upsert(table, session(1, 42))?;
            Ok(())
        }))
        .await
        .unwrap();
    // Observe the rewrite (refresh witness at this time)...
    let (r, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(20_000))
        .await
        .unwrap();
    assert!(r.is_none(), "rewrite refreshed the window");
    // ...and it survives until the new window elapses.
    let (r, _) = sweeper
        .sweep_once_at(Timestamp::from_micros(20_000 + after_us + 1))
        .await
        .unwrap();
    assert!(r.is_some(), "past the refreshed window ⇒ deleted");
    assert_eq!(row_count(&h), 0);
}

/// Construction + cadence surface: `from_registered` is `None` when no table
/// declares `#[ttl]`; `cadence` clamps and reflects the sliding window;
/// `sweep_once` at the wall clock is a no-op on future-dated rows.
#[tokio::test(flavor = "multi_thread")]
async fn cadence_and_construction_surface() {
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());

    // No #[ttl] table is registered in this test binary.
    assert!(TtlSweeper::from_registered(h.pipeline.clone()).is_none());

    // Field-only tables have no intrinsic cadence → the clamped default.
    let field = ttl_field_sweeper(&h, 1);
    assert_eq!(field.cadence(), Duration::from_secs(1));

    // A short sliding window drives a shorter cadence, floored at 100 ms.
    let sliding = TtlSweeper::for_tables_test(
        h.pipeline.clone(),
        vec![(h.table, &SESSION, TtlKind::After { after_us: 200_000 })],
        1024,
    );
    assert_eq!(sliding.cadence(), Duration::from_millis(100));

    // A future-dated row is not swept at the current wall clock.
    let table = h.table;
    h.pipeline
        .call(Box::new(move |tx| {
            tx.insert(table, session(1, i64::MAX))?;
            Ok(())
        }))
        .await
        .unwrap();
    let (receipt, more) = field.sweep_once().await.unwrap();
    assert!(receipt.is_none() && !more, "nothing due at the wall clock");
}

// --- Sweeper construction seam ------------------------------------------------
// `TtlSweeper::from_registered` reads the link-time #[ttl] registry; these
// tests build the equivalent sweeper against the local schema through a small
// test-only constructor exercised via a registered def.

fn ttl_field_sweeper(h: &Harness, column: u16) -> TtlSweeper {
    ttl_field_sweeper_capped(h, column, 1024)
}

fn ttl_field_sweeper_capped(h: &Harness, column: u16, cap: usize) -> TtlSweeper {
    TtlSweeper::for_tables_test(
        h.pipeline.clone(),
        vec![(h.table, &SESSION, TtlKind::Field { column })],
        cap,
    )
}
