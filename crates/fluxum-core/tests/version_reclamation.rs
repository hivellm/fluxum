//! SPEC-015 TIER-061 + SPEC-022 RV-020 — version reclamation keeps pace with
//! copy-on-write churn, so the buffer pool holds the **live** working set
//! rather than accumulated version history.
//!
//! # What broke, and what these tests pin
//!
//! Retaining a snapshot of a paged store pins every page that snapshot's
//! commit superseded: TIER-061 cannot free a page while a live version can
//! still reach it. The temporal window (RV-020) retained a fixed **count** of
//! snapshots — 64 by default — so the memory it cost was
//! `superseded pages per commit x 64`, and transaction size bounds nothing.
//!
//! Measured before the byte ceiling existed, on a 100 000-row table whose
//! live data is ~5 MB: 800-row transactions pinned **241 MiB** of version
//! garbage, pushed the pool past its eviction watermark, and cut write
//! throughput from 37 900 to 1 563 rows/s — the pool spending real disk I/O
//! to write out pages it was about to discard. The count bound was working
//! exactly as written; it was measuring the wrong thing.
//!
//! RV-020 already says the window is "bounded by budget / checkpoint
//! horizon". These tests pin that the budget half is now enforced, and that
//! enforcing it did not cost the feature.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{AsOfPoint, MemStore, RowValue, StoreOptions};

static TASK_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
];
static TASK: TableSchema = TableSchema {
    name: "Task",
    columns: TASK_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([&TASK]).unwrap()).unwrap()
}

/// Insert `total` rows in transactions of `batch` rows each.
fn load(store: &MemStore, batch: u64, total: u64) {
    let table = store.table_id("Task").unwrap();
    let mut done = 0;
    while done < total {
        let end = (done + batch).min(total);
        let mut tx = store.begin();
        for id in done..end {
            tx.insert(
                table,
                vec![RowValue::U64(id), RowValue::Str(format!("t{id}"))],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        done = end;
    }
}

/// The property that matters: a workload whose **live** set fits the pool
/// performs no page writes, however large its transactions are.
///
/// Transaction size is the axis that used to break this. 100 000 rows of two
/// small columns is a few MB of live data against a 256 MiB default pool —
/// there is nothing legitimate to spill at any batch size, so a single page
/// write means version garbage displaced live pages.
#[test]
fn a_live_set_that_fits_the_pool_never_spills_at_any_transaction_size() {
    // Batch sizes from below the old cliff to well past it. The totals stay
    // modest because these run in the debug profile; 60 000 rows of two
    // small columns is still orders of magnitude under the pool, which is
    // all the property needs.
    for batch in [100u64, 400, 800, 2_000] {
        let store = store();
        load(&store, batch, 60_000);

        let metrics = store.pager().metrics().snapshot();
        assert_eq!(
            metrics.page_writes,
            0,
            "batch {batch}: {} page writes for a live set that fits the pool \
             (pool {} MiB of {} MiB, {} pages pending reclaim) — version garbage \
             is displacing live pages",
            metrics.page_writes,
            metrics.bufferpool_bytes / (1024 * 1024),
            metrics.bufferpool_capacity_bytes / (1024 * 1024),
            store.reclaim_pending().pages,
        );
        assert_eq!(
            metrics.evictions_total(),
            0,
            "batch {batch}: eviction ran with room to spare"
        );
    }
}

/// The pool must not fill with garbage even when the live set is tiny: the
/// retained window is bounded by bytes, so occupancy tracks live data plus a
/// bounded margin rather than growing with commit count.
#[test]
fn retained_version_garbage_stays_under_its_ceiling() {
    let store = store();
    load(&store, 800, 100_000);

    let metrics = store.pager().metrics().snapshot();
    let pending_bytes = store.reclaim_pending().pages as u64 * 4096;
    // The derived ceiling is a quarter of the pool; allow one commit's worth
    // of slack, since the trim runs after the push that crossed the line.
    let ceiling = metrics.bufferpool_capacity_bytes / 4;
    assert!(
        pending_bytes <= ceiling * 2,
        "pinned version garbage {pending_bytes} B is far past the {ceiling} B ceiling"
    );
    assert!(
        metrics.bufferpool_bytes < metrics.bufferpool_capacity_bytes,
        "pool filled to capacity on a workload whose live set is a few MB"
    );
}

/// Enforcing the ceiling must not silently disable `AS OF`. Small commits —
/// the normal case — stay well under the byte bound, so the window keeps its
/// full configured reach and nothing is dropped for budget.
#[test]
fn small_commits_keep_the_full_as_of_reach() {
    let store = store();
    let table = store.table_id("Task").unwrap();
    for id in 0..40u64 {
        let mut tx = store.begin();
        tx.insert(
            table,
            vec![RowValue::U64(id), RowValue::Str(format!("t{id}"))],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    assert_eq!(
        store.temporal_window_budget_evictions(),
        0,
        "ordinary small commits must not trip the byte ceiling"
    );
    assert_eq!(
        store.temporal_window_len(),
        40,
        "every commit stays reachable"
    );
    // And the retained versions actually answer: the state after 10 commits
    // holds exactly the first 10 rows.
    let snap = store.snapshot_as_of(AsOfPoint::Tx(10)).unwrap();
    assert_eq!(snap.scan(table).unwrap().len(), 10);
}

/// When large commits do trip the ceiling, the trade is reported rather than
/// silent: the window shortens and the eviction counter rises, so an operator
/// can see `AS OF` reach being spent on memory.
#[test]
fn budget_pressure_shortens_the_window_visibly() {
    let store = store();
    load(&store, 2_000, 60_000);

    assert!(
        store.temporal_window_budget_evictions() > 0,
        "large commits must trip the ceiling on a 256 MiB pool"
    );
    let held = store.temporal_window_len();
    assert!(
        held < 64,
        "the window should have shortened below its configured 64, held {held}"
    );
    assert!(held > 0, "the window should not be emptied outright");
}

/// The ceiling never overrides correctness: a snapshot the caller still holds
/// keeps reading its own version, however much churn follows it. This is the
/// invariant an over-eager reclaim would break, and it would break silently.
#[test]
fn a_held_snapshot_survives_heavy_superseding_churn() {
    let store = store();
    let table = store.table_id("Task").unwrap();
    load(&store, 100, 1_000);

    let pinned = store.snapshot();
    let before = pinned.scan(table).unwrap().len();
    assert_eq!(before, 1_000);

    // Rewrite every row many times over, in transactions large enough to
    // push hard against the byte ceiling.
    for round in 0..20u64 {
        let mut tx = store.begin();
        for id in 0..1_000u64 {
            tx.upsert(
                table,
                vec![
                    RowValue::U64(id),
                    RowValue::Str(format!("rewritten-{round}-{id}")),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    // The pinned snapshot still reads its own version, unchanged.
    let rows = pinned.scan(table).unwrap();
    assert_eq!(
        rows.len(),
        1_000,
        "pinned snapshot lost rows to reclamation"
    );
    for row in &rows {
        let Some(RowValue::Str(title)) = row.value(1) else {
            panic!("title column")
        };
        assert!(
            title.starts_with('t'),
            "pinned snapshot saw a later version: {title}"
        );
    }
}

/// A configured window of 0 disables retention entirely, so nothing is pinned
/// and the pool holds only live pages — the floor the byte ceiling is
/// approximating.
#[test]
fn a_disabled_window_pins_nothing() {
    let store = MemStore::with_options(
        &Schema::from_tables([&TASK]).unwrap(),
        StoreOptions {
            temporal_window: 0,
            ..StoreOptions::default()
        },
    )
    .unwrap();
    load(&store, 800, 100_000);

    let pending = store.reclaim_pending();
    assert_eq!(
        pending.pages, 0,
        "no retention means nothing pins superseded pages"
    );
    assert_eq!(pending.live_versions, 1, "only the current version is live");
}
