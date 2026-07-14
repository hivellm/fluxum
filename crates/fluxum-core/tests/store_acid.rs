//! ACID unit tests for `MemStore` (T2.1 item 1.6, DAG exit test):
//! insert/delete/query_pk/scan, MVCC read isolation, atomic merge, rollback
//! exactness (STG-007), auto-inc batching + gap-after-rollback (STG-040),
//! and lock-free concurrent readers (STG-004/FR-10).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, Row, RowValue, StoreOptions, TableId};

// --- Hand-built static schemas (macro output stand-ins, like the registry
// --- unit tests; the macro -> registry path is covered in fluxum-macros).

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

fn store() -> MemStore {
    let schema = Schema::from_tables([&USER, &SENSOR]).expect("schema assembles");
    MemStore::new(&schema).expect("store builds")
}

fn store_with_step(step: u64) -> MemStore {
    let schema = Schema::from_tables([&USER, &SENSOR]).expect("schema assembles");
    MemStore::with_options(
        &schema,
        StoreOptions {
            auto_inc_allocation_step: step,
            ..StoreOptions::default()
        },
    )
    .expect("store builds")
}

fn user(id: u64, name: &str) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::Str(name.into())]
}

fn sensor(x: i32, y: i32, reading: f64) -> Vec<RowValue> {
    vec![RowValue::I32(x), RowValue::I32(y), RowValue::F64(reading)]
}

fn insert_user(store: &MemStore, id: u64, name: &str) -> Row {
    let uid = store.table_id("User").unwrap();
    let mut tx = store.begin();
    let row = tx.insert(uid, user(id, name)).expect("insert ok");
    tx.commit().expect("commit ok");
    row
}

// --- Basic CRUD (DAG exit test: insert/delete/query_pk/scan) ---

#[test]
fn insert_commit_query_and_scan() {
    let store = store();
    let uid = store.table_id("User").unwrap();

    let mut tx = store.begin();
    let alice = tx.insert(uid, user(0, "alice")).unwrap();
    let bob = tx.insert(uid, user(0, "bob")).unwrap();
    assert_eq!(alice.value(0), Some(&RowValue::U64(1)));
    assert_eq!(bob.value(0), Some(&RowValue::U64(2)));
    let diff = tx.commit().unwrap();
    assert_eq!(diff.tx_id, 1);
    assert_eq!(diff.tables.len(), 1);
    assert_eq!(diff.tables[0].inserts.len(), 2);
    assert!(diff.tables[0].deletes.is_empty());

    let snap = store.snapshot();
    assert_eq!(snap.row_count(uid).unwrap(), 2);
    assert_eq!(
        snap.query_pk(uid, &[RowValue::U64(1)]).unwrap(),
        Some(alice)
    );
    assert_eq!(snap.query_pk(uid, &[RowValue::U64(99)]).unwrap(), None);
    let names: Vec<&RowValue> = snap.scan(uid).unwrap().filter_map(|r| r.value(1)).collect();
    assert_eq!(
        names,
        [&RowValue::Str("alice".into()), &RowValue::Str("bob".into())]
    );
}

#[test]
fn delete_removes_committed_row_and_reports_diff() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    let alice = insert_user(&store, 0, "alice");
    insert_user(&store, 0, "bob");

    let mut tx = store.begin();
    assert!(tx.delete(uid, &[RowValue::U64(1)]).unwrap());
    assert!(!tx.delete(uid, &[RowValue::U64(1)]).unwrap()); // already deleted
    assert!(!tx.delete(uid, &[RowValue::U64(42)]).unwrap()); // never existed
    let diff = tx.commit().unwrap();
    assert_eq!(diff.tables[0].deletes.len(), 1);
    assert!(diff.tables[0].deletes[0].1.same_identity(&alice));

    let snap = store.snapshot();
    assert_eq!(snap.row_count(uid).unwrap(), 1);
    assert_eq!(snap.query_pk(uid, &[RowValue::U64(1)]).unwrap(), None);
}

#[test]
fn composite_pk_insert_query_delete() {
    let store = store();
    let sid = store.table_id("Sensor").unwrap();

    let mut tx = store.begin();
    tx.insert(sid, sensor(-2, 9, 101.25)).unwrap();
    tx.insert(sid, sensor(-2, 10, 7.5)).unwrap();
    tx.commit().unwrap();

    let snap = store.snapshot();
    let hit = snap
        .query_pk(sid, &[RowValue::I32(-2), RowValue::I32(9)])
        .unwrap()
        .expect("row present");
    assert_eq!(hit.value(2), Some(&RowValue::F64(101.25)));

    let mut tx = store.begin();
    assert!(
        tx.delete(sid, &[RowValue::I32(-2), RowValue::I32(9)])
            .unwrap()
    );
    tx.commit().unwrap();
    assert_eq!(store.snapshot().row_count(sid).unwrap(), 1);
}

#[test]
fn unknown_table_and_bad_rows_are_descriptive_errors() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    assert!(store.table_id("Nope").is_none());

    let mut tx = store.begin();
    let err = tx
        .insert(TableId::from_raw(0xDEAD_BEEF), user(0, "x"))
        .unwrap_err();
    assert!(err.to_string().contains("unknown table"), "{err}");

    let err = tx.insert(uid, vec![RowValue::U64(0)]).unwrap_err();
    assert!(err.to_string().contains("declares 2 columns"), "{err}");

    let err = tx
        .insert(
            uid,
            vec![RowValue::Str("id?".into()), RowValue::Str("x".into())],
        )
        .unwrap_err();
    assert!(err.to_string().contains("column `id`"), "{err}");

    // Wrong PK arity on the read path.
    let err = tx.query_pk(uid, &[]).unwrap_err();
    assert!(err.to_string().contains("primary key takes 1"), "{err}");
}

// --- MVCC read isolation (STG-004) ---

#[test]
fn default_reads_never_see_pending_writes() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    insert_user(&store, 0, "alice");

    let mut tx = store.begin();
    tx.insert(uid, user(0, "bob")).unwrap();
    assert!(tx.delete(uid, &[RowValue::U64(1)]).unwrap());

    // The same transaction reads only CommittedState: the pending insert is
    // invisible, the pending delete has not happened.
    assert_eq!(tx.scan(uid).unwrap().count(), 1);
    assert!(tx.query_pk(uid, &[RowValue::U64(1)]).unwrap().is_some());
    assert!(tx.query_pk(uid, &[RowValue::U64(2)]).unwrap().is_none());
    tx.commit().unwrap();

    let snap = store.snapshot();
    assert!(snap.query_pk(uid, &[RowValue::U64(1)]).unwrap().is_none());
    assert!(snap.query_pk(uid, &[RowValue::U64(2)]).unwrap().is_some());
}

#[test]
fn snapshots_pin_their_state_across_later_commits() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    insert_user(&store, 0, "alice");

    let before = store.snapshot();
    insert_user(&store, 0, "bob");
    let after = store.snapshot();

    // The old handle still reads the old state (TXN-061 semantics).
    assert_eq!(before.row_count(uid).unwrap(), 1);
    assert_eq!(after.row_count(uid).unwrap(), 2);
    assert!(!before.same_state(&after));
}

// --- Constraint overlay (STG-007 tail, TXN-040) ---

#[test]
fn pk_conflicts_against_committed_and_pending_rows() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    insert_user(&store, 7, "alice");

    let mut tx = store.begin();
    // Conflict with a committed row.
    let err = tx.insert(uid, user(7, "impostor")).unwrap_err();
    assert_eq!(
        err.to_string(),
        "storage error: primary key conflict: table=User pk=(7)"
    );
    // Conflict with a pending insert of this same transaction.
    tx.insert(uid, user(8, "bob")).unwrap();
    let err = tx.insert(uid, user(8, "impostor")).unwrap_err();
    assert!(err.to_string().contains("pk=(8)"), "{err}");
    tx.rollback();
}

#[test]
fn tx_deleted_committed_rows_do_not_conflict() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    insert_user(&store, 7, "alice");

    // STG-007: a committed row marked deleted by this transaction does not
    // count as a PK conflict for a reinsert.
    let mut tx = store.begin();
    assert!(tx.delete(uid, &[RowValue::U64(7)]).unwrap());
    tx.insert(uid, user(7, "renamed")).unwrap();
    // ...but the key is now occupied by the pending reinsert.
    assert!(tx.insert(uid, user(7, "again")).is_err());
    tx.commit().unwrap();

    let row = store
        .snapshot()
        .query_pk(uid, &[RowValue::U64(7)])
        .unwrap()
        .expect("row present");
    assert_eq!(row.value(1), Some(&RowValue::Str("renamed".into())));
}

// --- Cancellation and diff exactness (STG-007 rule 1) ---

#[test]
fn delete_then_reinsert_identical_row_cancels_to_a_noop() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    let alice = insert_user(&store, 0, "alice");
    let before = store.snapshot();

    let mut tx = store.begin();
    assert!(tx.delete(uid, &[RowValue::U64(1)]).unwrap());
    let reinserted = tx.insert(uid, user(1, "alice")).unwrap();
    // The committed row identity is preserved — not deleted and recreated.
    assert!(reinserted.same_identity(&alice));
    let diff = tx.commit().unwrap();
    assert!(diff.is_empty(), "{diff:?}");

    // The committed row is still the very same allocation.
    let now = store
        .snapshot()
        .query_pk(uid, &[RowValue::U64(1)])
        .unwrap()
        .expect("row present");
    assert!(now.same_identity(&alice));
    assert_eq!(before.row_count(uid).unwrap(), 1);
}

#[test]
fn delete_then_reinsert_different_content_merges_as_replacement() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    let alice = insert_user(&store, 0, "alice");

    let mut tx = store.begin();
    assert!(tx.delete(uid, &[RowValue::U64(1)]).unwrap());
    let renamed = tx.insert(uid, user(1, "alicia")).unwrap();
    let diff = tx.commit().unwrap();

    // The diff carries the old row out and the new row in (SPEC-005 needs
    // both sides).
    assert_eq!(diff.tables.len(), 1);
    assert_eq!(diff.tables[0].inserts.len(), 1);
    assert_eq!(diff.tables[0].deletes.len(), 1);
    assert!(diff.tables[0].deletes[0].1.same_identity(&alice));
    assert!(diff.tables[0].inserts[0].same_identity(&renamed));
    assert_eq!(store.snapshot().row_count(uid).unwrap(), 1);
}

#[test]
fn insert_then_delete_of_a_pending_row_vanishes() {
    let store = store();
    let uid = store.table_id("User").unwrap();

    let mut tx = store.begin();
    tx.insert(uid, user(5, "ghost")).unwrap();
    assert!(tx.delete(uid, &[RowValue::U64(5)]).unwrap());
    let diff = tx.commit().unwrap();
    assert!(diff.tables.is_empty(), "{diff:?}");
    assert_eq!(store.snapshot().row_count(uid).unwrap(), 0);
}

// --- Rollback exactness (STG-006, STG-007) ---

#[test]
fn rollback_restores_the_prior_state_exactly() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    let sid = store.table_id("Sensor").unwrap();
    insert_user(&store, 0, "alice");
    let before = store.snapshot();

    let mut tx = store.begin();
    tx.insert(uid, user(0, "bob")).unwrap();
    assert!(tx.delete(uid, &[RowValue::U64(1)]).unwrap()); // delete committed row
    tx.insert(sid, sensor(1, 1, 0.5)).unwrap();
    tx.rollback();

    // Undelete is exact: the published state is the *same* state, not an
    // equal reconstruction (STG-007 "restore the prior state atomically").
    let after = store.snapshot();
    assert!(before.same_state(&after));
    assert!(after.query_pk(uid, &[RowValue::U64(1)]).unwrap().is_some());
    assert_eq!(after.row_count(sid).unwrap(), 0);
}

#[test]
fn dropping_a_tx_without_commit_rolls_back() {
    let store = store();
    let uid = store.table_id("User").unwrap();
    let before = store.snapshot();
    {
        let mut tx = store.begin();
        tx.insert(uid, user(0, "ghost")).unwrap();
        // dropped here — e.g. a reducer panic unwinding (TXN-022)
    }
    assert!(before.same_state(&store.snapshot()));
    // The shard accepts the next transaction normally.
    insert_user(&store, 0, "alice");
    assert_eq!(store.snapshot().row_count(uid).unwrap(), 1);
}

#[test]
fn rolled_back_transactions_do_not_consume_tx_ids() {
    let store = store();
    let uid = store.table_id("User").unwrap();

    let diff1 = {
        let mut tx = store.begin();
        tx.insert(uid, user(0, "a")).unwrap();
        tx.commit().unwrap()
    };
    {
        let mut tx = store.begin();
        assert_eq!(tx.tx_id(), 2);
        tx.insert(uid, user(0, "b")).unwrap();
        tx.rollback();
    }
    let diff2 = {
        let mut tx = store.begin();
        tx.insert(uid, user(0, "c")).unwrap();
        tx.commit().unwrap()
    };
    assert_eq!(diff1.tx_id, 1);
    assert_eq!(diff2.tx_id, 2); // the rollback's id was reissued (TXN-030)
}

// --- Auto-inc (STG-040, TXN-042) ---

#[test]
fn auto_inc_assigns_at_insert_time_and_batches_the_high_water_mark() {
    let store = store_with_step(10);
    let uid = store.table_id("User").unwrap();

    let mut tx = store.begin();
    let a = tx.insert(uid, user(0, "a")).unwrap();
    let b = tx.insert(uid, user(0, "b")).unwrap();
    // TXN-042: ids observable immediately, before commit.
    assert_eq!(a.value(0), Some(&RowValue::U64(1)));
    assert_eq!(b.value(0), Some(&RowValue::U64(2)));
    let diff = tx.commit().unwrap();
    // One batch allocation covers both inserts: high-water = step, and the
    // advance rides the TxDiff for the commit log (T2.2).
    assert_eq!(diff.auto_inc, vec![(uid, 10)]);
    assert_eq!(store.snapshot().auto_inc_high_water(uid).unwrap(), 10);

    // The next 8 inserts stay inside the batch — no further advance.
    let mut tx = store.begin();
    for _ in 0..8 {
        tx.insert(uid, user(0, "x")).unwrap();
    }
    let diff = tx.commit().unwrap();
    assert!(diff.auto_inc.is_empty());

    // The 11th allocation opens the next batch.
    let mut tx = store.begin();
    let eleventh = tx.insert(uid, user(0, "y")).unwrap();
    assert_eq!(eleventh.value(0), Some(&RowValue::U64(11)));
    let diff = tx.commit().unwrap();
    assert_eq!(diff.auto_inc, vec![(uid, 20)]);
}

#[test]
fn auto_inc_gaps_after_rollback_are_normal_and_ride_the_next_commit() {
    let store = store_with_step(4);
    let uid = store.table_id("User").unwrap();

    insert_user(&store, 0, "a"); // consumes 1

    // This transaction consumes ids 2 and 3, then rolls back: the values
    // are NOT returned (STG-040 — gaps are normal, ids are never dense).
    {
        let mut tx = store.begin();
        assert_eq!(
            tx.insert(uid, user(0, "gone")).unwrap().value(0),
            Some(&RowValue::U64(2))
        );
        assert_eq!(
            tx.insert(uid, user(0, "gone2")).unwrap().value(0),
            Some(&RowValue::U64(3))
        );
        tx.rollback();
    }

    let mut tx = store.begin();
    let next = tx.insert(uid, user(0, "b")).unwrap();
    // The sequence resumes after the gap...
    assert_eq!(next.value(0), Some(&RowValue::U64(4)));
    let diff = tx.commit().unwrap();
    // ...and ids 2 and 3 are absent forever.
    let snap = store.snapshot();
    assert!(snap.query_pk(uid, &[RowValue::U64(2)]).unwrap().is_none());
    assert!(snap.query_pk(uid, &[RowValue::U64(3)]).unwrap().is_none());
    // A high-water advance made by the rolled-back transaction would ride
    // this commit; here 1..=4 sit inside the first batch, so none is due.
    assert!(diff.auto_inc.is_empty());
    assert_eq!(snap.auto_inc_high_water(uid).unwrap(), 4);
}

#[test]
fn explicit_ids_advance_the_counter_past_them() {
    let store = store_with_step(4);
    let uid = store.table_id("User").unwrap();

    let mut tx = store.begin();
    tx.insert(uid, user(100, "explicit")).unwrap();
    let next = tx.insert(uid, user(0, "auto")).unwrap();
    assert_eq!(next.value(0), Some(&RowValue::U64(101)));
    let diff = tx.commit().unwrap();
    // The explicit id pushed the high-water to 100; the following automatic
    // allocation opened a batch beyond it.
    assert_eq!(diff.auto_inc, vec![(uid, 104)]);
}

// --- Concurrency (STG-003 single writer, STG-004/FR-10 lock-free reads) ---

#[test]
fn concurrent_readers_see_pre_commit_state_until_the_merge_and_never_a_partial_tx() {
    let store = std::sync::Arc::new(store());
    let uid = store.table_id("User").unwrap();
    let sid = store.table_id("Sensor").unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let store = std::sync::Arc::clone(&store);
            let stop = std::sync::Arc::clone(&stop);
            let ready = ready_tx.clone();
            thread::spawn(move || {
                let mut observed_states = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let snap = store.snapshot();
                    let users = snap.row_count(uid).unwrap();
                    let sensors = snap.row_count(sid).unwrap();
                    // Every transaction inserts one User AND one Sensor row:
                    // atomicity of the merge (STG-005) means the counts can
                    // never diverge, even mid-commit.
                    assert_eq!(
                        users, sensors,
                        "reader observed a partial transaction: {users} users vs {sensors} sensors"
                    );
                    observed_states += 1;
                    if observed_states == 1 {
                        ready.send(()).expect("main thread is waiting");
                    }
                }
                observed_states
            })
        })
        .collect();
    drop(ready_tx);
    // Don't start committing until every reader has observed at least one
    // state — otherwise a fast writer can finish all 500 commits before a
    // reader thread is even scheduled (seen on Windows CI).
    for _ in 0..readers.len() {
        ready_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("all readers observe the initial state");
    }

    for i in 0..500 {
        let mut tx = store.begin();
        tx.insert(uid, user(0, "u")).unwrap();
        tx.insert(sid, sensor(i, i, f64::from(i))).unwrap();

        // A snapshot taken while the transaction is in flight sees the
        // pre-commit state (STG-004).
        let during = store.snapshot();
        assert_eq!(during.row_count(uid).unwrap(), usize::try_from(i).unwrap());

        tx.commit().unwrap();
        assert_eq!(
            store.snapshot().row_count(uid).unwrap(),
            usize::try_from(i + 1).unwrap()
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for reader in readers {
        let observed = reader.join().expect("reader thread panicked");
        assert!(observed > 0);
    }
}

#[test]
fn second_begin_blocks_until_the_first_transaction_finishes() {
    let store = std::sync::Arc::new(store());
    let uid = store.table_id("User").unwrap();

    let tx = store.begin();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let contender = {
        let store = std::sync::Arc::clone(&store);
        thread::spawn(move || {
            started_tx.send(()).unwrap();
            let mut tx2 = store.begin(); // must block: single writer (STG-003)
            tx2.insert(uid, user(0, "second")).unwrap();
            tx2.commit().unwrap();
            done_tx.send(()).unwrap();
        })
    };

    started_rx.recv().unwrap();
    // While the first transaction is open, the contender cannot finish.
    assert!(
        done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "second transaction ran while the first was still open"
    );
    drop(tx); // rollback releases the writer
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("second transaction should proceed after release");
    contender.join().expect("contender panicked");
    assert_eq!(store.snapshot().row_count(uid).unwrap(), 1);
}
