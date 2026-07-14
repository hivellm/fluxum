//! T2.4 index tests (tasks 1.1–1.3, 1.5; SPEC-001 acceptance 7, STG-007):
//! equality/range scans on single-column indexes and prefix scans on
//! composite indexes return exactly the rows a full scan would; range
//! results are value-ordered (memcomparable keys) across signed ints and
//! strings; commit maintains, rollback leaves every index bit-identical to
//! a fresh rebuild over `CommittedState` — verified under random op
//! sequences including rollbacks.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::ops::Bound;

use proptest::prelude::*;

use fluxum_core::index::IndexId;
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, Row, RowValue, TableId};

// --- Hand-built static schema (macro output stand-in, like store_acid.rs) ---

static MSG_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "channel",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "sent_at",
        ty: FluxType::I64,
    },
];

static MSG: TableSchema = TableSchema {
    name: "Msg",
    columns: MSG_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[
        // DM-031 composite: equality on channel + range on sent_at.
        IndexSchema::BTree { columns: &[1, 2] },
        // DM-030 single-column, signed-int range ordering.
        IndexSchema::BTree { columns: &[2] },
        // DM-030 single-column, string range ordering.
        IndexSchema::BTree { columns: &[1] },
    ],
    visibility: VisibilityRule::PublicAll,
};

struct Ids {
    table: TableId,
    composite: IndexId,
    by_sent: IndexId,
    by_channel: IndexId,
}

fn store() -> (MemStore, Ids) {
    let schema = Schema::from_tables([&MSG]).expect("schema assembles");
    let store = MemStore::new(&schema).expect("store builds");
    let ids = Ids {
        table: store.table_id("Msg").unwrap(),
        composite: store.index_id("Msg", &["channel", "sent_at"]).unwrap(),
        by_sent: store.index_id("Msg", &["sent_at"]).unwrap(),
        by_channel: store.index_id("Msg", &["channel"]).unwrap(),
    };
    (store, ids)
}

fn msg(id: u64, channel: &str, sent_at: i64) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Str(channel.into()),
        RowValue::I64(sent_at),
    ]
}

fn triple(row: &Row) -> (u64, String, i64) {
    match (row.value(0), row.value(1), row.value(2)) {
        (Some(RowValue::U64(id)), Some(RowValue::Str(channel)), Some(RowValue::I64(sent_at))) => {
            (*id, channel.clone(), *sent_at)
        }
        other => panic!("malformed Msg row: {other:?}"),
    }
}

fn insert_all(store: &MemStore, table: TableId, rows: &[(u64, &str, i64)]) {
    let mut tx = store.begin();
    for &(id, channel, sent_at) in rows {
        tx.insert(table, msg(id, channel, sent_at)).unwrap();
    }
    tx.commit().unwrap();
}

// --- Equality and range scans vs the full-scan oracle (task 1.5) ---

#[test]
fn index_eq_returns_exactly_the_matching_rows() {
    let (store, ids) = store();
    insert_all(
        &store,
        ids.table,
        &[
            (1, "a", 5),
            (2, "b", 5),
            (3, "a", -1),
            (4, "a", 5),
            (5, "", 0),
        ],
    );

    let snap = store.snapshot();
    for channel in ["a", "b", "", "nope"] {
        let mut got: Vec<_> = snap
            .index_eq(ids.table, ids.by_channel, &[RowValue::Str(channel.into())])
            .unwrap()
            .map(triple)
            .collect();
        got.sort();
        let mut want: Vec<_> = snap
            .scan(ids.table)
            .unwrap()
            .map(triple)
            .filter(|(_, c, _)| c == channel)
            .collect();
        want.sort();
        assert_eq!(got, want, "channel={channel:?}");
    }
    // Point lookup on the full composite key.
    let got: Vec<_> = snap
        .index_eq(
            ids.table,
            ids.composite,
            &[RowValue::Str("a".into()), RowValue::I64(5)],
        )
        .unwrap()
        .map(triple)
        .collect();
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(|(_, c, t)| c == "a" && *t == 5));
}

#[test]
fn range_scans_on_signed_ints_are_value_ordered() {
    let (store, ids) = store();
    insert_all(
        &store,
        ids.table,
        &[
            (1, "a", -5),
            (2, "a", -1),
            (3, "a", 0),
            (4, "a", 3),
            (5, "a", 7),
            (6, "a", i64::MIN),
            (7, "a", i64::MAX),
        ],
    );
    let snap = store.snapshot();

    let scan = |lower: Bound<i64>, upper: Bound<i64>| -> Vec<i64> {
        let (lo, hi) = (lower.map(RowValue::I64), upper.map(RowValue::I64));
        snap.index_scan(ids.table, ids.by_sent, &[], lo.as_ref(), hi.as_ref())
            .unwrap()
            .map(|row| triple(row).2)
            .collect()
    };

    // Full scan through the index: ascending numeric order (LE PK bytes
    // would give a completely different order — the memcomparable point).
    assert_eq!(
        scan(Bound::Unbounded, Bound::Unbounded),
        [i64::MIN, -5, -1, 0, 3, 7, i64::MAX]
    );
    assert_eq!(scan(Bound::Included(-1), Bound::Excluded(7)), [-1, 0, 3]);
    assert_eq!(scan(Bound::Excluded(-1), Bound::Included(7)), [0, 3, 7]);
    assert_eq!(
        scan(Bound::Unbounded, Bound::Included(-1)),
        [i64::MIN, -5, -1]
    );
    assert_eq!(scan(Bound::Excluded(3), Bound::Unbounded), [7, i64::MAX]);
    // Inverted and empty ranges.
    assert_eq!(
        scan(Bound::Included(5), Bound::Included(-5)),
        [] as [i64; 0]
    );
    assert_eq!(scan(Bound::Excluded(0), Bound::Excluded(0)), [] as [i64; 0]);
}

#[test]
fn range_scans_on_strings_are_value_ordered_including_embedded_nul() {
    let (store, ids) = store();
    insert_all(
        &store,
        ids.table,
        &[
            (1, "b", 0),
            (2, "a", 0),
            (3, "a\0", 0),
            (4, "ab", 0),
            (5, "", 0),
            (6, "a\0b", 0),
        ],
    );
    let snap = store.snapshot();

    let channels = |lower: Bound<&str>, upper: Bound<&str>| -> Vec<String> {
        let (lo, hi) = (
            lower.map(|s| RowValue::Str(s.into())),
            upper.map(|s| RowValue::Str(s.into())),
        );
        snap.index_scan(ids.table, ids.by_channel, &[], lo.as_ref(), hi.as_ref())
            .unwrap()
            .map(|row| triple(row).1)
            .collect()
    };

    assert_eq!(
        channels(Bound::Unbounded, Bound::Unbounded),
        ["", "a", "a\0", "a\0b", "ab", "b"]
    );
    assert_eq!(
        channels(Bound::Included("a"), Bound::Excluded("b")),
        ["a", "a\0", "a\0b", "ab"]
    );
    assert_eq!(
        channels(Bound::Excluded("a"), Bound::Included("ab")),
        ["a\0", "a\0b", "ab"]
    );
}

#[test]
fn composite_prefix_scans_resolve_equality_plus_range() {
    let (store, ids) = store();
    insert_all(
        &store,
        ids.table,
        &[
            (1, "a", 3),
            (2, "a", -2),
            (3, "a", 9),
            (4, "ab", -100), // must never leak into the "a" prefix
            (5, "b", 0),
            (6, "a", 3),
        ],
    );
    let snap = store.snapshot();

    // Equality on channel alone (prefix shorter than the key).
    let got: Vec<_> = snap
        .index_eq(ids.table, ids.composite, &[RowValue::Str("a".into())])
        .unwrap()
        .map(triple)
        .collect();
    assert!(got.iter().all(|(_, c, _)| c == "a"));
    let sent: Vec<i64> = got.iter().map(|&(_, _, t)| t).collect();
    assert_eq!(sent, [-2, 3, 3, 9]); // range-ordered within the prefix

    // Equality on channel + range on sent_at (DM-031's example shape).
    let (lo, hi) = (RowValue::I64(-2), RowValue::I64(9));
    let got: Vec<_> = snap
        .index_scan(
            ids.table,
            ids.composite,
            &[RowValue::Str("a".into())],
            Bound::Excluded(&lo),
            Bound::Excluded(&hi),
        )
        .unwrap()
        .map(triple)
        .collect();
    let sent: Vec<i64> = got.iter().map(|&(_, _, t)| t).collect();
    assert_eq!(sent, [3, 3]);
    assert!(got.iter().all(|(_, c, _)| c == "a"));

    // Empty prefix over the composite index: (channel, sent_at) tuple order.
    let all: Vec<_> = snap
        .index_eq(ids.table, ids.composite, &[])
        .unwrap()
        .map(triple)
        .map(|(_, c, t)| (c, t))
        .collect();
    assert_eq!(
        all,
        [
            ("a".to_string(), -2),
            ("a".to_string(), 3),
            ("a".to_string(), 3),
            ("a".to_string(), 9),
            ("ab".to_string(), -100),
            ("b".to_string(), 0),
        ]
    );
}

// --- Maintenance on commit (task 1.1/1.2) ---

#[test]
fn commit_maintains_indexes_across_insert_update_delete() {
    let (store, ids) = store();
    insert_all(&store, ids.table, &[(1, "a", 5), (2, "b", 6)]);

    // Update = delete + reinsert with different content: the entry moves.
    let mut tx = store.begin();
    assert!(tx.delete(ids.table, &[RowValue::U64(1)]).unwrap());
    tx.insert(ids.table, msg(1, "c", 7)).unwrap();
    tx.commit().unwrap();

    let snap = store.snapshot();
    snap.verify_index_integrity(ids.table).unwrap();
    assert_eq!(
        snap.index_eq(ids.table, ids.by_channel, &[RowValue::Str("a".into())])
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        snap.index_eq(ids.table, ids.by_channel, &[RowValue::Str("c".into())])
            .unwrap()
            .map(triple)
            .collect::<Vec<_>>(),
        [(1, "c".to_string(), 7)]
    );

    // Delete removes the entries from every index.
    let mut tx = store.begin();
    assert!(tx.delete(ids.table, &[RowValue::U64(2)]).unwrap());
    tx.commit().unwrap();
    let snap = store.snapshot();
    snap.verify_index_integrity(ids.table).unwrap();
    assert_eq!(
        snap.index_eq(ids.table, ids.by_channel, &[RowValue::Str("b".into())])
            .unwrap()
            .count(),
        0
    );
}

// --- MVCC (STG-004): index reads see only the committed snapshot ---

#[test]
fn index_reads_in_a_tx_see_committed_state_only() {
    let (store, ids) = store();
    insert_all(&store, ids.table, &[(1, "a", 5)]);

    let mut tx = store.begin();
    tx.insert(ids.table, msg(2, "a", 6)).unwrap();
    assert!(tx.delete(ids.table, &[RowValue::U64(1)]).unwrap());

    // The pending insert is invisible; the pending delete has not happened.
    let got: Vec<_> = tx
        .index_eq(ids.table, ids.by_channel, &[RowValue::Str("a".into())])
        .unwrap()
        .map(triple)
        .collect();
    assert_eq!(got, [(1, "a".to_string(), 5)]);
    tx.commit().unwrap();

    let got: Vec<_> = store
        .snapshot()
        .index_eq(ids.table, ids.by_channel, &[RowValue::Str("a".into())])
        .unwrap()
        .map(triple)
        .collect();
    assert_eq!(got, [(2, "a".to_string(), 6)]);
}

#[test]
fn snapshots_pin_rows_and_indexes_together() {
    let (store, ids) = store();
    insert_all(&store, ids.table, &[(1, "a", 5)]);
    let before = store.snapshot();

    insert_all(&store, ids.table, &[(2, "a", 6)]);

    // The old snapshot's index still returns the old row set.
    assert_eq!(
        before
            .index_eq(ids.table, ids.by_channel, &[RowValue::Str("a".into())])
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        store
            .snapshot()
            .index_eq(ids.table, ids.by_channel, &[RowValue::Str("a".into())])
            .unwrap()
            .count(),
        2
    );
}

// --- Rollback (task 1.3, STG-007 rule 2) ---

#[test]
fn rollback_leaves_indexes_bit_identical_to_a_rebuild() {
    let (store, ids) = store();
    insert_all(&store, ids.table, &[(1, "a", 5), (2, "b", 6)]);
    let before = store.snapshot();

    let mut tx = store.begin();
    tx.insert(ids.table, msg(3, "c", 7)).unwrap();
    assert!(tx.delete(ids.table, &[RowValue::U64(1)]).unwrap());
    tx.insert(ids.table, msg(1, "z", 99)).unwrap(); // reinsert-different
    tx.rollback();

    let after = store.snapshot();
    assert!(before.same_state(&after)); // the exact prior state (STG-007)
    after.verify_index_integrity(ids.table).unwrap();
    let got: Vec<_> = after
        .index_eq(ids.table, ids.composite, &[])
        .unwrap()
        .map(triple)
        .collect();
    assert_eq!(got, [(1, "a".to_string(), 5), (2, "b".to_string(), 6)]);
}

// --- API errors and index resolution ---

#[test]
fn index_resolution_and_scan_validation_errors_are_descriptive() {
    let (store, ids) = store();
    insert_all(&store, ids.table, &[(1, "a", 5)]);
    let snap = store.snapshot();

    // Resolution: only declared column sets (in declared order) resolve.
    assert!(store.index_id("Msg", &["sent_at", "channel"]).is_none());
    assert!(store.index_id("Msg", &["id"]).is_none());
    assert!(store.index_id("Nope", &["channel"]).is_none());

    // Unknown index id.
    let err = snap
        .index_eq(ids.table, IndexId::from_raw(0xDEAD_BEEF), &[])
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("unknown index"), "{err}");

    // Prefix longer than the index key.
    let long = [RowValue::I64(1), RowValue::I64(2)];
    let err = snap
        .index_eq(ids.table, ids.by_sent, &long)
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("has 2 value(s)"), "{err}");

    // Prefix value of the wrong type.
    let err = snap
        .index_eq(ids.table, ids.by_channel, &[RowValue::I64(1)])
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("expects Str"), "{err}");

    // Range bounds after a prefix that already covers every column.
    let bound = RowValue::I64(0);
    let err = snap
        .index_scan(
            ids.table,
            ids.by_sent,
            &[RowValue::I64(5)],
            Bound::Included(&bound),
            Bound::Unbounded,
        )
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("already covers"), "{err}");

    // Range bound of the wrong type.
    let bad = RowValue::Str("not a time".into());
    let err = snap
        .index_scan(
            ids.table,
            ids.composite,
            &[RowValue::Str("a".into())],
            Bound::Included(&bad),
            Bound::Unbounded,
        )
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("expects I64"), "{err}");
}

// --- Property suite (task 1.5, DAG exit test): index ≡ full-scan oracle
// --- under random op sequences including rollbacks (SPEC-001 acceptance 7,
// --- SPEC-002 acceptance 7).

const CHANNELS: &[&str] = &["", "a", "a\0", "b"];

#[derive(Debug, Clone)]
enum Op {
    Insert {
        id: u64,
        channel: usize,
        sent_at: i64,
    },
    Delete {
        id: u64,
    },
    Commit,
    Rollback,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u64..=12, 0..CHANNELS.len(), -3i64..=3)
            .prop_map(|(id, channel, sent_at)| Op::Insert { id, channel, sent_at }),
        2 => (1u64..=12).prop_map(|id| Op::Delete { id }),
        1 => Just(Op::Commit),
        1 => Just(Op::Rollback),
    ]
}

type Model = BTreeMap<u64, (String, i64)>;

/// Every index answer must equal the full-scan oracle over the committed
/// snapshot, every range must come back value-ordered, and every index must
/// be bit-identical to a fresh rebuild (STG-007 rule 2).
fn check_against_model(store: &MemStore, ids: &Ids, model: &Model) {
    let snap = store.snapshot();
    snap.verify_index_integrity(ids.table).unwrap();

    // Full scan == model.
    let mut scanned: Vec<_> = snap.scan(ids.table).unwrap().map(triple).collect();
    scanned.sort();
    let expected: Vec<_> = model
        .iter()
        .map(|(&id, (c, t))| (id, c.clone(), *t))
        .collect();
    assert_eq!(scanned, expected);

    // Equality per channel == filtered full scan.
    for channel in CHANNELS {
        let mut got: Vec<_> = snap
            .index_eq(
                ids.table,
                ids.by_channel,
                &[RowValue::Str((*channel).into())],
            )
            .unwrap()
            .map(triple)
            .collect();
        got.sort();
        let want: Vec<_> = expected
            .iter()
            .filter(|(_, c, _)| c == channel)
            .cloned()
            .collect();
        assert_eq!(got, want, "index_eq(channel={channel:?})");
    }

    // Ranges over sent_at: exact row set and ascending value order.
    let combos: [(Bound<i64>, Bound<i64>); 3] = [
        (Bound::Unbounded, Bound::Unbounded),
        (Bound::Included(-1), Bound::Excluded(2)),
        (Bound::Excluded(-2), Bound::Included(1)),
    ];
    for (lower, upper) in &combos {
        let (lo, hi) = (lower.map(RowValue::I64), upper.map(RowValue::I64));
        let got: Vec<_> = snap
            .index_scan(ids.table, ids.by_sent, &[], lo.as_ref(), hi.as_ref())
            .unwrap()
            .map(triple)
            .collect();
        assert!(
            got.windows(2).all(|w| w[0].2 <= w[1].2),
            "range scan not value-ordered: {got:?}"
        );
        let mut got_sorted = got.clone();
        got_sorted.sort();
        let in_range = |t: i64| {
            (match lower {
                Bound::Unbounded => true,
                Bound::Included(v) => t >= *v,
                Bound::Excluded(v) => t > *v,
            }) && (match upper {
                Bound::Unbounded => true,
                Bound::Included(v) => t <= *v,
                Bound::Excluded(v) => t < *v,
            })
        };
        let want: Vec<_> = expected
            .iter()
            .filter(|(_, _, t)| in_range(*t))
            .cloned()
            .collect();
        assert_eq!(got_sorted, want, "range {lower:?}..{upper:?}");

        // Composite prefix scan per channel: equality + the same range.
        for channel in CHANNELS {
            let got: Vec<_> = snap
                .index_scan(
                    ids.table,
                    ids.composite,
                    &[RowValue::Str((*channel).into())],
                    lo.as_ref(),
                    hi.as_ref(),
                )
                .unwrap()
                .map(triple)
                .collect();
            assert!(
                got.windows(2).all(|w| w[0].2 <= w[1].2),
                "prefix scan not value-ordered: {got:?}"
            );
            let mut got_sorted = got.clone();
            got_sorted.sort();
            let want: Vec<_> = expected
                .iter()
                .filter(|(_, c, t)| c == channel && in_range(*t))
                .cloned()
                .collect();
            assert_eq!(
                got_sorted, want,
                "prefix {channel:?} range {lower:?}..{upper:?}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn indexes_equal_the_full_scan_oracle_under_random_ops(
        ops in prop::collection::vec(op_strategy(), 1..80),
    ) {
        let (store, ids) = store();
        let mut committed: Model = BTreeMap::new();
        let mut pending: Model = BTreeMap::new();
        let mut tx = Some(store.begin());

        for op in ops {
            match op {
                Op::Insert { id, channel, sent_at } => {
                    let channel = CHANNELS[channel];
                    let result = tx
                        .as_mut()
                        .unwrap()
                        .insert(ids.table, msg(id, channel, sent_at));
                    // The overlay conflict rule (STG-007 tail) mirrors the
                    // model exactly: occupied key => PK conflict.
                    if let std::collections::btree_map::Entry::Vacant(slot) = pending.entry(id) {
                        prop_assert!(result.is_ok(), "{result:?}");
                        slot.insert((channel.to_string(), sent_at));
                    } else {
                        prop_assert!(result.is_err());
                    }
                }
                Op::Delete { id } => {
                    let existed = pending.remove(&id).is_some();
                    let deleted = tx
                        .as_mut()
                        .unwrap()
                        .delete(ids.table, &[RowValue::U64(id)])
                        .unwrap();
                    prop_assert_eq!(deleted, existed);
                }
                Op::Commit => {
                    tx.take().unwrap().commit().unwrap();
                    committed.clone_from(&pending);
                    check_against_model(&store, &ids, &committed);
                    tx = Some(store.begin());
                }
                Op::Rollback => {
                    tx.take().unwrap().rollback();
                    pending.clone_from(&committed);
                    check_against_model(&store, &ids, &committed);
                    tx = Some(store.begin());
                }
            }
        }

        // Dropping the trailing transaction rolls it back (STG-006).
        drop(tx);
        check_against_model(&store, &ids, &committed);
    }
}
