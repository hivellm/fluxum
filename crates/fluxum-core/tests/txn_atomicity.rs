//! T3.1 atomicity property test (SPEC-003 acceptance 1, TXN-001/TXN-022):
//! reducers performing arbitrary sequences of inserts/upserts/deletes that
//! end in `Err` or panic — or that trip a PK/`#[unique]` constraint mid-way
//! — leave `CommittedState` byte-identical (pointer-identical, in fact) to
//! the pre-transaction state; successful reducers apply exactly their write
//! set, verified against an independent model.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Once};

use proptest::prelude::*;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId, Tx};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

const SHARD: u32 = 6;
const PANIC_MARKER: &str = "proptest reducer panic (deliberate)";

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "tag",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "val",
        ty: FluxType::U64,
    },
];

/// Explicit PK plus a `#[unique]` tag, so random sequences exercise both
/// constraint families (TXN-040/041).
static ITEM: TableSchema = TableSchema {
    name: "Item",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[&[1]],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, Copy)]
enum Op {
    Insert { id: u32, tag: u32, val: u64 },
    Upsert { id: u32, tag: u32, val: u64 },
    Delete { id: u32 },
}

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Commit,
    Fail,
    Panic,
}

/// The independent model: id → (tag, val), with exactly the store's overlay
/// semantics (insert conflicts on an occupied PK or a tag owned by another
/// visible row; upsert replaces but still respects the tag constraint;
/// delete frees both).
type Model = BTreeMap<u32, (u32, u64)>;

/// Apply `op` to the model; `Err(())` when the store would reject it.
fn model_apply(model: &mut Model, op: &Op) -> Result<(), ()> {
    let tag_owned_by_other = |model: &Model, id: u32, tag: u32| {
        model
            .iter()
            .any(|(&other, &(other_tag, _))| other != id && other_tag == tag)
    };
    match *op {
        Op::Insert { id, tag, val } => {
            if model.contains_key(&id) || tag_owned_by_other(model, id, tag) {
                return Err(());
            }
            model.insert(id, (tag, val));
            Ok(())
        }
        Op::Upsert { id, tag, val } => {
            if tag_owned_by_other(model, id, tag) {
                return Err(());
            }
            model.insert(id, (tag, val));
            Ok(())
        }
        Op::Delete { id } => {
            model.remove(&id);
            Ok(())
        }
    }
}

fn tx_apply(tx: &mut Tx<'_>, iid: TableId, op: &Op) -> fluxum_core::Result<()> {
    match *op {
        Op::Insert { id, tag, val } => tx
            .insert(
                iid,
                vec![RowValue::U32(id), RowValue::U32(tag), RowValue::U64(val)],
            )
            .map(|_| ()),
        Op::Upsert { id, tag, val } => tx
            .upsert(
                iid,
                vec![RowValue::U32(id), RowValue::U32(tag), RowValue::U64(val)],
            )
            .map(|_| ()),
        Op::Delete { id } => tx.delete(iid, &[RowValue::U32(id)]).map(|_| ()),
    }
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u32..6, 0u32..4, 0u64..100).prop_map(|(id, tag, val)| Op::Insert { id, tag, val }),
        (0u32..6, 0u32..4, 0u64..100).prop_map(|(id, tag, val)| Op::Upsert { id, tag, val }),
        (0u32..6).prop_map(|id| Op::Delete { id }),
    ]
}

fn outcome_strategy() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        3 => Just(Outcome::Commit),
        1 => Just(Outcome::Fail),
        1 => Just(Outcome::Panic),
    ]
}

/// Silence only the deliberate reducer panics; every other panic keeps the
/// default hook (test diagnostics stay intact).
fn install_quiet_panic_hook() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let deliberate = info
                .payload()
                .downcast_ref::<&str>()
                .is_some_and(|s| s.starts_with(PANIC_MARKER));
            if !deliberate {
                default(info);
            }
        }));
    });
}

fn run_case(base_ops: Vec<Op>, ops: Vec<Op>, outcome: Outcome) {
    install_quiet_panic_hook();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new(&Schema::from_tables([&ITEM]).unwrap()).unwrap());
        let log =
            Arc::new(CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap());
        let (pipeline, worker) =
            TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
        let worker = tokio::spawn(worker.run());
        let iid = store.table_id("Item").unwrap();

        // Committed base state: apply exactly the base ops the model accepts.
        let mut model = Model::new();
        let applied_base: Vec<Op> = base_ops
            .into_iter()
            .filter(|op| model_apply(&mut model, op).is_ok())
            .collect();
        if !applied_base.is_empty() {
            pipeline
                .call(Box::new(move |tx| {
                    for op in &applied_base {
                        tx_apply(tx, iid, op)?;
                    }
                    Ok(())
                }))
                .await
                .unwrap();
        }
        let before = store.snapshot();

        // Predict: does any op trip a constraint, and if not, what state
        // results?
        let mut expected = model.clone();
        let constraint_trips = ops.iter().any(|op| model_apply(&mut expected, op).is_err());
        let should_commit = !constraint_trips && matches!(outcome, Outcome::Commit);

        let run_ops = ops.clone();
        let result = pipeline
            .call(Box::new(move |tx| {
                for op in &run_ops {
                    tx_apply(tx, iid, op)?;
                }
                match outcome {
                    Outcome::Commit => Ok(()),
                    Outcome::Fail => Err(fluxum_core::FluxumError::Storage(
                        "deliberate reducer failure".into(),
                    )),
                    Outcome::Panic => panic!("{PANIC_MARKER}"),
                }
            }))
            .await;

        if should_commit {
            let receipt = result.unwrap_or_else(|e| panic!("expected commit, got: {e}"));
            let snap = store.snapshot();
            let got: Model = snap
                .scan(iid)
                .unwrap()
                .map(|row| {
                    let (
                        Some(&RowValue::U32(id)),
                        Some(&RowValue::U32(tag)),
                        Some(&RowValue::U64(val)),
                    ) = (row.value(0), row.value(1), row.value(2))
                    else {
                        panic!("malformed committed row: {row:?}");
                    };
                    (id, (tag, val))
                })
                .collect();
            assert_eq!(
                got, expected,
                "committed state must apply exactly the write set (ops {ops:?})"
            );
            snap.verify_index_integrity(iid).unwrap();
            // The logged record is the same diff the receipt carries — the
            // append happened before the response (TXN-004/TXN-021).
            assert!(receipt.tx_id >= 1);
        } else {
            assert!(
                result.is_err(),
                "expected rollback (constraint trip or Err/panic outcome), ops {ops:?}"
            );
            // TXN-001/TXN-022: byte-identical is guaranteed by pointer
            // identity — rollback publishes nothing.
            assert!(
                before.same_state(&store.snapshot()),
                "rollback must leave CommittedState untouched (ops {ops:?}, {outcome:?})"
            );
            snap_integrity(&store, iid);
        }

        drop(pipeline);
        worker.await.unwrap();
    });
}

fn snap_integrity(store: &MemStore, iid: TableId) {
    store.snapshot().verify_index_integrity(iid).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        ..ProptestConfig::default()
    })]

    #[test]
    fn arbitrary_reducers_are_atomic(
        base_ops in proptest::collection::vec(op_strategy(), 0..10),
        ops in proptest::collection::vec(op_strategy(), 0..12),
        outcome in outcome_strategy(),
    ) {
        run_case(base_ops, ops, outcome);
    }
}
