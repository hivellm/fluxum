//! SPEC-021 §4 (CS-030/031) — exactly-once reducer submission: a replayed
//! `idempotency_key` returns without re-running the body, the dedup record
//! commits atomically with the effects it guards, and keys are scoped per
//! `(Identity, reducer)`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::Result;
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    CallOutcome, FluxValue, IDEMPOTENCY_TABLE, LifecycleHooks, ReducerCaller, ReducerContext,
    ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};

const SHARD: u32 = 0;

static ACCOUNT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "owner",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "balance",
        ty: FluxType::I64,
    },
];
static ACCOUNT: TableSchema = TableSchema {
    name: "Account",
    columns: ACCOUNT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Account {
    owner: u64,
    balance: i64,
}
impl Table for Account {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &ACCOUNT;
    fn primary_key(&self) -> u64 {
        self.owner
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.owner), RowValue::I64(self.balance)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(owner), RowValue::I64(balance)] => Ok(Self {
                owner: *owner,
                balance: *balance,
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

/// The double-apply hazard: move funds into account 1.
fn transfer(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let Some(FluxValue::I64(amount)) = args.first() else {
        return Err(fluxum_core::FluxumError::Reducer("transfer(amount)".into()));
    };
    let current = ctx.tx.query_pk::<Account>(1)?.map_or(0, |a| a.balance);
    ctx.tx.upsert(Account {
        owner: 1,
        balance: current + amount,
    })?;
    Ok(())
}
fn always_fails(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    Err(fluxum_core::FluxumError::Reducer("nope".into()))
}
fn nop_check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static TRANSFER: ReducerDef = ReducerDef {
    name: "transfer",
    handler: transfer,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static ALWAYS_FAILS: ReducerDef = ReducerDef {
    name: "always_fails",
    handler: always_fails,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn caller(identity: Identity) -> ReducerCaller {
    ReducerCaller {
        identity,
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: SHARD,
    }
}
fn alice() -> Identity {
    Identity::from_bytes([1u8; 32])
}
fn bob() -> Identity {
    Identity::from_bytes([2u8; 32])
}

struct Harness {
    engine: ReducerEngine,
    store: Arc<MemStore>,
}

fn boot(dir: &std::path::Path) -> Harness {
    // The dedup window is a system table, like `__schedule__`.
    let schema = Schema::from_tables([&ACCOUNT, &IDEMPOTENCY_TABLE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&TRANSFER, &ALWAYS_FAILS]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("idem-test"),
    );
    Harness { engine, store }
}

fn balance(store: &MemStore) -> i64 {
    store
        .snapshot()
        .query_pk(TableId::of("Account"), &[RowValue::U64(1)])
        .unwrap()
        .map_or(0, |row| match row.values()[1] {
            RowValue::I64(b) => b,
            _ => 0,
        })
}

// --- CS-030: the retry-after-lost-ack scenario -----------------------------------

#[tokio::test]
async fn a_replayed_key_applies_once_and_skips_the_body() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    // The client sends transfer with key K and loses the ack.
    let first = h
        .engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(100)],
            Some("K"),
        )
        .await
        .unwrap();
    assert!(matches!(first, CallOutcome::Committed(_)));
    assert_eq!(balance(&h.store), 100);

    // It reconnects and resends the identical call with the same key.
    let replay = h
        .engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(100)],
            Some("K"),
        )
        .await
        .unwrap();
    assert!(
        matches!(replay, CallOutcome::Deduplicated),
        "the body must not run again"
    );
    assert_eq!(balance(&h.store), 100, "the funds moved exactly once");

    // A *different* key from the same caller is a genuinely new call.
    h.engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(100)],
            Some("K2"),
        )
        .await
        .unwrap();
    assert_eq!(balance(&h.store), 200);

    // An unkeyed call is never deduplicated (opt-in, CS-030).
    h.engine
        .call_idempotent(caller(alice()), "transfer", vec![FluxValue::I64(5)], None)
        .await
        .unwrap();
    h.engine
        .call_idempotent(caller(alice()), "transfer", vec![FluxValue::I64(5)], None)
        .await
        .unwrap();
    assert_eq!(balance(&h.store), 210, "both unkeyed calls applied");
}

// --- CS-031: keys are scoped per (Identity, reducer) -----------------------------

#[tokio::test]
async fn keys_do_not_collide_across_callers() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    // Alice and Bob independently pick the key "1" — a plausible collision
    // if the window were global.
    h.engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(10)],
            Some("1"),
        )
        .await
        .unwrap();
    let bob_call = h
        .engine
        .call_idempotent(
            caller(bob()),
            "transfer",
            vec![FluxValue::I64(10)],
            Some("1"),
        )
        .await
        .unwrap();
    assert!(
        matches!(bob_call, CallOutcome::Committed(_)),
        "CS-031: Bob's key is his own"
    );
    assert_eq!(balance(&h.store), 20, "both applied");

    // Each caller's own replay is still caught.
    assert!(matches!(
        h.engine
            .call_idempotent(
                caller(bob()),
                "transfer",
                vec![FluxValue::I64(10)],
                Some("1")
            )
            .await
            .unwrap(),
        CallOutcome::Deduplicated
    ));
    assert_eq!(balance(&h.store), 20);
}

// --- CS-031: the record commits with the effects, and rolls back with them -------

#[tokio::test]
async fn a_failed_call_records_nothing_so_its_retry_re_executes() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    // The reducer errors → its transaction, and the dedup row written in
    // it, roll back together.
    let err = h
        .engine
        .call_idempotent(caller(alice()), "always_fails", vec![], Some("K"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");

    // The key was never recorded, so it is free for a real call: a failed
    // call applied nothing, so re-running it is safe (and not a silent
    // no-op forever).
    let retry = h
        .engine
        .call_idempotent(caller(alice()), "always_fails", vec![], Some("K"))
        .await
        .unwrap_err();
    assert!(
        retry.to_string().contains("nope"),
        "re-executed, not deduped"
    );

    // The window holds nothing for the rolled-back call.
    let window = h.store.snapshot();
    assert_eq!(
        window.scan(TableId::of("__idempotency__")).unwrap().count(),
        0,
        "a rollback leaves no dedup record"
    );
}

// --- The dedup record is durable: it rides the commit log ------------------------

#[tokio::test]
async fn the_dedup_record_is_written_in_the_reducer_commit() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    let CallOutcome::Committed(receipt) = h
        .engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(7)],
            Some("K"),
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit");
    };

    // CS-031: one transaction carries BOTH the effect and its dedup record,
    // so they are durable together — a crash cannot keep one without the
    // other.
    let idem = TableId::of("__idempotency__");
    let account = TableId::of("Account");
    let tables: Vec<TableId> = receipt.diff.tables.iter().map(|t| t.table_id).collect();
    assert!(tables.contains(&account), "the effect");
    assert!(tables.contains(&idem), "the dedup record, same commit");

    // And the record is scoped to (identity, reducer, key).
    let row = h
        .store
        .snapshot()
        .query_pk(
            idem,
            &[
                RowValue::Identity(alice()),
                RowValue::Str("transfer".into()),
                RowValue::Str("K".into()),
            ],
        )
        .unwrap();
    assert!(row.is_some(), "addressed by its CS-031 scope");
}

// --- CS-031: pruning the window frees keys for re-execution ----------------------

#[tokio::test]
async fn a_pruned_key_is_executed_again() {
    use fluxum_core::reducer::idempotency::{IdempotencyOptions, prunable};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());
    let idem = TableId::of("__idempotency__");

    h.engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(10)],
            Some("K"),
        )
        .await
        .unwrap();
    assert_eq!(balance(&h.store), 10);

    // Prune everything (a zero-length window stands in for "an hour
    // later"), exactly as the schedule worker does: decide from the
    // committed snapshot, then delete in one transaction.
    let options = IdempotencyOptions {
        max_records: 0,
        max_age: Duration::from_secs(0),
    };
    let doomed = {
        let snapshot = h.store.snapshot();
        let records: Vec<(Vec<RowValue>, i64)> = snapshot
            .scan(idem)
            .unwrap()
            .map(|row| {
                let values = row.values();
                let created_us = match values[3] {
                    RowValue::I64(us) => us,
                    _ => 0,
                };
                (values[..3].to_vec(), created_us)
            })
            .collect();
        prunable(records, Timestamp::now().as_micros() + 1_000_000, &options)
    };
    assert_eq!(doomed.len(), 1, "the record is outside the window");
    let mut tx = h.store.begin();
    for pk in doomed {
        assert!(tx.delete(idem, &pk).unwrap());
    }
    tx.commit().unwrap();

    // The key is forgotten, so a late retry re-executes rather than being
    // silently ignored: the window is a bounded safety net for prompt
    // retries, not an indefinite promise (CS-031).
    let late = h
        .engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(10)],
            Some("K"),
        )
        .await
        .unwrap();
    assert!(matches!(late, CallOutcome::Committed(_)));
    assert_eq!(balance(&h.store), 20, "a pruned key no longer dedupes");
}

// --- A key on a shard without the window is an error, not a silent downgrade -----

#[tokio::test]
async fn a_key_without_the_window_table_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    // Assemble WITHOUT the __idempotency__ table.
    let schema = Schema::from_tables([&ACCOUNT]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&TRANSFER]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("idem-test"),
    );

    let err = engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(1)],
            Some("K"),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("__idempotency__"),
        "a client that asked for exactly-once is told, not quietly downgraded: {err}"
    );
    // Unkeyed calls still work on such a shard.
    assert!(
        engine
            .call_idempotent(caller(alice()), "transfer", vec![FluxValue::I64(1)], None)
            .await
            .is_ok()
    );
}

// --- SEC-048 (F-017): the decode-time key length cap -------------------------------

#[tokio::test]
async fn an_over_length_idempotency_key_is_refused_before_any_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    let oversized = "k".repeat(fluxum_core::reducer::MAX_IDEMPOTENCY_KEY_BYTES + 1);
    let err = h
        .engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(100)],
            Some(&oversized),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_BAD_ARGS),
        "{err}"
    );
    assert_eq!(balance(&h.store), 0, "nothing ran, nothing committed");

    // A key exactly at the cap is admitted.
    let at_cap = "k".repeat(fluxum_core::reducer::MAX_IDEMPOTENCY_KEY_BYTES);
    h.engine
        .call_idempotent(
            caller(alice()),
            "transfer",
            vec![FluxValue::I64(100)],
            Some(&at_cap),
        )
        .await
        .unwrap();
    assert_eq!(balance(&h.store), 100);
}
