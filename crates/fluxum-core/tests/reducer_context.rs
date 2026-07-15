//! T3.2 verification suite (SPEC-004 RED-001..005; SPEC-003 TXN-050/051;
//! FR-17, FR-20; DAG exit test): the typed `TxHandle` write/read surface,
//! the intra-transaction visibility split (`scan` excludes pending,
//! `scan_pending` exact, `scan_all` deduplicated union), caller metadata on
//! `ReducerContext`, and nested reducer calls sharing one transaction with
//! propagate-vs-handle rollback semantics through the T3.1 pipeline.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, handler, with_context};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};

const SHARD: u32 = 5;

// --- Hand-built typed tables (macro output stand-ins, as in txn_pipeline;
// --- the macro-generated conversions are exercised in
// --- fluxum-macros/tests/typed_txhandle.rs) --------------------------------

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

#[derive(Debug, Clone, PartialEq)]
struct Account {
    id: u64,
    email: String,
    balance: i64,
}

impl Table for Account {
    type Pk = u64;

    const SCHEMA: &'static TableSchema = &ACCOUNT;

    fn primary_key(&self) -> u64 {
        self.id
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::U64(self.id),
            RowValue::Str(self.email),
            RowValue::I64(self.balance),
        ]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::U64(id),
                RowValue::Str(email),
                RowValue::I64(balance),
            ] => Ok(Self {
                id: *id,
                email: email.clone(),
                balance: *balance,
            }),
            _ => Err(FluxumError::Storage("Account row shape mismatch".into())),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn account(email: &str, balance: i64) -> Account {
    Account {
        id: 0, // auto_inc placeholder
        email: email.into(),
        balance,
    }
}

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

/// Composite PK — exercises tuple `Table::Pk` through the typed handle.
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

#[derive(Debug, Clone, PartialEq)]
struct Sensor {
    grid_x: i32,
    grid_y: i32,
    reading: f64,
}

impl Table for Sensor {
    type Pk = (i32, i32);

    const SCHEMA: &'static TableSchema = &SENSOR;

    fn primary_key(&self) -> (i32, i32) {
        (self.grid_x, self.grid_y)
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::I32(self.grid_x),
            RowValue::I32(self.grid_y),
            RowValue::F64(self.reading),
        ]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::I32(grid_x),
                RowValue::I32(grid_y),
                RowValue::F64(reading),
            ] => Ok(Self {
                grid_x: *grid_x,
                grid_y: *grid_y,
                reading: *reading,
            }),
            _ => Err(FluxumError::Storage("Sensor row shape mismatch".into())),
        }
    }

    fn pk_values(pk: &(i32, i32)) -> Vec<RowValue> {
        vec![RowValue::I32(pk.0), RowValue::I32(pk.1)]
    }
}

// --- Harness ----------------------------------------------------------------

fn mem_store() -> Arc<MemStore> {
    let schema = Schema::from_tables([&ACCOUNT, &SENSOR]).unwrap();
    Arc::new(MemStore::new(&schema).unwrap())
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("tester"),
        connection_id: ConnectionId::new(7),
        timestamp: Timestamp::from_micros(1_720_000_000_000_000),
        shard_id: SHARD,
    }
}

/// Run a typed reducer body in its own committed transaction.
fn commit_with<R>(
    store: &MemStore,
    registry: &ReducerRegistry,
    body: impl FnOnce(&fluxum_core::ReducerContext<'_, '_, '_>) -> Result<R>,
) -> R {
    let mut tx = store.begin();
    let out = with_context(registry, caller(), &mut tx, body).unwrap();
    tx.commit().unwrap();
    out
}

// --- Typed CRUD (RED-003 writes + TXN-050 reads; checklist 1.1/1.2) --------

#[test]
fn typed_crud_round_trips_through_txhandle() {
    let store = mem_store();
    let registry = ReducerRegistry::new();

    let stored = commit_with(&store, &registry, |ctx| {
        // Auto-inc: the returned row carries the assigned id (TXN-042).
        let ana = ctx.tx.insert(account("ana@example.com", 100))?;
        assert_eq!(ana.id, 1);
        let bo = ctx.tx.insert(account("bo@example.com", 250))?;
        assert_eq!(bo.id, 2);
        ctx.tx.insert(Sensor {
            grid_x: -2,
            grid_y: 9,
            reading: 1.5,
        })?;
        // Default reads are committed-only (TXN-050): nothing yet.
        assert_eq!(ctx.tx.query_pk::<Account>(1)?, None);
        assert!(ctx.tx.scan::<Account>()?.is_empty());
        Ok(ana)
    });
    assert_eq!(stored.email, "ana@example.com");

    commit_with(&store, &registry, |ctx| {
        // Committed now: typed point lookup, scans, filtered scans.
        let ana = ctx.tx.query_pk::<Account>(1)?.unwrap();
        assert_eq!(ana, stored);
        assert_eq!(ctx.tx.query_pk::<Account>(99)?, None);
        assert_eq!(ctx.tx.scan::<Account>()?.len(), 2);
        let rich = ctx.tx.scan_where::<Account>(|a| a.balance > 200)?;
        assert_eq!(rich.len(), 1);
        assert_eq!(rich[0].email, "bo@example.com");

        // Composite PK lookup + upsert replacement.
        let sensor = ctx.tx.query_pk::<Sensor>((-2, 9))?.unwrap();
        assert_eq!(sensor.reading, 1.5);
        ctx.tx.upsert(Sensor {
            reading: 2.25,
            ..sensor
        })?;

        // Delete by PK: true for a hit, false for a miss.
        assert!(ctx.tx.delete::<Account>(2)?);
        assert!(!ctx.tx.delete::<Account>(2)?);
        Ok(())
    });

    commit_with(&store, &registry, |ctx| {
        assert_eq!(ctx.tx.scan::<Account>()?.len(), 1);
        assert_eq!(ctx.tx.query_pk::<Sensor>((-2, 9))?.unwrap().reading, 2.25);
        Ok(())
    });
}

#[test]
fn typed_writes_surface_constraint_errors_and_delete_where_counts() {
    let store = mem_store();
    let registry = ReducerRegistry::new();

    commit_with(&store, &registry, |ctx| {
        ctx.tx.insert(account("taken@example.com", 10))?;
        ctx.tx.insert(account("other@example.com", 20))?;
        Ok(())
    });

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        // PK conflict (TXN-040) through the typed path.
        let err = ctx
            .tx
            .insert(Account {
                id: 1,
                email: "new@example.com".into(),
                balance: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("primary key conflict"), "{err}");

        // `#[unique]` violation (TXN-041) through the typed path.
        let err = ctx.tx.insert(account("taken@example.com", 0)).unwrap_err();
        assert!(
            err.to_string().contains("unique constraint violation"),
            "{err}"
        );
        Ok(())
    })
    .unwrap();
    tx.rollback();

    // delete_where: committed matches only, returns the deleted count.
    let deleted = commit_with(&store, &registry, |ctx| {
        let fresh = ctx.tx.insert(account("pending@example.com", 5))?;
        let deleted = ctx.tx.delete_where::<Account>(|a| a.balance >= 10)?;
        // The pending insert is not a candidate (committed snapshot only)…
        assert_eq!(ctx.tx.scan_pending::<Account>()?, vec![fresh]);
        Ok(deleted)
    });
    assert_eq!(deleted, 2);

    commit_with(&store, &registry, |ctx| {
        let left = ctx.tx.scan::<Account>()?;
        assert_eq!(left.len(), 1, "{left:?}");
        assert_eq!(left[0].email, "pending@example.com");
        Ok(())
    });
}

// --- Intra-transaction visibility (TXN-050/051, FR-17; checklist 1.3/1.5) --

#[test]
fn intra_transaction_visibility_suite() {
    let store = mem_store();
    let registry = ReducerRegistry::new();

    // Seed three committed accounts (ids 1..=3).
    commit_with(&store, &registry, |ctx| {
        ctx.tx.insert(account("a@example.com", 10))?;
        ctx.tx.insert(account("b@example.com", 20))?;
        ctx.tx.insert(account("c@example.com", 30))?;
        Ok(())
    });
    let before = store.snapshot();

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        // This transaction: 2 inserts, 1 upsert over committed id 2,
        // 1 delete of committed id 3.
        let d = ctx.tx.insert(account("d@example.com", 40))?;
        let e = ctx.tx.insert(account("e@example.com", 50))?;
        assert_eq!((d.id, e.id), (4, 5));
        ctx.tx.upsert(Account {
            id: 2,
            email: "b@example.com".into(),
            balance: 999,
        })?;
        assert!(ctx.tx.delete::<Account>(3)?);

        // `scan` / `query_pk` exclude every pending effect (TXN-050).
        let committed = ctx.tx.scan::<Account>()?;
        assert_eq!(
            committed.iter().map(|a| a.id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "scan must see the pre-transaction snapshot only"
        );
        assert_eq!(
            committed[1].balance, 20,
            "scan must see the committed content, not the pending upsert"
        );
        assert_eq!(ctx.tx.query_pk::<Account>(4)?, None);
        assert_eq!(ctx.tx.query_pk::<Account>(3)?.unwrap().balance, 30);

        // `scan_pending`: exactly this transaction's written rows — the two
        // inserts plus the upsert replacement content (TXN-051).
        let pending = ctx.tx.scan_pending::<Account>()?;
        let mut pending_ids: Vec<u64> = pending.iter().map(|a| a.id).collect();
        pending_ids.sort_unstable();
        assert_eq!(pending_ids, vec![2, 4, 5]);
        assert!(
            pending.iter().any(|a| a.id == 2 && a.balance == 999),
            "pending view carries the new upsert content"
        );

        // `count_pending` applies its predicate to the pending rows only.
        assert_eq!(ctx.tx.count_pending::<Account>(|a| a.balance >= 50)?, 2);
        assert_eq!(ctx.tx.count_pending::<Account>(|_| true)?, 3);

        // `scan_all`: committed ∪ pending, deduplicated by PK — pending
        // wins on id 2, the deleted id 3 is gone, inserts appear once.
        let all = ctx.tx.scan_all::<Account>()?;
        let mut all_ids: Vec<u64> = all.iter().map(|a| a.id).collect();
        all_ids.sort_unstable();
        assert_eq!(all_ids, vec![1, 2, 4, 5]);
        let two: Vec<&Account> = all.iter().filter(|a| a.id == 2).collect();
        assert_eq!(two.len(), 1, "deduplicated by primary key");
        assert_eq!(two[0].balance, 999, "pending upsert wins over committed");

        // `scan_all_where` filters the combined view.
        let heavy = ctx.tx.scan_all_where::<Account>(|a| a.balance > 30)?;
        let mut heavy_ids: Vec<u64> = heavy.iter().map(|a| a.id).collect();
        heavy_ids.sort_unstable();
        assert_eq!(heavy_ids, vec![2, 4, 5]);
        Ok(())
    })
    .unwrap();

    // Roll back: none of it happened (STG-006) — the visibility split never
    // leaked pending state into the committed snapshot.
    tx.rollback();
    assert!(before.same_state(&store.snapshot()));
}

// --- Context metadata (RED-002; checklist 1.1) ------------------------------

#[test]
fn context_exposes_caller_metadata_and_shares_it_with_nested_calls() {
    let store = mem_store();
    let expected = caller();

    let mut registry = ReducerRegistry::new();
    registry
        .register(
            "probe",
            handler(move |ctx, _args| {
                if ctx.identity == expected.identity
                    && ctx.connection_id == expected.connection_id
                    && ctx.timestamp == expected.timestamp
                    && ctx.shard_id == expected.shard_id
                {
                    Ok(())
                } else {
                    Err(FluxumError::Storage(
                        "nested call saw different caller metadata".into(),
                    ))
                }
            }),
        )
        .unwrap();

    let mut tx = store.begin();
    with_context(&registry, expected, &mut tx, |ctx| {
        assert_eq!(ctx.identity, expected.identity);
        assert_eq!(ctx.connection_id, expected.connection_id);
        assert_eq!(ctx.timestamp, expected.timestamp);
        assert_eq!(ctx.shard_id, expected.shard_id);
        // The callee runs under the same caller (RED-005).
        ctx.tx.call("probe", &[])
    })
    .unwrap();
    tx.rollback();
}

// --- Registry (RED-006 slice needed by ctx.tx.call) -------------------------

#[test]
fn registry_rejects_duplicates_and_unknown_dispatch() {
    let mut registry = ReducerRegistry::new();
    registry
        .register("credit", handler(|_ctx, _args| Ok(())))
        .unwrap();
    assert!(registry.contains("credit"));

    let err = registry
        .register("credit", handler(|_ctx, _args| Ok(())))
        .unwrap_err();
    assert!(
        err.to_string().contains("duplicate reducer name `credit`"),
        "{err}"
    );

    // Unknown reducer: rejected with a wire-ready 404, no table access.
    let store = mem_store();
    let mut tx = store.begin();
    let err = registry
        .dispatch(caller(), "missing", &[], &mut tx)
        .unwrap_err();
    assert_eq!(err.query_code(), Some(404));
    assert!(
        err.to_string().contains("unknown reducer `missing`"),
        "{err}"
    );
    tx.rollback();

    // Same 404 from a nested call naming an unknown reducer.
    let mut tx = store.begin();
    let err = with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.call("missing", &[])
    })
    .unwrap_err();
    assert_eq!(err.query_code(), Some(404));
    tx.rollback();
}

#[test]
fn call_depth_guard_turns_unbounded_recursion_into_an_error() {
    let mut registry = ReducerRegistry::new();
    registry
        .register("recurse", handler(|ctx, args| ctx.tx.call("recurse", args)))
        .unwrap();

    let store = mem_store();
    let mut tx = store.begin();
    let err = registry
        .dispatch(caller(), "recurse", &[], &mut tx)
        .unwrap_err();
    assert!(
        err.to_string().contains("reducer call depth exceeded"),
        "{err}"
    );
    tx.rollback();
}

// --- Nested calls share one transaction (RED-005; checklist 1.4) -----------

fn nested_registry() -> ReducerRegistry {
    let mut registry = ReducerRegistry::new();
    registry
        .register(
            "inner_fail",
            handler(|ctx, _args| {
                ctx.tx.insert(account("inner@example.com", 1))?;
                Err(FluxumError::Storage("task not found".into()))
            }),
        )
        .unwrap();
    registry
        .register(
            "outer_propagate",
            handler(|ctx, _args| {
                ctx.tx.insert(account("outer@example.com", 2))?;
                ctx.tx.call("inner_fail", &[])
            }),
        )
        .unwrap();
    registry
        .register(
            "outer_handle",
            handler(|ctx, _args| {
                ctx.tx.insert(account("handled@example.com", 3))?;
                let err = ctx.tx.call("inner_fail", &[]).unwrap_err();
                // Handled (RED-005): the transaction stays alive. No
                // savepoints exist — the callee's pre-error insert remains
                // part of this transaction.
                assert!(err.to_string().contains("task not found"), "{err}");
                Ok(())
            }),
        )
        .unwrap();
    registry
        .register(
            "check_shared",
            handler(|ctx, _args| {
                // Same TxState as the caller: its pending write is visible
                // through the explicit intra-tx read (TXN-051).
                let pending = ctx.tx.scan_pending::<Account>()?;
                if pending.iter().any(|a| a.email == "shared@example.com") {
                    Ok(())
                } else {
                    Err(FluxumError::Storage(
                        "callee cannot see the caller's pending write".into(),
                    ))
                }
            }),
        )
        .unwrap();
    registry
        .register(
            "outer_shares",
            handler(|ctx, _args| {
                ctx.tx.insert(account("shared@example.com", 4))?;
                ctx.tx.call("check_shared", &[])
            }),
        )
        .unwrap();
    registry
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_calls_share_one_transaction_through_the_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let store = mem_store();
    let log = Arc::new(CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let worker = tokio::spawn(worker.run());
    let registry = Arc::new(nested_registry());
    let who = caller();

    // Propagated callee Err: the WHOLE transaction (caller's and callee's
    // writes) rolls back (RED-005 / acceptance 2).
    let before = store.snapshot();
    let reg = Arc::clone(&registry);
    let err = pipeline
        .call(Box::new(move |tx| {
            reg.dispatch(who, "outer_propagate", &[], tx)
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("task not found"), "{err}");
    assert!(before.same_state(&store.snapshot()));

    // Handled callee Err: the transaction commits. Both the caller's write
    // and the callee's pre-error write land (shared TxState, no savepoints).
    let reg = Arc::clone(&registry);
    pipeline
        .call(Box::new(move |tx| {
            reg.dispatch(who, "outer_handle", &[], tx)
        }))
        .await
        .unwrap();
    let mut tx = store.begin();
    with_context(&registry, who, &mut tx, |ctx| {
        let emails: Vec<String> = ctx
            .tx
            .scan::<Account>()?
            .into_iter()
            .map(|a| a.email)
            .collect();
        assert!(
            emails.contains(&"handled@example.com".to_string()),
            "{emails:?}"
        );
        assert!(
            emails.contains(&"inner@example.com".to_string()),
            "{emails:?}"
        );
        assert!(
            !emails.contains(&"outer@example.com".to_string()),
            "{emails:?}"
        );
        Ok(())
    })
    .unwrap();
    tx.rollback();

    // The callee observes the caller's pending write: one shared TxState.
    let reg = Arc::clone(&registry);
    pipeline
        .call(Box::new(move |tx| {
            reg.dispatch(who, "outer_shares", &[], tx)
        }))
        .await
        .unwrap();

    drop(pipeline);
    worker.await.unwrap();
}
