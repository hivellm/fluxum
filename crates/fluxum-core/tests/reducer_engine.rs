//! T3.3 verification suite (SPEC-004 RED-001/004/006/010..013/030/031/061;
//! FR-20, FR-23, FR-25; DAG exit test: panic injection): reducer dispatch
//! through the link-time-shaped registry with pre-transaction admission
//! (unknown name, argument mismatch — no `TxState`, no log entry),
//! `catch_unwind` panic isolation (rollback, wire-ready 500, shard keeps
//! serving), the shard lifecycle (`on_init` exactly once on a fresh shard,
//! `on_shard_start` every boot, connect/disconnect presence end to end),
//! and read-only views over `ReadOnlyTxHandle`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, replay};
use fluxum_core::reducer::{
    FluxValue, LifecycleDef, LifecycleHooks, LifecycleKind, ReducerCaller, ReducerContext,
    ReducerDef, ReducerEngine, ReducerRegistry, ViewDef, ViewRegistry, args,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};

const SHARD: u32 = 7;
const EPOCH: u64 = 1;

// --- Hand-built typed tables (macro output stand-ins, as in
// --- reducer_context.rs; the macro end-to-end path lives in
// --- fluxum-macros/tests/reducer_lifecycle.rs) ------------------------------

static ONLINE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "identity",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "connection_id",
        ty: FluxType::ConnectionId,
    },
    ColumnSchema {
        name: "connected_at",
        ty: FluxType::Timestamp,
    },
];

static ONLINE: TableSchema = TableSchema {
    name: "OnlineUser",
    columns: ONLINE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct OnlineUser {
    identity: Identity,
    connection_id: ConnectionId,
    connected_at: Timestamp,
}

impl Table for OnlineUser {
    type Pk = Identity;

    const SCHEMA: &'static TableSchema = &ONLINE;

    fn primary_key(&self) -> Identity {
        self.identity
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::Identity(self.identity),
            RowValue::ConnectionId(self.connection_id),
            RowValue::Timestamp(self.connected_at),
        ]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::Identity(identity),
                RowValue::ConnectionId(connection_id),
                RowValue::Timestamp(connected_at),
            ] => Ok(Self {
                identity: *identity,
                connection_id: *connection_id,
                connected_at: *connected_at,
            }),
            other => Err(FluxumError::Storage(format!(
                "OnlineUser: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &Identity) -> Vec<RowValue> {
        vec![RowValue::Identity(*pk)]
    }
}

static EVENT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "label",
        ty: FluxType::Str,
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

#[derive(Debug, Clone, PartialEq)]
struct Event {
    id: u64,
    label: String,
}

impl Table for Event {
    type Pk = u64;

    const SCHEMA: &'static TableSchema = &EVENT;

    fn primary_key(&self) -> u64 {
        self.id
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.label)]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(label)] => Ok(Self {
                id: *id,
                label: label.clone(),
            }),
            other => Err(FluxumError::Storage(format!(
                "Event: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

// --- Reducer / lifecycle / view defs (macro output stand-ins) ---------------

fn record_event(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    args::check_arity("record_event", args, 1)?;
    let label: String = args::decode_arg("record_event", args, 0, "label")?;
    if label.is_empty() {
        return Err(FluxumError::Reducer("empty label".into()));
    }
    ctx.tx.insert(Event { id: 0, label })?;
    Ok(())
}

fn check_record_event(args: &[FluxValue]) -> Result<()> {
    args::check_arity("record_event", args, 1)?;
    let _ = args::decode_arg::<String>("record_event", args, 0, "label")?;
    Ok(())
}

static RECORD_EVENT: ReducerDef = ReducerDef {
    name: "record_event",
    handler: record_event,
    check_args: check_record_event,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn explode(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    // A buffered write that MUST be rolled back by the panic.
    ctx.tx.insert(Event {
        id: 0,
        label: "never visible".into(),
    })?;
    panic!("reducer bug (test)");
}

fn check_none(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static EXPLODE: ReducerDef = ReducerDef {
    name: "explode",
    handler: explode,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};

static DUPLICATE_A: ReducerDef = ReducerDef {
    name: "dup",
    handler: record_event,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};
static DUPLICATE_B: ReducerDef = ReducerDef {
    name: "dup",
    handler: record_event,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};

static INIT_RUNS: AtomicU64 = AtomicU64::new(0);
static SHARD_START_RUNS: AtomicU64 = AtomicU64::new(0);

fn seed_config(ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    INIT_RUNS.fetch_add(1, Ordering::SeqCst);
    ctx.tx.insert(Event {
        id: 0,
        label: "seed".into(),
    })?;
    Ok(())
}

fn warm_caches(_ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    SHARD_START_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn presence_up(ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    ctx.tx.upsert(OnlineUser {
        identity: ctx.identity,
        connection_id: ctx.connection_id,
        connected_at: ctx.timestamp,
    })?;
    Ok(())
}

fn presence_down(ctx: &ReducerContext<'_, '_, '_>) -> Result<()> {
    ctx.tx.delete::<OnlineUser>(ctx.identity)?;
    Ok(())
}

static ON_INIT: LifecycleDef = LifecycleDef {
    kind: LifecycleKind::OnInit,
    name: "seed_config",
    handler: seed_config,
};
static ON_SHARD_START: LifecycleDef = LifecycleDef {
    kind: LifecycleKind::OnShardStart,
    name: "warm_caches",
    handler: warm_caches,
};
static ON_CONNECT: LifecycleDef = LifecycleDef {
    kind: LifecycleKind::OnConnect,
    name: "presence_up",
    handler: presence_up,
};
static ON_DISCONNECT: LifecycleDef = LifecycleDef {
    kind: LifecycleKind::OnDisconnect,
    name: "presence_down",
    handler: presence_down,
};

// --- One simulated shard boot ------------------------------------------------

struct Shard {
    engine: ReducerEngine,
    store: Arc<MemStore>,
    fresh: bool,
}

fn schema() -> Schema {
    Schema::from_tables([&ONLINE, &EVENT]).unwrap()
}

/// Boot a shard against `dir`: fresh store, log open, checkpoint+replay
/// recovery, pipeline worker spawned, engine assembled — the phase-5 server
/// assembly in miniature.
fn boot(dir: &Path) -> Shard {
    let schema = schema();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(&dir.join("log"), SHARD, EPOCH, CommitLogOptions::default()).unwrap(),
    );
    let repo = CheckpointRepo::open(&dir.join("snapshots")).unwrap();
    let outcome = recover(&store, &repo, &dir.join("log"), SHARD).unwrap();
    let fresh = outcome.last_tx_id.is_none();

    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());

    let registry = Arc::new(ReducerRegistry::from_defs([&RECORD_EVENT, &EXPLODE]).unwrap());
    let hooks = LifecycleHooks::from_defs([&ON_INIT, &ON_SHARD_START, &ON_CONNECT, &ON_DISCONNECT]);
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        hooks,
        SHARD,
        fluxum_core::auth::server_identity("test-shard"),
    );
    Shard {
        engine,
        store,
        fresh,
    }
}

fn client(seed: u8) -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([seed; 32]),
        connection_id: ConnectionId::new(u128::from(seed) + 1),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

/// Number of records in the shard's log (admission rejections and rollbacks
/// must never append).
fn logged_records(dir: &Path) -> usize {
    let mut count = 0usize;
    let report = replay(&dir.join("log"), SHARD, |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert!(report.corruption.is_none());
    count
}

// --- RED-006 / RED-001: admission before any transaction ---------------------

#[tokio::test(flavor = "multi_thread")]
async fn admission_rejects_unknown_names_and_bad_args_without_a_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());

    // A healthy call commits and is logged.
    let receipt = shard
        .engine
        .call(client(1), "record_event", vec![FluxValue::Str("a".into())])
        .await
        .unwrap();
    let baseline_tx = receipt.tx_id;
    let baseline_records = logged_records(dir.path());

    // Unknown reducer: wire-ready 404, no transaction, no log entry.
    let err = shard
        .engine
        .call(client(1), "no_such_reducer", vec![])
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(404), "{err}");

    // Argument-count mismatch: 400 before any transaction (RED-001).
    let err = shard
        .engine
        .call(client(1), "record_event", vec![])
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");

    // Argument-type mismatch: 400 naming the parameter.
    let err = shard
        .engine
        .call(client(1), "record_event", vec![FluxValue::I64(7)])
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");
    assert!(err.to_string().contains("`label`"), "{err}");

    assert_eq!(
        logged_records(dir.path()),
        baseline_records,
        "rejected calls must not reach the log"
    );

    // The tx-id sequence is gap-free: rejections consumed nothing.
    let receipt = shard
        .engine
        .call(client(1), "record_event", vec![FluxValue::Str("b".into())])
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, baseline_tx + 1);
}

#[test]
fn duplicate_reducer_names_abort_startup() {
    let err = match ReducerRegistry::from_defs([&DUPLICATE_A, &DUPLICATE_B]) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("duplicate names must abort startup (RED-006)"),
    };
    assert!(err.contains("duplicate reducer name `dup`"), "{err}");
}

// --- RED-004 / RED-060: Err rolls back and carries the message ---------------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_err_rolls_back_and_carries_the_message_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let before = shard.store.snapshot();

    let err = shard
        .engine
        .call(
            client(2),
            "record_event",
            vec![FluxValue::Str(String::new())],
        )
        .await
        .unwrap_err();
    let FluxumError::Reducer(message) = &err else {
        panic!("expected FluxumError::Reducer, got {err:?}");
    };
    assert_eq!(message, "empty label", "verbatim to the caller (RED-060)");
    assert!(
        before.same_state(&shard.store.snapshot()),
        "rollback is a pure discard (RED-004)"
    );
    assert_eq!(logged_records(dir.path()), 0);
}

// --- RED-061: panic isolation (DAG exit test) ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn panic_rolls_back_answers_500_and_the_shard_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let before = shard.store.snapshot();
    let baseline_records = logged_records(dir.path());

    let err = shard
        .engine
        .call(client(3), "explode", vec![])
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(500), "{err}");
    assert!(err.to_string().contains("reducer bug"), "{err}");
    assert!(
        before.same_state(&shard.store.snapshot()),
        "the buffered insert must be discarded with the panic (RED-061)"
    );
    assert_eq!(
        logged_records(dir.path()),
        baseline_records,
        "no commit-log entry for a panicked call"
    );

    // The shard never dies: the very next call succeeds (FR-25).
    let receipt = shard
        .engine
        .call(
            client(3),
            "record_event",
            vec![FluxValue::Str("alive".into())],
        )
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, 1, "panic consumed no tx id");
    let events = shard.store.snapshot();
    let id = shard.store.table_id("Event").unwrap();
    assert_eq!(events.scan(id).unwrap().count(), 1);
}

// --- RED-010..RED-013: lifecycle ----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn on_init_runs_exactly_once_and_on_shard_start_every_boot() {
    let dir = tempfile::tempdir().unwrap();
    INIT_RUNS.store(0, Ordering::SeqCst);
    SHARD_START_RUNS.store(0, Ordering::SeqCst);

    {
        let shard = boot(dir.path());
        assert!(shard.fresh, "no checkpoint and no log yet");
        let report = shard.engine.start(shard.fresh).await.unwrap();
        assert_eq!(report.ran_on_init, ["seed_config"]);
        assert_eq!(report.ran_on_shard_start, ["warm_caches"]);
        // The seed row committed (RED-010).
        let id = shard.store.table_id("Event").unwrap();
        assert_eq!(shard.store.snapshot().scan(id).unwrap().count(), 1);
        shard.engine.pipeline().log().wait_durable(1).await.unwrap();
    }
    {
        // Second boot recovers the seed: NOT fresh, on_init skipped,
        // on_shard_start runs again (RED-013).
        let shard = boot(dir.path());
        assert!(!shard.fresh, "recovered from the log");
        let report = shard.engine.start(shard.fresh).await.unwrap();
        assert!(
            report.ran_on_init.is_empty(),
            "on_init is once-ever (RED-010)"
        );
        assert_eq!(report.ran_on_shard_start, ["warm_caches"]);
        let id = shard.store.table_id("Event").unwrap();
        assert_eq!(
            shard.store.snapshot().scan(id).unwrap().count(),
            1,
            "no duplicate seed"
        );
    }
    assert_eq!(INIT_RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(SHARD_START_RUNS.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_and_disconnect_drive_presence_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let online = shard.store.table_id("OnlineUser").unwrap();

    let ana = Identity::from_bytes([0xA1; 32]);
    let bo = Identity::from_bytes([0xB2; 32]);
    shard
        .engine
        .client_connected(ana, ConnectionId::new(11))
        .await
        .unwrap();
    shard
        .engine
        .client_connected(bo, ConnectionId::new(22))
        .await
        .unwrap();

    let snapshot = shard.store.snapshot();
    assert_eq!(
        snapshot.scan(online).unwrap().count(),
        2,
        "both online (UC-1)"
    );
    let row = snapshot
        .query_pk(online, &[RowValue::Identity(ana)])
        .unwrap()
        .unwrap();
    assert_eq!(
        row.value(1),
        Some(&RowValue::ConnectionId(ConnectionId::new(11)))
    );

    shard
        .engine
        .client_disconnected(ana, ConnectionId::new(11))
        .await
        .unwrap();
    let snapshot = shard.store.snapshot();
    assert!(
        snapshot
            .query_pk(online, &[RowValue::Identity(ana)])
            .unwrap()
            .is_none(),
        "disconnect removes presence (RED-012)"
    );
    assert_eq!(snapshot.scan(online).unwrap().count(), 1, "bo stays online");
}

// --- RED-030/RED-031: read-only views ------------------------------------------

fn count_events(
    ctx: &fluxum_core::reducer::ViewContext<'_>,
    view_args: &[FluxValue],
) -> Result<serde_json::Value> {
    args::check_arity("count_events", view_args, 1)?;
    let prefix: String = args::decode_arg("count_events", view_args, 0, "prefix")?;
    let count = ctx
        .tx
        .scan_where::<Event>(|event| event.label.starts_with(&prefix))?
        .len();
    Ok(serde_json::json!({ "count": count }))
}

static COUNT_EVENTS: ViewDef = ViewDef {
    name: "count_events",
    handler: count_events,
};

#[tokio::test(flavor = "multi_thread")]
async fn views_read_committed_state_and_reject_unknown_names() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    for label in ["alert:high", "alert:low", "info:boot"] {
        shard
            .engine
            .call(
                client(4),
                "record_event",
                vec![FluxValue::Str(label.into())],
            )
            .await
            .unwrap();
    }

    let views = ViewRegistry::from_defs([&COUNT_EVENTS]).unwrap();
    assert!(views.contains("count_events"));
    let snapshot = shard.store.snapshot();
    let result = views
        .dispatch(
            "count_events",
            &snapshot,
            SHARD,
            &[FluxValue::Str("alert:".into())],
        )
        .unwrap();
    assert_eq!(result, serde_json::json!({ "count": 2 }));

    // query_pk through the read-only handle too (RED-031 surface).
    let via_pk = fluxum_core::reducer::ReadOnlyTxHandle::new(&snapshot)
        .query_pk::<Event>(1)
        .unwrap()
        .unwrap();
    assert_eq!(via_pk.label, "alert:high");

    let err = views
        .dispatch("no_such_view", &snapshot, SHARD, &[])
        .unwrap_err();
    assert_eq!(err.query_code(), Some(404), "{err}");

    // Duplicate view names abort startup.
    let err = match ViewRegistry::from_defs([&COUNT_EVENTS, &COUNT_EVENTS]) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("duplicate view names must abort startup"),
    };
    assert!(err.contains("duplicate view name"), "{err}");
}
