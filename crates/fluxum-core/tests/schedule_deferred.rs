//! T3.4 deferred-scheduling suite (SPEC-004 RED-021..RED-025 acceptance
//! 6/11/12; FR-22): `ctx.schedule_after` fires once and removes its row in
//! the same transaction; scheduling inside a rolled-back transaction never
//! fires; pending rows survive restart and past-due entries fire once with
//! no backfill; recurring entries reschedule from the intended time
//! (anti-drift); a self-rescheduling chain runs 10+ cycles; firings run
//! under the server identity with the nil connection; schedule-only
//! reducers reject client calls with 403 unless client-callable.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerDef, ReducerEngine,
    ReducerRegistry, args,
};
use fluxum_core::scheduler::{
    SCHEDULE_TABLE, ScheduleDef, ScheduleEntry, Scheduler, SchedulerHandle, SchedulerOptions,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};

const SHARD: u32 = 17;
const EPOCH: u64 = 1;

// --- Fired-mark table --------------------------------------------------------

static MARK_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "label",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "fired_at_us",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "server_context",
        ty: FluxType::Bool,
    },
];

static MARK: TableSchema = TableSchema {
    name: "Mark",
    columns: MARK_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Mark {
    id: u64,
    label: String,
    fired_at_us: i64,
    server_context: bool,
}

impl Table for Mark {
    type Pk = u64;

    const SCHEMA: &'static TableSchema = &MARK;

    fn primary_key(&self) -> u64 {
        self.id
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::U64(self.id),
            RowValue::Str(self.label),
            RowValue::I64(self.fired_at_us),
            RowValue::Bool(self.server_context),
        ]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::U64(id),
                RowValue::Str(label),
                RowValue::I64(fired_at_us),
                RowValue::Bool(server_context),
            ] => Ok(Self {
                id: *id,
                label: label.clone(),
                fired_at_us: *fired_at_us,
                server_context: *server_context,
            }),
            other => Err(FluxumError::Storage(format!(
                "Mark: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

// --- Reducers ------------------------------------------------------------------

/// The RED-025 context witness: records whether the call ran under the
/// server identity with the reserved nil connection.
fn append_mark(ctx: &ReducerContext<'_, '_, '_>, call_args: &[FluxValue]) -> Result<()> {
    args::check_arity("append_mark", call_args, 1)?;
    let label: String = args::decode_arg("append_mark", call_args, 0, "label")?;
    let server_context = ctx.identity == fluxum_core::auth::server_identity("sched-test")
        && ctx.connection_id == ConnectionId::new(0);
    ctx.tx.insert(Mark {
        id: 0,
        label,
        fired_at_us: Timestamp::now().as_micros(),
        server_context,
    })?;
    Ok(())
}

fn check_one_str(call_args: &[FluxValue]) -> Result<()> {
    args::check_arity("append_mark", call_args, 1)?;
    let _ = args::decode_arg::<String>("append_mark", call_args, 0, "label")?;
    Ok(())
}

/// Slow recurring handler for the RED-024 anti-drift phase.
fn slow_mark(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(Mark {
        id: 0,
        label: "slow".into(),
        fired_at_us: Timestamp::now().as_micros(),
        server_context: true,
    })?;
    std::thread::sleep(Duration::from_millis(250));
    Ok(())
}

/// Schedules a mark, then fails: the enqueue must roll back with it.
fn schedule_then_fail(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.schedule_after(
        Duration::from_millis(30),
        "append_mark",
        &[FluxValue::Str("never".into())],
    )?;
    Err(FluxumError::Reducer("business rule violated (test)".into()))
}

/// Schedules a mark and commits.
fn schedule_ok(ctx: &ReducerContext<'_, '_, '_>, call_args: &[FluxValue]) -> Result<()> {
    args::check_arity("schedule_ok", call_args, 1)?;
    let label: String = args::decode_arg("schedule_ok", call_args, 0, "label")?;
    ctx.schedule_after(
        Duration::from_millis(40),
        "append_mark",
        &[FluxValue::Str(label)],
    )
}

/// RED-022: a one-shot chain forming a recurring schedule.
fn rearm(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(Mark {
        id: 0,
        label: "cycle".into(),
        fired_at_us: Timestamp::now().as_micros(),
        server_context: true,
    })?;
    ctx.schedule_after(Duration::from_millis(30), "rearm", &[])
}

fn check_none(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static APPEND_MARK: ReducerDef = ReducerDef {
    name: "append_mark",
    handler: append_mark,
    check_args: check_one_str,
    client_callable: true,
    max_rate_per_sec: 0,
};
static SLOW_MARK: ReducerDef = ReducerDef {
    name: "slow_mark",
    handler: slow_mark,
    check_args: check_none,
    client_callable: false, // schedule-only (RED-025)
    max_rate_per_sec: 0,
};
static SCHEDULE_THEN_FAIL: ReducerDef = ReducerDef {
    name: "schedule_then_fail",
    handler: schedule_then_fail,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};
static SCHEDULE_OK: ReducerDef = ReducerDef {
    name: "schedule_ok",
    handler: schedule_ok,
    check_args: check_one_str,
    client_callable: true,
    max_rate_per_sec: 0,
};
static REARM: ReducerDef = ReducerDef {
    name: "rearm",
    handler: rearm,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};

// --- Harness -------------------------------------------------------------------

struct Shard {
    engine: ReducerEngine,
    store: Arc<MemStore>,
    pipeline: TxPipeline,
    registry: Arc<ReducerRegistry>,
}

fn boot(dir: &Path) -> Shard {
    let schema = Schema::from_tables([&MARK, &SCHEDULE_TABLE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(&dir.join("log"), SHARD, EPOCH, CommitLogOptions::default()).unwrap(),
    );
    let repo = CheckpointRepo::open(&dir.join("snapshots")).unwrap();
    recover(&store, &repo, &dir.join("log"), SHARD).unwrap();
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(
        ReducerRegistry::from_defs([
            &APPEND_MARK,
            &SLOW_MARK,
            &SCHEDULE_THEN_FAIL,
            &SCHEDULE_OK,
            &REARM,
        ])
        .unwrap(),
    );
    let engine = ReducerEngine::new(
        pipeline.clone(),
        Arc::clone(&registry),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("sched-test"),
    );
    Shard {
        engine,
        store,
        pipeline,
        registry,
    }
}

async fn start_scheduler(shard: &Shard, schedules: Vec<&'static ScheduleDef>) -> SchedulerHandle {
    Scheduler::new(
        shard.pipeline.clone(),
        Arc::clone(&shard.registry),
        SHARD,
        fluxum_core::auth::server_identity("sched-test"),
        SchedulerOptions::default(),
        vec![],
        schedules,
    )
    .unwrap()
    .start()
    .await
    .unwrap()
}

fn client() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([0xC1; 32]),
        connection_id: ConnectionId::new(77),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

fn marks(shard: &Shard) -> Vec<Mark> {
    let id = shard.store.table_id("Mark").unwrap();
    shard
        .store
        .snapshot()
        .scan(id)
        .unwrap()
        .map(|row| Mark::from_values(row.values()).unwrap())
        .collect()
}

fn pending_schedule_rows(shard: &Shard) -> Vec<ScheduleEntry> {
    let id = shard.store.table_id("__schedule__").unwrap();
    shard
        .store
        .snapshot()
        .scan(id)
        .unwrap()
        .map(|row| ScheduleEntry::from_values(row.values()).unwrap())
        .collect()
}

async fn wait_for_marks(shard: &Shard, at_least: usize, timeout: Duration) -> Vec<Mark> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = marks(shard);
        if current.len() >= at_least || tokio::time::Instant::now() >= deadline {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// --- RED-021/RED-023/RED-025: one-shot delivery ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schedule_after_fires_once_atomically_under_the_server_context() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let scheduler = start_scheduler(&shard, vec![]).await;

    shard
        .engine
        .call(
            client(),
            "schedule_ok",
            vec![FluxValue::Str("deferred".into())],
        )
        .await
        .unwrap();
    assert_eq!(pending_schedule_rows(&shard).len(), 1, "row committed");

    let fired = wait_for_marks(&shard, 1, Duration::from_secs(2)).await;
    assert_eq!(fired.len(), 1, "fires exactly once");
    assert_eq!(fired[0].label, "deferred", "args round-trip (MessagePack)");
    assert!(
        fired[0].server_context,
        "fired under server identity + ConnectionId(0) (RED-025)"
    );
    // Removal happened in the same transaction as the execution (RED-023).
    assert!(pending_schedule_rows(&shard).is_empty());

    // No second delivery.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(marks(&shard).len(), 1);
    scheduler.stop().await;
}

// --- RED-021: rollback safety -----------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn scheduling_inside_a_rolled_back_transaction_never_fires() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let scheduler = start_scheduler(&shard, vec![]).await;

    let err = shard
        .engine
        .call(client(), "schedule_then_fail", vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, FluxumError::Reducer(_)), "{err:?}");
    assert!(
        pending_schedule_rows(&shard).is_empty(),
        "enqueue rolled back"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        marks(&shard).is_empty(),
        "a rolled-back enqueue never fires"
    );
    scheduler.stop().await;
}

// --- RED-023: restart rescan, past-due fires once, no backfill --------------------

#[tokio::test(flavor = "multi_thread")]
async fn pending_rows_survive_restart_and_past_due_fires_once_without_backfill() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Boot A: commit one past-due one-shot and one recurring entry that
        // "missed" five occurrences — then die without ever firing them.
        let shard = boot(dir.path());
        let now_us = Timestamp::now().as_micros();
        let registry = Arc::clone(&shard.registry);
        let caller = ReducerCaller {
            identity: fluxum_core::auth::server_identity("sched-test"),
            connection_id: ConnectionId::new(0),
            timestamp: Timestamp::now(),
            shard_id: SHARD,
        };
        let receipt = shard
            .pipeline
            .call(Box::new(move |tx| {
                fluxum_core::reducer::with_context(&registry, caller, tx, |ctx| {
                    ctx.tx.insert(ScheduleEntry {
                        id: 0,
                        reducer_name: "append_mark".into(),
                        args: rmp_serde::to_vec(&[FluxValue::Str("past-due".into())])
                            .map_err(|e| FluxumError::Storage(e.to_string()))?,
                        execute_at_us: now_us - 500_000,
                        period_us: 0,
                        shard_id: SHARD,
                    })?;
                    ctx.tx.insert(ScheduleEntry {
                        id: 0,
                        reducer_name: "rearm".into(),
                        args: rmp_serde::to_vec(&Vec::<FluxValue>::new())
                            .map_err(|e| FluxumError::Storage(e.to_string()))?,
                        // Five 100 ms occurrences in the past.
                        execute_at_us: now_us - 500_000,
                        period_us: 100_000,
                        shard_id: SHARD,
                    })?;
                    Ok(())
                })
            }))
            .await
            .unwrap();
        shard
            .pipeline
            .log()
            .wait_durable(receipt.tx_id)
            .await
            .unwrap();
        // kill -9: no scheduler ever ran, no checkpoint, just the log.
    }
    {
        // Boot B: recovery + scheduler. Both entries are due immediately.
        let shard = boot(dir.path());
        assert_eq!(pending_schedule_rows(&shard).len(), 2, "rows survived");
        let scheduler = start_scheduler(&shard, vec![]).await;

        let fired = wait_for_marks(&shard, 2, Duration::from_secs(2)).await;
        let past_due = fired.iter().filter(|m| m.label == "past-due").count();
        assert_eq!(past_due, 1, "past-due one-shot fires exactly once");

        // The recurring entry fired once immediately — never five times for
        // the five missed occurrences (RED-023 no-backfill). Its handler
        // re-arms one-shot chains, so within a slack window the cycle count
        // stays far below a backfill burst.
        let cycles_now = fired.iter().filter(|m| m.label == "cycle").count();
        assert!(
            (1..=2).contains(&cycles_now),
            "one immediate firing, no 5-occurrence backfill burst; got {cycles_now}"
        );
        scheduler.stop().await;
    }
}

// --- RED-024: recurring anti-drift --------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn recurring_entries_reschedule_from_the_intended_time() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    // A 500 ms recurring entry whose handler takes 250 ms: completion-time
    // rescheduling would space firings 750 ms apart and drift +250 ms per
    // cycle; intended-time rescheduling keeps them at 500 ms.
    let now_us = Timestamp::now().as_micros();
    let registry = Arc::clone(&shard.registry);
    let caller = ReducerCaller {
        identity: fluxum_core::auth::server_identity("sched-test"),
        connection_id: ConnectionId::new(0),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    };
    shard
        .pipeline
        .call(Box::new(move |tx| {
            fluxum_core::reducer::with_context(&registry, caller, tx, |ctx| {
                ctx.tx.insert(ScheduleEntry {
                    id: 0,
                    reducer_name: "slow_mark".into(),
                    args: rmp_serde::to_vec(&Vec::<FluxValue>::new())
                        .map_err(|e| FluxumError::Storage(e.to_string()))?,
                    execute_at_us: now_us + 500_000,
                    period_us: 500_000,
                    shard_id: SHARD,
                })?;
                Ok(())
            })
        }))
        .await
        .unwrap();
    let scheduler = start_scheduler(&shard, vec![]).await;

    let fired = wait_for_marks(&shard, 3, Duration::from_secs(4)).await;
    scheduler.stop().await;
    assert!(
        fired.len() >= 3,
        "three firings within budget: {}",
        fired.len()
    );
    let mut times: Vec<i64> = fired.iter().map(|m| m.fired_at_us).collect();
    times.sort_unstable();
    for pair in times.windows(2).take(2) {
        let gap_ms = (pair[1] - pair[0]) / 1_000;
        assert!(
            (380..=650).contains(&gap_ms),
            "intended-time cadence is ~500 ms (drifting impls give ~750 ms), got {gap_ms} ms"
        );
    }
    // The recurring row is still pending (rescheduled, not deleted).
    assert_eq!(pending_schedule_rows(&shard).len(), 1);
}

// --- RED-022: self-rescheduling chain (acceptance 6 tail) ---------------------------

#[tokio::test(flavor = "multi_thread")]
async fn self_rescheduling_chain_runs_ten_consecutive_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let scheduler = start_scheduler(&shard, vec![]).await;

    shard.engine.call(client(), "rearm", vec![]).await.unwrap();
    let fired = wait_for_marks(&shard, 10, Duration::from_secs(5)).await;
    scheduler.stop().await;
    assert!(
        fired.iter().filter(|m| m.label == "cycle").count() >= 10,
        "10+ consecutive cycles (RED-022), got {}",
        fired.len()
    );
}

// --- RED-025: schedule-only client rejection ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schedule_only_reducers_reject_clients_with_403() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());

    // slow_mark is schedule-only: clients get a wire-ready 403 with no
    // transaction; append_mark (client_callable) is admitted.
    let err = shard
        .engine
        .call(client(), "slow_mark", vec![])
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_SCHEDULE_ONLY),
        "{err}"
    );
    assert!(err.to_string().contains("schedule-only"), "{err}");

    shard
        .engine
        .call(
            client(),
            "append_mark",
            vec![FluxValue::Str("direct".into())],
        )
        .await
        .unwrap();
    assert_eq!(marks(&shard).len(), 1);

    // schedule_after validates the target reducer name.
    let err = shard
        .engine
        .call(client(), "schedule_ok", vec![FluxValue::Str("x".into())])
        .await;
    assert!(err.is_ok());
    let registry = Arc::clone(&shard.registry);
    let caller = client();
    let err = shard
        .pipeline
        .call(Box::new(move |tx| {
            fluxum_core::reducer::with_context(&registry, caller, tx, |ctx| {
                ctx.schedule_after(Duration::from_millis(1), "no_such_reducer", &[])
            })
        }))
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_UNKNOWN),
        "{err}"
    );
}

// --- Scheduler assembly validation (RED-020/RED-021) --------------------------------

static BAD_RATE_TICK: fluxum_core::scheduler::TickDef = fluxum_core::scheduler::TickDef {
    name: "append_mark",
    rate_hz: 0,
};
static GHOST_TICK: fluxum_core::scheduler::TickDef = fluxum_core::scheduler::TickDef {
    name: "no_such_tick",
    rate_hz: 10,
};
static GHOST_SCHEDULE: ScheduleDef = ScheduleDef {
    name: "no_such_scheduled",
    delay_us: 1,
    period_us: 0,
};

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_assembly_rejects_invalid_tick_and_schedule_defs() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let build = |ticks, schedules| {
        Scheduler::new(
            shard.pipeline.clone(),
            Arc::clone(&shard.registry),
            SHARD,
            fluxum_core::auth::server_identity("sched-test"),
            SchedulerOptions::default(),
            ticks,
            schedules,
        )
    };

    let err = build(vec![&BAD_RATE_TICK], vec![]).err().unwrap();
    assert!(err.to_string().contains("rate = 0"), "{err}");
    let err = build(vec![&GHOST_TICK], vec![]).err().unwrap();
    assert!(
        err.to_string().contains("not in the reducer registry"),
        "{err}"
    );
    let err = build(vec![], vec![&GHOST_SCHEDULE]).err().unwrap();
    assert!(
        err.to_string()
            .contains("scheduled reducer `no_such_scheduled`"),
        "{err}"
    );
}

// --- ScheduleEntry Table plumbing ---------------------------------------------------

#[test]
fn schedule_entry_table_roundtrip_and_shape_errors() {
    let entry = ScheduleEntry {
        id: 9,
        reducer_name: "append_mark".into(),
        args: vec![1, 2],
        execute_at_us: 100,
        period_us: 0,
        shard_id: SHARD,
    };
    assert_eq!(entry.primary_key(), 9);
    let values = entry.clone().into_values();
    assert_eq!(ScheduleEntry::from_values(&values).unwrap(), entry);
    assert_eq!(ScheduleEntry::pk_values(&9), vec![RowValue::U64(9)]);

    // A malformed row shape is a typed storage error, never a panic.
    let err = ScheduleEntry::from_values(&[RowValue::Bool(true)]).unwrap_err();
    assert!(err.to_string().contains("unexpected row shape"), "{err}");
}

// --- RED-021: undecodable args back off instead of hot-looping ----------------------

#[tokio::test(flavor = "multi_thread")]
async fn undecodable_schedule_args_fail_the_firing_and_back_off() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path());
    let now_us = Timestamp::now().as_micros();
    let registry = Arc::clone(&shard.registry);
    let caller = ReducerCaller {
        identity: fluxum_core::auth::server_identity("sched-test"),
        connection_id: ConnectionId::new(0),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    };
    shard
        .pipeline
        .call(Box::new(move |tx| {
            fluxum_core::reducer::with_context(&registry, caller, tx, |ctx| {
                ctx.tx.insert(ScheduleEntry {
                    id: 0,
                    reducer_name: "append_mark".into(),
                    // 0xC1 is never valid MessagePack.
                    args: vec![0xC1],
                    execute_at_us: now_us - 1_000,
                    period_us: 0,
                    shard_id: SHARD,
                })?;
                Ok(())
            })
        }))
        .await
        .unwrap();

    let scheduler = start_scheduler(&shard, vec![]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    scheduler.stop().await;

    assert!(marks(&shard).is_empty(), "a failed decode never dispatches");
    assert_eq!(
        pending_schedule_rows(&shard).len(),
        1,
        "at-least-once: the row stays for re-delivery after the backoff"
    );
}

// --- The worker tolerates a shard without the __schedule__ table --------------------

#[tokio::test(flavor = "multi_thread")]
async fn scheduler_idles_on_a_shard_without_the_schedule_table() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_tables([&MARK]).unwrap(); // no __schedule__
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            EPOCH,
            fluxum_core::commitlog::CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(ReducerRegistry::from_defs([&APPEND_MARK]).unwrap());

    let scheduler = Scheduler::new(
        pipeline,
        registry,
        SHARD,
        fluxum_core::auth::server_identity("sched-test"),
        SchedulerOptions::default(),
        vec![],
        vec![],
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    assert!(scheduler.tick_stats("nope").is_none(), "no ticks running");
    // A few polls against the missing table are clean no-ops.
    tokio::time::sleep(Duration::from_millis(50)).await;
    scheduler.stop().await;
}

// --- RED-020: failing ticks roll back and a long stall warns + resets ---------------

static STALL_FIRED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn failing_slow_tick(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    // One 80 ms stall (8 periods at 100 Hz) on the first firing, then fast.
    if !STALL_FIRED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(80));
    }
    Err(FluxumError::Reducer("tick business error (test)".into()))
}

static FAILING_SLOW_TICK: ReducerDef = ReducerDef {
    name: "failing_slow_tick",
    handler: failing_slow_tick,
    check_args: check_none,
    client_callable: false,
    max_rate_per_sec: 0,
};

static FAILING_TICK_DEF: fluxum_core::scheduler::TickDef = fluxum_core::scheduler::TickDef {
    name: "failing_slow_tick",
    rate_hz: 100,
};

#[tokio::test(flavor = "multi_thread")]
async fn failing_ticks_roll_back_and_long_stalls_warn_once_and_reset() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_tables([&MARK, &SCHEDULE_TABLE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            EPOCH,
            fluxum_core::commitlog::CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(ReducerRegistry::from_defs([&FAILING_SLOW_TICK]).unwrap());

    let handle = Scheduler::new(
        pipeline,
        registry,
        SHARD,
        fluxum_core::auth::server_identity("sched-test"),
        SchedulerOptions::default(),
        vec![&FAILING_TICK_DEF],
        vec![],
    )
    .unwrap()
    .start()
    .await
    .unwrap();

    // Wait until the stalled first firing plus a few more have happened.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stats = handle.tick_stats("failing_slow_tick").unwrap();
        let executions = stats.executions.load(std::sync::atomic::Ordering::SeqCst);
        let warnings = stats.warnings.load(std::sync::atomic::Ordering::SeqCst);
        if (executions >= 3 && warnings >= 1) || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stats = handle.tick_stats("failing_slow_tick").unwrap();
    let executions = stats.executions.load(std::sync::atomic::Ordering::SeqCst);
    let warnings = stats.warnings.load(std::sync::atomic::Ordering::SeqCst);
    handle.stop().await;

    assert!(executions >= 3, "the clock survives failing ticks");
    assert!(
        warnings >= 1,
        "an 8-period stall must warn and reset the clock (RED-020)"
    );
    // Every firing rolled back: no rows were ever committed by the tick.
    assert_eq!(
        store
            .snapshot()
            .scan(store.table_id("Mark").unwrap())
            .unwrap()
            .count(),
        0
    );
}

// --- Static #[fluxum::schedule] defs: enqueue at start, restart-safe ---------------

static STATIC_MARK: ScheduleDef = ScheduleDef {
    name: "append_mark_static",
    delay_us: 40_000,
    period_us: 0,
};

fn append_mark_static(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    append_mark(ctx, &[FluxValue::Str("static".into())])
}

static APPEND_MARK_STATIC: ReducerDef = ReducerDef {
    name: "append_mark_static",
    handler: append_mark_static,
    check_args: check_none,
    client_callable: false,
    max_rate_per_sec: 0,
};

#[tokio::test(flavor = "multi_thread")]
async fn static_defs_enqueue_at_start_without_duplicating_pending_rows() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_tables([&MARK, &SCHEDULE_TABLE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            EPOCH,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(ReducerRegistry::from_defs([&APPEND_MARK_STATIC]).unwrap());

    let build = || {
        Scheduler::new(
            pipeline.clone(),
            Arc::clone(&registry),
            SHARD,
            fluxum_core::auth::server_identity("sched-test"),
            SchedulerOptions {
                // Slow poll: the pending row must still be there when the
                // second scheduler starts.
                poll_interval: Duration::from_millis(300),
                ..SchedulerOptions::default()
            },
            vec![],
            vec![&STATIC_MARK],
        )
        .unwrap()
    };

    // Two schedulers started back-to-back (a crash-restart in miniature):
    // the second start sees the pending row and does not double-enqueue.
    let first = build().start().await.unwrap();
    let second = build().start().await.unwrap();
    let table = store.table_id("__schedule__").unwrap();
    let pending = store.snapshot().scan(table).unwrap().count();
    assert_eq!(pending, 1, "deduplicated by reducer name at start");

    // The one-shot then fires exactly once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mark_table = store.table_id("Mark").unwrap();
    loop {
        let count = store.snapshot().scan(mark_table).unwrap().count();
        if count >= 1 || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(store.snapshot().scan(mark_table).unwrap().count(), 1);
    first.stop().await;
    second.stop().await;
}
