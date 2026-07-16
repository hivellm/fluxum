//! Scheduled execution (SPEC-004 §4, T3.4): the `#[fluxum::tick(rate)]`
//! fixed-timestep clock (RED-020) and the `#[fluxum::schedule]` /
//! `ctx.schedule_after` durable deferred reducers over the `__schedule__`
//! system table (RED-021..RED-025; FR-21/FR-22).
//!
//! # Architecture (OQ-4 decision)
//!
//! Scheduled work runs on **dedicated scheduler tasks feeding the reducer
//! queue** — not on the writer thread. Each `#[fluxum::tick]` gets its own
//! task holding the absolute-target clock; one [`ScheduleWorker`] task per
//! shard polls `__schedule__` (the polling design RED-021 specifies). Both
//! submit ordinary jobs to the T3.1 [`TxPipeline`], so scheduled executions
//! serialize with client calls on the single writer and inherit its
//! `catch_unwind` boundary (TXN-022) — a panicking tick cannot kill the
//! shard any more than a panicking client call can.
//!
//! # Semantics pinned here
//!
//! - **Fixed timestep, no accumulation** (RED-020): absolute `next_target`
//!   advanced by exactly one period per firing; a 1–3-period stall re-fires
//!   immediately with no warning; a stall past 3 periods logs exactly one
//!   warning and resets the clock — never a catch-up burst. A tick function
//!   never runs concurrently with itself (the worker awaits each firing).
//! - **Rollback-safe at-least-once delivery** (RED-021/RED-023): a firing
//!   re-reads the committed `__schedule__` row inside its own transaction —
//!   a row whose scheduling transaction rolled back (or that was deleted
//!   since) is a no-op. One-shot rows are deleted, recurring rows
//!   rescheduled, **in the same transaction** as the execution: success is
//!   exactly-once, a crash before commit re-delivers.
//! - **Restart rescan, no backfill** (RED-023): the worker reads committed
//!   rows every poll, so recovery needs no special path — pending rows
//!   simply become due; a past-due row fires once immediately and missed
//!   occurrences of a recurring entry are never backfilled.
//! - **Recurring anti-drift** (RED-024): the next occurrence is
//!   `intended + period`; only when that is already in the past does it
//!   rebase to `now + period`.
//! - **Execution context** (RED-025): firings run under the server identity
//!   with the reserved nil `ConnectionId(0)`; schedule-only reducers reject
//!   client `ReducerCall`s with 403 at admission
//!   ([`ReducerRegistry::check_call`]).

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{FluxumError, Result};
use crate::reducer::{FluxValue, ReducerCaller, ReducerRegistry, with_context};
use crate::schema::{ColumnSchema, FluxType, Table, TableAccess, TableSchema, VisibilityRule};
use crate::store::{PkBytes, Row, RowValue, TableId};
use crate::txn::{CommitReceipt, TxPipeline};
use crate::types::{ConnectionId, Identity, Timestamp};

/// Stored name of the schedule system table (RED-021).
pub const SCHEDULE_TABLE_NAME: &str = "__schedule__";

/// Sentinel for "the fired row was absent at re-read" (RED-021 rollback
/// safety): the firing transaction rolls back with this message and the
/// worker treats it as a clean no-op. Never surfaces to callers.
const ABSENT_ROW: &str = "__fluxum_schedule_row_absent__";

static SCHEDULE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "reducer_name",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "args",
        ty: FluxType::Bytes,
    },
    ColumnSchema {
        name: "execute_at_us",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "period_us",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "shard_id",
        ty: FluxType::U32,
    },
];

/// The `__schedule__` system table (RED-021): durable pending scheduled
/// calls — they survive crash recovery like any other table.
///
/// Defined by the runtime, not by application code; the server assembly
/// must include it when building the [`crate::schema::Schema`] of a shard
/// that runs a [`Scheduler`] (exactly like
/// [`crate::migration::SCHEMA_META`]). `Private`: never sent to clients.
pub static SCHEDULE_TABLE: TableSchema = TableSchema {
    name: SCHEDULE_TABLE_NAME,
    columns: SCHEDULE_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// One pending scheduled call (a `__schedule__` row).
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleEntry {
    /// Auto-assigned schedule id.
    pub id: u64,
    /// The registered reducer to fire.
    pub reducer_name: String,
    /// MessagePack-encoded `Vec<FluxValue>` argument list.
    pub args: Vec<u8>,
    /// When to fire, µs since the Unix epoch.
    pub execute_at_us: i64,
    /// Recurrence period in µs; `0` = one-shot (RED-022/RED-024).
    pub period_us: i64,
    /// Owning shard.
    pub shard_id: u32,
}

impl Table for ScheduleEntry {
    type Pk = u64;

    const SCHEMA: &'static TableSchema = &SCHEDULE_TABLE;

    fn primary_key(&self) -> u64 {
        self.id
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::U64(self.id),
            RowValue::Str(self.reducer_name),
            RowValue::Bytes(self.args),
            RowValue::I64(self.execute_at_us),
            RowValue::I64(self.period_us),
            RowValue::U32(self.shard_id),
        ]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::U64(id),
                RowValue::Str(reducer_name),
                RowValue::Bytes(args),
                RowValue::I64(execute_at_us),
                RowValue::I64(period_us),
                RowValue::U32(shard_id),
            ] => Ok(Self {
                id: *id,
                reducer_name: reducer_name.clone(),
                args: args.clone(),
                execute_at_us: *execute_at_us,
                period_us: *period_us,
                shard_id: *shard_id,
            }),
            other => Err(FluxumError::Storage(format!(
                "__schedule__: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

/// One `#[fluxum::tick(rate = N)]` in the link-time registry (RED-020).
/// The function itself is registered as a (schedule-only) reducer under
/// `name`; the tick worker fires it by name.
pub struct TickDef {
    /// The tick function's reducer name.
    pub name: &'static str,
    /// Firing rate in Hz (`period = 1s / rate`).
    pub rate_hz: u32,
}

inventory::collect!(TickDef);

/// Iterate every `#[fluxum::tick]` registered in this binary.
pub fn registered_ticks() -> impl Iterator<Item = &'static TickDef> {
    inventory::iter::<TickDef>()
}

/// One `#[fluxum::schedule(...)]` in the link-time registry (RED-021): a
/// statically declared deferred reducer, enqueued at shard start.
pub struct ScheduleDef {
    /// The scheduled function's reducer name.
    pub name: &'static str,
    /// Delay from shard start to the first firing, µs.
    pub delay_us: i64,
    /// Recurrence period in µs; `0` = one-shot.
    pub period_us: i64,
}

inventory::collect!(ScheduleDef);

/// Iterate every `#[fluxum::schedule]` registered in this binary.
pub fn registered_schedules() -> impl Iterator<Item = &'static ScheduleDef> {
    inventory::iter::<ScheduleDef>()
}

/// Live counters of one tick worker (RED-020 observability and the
/// tick-drift verification surface).
#[derive(Debug, Default)]
pub struct TickStats {
    /// Completed firings (committed or rolled back — each is one attempt).
    pub executions: AtomicU64,
    /// Clock resets after a >3-period stall (exactly one warning each).
    pub warnings: AtomicU64,
}

/// Tuning knobs for a [`Scheduler`].
#[derive(Debug, Clone, Copy)]
pub struct SchedulerOptions {
    /// How often the [`ScheduleWorker`] polls `__schedule__` for due rows
    /// (RED-021). Default 5 ms.
    pub poll_interval: Duration,
    /// How long a failing scheduled entry backs off before re-delivery
    /// (at-least-once without a hot loop). Recurring entries use
    /// `max(period, retry_backoff)`. Default 1 s.
    pub retry_backoff: Duration,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(5),
            retry_backoff: Duration::from_secs(1),
        }
    }
}

/// The per-shard scheduler assembly (T3.4): tick clocks + schedule worker.
pub struct Scheduler {
    pipeline: TxPipeline,
    registry: Arc<ReducerRegistry>,
    shard_id: u32,
    server_identity: Identity,
    options: SchedulerOptions,
    ticks: Vec<&'static TickDef>,
    schedules: Vec<&'static ScheduleDef>,
}

impl Scheduler {
    /// Assemble a scheduler with explicit tick/schedule defs (the link-time
    /// path is [`Scheduler::from_registered`]).
    pub fn new(
        pipeline: TxPipeline,
        registry: Arc<ReducerRegistry>,
        shard_id: u32,
        server_identity: Identity,
        options: SchedulerOptions,
        ticks: Vec<&'static TickDef>,
        schedules: Vec<&'static ScheduleDef>,
    ) -> Result<Self> {
        for def in &ticks {
            if def.rate_hz == 0 {
                return Err(FluxumError::Schema(format!(
                    "tick `{}` declares rate = 0: the rate is in Hz and must be >= 1 (RED-020)",
                    def.name
                )));
            }
            if !registry.contains(def.name) {
                return Err(FluxumError::Schema(format!(
                    "tick `{}` is not in the reducer registry (RED-020)",
                    def.name
                )));
            }
        }
        for def in &schedules {
            if !registry.contains(def.name) {
                return Err(FluxumError::Schema(format!(
                    "scheduled reducer `{}` is not in the reducer registry (RED-021)",
                    def.name
                )));
            }
        }
        Ok(Self {
            pipeline,
            registry,
            shard_id,
            server_identity,
            options,
            ticks,
            schedules,
        })
    }

    /// [`Scheduler::new`] over the link-time `#[fluxum::tick]` /
    /// `#[fluxum::schedule]` registries.
    pub fn from_registered(
        pipeline: TxPipeline,
        registry: Arc<ReducerRegistry>,
        shard_id: u32,
        server_identity: Identity,
        options: SchedulerOptions,
    ) -> Result<Self> {
        Self::new(
            pipeline,
            registry,
            shard_id,
            server_identity,
            options,
            registered_ticks().collect(),
            registered_schedules().collect(),
        )
    }

    /// Start the scheduler: enqueue the static `#[fluxum::schedule]` defs
    /// (deduplicated by reducer name against pending committed rows, so a
    /// restart never double-enqueues), then spawn one task per tick and the
    /// schedule worker. Runs until [`SchedulerHandle::stop`].
    pub async fn start(self) -> Result<SchedulerHandle> {
        self.enqueue_static_schedules().await?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let mut tick_stats = HashMap::new();

        for def in &self.ticks {
            let stats = Arc::new(TickStats::default());
            tick_stats.insert(def.name, Arc::clone(&stats));
            tasks.push(tokio::spawn(tick_worker(
                FireContext {
                    pipeline: self.pipeline.clone(),
                    registry: Arc::clone(&self.registry),
                    shard_id: self.shard_id,
                    server_identity: self.server_identity,
                },
                def,
                stats,
                shutdown_rx.clone(),
            )));
        }

        tasks.push(tokio::spawn(schedule_worker(
            FireContext {
                pipeline: self.pipeline.clone(),
                registry: Arc::clone(&self.registry),
                shard_id: self.shard_id,
                server_identity: self.server_identity,
            },
            self.options,
            shutdown_rx,
        )));

        Ok(SchedulerHandle {
            shutdown: shutdown_tx,
            tasks,
            tick_stats,
        })
    }

    /// Insert one `__schedule__` row per static def whose reducer has no
    /// pending row yet (RED-021 "enqueued at shard start", restart-safe).
    async fn enqueue_static_schedules(&self) -> Result<()> {
        if self.schedules.is_empty() {
            return Ok(());
        }
        let defs: Vec<&'static ScheduleDef> = self.schedules.clone();
        let registry = Arc::clone(&self.registry);
        let caller = server_caller(self.server_identity, self.shard_id);
        let shard_id = self.shard_id;
        self.pipeline
            .call(Box::new(move |tx| {
                with_context(&registry, caller, tx, |ctx| {
                    let pending = ctx.tx.scan_all::<ScheduleEntry>()?;
                    let now_us = caller.timestamp.as_micros();
                    for def in defs {
                        if pending.iter().any(|row| row.reducer_name == def.name) {
                            continue;
                        }
                        ctx.tx.insert(ScheduleEntry {
                            id: 0,
                            reducer_name: def.name.to_owned(),
                            args: encode_args(&[])?,
                            execute_at_us: now_us.saturating_add(def.delay_us),
                            period_us: def.period_us,
                            shard_id,
                        })?;
                    }
                    Ok(())
                })
            }))
            .await
            .map(|_| ())
    }
}

/// Running scheduler tasks of one shard; dropping without
/// [`SchedulerHandle::stop`] aborts nothing (tasks keep running detached),
/// so assemblies should stop explicitly on shutdown.
pub struct SchedulerHandle {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    tick_stats: HashMap<&'static str, Arc<TickStats>>,
}

impl SchedulerHandle {
    /// Live counters of tick `name`.
    pub fn tick_stats(&self, name: &str) -> Option<&Arc<TickStats>> {
        self.tick_stats.get(name)
    }

    /// Signal shutdown and wait for every scheduler task to finish its
    /// in-flight firing.
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

/// Everything a firing needs (shared by tick and schedule workers).
#[derive(Clone)]
struct FireContext {
    pipeline: TxPipeline,
    registry: Arc<ReducerRegistry>,
    shard_id: u32,
    server_identity: Identity,
}

/// The RED-025 execution context: server identity, nil connection.
fn server_caller(identity: Identity, shard_id: u32) -> ReducerCaller {
    ReducerCaller {
        identity,
        connection_id: ConnectionId::new(0),
        timestamp: Timestamp::now(),
        shard_id,
    }
}

/// Encode a `FluxValue` argument list for a `__schedule__` row.
pub(crate) fn encode_args(args: &[FluxValue]) -> Result<Vec<u8>> {
    rmp_serde::to_vec(args)
        .map_err(|e| FluxumError::Storage(format!("schedule args encoding failed: {e}")))
}

/// The RED-020 fixed-timestep loop: absolute targets, immediate re-fire on
/// small stalls, one warning + clock reset past 3 periods, never concurrent
/// with itself (each firing is awaited).
async fn tick_worker(
    fire: FireContext,
    def: &'static TickDef,
    stats: Arc<TickStats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let period = Duration::from_micros(1_000_000 / u64::from(def.rate_hz));
    let mut next_target = Instant::now();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            () = tokio::time::sleep_until(next_target) => {}
        }
        let caller = server_caller(fire.server_identity, fire.shard_id);
        let registry = Arc::clone(&fire.registry);
        let result = fire
            .pipeline
            .call(Box::new(move |tx| {
                registry.dispatch(caller, def.name, &[], tx)
            }))
            .await;
        stats.executions.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = result {
            // Tick errors roll back and the clock keeps ticking (RED-004);
            // panics were already converted by the pipeline (TXN-022).
            tracing::error!(target: "fluxum::scheduler", tick = def.name, error = %e,
                "tick execution failed; transaction rolled back");
        }
        next_target += period;
        let now = Instant::now();
        if now > next_target + 3 * period {
            // Missed-deadline detection: one warning, clock reset, no
            // backlog accumulation (RED-020).
            stats.warnings.fetch_add(1, Ordering::SeqCst);
            tracing::warn!(target: "fluxum::scheduler", tick = def.name,
                rate_hz = def.rate_hz, "tick budget exceeded by >3 periods; clock reset");
            next_target = now;
        }
    }
}

/// The RED-021/RED-023 polling worker: every `poll_interval`, fire each
/// committed `__schedule__` row that is due (and not backing off after a
/// failure), one transaction per firing.
async fn schedule_worker(
    fire: FireContext,
    options: SchedulerOptions,
    mut shutdown: watch::Receiver<bool>,
) {
    // id → earliest next delivery attempt, for failed firings only.
    let mut backoff: HashMap<u64, Instant> = HashMap::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            () = tokio::time::sleep(options.poll_interval) => {}
        }
        let now_us = Timestamp::now().as_micros();
        let due = due_entries(&fire, now_us, &mut backoff);
        for entry in due {
            fire_entry(&fire, &options, entry, &mut backoff).await;
        }
    }
}

/// Committed rows due at `now_us`, skipping entries in failure backoff.
/// Reading the committed snapshot each poll IS the restart rescan: pending
/// rows survive recovery as ordinary table rows and a past-due row is
/// simply due on the first poll — it fires once, with no backfill of
/// missed occurrences (RED-023).
fn due_entries(
    fire: &FireContext,
    now_us: i64,
    backoff: &mut HashMap<u64, Instant>,
) -> Vec<ScheduleEntry> {
    let now = Instant::now();
    backoff.retain(|_, not_before| *not_before > now);
    let snapshot = fire.pipeline.store().snapshot();
    let Some(table) = fire.pipeline.store().table_id(SCHEDULE_TABLE_NAME) else {
        return Vec::new();
    };
    let Ok(rows) = snapshot.scan(table) else {
        return Vec::new();
    };
    let mut due: Vec<ScheduleEntry> = rows
        .filter_map(|row| ScheduleEntry::from_values(row.values()).ok())
        .filter(|entry| entry.execute_at_us <= now_us && !backoff.contains_key(&entry.id))
        .collect();
    due.sort_by_key(|entry| (entry.execute_at_us, entry.id));
    due
}

/// Fire one due entry in one transaction (RED-021/RED-023/RED-024):
/// committed re-read (absent = rolled-back scheduling or cancellation →
/// no-op), dispatch, then delete (one-shot) or intended-time reschedule
/// (recurring) — all atomically with the execution.
async fn fire_entry(
    fire: &FireContext,
    options: &SchedulerOptions,
    entry: ScheduleEntry,
    backoff: &mut HashMap<u64, Instant>,
) {
    let registry = Arc::clone(&fire.registry);
    let caller = server_caller(fire.server_identity, fire.shard_id);
    let id = entry.id;
    let result = fire
        .pipeline
        .call(Box::new(move |tx| {
            with_context(&registry, caller, tx, |ctx| {
                // Rollback safety at fire time (RED-021): the committed row
                // is the sole source of truth.
                let Some(row) = ctx.tx.query_pk::<ScheduleEntry>(id)? else {
                    return Err(FluxumError::Reducer(ABSENT_ROW.into()));
                };
                let args: Vec<FluxValue> = rmp_serde::from_slice(&row.args).map_err(|e| {
                    FluxumError::Storage(format!(
                        "__schedule__ row {id}: args failed MessagePack decode: {e}"
                    ))
                })?;
                ctx.tx.call(&row.reducer_name, &args)?;
                if row.period_us > 0 {
                    // RED-024: next occurrence from the INTENDED time; a
                    // next already in the past rebases to the present
                    // (no catch-up burst).
                    let now_us = caller.timestamp.as_micros();
                    let mut next = row.execute_at_us.saturating_add(row.period_us);
                    if next <= now_us {
                        next = now_us.saturating_add(row.period_us);
                    }
                    ctx.tx.upsert(ScheduleEntry {
                        execute_at_us: next,
                        ..row
                    })?;
                } else {
                    // Removal in the SAME transaction as the execution
                    // (RED-023): success = exactly-once.
                    ctx.tx.delete::<ScheduleEntry>(id)?;
                }
                Ok(())
            })
        }))
        .await;
    match result {
        Ok(_) => {}
        Err(FluxumError::Reducer(message)) if message == ABSENT_ROW => {
            // Scheduling transaction rolled back, or the entry was
            // cancelled: a clean no-op (RED-021).
        }
        Err(e) => {
            // At-least-once: the row stays committed; back off before the
            // next delivery attempt so a persistently failing handler
            // cannot hot-loop the writer.
            let period = Duration::from_micros(entry.period_us.max(0).unsigned_abs());
            let wait = options.retry_backoff.max(period);
            backoff.insert(id, Instant::now() + wait);
            tracing::error!(target: "fluxum::scheduler", schedule_id = id,
                reducer = %entry.reducer_name, error = %e,
                "scheduled execution failed; transaction rolled back, will re-deliver");
        }
    }
}

// ---------------------------------------------------------------------------
// Ephemeral TTL sweeper (SPEC-023 DMX-011)
// ---------------------------------------------------------------------------

/// TTL sweeper for ephemeral tables declaring `expire_after` (DMX-011).
///
/// A row expires `expire_after` after its **last write**, at sweep-cadence
/// granularity, tracked without touching the storage engine: the sweeper
/// keeps a per-PK *identity witness* — the [`Row`]'s shared allocation
/// ([`Row::same_identity`]) plus the time it was last observed to change. A
/// rewrite (upsert) replaces the row allocation, so the witness refreshes and
/// an actively-updated cursor never expires; a row untouched past its TTL is
/// deleted in an ordinary transaction whose delete diffs fan out to
/// subscribers like any commit.
///
/// The scan runs on a wait-free snapshot; a transaction is only started when
/// something is actually due (idle sweeps consume no `tx_id`), and each
/// doomed row is re-verified by identity inside the transaction, so a write
/// racing the sweep wins.
pub struct EphemeralSweeper {
    pipeline: TxPipeline,
    tables: Vec<SweepTable>,
    witnesses: Mutex<HashMap<(TableId, PkBytes), Witness>>,
}

#[derive(Clone, Copy)]
struct SweepTable {
    table: TableId,
    schema: &'static TableSchema,
    expire_after_us: i64,
}

struct Witness {
    row: Row,
    changed_at: Timestamp,
}

impl EphemeralSweeper {
    /// Build a sweeper for every registered ephemeral table with an
    /// `expire_after` that resolves against `pipeline`'s store. `None` when
    /// no table needs sweeping.
    pub fn from_registered(pipeline: TxPipeline) -> Option<Self> {
        let store = Arc::clone(pipeline.store());
        let tables: Vec<SweepTable> = crate::schema::registered_ephemeral()
            .filter_map(|def| {
                let expire_after_us = def.expire_after_us?;
                let table = store.table_id(def.table)?;
                let schema = store.table_schema(table)?;
                Some(SweepTable {
                    table,
                    schema,
                    expire_after_us,
                })
            })
            .collect();
        if tables.is_empty() {
            return None;
        }
        Some(Self {
            pipeline,
            tables,
            witnesses: Mutex::new(HashMap::new()),
        })
    }

    /// The recommended sweep interval: a quarter of the shortest TTL,
    /// clamped to `[100 ms, 5 s]`.
    pub fn cadence(&self) -> Duration {
        let min_us = self
            .tables
            .iter()
            .map(|t| t.expire_after_us)
            .min()
            .unwrap_or(1_000_000);
        Duration::from_micros(u64::try_from(min_us / 4).unwrap_or(1_000_000))
            .clamp(Duration::from_millis(100), Duration::from_secs(5))
    }

    /// One sweep at the wall clock.
    pub async fn sweep_once(&self) -> Result<Option<CommitReceipt>> {
        self.sweep_once_at(Timestamp::now()).await
    }

    /// One sweep pass at `now` (injectable for tests): refresh witnesses
    /// from a snapshot, then delete every row unchanged for longer than its
    /// table's TTL. Returns the delete transaction's receipt, or `None` when
    /// nothing was due (no transaction, no `tx_id` consumed).
    pub async fn sweep_once_at(&self, now: Timestamp) -> Result<Option<CommitReceipt>> {
        // Phase 1 — bookkeeping on a wait-free snapshot (no transaction).
        let snapshot = self.pipeline.store().snapshot();
        let mut doomed: Vec<(SweepTable, Vec<RowValue>, Row)> = Vec::new();
        {
            let mut witnesses = self.witnesses.lock().unwrap_or_else(|e| e.into_inner());
            let mut live: HashSet<(TableId, PkBytes)> = HashSet::new();
            for entry in &self.tables {
                for row in snapshot.scan(entry.table)? {
                    let pk = crate::store::row::encode_pk_of_row(entry.schema, row.values())?;
                    let key = (entry.table, pk);
                    live.insert(key.clone());
                    match witnesses.get_mut(&key) {
                        Some(witness) if witness.row.same_identity(row) => {
                            let age = now.as_micros() - witness.changed_at.as_micros();
                            if age > entry.expire_after_us {
                                let pk_values = entry
                                    .schema
                                    .primary_key
                                    .iter()
                                    .filter_map(|&ord| row.value(ord).cloned())
                                    .collect();
                                doomed.push((*entry, pk_values, row.clone()));
                            }
                        }
                        // New row, or rewritten since last observed: refresh.
                        _ => {
                            witnesses.insert(
                                key,
                                Witness {
                                    row: row.clone(),
                                    changed_at: now,
                                },
                            );
                        }
                    }
                }
            }
            // Rows gone from the snapshot no longer need a witness.
            witnesses.retain(|key, _| live.contains(key));
        }
        if doomed.is_empty() {
            return Ok(None);
        }

        // Phase 2 — delete in one ordinary transaction, re-verifying each row
        // by identity so a write that raced the snapshot wins.
        let plan: Vec<(TableId, Vec<RowValue>, Row)> = doomed
            .iter()
            .map(|(entry, pk_values, row)| (entry.table, pk_values.clone(), row.clone()))
            .collect();
        let receipt = self
            .pipeline
            .call(Box::new(move |tx| {
                for (table, pk_values, witness) in &plan {
                    match tx.query_pk(*table, pk_values)? {
                        Some(current) if current.same_identity(witness) => {
                            tx.delete(*table, pk_values)?;
                        }
                        // Rewritten or already gone: leave it alone.
                        _ => {}
                    }
                }
                Ok(())
            }))
            .await?;
        // Deleted rows lose their witness (a survivor re-registers next pass).
        {
            let mut witnesses = self.witnesses.lock().unwrap_or_else(|e| e.into_inner());
            for (entry, pk_values, _) in &doomed {
                if let Ok(pk) = crate::store::row::encode_pk_values(entry.schema, pk_values) {
                    witnesses.remove(&(entry.table, pk));
                }
            }
        }
        Ok(Some(receipt))
    }
}
