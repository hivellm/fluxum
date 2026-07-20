//! The reducer engine (SPEC-004 §2–3, T3.3): the transport-independent
//! entry point that turns admitted `ReducerCall`s into transaction-pipeline
//! jobs and drives the shard lifecycle hooks.
//!
//! # What lives where
//!
//! - **Admission** (RED-001/RED-006) happens here, *before* the pipeline:
//!   an unknown reducer name is a wire-ready 404 and a declared-signature
//!   argument mismatch is a 400 — in both cases no transaction, no
//!   `TxState`, no commit-log entry ever exists.
//! - **Execution** rides the T3.1 pipeline ([`crate::txn::TxPipeline`]):
//!   the single writer runs the dispatch under its `catch_unwind` boundary,
//!   so a panicking reducer is a rollback plus a wire-ready 500 — never a
//!   dead shard (RED-061, TXN-022, FR-25).
//! - **Lifecycle** (RED-010..RED-013): `on_init` runs exactly once — the
//!   first startup with an empty `CommittedState` (no checkpoint, no commit
//!   log; the caller derives that from
//!   [`crate::checkpoint::RecoveryOutcome::last_tx_id`]) — and
//!   `on_shard_start` runs on every startup after recovery, both before the
//!   shard accepts calls (the server assembly orders that). `on_connect` /
//!   `on_disconnect` run per client session under the client's identity.
//!   Hooks of one kind run inside **one** transaction, in ascending function
//!   name order (deterministic across binaries; linker order is not).
//! - Lifecycle and scheduled executions run under the **server identity**
//!   with the reserved nil `ConnectionId(0)` (RED-025; never assigned to a
//!   real connection).

use std::sync::Arc;
use std::time::Instant;

use crate::error::{FluxumError, Result};
use crate::metrics::{Metrics, ReducerOutcome};
use crate::reducer::idempotency;
use crate::txn::{CommitReceipt, TxPipeline};
use crate::types::{ConnectionId, Identity, Timestamp};

use super::ratelimit::{RateLimiter, RateLimiterOptions};
use super::{ExecBounds, FluxValue, ReducerCaller, ReducerContext, ReducerRegistry, with_context};

/// Decode-time cap on `idempotency_key` (SPEC-021 / SEC-048, F-017): an
/// over-length key is refused at admission, before the dedup table or any
/// transaction is touched. Generous — a UUID is 36 bytes.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// SEC-046 reducer execution bounds, interior-mutable for OPS-040 hot
/// reload. `0` disables a bound; the built-in default is fully unbounded
/// (embedders/tests) — the server config supplies generous production
/// defaults.
#[derive(Debug, Default)]
pub struct ReducerBounds {
    /// Cooperative execution deadline, milliseconds (`0` = none).
    max_execution_ms: std::sync::atomic::AtomicU64,
    /// Per-transaction write ceiling, bytes (`0` = unbounded).
    max_tx_bytes: std::sync::atomic::AtomicU64,
}

impl ReducerBounds {
    /// Publish new bounds (boot and OPS-040 hot reload). In-flight calls
    /// keep the bounds they started with.
    pub fn set(&self, max_execution_ms: u64, max_tx_bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.max_execution_ms.store(max_execution_ms, Relaxed);
        self.max_tx_bytes.store(max_tx_bytes, Relaxed);
    }

    /// The current `(max_execution_ms, max_tx_bytes)` (`0` = off).
    pub fn get(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.max_execution_ms.load(Relaxed),
            self.max_tx_bytes.load(Relaxed),
        )
    }

    /// The [`ExecBounds`] of a call starting **now** — the deadline clock
    /// starts when the writer begins executing the body, not while the call
    /// waits in the queue (queue pressure is TXN-011's problem).
    fn starting_now(&self) -> ExecBounds {
        let (max_execution_ms, max_tx_bytes) = self.get();
        ExecBounds {
            deadline: (max_execution_ms > 0)
                .then(|| Instant::now() + std::time::Duration::from_millis(max_execution_ms)),
            max_tx_bytes,
        }
    }
}

/// Which lifecycle moment a hook is registered for (SPEC-004 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleKind {
    /// First startup with an empty `CommittedState` only (RED-010).
    OnInit,
    /// Every startup, after recovery, before the first call (RED-013).
    OnShardStart,
    /// An authenticated client connected (RED-011).
    OnConnect,
    /// A client connection dropped — clean close or timeout (RED-012).
    OnDisconnect,
}

/// The static handler shape the lifecycle macros emit.
pub type LifecycleFnPtr = fn(&ReducerContext<'_, '_, '_>) -> Result<()>;

/// One lifecycle hook in the link-time registry (RED-010..RED-013),
/// submitted by `#[fluxum::on_init]` / `#[fluxum::on_shard_start]` /
/// `#[fluxum::on_connect]` / `#[fluxum::on_disconnect]`.
pub struct LifecycleDef {
    /// Which moment the hook runs at.
    pub kind: LifecycleKind,
    /// Function name (deterministic execution order within one kind).
    pub name: &'static str,
    /// The hook body.
    pub handler: LifecycleFnPtr,
}

inventory::collect!(LifecycleDef);

/// Iterate every lifecycle hook registered in this binary.
pub fn registered_lifecycle() -> impl Iterator<Item = &'static LifecycleDef> {
    inventory::iter::<LifecycleDef>()
}

/// The shard's lifecycle hooks, grouped by kind and sorted by function name
/// (RED-010..RED-013).
#[derive(Default)]
pub struct LifecycleHooks {
    on_init: Vec<&'static LifecycleDef>,
    on_shard_start: Vec<&'static LifecycleDef>,
    on_connect: Vec<&'static LifecycleDef>,
    on_disconnect: Vec<&'static LifecycleDef>,
}

impl std::fmt::Debug for LifecycleHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names =
            |defs: &[&'static LifecycleDef]| -> Vec<&str> { defs.iter().map(|d| d.name).collect() };
        f.debug_struct("LifecycleHooks")
            .field("on_init", &names(&self.on_init))
            .field("on_shard_start", &names(&self.on_shard_start))
            .field("on_connect", &names(&self.on_connect))
            .field("on_disconnect", &names(&self.on_disconnect))
            .finish()
    }
}

impl LifecycleHooks {
    /// No hooks.
    pub fn none() -> Self {
        Self::default()
    }

    /// Collect every lifecycle hook of this binary from the link-time
    /// registry.
    pub fn from_registered() -> Self {
        Self::from_defs(registered_lifecycle())
    }

    /// [`LifecycleHooks::from_registered`] with explicit defs — the seam
    /// tests and embedders use instead of the link-time registry.
    pub fn from_defs(defs: impl IntoIterator<Item = &'static LifecycleDef>) -> Self {
        let mut hooks = Self::default();
        for def in defs {
            match def.kind {
                LifecycleKind::OnInit => hooks.on_init.push(def),
                LifecycleKind::OnShardStart => hooks.on_shard_start.push(def),
                LifecycleKind::OnConnect => hooks.on_connect.push(def),
                LifecycleKind::OnDisconnect => hooks.on_disconnect.push(def),
            }
        }
        for group in [
            &mut hooks.on_init,
            &mut hooks.on_shard_start,
            &mut hooks.on_connect,
            &mut hooks.on_disconnect,
        ] {
            group.sort_by_key(|def| def.name);
        }
        hooks
    }
}

/// What [`ReducerEngine::start`] did (RED-010/RED-013 observability).
#[derive(Debug, Default)]
pub struct StartupReport {
    /// `on_init` hooks that ran (fresh shard only), in execution order.
    pub ran_on_init: Vec<&'static str>,
    /// `on_shard_start` hooks that ran, in execution order.
    pub ran_on_shard_start: Vec<&'static str>,
}

/// The transport-independent reducer engine of one shard (T3.3).
///
/// Owns the admission path (registry pre-checks), the lifecycle hooks, and
/// the handle to the shard's transaction pipeline. Transports and the
/// scheduler (T3.4/T3.5) construct [`ReducerCaller`]s and call in; the
/// server assembly (phase 5) wires recovery → [`ReducerEngine::start`] →
/// transport accept, in that order.
pub struct ReducerEngine {
    registry: Arc<ReducerRegistry>,
    hooks: LifecycleHooks,
    pipeline: TxPipeline,
    shard_id: u32,
    server_identity: Identity,
    rate_limiter: RateLimiter,
    metrics: Arc<Metrics>,
    /// SEC-046 client-call execution bounds (shared with the server's
    /// OPS-040 hot-reload path).
    bounds: Arc<ReducerBounds>,
}

impl ReducerEngine {
    /// Assemble an engine over a shard's pipeline.
    ///
    /// `server_identity` is the SPEC-009 §8 database identity lifecycle and
    /// scheduled executions run under (RED-025);
    /// [`crate::auth::server_identity`] derives it. Rate limiting starts
    /// with [`RateLimiterOptions::default`] and only the shard's own server
    /// identity exempt — [`ReducerEngine::with_rate_limiter`] installs the
    /// assembly's limiter (server-peer exemptions, configured shard cap).
    pub fn new(
        pipeline: TxPipeline,
        registry: Arc<ReducerRegistry>,
        hooks: LifecycleHooks,
        shard_id: u32,
        server_identity: Identity,
    ) -> Self {
        Self {
            registry,
            hooks,
            pipeline,
            shard_id,
            server_identity,
            rate_limiter: RateLimiter::new(RateLimiterOptions::default(), [server_identity]),
            metrics: Metrics::new(shard_id),
            bounds: Arc::new(ReducerBounds::default()),
        }
    }

    /// The SEC-046 execution bounds handle — the server assembly publishes
    /// `reducer.max_execution_ms` / `reducer.max_tx_bytes` through it (boot
    /// and OPS-040 hot reload; internally synchronized, no `&mut` needed).
    pub fn bounds(&self) -> &Arc<ReducerBounds> {
        &self.bounds
    }

    /// The shard's `fluxum_*` metrics registry (SPEC-012 T5.6). The server
    /// transport records fan-out/connection counters against the same `Arc`
    /// and the admin `/metrics` endpoint renders it.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Replace the admission rate limiter (RED-050..RED-052) — the server
    /// assembly wires the configured `shard_max_reducers_per_sec` and the
    /// AUTH-062 server-peer exemptions through here.
    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: RateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    /// The shard's admission rate limiter (RED-050..RED-052). Config hot
    /// reload publishes a new `reducer.shard_max_reducers_per_sec` through
    /// this handle (OPS-040) — the limiter is internally synchronized, so
    /// retuning it needs no `&mut` and never pauses admission.
    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    /// The engine's reducer registry (dispatch and admission share it).
    pub fn registry(&self) -> &Arc<ReducerRegistry> {
        &self.registry
    }

    /// The shard's transaction pipeline.
    pub fn pipeline(&self) -> &TxPipeline {
        &self.pipeline
    }

    /// Run the startup lifecycle (RED-010/RED-013): `on_init` hooks when
    /// `fresh` (first boot: no checkpoint and no commit log —
    /// `recovery.last_tx_id.is_none()`), then `on_shard_start` hooks on
    /// every boot. Each kind runs inside one transaction; an `Err` (or
    /// panic) rolls that transaction back and aborts startup.
    ///
    /// The server assembly must call this after recovery and **before**
    /// accepting any `ReducerCall` (RED-013).
    pub async fn start(&self, fresh: bool) -> Result<StartupReport> {
        let mut report = StartupReport::default();
        if fresh && !self.hooks.on_init.is_empty() {
            let defs = self.hooks.on_init.clone();
            report.ran_on_init = defs.iter().map(|def| def.name).collect();
            self.run_hooks(defs).await?;
        }
        if !self.hooks.on_shard_start.is_empty() {
            let defs = self.hooks.on_shard_start.clone();
            report.ran_on_shard_start = defs.iter().map(|def| def.name).collect();
            self.run_hooks(defs).await?;
        }
        Ok(report)
    }

    /// Run the `on_connect` hooks for an authenticated client session
    /// (RED-011), inside one transaction under the client's identity.
    /// Returns the hook transaction's [`CommitReceipt`] so the transport can
    /// publish its `TxDiff` to the shard fan-out (SUB-021) — an `on_connect`
    /// that inserts a presence row must reach subscribers. `None` when no
    /// `on_connect` hook is registered (no transaction, no `tx_id` consumed).
    pub async fn client_connected(
        &self,
        identity: Identity,
        connection_id: ConnectionId,
    ) -> Result<Option<CommitReceipt>> {
        if self.hooks.on_connect.is_empty() {
            return Ok(None);
        }
        let caller = ReducerCaller {
            identity,
            connection_id,
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        self.run_hooks_as(self.hooks.on_connect.clone(), caller)
            .await
            .map(Some)
    }

    /// Run the `on_disconnect` hooks when a client connection drops —
    /// clean close or timeout (RED-012) — plus the built-in ephemeral
    /// `#[owner]` cleanup (SPEC-023 DMX-011): every ephemeral table bound to
    /// a `ConnectionId` column drops this connection's rows, in the **same
    /// transaction** as the user hooks, so presence rows and their cleanup
    /// fan out atomically. Like [`Self::client_connected`], returns the
    /// transaction's receipt for fan-out publication, or `None` when there is
    /// neither a hook nor an owner-bound ephemeral table.
    pub async fn client_disconnected(
        &self,
        identity: Identity,
        connection_id: ConnectionId,
    ) -> Result<Option<CommitReceipt>> {
        // Resolve owner-bound ephemeral tables against this shard's store
        // (defs for tables absent from the schema are skipped).
        let store = self.pipeline.store();
        let cleanup: Vec<(
            crate::store::TableId,
            u16,
            &'static crate::schema::TableSchema,
        )> = crate::schema::registered_ephemeral()
            .filter_map(|def| {
                let owner = def.owner?;
                let table = store.table_id(def.table)?;
                let schema = store.table_schema(table)?;
                Some((table, owner, schema))
            })
            .collect();
        if self.hooks.on_disconnect.is_empty() && cleanup.is_empty() {
            return Ok(None);
        }
        let caller = ReducerCaller {
            identity,
            connection_id,
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        let defs = self.hooks.on_disconnect.clone();
        let registry = Arc::clone(&self.registry);
        self.pipeline
            .call(Box::new(move |tx| {
                with_context(&registry, caller, tx, |ctx| {
                    for def in &defs {
                        (def.handler)(ctx)?;
                    }
                    Ok(())
                })?;
                // Built-in DMX-011 owner cleanup, same transaction.
                use crate::store::RowValue;
                for (table, owner, schema) in &cleanup {
                    let doomed: Vec<Vec<RowValue>> = tx
                        .scan(*table)?
                        .filter(|row| {
                            matches!(
                                row.value(*owner),
                                Some(RowValue::ConnectionId(c)) if *c == connection_id
                            )
                        })
                        .map(|row| {
                            schema
                                .primary_key
                                .iter()
                                .filter_map(|&ord| row.value(ord).cloned())
                                .collect()
                        })
                        .collect();
                    for pk_values in doomed {
                        tx.delete(*table, &pk_values)?;
                    }
                }
                Ok(())
            }))
            .await
            .map(Some)
    }

    /// Execute reducer `name` for `caller` (FR-20).
    ///
    /// Admission runs first, with no transaction: an unregistered name is a
    /// 404 (RED-006), a schedule-only reducer is a 403 (RED-025), a
    /// rate-limited caller is a 429 — or 503 past the shard cap — with zero
    /// storage cost (RED-050/RED-052), and — for `#[fluxum::reducer]`-
    /// declared signatures — an argument count or type mismatch is a 400
    /// (RED-001). Admitted calls execute on the shard's single writer;
    /// `Err` or panic rolls back with no commit-log entry and no
    /// subscription events, and the shard keeps serving (RED-004, RED-061).
    pub async fn call(
        &self,
        caller: ReducerCaller,
        name: &str,
        args: Vec<FluxValue>,
    ) -> Result<CommitReceipt> {
        let start = Instant::now();
        let identity = caller.identity;
        // Admission + admission-time rejections carry no transaction; each
        // records its OBS-010 outcome before returning (RED-006/RED-001).
        let max_rate = match self.registry.admission(name) {
            Ok(rate) => rate,
            Err(error) => return self.reject(name, ReducerOutcome::Err, start, error),
        };
        if let Err(error) = self.rate_limiter.check(&identity, name, max_rate) {
            return self.reject(name, ReducerOutcome::RateLimited, start, error);
        }
        if let Err(error) = self.registry.check_args(name, &args) {
            return self.reject(name, ReducerOutcome::Err, start, error);
        }

        let registry = Arc::clone(&self.registry);
        let dispatch_name = name.to_owned();
        let bounds = Arc::clone(&self.bounds);
        // OPS-020: tag the commit with its caller + reducer for the audit
        // trail.
        let meta = crate::txn::CommitMeta {
            caller: identity,
            reducer_name: name.to_owned(),
        };
        let result = self
            .pipeline
            .call_with(
                meta,
                Box::new(move |tx| {
                    // SEC-046: the deadline clock starts on the writer.
                    registry.dispatch_bounded(
                        caller,
                        &dispatch_name,
                        &args,
                        tx,
                        bounds.starting_now(),
                    )
                }),
            )
            .await;

        let duration_us = duration_us(start);
        match result {
            Ok(receipt) => {
                // OBS-010/013: committed.
                self.metrics
                    .record_reducer(name, ReducerOutcome::Ok, duration_us);
                self.metrics.note_commit();
                self.warn_if_slow(name, duration_us);
                Ok(receipt)
            }
            Err(error) if is_queue_full(&error) => {
                // TXN-011: the writer queue was full; the reducer never ran.
                self.metrics
                    .record_reducer(name, ReducerOutcome::QueueFull, duration_us);
                Err(error)
            }
            Err(error) => {
                // OBS-010/013: the reducer returned Err or panicked → rollback.
                self.note_abort(&error);
                self.metrics
                    .record_reducer(name, ReducerOutcome::Err, duration_us);
                self.metrics.note_rollback();
                self.log_failure(name, &identity, &error, duration_us);
                self.warn_if_slow(name, duration_us);
                Err(error)
            }
        }
    }

    /// SEC-046 observability: count a bounds-driven abort under its reason.
    fn note_abort(&self, error: &FluxumError) {
        use crate::metrics::ReducerAbortReason;
        match error.query_code() {
            Some(fluxum_protocol::codes::REDUCER_DEADLINE_EXCEEDED) => {
                self.metrics
                    .note_reducer_aborted(ReducerAbortReason::Deadline);
            }
            Some(fluxum_protocol::codes::REDUCER_TX_BUDGET_EXCEEDED) => {
                self.metrics.note_reducer_aborted(ReducerAbortReason::Alloc);
            }
            _ => {}
        }
    }

    /// [`ReducerEngine::call`] with SPEC-021 CS-030 exactly-once
    /// submission: if `key` has already been applied for this
    /// `(caller identity, reducer)`, the body does not run and the answer is
    /// [`CallOutcome::Deduplicated`].
    ///
    /// `None` is an ordinary call. Passing a key on a shard whose schema has
    /// no `__idempotency__` table is an error rather than a silent
    /// downgrade: a client that asked for exactly-once must not be given
    /// at-least-once quietly.
    ///
    /// # Atomicity
    ///
    /// The check and the record both happen **inside the reducer's own
    /// transaction**, on the shard's single writer — so two concurrent calls
    /// bearing the same key cannot both miss the check, and the record
    /// commits with the effects it guards or rolls back with them (CS-031).
    /// A reducer that returns `Err` or panics therefore records nothing and
    /// its retry re-executes, which is safe: it applied nothing.
    pub async fn call_idempotent(
        &self,
        caller: ReducerCaller,
        name: &str,
        args: Vec<FluxValue>,
        key: Option<&str>,
    ) -> Result<CallOutcome> {
        let Some(key) = key else {
            return self
                .call(caller, name, args)
                .await
                .map(CallOutcome::Committed);
        };
        // SEC-048 (F-017): cap the key before it reaches the dedup table —
        // an unbounded client-chosen string must not become an unbounded
        // durable row.
        if key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return self.reject(
                name,
                ReducerOutcome::Err,
                Instant::now(),
                FluxumError::query(
                    fluxum_protocol::codes::REDUCER_BAD_ARGS,
                    format!(
                        "idempotency_key is {} bytes; the maximum is {MAX_IDEMPOTENCY_KEY_BYTES} \
                         (SEC-048)",
                        key.len()
                    ),
                ),
            );
        }
        let table = self
            .pipeline
            .store()
            .table_id(idempotency::IDEMPOTENCY_TABLE_NAME)
            .ok_or_else(|| {
                FluxumError::Reducer(format!(
                    "idempotency_key requires the `{}` table in the schema (SPEC-021 CS-030)",
                    idempotency::IDEMPOTENCY_TABLE_NAME
                ))
            })?;

        let start = Instant::now();
        let identity = caller.identity;
        let max_rate = match self.registry.admission(name) {
            Ok(rate) => rate,
            Err(error) => return self.reject(name, ReducerOutcome::Err, start, error),
        };
        if let Err(error) = self.rate_limiter.check(&identity, name, max_rate) {
            return self.reject(name, ReducerOutcome::RateLimited, start, error);
        }
        if let Err(error) = self.registry.check_args(name, &args) {
            return self.reject(name, ReducerOutcome::Err, start, error);
        }

        // A replay aborts the transaction rather than committing an empty
        // one, so a deduplicated call consumes no tx id (TXN-030). The flag
        // — not the error text — is what distinguishes it.
        let hit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let registry = Arc::clone(&self.registry);
        let dispatch_name = name.to_owned();
        let key_owned = key.to_owned();
        let flag = Arc::clone(&hit);
        let bounds = Arc::clone(&self.bounds);
        let meta = crate::txn::CommitMeta {
            caller: identity,
            reducer_name: name.to_owned(),
        };
        let result = self
            .pipeline
            .call_with(
                meta,
                Box::new(move |tx| {
                    if idempotency::already_applied(
                        tx,
                        table,
                        &identity,
                        &dispatch_name,
                        &key_owned,
                    )? {
                        flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        return Err(FluxumError::Reducer("idempotency replay".into()));
                    }
                    // SEC-046: same bounds as the plain path.
                    registry.dispatch_bounded(
                        caller,
                        &dispatch_name,
                        &args,
                        tx,
                        bounds.starting_now(),
                    )?;
                    idempotency::record(tx, table, &identity, &dispatch_name, &key_owned)
                }),
            )
            .await;

        let duration_us = duration_us(start);
        if hit.load(std::sync::atomic::Ordering::SeqCst) {
            // CS-030: not a failure — the original call already applied.
            tracing::debug!(
                shard = self.shard_id,
                reducer = name,
                identity = %identity,
                "idempotency key replayed; reducer body skipped"
            );
            return Ok(CallOutcome::Deduplicated);
        }
        match result {
            Ok(receipt) => {
                self.metrics
                    .record_reducer(name, ReducerOutcome::Ok, duration_us);
                self.metrics.note_commit();
                self.warn_if_slow(name, duration_us);
                Ok(CallOutcome::Committed(receipt))
            }
            Err(error) if is_queue_full(&error) => {
                self.metrics
                    .record_reducer(name, ReducerOutcome::QueueFull, duration_us);
                Err(error)
            }
            Err(error) => {
                self.note_abort(&error);
                self.metrics
                    .record_reducer(name, ReducerOutcome::Err, duration_us);
                self.metrics.note_rollback();
                self.log_failure(name, &identity, &error, duration_us);
                self.warn_if_slow(name, duration_us);
                Err(error)
            }
        }
    }

    /// Record an admission-time rejection outcome and return its error.
    /// Generic in the success type — it only ever returns `Err`, so both the
    /// `CommitReceipt` and `CallOutcome` paths share it.
    fn reject<T>(
        &self,
        name: &str,
        outcome: ReducerOutcome,
        start: Instant,
        error: FluxumError,
    ) -> Result<T> {
        let duration_us = duration_us(start);
        self.metrics.record_reducer(name, outcome, duration_us);
        if outcome == ReducerOutcome::RateLimited {
            // OBS-070: queue-full / rate-limit is a WARN-worthy admission event.
            tracing::warn!(
                shard = self.shard_id,
                reducer = name,
                "reducer rejected by rate limiter"
            );
        }
        Err(error)
    }

    /// OBS-071: log a reducer failure — a panic is an `ERROR` with a
    /// backtrace (SPEC-004 isolation boundary), a business `Err` a `DEBUG`.
    fn log_failure(&self, name: &str, identity: &Identity, error: &FluxumError, duration_us: u64) {
        if matches!(error, FluxumError::ReducerPanic(_)) {
            tracing::error!(
                shard = self.shard_id,
                reducer = name,
                identity = %identity,
                duration_us,
                backtrace = %std::backtrace::Backtrace::capture(),
                err = %error,
                "reducer panicked (transaction rolled back)"
            );
        } else {
            tracing::debug!(
                shard = self.shard_id,
                reducer = name,
                identity = %identity,
                duration_us,
                err = %error,
                "reducer returned Err"
            );
        }
    }

    /// OBS-072: WARN when a reducer exceeds the configured threshold.
    fn warn_if_slow(&self, name: &str, duration_us: u64) {
        if self.metrics.is_slow(duration_us) {
            tracing::warn!(
                event = "slow_reducer",
                shard = self.shard_id,
                reducer = name,
                duration_us,
                "reducer exceeded the slow-reducer threshold"
            );
        }
    }

    /// Run `defs` in one transaction under the server identity (RED-025:
    /// nil `ConnectionId(0)`).
    async fn run_hooks(&self, defs: Vec<&'static LifecycleDef>) -> Result<CommitReceipt> {
        let caller = ReducerCaller {
            identity: self.server_identity,
            connection_id: ConnectionId::new(0),
            timestamp: Timestamp::now(),
            shard_id: self.shard_id,
        };
        self.run_hooks_as(defs, caller).await
    }

    /// Run `defs` in one transaction as `caller`, in order; the first `Err`
    /// rolls the whole transaction back.
    async fn run_hooks_as(
        &self,
        defs: Vec<&'static LifecycleDef>,
        caller: ReducerCaller,
    ) -> Result<CommitReceipt> {
        let registry = Arc::clone(&self.registry);
        self.pipeline
            .call(Box::new(move |tx| {
                with_context(&registry, caller, tx, |ctx| {
                    for def in &defs {
                        (def.handler)(ctx)?;
                    }
                    Ok(())
                })
            }))
            .await
    }
}

/// The result of an idempotent call (SPEC-021 CS-030).
#[derive(Debug)]
pub enum CallOutcome {
    /// The reducer ran and its transaction committed.
    Committed(CommitReceipt),
    /// The `idempotency_key` had already been applied for this
    /// `(identity, reducer)`: the body did **not** run and nothing was
    /// committed. The caller answers the original result — for a committed
    /// call that is `Ok` — and publishes no new diff (the original commit
    /// already fanned out).
    Deduplicated,
}

/// Elapsed microseconds since `start`, saturating (OBS-011).
fn duration_us(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// Whether `error` is the TXN-011 full-queue rejection (the reducer never
/// executed — outcome `queue_full`, not a rollback).
fn is_queue_full(error: &FluxumError) -> bool {
    error.to_wire().code == fluxum_protocol::codes::CLUSTER_SHARD_UNAVAILABLE
}
