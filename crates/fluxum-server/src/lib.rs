//! Fluxum server presentation layer (SPEC-006): the FluxRPC TCP transport
//! (:15801), the per-connection session state machine, message routing, and
//! the post-commit `TxUpdate` fan-out onto subscribed connections.
//!
//! # Layers
//!
//! - [`ShardContext`] — the shared per-shard state a connection needs: the
//!   [`ReducerEngine`](fluxum_core::reducer::ReducerEngine), the
//!   [`SubscriptionManager`](fluxum_core::subscription::SubscriptionManager)
//!   behind its SUB-041 async mutex, the
//!   [`Authenticator`](fluxum_core::auth::Authenticator), a connection
//!   registry, and a commit broadcast that drives live updates.
//! - [`session`] — the sans-socket router: turns one decoded
//!   [`ClientMessage`](fluxum_protocol::ClientMessage) into the
//!   [`ServerMessage`](fluxum_protocol::ServerMessage)s to send back,
//!   enforcing the pre-auth `401` gate (AUTH-020) and the SPEC-006 error
//!   mapping. Independent of any socket, so it is unit-testable directly.
//! - [`tcp`] — the tokio listener that drives sessions over real sockets:
//!   frame decode with the RPC-061 size limit (`413`), the RPC-060 idle
//!   timeout (`408`), a per-connection writer that multiplexes responses by
//!   echoed id (RPC-002), and the fan-out task that pushes `TxUpdate`s.

pub mod admin;
pub mod http;
pub mod logging;
pub mod session;
pub mod shard;
pub mod tcp;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, Notify, broadcast, mpsc};

use fluxum_core::auth::Authenticator;
use fluxum_core::reducer::{ReducerEngine, ViewRegistry};
use fluxum_core::store::{MemStore, TxDiff};
use fluxum_core::subscription::SubscriptionManager;
use fluxum_core::types::Identity;

/// One encoded, framed message ready for a connection's socket.
pub type OutFrame = Arc<Vec<u8>>;

/// A live connection's fan-out handle: a bounded outbound queue (drained by
/// the connection's writer task) plus a shutdown signal. A full queue is the
/// SUB-042 "Full" tier — the fan-out notifies shutdown and drops the
/// connection rather than ever blocking the commit path.
#[derive(Clone)]
pub struct ConnHandle {
    /// Outbound frame queue (bounded — the per-client send buffer, SUB-042).
    pub sink: mpsc::Sender<OutFrame>,
    /// Forces the connection to close (slow-consumer drop, SUB-042).
    pub shutdown: Arc<Notify>,
}

/// Live connection registry: `connection_id` → its fan-out handle. The
/// fan-out task looks a subscriber up here to push a `TxUpdate` without ever
/// touching the connection's read/route path.
#[derive(Default)]
pub struct ConnectionRegistry {
    handles: Mutex<HashMap<u128, ConnHandle>>,
}

impl ConnectionRegistry {
    /// Register a connection's fan-out handle at authentication time.
    pub async fn insert(&self, connection_id: u128, handle: ConnHandle) {
        self.handles.lock().await.insert(connection_id, handle);
    }

    /// Remove a connection on disconnect.
    pub async fn remove(&self, connection_id: u128) {
        self.handles.lock().await.remove(&connection_id);
    }

    /// Handles for a set of subscriber ids (fan-out targets).
    async fn handles_for(&self, connections: &[u128]) -> Vec<(u128, ConnHandle)> {
        let guard = self.handles.lock().await;
        connections
            .iter()
            .filter_map(|conn| guard.get(conn).map(|h| (*conn, h.clone())))
            .collect()
    }
}

/// The shared per-shard state every connection session reads from (SPEC-006
/// server assembly; the full multi-shard `ShardHost` is T5.4).
pub struct ShardContext {
    /// The reducer engine (admission + dispatch through the T3.1 pipeline).
    pub engine: ReducerEngine,
    /// The subscription registry + fan-out, behind the SUB-041 async mutex.
    pub subscriptions: Mutex<SubscriptionManager>,
    /// The single authentication entry point (AUTH-020/021).
    pub authenticator: Authenticator,
    /// Live connections, for the commit fan-out.
    pub connections: ConnectionRegistry,
    /// The `#[fluxum::view]` registry for the HTTP admin `GET /view/:name`
    /// (RED-030). Empty unless the assembly installs views.
    pub views: ViewRegistry,
    /// This shard's id (carried in every `ReducerCaller`).
    pub shard_id: u32,
    /// The server (admin) identity every HTTP admin call runs under
    /// (bypasses RLS, AUTH-062) — admin tooling is a trusted operator.
    pub admin_identity: Identity,
    /// Broadcast of every committed [`TxDiff`]; the fan-out task evaluates
    /// subscriptions against each and pushes `TxUpdate`s (SUB-021).
    commit_tx: broadcast::Sender<Arc<TxDiff>>,
    /// Monotonic `ConnectionId` allocator (ephemeral, never reused within a
    /// process; `0` is reserved for scheduled/system callers, RED-025).
    next_connection_id: AtomicU64,
    /// Last committed `tx_id` (atomic, for the lock-free `/health` — RPC-053
    /// forbids taking storage locks on the health path).
    last_tx_id: AtomicU64,
    /// Whether the DMX-011 ephemeral TTL sweeper has been spawned (both
    /// transports request it on serve; only the first call spawns).
    sweeper_started: std::sync::atomic::AtomicBool,
    /// Whether the DMX-020 row-TTL sweeper has been spawned (idempotent, as
    /// above).
    ttl_sweeper_started: std::sync::atomic::AtomicBool,
    /// The shard's blob store (SPEC-023 DMX-040), once installed.
    blob_store: std::sync::OnceLock<Arc<fluxum_core::commitlog::BlobStore>>,
    /// The validated plugin registry (SPEC-020), once installed: drives
    /// `GET /plugins` introspection and hot disable (PLG-060/061).
    plugins: std::sync::OnceLock<Arc<fluxum_core::plugin::PluginRegistry>>,
    /// Process start instant, for the `/health` `uptime_s` field (OBS-060).
    started: std::time::Instant,
    /// SPEC-025 OPS-030: the shard is draining for a rolling restart. New
    /// connections, subscriptions and reducer calls are refused with a
    /// *retryable* signal while in-flight transactions finish, so an SDK
    /// retries them against the restarted process (OPS-031) instead of
    /// losing them.
    draining: std::sync::atomic::AtomicBool,
    /// The boot-time [`EffectiveConfig`] rendered once (HWA-013): probe
    /// inputs, every derived value with its source, and the per-kernel SIMD
    /// selection. Serialized at install so `/health` stays a clone, not a
    /// serialization, on the < 50 ms path (OBS-061).
    ///
    /// [`EffectiveConfig`]: fluxum_core::hw::EffectiveConfig
    effective_config: std::sync::OnceLock<serde_json::Value>,
}

/// A lock-free health snapshot (RPC-053 / OBS-060): read from atomics only,
/// never touching a storage lock, so `/health` answers in < 50 ms even
/// under sustained write load.
#[derive(Debug, Clone, Copy)]
pub struct Health {
    /// This shard's id.
    pub shard_id: u32,
    /// Last committed transaction id (`0` before the first commit).
    pub last_tx_id: u64,
    /// Lifecycle state (OBS-050): drives the `/health` `status`.
    pub state: fluxum_core::metrics::ShardState,
    /// Pending `ReducerCall`s in the single-writer queue (OBS-012).
    pub queue_depth: u64,
}

impl ShardContext {
    /// Assemble a shard context. `commit_capacity` bounds the commit
    /// broadcast backlog (a slow fan-out task lags, never blocks commits).
    pub fn new(
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        authenticator: Authenticator,
        shard_id: u32,
        commit_capacity: usize,
    ) -> Arc<Self> {
        Self::with_views(
            engine,
            subscriptions,
            authenticator,
            ViewRegistry::new(),
            shard_id,
            commit_capacity,
        )
    }

    /// [`ShardContext::new`] with a `#[fluxum::view]` registry installed.
    pub fn with_views(
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        authenticator: Authenticator,
        views: ViewRegistry,
        shard_id: u32,
        commit_capacity: usize,
    ) -> Arc<Self> {
        let (commit_tx, _) = broadcast::channel(commit_capacity.max(1));
        let admin_identity = fluxum_core::auth::server_identity("__admin__");
        Arc::new(Self {
            engine,
            subscriptions: Mutex::new(subscriptions),
            authenticator,
            connections: ConnectionRegistry::default(),
            views,
            shard_id,
            admin_identity,
            commit_tx,
            next_connection_id: AtomicU64::new(1),
            last_tx_id: AtomicU64::new(0),
            sweeper_started: std::sync::atomic::AtomicBool::new(false),
            ttl_sweeper_started: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::OnceLock::new(),
            plugins: std::sync::OnceLock::new(),
            started: std::time::Instant::now(),
            effective_config: std::sync::OnceLock::new(),
            draining: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Whether this shard is draining (SPEC-025 OPS-030). Checked by the
    /// accept loops and the session router.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    /// Enter the drain state: stop admitting new work. Idempotent, and
    /// deliberately one-way — a drain precedes exit, so there is no
    /// "un-drain" to race with an in-flight shutdown. Existing connections
    /// keep being serviced until [`drain`] quiesces them.
    pub fn begin_drain(&self) {
        if !self.draining.swap(true, Ordering::SeqCst) {
            self.metrics()
                .set_shard_state(fluxum_core::metrics::ShardState::ShuttingDown);
            tracing::info!(
                target: "fluxum::server",
                shard = self.shard_id,
                "draining: refusing new work, finishing in-flight transactions"
            );
        }
    }

    /// Install the boot-time effective configuration (HWA-013): the assembly
    /// calls this after `hw::derive(&probe, &config)` so `GET /health`
    /// exposes the probe inputs, each derived value with its provenance, and
    /// the resolved per-kernel SIMD selection (HWA-033). A second call is
    /// ignored; without it `/health` simply omits the `config` key.
    pub fn set_effective_config(&self, effective: &fluxum_core::hw::EffectiveConfig) {
        if let Ok(value) = serde_json::to_value(effective) {
            let _ = self.effective_config.set(value);
        }
    }

    /// The rendered effective configuration, if the assembly installed one.
    pub fn effective_config(&self) -> Option<&serde_json::Value> {
        self.effective_config.get()
    }

    /// Install the validated plugin registry (SPEC-020 PLG-032): built by
    /// `PluginRegistry::build(schema, config)` at assembly — a validation
    /// failure there aborts startup before this is reached. Enables
    /// `GET /plugins` and the hot disable/enable endpoints (PLG-060/061).
    /// A second call is ignored.
    pub fn set_plugins(&self, registry: Arc<fluxum_core::plugin::PluginRegistry>) {
        let _ = self.plugins.set(registry);
    }

    /// The installed plugin registry, if any.
    pub fn plugins(&self) -> Option<&Arc<fluxum_core::plugin::PluginRegistry>> {
        self.plugins.get()
    }

    /// Install the shard's blob store (SPEC-023 DMX-040): attaches it to the
    /// store (write validation + commit refcounts, rebuilding counts from
    /// the current snapshot) and enables the `/blob` HTTP endpoints. Call
    /// after recovery, before serving. A second call is ignored.
    pub fn set_blob_store(&self, blobs: Arc<fluxum_core::commitlog::BlobStore>) {
        self.store().attach_blob_store(Arc::clone(&blobs));
        let _ = self.blob_store.set(blobs);
    }

    /// The installed blob store, if any.
    pub fn blob_store(&self) -> Option<&Arc<fluxum_core::commitlog::BlobStore>> {
        self.blob_store.get()
    }

    /// Start the ephemeral TTL sweeper (SPEC-023 DMX-011) if any registered
    /// ephemeral table declares `expire_after`. Idempotent — both transports
    /// call this on serve; only the first call spawns. The sweep's delete
    /// diffs are published to the shard fan-out like any commit.
    pub fn start_ephemeral_sweeper(self: &Arc<Self>) {
        if self
            .sweeper_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let Some(sweeper) = fluxum_core::scheduler::EphemeralSweeper::from_registered(
            self.engine.pipeline().clone(),
        ) else {
            return;
        };
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            let cadence = sweeper.cadence();
            loop {
                tokio::time::sleep(cadence).await;
                match sweeper.sweep_once().await {
                    Ok(Some(receipt)) => ctx.publish_commit(receipt.diff),
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(target: "fluxum::server", error = %e, "ephemeral sweep failed");
                    }
                }
            }
        });
    }

    /// Start the row-TTL sweeper (SPEC-023 DMX-020) if any registered table
    /// declares `#[ttl(...)]`. Idempotent (only the first call spawns). A
    /// backlog that hits the batch cap keeps sweeping without the full cadence
    /// wait, so a mass expiry drains promptly without one giant delete (DMX-021);
    /// its delete diffs fan out like any commit.
    pub fn start_ttl_sweeper(self: &Arc<Self>) {
        if self
            .ttl_sweeper_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let Some(sweeper) =
            fluxum_core::scheduler::TtlSweeper::from_registered(self.engine.pipeline().clone())
        else {
            return;
        };
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            let cadence = sweeper.cadence();
            loop {
                tokio::time::sleep(cadence).await;
                // Drain the backlog: keep sweeping while a pass hits the cap.
                loop {
                    match sweeper.sweep_once().await {
                        Ok((receipt, more)) => {
                            if let Some(receipt) = receipt {
                                ctx.publish_commit(receipt.diff);
                            }
                            if !more {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(target: "fluxum::server", error = %e, "row-TTL sweep failed");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// A lock-free health snapshot (RPC-053 / OBS-060/061): reads only
    /// atomics and the pipeline's channel gauge — never a storage lock.
    pub fn health(&self) -> Health {
        Health {
            shard_id: self.shard_id,
            last_tx_id: self.last_tx_id.load(Ordering::Relaxed),
            state: self.metrics().shard_state(),
            queue_depth: self.engine.pipeline().queue_depth(),
        }
    }

    /// The shard's `fluxum_*` metrics registry (SPEC-012 T5.6), owned by the
    /// reducer engine and shared with the transport for fan-out/connection
    /// counters and the `/metrics` export.
    pub fn metrics(&self) -> &Arc<fluxum_core::metrics::Metrics> {
        self.engine.metrics()
    }

    /// Seconds since this shard context was created (`/health` uptime).
    pub fn uptime_s(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// The shard's committed store (lock-free snapshots for InitialData /
    /// one-off queries).
    pub fn store(&self) -> &Arc<MemStore> {
        self.engine.pipeline().store()
    }

    /// Allocate the next ephemeral `ConnectionId` (RPC-002).
    pub fn allocate_connection_id(&self) -> u128 {
        u128::from(self.next_connection_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Publish a committed diff to the fan-out (called after a reducer
    /// commit). A lagging fan-out drops old diffs rather than block the
    /// commit path — clients recover missed updates on reconnect via the
    /// `tx_id` gap (SPEC-006 acceptance 14).
    pub fn publish_commit(&self, diff: TxDiff) {
        self.last_tx_id.fetch_max(diff.tx_id, Ordering::Relaxed);
        let _ = self.commit_tx.send(Arc::new(diff));
    }

    /// A receiver for the commit broadcast (one per fan-out task).
    pub fn subscribe_commits(&self) -> broadcast::Receiver<Arc<TxDiff>> {
        self.commit_tx.subscribe()
    }
}

/// Spawn the shard-wide commit fan-out task (SUB-021/024): evaluate each
/// committed diff against the subscription manager once (mutex held only
/// across evaluation, SUB-041) and push the shared, once-encoded `TxUpdate`
/// frame to every subscriber's queue, dropping a slow consumer on a full
/// queue (SUB-042).
///
/// A standalone `tcp::serve` / `http::serve` spawns one so a single-transport
/// deployment works out of the box. The combined multi-transport assembly
/// (the T5.4 `ShardHost`) instead spawns exactly one and starts each
/// transport without its own — two fan-out tasks over one broadcast would
/// double-deliver to a subscriber registered in the shared registry.
pub(crate) fn spawn_fanout(ctx: Arc<ShardContext>, shutdown: Arc<Notify>) {
    use fluxum_protocol::{FrameCodec, ServerMessage};

    tokio::spawn(async move {
        let mut commits = ctx.subscribe_commits();
        let codec = FrameCodec::default();
        loop {
            let diff = tokio::select! {
                _ = shutdown.notified() => break,
                recv = commits.recv() => match recv {
                    Ok(diff) => diff,
                    // Lagged: the fan-out fell behind; clients recover on
                    // reconnect via the tx_id gap (SPEC-006 acceptance 14).
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            };

            // Evaluate once (SUB-041: mutex held only across evaluation).
            let deltas = {
                let manager = ctx.subscriptions.lock().await;
                match manager.on_commit(&diff) {
                    Ok(deltas) => deltas,
                    Err(e) => {
                        tracing::error!(target: "fluxum::fanout", error = %e,
                            "fan-out evaluation failed");
                        continue;
                    }
                }
            };

            for delta in deltas {
                let mut tx_update = SubscriptionManager::tx_update(&diff, &delta);
                // SPEC-007 SHD-051: tag the originating shard so a client
                // subscribed on several shards can attribute per-shard order.
                tx_update.shard_id = ctx.shard_id;
                // OBS-021: rows delivered per TxUpdate (insert + delete).
                let rows: u64 = tx_update
                    .tables
                    .iter()
                    .map(|t| u64::try_from(t.inserts.len() + t.deletes.len()).unwrap_or(u64::MAX))
                    .sum();
                let body = match ServerMessage::TxUpdate(tx_update).encode() {
                    Ok(body) => body,
                    Err(_) => continue,
                };
                let Ok(framed) = codec.encode(&body) else {
                    continue;
                };
                let frame: OutFrame = Arc::new(framed);
                for (conn_id, handle) in ctx.connections.handles_for(&delta.subscribers).await {
                    match handle.sink.try_send(Arc::clone(&frame)) {
                        // OBS-021: one TxUpdate delivered.
                        Ok(()) => ctx.metrics().note_fanout(rows),
                        // SUB-042 Full tier: never block — drop the consumer.
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(target: "fluxum::fanout", connection = conn_id,
                                "subscriber dropped: send buffer full");
                            ctx.metrics()
                                .note_drop(fluxum_core::metrics::DropReason::BufferFull);
                            handle.shutdown.notify_waiters();
                            ctx.connections.remove(conn_id).await;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            ctx.connections.remove(conn_id).await;
                        }
                    }
                }
            }
        }
    });
}

// --- Graceful drain (SPEC-025 §4, OPS-030/031) ----------------------------------

/// Tuning for [`drain`].
#[derive(Debug, Clone, Copy)]
pub struct DrainOptions {
    /// The whole drain must finish inside this budget (OPS-030 "exit
    /// cleanly within a bounded deadline"). A straggler past it is
    /// force-closed and logged rather than hanging the deploy.
    pub deadline: std::time::Duration,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self {
            deadline: std::time::Duration::from_secs(30),
        }
    }
}

/// What a drain did — the deploy's evidence that nothing was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    /// Whether in-flight transactions quiesced inside the deadline. `false`
    /// means the barrier timed out and stragglers were force-closed.
    pub quiesced: bool,
    /// The last **durable** tx id at the end of drain — what is actually on
    /// disk, which is what a restart would replay from. Includes the drain's
    /// own quiesce barrier, which commits like any other transaction.
    pub last_tx_id: u64,
    /// The tx id the final checkpoint captured, if one was taken. Restart
    /// replays only what committed after it — with a checkpoint at
    /// `last_tx_id`, that is nothing.
    pub checkpoint_tx_id: Option<u64>,
}

/// Drain `ctx` for a rolling restart (OPS-030): refuse new work, let
/// in-flight transactions commit, checkpoint, and report — all inside
/// `options.deadline`.
///
/// The steps, in order:
/// 1. **Refuse new work.** [`ShardContext::begin_drain`] flips the accept
///    loops and the session router; new connections, subscriptions and
///    reducer calls get a *retryable* signal, so an SDK reconnects to the
///    restarted process and retries rather than surfacing an error
///    (OPS-031). Already-admitted calls are untouched.
/// 2. **Quiesce.** A barrier job on the shard's FIFO single-writer queue
///    completes only after every previously-submitted reducer has committed
///    or rolled back — the queue's own ordering *is* the proof, so no
///    separate in-flight counter can drift from reality.
/// 3. **Checkpoint.** With the writer idle, `checkpoint` captures a
///    snapshot at the final commit, so restart replays little or nothing.
///    `None` skips it (the caller has no snapshot repo).
///
/// Callers own the actual exit: `drain` deliberately does not stop the
/// transports, so a caller can drain, inspect the report, and only then
/// `shutdown()` — the existing force path stays available and unchanged.
///
/// # Errors
/// Returns the checkpoint's error if the final checkpoint fails. Failing to
/// quiesce is not an error — it is reported as `quiesced: false`, since a
/// straggler must not block the deploy.
pub async fn drain(
    ctx: &Arc<ShardContext>,
    checkpoint: Option<&fluxum_core::checkpoint::SnapshotWorker>,
    options: DrainOptions,
) -> Result<DrainReport, fluxum_core::FluxumError> {
    let started = std::time::Instant::now();
    // 1. Stop admitting new work.
    ctx.begin_drain();

    // 2. Wait for in-flight transactions, bounded by the deadline.
    //
    // The barrier is submitted to the same FIFO single-writer queue, so it
    // runs only after every call already admitted — the queue's ordering
    // *is* the proof, which no separate in-flight counter could match. It is
    // also the shard's final commit, so its receipt names the tx id the
    // whole drain must make durable.
    let barrier = ctx.engine.pipeline().call(Box::new(|_tx| Ok(())));
    let mut barrier_tx: Option<u64> = None;
    let quiesced = match tokio::time::timeout(options.deadline, barrier).await {
        Ok(Ok(receipt)) => {
            barrier_tx = Some(receipt.tx_id);
            true
        }
        // The barrier itself failing (a rolled-back no-op) still means the
        // writer reached it, so everything before it is done.
        Ok(Err(_)) => true,
        Err(_) => {
            tracing::warn!(
                target: "fluxum::server",
                shard = ctx.shard_id,
                deadline_ms = u64::try_from(options.deadline.as_millis()).unwrap_or(u64::MAX),
                "drain deadline elapsed with transactions still in flight; \
                 force-closing stragglers"
            );
            false
        }
    };

    // 3. Final checkpoint, so restart replays little or nothing.
    //
    // The stamp comes from the **commit log**, not `health().last_tx_id`:
    // health tracks what the assembly *published* to the fan-out, whereas
    // the log is what is actually on disk. The distinction matters in both
    // directions — a checkpoint stamped past the durable tail would make
    // replay skip real commits, and one that trusted an assembly which
    // forgot to publish would silently under-cover.
    let log = ctx.engine.pipeline().log();
    // The log appends asynchronously, so a commit that has *returned* is not
    // yet on disk. A drain exists to lose nothing, so wait for the tail to
    // fsync before checkpointing and exiting — otherwise the process could
    // exit having acked writes that never landed. Bounded by the same
    // deadline: an fsync that will not complete must not hang the deploy.
    if let Some(tx_id) = barrier_tx
        && tokio::time::timeout(options.deadline, log.wait_durable(tx_id))
            .await
            .is_err()
    {
        tracing::warn!(
            target: "fluxum::server",
            shard = ctx.shard_id,
            tx_id,
            "drain deadline elapsed waiting for the commit log to become durable"
        );
    }
    let durable = log.durable_tx_id()?.unwrap_or(0);
    let checkpoint_tx_id = match checkpoint {
        // The worker only snapshots commits it has been *told* about (its
        // feed is an accelerator, decoupled from the commit path — see
        // `observe_commit`), so name the durable tail explicitly rather than
        // hoping that feed kept up: a drain that checkpointed short would
        // leave exactly the replay it exists to prevent.
        Some(worker) if durable > 0 => {
            worker.observe_commit(durable);
            let stats = worker.checkpoint_now()?;
            Some(stats.last_tx_id)
        }
        // Nothing durable has ever committed: there is no state to snapshot,
        // and asking would error rather than no-op.
        Some(_) | None => None,
    };

    let report = DrainReport {
        quiesced,
        last_tx_id: durable,
        checkpoint_tx_id,
    };
    tracing::info!(
        target: "fluxum::server",
        shard = ctx.shard_id,
        quiesced = report.quiesced,
        last_tx_id = report.last_tx_id,
        checkpoint_tx_id = report.checkpoint_tx_id,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "drain complete"
    );
    Ok(report)
}

/// Resolve when the process is asked to terminate — SIGTERM on Unix (what
/// an orchestrator sends before SIGKILL), Ctrl-C everywhere.
///
/// The trigger half of OPS-030: an assembly awaits this, calls [`drain`],
/// then stops its transports. It is a separate function from `drain` so the
/// signal source is swappable — tests and a `fluxum drain` command drive the
/// same drain path without a signal.
///
/// # Errors
/// Returns an error if the signal handler cannot be registered.
pub async fn terminate_requested() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigterm.recv() => {}
            result = tokio::signal::ctrl_c() => result?,
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        // Windows has no SIGTERM; Ctrl-C (and CTRL_CLOSE_EVENT, which tokio
        // maps onto it) is the equivalent stop request.
        tokio::signal::ctrl_c().await
    }
}
