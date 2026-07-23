//! Transaction pipeline (SPEC-003, T3.1) — the per-shard single-writer loop
//! that turns reducer jobs into durable commits:
//! **validate → merge into `CommittedState` → append to the `CommitLog` →
//! respond** (TXN-021), with rollback as pure `TxState` discard (TXN-022).
//!
//! This module is orchestration only: it composes the T2.1 store
//! ([`MemStore`] — MVCC snapshot + single-writer [`Tx`], eager TXN-040/041
//! constraint checks, TXN-042 auto-inc) with the T2.2 log ([`CommitLog`] —
//! group-commit flush actor, STG-012) and adds nothing the seams already
//! provide.
//!
//! # Design decisions (T3.1)
//!
//! - **One queue, one writer** (TXN-010): every write reaches a shard through
//!   [`TxPipeline::submit`], which enqueues onto a bounded MPSC channel
//!   drained by exactly one [`TxPipelineWorker`]. Jobs execute strictly in
//!   arrival order, so at most one `TxState` exists per shard at any time —
//!   the commit history is serial by construction. Readers never touch the
//!   queue: [`MemStore::snapshot`] stays wait-free, so concurrent reads never
//!   block (and are never blocked by) the sequential writer (TXN-060).
//! - **Backpressure is immediate, not blocking** (TXN-011): `submit` uses
//!   `try_send`; a full queue answers `CLUSTER_SHARD_UNAVAILABLE` ("shard
//!   busy") right away instead of parking the transport task. Capacity
//!   defaults to 1,000 ([`TxPipelineOptions::queue_capacity`]).
//! - **Respond after the in-memory merge, not after fsync** (TXN-021 steps
//!   9/12, TXN-004): a successful commit is appended to the `CommitLog`
//!   writer queue — the durability handoff — and the caller gets its
//!   [`CommitReceipt`] without waiting for the disk write. Callers that need
//!   the durable watermark gate on [`CommitLog::wait_durable`] /
//!   [`CommitLog::subscribe_durable`]. This is the documented ~50 ms
//!   OS-crash window that buys NFR-03 (commit p99 < 1 ms).
//! - **Every committed `tx_id` is logged, gap-free** (TXN-030): ids are
//!   assigned by the store's writer state (rollbacks never consume one) and
//!   every commit — including an empty diff, e.g. a transaction whose writes
//!   cancelled out — is appended, so the log's `tx_id` sequence is exactly
//!   1, 2, 3, … and recovery resumes at `last_replayed_tx_id + 1`
//!   ([`crate::checkpoint::recover`] + [`CommitLog::open`]).
//! - **Panic = rollback** (TXN-022): the reducer job runs under
//!   `catch_unwind`; a panic produces the same pure `TxState` discard as an
//!   `Err` return, responds with a wire-ready 500, writes no log entry, and
//!   the worker keeps serving subsequent calls — a module author's bug never
//!   takes the shard down.
//! - **The receipt carries the [`TxDiff`]** — the seam SPEC-005's
//!   `SubscriptionManager::on_commit` (TXN-021 step 10) and the T3.2
//!   `ReducerContext` result path consume; T3.1 deliberately does not
//!   interpret it.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use fluxum_protocol::codes;

use crate::commitlog::CommitLog;
use crate::error::{FluxumError, Result};
use crate::store::{MemStore, Tx, TxDiff};
use crate::types::Timestamp;

/// A reducer job: the transaction body executed by the shard's single
/// writer. `Ok(())` commits; `Err` (or a panic) rolls back (TXN-021/022).
///
/// T3.2's `ReducerContext` wraps the module author's typed reducer into one
/// of these; T3.1 tests and internal callers hand closures in directly.
pub type ReducerFn = Box<dyn for<'a, 'b> FnOnce(&'a mut Tx<'b>) -> Result<()> + Send + 'static>;

/// The fan-out seam (TXN-021 steps 9/10): called by the shard's single
/// writer the moment a commit becomes visible — after the atomic merge,
/// before the commit-log handoff — so subscription delivery starts without
/// waiting for the durability enqueue, the written watermark, or the
/// caller's response hop. The *ack* still gates on
/// [`CommitLog::wait_written`] (TXN-004): a subscriber may see a commit that
/// a crash inside the documented ~50 ms window erases, which the SPEC-021
/// reconnect resync heals — the same trade respond-after-merge already made.
pub type CommitHook = Box<dyn Fn(&TxDiff) + Send + Sync + 'static>;

/// Tuning knobs for a [`TxPipeline`] (SPEC-003 §3; wired into `config.yml`
/// with the server assembly).
#[derive(Debug, Clone, Copy)]
pub struct TxPipelineOptions {
    /// Bounded reducer-queue capacity (TXN-011; default 1,000). When the
    /// queue is full, [`TxPipeline::submit`] answers `503 "shard busy"`
    /// immediately. Must be ≥ 1.
    pub queue_capacity: usize,
}

impl Default for TxPipelineOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 1_000,
        }
    }
}

/// What a committed transaction hands back to its caller (TXN-021 step 12).
///
/// Returned after the atomic in-memory merge and the commit-log enqueue —
/// durability is asynchronous (TXN-004; gate on
/// [`CommitLog::wait_durable`] where required).
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    /// The committed transaction's id (strictly increasing per shard,
    /// TXN-030) — the value `TxUpdate` messages carry (TXN-031).
    pub tx_id: u64,
    /// The committed effect — the input to subscription evaluation
    /// (SPEC-005, TXN-021 step 10). Rows are `Arc`-shared with the store.
    pub diff: TxDiff,
}

/// Audit provenance for a commit (SPEC-025 OPS-020): who called, and which
/// reducer. Carried through the pipeline to the commit-log record so the
/// audit trail can answer "who changed this row, and when".
#[derive(Debug, Clone)]
pub struct CommitMeta {
    /// The identity the reducer ran under.
    pub caller: crate::types::Identity,
    /// The reducer's name.
    pub reducer_name: String,
}

impl CommitMeta {
    /// Untagged provenance (zero identity, no name) — for internal or test
    /// commits whose caller/reducer are not meaningful to an audit.
    pub fn anonymous() -> Self {
        Self {
            caller: crate::types::Identity::from_bytes([0u8; 32]),
            reducer_name: String::new(),
        }
    }
}

/// One queued reducer call.
struct Job {
    reducer: ReducerFn,
    meta: CommitMeta,
    respond: oneshot::Sender<Result<CommitReceipt>>,
}

/// The submission handle to a shard's transaction pipeline (TXN-010).
///
/// Cheap to clone — transports hold one per connection task. All writes
/// funnel through [`TxPipeline::submit`] / [`TxPipeline::call`]; reads go
/// straight to [`MemStore::snapshot`] via [`TxPipeline::store`] and never
/// queue.
#[derive(Clone)]
pub struct TxPipeline {
    sender: mpsc::Sender<Job>,
    store: Arc<MemStore>,
    log: Arc<CommitLog>,
    /// Shared with the worker; installed once by the assembly that owns the
    /// shard's fan-out (see [`TxPipeline::set_commit_hook`]).
    commit_hook: Arc<std::sync::OnceLock<CommitHook>>,
}

impl std::fmt::Debug for TxPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxPipeline")
            .field("commit_hook", &self.commit_hook.get().is_some())
            .finish_non_exhaustive()
    }
}

impl TxPipeline {
    /// Build a pipeline over an assembled store and its shard's commit log.
    ///
    /// The store must already be recovered ([`crate::checkpoint::recover`])
    /// when starting from existing data, so tx-id assignment resumes at
    /// `last_replayed_tx_id + 1` (TXN-030) — the log rejects anything else
    /// at the door (STG-015).
    ///
    /// Returns the clonable submission handle and the [`TxPipelineWorker`],
    /// which the caller drives (`worker.run().await`) as the shard's single
    /// writer — typically on its own spawned task.
    pub fn new(
        store: Arc<MemStore>,
        log: Arc<CommitLog>,
        options: TxPipelineOptions,
    ) -> Result<(Self, TxPipelineWorker)> {
        if options.queue_capacity == 0 {
            return Err(FluxumError::Storage(
                "reducer queue_capacity must be >= 1 (TXN-011)".into(),
            ));
        }
        let (sender, receiver) = mpsc::channel(options.queue_capacity);
        let commit_hook = Arc::new(std::sync::OnceLock::new());
        let pipeline = Self {
            sender,
            store: Arc::clone(&store),
            log: Arc::clone(&log),
            commit_hook: Arc::clone(&commit_hook),
        };
        let worker = TxPipelineWorker {
            receiver,
            store,
            log,
            commit_hook,
        };
        Ok((pipeline, worker))
    }

    /// Install the commit-visibility fan-out hook (TXN-021 steps 9/10) —
    /// the single writer calls it with every committed diff, in `tx_id`
    /// order, before the commit-log handoff. First install wins (`false`
    /// reports a hook was already there); the server assembly binds it to
    /// the shard's commit broadcast when the owning context is created, so
    /// a pipeline without an assembly simply commits without fan-out,
    /// exactly as before.
    pub fn set_commit_hook(&self, hook: CommitHook) -> bool {
        self.commit_hook.set(hook).is_ok()
    }

    /// Enqueue a reducer job (TXN-010: processed in arrival order by the
    /// single writer). Never blocks: a full queue answers
    /// `CLUSTER_SHARD_UNAVAILABLE` ("shard busy") immediately (TXN-011), and a stopped worker is a
    /// storage error. On success, the returned receiver resolves to the
    /// job's commit receipt or rollback error once the writer reaches it.
    pub fn submit(&self, reducer: ReducerFn) -> Result<oneshot::Receiver<Result<CommitReceipt>>> {
        self.submit_with(CommitMeta::anonymous(), reducer)
    }

    /// [`TxPipeline::submit`] tagging the commit with its audit provenance
    /// (SPEC-025 OPS-020) — the reducer engine passes the real caller/name.
    pub fn submit_with(
        &self,
        meta: CommitMeta,
        reducer: ReducerFn,
    ) -> Result<oneshot::Receiver<Result<CommitReceipt>>> {
        let (respond, receipt) = oneshot::channel();
        match self.sender.try_send(Job {
            reducer,
            meta,
            respond,
        }) {
            Ok(()) => Ok(receipt),
            Err(mpsc::error::TrySendError::Full(_)) => Err(FluxumError::query(
                codes::CLUSTER_SHARD_UNAVAILABLE,
                "shard busy",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(FluxumError::Storage(
                "transaction pipeline worker stopped".into(),
            )),
        }
    }

    /// Pending jobs in the single-writer queue (OBS-012): the bounded
    /// channel's capacity minus its remaining slots. A sustained high value
    /// means the shard is overloaded.
    pub fn queue_depth(&self) -> u64 {
        let capacity = self.sender.max_capacity();
        let available = self.sender.capacity();
        u64::try_from(capacity.saturating_sub(available)).unwrap_or(0)
    }

    /// Submit a reducer job and await its outcome — `submit` plus the
    /// response wait. Backpressure semantics are unchanged: a full queue
    /// errors immediately with `503 "shard busy"` (TXN-011).
    pub async fn call(&self, reducer: ReducerFn) -> Result<CommitReceipt> {
        self.call_with(CommitMeta::anonymous(), reducer).await
    }

    /// [`TxPipeline::call`] tagging the commit with its audit provenance
    /// (SPEC-025 OPS-020).
    pub async fn call_with(&self, meta: CommitMeta, reducer: ReducerFn) -> Result<CommitReceipt> {
        let receipt = self.submit_with(meta, reducer)?;
        receipt.await.map_err(|_| {
            FluxumError::Storage("transaction pipeline worker dropped the call".into())
        })?
    }

    /// The shard's store — the lock-free read surface
    /// ([`MemStore::snapshot`], TXN-060: reads never queue behind writes).
    pub fn store(&self) -> &Arc<MemStore> {
        &self.store
    }

    /// The shard's commit log — for durability gating
    /// ([`CommitLog::wait_durable`], [`CommitLog::subscribe_durable`]).
    pub fn log(&self) -> &Arc<CommitLog> {
        &self.log
    }
}

/// The shard's single writer (TXN-010): drains the reducer queue in arrival
/// order, one transaction at a time.
pub struct TxPipelineWorker {
    receiver: mpsc::Receiver<Job>,
    store: Arc<MemStore>,
    log: Arc<CommitLog>,
    commit_hook: Arc<std::sync::OnceLock<CommitHook>>,
}

impl std::fmt::Debug for TxPipelineWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxPipelineWorker")
            .field("commit_hook", &self.commit_hook.get().is_some())
            .finish_non_exhaustive()
    }
}

impl TxPipelineWorker {
    /// The ShardHost main loop (SPEC-003 §3). Runs until every
    /// [`TxPipeline`] handle is dropped, then drains what was already
    /// queued and returns.
    pub async fn run(mut self) {
        while let Some(job) = self.receiver.recv().await {
            let result = self.process(job.reducer, &job.meta).await;
            // A caller that gave up on its receipt does not affect the
            // committed state; drop the response.
            let _ = job.respond.send(result);
        }
    }

    /// One full TXN-021/022 cycle: begin → execute → commit or rollback →
    /// append. Validation already happened eagerly inside the `Tx` write
    /// methods (TXN-040/041/042), so a returned diff is valid by
    /// construction.
    async fn process(&self, reducer: ReducerFn, meta: &CommitMeta) -> Result<CommitReceipt> {
        let timestamp = Timestamp::now();
        let diff = execute(&self.store, reducer)?;
        // TXN-021 steps 9/10 are concurrent: fan-out starts at commit
        // visibility, in tx_id order by construction (this loop is the only
        // caller). The append below is the durability handoff, not a
        // delivery gate — the ack's TXN-004 written-watermark wait stays
        // with the caller (P0-A 1.3, F-006).
        if let Some(hook) = self.commit_hook.get() {
            hook(&diff);
        }
        let tx_id = diff.tx_id;
        // TXN-021 step 9 / TXN-004: enqueue on the commit-log writer before
        // responding. `append` waits for queue acceptance (STG-012
        // backpressure), never for fsync. Every commit is appended — even
        // an empty diff — so the logged tx_id sequence stays gap-free
        // (TXN-030). An append failure after the in-memory merge means the
        // log writer died (fatal per STG-012): the error is reported and
        // every subsequent commit will fail the same way — memory and log
        // never silently diverge.
        // SPEC-023 DMX-010: ephemeral-table mutations never reach the commit
        // log (nor, downstream, checkpoints or replication). The full `diff`
        // still drives subscription fan-out below — only the *logged* view is
        // filtered. An ephemeral-only transaction still appends a (row-data-
        // free) record, so the durable `tx_id` sequence stays gap-free
        // (TXN-030); the zero-append optimization is a documented follow-up.
        let has_ephemeral = diff
            .tables
            .iter()
            .any(|t| self.store.is_ephemeral(t.table_id))
            || diff
                .auto_inc
                .iter()
                .any(|(table, _)| self.store.is_ephemeral(*table));
        if has_ephemeral {
            let durable = TxDiff {
                tx_id: diff.tx_id,
                tables: diff
                    .tables
                    .iter()
                    .filter(|t| !self.store.is_ephemeral(t.table_id))
                    .cloned()
                    .collect(),
                auto_inc: diff
                    .auto_inc
                    .iter()
                    .copied()
                    .filter(|(table, _)| !self.store.is_ephemeral(*table))
                    .collect(),
            };
            self.log
                .append_diff_as(&durable, timestamp, meta.caller, &meta.reducer_name)
                .await?;
        } else {
            self.log
                .append_diff_as(&diff, timestamp, meta.caller, &meta.reducer_name)
                .await?;
        }
        Ok(CommitReceipt { tx_id, diff })
    }
}

/// Execute one reducer job on the writer: TXN-020 begin, run under a panic
/// boundary, then TXN-021 commit or TXN-022 rollback.
fn execute(store: &MemStore, reducer: ReducerFn) -> Result<TxDiff> {
    let mut tx = store.begin();
    // AssertUnwindSafe: the closure only *borrows* `tx`, so an unwinding
    // panic drops nothing here — `tx` survives and is rolled back below,
    // discarding the whole `TxState` buffer (no partial state can be
    // observed: buffered writes never touch shared structures, STG-006).
    // The store's writer mutex is not poisoned by this panic (its guard
    // lives in `tx`, which does not unwind), and `MemStore::begin` recovers
    // from poison regardless.
    match std::panic::catch_unwind(AssertUnwindSafe(|| reducer(&mut tx))) {
        // TXN-021: the merge publishes atomically; constraint validation
        // already ran at write time. A (never-expected) merge invariant
        // failure returns Err *before* the snapshot swap and before the
        // tx id is consumed — indistinguishable from a rollback.
        Ok(Ok(())) => tx.commit(),
        // TXN-022: pure discard; the tx id is not consumed (TXN-030).
        Ok(Err(e)) => {
            tx.rollback();
            Err(e)
        }
        Err(payload) => {
            tx.rollback();
            // SPEC-028: a panic is 5002 REDUCER_PANIC, never a user error.
            Err(FluxumError::ReducerPanic(format!(
                "{} (transaction rolled back, TXN-022)",
                panic_message(payload.as_ref())
            )))
        }
    }
}

/// Best-effort human-readable panic payload (`panic!` with a literal or a
/// formatted string covers virtually all reducer panics). Shared with the
/// SPEC-010 migration runner's MIG-040 panic boundary.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "non-string panic payload"
    }
}
