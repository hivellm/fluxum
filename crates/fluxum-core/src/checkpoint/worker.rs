//! [`SnapshotWorker`] — the periodic checkpoint actor (STG-020/STG-022):
//! writes a checkpoint every `interval_tx` committed transactions on its own
//! OS thread, prunes retention (STG-023), and routes log truncation through
//! the archival hook (FR-104).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};

use crate::error::{FluxumError, Result};
use crate::hw::HardwareProfile;
use crate::store::MemStore;

use super::compact::{DirectoryArchive, SegmentArchive, compact_covered};
use super::repo::{CheckpointRepo, CheckpointStats};

/// Reference memory size for the adaptive cadence derivation (the 512 MiB
/// droplet profile, SPEC-016 §2).
const ADAPTIVE_REFERENCE_BYTES: u64 = 512 << 20;

/// Adaptive checkpoint cadence (FR-14 / FR-113): the STG-020 default of
/// 10,000 committed transactions at the 512 MiB reference profile, scaled
/// linearly with effective memory — larger machines hold more state per
/// checkpoint and replay faster, so they checkpoint less often — clamped to
/// `[1_000, 200_000]`. An explicit `storage.checkpoint_interval_tx` always
/// wins (HWA-010); this derivation is the `auto` path.
pub fn adaptive_interval_tx(profile: &HardwareProfile) -> u64 {
    let mib = profile.effective_memory_bytes() >> 20;
    (mib.saturating_mul(10_000) / (ADAPTIVE_REFERENCE_BYTES >> 20)).clamp(1_000, 200_000)
}

/// Log truncation wiring for the worker (STG-013 / FR-104).
#[derive(Debug, Clone)]
pub struct LogCompaction {
    /// The shard's commit-log directory.
    pub log_dir: PathBuf,
    /// Archive directory — segments are archived here before deletion when
    /// set (log archival enabled, the PITR source); deleted outright when
    /// `None`.
    pub archive_dir: Option<PathBuf>,
    /// Archived-copy retention (SPEC-014 REP-062, `replication.archive.
    /// retention`): copies older than this are swept after each checkpoint.
    /// `None` retains forever. This window IS the PITR window (§9).
    pub archive_retention: Option<std::time::Duration>,
}

/// Tuning knobs for a [`SnapshotWorker`].
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Checkpoint every this many committed transactions
    /// (`storage.checkpoint_interval_tx`, STG-020; default 10,000).
    pub interval_tx: u64,
    /// Checkpoints retained (`snapshot_retention` semantics, STG-023;
    /// default 3, minimum 2).
    pub retention: usize,
    /// Fencing epoch stamped into manifests (STG-011; replication raises it
    /// via a fresh worker, SPEC-014).
    pub epoch: u64,
    /// Optional checkpoint-driven log truncation (STG-013 / FR-104).
    pub compaction: Option<LogCompaction>,
    /// The shard's metrics registry, for the REP-081 pending-archival gauge.
    /// `None` (tests, embedded use) skips gauge publication.
    pub metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            interval_tx: 10_000,
            retention: 3,
            epoch: 0,
            compaction: None,
            metrics: None,
        }
    }
}

impl WorkerOptions {
    /// Defaults with the cadence derived from the hardware profile
    /// ([`adaptive_interval_tx`], FR-113).
    pub fn adaptive(profile: &HardwareProfile) -> Self {
        Self {
            interval_tx: adaptive_interval_tx(profile),
            ..Self::default()
        }
    }

    fn validate(&self) -> Result<()> {
        if self.interval_tx == 0 {
            return Err(FluxumError::Storage(
                "checkpoint interval_tx must be >= 1 (STG-020)".into(),
            ));
        }
        if self.retention < 2 {
            return Err(FluxumError::Storage(format!(
                "checkpoint retention {} is below the minimum of 2 (STG-023)",
                self.retention
            )));
        }
        Ok(())
    }
}

/// What the worker did over its lifetime, returned by
/// [`SnapshotWorker::close`].
#[derive(Debug, Clone)]
pub struct WorkerReport {
    /// Checkpoints written successfully.
    pub checkpoints: u64,
    /// Checkpoint / prune / compaction attempts that failed (each logged).
    pub failures: u64,
    /// `last_tx_id` of the newest checkpoint written (0 = none).
    pub last_checkpoint_tx: u64,
}

enum Msg {
    /// One transaction committed (its id).
    Committed(u64),
    /// Checkpoint immediately at the highest observed commit.
    Now(SyncSender<Result<CheckpointStats>>),
}

/// The periodic checkpoint actor (STG-020). Runs on a dedicated OS thread —
/// checkpoint writes never touch the commit path or an async runtime, and
/// the input is a wait-free [`MemStore::snapshot`], so reducer execution is
/// never blocked (STG-022): commits proceed while objects and the manifest
/// hit disk.
///
/// The manifest is stamped with the highest commit the worker was notified
/// of *before* taking the snapshot — a lower bound of the snapshot's actual
/// state, which [`super::recover`]'s convergent replay application is
/// defined for.
#[derive(Debug)]
pub struct SnapshotWorker {
    sender: Option<Sender<Msg>>,
    handle: Option<std::thread::JoinHandle<WorkerReport>>,
}

impl SnapshotWorker {
    /// Spawn the worker for `store`'s shard. The cadence resumes from the
    /// newest verified checkpoint already in `repo` (restart-safe).
    pub fn spawn(
        store: Arc<MemStore>,
        repo: Arc<CheckpointRepo>,
        shard_id: u32,
        options: WorkerOptions,
    ) -> Result<Self> {
        options.validate()?;
        let archive = match &options.compaction {
            Some(compaction) => match &compaction.archive_dir {
                Some(dir) => Some(DirectoryArchive::open(dir)?),
                None => None,
            },
            None => None,
        };
        let last_checkpoint_tx = repo.latest_verified_tx(shard_id)?.unwrap_or(0);
        let (sender, receiver) = channel();
        let actor = Actor {
            store,
            repo,
            shard_id,
            options,
            archive,
            last_checkpoint_tx,
            highest_committed: last_checkpoint_tx,
            report: WorkerReport {
                checkpoints: 0,
                failures: 0,
                last_checkpoint_tx,
            },
        };
        let handle = std::thread::Builder::new()
            .name(format!("fluxum-checkpoint-{shard_id}"))
            .spawn(move || actor.run(receiver))
            .map_err(FluxumError::Io)?;
        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
        })
    }

    /// Notify the worker of a committed transaction (call after the commit
    /// swap, with the committed `tx_id`). Never blocks the committer: the
    /// queue is unbounded and the worker drains it between checkpoints.
    pub fn observe_commit(&self, tx_id: u64) {
        if let Some(sender) = &self.sender {
            // A disconnected worker (already closed / panicked) is a no-op:
            // checkpointing is an accelerator, never a commit dependency.
            let _ = sender.send(Msg::Committed(tx_id));
        }
    }

    /// Write a checkpoint at the highest observed commit right now,
    /// bypassing the cadence (tests, operator tooling, replica seeding).
    /// Blocks until the write completes.
    pub fn checkpoint_now(&self) -> Result<CheckpointStats> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| FluxumError::Storage("checkpoint worker already closed".into()))?;
        let (ack, done) = sync_channel(1);
        sender
            .send(Msg::Now(ack))
            .map_err(|_| FluxumError::Storage("checkpoint worker stopped".into()))?;
        done.recv()
            .map_err(|_| FluxumError::Storage("checkpoint worker stopped".into()))?
    }

    /// Shut the worker down: drain pending notifications (writing any
    /// checkpoint the cadence still owes) and return the lifetime report.
    pub fn close(mut self) -> Result<WorkerReport> {
        self.sender.take(); // closes the queue; the actor drains and exits
        match self.handle.take() {
            Some(handle) => handle
                .join()
                .map_err(|_| FluxumError::Storage("checkpoint worker panicked".into())),
            None => Err(FluxumError::Storage(
                "checkpoint worker already closed".into(),
            )),
        }
    }
}

impl Drop for SnapshotWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct Actor {
    store: Arc<MemStore>,
    repo: Arc<CheckpointRepo>,
    shard_id: u32,
    options: WorkerOptions,
    archive: Option<DirectoryArchive>,
    last_checkpoint_tx: u64,
    highest_committed: u64,
    report: WorkerReport,
}

impl Actor {
    fn run(mut self, receiver: Receiver<Msg>) -> WorkerReport {
        while let Ok(msg) = receiver.recv() {
            self.handle(msg);
            // Drain whatever queued while we were checkpointing, so the
            // cadence check always sees the freshest commit.
            while let Ok(msg) = receiver.try_recv() {
                self.handle(msg);
            }
            if self.highest_committed
                >= self
                    .last_checkpoint_tx
                    .saturating_add(self.options.interval_tx)
            {
                self.checkpoint();
            }
        }
        self.report
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Committed(tx_id) => {
                self.highest_committed = self.highest_committed.max(tx_id);
            }
            Msg::Now(ack) => {
                let result = if self.highest_committed > self.last_checkpoint_tx {
                    self.checkpoint().ok_or_else(|| {
                        FluxumError::Storage("checkpoint write failed (see log)".into())
                    })
                } else {
                    Err(FluxumError::Storage(format!(
                        "nothing to checkpoint: no commit past {} observed",
                        self.last_checkpoint_tx
                    )))
                };
                let _ = ack.send(result);
            }
        }
    }

    /// Write one checkpoint stamped at `highest_committed` (a lower bound of
    /// the snapshot actually taken — see the type-level docs), then prune
    /// retention and compact the log up to the *oldest retained* checkpoint.
    fn checkpoint(&mut self) -> Option<CheckpointStats> {
        let stamp = self.highest_committed;
        let snapshot = self.store.snapshot();
        match self
            .repo
            .write(&snapshot, self.shard_id, stamp, self.options.epoch)
        {
            Ok(stats) => {
                self.last_checkpoint_tx = stamp;
                self.report.checkpoints += 1;
                self.report.last_checkpoint_tx = stamp;
                tracing::info!(
                    shard_id = self.shard_id,
                    last_tx_id = stamp,
                    objects_written = stats.objects_written,
                    objects_shared = stats.objects_shared,
                    "checkpoint written (STG-020)"
                );
                self.maintain();
                Some(stats)
            }
            Err(e) => {
                self.report.failures += 1;
                tracing::error!(
                    shard_id = self.shard_id,
                    error = %e,
                    "checkpoint write failed; will retry at the next cadence boundary"
                );
                None
            }
        }
    }

    /// Post-checkpoint maintenance: retention pruning (STG-023) and log
    /// truncation through the archival hook (STG-013 / FR-104). Failures are
    /// logged, never fatal — the checkpoint itself already committed.
    fn maintain(&mut self) {
        if let Err(e) = self.repo.prune(self.shard_id, self.options.retention) {
            self.report.failures += 1;
            tracing::error!(shard_id = self.shard_id, error = %e, "checkpoint prune failed");
        }
        let Some(compaction) = &self.options.compaction else {
            return;
        };
        // Compact only up to the OLDEST retained checkpoint: every retained
        // checkpoint keeps the log suffix it needs to replay forward, so the
        // STG-021 fallback stays recoverable after compaction.
        let oldest = match self.repo.list(self.shard_id) {
            Ok(refs) => match refs.first() {
                Some(r) => r.last_tx_id,
                None => return,
            },
            Err(e) => {
                self.report.failures += 1;
                tracing::error!(shard_id = self.shard_id, error = %e, "checkpoint list failed");
                return;
            }
        };
        let hook = self.archive.as_ref().map(|a| a as &dyn SegmentArchive);
        match compact_covered(&compaction.log_dir, self.shard_id, oldest, None, hook) {
            Ok(_removed) => {
                if let Some(metrics) = &self.options.metrics {
                    metrics.set_archive_segments_pending(0);
                }
            }
            Err(e) => {
                // REP-062: a failed archival blocks segment DELETION, never
                // writes — so this is a WARN plus the pending gauge, not a
                // fatal error. The segments stay in the live log and the
                // next checkpoint retries.
                self.report.failures += 1;
                let pending = covered_pending(&compaction.log_dir, self.shard_id, oldest);
                if let Some(metrics) = &self.options.metrics {
                    metrics.set_archive_segments_pending(pending);
                }
                tracing::warn!(
                    shard_id = self.shard_id,
                    error = %e,
                    segments_pending = pending,
                    "segment archival failed; covered segments retained until it succeeds \
                     (REP-062: deletion blocks, writes never do)"
                );
            }
        }
        // REP-062 retention: archived copies older than the window leave the
        // archive — which is exactly the PITR window shrinking forward.
        if let (Some(dir), Some(retention)) =
            (&compaction.archive_dir, compaction.archive_retention)
            && let Err(e) = sweep_archive(dir, retention)
        {
            tracing::warn!(
                shard_id = self.shard_id,
                error = %e,
                "archive retention sweep failed; stale copies remain until the next pass"
            );
        }
    }
}

/// How many fully covered segments are still awaiting archival in the live
/// log (the REP-081 `fluxum_archive_segments_pending` gauge after a failed
/// archival pass). Best-effort: an unreadable directory reports 0.
fn covered_pending(log_dir: &std::path::Path, shard_id: u32, covered_up_to_tx: u64) -> u64 {
    let Ok(segments) = crate::commitlog::segment::list_segments(log_dir, shard_id) else {
        return 0;
    };
    segments
        .windows(2)
        .filter(|pair| pair[1].first_tx_id <= covered_up_to_tx.saturating_add(1))
        .count() as u64
}

/// Delete archived segment copies older than `retention` (by modification
/// time — the archival copy instant, which is when the copy became durable).
fn sweep_archive(
    dir: &std::path::Path,
    retention: std::time::Duration,
) -> crate::error::Result<()> {
    let now = std::time::SystemTime::now();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Only segment copies are swept — never lineage markers, sidecars,
        // or anything else an operator parked in the directory.
        if !(name.starts_with("shard-") && name.ends_with(".log")) {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if now.duration_since(modified).unwrap_or_default() > retention {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
