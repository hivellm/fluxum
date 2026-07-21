//! [`CommitLog`] — the per-shard append-only log writer (STG-010): recovery
//! on open (torn-tail quarantine, STG-031), the group-commit flush actor
//! with a published durable offset (STG-012), rotation (STG-013), and
//! compaction with a replication retention hold.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{mpsc, oneshot, watch};

use crate::error::{FluxumError, Result};
use crate::store::TxDiff;
use crate::types::{Identity, Timestamp};

use super::CommitLogOptions;
use super::format::encode_entry;
use super::record::TxRecord;
use super::segment::{
    QuarantineReport, ScanOutcome, SegmentFile, SegmentRef, create_segment, list_segments,
    open_segment_for_append, quarantine_tail, quarantine_whole_file, scan_segment,
};

/// What recovery found when the log was opened (STG-030/STG-031 inputs).
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// Last valid transaction on disk; `None` for an empty log (distinct
    /// from "tx 0 durable" — there is no tx 0).
    pub last_tx_id: Option<u64>,
    /// Highest epoch recovered from headers and entry envelopes.
    pub epoch: u64,
    /// Number of live segment files after recovery.
    pub segments: usize,
    /// The STG-031 quarantine performed, if the tail was torn or corrupt.
    pub quarantine: Option<QuarantineReport>,
}

/// Durability watermark published by the flush actor (STG-012).
#[derive(Debug, Clone)]
pub enum DurableState {
    /// Highest fsynced `tx_id`; `None` = nothing durable yet. Advances
    /// monotonically.
    Durable(Option<u64>),
    /// The writer hit a fatal I/O error (a failed fsync leaves the on-disk
    /// state undefined — no retry, STG-012) and has stopped.
    Failed(Arc<str>),
}

enum Cmd {
    Append(TxRecord),
    SetEpoch {
        epoch: u64,
        ack: oneshot::Sender<Result<()>>,
    },
}

/// The per-shard append-only commit log (STG-010). Also the replication
/// stream (STG-016): replicas consume the same entry bytes by offset.
///
/// Appends are fed to a dedicated background flush actor over a bounded
/// queue; the actor drains the queue in batches, appends, and performs one
/// `fsync` per batch, then publishes the durable offset on a watch channel
/// (STG-012). The ack path never calls `fsync` inline.
#[derive(Debug)]
pub struct CommitLog {
    dir: PathBuf,
    shard_id: u32,
    sender: Option<mpsc::Sender<Cmd>>,
    durable: watch::Receiver<DurableState>,
    /// The TXN-004 ack watermark: highest `tx_id` whose bytes reached the OS
    /// (`write` returned; fsync may still be pending). Survives a *process*
    /// crash; an OS crash can still lose the write-behind window (~50 ms).
    written: watch::Receiver<DurableState>,
    /// Highest tx id accepted into the queue (0 = none yet) — enforces
    /// strictly increasing `tx_id` at the door (STG-015).
    last_appended: AtomicU64,
    epoch: AtomicU64,
    fsyncs: Arc<AtomicU64>,
    /// Lowest tx offset still needed by a connected replica; segments
    /// containing it or later entries survive compaction (STG-013).
    retention_hold: Mutex<Option<u64>>,
    actor: Option<std::thread::JoinHandle<()>>,
    recovery: RecoveryReport,
}

impl CommitLog {
    /// Open (or create) the shard's log in `dir` under `epoch`, running
    /// recovery first: every segment is validated; a torn or corrupt tail is
    /// quarantined to a sidecar (STG-031) and appends resume at the last
    /// valid boundary. Fails if `epoch` is lower than the highest durably
    /// written epoch (STG-011) or if a non-tail segment is corrupt (that is
    /// `reset_to` territory for the replication layer, never implicit).
    pub fn open(dir: &Path, shard_id: u32, epoch: u64, options: CommitLogOptions) -> Result<Self> {
        options.validate()?;
        std::fs::create_dir_all(dir)?;
        let (recovery, tail) = recover(dir, shard_id)?;
        if epoch < recovery.epoch {
            return Err(FluxumError::Storage(format!(
                "epoch {epoch} rejected: the log has durably written epoch {} (STG-011)",
                recovery.epoch
            )));
        }
        let tail = match tail {
            Some((path, len)) => Some(open_segment_for_append(&path, len)?),
            None => None,
        };

        let (sender, receiver) = mpsc::channel(options.queue_depth);
        let (watch_tx, watch_rx) = watch::channel(DurableState::Durable(recovery.last_tx_id));
        let (written_tx, written_rx) = watch::channel(DurableState::Durable(recovery.last_tx_id));
        let fsyncs = Arc::new(AtomicU64::new(0));
        let sync_handle = match &tail {
            Some(seg) => Some(Arc::new(seg.file.try_clone().map_err(FluxumError::Io)?)),
            None => None,
        };
        let sync = Arc::new(SyncShared {
            watch: watch_tx,
            cursor: std::sync::Mutex::new(SyncCursor {
                handle: sync_handle.clone(),
                written: recovery.last_tx_id,
                synced: recovery.last_tx_id,
                shutdown: false,
            }),
            cv: std::sync::Condvar::new(),
            fsyncs: Arc::clone(&fsyncs),
        });
        let syncer = {
            let sync = Arc::clone(&sync);
            std::thread::Builder::new()
                .name(format!("fluxum-commitlog-sync-{shard_id}"))
                .spawn(move || sync.run())
                .map_err(FluxumError::Io)?
        };
        let actor_state = Actor {
            dir: dir.to_path_buf(),
            shard_id,
            options,
            epoch,
            current: tail,
            buf: Vec::with_capacity(options.write_buffer_bytes),
            last_written: recovery.last_tx_id,
            written_watch: written_tx,
            sync,
            sync_handle,
            syncer: Some(syncer),
        };
        let actor = std::thread::Builder::new()
            .name(format!("fluxum-commitlog-{shard_id}"))
            .spawn(move || actor_state.run(receiver))
            .map_err(FluxumError::Io)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            shard_id,
            sender: Some(sender),
            durable: watch_rx,
            written: written_rx,
            last_appended: AtomicU64::new(recovery.last_tx_id.unwrap_or(0)),
            epoch: AtomicU64::new(epoch),
            fsyncs,
            retention_hold: Mutex::new(None),
            actor: Some(actor),
            recovery,
        })
    }

    /// What recovery found when this log was opened.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// This log's shard.
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// Total on-disk bytes of this log's segments — the durable footprint a
    /// per-tenant storage quota is measured against (SPEC-025 OPS-060). A
    /// segment that vanishes mid-scan (compaction) simply contributes zero.
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0u64;
        for seg in list_segments(&self.dir, self.shard_id)? {
            if let Ok(meta) = std::fs::metadata(&seg.path) {
                total = total.saturating_add(meta.len());
            }
        }
        Ok(total)
    }

    /// Run an audit query over this log's durable segments (SPEC-025 OPS-020).
    /// Reads only flushed records; a just-committed transaction appears once
    /// it is durable ([`CommitLog::wait_durable`]).
    pub fn audit(
        &self,
        schema: &crate::schema::TableSchema,
        query: &super::audit::AuditQuery,
    ) -> Result<Vec<super::audit::AuditEntry>> {
        super::audit::audit(&self.dir, self.shard_id, schema, query)
    }

    /// The current fencing epoch entries are stamped with.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Enqueue one committed transaction for durable append (STG-010). The
    /// call returns once the record is accepted by the flush actor's bounded
    /// queue (backpressure) — durability is asynchronous; gate on
    /// [`CommitLog::wait_durable`] or the watch channel where required.
    ///
    /// Rejects a `tx_id` that does not strictly increase (STG-015) and a
    /// record for another shard.
    pub async fn append(&self, record: TxRecord) -> Result<u64> {
        if record.shard_id != self.shard_id {
            return Err(FluxumError::Storage(format!(
                "record for shard {} appended to the shard-{} log",
                record.shard_id, self.shard_id
            )));
        }
        let tx_id = record.tx_id;
        let last = self.last_appended.load(Ordering::SeqCst);
        if tx_id <= last {
            return Err(FluxumError::Storage(format!(
                "tx_id {tx_id} does not strictly increase past {last} (STG-015)"
            )));
        }
        self.send(Cmd::Append(record)).await?;
        self.last_appended.store(tx_id, Ordering::SeqCst);
        Ok(tx_id)
    }

    /// Convenience: build a [`TxRecord`] from the T2.1 commit output and
    /// append it. The commit is tagged anonymously (zero identity, no reducer
    /// name) — for an audited commit use [`CommitLog::append_diff_as`].
    pub async fn append_diff(&self, diff: &TxDiff, timestamp: Timestamp) -> Result<u64> {
        self.append_diff_as(
            diff,
            timestamp,
            Identity::from_bytes([0u8; 32]),
            String::new(),
        )
        .await
    }

    /// Append a commit tagged with the identity and reducer that produced it
    /// (SPEC-025 OPS-020 audit trail).
    pub async fn append_diff_as(
        &self,
        diff: &TxDiff,
        timestamp: Timestamp,
        caller: Identity,
        reducer: impl AsRef<str>,
    ) -> Result<u64> {
        self.append(TxRecord::from_diff(
            diff,
            self.shard_id,
            timestamp,
            caller,
            reducer.as_ref(),
        ))
        .await
    }

    /// Raise the fencing epoch (SPEC-014 leader lineage). Pending appends
    /// are flushed and fsynced under the old epoch first; a value lower than
    /// the current epoch is rejected (STG-011).
    pub async fn set_epoch(&self, epoch: u64) -> Result<()> {
        let current = self.epoch.load(Ordering::SeqCst);
        if epoch < current {
            return Err(FluxumError::Storage(format!(
                "epoch {epoch} rejected: current epoch is {current} (STG-011)"
            )));
        }
        let (ack, done) = oneshot::channel();
        self.send(Cmd::SetEpoch { epoch, ack }).await?;
        done.await.map_err(|_| self.writer_gone_error())??;
        self.epoch.store(epoch, Ordering::SeqCst);
        Ok(())
    }

    /// The current durable watermark: highest fsynced `tx_id` (STG-012).
    pub fn durable_tx_id(&self) -> Result<Option<u64>> {
        match &*self.durable.borrow() {
            DurableState::Durable(tx) => Ok(*tx),
            DurableState::Failed(msg) => Err(FluxumError::Storage(msg.to_string())),
        }
    }

    /// Subscribe to the durable-offset watch channel — the primitive
    /// replication quorum acks and confirmed reads gate on (STG-012).
    pub fn subscribe_durable(&self) -> watch::Receiver<DurableState> {
        self.durable.clone()
    }

    /// Wait until `tx_id` is fsynced (or the writer fails).
    pub async fn wait_durable(&self, tx_id: u64) -> Result<()> {
        Self::wait_watermark(self.durable.clone(), tx_id, "durable").await
    }

    /// The TXN-004 ack watermark: highest `tx_id` handed to the OS (`write`
    /// returned; fsync possibly still pending). `None` = nothing written yet.
    pub fn written_tx_id(&self) -> Result<Option<u64>> {
        match &*self.written.borrow() {
            DurableState::Durable(tx) => Ok(*tx),
            DurableState::Failed(msg) => Err(FluxumError::Storage(msg.to_string())),
        }
    }

    /// Wait until `tx_id`'s bytes have reached the OS (or the writer fails) —
    /// the TXN-004 gate a `ReducerResult` ack requires: past this point a
    /// *process* crash cannot lose the transaction, while fsync stays off the
    /// ack path (the documented ~50 ms window applies to OS crashes only).
    pub async fn wait_written(&self, tx_id: u64) -> Result<()> {
        Self::wait_watermark(self.written.clone(), tx_id, "written").await
    }

    /// Wait until a watermark channel reaches `tx_id` (or its writer fails).
    async fn wait_watermark(
        mut rx: watch::Receiver<DurableState>,
        tx_id: u64,
        what: &str,
    ) -> Result<()> {
        loop {
            match &*rx.borrow_and_update() {
                DurableState::Failed(msg) => {
                    return Err(FluxumError::Storage(msg.to_string()));
                }
                DurableState::Durable(Some(mark)) if *mark >= tx_id => return Ok(()),
                DurableState::Durable(_) => {}
            }
            if rx.changed().await.is_err() {
                // Writer exited; one final check against its last publish.
                return match &*rx.borrow() {
                    DurableState::Durable(Some(mark)) if *mark >= tx_id => Ok(()),
                    DurableState::Failed(msg) => Err(FluxumError::Storage(msg.to_string())),
                    DurableState::Durable(_) => Err(FluxumError::Storage(format!(
                        "commit-log writer closed before tx {tx_id} became {what}"
                    ))),
                };
            }
        }
    }

    /// Number of `fsync` calls performed so far — far below the transaction
    /// count under load (STG-012, SPEC-002 acceptance 8).
    pub fn fsync_count(&self) -> u64 {
        self.fsyncs.load(Ordering::SeqCst)
    }

    /// Set (or clear) the replication retention hold: the lowest tx offset a
    /// connected replica still needs. Segments containing entries at or past
    /// the hold survive compaction regardless of checkpoint coverage
    /// (STG-013).
    pub fn set_retention_hold(&self, hold: Option<u64>) {
        *self
            .retention_hold
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = hold;
    }

    /// Delete segments fully covered by a completed checkpoint at
    /// `covered_up_to_tx` (STG-013 / FR-14). A segment is deletable only if
    /// it is not the active tail, every entry in it is `<= covered_up_to_tx`,
    /// and no retention hold needs it. Returns the deleted paths.
    pub fn compact(&self, covered_up_to_tx: u64) -> Result<Vec<PathBuf>> {
        let hold = *self
            .retention_hold
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let segments = list_segments(&self.dir, self.shard_id)?;
        let mut deleted = Vec::new();
        for pair in segments.windows(2) {
            // The segment's entries end where the next segment begins.
            let next_first = pair[1].first_tx_id;
            let covered = next_first <= covered_up_to_tx.saturating_add(1);
            let held = hold.is_some_and(|h| next_first > h);
            if covered && !held {
                std::fs::remove_file(&pair[0].path)?;
                deleted.push(pair[0].path.clone());
            }
        }
        Ok(deleted)
    }

    /// Shut the writer down: drain and fsync everything queued, then return
    /// the final durable offset. Blocks briefly on the actor thread.
    pub fn close(mut self) -> Result<Option<u64>> {
        self.sender.take(); // closes the queue; the actor drains and exits
        if let Some(actor) = self.actor.take() {
            actor
                .join()
                .map_err(|_| FluxumError::Storage("commit-log writer panicked".into()))?;
        }
        match &*self.durable.borrow() {
            DurableState::Durable(tx) => Ok(*tx),
            DurableState::Failed(msg) => Err(FluxumError::Storage(msg.to_string())),
        }
    }

    async fn send(&self, cmd: Cmd) -> Result<()> {
        // A fatal write poisons the writer: the actor publishes `Failed` on
        // the durable watch, then exits and drops the receiver. Between those
        // two steps the bounded channel still has buffer, so a `send` could
        // be accepted after the failure is already observable through
        // `wait_durable`/`durable_tx_id` — a race that let a post-failure
        // append return `Ok`. Gate on the published state first so every
        // command surface reports the poison deterministically, rather than
        // relying on the receiver drop having landed.
        if matches!(&*self.durable.borrow(), DurableState::Failed(_)) {
            return Err(self.writer_gone_error());
        }
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| FluxumError::Storage("commit-log writer already closed".into()))?;
        sender.send(cmd).await.map_err(|_| self.writer_gone_error())
    }

    fn writer_gone_error(&self) -> FluxumError {
        match &*self.durable.borrow() {
            DurableState::Failed(msg) => FluxumError::Storage(format!(
                "commit-log writer stopped after a fatal error: {msg}"
            )),
            DurableState::Durable(_) => FluxumError::Storage("commit-log writer stopped".into()),
        }
    }
}

/// Scan and repair the on-disk log; returns the report and the resume tail
/// `(path, valid_len)`, if any segment survives.
fn recover(dir: &Path, shard_id: u32) -> Result<(RecoveryReport, Option<(PathBuf, u64)>)> {
    let segments = list_segments(dir, shard_id)?;
    let mut report = RecoveryReport {
        last_tx_id: None,
        epoch: 0,
        segments: segments.len(),
        quarantine: None,
    };
    let mut tail: Option<(PathBuf, u64)> = None;
    let mut prev_tx: Option<u64> = None;
    let mut min_epoch = 0u64;
    let mut visit = |_: u64, _: TxRecord| Ok(());

    let non_tail_corruption = |seg: &SegmentRef, detail: &str| {
        FluxumError::Storage(format!(
            "corruption in non-tail segment {}: {detail} — refusing to open for append; \
             repair requires an explicit reset_to (STG-031)",
            seg.path.display()
        ))
    };

    for (i, seg) in segments.iter().enumerate() {
        let is_tail = i == segments.len() - 1;
        match scan_segment(&seg.path, shard_id, prev_tx, min_epoch, &mut visit)? {
            ScanOutcome::HeaderCorrupt(reason) => {
                if !is_tail {
                    return Err(non_tail_corruption(seg, &reason));
                }
                let q = quarantine_whole_file(&seg.path, &reason)?;
                notify_quarantine(&q, report.last_tx_id);
                report.quarantine = Some(q);
                report.segments -= 1;
                // `tail` stays on the previous (clean) segment.
            }
            ScanOutcome::Scanned(scan) => {
                if let Some(fault) = &scan.fault {
                    if !is_tail {
                        return Err(non_tail_corruption(
                            seg,
                            &format!("{} at byte {}", fault.reason, fault.offset),
                        ));
                    }
                    let q = quarantine_tail(&seg.path, fault.offset, &fault.reason)?;
                    notify_quarantine(&q, scan.last_tx.or(prev_tx));
                    report.quarantine = Some(q);
                }
                prev_tx = scan.last_tx.or(prev_tx);
                min_epoch = scan.max_epoch;
                report.last_tx_id = prev_tx;
                report.epoch = report.epoch.max(scan.max_epoch);
                tail = Some((seg.path.clone(), scan.valid_len));
            }
        }
    }
    Ok((report, tail))
}

/// STG-031 operator notification: structured `tracing` output with the
/// quarantined byte range and sidecar path.
fn notify_quarantine(q: &QuarantineReport, last_recovered_tx: Option<u64>) {
    tracing::warn!(
        segment = %q.segment.display(),
        from_offset = q.from_offset,
        quarantined_bytes = q.bytes,
        sidecar = %q.sidecar.display(),
        reason = %q.reason,
        last_recovered_tx_id = ?last_recovered_tx,
        "commit-log tail quarantined; appends resume at the last valid boundary (STG-031)"
    );
}

/// Writer↔syncer shared state: the durable watch channel and the cursor
/// naming what is written vs what is fsynced.
struct SyncShared {
    /// Durability watermark channel (STG-012). Advanced monotonically by
    /// whichever thread finished an fsync last; `Failed` sticks.
    watch: watch::Sender<DurableState>,
    cursor: std::sync::Mutex<SyncCursor>,
    cv: std::sync::Condvar,
    fsyncs: Arc<AtomicU64>,
}

struct SyncCursor {
    /// The active segment's handle as of the last written batch — what an
    /// fsync targets. Entries in earlier segments were sealed durably at
    /// rotation, so syncing the active segment is always sufficient.
    handle: Option<Arc<std::fs::File>>,
    /// Highest tx id whose bytes reached the OS (the sync target).
    written: Option<u64>,
    /// Highest tx id known fsynced.
    synced: Option<u64>,
    /// The writer is done (or failed); finish and exit.
    shutdown: bool,
}

impl SyncShared {
    fn lock(&self) -> std::sync::MutexGuard<'_, SyncCursor> {
        self.cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Advance the durable watermark; never regress, and `Failed` sticks.
    fn publish_durable(&self, mark: Option<u64>) {
        self.watch.send_modify(|state| {
            if let DurableState::Durable(current) = state
                && mark > *current
            {
                *state = DurableState::Durable(mark);
            }
        });
    }

    fn fail(&self, message: String) {
        let _ = self.watch.send(DurableState::Failed(Arc::from(message)));
    }

    /// The syncer thread: fsync behind the writer, group-commit style — one
    /// fsync covers every batch that landed while the previous one ran.
    fn run(self: &Arc<Self>) {
        loop {
            let (handle, target) = {
                let mut cursor = self.lock();
                loop {
                    if cursor.written > cursor.synced
                        && let Some(handle) = cursor.handle.as_ref()
                    {
                        break (Arc::clone(handle), cursor.written);
                    }
                    if cursor.shutdown {
                        return;
                    }
                    cursor = self
                        .cv
                        .wait(cursor)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            };
            match handle.sync_data() {
                Ok(()) => {
                    self.fsyncs.fetch_add(1, Ordering::SeqCst);
                    {
                        let mut cursor = self.lock();
                        if target > cursor.synced {
                            cursor.synced = target;
                        }
                    }
                    self.publish_durable(target);
                }
                Err(e) => {
                    // A failed fsync leaves the on-disk state undefined —
                    // no retry (STG-012). The writer sees Failed on the
                    // watch and stops accepting.
                    self.fail(format!("commit-log fsync failed: {e}"));
                    self.lock().shutdown = true;
                    return;
                }
            }
        }
    }
}

/// The flush actor (STG-012), two OS threads so blocking file I/O never
/// touches an async runtime:
/// - the **writer** (`run`): drains the command queue, appends frames with
///   `write`, and publishes the TXN-004 `written` watermark the moment the
///   OS holds the bytes — the reducer-ack path gates here and never waits
///   for a disk flush;
/// - the **syncer** ([`SyncShared::run`]): fsyncs the active segment behind
///   the writer and publishes the durable watermark. Group commit falls out
///   naturally: every batch written while one fsync runs is covered by the
///   next, so fsync count stays far below tx count (STG-012) *and* the
///   writer never stalls behind the disk.
///
/// Rotation, epoch bumps, and shutdown still sync inline on the writer:
/// they are rare, and their correctness depends on "old bytes durable
/// before new bytes exist".
struct Actor {
    dir: PathBuf,
    shard_id: u32,
    options: CommitLogOptions,
    epoch: u64,
    current: Option<SegmentFile>,
    /// Write buffer: frames accumulate here and hit the file at flush time.
    buf: Vec<u8>,
    last_written: Option<u64>,
    /// The TXN-004 ack watermark: advanced the moment `write` hands the
    /// batch to the OS, before the fsync — so acks never wait on the disk.
    written_watch: watch::Sender<DurableState>,
    /// Shared with the syncer thread.
    sync: Arc<SyncShared>,
    /// Clone of the active segment's handle, refreshed on rotation.
    sync_handle: Option<Arc<std::fs::File>>,
    /// The syncer thread, joined before the writer exits so `close()`
    /// joining the writer means both are done.
    syncer: Option<std::thread::JoinHandle<()>>,
}

impl Actor {
    fn run(mut self, mut rx: mpsc::Receiver<Cmd>) {
        let mut batch = Vec::new();
        while let Some(first) = rx.blocking_recv() {
            // A dead syncer (failed fsync) poisons the log: stop accepting.
            if matches!(&*self.sync.watch.borrow(), DurableState::Failed(_)) {
                rx.close();
                break;
            }
            batch.push(first);
            while batch.len() < self.options.max_batch {
                match rx.try_recv() {
                    Ok(cmd) => batch.push(cmd),
                    Err(_) => break,
                }
            }
            if let Err(e) = self.process(&mut batch) {
                tracing::error!(
                    shard_id = self.shard_id,
                    error = %e,
                    "commit-log writer failed; log state on disk is undefined after a \
                     failed write/fsync — stopping (STG-012)"
                );
                let message = e.to_string();
                let _ = self
                    .written_watch
                    .send(DurableState::Failed(Arc::from(message.clone())));
                self.sync.fail(message);
                rx.close();
                break;
            }
        }
        // Queue closed: everything received is written AND fsynced before
        // the writer retires (close() joins this thread and relies on it).
        if let Err(e) = self.flush_sync() {
            let message = e.to_string();
            let _ = self
                .written_watch
                .send(DurableState::Failed(Arc::from(message.clone())));
            self.sync.fail(message);
        }
        {
            let mut cursor = self.sync.lock();
            cursor.shutdown = true;
        }
        self.sync.cv.notify_one();
        if let Some(syncer) = self.syncer.take() {
            let _ = syncer.join();
        }
    }

    /// Append every command in the batch, write the frames to the OS,
    /// publish the `written` watermark, and hand the fsync to the syncer.
    fn process(&mut self, batch: &mut Vec<Cmd>) -> Result<()> {
        let mut wrote = false;
        for cmd in batch.drain(..) {
            match cmd {
                Cmd::Append(record) => {
                    self.write_record(&record)?;
                    wrote = true;
                }
                Cmd::SetEpoch { epoch, ack } => {
                    // Flush pending data durably under the old epoch first.
                    if wrote {
                        self.flush_sync()?;
                        wrote = false;
                    }
                    let result = if epoch < self.epoch {
                        Err(FluxumError::Storage(format!(
                            "epoch {epoch} rejected: current epoch is {} (STG-011)",
                            self.epoch
                        )))
                    } else {
                        self.epoch = epoch;
                        Ok(())
                    };
                    let _ = ack.send(result);
                }
            }
        }
        if wrote {
            self.flush_written()?;
            // Hand the batch to the syncer and keep draining.
            {
                let mut cursor = self.sync.lock();
                cursor.handle = self.sync_handle.clone();
                cursor.written = self.last_written;
            }
            self.sync.cv.notify_one();
        }
        Ok(())
    }

    fn write_record(&mut self, record: &TxRecord) -> Result<()> {
        let body = record.encode()?;
        let frame = encode_entry(self.epoch, &body).map_err(FluxumError::Storage)?;
        let needs_rotation = match &self.current {
            None => true,
            Some(seg) => seg.len >= self.options.segment_max_bytes,
        };
        if needs_rotation {
            // Seal the old segment durably before the new one exists
            // (rotation, STG-013).
            if self.current.is_some() {
                self.flush_sync()?;
            }
            let seg = create_segment(&self.dir, self.shard_id, record.tx_id, self.epoch)?;
            self.sync_handle = Some(Arc::new(seg.file.try_clone().map_err(FluxumError::Io)?));
            self.current = Some(seg);
        }
        let Some(seg) = self.current.as_mut() else {
            return Err(FluxumError::Storage(
                "internal invariant violated: no active segment after rotation".into(),
            ));
        };
        self.buf.extend_from_slice(&frame);
        seg.len += frame.len() as u64;
        self.last_written = Some(record.tx_id);
        Ok(())
    }

    /// Write buffered frames to the active segment and publish the TXN-004
    /// `written` watermark — no fsync here; that is the syncer's job.
    fn flush_written(&mut self) -> Result<()> {
        let Some(seg) = self.current.as_mut() else {
            return Ok(());
        };
        if !self.buf.is_empty() {
            use std::io::Write as _;
            seg.file.write_all(&self.buf)?;
            self.buf.clear();
            // The bytes are the OS's now: a process crash can no longer
            // lose them, so acks gated on `wait_written` may proceed while
            // the fsync happens behind this thread.
            let _ = self
                .written_watch
                .send(DurableState::Durable(self.last_written));
        }
        Ok(())
    }

    /// Write AND fsync inline — the rotation/epoch/shutdown path, where the
    /// caller needs "durable now" before proceeding.
    fn flush_sync(&mut self) -> Result<()> {
        self.flush_written()?;
        let Some(seg) = self.current.as_mut() else {
            return Ok(());
        };
        seg.file.sync_data()?;
        self.sync.fsyncs.fetch_add(1, Ordering::SeqCst);
        {
            let mut cursor = self.sync.lock();
            if self.last_written > cursor.synced {
                cursor.synced = self.last_written;
            }
        }
        self.sync.publish_durable(self.last_written);
        Ok(())
    }
}
