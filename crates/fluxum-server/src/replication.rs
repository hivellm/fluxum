//! Replication log streaming (SPEC-014 §3/§4; T7.1; FR-100): the commit log
//! IS the replication protocol — the primary streams raw STG-011 entry
//! frames read from its own durable segments, replicas apply them through
//! [`fluxum_core::store::MemStore::apply_replica_record`] and append them to
//! their own logs byte-identically (REP-010).
//!
//! Topology per shard: one primary (accepts `ReplicaHello` sessions over
//! the ordinary TCP transport, after server-peer auth — REP-005/REP-011)
//! and N replicas (each runs a [`spawn_replica`] client task that dials the
//! primary, syncs, applies, and acknowledges).
//!
//! Sync decision (REP-011): a replica whose `last_applied_tx_id + 1` is
//! still covered by the primary's retained segments partial-syncs from its
//! offset; an empty or too-far-behind replica full-syncs — the primary
//! packs its newest checkpoint (the T7.3 `checkpoint.pack`) and streams it
//! in chunks, then the log tail (REP-012). Election/consensus is T7.2; the
//! epoch here is persisted, carried on every envelope, and fenced
//! mechanically (REP-004/REP-031): a member that sees a higher epoch adopts
//! it, a replica rejects stale-epoch batches, and a primary that observes a
//! higher epoch on ANY channel (hello or ack) stops acknowledging writes —
//! [`ReplicationPrimary::visibility_barrier`] refuses once fenced. The
//! demote-to-replica + diverged-suffix truncation flow needs the T7.2
//! election machinery (only a deposed primary can diverge); until then a
//! replica offering a future offset is refused loudly.
//!
//! Acknowledgment modes (REP-020/021/022): `async` acks at local commit;
//! `semi_sync` withholds every client-visible acknowledgment — the
//! `ReducerResult`, the admin `committed` reply, and the `TxUpdate`
//! fan-out — behind [`ReplicationPrimary::visibility_barrier`] until a
//! quorum of members (counting the primary) holds the entry durably.
//! On `ack_timeout_ms` without quorum: `block` refuses the ack with a
//! retryable `CLUSTER_SHARD_UNAVAILABLE`; `degrade` acks anyway and raises
//! `fluxum_replication_degraded` until quorum returns.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};

use fluxum_core::FluxumError;
use fluxum_core::commitlog::{self, TxRecord};
use fluxum_core::metrics::Metrics;
use fluxum_core::txn::CommitMeta;
use fluxum_protocol::{
    ClientMessage, Frame, FrameCodec, PrimaryHello, ReplAck, ReplBatch, ReplCheckpoint,
    ReplHeartbeat, ReplicaHello, ServerMessage, codes,
};

use crate::{OutFrame, ShardContext};

/// The persisted-epoch marker file, next to the commit log (REP-004).
pub const EPOCH_FILE: &str = "replication.epoch";

/// Checkpoint-transfer chunk size (REP-012): bounded frames, well under the
/// RPC-061 limit.
const CHECKPOINT_CHUNK_BYTES: usize = 256 * 1024;

/// Per-batch frame budget for the streamer: bounded memory per send.
const BATCH_BUDGET_BYTES: usize = 1024 * 1024;

/// Load the persisted epoch (REP-004); 1 when no marker exists yet.
///
/// # Errors
/// An unreadable or undecodable marker.
pub fn load_epoch(log_dir: &Path) -> Result<u64, FluxumError> {
    let path = log_dir.join(EPOCH_FILE);
    if !path.exists() {
        return Ok(1);
    }
    let bytes = std::fs::read(&path)?;
    rmp_serde::from_slice(&bytes)
        .map_err(|e| FluxumError::Storage(format!("replication.epoch decode failed: {e}")))
}

/// Durably persist `epoch` (REP-004: a member must persist before acting
/// under an epoch).
///
/// # Errors
/// I/O failures.
pub fn persist_epoch(log_dir: &Path, epoch: u64) -> Result<(), FluxumError> {
    std::fs::create_dir_all(log_dir)?;
    let path = log_dir.join(EPOCH_FILE);
    let bytes = rmp_serde::to_vec(&epoch)
        .map_err(|e| FluxumError::Storage(format!("replication.epoch encode failed: {e}")))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    std::io::Write::write_all(&mut file, &bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Runtime knobs the primary side uses (from `replication.*` config).
#[derive(Debug, Clone)]
pub struct PrimaryOptions {
    /// `replication.heartbeat_interval_ms` (REP-016).
    pub heartbeat_interval: Duration,
    /// `replication.window_bytes` (REP-017).
    pub window_bytes: u64,
    /// The REP-021 semi-sync barrier; `None` = `async` mode (REP-020).
    /// Populated only on the primary role — a replica's local fan-out is
    /// not quorum-gated in T7.1 (it needs the T7.2 consensus watermark).
    pub semi_sync: Option<SemiSyncRuntime>,
}

impl Default for PrimaryOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            window_bytes: 8 << 20,
            semi_sync: None,
        }
    }
}

/// The resolved `replication.semi_sync` block (REP-021/REP-022).
#[derive(Debug, Clone)]
pub struct SemiSyncRuntime {
    /// Members that must hold the entry durably, INCLUDING the primary.
    pub quorum_total: usize,
    /// `semi_sync.ack_timeout_ms` (REP-022).
    pub ack_timeout: Duration,
    /// `on_quorum_loss: degrade` (true) vs `block` (false).
    pub degrade: bool,
}

/// Resolve `semi_sync.quorum` against the member count (REP-021):
/// `majority` = ⌈(members + 1) / 2⌉; an explicit count is used as-is
/// (config validation bounds it to `1..=members`).
pub fn quorum_total(quorum: &str, members: usize) -> usize {
    match quorum.parse::<usize>() {
        Ok(count) => count,
        Err(_) => members / 2 + 1,
    }
}

/// REP-031: a batch whose envelope epoch is older than the highest epoch
/// this member has persisted must be rejected, never applied.
///
/// # Errors
/// The stale-epoch rejection itself.
fn admit_batch_epoch(batch_epoch: u64, persisted: u64) -> Result<(), FluxumError> {
    if batch_epoch < persisted {
        return Err(FluxumError::Storage(format!(
            "stale-epoch batch rejected: {batch_epoch} < persisted {persisted} (REP-031)"
        )));
    }
    Ok(())
}

/// One live replica session's shared state (acks + flow control).
#[derive(Debug)]
struct ReplicaSession {
    /// Highest tx id the replica has APPLIED (REP-017 acks).
    applied: AtomicU64,
    /// Highest tx id the replica has DURABLY appended (REP-021 quorum).
    durable: AtomicU64,
    /// Wakes the streamer when an ack arrives (window release).
    ack_notify: Notify,
}

/// The primary's replication service: accepts sessions handed over by the
/// TCP transport and streams the log to each (REP-011..REP-017).
pub struct ReplicationPrimary {
    shard_id: u32,
    log_dir: PathBuf,
    checkpoint_dir: PathBuf,
    epoch: AtomicU64,
    options: PrimaryOptions,
    sessions: std::sync::Mutex<std::collections::HashMap<String, Arc<ReplicaSession>>>,
    /// REP-031: a higher epoch was observed on some channel — this member
    /// is a deposed primary and stops acknowledging writes. Cleared only
    /// by the T7.2 demote/re-election flow (i.e. never, in T7.1).
    fenced: AtomicBool,
    /// REP-022 `degrade`: quorum currently lost (drives the gauge edge).
    degraded: AtomicBool,
    /// Wakes barrier waiters when any replica ack lands (REP-021).
    quorum_notify: Notify,
}

impl std::fmt::Debug for ReplicationPrimary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationPrimary")
            .field("shard_id", &self.shard_id)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ReplicationPrimary {
    /// Build the service; `epoch` is the persisted REP-004 epoch.
    pub fn new(
        shard_id: u32,
        log_dir: PathBuf,
        checkpoint_dir: PathBuf,
        epoch: u64,
        options: PrimaryOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            log_dir,
            checkpoint_dir,
            epoch: AtomicU64::new(epoch),
            options,
            sessions: std::sync::Mutex::new(std::collections::HashMap::new()),
            fenced: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            quorum_notify: Notify::new(),
        })
    }

    /// The current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// REP-031: whether a higher epoch was observed — a fenced member no
    /// longer acknowledges writes (the barrier refuses).
    pub fn fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    /// REP-031: a higher epoch reached us via `channel` — stop
    /// acknowledging writes. Demote + resync is the T7.2 election flow.
    fn fence(&self, metrics: &Metrics, theirs: u64, channel: &str) {
        metrics.note_replication_fenced();
        if !self.fenced.swap(true, Ordering::SeqCst) {
            tracing::warn!(target: "fluxum::repl",
                ours = self.epoch(), theirs, channel,
                "higher epoch observed; this primary is stale and stops \
                 acknowledging writes (REP-031)");
        }
        // Wake barrier waiters so blocked acks fail fast, not on timeout.
        self.quorum_notify.notify_waiters();
    }

    /// Record a replica's acknowledgment (REP-017; quorum input for
    /// REP-021). Unknown members are ignored (a late ack after drop). An
    /// ack carrying a HIGHER epoch fences this primary (REP-031: any
    /// channel counts).
    pub fn ack(&self, metrics: &Metrics, member: &str, ack: &ReplAck) {
        if ack.epoch > self.epoch() {
            self.fence(metrics, ack.epoch, "ack");
        }
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = sessions.get(member) {
            session
                .applied
                .fetch_max(ack.applied_tx_id, Ordering::SeqCst);
            session
                .durable
                .fetch_max(ack.durable_tx_id, Ordering::SeqCst);
            session.ack_notify.notify_waiters();
            self.quorum_notify.notify_waiters();
        }
    }

    /// The REP-021 visibility barrier: hold a client-visible acknowledgment
    /// of `tx_id` (ReducerResult, admin reply, TxUpdate fan-out) until the
    /// quorum holds it durably. `async` mode returns immediately; a fenced
    /// member refuses (REP-031).
    ///
    /// # Errors
    /// `CLUSTER_SHARD_UNAVAILABLE` (retryable) when fenced, or when the
    /// quorum is not reached within `ack_timeout_ms` under
    /// `on_quorum_loss: block` (REP-022).
    pub async fn visibility_barrier(
        &self,
        metrics: &Metrics,
        tx_id: u64,
    ) -> Result<(), FluxumError> {
        let fenced_err = || {
            FluxumError::query_retryable(
                codes::CLUSTER_SHARD_UNAVAILABLE,
                "fenced: a higher epoch exists — this member no longer \
                 acknowledges writes (REP-031)",
                Some(1_000),
            )
        };
        if self.fenced() {
            return Err(fenced_err());
        }
        let Some(semi) = &self.options.semi_sync else {
            return Ok(()); // REP-020: async acks at local commit.
        };
        let needed_replicas = semi.quorum_total.saturating_sub(1);
        if needed_replicas == 0 {
            return Ok(());
        }
        let started = tokio::time::Instant::now();
        let deadline = started + semi.ack_timeout;
        loop {
            // Register for the wake-up BEFORE counting, so an ack landing
            // between the count and the await is never missed.
            let notified = self.quorum_notify.notified();
            let have = self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .filter(|s| s.durable.load(Ordering::SeqCst) >= tx_id)
                .count();
            if have >= needed_replicas {
                metrics.note_semi_sync_wait(elapsed_us(started));
                if self.degraded.swap(false, Ordering::SeqCst) {
                    metrics.set_replication_degraded(false);
                    tracing::info!(target: "fluxum::repl",
                        "semi-sync quorum restored; zero-loss guarantee back (REP-022)");
                }
                return Ok(());
            }
            if self.fenced() {
                return Err(fenced_err());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                if semi.degrade {
                    if !self.degraded.swap(true, Ordering::SeqCst) {
                        metrics.set_replication_degraded(true);
                        tracing::warn!(target: "fluxum::repl", tx_id,
                            "semi-sync quorum lost; DEGRADED to async — the \
                             zero-loss guarantee is suspended (REP-022)");
                    }
                    metrics.note_semi_sync_wait(elapsed_us(started));
                    return Ok(());
                }
                return Err(FluxumError::query_retryable(
                    codes::CLUSTER_SHARD_UNAVAILABLE,
                    format!(
                        "semi-sync quorum of {} members (incl. this primary) \
                         not reached within {} ms for tx {tx_id} \
                         (REP-022 on_quorum_loss: block)",
                        semi.quorum_total,
                        semi.ack_timeout.as_millis(),
                    ),
                    Some(u32::try_from(semi.ack_timeout.as_millis()).unwrap_or(1_000)),
                ));
            }
        }
    }

    /// The highest tx id each connected replica has durably appended —
    /// the REP-021 quorum inputs (the primary itself counts separately).
    pub fn durable_offsets(&self) -> Vec<(String, u64)> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, s)| (name.clone(), s.durable.load(Ordering::SeqCst)))
            .collect()
    }

    /// Connected replica count (REP-081).
    pub fn connected(&self) -> usize {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Accept a replication session handed over by the TCP transport after
    /// server-peer auth (REP-011): decide full vs partial sync, answer
    /// `PrimaryHello`, and spawn the streamer over the connection's
    /// outbound queue. Returns `false` when the hello is refused (wrong
    /// shard, or the caller's epoch is AHEAD — this primary is stale).
    pub fn accept(
        self: &Arc<Self>,
        ctx: &Arc<ShardContext>,
        hello: &ReplicaHello,
        member: String,
        out: mpsc::Sender<OutFrame>,
        codec: FrameCodec,
    ) -> bool {
        if hello.shard_id != self.shard_id {
            tracing::warn!(target: "fluxum::repl", got = hello.shard_id, have = self.shard_id,
                "replica hello for another shard refused");
            return false;
        }
        let epoch = self.epoch();
        if hello.epoch > epoch {
            // REP-031: the caller has seen a newer epoch — WE are stale.
            // The barrier stops acknowledging; demote/step-down is T7.2.
            self.fence(ctx.metrics(), hello.epoch, "hello");
            let fence = ServerMessage::ReplFence(fluxum_protocol::ReplFence { epoch: hello.epoch });
            if let Ok(frame) = frame(&codec, &fence) {
                let _ = out.try_send(frame);
            }
            return false;
        }

        let first_available = commitlog::first_available_tx_id(&self.log_dir, self.shard_id)
            .ok()
            .flatten();
        let latest = ctx
            .engine
            .pipeline()
            .log()
            .durable_tx_id()
            .ok()
            .flatten()
            .unwrap_or(0);
        if hello.last_applied_tx_id > latest {
            // REP-013: a replica AHEAD of its primary holds a diverged
            // suffix — only a deposed primary can be (T7.2). Refuse loudly
            // rather than stream past a divergence; the truncate + rebuild
            // repair lands with the T7.2 demote flow.
            tracing::warn!(target: "fluxum::repl",
                theirs = hello.last_applied_tx_id, ours = latest,
                "replica offers a FUTURE offset (diverged suffix); session \
                 refused until the T7.2 demote/truncate flow (REP-013)");
            return false;
        }
        // REP-011: partial when the replica's next entry is still on disk.
        let next_needed = hello.last_applied_tx_id.saturating_add(1);
        let sync_full = match first_available {
            Some(first) => hello.last_applied_tx_id == 0 || next_needed < first,
            // No segments at all: nothing to stream yet — partial (live tail).
            None => hello.last_applied_tx_id == 0 && latest > 0,
        };

        let session = Arc::new(ReplicaSession {
            applied: AtomicU64::new(hello.last_applied_tx_id),
            durable: AtomicU64::new(hello.last_applied_tx_id),
            ack_notify: Notify::new(),
        });
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(member.clone(), Arc::clone(&session));
        ctx.metrics()
            .set_replication_connected(self.connected() as u64);

        let this = Arc::clone(self);
        let ctx = Arc::clone(ctx);
        let replica_applied = hello.last_applied_tx_id;
        tokio::spawn(async move {
            let result = this
                .stream_session(&ctx, sync_full, replica_applied, &session, &out, &codec)
                .await;
            if let Err(e) = result {
                tracing::warn!(target: "fluxum::repl", member = %member, error = %e,
                    "replication session ended");
            }
            this.sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&member);
            ctx.metrics()
                .set_replication_connected(this.connected() as u64);
            ctx.metrics().remove_replication_peer(&member);
        });
        true
    }

    /// The streaming loop of one session: hello, optional checkpoint
    /// transfer (REP-012), then batches + heartbeats under flow control
    /// (REP-016/REP-017).
    async fn stream_session(
        &self,
        ctx: &Arc<ShardContext>,
        sync_full: bool,
        replica_applied: u64,
        session: &Arc<ReplicaSession>,
        out: &mpsc::Sender<OutFrame>,
        codec: &FrameCodec,
    ) -> Result<(), FluxumError> {
        let epoch = self.epoch();
        let log = ctx.engine.pipeline().log();
        let latest = log.durable_tx_id()?.unwrap_or(0);
        let first_available =
            commitlog::first_available_tx_id(&self.log_dir, self.shard_id)?.unwrap_or(1);

        // REP-012: pack + stream the checkpoint first on a full sync.
        let mut from_tx = replica_applied;
        let checkpoint = if sync_full {
            let dir = self.checkpoint_dir.clone();
            let shard = self.shard_id;
            tokio::task::spawn_blocking(move || {
                fluxum_core::backup::pack_latest_checkpoint(&dir, shard)
            })
            .await
            .map_err(|e| FluxumError::Storage(format!("checkpoint pack task: {e}")))??
        } else {
            None
        };
        if let Some((_, last_tx)) = &checkpoint {
            from_tx = *last_tx;
        }

        let hello = ServerMessage::PrimaryHello(PrimaryHello {
            shard_id: self.shard_id,
            epoch,
            first_available_tx_id: first_available,
            latest_tx_id: latest,
            sync_full: checkpoint.is_some(),
            from_tx_id: from_tx.saturating_add(1),
        });
        send(out, frame(codec, &hello)?).await?;

        if let Some((pack, last_tx)) = checkpoint {
            for (i, chunk) in pack.chunks(CHECKPOINT_CHUNK_BYTES).enumerate() {
                let done = (i + 1) * CHECKPOINT_CHUNK_BYTES >= pack.len();
                let msg = ServerMessage::ReplCheckpoint(ReplCheckpoint {
                    last_tx_id: last_tx,
                    chunk: chunk.to_vec(),
                    done,
                });
                send(out, frame(codec, &msg)?).await?;
            }
        }

        // Continuous streaming (REP-011 step 4): follow the durable log.
        let mut durable_watch = log.subscribe_durable();
        let mut heartbeat = tokio::time::interval(self.options.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_sent = from_tx;
        // REP-017 window accounting: bytes sent but not yet applied.
        let mut in_flight: std::collections::VecDeque<(u64, usize)> = Default::default();
        loop {
            // Release the window as acks land.
            let acked = session.applied.load(Ordering::SeqCst);
            while in_flight.front().is_some_and(|(tx, _)| *tx <= acked) {
                in_flight.pop_front();
            }
            let in_flight_bytes: usize = in_flight.iter().map(|(_, b)| *b).sum();
            if in_flight_bytes as u64 >= self.options.window_bytes {
                // Window full: wait for an ack (or heartbeat to keep alive).
                tokio::select! {
                    () = session.ack_notify.notified() => continue,
                    _ = heartbeat.tick() => {
                        let hb = ServerMessage::ReplHeartbeat(ReplHeartbeat {
                            epoch: self.epoch(),
                            latest_tx_id: log.durable_tx_id()?.unwrap_or(0),
                        });
                        send(out, frame(codec, &hb)?).await?;
                        continue;
                    }
                }
            }

            let durable = log.durable_tx_id()?.unwrap_or(0);
            if durable > last_sent {
                let dir = self.log_dir.clone();
                let shard = self.shard_id;
                let after = last_sent;
                let (frames, new_last) = tokio::task::spawn_blocking(move || {
                    commitlog::read_frames_after(&dir, shard, after, BATCH_BUDGET_BYTES)
                })
                .await
                .map_err(|e| FluxumError::Storage(format!("stream read task: {e}")))??;
                if !frames.is_empty() {
                    let bytes: usize = frames.iter().map(Vec::len).sum();
                    let batch = ServerMessage::ReplBatch(ReplBatch {
                        epoch: self.epoch(),
                        frames: frames.into_iter().map(serde_bytes::ByteBuf::from).collect(),
                    });
                    send(out, frame(codec, &batch)?).await?;
                    in_flight.push_back((new_last, bytes));
                    last_sent = new_last;
                    // REP-081: per-peer offset/lag from the primary's view.
                    ctx.metrics().set_replication_peer(
                        "session",
                        session.applied.load(Ordering::SeqCst),
                        durable.saturating_sub(session.applied.load(Ordering::SeqCst)),
                    );
                    continue;
                }
            }

            tokio::select! {
                changed = durable_watch.changed() => {
                    if changed.is_err() {
                        return Ok(()); // log closed — shutdown
                    }
                }
                _ = heartbeat.tick() => {
                    let hb = ServerMessage::ReplHeartbeat(ReplHeartbeat {
                        epoch: self.epoch(),
                        latest_tx_id: log.durable_tx_id()?.unwrap_or(0),
                    });
                    send(out, frame(codec, &hb)?).await?;
                }
            }
        }
    }
}

/// Elapsed µs since `started`, saturating.
fn elapsed_us(started: tokio::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn frame(codec: &FrameCodec, message: &ServerMessage) -> Result<OutFrame, FluxumError> {
    let body = message
        .encode()
        .map_err(|e| FluxumError::Storage(format!("replication frame encode: {e}")))?;
    let framed = codec
        .encode(&body)
        .map_err(|e| FluxumError::Storage(format!("replication frame too large: {e}")))?;
    Ok(OutFrame::now(Arc::new(framed)))
}

async fn send(out: &mpsc::Sender<OutFrame>, frame: OutFrame) -> Result<(), FluxumError> {
    out.send(frame)
        .await
        .map_err(|_| FluxumError::Storage("replication session connection closed".into()))
}

// --- the replica client (REP-011..REP-014) ----------------------------------------

/// Configuration for one replica client task.
#[derive(Debug, Clone)]
pub struct ReplicaOptions {
    /// The primary's `host:port` (FluxRPC TCP).
    pub primary: String,
    /// This member's name (REP-005).
    pub member_name: String,
    /// The server-peer token to authenticate with.
    pub token: Vec<u8>,
    /// The replica's own log directory (durability + epoch marker).
    pub log_dir: PathBuf,
    /// The replica's checkpoint directory (full-sync install target).
    pub checkpoint_dir: PathBuf,
    /// `replication.ack_interval_ms` (REP-017).
    pub ack_interval: Duration,
}

/// A running replica client; dropping it does NOT stop the task — call
/// [`ReplicaHandle::stop`].
#[derive(Debug, Clone)]
pub struct ReplicaHandle {
    stop: Arc<Notify>,
}

impl ReplicaHandle {
    /// Stop the client loop (the in-flight session ends at its next await).
    pub fn stop(&self) {
        self.stop.notify_waiters();
    }
}

/// Spawn the replica client: dial, sync, apply, acknowledge — reconnecting
/// with backoff until stopped (the T7.2 election machinery replaces the
/// fixed primary endpoint with discovery).
pub fn spawn_replica(ctx: Arc<ShardContext>, options: ReplicaOptions) -> ReplicaHandle {
    let stop = Arc::new(Notify::new());
    let handle = ReplicaHandle {
        stop: Arc::clone(&stop),
    };
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            tokio::select! {
                () = stop.notified() => return,
                result = replica_session(&ctx, &options) => match result {
                    Ok(()) => backoff = Duration::from_millis(200),
                    Err(e) => {
                        tracing::warn!(target: "fluxum::repl", error = %e,
                            "replica session ended; reconnecting");
                    }
                }
            }
            tokio::select! {
                () = stop.notified() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    });
    handle
}

/// One replica session: authenticate, hello, sync, apply until the
/// connection drops.
async fn replica_session(
    ctx: &Arc<ShardContext>,
    options: &ReplicaOptions,
) -> Result<(), FluxumError> {
    let io_err = |e: std::io::Error| FluxumError::Storage(format!("replication io: {e}"));
    let mut stream = tokio::net::TcpStream::connect(&options.primary)
        .await
        .map_err(io_err)?;
    let codec = FrameCodec::default();

    let auth = ClientMessage::Authenticate(fluxum_protocol::Authenticate {
        id: 1,
        token: options.token.clone(),
        compression: None,
        tx_updates: None,
        namespace: None,
    });
    write_message(
        &mut stream,
        &codec,
        &auth
            .encode()
            .map_err(|e| FluxumError::Storage(format!("replication envelope encode: {e}")))?,
    )
    .await?;

    let epoch = load_epoch(&options.log_dir)?;
    let log = ctx.engine.pipeline().log();
    let last_applied = log.durable_tx_id()?.unwrap_or(0);

    let mut reader = FrameReader::new(codec);
    // Expect AuthResult, then answer with the hello.
    let first = reader.next_message(&mut stream).await?;
    match first {
        ServerMessage::AuthResult(_) => {}
        ServerMessage::Error(e) => {
            return Err(FluxumError::Storage(format!(
                "replication auth refused: {}",
                e.message
            )));
        }
        other => {
            return Err(FluxumError::Storage(format!(
                "unexpected pre-hello message: {other:?}"
            )));
        }
    }
    let hello = ClientMessage::ReplicaHello(ReplicaHello {
        shard_id: ctx.shard_id,
        member_name: options.member_name.clone(),
        epoch,
        last_applied_tx_id: last_applied,
    });
    write_message(
        &mut stream,
        &codec,
        &hello
            .encode()
            .map_err(|e| FluxumError::Storage(format!("replication envelope encode: {e}")))?,
    )
    .await?;

    let mut applied = last_applied;
    let mut checkpoint_buf: Option<(u64, Vec<u8>)> = None;
    let mut ack_tick = tokio::time::interval(options.ack_interval);
    ack_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let message = tokio::select! {
            message = reader.next_message(&mut stream) => message?,
            _ = ack_tick.tick() => {
                let ack = ClientMessage::ReplAck(ReplAck {
                    epoch: load_epoch(&options.log_dir)?,
                    applied_tx_id: applied,
                    durable_tx_id: log.durable_tx_id()?.unwrap_or(0),
                });
                write_message(&mut stream, &codec, &ack.encode().map_err(|e| FluxumError::Storage(format!("replication envelope encode: {e}")))?).await?;
                continue;
            }
        };
        match message {
            ServerMessage::PrimaryHello(hello) => {
                if hello.epoch > epoch {
                    persist_epoch(&options.log_dir, hello.epoch)?;
                }
                tracing::info!(target: "fluxum::repl",
                    epoch = hello.epoch, full = hello.sync_full, from = hello.from_tx_id,
                    "replication session established (REP-011)");
            }
            ServerMessage::ReplCheckpoint(chunk) => {
                let buf = checkpoint_buf.get_or_insert((chunk.last_tx_id, Vec::new()));
                buf.1.extend_from_slice(&chunk.chunk);
                if chunk.done {
                    let (last_tx, pack) = checkpoint_buf.take().unwrap_or((0, Vec::new()));
                    let store = Arc::clone(ctx.store());
                    let ckpt_dir = options.checkpoint_dir.clone();
                    let log_dir = options.log_dir.clone();
                    let shard = ctx.shard_id;
                    let installed = tokio::task::spawn_blocking(move || {
                        fluxum_core::backup::install_checkpoint_pack(
                            &store, &ckpt_dir, &log_dir, shard, &pack,
                        )
                    })
                    .await
                    .map_err(|e| FluxumError::Storage(format!("install task: {e}")))??;
                    applied = installed;
                    tracing::info!(target: "fluxum::repl", last_tx_id = last_tx,
                        "full-sync checkpoint installed (REP-012)");
                }
            }
            ServerMessage::ReplBatch(batch) => {
                // REP-031: a batch from a deposed primary (older epoch than
                // this member has persisted) ends the session; the reconnect
                // hello then carries the higher epoch and fences the sender.
                admit_batch_epoch(batch.epoch, load_epoch(&options.log_dir)?)?;
                for raw in &batch.frames {
                    let (envelope_epoch, record) = commitlog::decode_entry_frame(raw)?;
                    if envelope_epoch > log.epoch() {
                        log.set_epoch(envelope_epoch).await?;
                        persist_epoch(&options.log_dir, envelope_epoch)?;
                    }
                    applied = apply_one(ctx, log, &record).await?;
                }
                ctx.metrics().set_replication_peer(
                    "primary", applied, 0, // lag is measured against heartbeats below
                );
            }
            ServerMessage::ReplHeartbeat(hb) => {
                if hb.epoch > load_epoch(&options.log_dir)? {
                    persist_epoch(&options.log_dir, hb.epoch)?;
                }
                ctx.metrics().set_replication_peer(
                    "primary",
                    applied,
                    hb.latest_tx_id.saturating_sub(applied),
                );
            }
            ServerMessage::ReplFence(fence) => {
                persist_epoch(&options.log_dir, fence.epoch)?;
                return Err(FluxumError::Storage(format!(
                    "fenced: a higher epoch {} exists (REP-031); resyncing",
                    fence.epoch
                )));
            }
            ServerMessage::Error(e) => {
                return Err(FluxumError::Storage(format!(
                    "replication session error: {}",
                    e.message
                )));
            }
            other => {
                tracing::debug!(target: "fluxum::repl", ?other, "ignoring non-replication frame");
            }
        }
    }
}

/// REP-014 steps 3–5 for one record: apply to `CommittedState`, append to
/// the local log, and fan out to this replica's own subscribers.
async fn apply_one(
    ctx: &Arc<ShardContext>,
    log: &fluxum_core::commitlog::CommitLog,
    record: &TxRecord,
) -> Result<u64, FluxumError> {
    let diff = ctx.store().apply_replica_record(record)?;
    log.append(record.clone()).await?;
    let meta = CommitMeta {
        caller: record.caller_identity(),
        reducer_name: record.reducer_name.clone(),
    };
    // REP-043: identical TxUpdate content, emitted by the replica's own
    // fan-out over its own subscriptions.
    ctx.publish_commit_meta(diff, meta);
    Ok(record.tx_id)
}

// --- minimal framed-message client I/O --------------------------------------------

async fn write_message(
    stream: &mut tokio::net::TcpStream,
    codec: &FrameCodec,
    body: &[u8],
) -> Result<(), FluxumError> {
    let framed = codec
        .encode(body)
        .map_err(|e| FluxumError::Storage(format!("frame encode: {e}")))?;
    stream
        .write_all(&framed)
        .await
        .map_err(|e| FluxumError::Storage(format!("replication write: {e}")))
}

/// Buffered frame reader over the replica's client socket.
struct FrameReader {
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl FrameReader {
    fn new(codec: FrameCodec) -> Self {
        Self {
            codec,
            buf: Vec::new(),
        }
    }

    async fn next_message(
        &mut self,
        stream: &mut tokio::net::TcpStream,
    ) -> Result<ServerMessage, FluxumError> {
        loop {
            match self.codec.decode(&self.buf) {
                Ok(Some((Frame::Body(body), consumed))) => {
                    let message = ServerMessage::decode(body).map_err(|e| {
                        FluxumError::Storage(format!("replication message decode: {e}"))
                    })?;
                    self.buf.drain(..consumed);
                    return Ok(message);
                }
                Ok(Some((Frame::KeepAlive, consumed))) => {
                    self.buf.drain(..consumed);
                }
                Ok(None) => {
                    let mut chunk = [0u8; 16 * 1024];
                    let n = stream
                        .read(&mut chunk)
                        .await
                        .map_err(|e| FluxumError::Storage(format!("replication read: {e}")))?;
                    if n == 0 {
                        return Err(FluxumError::Storage("replication connection closed".into()));
                    }
                    self.buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(FluxumError::Storage(format!(
                        "replication frame decode: {e}"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_marker_round_trips_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        // No marker yet: epoch 1 (REP-004 starts at 1).
        assert_eq!(load_epoch(dir.path()).unwrap(), 1);
        persist_epoch(dir.path(), 5).unwrap();
        assert_eq!(load_epoch(dir.path()).unwrap(), 5);
        persist_epoch(dir.path(), 9).unwrap();
        assert_eq!(load_epoch(dir.path()).unwrap(), 9);
        // A corrupt marker is a loud error, never a silent epoch reset.
        std::fs::write(dir.path().join(EPOCH_FILE), b"\xc1garbage").unwrap();
        assert!(load_epoch(dir.path()).is_err());
    }

    #[test]
    fn primary_options_defaults_match_the_spec() {
        let options = PrimaryOptions::default();
        assert_eq!(options.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(options.window_bytes, 8 << 20);
    }

    #[test]
    fn debug_impls_never_leak_session_state() {
        let dir = tempfile::tempdir().unwrap();
        let primary = ReplicationPrimary::new(
            3,
            dir.path().join("log"),
            dir.path().join("ckpt"),
            4,
            PrimaryOptions::default(),
        );
        let rendered = format!("{primary:?}");
        assert!(rendered.contains("shard_id: 3"), "{rendered}");
        assert!(rendered.contains("epoch: 4"), "{rendered}");
        assert_eq!(primary.epoch(), 4);
        assert_eq!(primary.connected(), 0);
        // An ack for a member with no session is a no-op, never a panic.
        let metrics = Metrics::new(3);
        primary.ack(
            &metrics,
            "ghost",
            &ReplAck {
                epoch: 4,
                applied_tx_id: 1,
                durable_tx_id: 1,
            },
        );
        assert!(primary.durable_offsets().is_empty());
        assert!(!primary.fenced());
        // A ghost ack carrying a HIGHER epoch still fences (REP-031: any
        // channel counts) — the member no longer acknowledges writes.
        primary.ack(
            &metrics,
            "ghost",
            &ReplAck {
                epoch: 9,
                applied_tx_id: 1,
                durable_tx_id: 1,
            },
        );
        assert!(primary.fenced());
        assert_eq!(metrics.replication_fenced_total(), 1);
    }

    #[test]
    fn oversized_replication_frames_are_a_typed_error() {
        // A codec with a tiny limit refuses to frame the message — the
        // streamer surfaces it instead of sending a torn frame.
        let codec = FrameCodec::new(8);
        let hb = ServerMessage::ReplHeartbeat(ReplHeartbeat {
            epoch: 1,
            latest_tx_id: 42,
        });
        let err = frame(&codec, &hb).unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
    }

    #[tokio::test]
    async fn send_surfaces_a_closed_connection() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let out = OutFrame::now(Arc::new(vec![1, 2, 3]));
        let err = send(&tx, out).await.unwrap_err();
        assert!(err.to_string().contains("closed"), "{err}");
    }

    #[tokio::test]
    async fn the_frame_reader_skips_keepalives_and_rejects_garbage() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let codec = FrameCodec::default();
            // A keep-alive, then a real message, then EOF.
            let keepalive = codec.encode(&[]).unwrap();
            let hb = ServerMessage::ReplHeartbeat(ReplHeartbeat {
                epoch: 2,
                latest_tx_id: 7,
            });
            let body = codec.encode(&hb.encode().unwrap()).unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut sock, &keepalive)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut sock, &body)
                .await
                .unwrap();
        });
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut reader = FrameReader::new(FrameCodec::default());
        let message = reader.next_message(&mut stream).await.unwrap();
        assert!(matches!(message, ServerMessage::ReplHeartbeat(hb) if hb.latest_tx_id == 7));
        // After the server hangs up, the reader reports the close.
        server.await.unwrap();
        let err = reader.next_message(&mut stream).await.unwrap_err();
        assert!(err.to_string().contains("closed"), "{err}");

        // Garbage that decodes as a frame but not as a message is typed.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let codec = FrameCodec::default();
            let bad = codec.encode(b"\xc1 not an envelope").unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut sock, &bad)
                .await
                .unwrap();
        });
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut reader = FrameReader::new(FrameCodec::default());
        let err = reader.next_message(&mut stream).await.unwrap_err();
        assert!(err.to_string().contains("decode"), "{err}");
    }

    // --- the REP-021/022 visibility barrier ---------------------------------

    fn semi_primary(
        quorum_total: usize,
        ack_timeout_ms: u64,
        degrade: bool,
    ) -> Arc<ReplicationPrimary> {
        ReplicationPrimary::new(
            0,
            PathBuf::new(),
            PathBuf::new(),
            1,
            PrimaryOptions {
                semi_sync: Some(SemiSyncRuntime {
                    quorum_total,
                    ack_timeout: Duration::from_millis(ack_timeout_ms),
                    degrade,
                }),
                ..PrimaryOptions::default()
            },
        )
    }

    /// Register a live replica session at `durable` (what `accept` does
    /// after the hello).
    fn register(primary: &ReplicationPrimary, member: &str, durable: u64) {
        primary.sessions.lock().unwrap().insert(
            member.to_owned(),
            Arc::new(ReplicaSession {
                applied: AtomicU64::new(durable),
                durable: AtomicU64::new(durable),
                ack_notify: Notify::new(),
            }),
        );
    }

    #[test]
    fn quorum_math_matches_rep_021() {
        // majority = ⌈(members + 1) / 2⌉ — a strict majority.
        assert_eq!(quorum_total("majority", 1), 1);
        assert_eq!(quorum_total("majority", 2), 2);
        assert_eq!(quorum_total("majority", 3), 2);
        assert_eq!(quorum_total("majority", 4), 3);
        assert_eq!(quorum_total("majority", 5), 3);
        // An explicit count is used as-is (config validation bounds it).
        assert_eq!(quorum_total("2", 5), 2);
        assert_eq!(quorum_total("5", 5), 5);
    }

    #[test]
    fn stale_epoch_batches_are_rejected() {
        assert!(admit_batch_epoch(3, 3).is_ok());
        assert!(admit_batch_epoch(4, 3).is_ok());
        let err = admit_batch_epoch(2, 3).unwrap_err();
        assert!(err.to_string().contains("REP-031"), "{err}");
    }

    #[tokio::test]
    async fn the_barrier_is_a_no_op_in_async_mode_and_for_a_solo_quorum() {
        let metrics = Metrics::new(0);
        // REP-020: async acks at local commit — no wait, no sample.
        let plain = ReplicationPrimary::new(
            0,
            PathBuf::new(),
            PathBuf::new(),
            1,
            PrimaryOptions::default(),
        );
        plain.visibility_barrier(&metrics, 42).await.unwrap();
        // Quorum 1 = the primary alone (single-member set): no wait either.
        let solo = semi_primary(1, 10, false);
        solo.visibility_barrier(&metrics, 42).await.unwrap();
        assert_eq!(metrics.semi_sync_waits(), (0, 0));
    }

    #[tokio::test]
    async fn the_barrier_releases_when_the_quorum_ack_lands() {
        let metrics = Arc::new(Metrics::new(0));
        let primary = semi_primary(2, 5_000, false);
        register(&primary, "replica-1", 0);
        let p = Arc::clone(&primary);
        let m = Arc::clone(&metrics);
        let waiter = tokio::spawn(async move { p.visibility_barrier(&m, 3).await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiter.is_finished(), "the barrier must hold pre-quorum");
        primary.ack(
            &metrics,
            "replica-1",
            &ReplAck {
                epoch: 1,
                applied_tx_id: 3,
                durable_tx_id: 3,
            },
        );
        waiter.await.unwrap().unwrap();
        let (_, waits) = metrics.semi_sync_waits();
        assert_eq!(waits, 1, "one barrier wait recorded (REP-021)");
    }

    #[tokio::test]
    async fn quorum_loss_blocks_with_a_retryable_unavailable() {
        let metrics = Metrics::new(0);
        let primary = semi_primary(2, 40, false);
        register(&primary, "replica-1", 0); // never reaches tx 7
        let err = primary.visibility_barrier(&metrics, 7).await.unwrap_err();
        assert_eq!(err.to_wire().code, codes::CLUSTER_SHARD_UNAVAILABLE);
        assert!(err.to_string().contains("REP-022"), "{err}");
        assert!(!metrics.replication_degraded());
    }

    #[tokio::test]
    async fn quorum_loss_degrades_when_configured_and_recovers_on_ack() {
        let metrics = Metrics::new(0);
        let primary = semi_primary(2, 30, true);
        register(&primary, "replica-1", 0);
        // Degrade: the ack goes through anyway, with the gauge raised.
        primary.visibility_barrier(&metrics, 5).await.unwrap();
        assert!(metrics.replication_degraded(), "REP-022 degrade gauge");
        // The replica catches up — the next barrier passes AND clears it.
        primary.ack(
            &metrics,
            "replica-1",
            &ReplAck {
                epoch: 1,
                applied_tx_id: 5,
                durable_tx_id: 5,
            },
        );
        primary.visibility_barrier(&metrics, 5).await.unwrap();
        assert!(!metrics.replication_degraded(), "quorum restored");
        assert_eq!(metrics.semi_sync_waits().1, 2);
    }

    #[tokio::test]
    async fn a_fenced_member_refuses_every_ack_in_both_modes() {
        let metrics = Metrics::new(0);
        // Fencing arrives over the ACK channel (REP-031: any channel).
        let semi = semi_primary(1, 40, false);
        semi.ack(
            &metrics,
            "any",
            &ReplAck {
                epoch: 8,
                applied_tx_id: 0,
                durable_tx_id: 0,
            },
        );
        let err = semi.visibility_barrier(&metrics, 1).await.unwrap_err();
        assert_eq!(err.to_wire().code, codes::CLUSTER_SHARD_UNAVAILABLE);
        assert!(err.to_string().contains("REP-031"), "{err}");
        // An async-mode member stops acknowledging too.
        let plain = ReplicationPrimary::new(
            0,
            PathBuf::new(),
            PathBuf::new(),
            1,
            PrimaryOptions::default(),
        );
        plain.ack(
            &metrics,
            "any",
            &ReplAck {
                epoch: 8,
                applied_tx_id: 0,
                durable_tx_id: 0,
            },
        );
        assert!(plain.visibility_barrier(&metrics, 1).await.is_err());
    }
}
