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
//! it, and a primary whose session partner presents a higher epoch stops
//! streaming and reports itself stale.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};

use fluxum_core::FluxumError;
use fluxum_core::commitlog::{self, TxRecord};
use fluxum_core::txn::CommitMeta;
use fluxum_protocol::{
    ClientMessage, Frame, FrameCodec, PrimaryHello, ReplAck, ReplBatch, ReplCheckpoint,
    ReplHeartbeat, ReplicaHello, ServerMessage,
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
}

impl Default for PrimaryOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            window_bytes: 8 << 20,
        }
    }
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
        })
    }

    /// The current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Record a replica's acknowledgment (REP-017; quorum input for
    /// REP-021). Unknown members are ignored (a late ack after drop).
    pub fn ack(&self, member: &str, ack: &ReplAck) {
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
            // Mechanical fencing here; the demote/step-down flow is T7.2.
            ctx.metrics().note_replication_fenced();
            tracing::warn!(target: "fluxum::repl", ours = epoch, theirs = hello.epoch,
                "replica presented a higher epoch; this primary is stale (REP-031)");
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
        primary.ack(
            "ghost",
            &ReplAck {
                epoch: 4,
                applied_tx_id: 1,
                durable_tx_id: 1,
            },
        );
        assert!(primary.durable_offsets().is_empty());
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
}
