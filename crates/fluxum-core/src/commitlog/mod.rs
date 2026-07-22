//! CommitLog — the per-shard append-only durability log (SPEC-002 §3/§5,
//! T2.2). The entry format doubles as the replication stream (STG-016) and
//! freezes at gate G5.
//!
//! # Design decisions (T2.2)
//!
//! - **Entry format** (STG-011): `u32 LE length | u64 LE epoch | MessagePack
//!   TxRecord | u32 LE CRC32C`, with the CRC covering length + epoch + body,
//!   so a corrupted prefix is a checksum failure, never a mis-framed
//!   segment. The epoch (the shard leader lineage's fencing term, SPEC-014)
//!   is recorded durably in every entry *and* in each segment header, so
//!   divergence detection / PITR lineage need no separate epoch→offset map.
//!   Rows are stored as self-describing [`LogValue`] vectors — replay can
//!   always decode or skip a record without schema knowledge.
//! - **Group commit** (STG-012): [`CommitLog::append`] enqueues onto a
//!   bounded queue feeding a dedicated flush actor (its own OS thread —
//!   blocking I/O and `fsync` never run inline on a commit path or an async
//!   runtime). The actor drains the queue in batches, appends, and performs
//!   **one fsync per batch** — amortized to near zero per tx under load,
//!   degrading to fsync-per-tx when idle. After every successful fsync it
//!   publishes the durable offset on a `tokio::sync::watch` channel
//!   ([`DurableState`]) — the primitive replication quorum acks and
//!   confirmed reads gate on. A failed write/fsync is fatal for the writer:
//!   no retry, the failure is published, and subsequent appends error.
//! - **Rotation & retention** (STG-013/STG-014, OQ-5 defaults): segments
//!   rotate when the active one reaches `segment_max_bytes` (default
//!   128 MiB), named `shard-<shard_id>-<first_tx_id>.log` with the tx id
//!   zero-padded so directory order equals offset order. Compaction
//!   ([`CommitLog::compact`]) deletes only segments fully covered by a
//!   completed checkpoint, and never past the replication retention hold
//!   ([`CommitLog::set_retention_hold`]).
//! - **Torn-tail repair is non-destructive** (STG-031): on open, the tail
//!   segment is validated; the torn/corrupt suffix is copied byte-identically
//!   into a `<segment>.torn` sidecar (fsynced first), only then is the
//!   segment truncated to the last valid entry boundary and appends resume.
//!   The operator is notified via structured `tracing` output. Corruption in
//!   a *non-tail* segment refuses to open — destructive repair exists only
//!   as the replication layer's explicit `reset_to` (REP-013), never as a
//!   side effect of opening the log.
//! - **Strict ordering** (STG-015): `tx_id` must strictly increase — enforced
//!   at append time and re-verified on every scan; a decrease or repeat is
//!   corruption. An append/open under an epoch lower than the highest
//!   durably written epoch is rejected (STG-011).
//! - **Blob store** (STG-041): [`BlobStore`] holds large values out-of-row,
//!   content-addressed and refcounted — identical values are stored once,
//!   and bytes are reclaimed only when unreferenced *and* free of retention
//!   holds (checkpoints, retained segments). Row-level integration arrives
//!   with the pager work (SPEC-015).
//!
//! Recovery orchestration (checkpoint load + [`replay`] into the store,
//! STG-030) is assembled in T2.3; this module provides the log half:
//! [`replay`] yields every valid [`TxRecord`] in order and reports where and
//! why it stopped.

mod format;

pub(crate) mod segment;

pub mod audit;
pub mod blob;
pub mod record;
pub mod replay;
pub mod writer;

pub use audit::{AuditEntry, AuditQuery, DEFAULT_AUDIT_LIMIT, audit};
pub use blob::{BlobHash, BlobStore};
pub use record::{LogValue, TableMutation, TxRecord};
pub use replay::{Corruption, ReplayReport, replay};
pub use segment::QuarantineReport;
pub use writer::{CommitLog, DurableState, RecoveryReport};

use crate::error::{FluxumError, Result};

/// Tuning knobs for a [`CommitLog`] (SPEC-002 §8; wired into `config.yml`
/// with the server assembly).
#[derive(Debug, Clone, Copy)]
pub struct CommitLogOptions {
    /// Rotate to a new segment when the active one reaches this size
    /// (STG-013; default 128 MiB).
    pub segment_max_bytes: u64,
    /// Bounded append-queue depth feeding the flush actor (STG-012
    /// backpressure; default 4096).
    pub queue_depth: usize,
    /// Maximum entries drained into one write+fsync batch (default 4096).
    pub max_batch: usize,
    /// Initial capacity of the actor's write buffer (default 128 KiB).
    pub write_buffer_bytes: usize,
    /// Minimum spacing between group-commit fsyncs (default 5 ms;
    /// `Duration::ZERO` = fsync after every written batch).
    ///
    /// Why spacing exists: the OS serializes a file's writes against an
    /// in-flight flush of the same file, so a back-to-back fsync loop makes
    /// every append (and with it the TXN-004 `written` ack watermark) wait
    /// out the previous fsync — acks degrade to disk speed. Spaced fsyncs
    /// leave the writer unblocked for the whole interval and coalesce more
    /// batches per flush. The durable watermark lags by at most this
    /// interval + one fsync, well inside the NFR-08 ~50 ms crash-loss
    /// budget — and acked (`written`) data still survives a process crash
    /// regardless.
    pub sync_interval: std::time::Duration,
}

impl Default for CommitLogOptions {
    fn default() -> Self {
        Self {
            segment_max_bytes: 128 * 1024 * 1024,
            queue_depth: 4096,
            max_batch: 4096,
            write_buffer_bytes: 128 * 1024,
            sync_interval: std::time::Duration::from_millis(5),
        }
    }
}

impl CommitLogOptions {
    fn validate(&self) -> Result<()> {
        if self.queue_depth == 0 || self.max_batch == 0 {
            return Err(FluxumError::Storage(
                "commit-log queue_depth and max_batch must be >= 1".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
