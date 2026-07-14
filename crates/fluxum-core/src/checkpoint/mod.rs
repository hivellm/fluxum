//! Checkpoints — incremental, content-addressed point-in-time images of
//! `CommittedState` (SPEC-002 §4/§5, T2.3): the [`CheckpointRepo`]
//! (SnapshotRepo), the periodic [`SnapshotWorker`], checkpoint+replay
//! [`recover`]y with fallback, and checkpoint-driven log truncation routed
//! through an archival hook.
//!
//! # Design decisions (T2.3)
//!
//! - **Manifest + content-addressed objects** (STG-021): a checkpoint is a
//!   [`Manifest`] naming row-chunk objects stored by content hash
//!   (SHA-256; BLAKE3-class — same interim choice as the T2.2 blob store,
//!   free to change until the G5 format freeze) under
//!   `snapshot_dir/objects/`. Chunks unchanged since the previous checkpoint
//!   hash identically and are shared, never rewritten — a checkpoint costs
//!   only the changed objects, not a full dump. Chunk boundaries are
//!   **content-defined** (`crc32(pk) % 64 == 0`, capped at 256 rows): a
//!   row's chunk membership depends only on nearby keys, so one mutated row
//!   re-chunks only its neighborhood. When SPEC-015's pager lands, physical
//!   pages become the objects behind this same manifest scheme
//!   (TIER-060/TIER-063).
//! - **Two-phase crash safety** (STG-021): every object is written durably
//!   (temp + fsync + rename) *before* the fsynced manifest lands as the
//!   commit record — a checkpoint whose manifest is absent or fails
//!   verification does not exist. The manifest carries an integrity hash
//!   over its serialized bytes; restore verifies it and every object hash
//!   before adopting the checkpoint, **falling back to an older retained
//!   checkpoint** on permanent mismatch.
//! - **Non-blocking writes** (STG-022): the [`SnapshotWorker`] runs on its
//!   own OS thread and reads a wait-free [`crate::store::MemStore::snapshot`]
//!   — no store lock is held for any part of the write (T2.1's `ArcSwap`
//!   snapshot is even cheaper than STG-022's "brief read lock" ceiling), so
//!   reducer execution proceeds while objects hit disk. The manifest is
//!   stamped with the highest commit observed *before* the snapshot — a
//!   lower bound — and [`recover`]'s replay application is convergent
//!   (inserts upsert, deletes of absent keys are no-ops), so checkpoint +
//!   replay lands exactly on the full-log-replay state.
//! - **Recovery** (STG-030): newest fully-verified checkpoint + replay of
//!   log records past its `last_tx_id`, secondary indexes rebuilt from the
//!   recovered rows, auto-inc counters resumed from the recovered high-water
//!   marks (STG-040), and the next tx id at `last_tx_id + 1` (STG-015).
//!   Replay corruption keeps the valid prefix and is reported, never fatal
//!   (STG-031).
//! - **Log truncation through an archival hook** (STG-013 / FR-104): every
//!   truncation routes through [`compact_covered`] — with archival enabled
//!   ([`SegmentArchive`], [`DirectoryArchive`]) covered segments are
//!   durably archived before deletion (the PITR source, SPEC-014 REP-070).
//!   The worker compacts only up to the **oldest retained** checkpoint, so
//!   the STG-021 fallback always keeps the log suffix it needs.
//! - **Retention** (STG-023): the newest `retention` checkpoints (default 3,
//!   minimum 2) plus any pinned checkpoint (replica transfers,
//!   [`CheckpointRepo::pin`]) survive [`CheckpointRepo::prune`]; object GC
//!   removes only hashes no retained manifest references.
//! - **Adaptive cadence** (FR-14 / FR-113): checkpoint every
//!   `storage.checkpoint_interval_tx` committed transactions; the `auto`
//!   derivation ([`adaptive_interval_tx`]) scales the STG-020 default of
//!   10,000 with effective memory from the [`crate::hw::HardwareProfile`].

mod manifest;

pub mod compact;
pub mod recover;
pub mod repo;
pub mod worker;

pub use compact::{DirectoryArchive, SegmentArchive, compact_covered};
pub use manifest::{Manifest, ObjectHash, TableManifest};
pub use recover::{RecoveryOutcome, RejectedCheckpoint, recover};
pub use repo::{CheckpointRef, CheckpointRepo, CheckpointStats, LoadedCheckpoint, LoadedTable};
pub use worker::{
    LogCompaction, SnapshotWorker, WorkerOptions, WorkerReport, adaptive_interval_tx,
};

#[cfg(test)]
mod tests;
