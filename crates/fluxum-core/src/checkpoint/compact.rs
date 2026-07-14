//! Checkpoint-driven log truncation, routed through an archival hook
//! (STG-013 / FR-14 / FR-104): segments fully covered by a completed
//! checkpoint leave the live log — archived first when log archival is
//! enabled (the PITR source, SPEC-014 REP-070), deleted outright otherwise.

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crate::commitlog::segment::{list_segments, sync_dir};
use crate::error::{FluxumError, Result};

/// The archival hook (FR-104): takes custody of a covered segment's bytes
/// *before* [`compact_covered`] removes it from the live log. Implementors
/// must make the copy durable before returning — once the hook succeeds, the
/// original is deleted.
pub trait SegmentArchive: Send + Sync {
    /// Preserve `segment` (copy or upload). Failing here aborts compaction
    /// of this and later segments; nothing is deleted on failure.
    fn archive(&self, segment: &Path) -> Result<()>;
}

/// [`SegmentArchive`] into a local directory: byte-identical durable copies,
/// the simplest REP-070 archival target (remote sinks slot in behind the
/// same trait).
#[derive(Debug)]
pub struct DirectoryArchive {
    dir: PathBuf,
}

impl DirectoryArchive {
    /// Open (or create) the archive directory.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }
}

impl SegmentArchive for DirectoryArchive {
    fn archive(&self, segment: &Path) -> Result<()> {
        let name = segment.file_name().ok_or_else(|| {
            FluxumError::Storage(format!(
                "segment path {} has no file name",
                segment.display()
            ))
        })?;
        let dest = self.dir.join(name);
        fs::copy(segment, &dest)?;
        // The copy must be durable before the original may be deleted. The
        // handle needs write access: flushing to stable storage is denied on
        // read-only handles on Windows.
        OpenOptions::new().write(true).open(&dest)?.sync_all()?;
        sync_dir(&self.dir)?;
        Ok(())
    }
}

/// Truncate commit-log segments fully covered by a completed checkpoint at
/// `covered_up_to_tx` (STG-013 / FR-14) — every log truncation routes
/// through here, so enabling archival is a hook, not a code path change
/// (FR-104).
///
/// A segment is removable only if it is not the active tail, every entry in
/// it is `<= covered_up_to_tx`, and no replication retention hold needs it
/// (`retention_hold` = lowest tx offset a connected replica still requires).
/// With `archive` set, each segment is archived (durably, byte-identically)
/// before the original is deleted; without it, segments are deleted
/// outright. Returns the removed segment paths.
///
/// To keep STG-021's fallback recoverable, callers should pass the **oldest
/// retained** checkpoint's `last_tx_id` (see [`super::SnapshotWorker`]) —
/// compacting to the newest checkpoint would strand older retained
/// checkpoints without the log suffix they need to replay forward.
pub fn compact_covered(
    log_dir: &Path,
    shard_id: u32,
    covered_up_to_tx: u64,
    retention_hold: Option<u64>,
    archive: Option<&dyn SegmentArchive>,
) -> Result<Vec<PathBuf>> {
    let segments = list_segments(log_dir, shard_id)?;
    let mut removed = Vec::new();
    for pair in segments.windows(2) {
        // The segment's entries end where the next segment begins.
        let next_first = pair[1].first_tx_id;
        let covered = next_first <= covered_up_to_tx.saturating_add(1);
        let held = retention_hold.is_some_and(|hold| next_first > hold);
        if !covered || held {
            continue;
        }
        if let Some(hook) = archive {
            hook.archive(&pair[0].path)?;
        }
        fs::remove_file(&pair[0].path)?;
        removed.push(pair[0].path.clone());
    }
    if !removed.is_empty() {
        sync_dir(log_dir)?;
    }
    Ok(removed)
}
