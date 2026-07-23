//! Crash-fault injection (SPEC-024 DEV-021) — the physically possible
//! `kill -9` disk states, borrowed from the DST harness's crash model
//! (`fluxum-dst`, TST-131): the un-fsynced tail vanishing at an entry
//! boundary, or the last entry cut mid-frame. Recovery then runs the REAL
//! checkpoint + replay path, so a test exercises exactly what production
//! would do after the same crash.

use std::path::PathBuf;

use fluxum_core::{FluxumError, Result};
use fluxum_dst::SimRng;

use crate::{RecordedCall, TestShard};

/// Segment header length (frozen STG-011 format; see `commitlog::format`).
const SEGMENT_HEADER_LEN: usize = 24;
/// Entry envelope overhead: `length u32 | epoch u64 | crc32c u32` (STG-011).
const ENTRY_OVERHEAD: usize = 16;

/// A shard frozen at a simulated `kill -9` (DEV-021): the durable log is on
/// disk, nothing was cleanly closed. Apply zero or more tail faults, then
/// [`CrashedShard::recover`].
pub struct CrashedShard {
    root: tempfile::TempDir,
    recording: Vec<RecordedCall>,
    clock_us: i64,
    rng: SimRng,
}

impl CrashedShard {
    pub(crate) fn new(
        root: tempfile::TempDir,
        recording: Vec<RecordedCall>,
        clock_us: i64,
        rng: SimRng,
    ) -> Self {
        Self {
            root,
            recording,
            clock_us,
            rng,
        }
    }

    /// Simulate a **mid-commit crash** (lost fsync): the last log entry
    /// vanishes at its entry boundary, as if the machine died after the
    /// commit was acknowledged in memory but before its bytes were fsynced.
    /// Returns `false` when the log holds no entries to lose.
    pub fn lose_last_commit(&mut self) -> Result<bool> {
        let Some((segment, start, _end)) = self.last_entry()? else {
            return Ok(false);
        };
        let bytes = std::fs::read(&segment).map_err(FluxumError::from)?;
        std::fs::write(&segment, &bytes[..start]).map_err(FluxumError::from)?;
        Ok(true)
    }

    /// Simulate a **torn tail**: the last log entry is cut mid-frame — the
    /// partially-written state a crash can leave inside a sector. Recovery
    /// must quarantine the torn entry and keep the intact prefix. Returns
    /// `false` when the log holds no entries to tear.
    pub fn tear_last_commit(&mut self) -> Result<bool> {
        let Some((segment, start, end)) = self.last_entry()? else {
            return Ok(false);
        };
        let bytes = std::fs::read(&segment).map_err(FluxumError::from)?;
        // Cut somewhere strictly inside the frame — seeded, so the exact
        // tear point is reproducible from the shard seed.
        let span = end - start;
        let cut = start + 1 + self.rng.index(span - 1);
        std::fs::write(&segment, &bytes[..cut]).map_err(FluxumError::from)?;
        Ok(true)
    }

    /// Run the REAL recovery — quarantine, checkpoint adoption, log replay —
    /// and hand back a live shard over the surviving state. The recording,
    /// simulated clock, and RNG continue where the crashed life left off.
    pub fn recover(self) -> Result<TestShard> {
        TestShard::boot(
            0, // seed unused: the clock/rng carry over from the crashed life
            self.root,
            self.recording,
            Some((self.clock_us, self.rng)),
        )
    }

    /// Locate the LAST entry on disk: `(segment path, start, end)`, walking
    /// the frozen STG-011 framing exactly as the DST harness does.
    fn last_entry(&self) -> Result<Option<(PathBuf, usize, usize)>> {
        let log_dir = self.root.path().join("log");
        let mut segments: Vec<PathBuf> = std::fs::read_dir(&log_dir)
            .map_err(FluxumError::from)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect();
        segments.sort();
        for segment in segments.into_iter().rev() {
            let bytes = std::fs::read(&segment).map_err(FluxumError::from)?;
            let mut offset = SEGMENT_HEADER_LEN;
            let mut last: Option<(usize, usize)> = None;
            while offset + ENTRY_OVERHEAD <= bytes.len() {
                let len = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as usize;
                let end = offset + ENTRY_OVERHEAD + len;
                if end > bytes.len() {
                    break; // an already-torn tail: do not count it
                }
                last = Some((offset, end));
                offset = end;
            }
            if let Some((start, end)) = last {
                return Ok(Some((segment, start, end)));
            }
        }
        Ok(None)
    }
}
