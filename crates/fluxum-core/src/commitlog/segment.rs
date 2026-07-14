//! Segment files: naming/listing (STG-014), creation and append resume,
//! validating scans, and the STG-031 non-destructive torn-tail quarantine.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{FluxumError, Result};

use super::format::{
    SEGMENT_HEADER_LEN, ScannedEntry, decode_segment_header, encode_segment_header,
    parse_segment_file_name, scan_entry, segment_file_name,
};
use super::record::TxRecord;

/// One segment file of a shard's log, identified by its first tx id.
#[derive(Debug, Clone)]
pub(crate) struct SegmentRef {
    /// Absolute path of the segment file.
    pub path: PathBuf,
    /// The `first_tx_id` component of the file name (STG-014).
    pub first_tx_id: u64,
}

/// List the segments of `shard_id` in `dir`, sorted by `first_tx_id`.
pub(crate) fn list_segments(dir: &Path, shard_id: u32) -> Result<Vec<SegmentRef>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(first_tx_id) = parse_segment_file_name(name, shard_id) {
            segments.push(SegmentRef {
                path: entry.path(),
                first_tx_id,
            });
        }
    }
    segments.sort_by_key(|s| s.first_tx_id);
    Ok(segments)
}

/// An open segment the writer appends to.
#[derive(Debug)]
pub(crate) struct SegmentFile {
    /// Open handle positioned at the end (append mode).
    pub file: File,
    /// Current byte length, including buffered-but-unwritten bytes tracked
    /// by the caller.
    pub len: u64,
}

/// Create a new segment: write its header and make it durable before any
/// entry can reference it.
pub(crate) fn create_segment(
    dir: &Path,
    shard_id: u32,
    first_tx_id: u64,
    epoch: u64,
) -> Result<SegmentFile> {
    let path = dir.join(segment_file_name(shard_id, first_tx_id));
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&path)?;
    file.write_all(&encode_segment_header(epoch))?;
    file.sync_data()?;
    sync_dir(dir)?;
    Ok(SegmentFile {
        file,
        len: SEGMENT_HEADER_LEN as u64,
    })
}

/// Reopen an existing segment for appending at `len` (its validated end).
pub(crate) fn open_segment_for_append(path: &Path, len: u64) -> Result<SegmentFile> {
    let file = OpenOptions::new().append(true).open(path)?;
    Ok(SegmentFile { file, len })
}

/// Where and why a scan stopped before the end of the segment.
#[derive(Debug, Clone)]
pub(crate) struct ScanFault {
    /// Byte offset of the first invalid entry frame.
    pub offset: u64,
    /// Human-readable cause (torn write vs checksum/decode failure).
    pub reason: String,
}

/// The result of scanning one segment.
#[derive(Debug)]
pub(crate) struct SegmentScan {
    /// Number of valid entries.
    pub entries: u64,
    /// Last valid entry's tx id, if any entry is present.
    pub last_tx: Option<u64>,
    /// Highest epoch seen (header or entry envelope).
    pub max_epoch: u64,
    /// Byte length of the valid prefix (header + valid entries).
    pub valid_len: u64,
    /// Set when the segment has bytes past `valid_len` that do not form a
    /// valid entry (torn tail or corruption, STG-031).
    pub fault: Option<ScanFault>,
}

/// Scan outcome: a readable segment, or one whose header itself is invalid.
#[derive(Debug)]
pub(crate) enum ScanOutcome {
    /// Header verified; entry-level results inside.
    Scanned(SegmentScan),
    /// The 24-byte header failed validation — nothing in the file can be
    /// trusted.
    HeaderCorrupt(String),
}

/// Scan `path`, verifying framing, checksums, epoch monotonicity, and strict
/// `tx_id` increase (STG-015), invoking `visit` for every valid entry in
/// order. `prev_tx` / `min_epoch` carry cross-segment expectations.
pub(crate) fn scan_segment(
    path: &Path,
    shard_id: u32,
    mut prev_tx: Option<u64>,
    min_epoch: u64,
    visit: &mut dyn FnMut(u64, TxRecord) -> Result<()>,
) -> Result<ScanOutcome> {
    let buf = fs::read(path)?;
    let header_epoch = match decode_segment_header(&buf) {
        Ok(epoch) => epoch,
        Err(reason) => return Ok(ScanOutcome::HeaderCorrupt(reason)),
    };
    let mut scan = SegmentScan {
        entries: 0,
        last_tx: None,
        max_epoch: header_epoch.max(min_epoch),
        valid_len: SEGMENT_HEADER_LEN as u64,
        fault: None,
    };
    if header_epoch < min_epoch {
        scan.fault = Some(ScanFault {
            offset: 0,
            reason: format!("segment header epoch {header_epoch} regresses below {min_epoch}"),
        });
        return Ok(ScanOutcome::Scanned(scan));
    }
    let mut offset = SEGMENT_HEADER_LEN;
    loop {
        let fault = |offset: usize, reason: String| {
            Some(ScanFault {
                offset: offset as u64,
                reason,
            })
        };
        match scan_entry(&buf, offset) {
            ScannedEntry::CleanEof => break,
            ScannedEntry::Torn(reason) => {
                scan.fault = fault(offset, format!("torn write: {reason}"));
                break;
            }
            ScannedEntry::Corrupt(reason) => {
                scan.fault = fault(offset, reason);
                break;
            }
            ScannedEntry::Entry { epoch, body, end } => {
                if epoch < scan.max_epoch {
                    scan.fault = fault(
                        offset,
                        format!("entry epoch {epoch} regresses below {}", scan.max_epoch),
                    );
                    break;
                }
                let record = match TxRecord::decode(body) {
                    Ok(record) => record,
                    Err(reason) => {
                        scan.fault = fault(offset, reason);
                        break;
                    }
                };
                if record.shard_id != shard_id {
                    scan.fault = fault(
                        offset,
                        format!(
                            "entry shard_id {} in a shard-{shard_id} log",
                            record.shard_id
                        ),
                    );
                    break;
                }
                if prev_tx.is_some_and(|prev| record.tx_id <= prev) {
                    // A decrease or repeat is corruption (STG-015).
                    scan.fault = fault(
                        offset,
                        format!(
                            "tx_id {} does not strictly increase past {:?}",
                            record.tx_id, prev_tx
                        ),
                    );
                    break;
                }
                prev_tx = Some(record.tx_id);
                scan.last_tx = Some(record.tx_id);
                scan.max_epoch = scan.max_epoch.max(epoch);
                scan.entries += 1;
                scan.valid_len = end as u64;
                visit(epoch, record)?;
                offset = end;
            }
        }
    }
    Ok(ScanOutcome::Scanned(scan))
}

/// A completed STG-031 quarantine.
#[derive(Debug, Clone)]
pub struct QuarantineReport {
    /// The affected segment file.
    pub segment: PathBuf,
    /// First quarantined byte offset within the segment.
    pub from_offset: u64,
    /// Number of quarantined bytes.
    pub bytes: u64,
    /// Sidecar file holding the byte-identical torn tail.
    pub sidecar: PathBuf,
    /// Why the tail was invalid.
    pub reason: String,
}

/// Pick a sidecar path next to `path` that does not exist yet
/// (`<segment>.torn`, then `.torn-2`, `.torn-3`, …).
fn sidecar_path(path: &Path) -> PathBuf {
    let base = PathBuf::from(format!("{}.torn", path.display()));
    if !base.exists() {
        return base;
    }
    let mut n = 2u32;
    loop {
        let candidate = PathBuf::from(format!("{}.torn-{n}", path.display()));
        if !candidate.exists() {
            return candidate;
        }
        n += 1;
    }
}

/// Quarantine the tail of `path` from `from_offset`: copy the affected bytes
/// byte-identically into a sidecar file, make the sidecar durable, and only
/// then truncate the segment back to the last valid entry boundary so the
/// writer can resume (STG-031 — never a destructive truncation: the evidence
/// survives in the sidecar).
pub(crate) fn quarantine_tail(
    path: &Path,
    from_offset: u64,
    reason: &str,
) -> Result<QuarantineReport> {
    let buf = fs::read(path)?;
    let from = usize::try_from(from_offset).map_err(|_| {
        FluxumError::Storage(format!("quarantine offset {from_offset} out of range"))
    })?;
    let tail = buf.get(from..).unwrap_or(&[]);
    let sidecar = sidecar_path(path);
    let mut sidecar_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&sidecar)?;
    sidecar_file.write_all(tail)?;
    sidecar_file.sync_all()?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    // The tail is durable in the sidecar; the segment may now shrink back to
    // its last valid boundary.
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(from_offset)?;
    file.sync_data()?; // some filesystems require fsync after ftruncate
    Ok(QuarantineReport {
        segment: path.to_path_buf(),
        from_offset,
        bytes: tail.len() as u64,
        sidecar,
        reason: reason.to_string(),
    })
}

/// Quarantine a whole segment file whose header is unreadable: rename it to
/// a sidecar name (byte-identical preservation), removing it from the log.
pub(crate) fn quarantine_whole_file(path: &Path, reason: &str) -> Result<QuarantineReport> {
    let bytes = fs::metadata(path)?.len();
    let sidecar = sidecar_path(path);
    fs::rename(path, &sidecar)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    Ok(QuarantineReport {
        segment: path.to_path_buf(),
        from_offset: 0,
        bytes,
        sidecar,
        reason: reason.to_string(),
    })
}

/// Flush directory metadata so newly created/renamed files survive a crash.
/// POSIX only; Windows has no directory fsync.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}
