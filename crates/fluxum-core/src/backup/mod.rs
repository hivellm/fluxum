//! Hot backup, verification, restore, and point-in-time recovery
//! (SPEC-014 §8/§9, REP-060..REP-072; FR-103/FR-104; DAG T7.3).
//!
//! A backup is a directory:
//!
//! ```text
//! <out>/
//!   manifest.mpack                      REP-061 BackupManifest (MessagePack)
//!   shard-<id>/
//!     checkpoint.pack                   the latest checkpoint, packed into one
//!                                       file (manifest + content-addressed
//!                                       objects, each already zstd on disk)
//!     shard-<id>-<first_tx>.log.zst     each covering segment, zstd artifact
//! ```
//!
//! **Hot by construction** (REP-060): `create` only ever *reads* the
//! checkpoint repository (whose artifacts are immutable once named by a
//! manifest) and the segment files' validated prefixes — no lock is taken,
//! no writer path is touched. The segment scan stops at the last valid entry
//! boundary, so a concurrent append is simply not included.
//!
//! **PITR** (REP-070/071) restores the base backup, then extends the
//! `tx_id` chain with archived segments (REP-062 — byte-identical copies made
//! by the checkpoint worker's [`crate::checkpoint::DirectoryArchive`] hook
//! before truncation) and cuts the restored log at the entry boundary of the
//! inclusive target. The unit of replay is the whole `TxRecord`: the cut is
//! a byte-prefix truncation at a validated frame boundary, so every
//! per-entry CRC in the surviving prefix still holds and the normal STG-030
//! recovery replays exactly the transactions `<= target` on next boot.
//!
//! **Lineage** (REP-072): a PITR restore forks history, so `pitr` writes a
//! lineage marker next to the restored log naming the minimum fencing epoch
//! the next boot must adopt (strictly greater than any epoch in the restored
//! log). [`pitr_lineage_min_epoch`] is how the server assembly picks it up;
//! replication's handshake history check is what will refuse partial sync
//! into the old set once replica sets land (T7.1).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::checkpoint::{CheckpointRef, CheckpointRepo};
use crate::commitlog::record::{LogValue, TxRecord};
use crate::commitlog::segment::{ScanOutcome, list_segments, scan_segment_bytes, sync_dir};
use crate::error::{FluxumError, Result};
use crate::store::pager::codec::{
    DEFAULT_ARTIFACT_ZSTD_LEVEL, compress_artifact, decompress_artifact,
};

/// Backup layout format version ([`BackupManifest::format_version`]).
pub const BACKUP_FORMAT_VERSION: u32 = 1;

/// The backup manifest file name.
pub const MANIFEST_FILE: &str = "manifest.mpack";

/// The lineage marker a PITR restore leaves next to the log (REP-072).
pub const LINEAGE_FILE: &str = "pitr.lineage";

/// The REP-061 backup manifest (`manifest.mpack`, MessagePack). rmp-serde
/// serializes positionally; field order is frozen with the format version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Unique backup id (UUID v4).
    pub backup_id: String,
    /// Microseconds since the Unix epoch at creation.
    pub created_at: i64,
    /// Backup layout version ([`BACKUP_FORMAT_VERSION`]).
    pub format_version: u32,
    /// The module's `__schema_meta__` schema version at the checkpoint
    /// (SPEC-010); `0` when the backup carries no checkpoint or the meta
    /// table is absent (a pre-first-boot data directory).
    pub schema_version: u32,
    /// One entry per shard captured.
    pub shards: Vec<ShardBackup>,
}

/// One shard's slice of a backup (REP-061).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardBackup {
    /// The shard.
    pub shard_id: u32,
    /// The packed-checkpoint file, relative to the backup root; empty when
    /// the shard had no checkpoint yet (the segments then start at tx 1).
    pub checkpoint_file: String,
    /// The checkpoint's covering transaction id (0 = no checkpoint).
    pub checkpoint_last_tx_id: u64,
    /// The covering log segments, ascending by `first_tx_id`.
    pub segments: Vec<SegmentEntry>,
    /// CRC32C of the stored `checkpoint_file` bytes (0 = no checkpoint).
    pub checkpoint_crc32: u32,
}

/// One archived segment inside a backup (REP-061).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentEntry {
    /// File name relative to the backup root (`shard-<id>/<name>.zst`).
    pub file: String,
    /// First transaction id in the segment.
    pub first_tx_id: u64,
    /// Last transaction id in the segment.
    pub last_tx_id: u64,
    /// CRC32C of the stored (compressed) file bytes.
    pub crc32: u32,
}

/// The packed form of one checkpoint: its manifest plus every
/// content-addressed object it references, each byte-identical to its
/// on-disk (already zstd-compressed, possibly sealed) form.
#[derive(Debug, Serialize, Deserialize)]
struct CheckpointPack {
    /// The manifest's original file name (restored verbatim).
    manifest_name: String,
    /// The manifest file bytes.
    manifest: ByteBuf,
    /// `(hash-hex file name, stored bytes)` for every referenced object.
    objects: Vec<(String, ByteBuf)>,
}

/// Where a shard's live data lives — the inputs to [`create`].
#[derive(Debug, Clone)]
pub struct BackupSource {
    /// The checkpoint repository directory (`storage.checkpoint_dir`).
    pub checkpoint_dir: PathBuf,
    /// The commit-log directory (`storage.commit_log_dir`).
    pub log_dir: PathBuf,
}

/// What [`create`] captured.
#[derive(Debug, Clone)]
pub struct BackupReport {
    /// The manifest written.
    pub manifest_path: PathBuf,
    /// The backup id.
    pub backup_id: String,
    /// Shards captured.
    pub shards: u32,
    /// Segments captured across all shards.
    pub segments: u64,
    /// Highest `last_tx_id` across all captured segments/checkpoints.
    pub head_tx_id: u64,
}

/// One file's verification outcome (REP-064).
#[derive(Debug, Clone)]
pub struct FileCheck {
    /// The file, relative to the backup root.
    pub file: String,
    /// `None` = passed; otherwise the precise failure.
    pub error: Option<String>,
}

/// The [`verify`] outcome: per-file results, failures first preserved in
/// order of discovery.
#[derive(Debug, Clone, Default)]
pub struct VerifyReport {
    /// Every file checked.
    pub files: Vec<FileCheck>,
}

impl VerifyReport {
    /// Whether every check passed.
    pub fn ok(&self) -> bool {
        self.files.iter().all(|f| f.error.is_none())
    }

    /// The failing checks.
    pub fn errors(&self) -> impl Iterator<Item = &FileCheck> {
        self.files.iter().filter(|f| f.error.is_some())
    }

    fn pass(&mut self, file: &str) {
        self.files.push(FileCheck {
            file: file.to_owned(),
            error: None,
        });
    }

    fn fail(&mut self, file: &str, error: impl Into<String>) {
        self.files.push(FileCheck {
            file: file.to_owned(),
            error: Some(error.into()),
        });
    }
}

/// What [`restore`] wrote.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// Shards restored.
    pub shards: u32,
    /// Segment files written into the log directory.
    pub segments: u64,
    /// The backup head: the state a full restore reproduces on next boot.
    pub head_tx_id: u64,
}

/// A PITR target (REP-070): mutually exclusive on the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PitrTarget {
    /// Apply every transaction with `tx_id <= n`, inclusive.
    TxId(u64),
    /// Apply every transaction with `timestamp <= µs`, inclusive.
    TimestampMicros(i64),
}

/// What a PITR restore applied (REP-071: the boundary is reported).
#[derive(Debug, Clone)]
pub struct PitrReport {
    /// The last applied transaction id.
    pub last_tx_id: u64,
    /// Its commit timestamp (µs since the Unix epoch).
    pub last_timestamp: i64,
    /// The minimum fencing epoch recorded in the lineage marker (REP-072):
    /// strictly greater than any epoch in the restored log.
    pub fork_min_epoch: u64,
}

// --- create (REP-060/REP-061) ----------------------------------------------------

/// Create a hot backup of every shard found under `source` into `out`
/// (REP-060): per shard, the latest checkpoint (packed into one file) plus
/// the validated prefix of every log segment covering
/// `checkpoint_last_tx_id + 1` through the log head at scan time.
///
/// Reads only immutable checkpoint artifacts and validated segment
/// prefixes — no lock is taken and the writer is never touched.
///
/// # Errors
/// I/O failures, a corrupt checkpoint manifest, or a segment whose valid
/// prefix breaks the `tx_id` chain.
pub fn create(source: &BackupSource, out: &Path) -> Result<BackupReport> {
    fs::create_dir_all(out)?;
    let repo = CheckpointRepo::open(&source.checkpoint_dir)?;
    let mut shards = Vec::new();
    let mut segments_total = 0u64;
    let mut head_tx_id = 0u64;
    let mut schema_version = 0u32;

    for shard_id in discover_shards(source)? {
        let shard_dir_name = format!("shard-{shard_id}");
        let shard_dir = out.join(&shard_dir_name);
        fs::create_dir_all(&shard_dir)?;

        // The newest checkpoint, packed into one file (or none yet).
        let newest = repo.list(shard_id)?.pop();
        let (checkpoint_file, checkpoint_last_tx_id, checkpoint_crc32) = match &newest {
            Some(checkpoint) => {
                let (pack_bytes, version) = pack_checkpoint(&repo, checkpoint)?;
                if version != 0 {
                    schema_version = version;
                }
                let rel = format!("{shard_dir_name}/checkpoint.pack");
                write_file(&out.join(&rel), &pack_bytes)?;
                head_tx_id = head_tx_id.max(checkpoint.last_tx_id);
                (rel, checkpoint.last_tx_id, crc32c::crc32c(&pack_bytes))
            }
            None => (String::new(), 0, 0),
        };

        // Every segment holding transactions past the checkpoint: validated
        // prefix, zstd-compressed as one artifact.
        let mut entries = Vec::new();
        for segment in list_segments(&source.log_dir, shard_id)? {
            let bytes = fs::read(&segment.path)?;
            let mut last_seen = None;
            let outcome =
                scan_segment_bytes(&bytes, shard_id, None, 0, &mut |_, record: TxRecord| {
                    last_seen = Some(record.tx_id);
                    Ok(())
                })?;
            let scan = match outcome {
                ScanOutcome::Scanned(scan) => scan,
                ScanOutcome::HeaderCorrupt(reason) => {
                    return Err(FluxumError::Storage(format!(
                        "segment {} has a corrupt header: {reason}",
                        segment.path.display()
                    )));
                }
            };
            let Some(last_tx) = scan.last_tx else {
                continue; // only-header segment: nothing to cover
            };
            if last_tx <= checkpoint_last_tx_id {
                continue; // fully covered by the checkpoint (REP-060)
            }
            let valid = usize::try_from(scan.valid_len).unwrap_or(bytes.len());
            let stored = compress_artifact(&bytes[..valid], DEFAULT_ARTIFACT_ZSTD_LEVEL, None)?;
            let name = segment
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_owned();
            let rel = format!("{shard_dir_name}/{name}.zst");
            write_file(&out.join(&rel), &stored)?;
            entries.push(SegmentEntry {
                file: rel,
                first_tx_id: segment.first_tx_id,
                last_tx_id: last_tx,
                crc32: crc32c::crc32c(&stored),
            });
            segments_total += 1;
            head_tx_id = head_tx_id.max(last_tx);
        }

        shards.push(ShardBackup {
            shard_id,
            checkpoint_file,
            checkpoint_last_tx_id,
            segments: entries,
            checkpoint_crc32,
        });
    }

    if shards.is_empty() {
        return Err(FluxumError::Storage(format!(
            "nothing to back up: no checkpoints under {} and no segments under {}",
            source.checkpoint_dir.display(),
            source.log_dir.display()
        )));
    }

    let manifest = BackupManifest {
        backup_id: uuid::Uuid::new_v4().to_string(),
        created_at: crate::types::Timestamp::now().as_micros(),
        format_version: BACKUP_FORMAT_VERSION,
        schema_version,
        shards,
    };
    let manifest_path = out.join(MANIFEST_FILE);
    let bytes = rmp_serde::to_vec(&manifest)
        .map_err(|e| FluxumError::Storage(format!("backup manifest encoding failed: {e}")))?;
    write_file(&manifest_path, &bytes)?;
    sync_dir(out)?;
    Ok(BackupReport {
        manifest_path,
        backup_id: manifest.backup_id,
        shards: u32::try_from(manifest.shards.len()).unwrap_or(u32::MAX),
        segments: segments_total,
        head_tx_id,
    })
}

/// The shard ids present in a source (segment files and checkpoint
/// manifests both carry the shard in their names).
fn discover_shards(source: &BackupSource) -> Result<BTreeSet<u32>> {
    let mut shards = BTreeSet::new();
    for dir in [&source.log_dir, &source.checkpoint_dir] {
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(dir)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            // `shard-<id>-<tx>.log` / `ckpt-<id>-<tx>.manifest`
            if let Some(rest) = name
                .strip_prefix("shard-")
                .or_else(|| name.strip_prefix("ckpt-"))
                && let Some((id, _)) = rest.split_once('-')
                && let Ok(id) = id.parse::<u32>()
            {
                shards.insert(id);
            }
        }
    }
    Ok(shards)
}

/// Pack one checkpoint (manifest + referenced objects, all byte-identical to
/// their stored form) and extract the `__schema_meta__` schema version.
fn pack_checkpoint(repo: &CheckpointRepo, checkpoint: &CheckpointRef) -> Result<(Vec<u8>, u32)> {
    let manifest_bytes = fs::read(&checkpoint.path)?;
    let manifest = crate::checkpoint::manifest::decode_manifest(&manifest_bytes, None)?;
    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let mut schema_version = 0u32;
    for table in &manifest.tables {
        let hashes = table.chunk_hashes()?;
        for hash in &hashes {
            let name = hash.to_string();
            if seen.insert(name.clone()) {
                let bytes = fs::read(repo.object_path(&name))?;
                objects.push((name, ByteBuf::from(bytes)));
            }
        }
        if table.table_name == crate::migration::META_TABLE {
            schema_version = meta_schema_version(repo, &hashes)?.unwrap_or(0);
        }
    }
    let name = checkpoint
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_owned();
    let pack = CheckpointPack {
        manifest_name: name,
        manifest: ByteBuf::from(manifest_bytes),
        objects,
    };
    let bytes = rmp_serde::to_vec(&pack)
        .map_err(|e| FluxumError::Storage(format!("checkpoint pack encoding failed: {e}")))?;
    Ok((bytes, schema_version))
}

/// Read the stored schema version out of the `__schema_meta__` chunks of a
/// checkpoint (SPEC-010 MIG-002): rows are `[Str(key), Bytes(value)]`.
fn meta_schema_version(
    repo: &CheckpointRepo,
    chunks: &[crate::checkpoint::ObjectHash],
) -> Result<Option<u32>> {
    for hash in chunks {
        let stored = fs::read(repo.object_path(&hash.to_string()))?;
        let raw = decompress_artifact(&stored, None)?;
        let rows: Vec<Vec<LogValue>> = rmp_serde::from_slice(&raw)
            .map_err(|e| FluxumError::Storage(format!("checkpoint chunk decode failed: {e}")))?;
        for row in rows {
            if let [LogValue::Str(key), LogValue::Bytes(value)] = row.as_slice()
                && key == crate::migration::META_KEY_VERSION
            {
                return crate::migration::catalog::decode_version(value).map(Some);
            }
        }
    }
    Ok(None)
}

// --- verify (REP-064) --------------------------------------------------------------

/// Validate a backup without restoring it (REP-064): manifest decodable and
/// complete, every file's CRC32C matches, every `TxRecord` in every segment
/// decodes with its per-entry CRC, and the `tx_id` chain is strictly
/// contiguous from `checkpoint_last_tx_id + 1` per shard.
///
/// # Errors
/// Only on I/O failures reading the backup *directory structure* itself; a
/// failed check is a [`VerifyReport`] entry, not an `Err`.
pub fn verify(from: &Path) -> Result<VerifyReport> {
    let mut report = VerifyReport::default();
    let manifest = match read_manifest(from) {
        Ok(manifest) => {
            report.pass(MANIFEST_FILE);
            manifest
        }
        Err(e) => {
            report.fail(MANIFEST_FILE, e.to_string());
            return Ok(report);
        }
    };

    for shard in &manifest.shards {
        // (b) checkpoint file present + CRC.
        if !shard.checkpoint_file.is_empty() {
            match fs::read(from.join(&shard.checkpoint_file)) {
                Ok(bytes) if crc32c::crc32c(&bytes) == shard.checkpoint_crc32 => {
                    match unpack_checkpoint_bytes(&bytes) {
                        Ok(_) => report.pass(&shard.checkpoint_file),
                        Err(e) => report.fail(&shard.checkpoint_file, e.to_string()),
                    }
                }
                Ok(_) => report.fail(
                    &shard.checkpoint_file,
                    format!(
                        "CRC32C mismatch (manifest says {:#010x})",
                        shard.checkpoint_crc32
                    ),
                ),
                Err(e) => report.fail(&shard.checkpoint_file, format!("unreadable: {e}")),
            }
        }
        // (b)+(c) segments: file CRC, then structural scan with chain
        // contiguity from the checkpoint.
        // The chain expectation: the FIRST segment may begin at or before
        // `checkpoint_last_tx_id + 1` (a segment that straddles the
        // checkpoint boundary is the normal un-rotated case — recovery's
        // convergent replay no-ops the covered prefix); every later segment
        // must continue exactly where the previous one ended.
        let mut chain_next: Option<u64> = None;
        for entry in &shard.segments {
            // On a failed file, resume the chain expectation from the
            // manifest's own claim: the report stays one line per actually
            // corrupt file instead of cascading derived chain breaks.
            let bytes = match fs::read(from.join(&entry.file)) {
                Ok(bytes) => bytes,
                Err(e) => {
                    report.fail(&entry.file, format!("unreadable: {e}"));
                    chain_next = Some(entry.last_tx_id + 1);
                    continue;
                }
            };
            if crc32c::crc32c(&bytes) != entry.crc32 {
                report.fail(
                    &entry.file,
                    format!("CRC32C mismatch (manifest says {:#010x})", entry.crc32),
                );
                chain_next = Some(entry.last_tx_id + 1);
                continue;
            }
            match segment_check(&bytes, shard, entry, &mut chain_next) {
                Ok(()) => report.pass(&entry.file),
                Err(e) => {
                    report.fail(&entry.file, e.to_string());
                    chain_next = Some(entry.last_tx_id + 1);
                }
            }
        }
    }
    Ok(report)
}

/// Structural + chain checks for one backup segment (REP-064 (c)).
/// `chain_next` is `None` for the first segment (which may straddle the
/// checkpoint boundary) and the exact required first tx id afterwards.
fn segment_check(
    stored: &[u8],
    shard: &ShardBackup,
    entry: &SegmentEntry,
    chain_next: &mut Option<u64>,
) -> Result<()> {
    let raw = decompress_artifact(stored, None)?;
    let mut first_seen = None;
    let mut last_seen = None;
    let prev = chain_next.map(|next| next - 1).filter(|prev| *prev > 0);
    let outcome = scan_segment_bytes(&raw, shard.shard_id, prev, 0, &mut |_, record| {
        first_seen.get_or_insert(record.tx_id);
        last_seen = Some(record.tx_id);
        Ok(())
    })?;
    let scan = match outcome {
        ScanOutcome::Scanned(scan) => scan,
        ScanOutcome::HeaderCorrupt(reason) => {
            return Err(FluxumError::Storage(format!("corrupt header: {reason}")));
        }
    };
    if let Some(fault) = scan.fault {
        return Err(FluxumError::Storage(format!(
            "invalid entry at byte {}: {}",
            fault.offset, fault.reason
        )));
    }
    match (first_seen, last_seen) {
        (Some(first), Some(last)) => {
            match *chain_next {
                // First segment: it must reach back to (or before) the
                // checkpoint boundary so nothing between checkpoint and
                // chain is missing.
                None => {
                    if first > shard.checkpoint_last_tx_id + 1 {
                        return Err(FluxumError::Storage(format!(
                            "tx chain break: segment starts at tx {first}, but the checkpoint \
                             covers only through {}",
                            shard.checkpoint_last_tx_id
                        )));
                    }
                }
                Some(expected) => {
                    if first != expected {
                        return Err(FluxumError::Storage(format!(
                            "tx chain break: segment starts at tx {first}, expected {expected}"
                        )));
                    }
                }
            }
            if last != entry.last_tx_id {
                return Err(FluxumError::Storage(format!(
                    "manifest says last_tx_id {}, segment ends at {last}",
                    entry.last_tx_id
                )));
            }
            *chain_next = Some(last + 1);
            Ok(())
        }
        _ => Err(FluxumError::Storage("segment contains no entries".into())),
    }
}

// --- restore (REP-063) -------------------------------------------------------------

/// Where a restore unpacks to.
#[derive(Debug, Clone)]
pub struct RestoreDirs {
    /// The checkpoint repository directory to populate.
    pub checkpoint_dir: PathBuf,
    /// The commit-log directory to populate.
    pub log_dir: PathBuf,
}

/// Restore a backup (REP-063): verify every CRC against the manifest, then
/// unpack the checkpoint into the checkpoint directory and the segments into
/// the commit-log directory. The normal STG-030 recovery reconstructs
/// `CommittedState` on next startup, reproducing exactly the state at the
/// backup head.
///
/// Refuses non-empty target directories unless `force`.
///
/// # Errors
/// A failed verification (reported precisely), a non-empty target without
/// `force`, or I/O.
pub fn restore(from: &Path, dirs: &RestoreDirs, force: bool) -> Result<RestoreReport> {
    let report = verify(from)?;
    if !report.ok() {
        let detail: Vec<String> = report
            .errors()
            .map(|f| format!("{}: {}", f.file, f.error.as_deref().unwrap_or("failed")))
            .collect();
        return Err(FluxumError::Storage(format!(
            "backup fails verification; refusing to restore:\n  {}",
            detail.join("\n  ")
        )));
    }
    for dir in [&dirs.checkpoint_dir, &dirs.log_dir] {
        if !force && dir.exists() && fs::read_dir(dir)?.next().is_some() {
            return Err(FluxumError::Storage(format!(
                "target directory {} is not empty; pass --force to restore into it anyway",
                dir.display()
            )));
        }
        fs::create_dir_all(dir)?;
    }

    let manifest = read_manifest(from)?;
    let mut segments = 0u64;
    let mut head_tx_id = 0u64;
    for shard in &manifest.shards {
        if !shard.checkpoint_file.is_empty() {
            let pack = unpack_checkpoint_bytes(&fs::read(from.join(&shard.checkpoint_file))?)?;
            let objects_dir = dirs.checkpoint_dir.join("objects");
            fs::create_dir_all(&objects_dir)?;
            for (name, bytes) in &pack.objects {
                write_file(&objects_dir.join(name), bytes)?;
            }
            // Objects land before the manifest, mirroring the two-phase
            // checkpoint write: a torn restore never leaves a manifest whose
            // objects are missing.
            write_file(
                &dirs.checkpoint_dir.join(&pack.manifest_name),
                &pack.manifest,
            )?;
            head_tx_id = head_tx_id.max(shard.checkpoint_last_tx_id);
        }
        for entry in &shard.segments {
            let stored = fs::read(from.join(&entry.file))?;
            let raw = decompress_artifact(&stored, None)?;
            let name = entry
                .file
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_suffix(".zst"))
                .ok_or_else(|| {
                    FluxumError::Storage(format!("segment entry `{}` has no .zst name", entry.file))
                })?;
            write_file(&dirs.log_dir.join(name), &raw)?;
            segments += 1;
            head_tx_id = head_tx_id.max(entry.last_tx_id);
        }
    }
    sync_dir(&dirs.checkpoint_dir)?;
    sync_dir(&dirs.log_dir)?;
    Ok(RestoreReport {
        shards: u32::try_from(manifest.shards.len()).unwrap_or(u32::MAX),
        segments,
        head_tx_id,
    })
}

// --- PITR (REP-070/071/072) --------------------------------------------------------

/// Restore to a point in time (REP-070): restore the base backup, extend the
/// `tx_id` chain with archived segments from `archive_dir` (REP-062), then
/// cut the restored log at the inclusive `target` boundary — whole
/// `TxRecord`s only (REP-071) — and write the REP-072 lineage marker.
///
/// # Errors
/// Everything [`restore`] can fail with; a chain gap before the target
/// (reported with the covered range); a target before the backup's
/// checkpoint; or no transaction at or before the target.
pub fn pitr(
    from: &Path,
    dirs: &RestoreDirs,
    archive_dir: Option<&Path>,
    target: PitrTarget,
    force: bool,
) -> Result<PitrReport> {
    restore(from, dirs, force)?;
    let manifest = read_manifest(from)?;

    let mut last_tx = 0u64;
    let mut last_timestamp = 0i64;
    let mut max_epoch = 0u64;
    for shard in &manifest.shards {
        // PITR only rolls FORWARD from the base: a target before the
        // backup's checkpoint cannot be reproduced from this backup — the
        // checkpoint state already exceeds it, and replay cannot un-apply.
        if let PitrTarget::TxId(n) = target
            && n < shard.checkpoint_last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "the PITR target tx {n} precedes this backup's checkpoint (covers through \
                 tx {}): replay can only roll forward from the base — restore from an \
                 earlier backup instead (REP-070)",
                shard.checkpoint_last_tx_id
            )));
        }
        // Extend the restored chain with archived segments that continue it.
        let mut chain_end = shard
            .segments
            .last()
            .map_or(shard.checkpoint_last_tx_id, |s| s.last_tx_id);
        if let Some(archive) = archive_dir
            && archive.exists()
        {
            for segment in list_segments(archive, shard.shard_id)? {
                // A continuation either starts right after the chain end, or
                // is the SAME segment the backup cut mid-file — the archived
                // copy is then a byte-superset of the restored prefix (the
                // log is append-only and archival copies byte-identically),
                // and its tail holds the transactions the backup missed.
                if segment.first_tx_id > chain_end + 1 {
                    continue; // gapped copy — the stranded check below reports it
                }
                let name = segment
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();
                let dest = dirs.log_dir.join(name);
                let extend = if dest.exists() {
                    fs::metadata(&segment.path)?.len() > fs::metadata(&dest)?.len()
                } else {
                    segment.first_tx_id == chain_end + 1
                };
                if !extend {
                    continue; // fully covered by what is already restored
                }
                fs::copy(&segment.path, &dest)?;
                let mut seg_last = chain_end;
                let outcome = scan_segment_bytes(
                    &fs::read(&dest)?,
                    shard.shard_id,
                    None,
                    0,
                    &mut |_, record: TxRecord| {
                        seg_last = seg_last.max(record.tx_id);
                        Ok(())
                    },
                )?;
                if let ScanOutcome::Scanned(scan) = outcome
                    && let Some(fault) = scan.fault
                {
                    return Err(FluxumError::Storage(format!(
                        "archived segment {name}: invalid entry at byte {}: {}",
                        fault.offset, fault.reason
                    )));
                }
                chain_end = chain_end.max(seg_last);
            }
        }

        // Walk the (now extended) restored chain and cut at the target.
        let (shard_last, shard_ts, shard_epoch, cut_fired) =
            cut_at_target(&dirs.log_dir, shard, target)?;

        // The timestamp flavor of the roll-forward guard: if the cut left
        // less history than the checkpoint already covers, the target lies
        // before the base state. (Conservative when the restored log holds
        // no records at or below the checkpoint boundary to compare against
        // — use a tx-id target or an earlier backup there.)
        if cut_fired && shard.checkpoint_last_tx_id > 0 && shard_last < shard.checkpoint_last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "the PITR timestamp target precedes this backup's checkpoint (covers through \
                 tx {}): replay can only roll forward from the base — restore from an earlier \
                 backup instead (REP-070)",
                shard.checkpoint_last_tx_id
            )));
        }

        // REP-071: never stop early silently. If the cut never fired (the
        // target lies past everything applied) and the archive holds
        // segments that could NOT be chained (a missing segment in between),
        // that is a gap — fail with the covered range.
        if !cut_fired
            && let Some(archive) = archive_dir
            && archive.exists()
        {
            let stranded = list_segments(archive, shard.shard_id)?
                .into_iter()
                .map(|s| s.first_tx_id)
                .filter(|first| *first > chain_end + 1)
                .min();
            if let Some(next_available) = stranded {
                let target_past_chain = match target {
                    PitrTarget::TxId(n) => n > chain_end,
                    // A timestamp target that never fired the cut may lie in
                    // the stranded suffix — the same missing-segment story.
                    PitrTarget::TimestampMicros(_) => true,
                };
                if target_past_chain {
                    return Err(FluxumError::Storage(format!(
                        "archive chain gap before the PITR target: transactions {} through \
                         {chain_end} are covered, but the next archived segment starts at \
                         {next_available} (REP-071 — the restore does not silently stop early)",
                        shard.checkpoint_last_tx_id + 1,
                    )));
                }
            }
        }

        last_tx = last_tx.max(shard_last);
        if shard_last > 0 {
            last_timestamp = last_timestamp.max(shard_ts);
        }
        max_epoch = max_epoch.max(shard_epoch);
    }

    if last_tx == 0 {
        return Err(FluxumError::Storage(
            "PITR target precedes every transaction in the backup — nothing to restore".into(),
        ));
    }

    // REP-072: the restored node has forked history. Record the minimum
    // fencing epoch the next boot must adopt — strictly greater than any
    // epoch in the restored log — so its lineage is distinguishable from the
    // one it forked from.
    let fork_min_epoch = max_epoch + 1;
    let marker = rmp_serde::to_vec(&(fork_min_epoch, last_tx, last_timestamp))
        .map_err(|e| FluxumError::Storage(format!("lineage marker encoding failed: {e}")))?;
    write_file(&dirs.log_dir.join(LINEAGE_FILE), &marker)?;
    sync_dir(&dirs.log_dir)?;

    Ok(PitrReport {
        last_tx_id: last_tx,
        last_timestamp,
        fork_min_epoch,
    })
}

/// Whether `record` is within the inclusive PITR target.
fn within(target: PitrTarget, record: &TxRecord) -> bool {
    match target {
        PitrTarget::TxId(n) => record.tx_id <= n,
        PitrTarget::TimestampMicros(t) => record.timestamp <= t,
    }
}

/// Truncate the restored log of one shard at the target: scan segments in
/// order, keep whole entries `<= target` (byte-prefix truncation at a
/// validated frame boundary — every surviving per-entry CRC still holds),
/// delete everything past the cut. Fails on a chain gap before the target
/// with the covered range (REP-071). The fourth return is whether the cut
/// actually fired — `false` means the target lies at or past the chain head.
fn cut_at_target(
    log_dir: &Path,
    shard: &ShardBackup,
    target: PitrTarget,
) -> Result<(u64, i64, u64, bool)> {
    use crate::commitlog::format::{SEGMENT_HEADER_LEN, ScannedEntry, scan_entry};

    let mut last_applied = (0u64, 0i64);
    let mut max_epoch = 0u64;
    let mut expected_next = shard.checkpoint_last_tx_id + 1;
    let mut cut_done = false;

    for segment in list_segments(log_dir, shard.shard_id)? {
        if cut_done {
            // Everything past the cut leaves the restored log entirely.
            fs::remove_file(&segment.path)?;
            continue;
        }
        if segment.first_tx_id > expected_next {
            return Err(FluxumError::Storage(format!(
                "archive chain gap before the PITR target: transactions {} through {} are \
                 covered, but the next segment starts at {} (REP-071 — the restore does not \
                 silently stop early)",
                shard.checkpoint_last_tx_id + 1,
                expected_next - 1,
                segment.first_tx_id
            )));
        }
        let bytes = fs::read(&segment.path)?;
        let mut offset = SEGMENT_HEADER_LEN;
        let mut segment_last: Option<u64> = None;
        let mut cut_offset: Option<u64> = None;
        loop {
            match scan_entry(&bytes, offset) {
                ScannedEntry::Entry { epoch, body, end } => {
                    let record = TxRecord::decode(body).map_err(FluxumError::Storage)?;
                    if !within(target, &record) {
                        // First entry past the target: the cut is right here.
                        cut_offset = Some(offset as u64);
                        break;
                    }
                    last_applied = (record.tx_id, record.timestamp);
                    max_epoch = max_epoch.max(epoch);
                    segment_last = Some(record.tx_id);
                    offset = end;
                }
                ScannedEntry::CleanEof => break,
                // Backup segments were verified by restore(); an archived
                // copy's invalid tail simply ends the usable chain here.
                ScannedEntry::Torn(_) | ScannedEntry::Corrupt(_) => break,
            }
        }
        match cut_offset {
            // Nothing in this segment is within the target: drop it whole.
            Some(cut) if cut == SEGMENT_HEADER_LEN as u64 => {
                fs::remove_file(&segment.path)?;
                cut_done = true;
            }
            Some(cut) => {
                let file = fs::OpenOptions::new().write(true).open(&segment.path)?;
                file.set_len(cut)?;
                file.sync_data()?;
                cut_done = true;
            }
            None => {
                if let Some(last) = segment_last {
                    expected_next = last + 1;
                }
            }
        }
    }
    Ok((last_applied.0, last_applied.1, max_epoch, cut_done))
}

/// The REP-072 lineage marker's minimum epoch, if `log_dir` was produced by
/// a PITR restore. The server assembly opens the commit log with an epoch
/// `>= max(this, 1)` so the forked lineage is distinguishable.
///
/// # Errors
/// An unreadable or undecodable marker file (a missing file is `Ok(None)`).
pub fn pitr_lineage_min_epoch(log_dir: &Path) -> Result<Option<u64>> {
    let path = log_dir.join(LINEAGE_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let (min_epoch, _last_tx, _ts): (u64, u64, i64) = rmp_serde::from_slice(&fs::read(&path)?)
        .map_err(|e| FluxumError::Storage(format!("pitr.lineage decode failed: {e}")))?;
    Ok(Some(min_epoch))
}

// --- shared helpers ---------------------------------------------------------------

/// Read and decode `manifest.mpack`.
fn read_manifest(from: &Path) -> Result<BackupManifest> {
    let path = from.join(MANIFEST_FILE);
    let bytes = fs::read(&path)
        .map_err(|e| FluxumError::Storage(format!("cannot read {}: {e}", path.display())))?;
    let manifest: BackupManifest = rmp_serde::from_slice(&bytes)
        .map_err(|e| FluxumError::Storage(format!("backup manifest decode failed: {e}")))?;
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(FluxumError::Storage(format!(
            "unsupported backup format version {} (supported: {BACKUP_FORMAT_VERSION})",
            manifest.format_version
        )));
    }
    Ok(manifest)
}

/// Decode a checkpoint pack.
fn unpack_checkpoint_bytes(bytes: &[u8]) -> Result<CheckpointPack> {
    rmp_serde::from_slice(bytes)
        .map_err(|e| FluxumError::Storage(format!("checkpoint pack decode failed: {e}")))
}

/// Write a file durably (content + fsync; the caller syncs the directory).
fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
