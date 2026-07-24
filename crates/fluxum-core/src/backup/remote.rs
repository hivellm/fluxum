//! Object-storage backup & archive (SPEC-025 OPS-010/011; FR-139): push a
//! hot backup to an [`ArtifactStore`], restore and PITR from it, and the
//! incremental archiver the checkpoint worker drives.
//!
//! Everything content-bearing is **content-addressed**: artifact bytes live
//! under `<prefix>/objects/<sha256>`, so incremental archival is a HEAD
//! probe (existence == already uploaded, OPS-011) and every download is
//! integrity-verified by re-hashing against the manifest's recorded hash —
//! a flipped bit fails the fetch with the artifact named, never a silent
//! adoption.
//!
//! Segments are stored in the seekable-zstd framing ([`super::seekable`]):
//! PITR fetches whole segments only up to the one containing the target,
//! and for THAT segment range-reads the index tail plus exactly the blocks
//! covering the cut window (OPS-010).
//!
//! Key layout under the configured prefix:
//!
//! ```text
//! <prefix>/objects/<sha256>                     artifact bytes (pack / seekable segment)
//! <prefix>/manifests/<created_at>-<backup_id>   immutable RemoteManifest (MessagePack)
//! <prefix>/latest                               the newest manifest, re-put on each push
//! ```

use std::fs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{FluxumError, Result};
use crate::store::pager::codec::{DEFAULT_ARTIFACT_ZSTD_LEVEL, decompress_artifact};

use super::seekable;
use super::store::ArtifactStore;
use super::{BackupSource, PitrReport, PitrTarget, RestoreDirs, RestoreReport};

/// Remote manifest format version.
pub const REMOTE_FORMAT_VERSION: u32 = 1;

/// The remote backup manifest (MessagePack, positional — tail-additive only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteManifest {
    /// Manifest format version ([`REMOTE_FORMAT_VERSION`]).
    pub format_version: u32,
    /// The push's unique id (UUID v4).
    pub backup_id: String,
    /// Microseconds since the Unix epoch at push time.
    pub created_at: i64,
    /// The module schema version captured (0 = none).
    pub schema_version: u32,
    /// One entry per shard.
    pub shards: Vec<RemoteShard>,
}

/// One shard's slice of a remote backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteShard {
    /// The shard.
    pub shard_id: u32,
    /// Content key of the packed checkpoint (`""` = no checkpoint).
    pub checkpoint_key: String,
    /// SHA-256 (hex) of the packed checkpoint bytes.
    pub checkpoint_sha256: String,
    /// The checkpoint's covering tx id (0 = none).
    pub checkpoint_last_tx_id: u64,
    /// Covering segments, ascending by `first_tx_id`.
    pub segments: Vec<RemoteSegment>,
}

/// One segment artifact in a remote backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSegment {
    /// Content key of the seekable artifact.
    pub key: String,
    /// SHA-256 (hex) of the STORED (seekable) artifact bytes.
    pub sha256: String,
    /// The segment's original file name (restored verbatim).
    pub file_name: String,
    /// First transaction id in the captured prefix.
    pub first_tx_id: u64,
    /// Last transaction id in the captured prefix.
    pub last_tx_id: u64,
    /// Raw (uncompressed) length of the captured prefix.
    pub raw_len: u64,
    /// Stored (seekable artifact) length.
    pub stored_len: u64,
    /// First record's commit timestamp (µs) — lets a timestamp-target PITR
    /// locate the cut segment without fetching (OPS-010).
    pub first_timestamp: i64,
    /// Last record's commit timestamp (µs).
    pub last_timestamp: i64,
}

/// What a push uploaded (OPS-011 observability: incremental is visible).
#[derive(Debug, Clone)]
pub struct PushReport {
    /// The manifest's backup id.
    pub backup_id: String,
    /// The immutable manifest key written.
    pub manifest_key: String,
    /// Artifacts uploaded by this push.
    pub uploaded: u64,
    /// Artifacts already present (content-addressed skip).
    pub skipped: u64,
    /// Bytes uploaded.
    pub bytes_uploaded: u64,
    /// Highest tx id captured.
    pub head_tx_id: u64,
}

/// Range-read accounting for a remote PITR (the OPS-010 economy is
/// observable and therefore testable).
#[derive(Debug, Clone, Default)]
pub struct RangeStats {
    /// Bytes fetched from the target (cut) segment, index tail included.
    pub target_segment_bytes_fetched: u64,
    /// The target segment's full stored length — what a non-seekable fetch
    /// would have transferred.
    pub target_segment_stored_len: u64,
}

fn sha_hex(bytes: &[u8]) -> String {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn object_key(prefix: &str, sha: &str) -> String {
    format!("{prefix}/objects/{sha}")
}

/// Upload `bytes` content-addressed; `Ok(true)` = uploaded, `Ok(false)` =
/// already present (OPS-011 incremental).
fn put_object(store: &dyn ArtifactStore, prefix: &str, sha: &str, bytes: &[u8]) -> Result<bool> {
    let key = object_key(prefix, sha);
    if store.head(&key)?.is_some() {
        return Ok(false);
    }
    store.put(&key, bytes)?;
    Ok(true)
}

/// Fetch an object and verify its content hash (OPS-011): a mismatch names
/// the key and both hashes — corruption is never silently adopted.
fn get_verified(store: &dyn ArtifactStore, key: &str, want_sha: &str) -> Result<Vec<u8>> {
    let bytes = store.get(key)?;
    let got = sha_hex(&bytes);
    if got != want_sha {
        return Err(FluxumError::Storage(format!(
            "integrity failure on `{key}`: downloaded bytes hash {got}, manifest says {want_sha} \
             (OPS-011 — refusing the artifact)"
        )));
    }
    Ok(bytes)
}

// --- push (OPS-010/011) -----------------------------------------------------------

/// Push a hot backup of `source` to the store under `prefix`: the local
/// [`super::create`] runs into a staging directory (the same validated-prefix
/// hot capture), then every artifact uploads content-addressed — checkpoint
/// packs as-is, segments re-framed as seekable-zstd — and the manifest lands
/// last (`latest` is only moved once everything it names is durable).
///
/// # Errors
/// Anything the local create can fail with, plus transport failures.
pub fn push(source: &BackupSource, store: &dyn ArtifactStore, prefix: &str) -> Result<PushReport> {
    push_with(source, store, prefix, seekable::DEFAULT_BLOCK_RAW_BYTES)
}

/// [`push`] with an explicit seekable block size — the range-read economy
/// scales with it, and tests pin the economy with small blocks.
///
/// # Errors
/// As [`push`].
pub fn push_with(
    source: &BackupSource,
    store: &dyn ArtifactStore,
    prefix: &str,
    block_raw_bytes: usize,
) -> Result<PushReport> {
    let staging = tempfile::tempdir().map_err(FluxumError::Io)?;
    let local = super::create(source, staging.path())?;
    let manifest_bytes = fs::read(&local.manifest_path)?;
    let local_manifest: super::BackupManifest = rmp_serde::from_slice(&manifest_bytes)
        .map_err(|e| FluxumError::Storage(format!("staging manifest decode failed: {e}")))?;

    let mut uploaded = 0u64;
    let mut skipped = 0u64;
    let mut bytes_uploaded = 0u64;
    let mut track = |fresh: bool, len: usize| {
        if fresh {
            uploaded += 1;
            bytes_uploaded += len as u64;
        } else {
            skipped += 1;
        }
    };

    let mut shards = Vec::new();
    for shard in &local_manifest.shards {
        let (checkpoint_key, checkpoint_sha256) = if shard.checkpoint_file.is_empty() {
            (String::new(), String::new())
        } else {
            let bytes = fs::read(staging.path().join(&shard.checkpoint_file))?;
            let sha = sha_hex(&bytes);
            track(put_object(store, prefix, &sha, &bytes)?, bytes.len());
            (object_key(prefix, &sha), sha)
        };
        let mut segments = Vec::new();
        for entry in &shard.segments {
            let stored = fs::read(staging.path().join(&entry.file))?;
            // The staging artifact is a single zstd frame; the remote form
            // is seekable so PITR can range-read it (OPS-010).
            let raw = decompress_artifact(&stored, None)?;
            // Record the segment's timestamp range so a timestamp-target
            // PITR can locate the cut segment from the manifest alone.
            let mut first_ts = 0i64;
            let mut last_ts = 0i64;
            crate::commitlog::segment::scan_segment_bytes(
                &raw,
                shard.shard_id,
                None,
                0,
                &mut |_, record: crate::commitlog::record::TxRecord| {
                    if first_ts == 0 {
                        first_ts = record.timestamp;
                    }
                    last_ts = record.timestamp;
                    Ok(())
                },
            )?;
            let artifact = seekable::encode(&raw, block_raw_bytes, DEFAULT_ARTIFACT_ZSTD_LEVEL)?;
            let sha = sha_hex(&artifact);
            track(put_object(store, prefix, &sha, &artifact)?, artifact.len());
            let file_name = entry
                .file
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_suffix(".zst"))
                .unwrap_or(&entry.file)
                .to_owned();
            segments.push(RemoteSegment {
                key: object_key(prefix, &sha),
                sha256: sha,
                file_name,
                first_tx_id: entry.first_tx_id,
                last_tx_id: entry.last_tx_id,
                raw_len: raw.len() as u64,
                stored_len: artifact.len() as u64,
                first_timestamp: first_ts,
                last_timestamp: last_ts,
            });
        }
        shards.push(RemoteShard {
            shard_id: shard.shard_id,
            checkpoint_key,
            checkpoint_sha256,
            checkpoint_last_tx_id: shard.checkpoint_last_tx_id,
            segments,
        });
    }

    let manifest = RemoteManifest {
        format_version: REMOTE_FORMAT_VERSION,
        backup_id: local_manifest.backup_id.clone(),
        created_at: local_manifest.created_at,
        schema_version: local_manifest.schema_version,
        shards,
    };
    let encoded = rmp_serde::to_vec(&manifest)
        .map_err(|e| FluxumError::Storage(format!("remote manifest encoding failed: {e}")))?;
    let manifest_key = format!(
        "{prefix}/manifests/{:020}-{}",
        manifest.created_at, manifest.backup_id
    );
    store.put(&manifest_key, &encoded)?;
    store.put(&format!("{prefix}/latest"), &encoded)?;
    Ok(PushReport {
        backup_id: manifest.backup_id,
        manifest_key,
        uploaded,
        skipped,
        bytes_uploaded,
        head_tx_id: local.head_tx_id,
    })
}

/// Fetch and decode the newest manifest under `prefix`.
///
/// # Errors
/// A missing `latest`, transport failures, or an undecodable manifest.
pub fn latest_manifest(store: &dyn ArtifactStore, prefix: &str) -> Result<RemoteManifest> {
    let bytes = store.get(&format!("{prefix}/latest"))?;
    let manifest: RemoteManifest = rmp_serde::from_slice(&bytes)
        .map_err(|e| FluxumError::Storage(format!("remote manifest decode failed: {e}")))?;
    if manifest.format_version != REMOTE_FORMAT_VERSION {
        return Err(FluxumError::Storage(format!(
            "unsupported remote manifest version {} (supported: {REMOTE_FORMAT_VERSION})",
            manifest.format_version
        )));
    }
    Ok(manifest)
}

// --- restore (OPS-010/011) --------------------------------------------------------

/// Restore the newest remote backup into `dirs`: every artifact download is
/// re-hashed against the manifest (OPS-011) before adoption; segments are
/// decoded from their seekable framing back to raw log files. The normal
/// STG-030 recovery reconstructs state on next boot.
///
/// # Errors
/// Integrity failures (named per artifact), non-empty targets without
/// `force`, transport failures.
pub fn restore(
    store: &dyn ArtifactStore,
    prefix: &str,
    dirs: &RestoreDirs,
    force: bool,
) -> Result<RestoreReport> {
    let manifest = latest_manifest(store, prefix)?;
    for dir in [&dirs.checkpoint_dir, &dirs.log_dir] {
        if !force && dir.exists() && fs::read_dir(dir)?.next().is_some() {
            return Err(FluxumError::Storage(format!(
                "target directory {} is not empty; pass --force to restore into it anyway",
                dir.display()
            )));
        }
        fs::create_dir_all(dir)?;
    }
    let mut segments = 0u64;
    let mut head_tx_id = 0u64;
    for shard in &manifest.shards {
        if !shard.checkpoint_key.is_empty() {
            let bytes = get_verified(store, &shard.checkpoint_key, &shard.checkpoint_sha256)?;
            let pack = super::unpack_checkpoint_bytes(&bytes)?;
            let objects_dir = dirs.checkpoint_dir.join("objects");
            fs::create_dir_all(&objects_dir)?;
            for (name, object) in &pack.objects {
                super::write_file(&objects_dir.join(name), object)?;
            }
            super::write_file(
                &dirs.checkpoint_dir.join(&pack.manifest_name),
                &pack.manifest,
            )?;
            head_tx_id = head_tx_id.max(shard.checkpoint_last_tx_id);
        }
        for segment in &shard.segments {
            let artifact = get_verified(store, &segment.key, &segment.sha256)?;
            let raw = seekable::decode_all(&artifact)?;
            super::write_file(&dirs.log_dir.join(&segment.file_name), &raw)?;
            segments += 1;
            head_tx_id = head_tx_id.max(segment.last_tx_id);
        }
    }
    crate::commitlog::segment::sync_dir(&dirs.log_dir)?;
    Ok(RestoreReport {
        shards: u32::try_from(manifest.shards.len()).unwrap_or(u32::MAX),
        segments,
        head_tx_id,
    })
}

// --- PITR (OPS-010: range-read the target segment) --------------------------------

/// PITR from the newest remote backup: whole segments are fetched only up
/// to the one containing the target; the target segment is range-read —
/// index tail first, then exactly the blocks covering the cut window — and
/// the standard cut/lineage machinery finishes locally (REP-070..072
/// semantics, including the roll-forward guard and the gap refusal).
///
/// # Errors
/// Everything [`restore`] can fail with, plus a target preceding the base
/// checkpoint, a chain gap, or a target before all history.
pub fn pitr(
    store: &dyn ArtifactStore,
    prefix: &str,
    dirs: &RestoreDirs,
    target: PitrTarget,
    force: bool,
) -> Result<(PitrReport, RangeStats)> {
    let manifest = latest_manifest(store, prefix)?;
    for dir in [&dirs.checkpoint_dir, &dirs.log_dir] {
        if !force && dir.exists() && fs::read_dir(dir)?.next().is_some() {
            return Err(FluxumError::Storage(format!(
                "target directory {} is not empty; pass --force to restore into it anyway",
                dir.display()
            )));
        }
        fs::create_dir_all(dir)?;
    }

    let mut last_tx = 0u64;
    let mut last_timestamp = 0i64;
    let mut max_epoch = 0u64;
    let mut stats = RangeStats::default();

    for shard in &manifest.shards {
        // Roll-forward guard (REP-070): a tx-id target before the base
        // checkpoint cannot be reproduced from this backup.
        if let PitrTarget::TxId(n) = target
            && n < shard.checkpoint_last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "the PITR target tx {n} precedes this backup's checkpoint (covers through \
                 tx {}): restore from an earlier backup instead (REP-070)",
                shard.checkpoint_last_tx_id
            )));
        }
        if !shard.checkpoint_key.is_empty() {
            let bytes = get_verified(store, &shard.checkpoint_key, &shard.checkpoint_sha256)?;
            let pack = super::unpack_checkpoint_bytes(&bytes)?;
            let objects_dir = dirs.checkpoint_dir.join("objects");
            fs::create_dir_all(&objects_dir)?;
            for (name, object) in &pack.objects {
                super::write_file(&objects_dir.join(name), object)?;
            }
            super::write_file(
                &dirs.checkpoint_dir.join(&pack.manifest_name),
                &pack.manifest,
            )?;
        }

        // Chain check up front (REP-071): the manifest knows every range.
        let mut expected_next = shard.checkpoint_last_tx_id + 1;
        for segment in &shard.segments {
            if segment.first_tx_id > expected_next {
                return Err(FluxumError::Storage(format!(
                    "archive chain gap before the PITR target: transactions {} through {} \
                     are covered, but the next remote segment starts at {} (REP-071)",
                    shard.checkpoint_last_tx_id + 1,
                    expected_next - 1,
                    segment.first_tx_id
                )));
            }
            expected_next = segment.last_tx_id + 1;
        }

        // Which segment holds the cut? For a tx target the manifest answers
        // directly; a timestamp target is resolved by scanning fetched
        // blocks (the scan stops the fetch as soon as the cut is passed).
        let cut_index = match target {
            PitrTarget::TxId(n) => shard
                .segments
                .iter()
                .position(|s| s.first_tx_id <= n && n <= s.last_tx_id),
            // The first segment whose recorded range passes the target holds
            // the cut; the local whole-record cut stays the authority.
            PitrTarget::TimestampMicros(t) => {
                shard.segments.iter().position(|s| s.last_timestamp > t)
            }
        };

        for (i, segment) in shard.segments.iter().enumerate() {
            let is_cut_segment = cut_index == Some(i);
            let past_cut = cut_index.is_some_and(|cut| i > cut);
            if past_cut {
                break; // never fetched at all
            }
            if is_cut_segment {
                let raw_prefix = fetch_cut_prefix(store, segment, target, &mut stats)?;
                super::write_file(&dirs.log_dir.join(&segment.file_name), &raw_prefix)?;
            } else {
                let artifact = get_verified(store, &segment.key, &segment.sha256)?;
                let raw = seekable::decode_all(&artifact)?;
                super::write_file(&dirs.log_dir.join(&segment.file_name), &raw)?;
            }
        }

        // The standard local cut + guards finish the job (whole TxRecords
        // only; boundary reported; timestamp roll-forward guard).
        let pseudo = super::ShardBackup {
            shard_id: shard.shard_id,
            checkpoint_file: String::new(),
            checkpoint_last_tx_id: shard.checkpoint_last_tx_id,
            segments: Vec::new(),
            checkpoint_crc32: 0,
        };
        let (shard_last, shard_ts, shard_epoch, cut_fired) =
            super::cut_at_target(&dirs.log_dir, &pseudo, target)?;
        if cut_fired && shard.checkpoint_last_tx_id > 0 && shard_last < shard.checkpoint_last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "the PITR timestamp target precedes this backup's checkpoint (covers through \
                 tx {}): restore from an earlier backup instead (REP-070)",
                shard.checkpoint_last_tx_id
            )));
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
    let fork_min_epoch = max_epoch + 1;
    let marker = rmp_serde::to_vec(&(fork_min_epoch, last_tx, last_timestamp))
        .map_err(|e| FluxumError::Storage(format!("lineage marker encoding failed: {e}")))?;
    super::write_file(&dirs.log_dir.join(super::LINEAGE_FILE), &marker)?;
    crate::commitlog::segment::sync_dir(&dirs.log_dir)?;
    Ok((
        PitrReport {
            last_tx_id: last_tx,
            last_timestamp,
            fork_min_epoch,
        },
        stats,
    ))
}

/// Fetch only the raw prefix of the cut segment that can contain entries
/// `<= target`: the index tail plus blocks in order, stopping as soon as a
/// fetched block's entries pass the target (OPS-010's range-read economy).
/// The returned bytes still end with entries past the target inside the
/// final block — the local cut truncates at the exact frame boundary.
fn fetch_cut_prefix(
    store: &dyn ArtifactStore,
    segment: &RemoteSegment,
    target: PitrTarget,
    stats: &mut RangeStats,
) -> Result<Vec<u8>> {
    stats.target_segment_stored_len = segment.stored_len;
    // Two tiny reads instead of a blind tail hint: the fixed-size trailer
    // (index_len + magic, 12 bytes) names the exact index size, so the
    // second read fetches precisely the index — the economy holds even for
    // artifacts smaller than any fixed hint.
    if segment.stored_len < 12 {
        return Err(FluxumError::Storage(format!(
            "remote segment `{}` is shorter than the seekable trailer",
            segment.key
        )));
    }
    let trailer = store.get_range(&segment.key, segment.stored_len - 12, 12)?;
    stats.target_segment_bytes_fetched += trailer.len() as u64;
    if trailer.len() != 12 || &trailer[4..] != seekable::INDEX_MAGIC {
        return Err(FluxumError::Storage(format!(
            "remote segment `{}`: bad seekable trailer",
            segment.key
        )));
    }
    let index_len = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let tail_len = (index_len + 12).min(segment.stored_len);
    let tail = store.get_range(&segment.key, segment.stored_len - tail_len, tail_len)?;
    stats.target_segment_bytes_fetched += tail.len() as u64;
    let index = seekable::parse_index(&tail, segment.stored_len)?;

    let mut raw = Vec::new();
    for block in &index {
        let frame = store.get_range(&segment.key, block.comp_off, block.comp_len)?;
        stats.target_segment_bytes_fetched += frame.len() as u64;
        raw.extend_from_slice(&seekable::decode_block(&frame)?);
        // Stop fetching once an entry past the target has landed: the local
        // cut discards the tail of this block.
        if block_passes_target(&raw, target) {
            break;
        }
    }
    Ok(raw)
}

/// Whether the raw prefix already contains a whole entry past the target.
fn block_passes_target(raw: &[u8], target: PitrTarget) -> bool {
    use crate::commitlog::format::{SEGMENT_HEADER_LEN, ScannedEntry, scan_entry};
    let mut offset = SEGMENT_HEADER_LEN;
    loop {
        match scan_entry(raw, offset) {
            ScannedEntry::Entry { body, end, .. } => {
                if let Ok(record) = crate::commitlog::record::TxRecord::decode(body)
                    && !super::within(target, &record)
                {
                    return true;
                }
                offset = end;
            }
            _ => return false,
        }
    }
}

// --- the scheduled incremental archiver (OPS-011) ---------------------------------

/// The checkpoint worker's remote sync (OPS-011): after each checkpoint +
/// compaction pass, upload — content-addressed, existence-probed — anything
/// new: checkpoint objects, checkpoint manifests, and freshly archived
/// segments (seekable-framed). Runs on the worker's own OS thread; writers
/// never wait on it.
pub struct RemoteArchiver {
    store: Arc<dyn ArtifactStore>,
    prefix: String,
}

impl std::fmt::Debug for RemoteArchiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteArchiver")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// What one incremental sync pass moved (OPS-011).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Artifacts uploaded this pass.
    pub uploaded: u64,
    /// Artifacts already present remotely.
    pub skipped: u64,
}

impl RemoteArchiver {
    /// Build an archiver over `store`, keys under `prefix`.
    pub fn new(store: Arc<dyn ArtifactStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// One incremental pass: checkpoint objects + manifests under
    /// `checkpoint_dir`, and archived segments under `archive_dir`, each
    /// uploaded only when absent remotely.
    ///
    /// # Errors
    /// Transport or I/O failures (the caller WARNs and retries next pass).
    pub fn sync(
        &self,
        checkpoint_dir: &std::path::Path,
        archive_dir: Option<&std::path::Path>,
        shard_id: u32,
    ) -> Result<SyncReport> {
        let mut report = SyncReport::default();
        // Checkpoint objects are already content-addressed on disk: their
        // file name IS the hash of their stored bytes.
        let objects = checkpoint_dir.join("objects");
        if objects.is_dir() {
            for entry in fs::read_dir(&objects)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                    continue; // temp files are not artifacts
                }
                let key = object_key(&self.prefix, &name);
                if self.store.head(&key)?.is_some() {
                    report.skipped += 1;
                } else {
                    self.store.put(&key, &fs::read(entry.path())?)?;
                    report.uploaded += 1;
                }
            }
        }
        // Checkpoint manifests: small, named by (shard, tx) — stable keys.
        if checkpoint_dir.is_dir() {
            for entry in fs::read_dir(checkpoint_dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.ends_with(".manifest") {
                    continue;
                }
                let key = format!("{}/checkpoints/{name}", self.prefix);
                if self.store.head(&key)?.is_some() {
                    report.skipped += 1;
                } else {
                    self.store.put(&key, &fs::read(entry.path())?)?;
                    report.uploaded += 1;
                }
            }
        }
        // Freshly archived segments: seekable-framed under a stable name,
        // existence-probed so a re-pass is free.
        if let Some(archive) = archive_dir
            && archive.is_dir()
        {
            for segment in crate::commitlog::segment::list_segments(archive, shard_id)? {
                let name = segment
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();
                let key = format!("{}/archive/{name}.zseg", self.prefix);
                if self.store.head(&key)?.is_some() {
                    report.skipped += 1;
                    continue;
                }
                let raw = fs::read(&segment.path)?;
                let artifact = seekable::encode(
                    &raw,
                    seekable::DEFAULT_BLOCK_RAW_BYTES,
                    DEFAULT_ARTIFACT_ZSTD_LEVEL,
                )?;
                self.store.put(&key, &artifact)?;
                report.uploaded += 1;
            }
        }
        Ok(report)
    }
}
