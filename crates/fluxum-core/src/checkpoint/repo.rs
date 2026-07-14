//! [`CheckpointRepo`] — the on-disk checkpoint repository (SnapshotRepo,
//! STG-020/STG-021/STG-023): a content-addressed object store plus manifest
//! files, with retention pruning and replica-transfer pins.
//!
//! Layout under `snapshot_dir`:
//!
//! ```text
//! snapshot_dir/
//!   objects/<64-hex-sha256>          row-chunk objects, shared by hash
//!   ckpt-<shard:010>-<tx:020>.manifest
//! ```
//!
//! Objects unchanged since the previous checkpoint hash identically and are
//! never rewritten — a checkpoint costs only the changed objects (STG-021).
//! Creation is two-phase crash-safe: every object is written durably
//! (temp + fsync + rename) before the fsynced manifest lands as the commit
//! record.
//!
//! Manifests and objects are zstd-compressed on disk (SPEC-015 TIER-042,
//! T2.9; level = `storage.checkpoint_compression_level`, default 3) via the
//! shared artifact codec ([`crate::store::pager::codec`], reused by the
//! T7.3 backup archives). Object identity is the hash of the **stored**
//! (compressed) bytes, so on-disk hash verification needs no decompression;
//! artifacts are self-describing through the zstd frame magic, and raw
//! artifacts written before compression landed keep reading correctly.

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use crate::commitlog::record::{LogValue, row_to_log};
use crate::commitlog::segment::sync_dir;
use crate::error::{FluxumError, Result};
use crate::store::committed::Snapshot;
use crate::store::crc32;
use crate::store::pager::codec::{
    DEFAULT_ARTIFACT_ZSTD_LEVEL, compress_artifact, decompress_artifact,
};
use crate::store::row::Row;
use crate::types::Timestamp;

use super::manifest::{Manifest, ObjectHash, TableManifest, decode_manifest, encode_manifest};

/// Content-defined chunking: a row ends its chunk when
/// `crc32(pk) % CHUNK_TARGET_ROWS == 0`, so chunk membership depends only on
/// the keys present between boundary keys — inserting or deleting one row
/// re-chunks only its own neighborhood, never the whole table (the STG-021
/// "no full-dump scaling cliff" property with rows instead of SPEC-015
/// pages; the pager swaps physical pages under this same manifest scheme).
const CHUNK_TARGET_ROWS: u32 = 64;

/// Hard upper bound on rows per chunk (keeps the worst case bounded when a
/// key range happens to contain no boundary hash).
const CHUNK_MAX_ROWS: usize = 256;

/// What one checkpoint write did (STG-021 incrementality is observable:
/// `objects_written` stays proportional to the change set, not to database
/// size).
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    /// The checkpoint's covering transaction id.
    pub last_tx_id: u64,
    /// The manifest file written as the commit record.
    pub manifest: PathBuf,
    /// Objects referenced by the manifest.
    pub objects_total: u64,
    /// Objects newly written by this checkpoint.
    pub objects_written: u64,
    /// Objects shared with earlier checkpoints (already present by hash).
    pub objects_shared: u64,
    /// Bytes of newly written object data.
    pub bytes_written: u64,
}

/// One checkpoint on disk, identified by its manifest file.
#[derive(Debug, Clone)]
pub struct CheckpointRef {
    /// The manifest path.
    pub path: PathBuf,
    /// The shard component of the file name.
    pub shard_id: u32,
    /// The covering transaction id component of the file name.
    pub last_tx_id: u64,
}

/// A fully verified checkpoint, loaded into memory for restore (STG-030
/// step 2): every object hash was checked before adoption.
#[derive(Debug)]
pub struct LoadedCheckpoint {
    /// Every transaction `<= last_tx_id` is covered.
    pub last_tx_id: u64,
    /// Fencing epoch recorded at checkpoint time.
    pub epoch: u64,
    /// Per-table contents.
    pub tables: Vec<LoadedTable>,
}

/// One table restored from a checkpoint.
#[derive(Debug)]
pub struct LoadedTable {
    /// Stable table id recorded in the manifest.
    pub table_id: u32,
    /// Table name (schema resolution key).
    pub table_name: String,
    /// Durable auto-inc high-water mark to resume from (STG-040).
    pub auto_inc_high_water: u64,
    /// All rows, in encoded-PK order.
    pub rows: Vec<Row>,
}

/// The per-shard checkpoint repository (STG-020 `SnapshotRepo`).
#[derive(Debug)]
pub struct CheckpointRepo {
    dir: PathBuf,
    objects: PathBuf,
    /// zstd level for manifests and objects (TIER-042).
    zstd_level: i32,
    /// Replica-transfer pins (STG-023): checkpoints being transferred are
    /// never pruned until the transfer completes.
    pins: Mutex<HashSet<u64>>,
}

impl CheckpointRepo {
    /// Open (or create) the repository in `dir` (`snapshot_dir`, STG-020)
    /// at the default artifact compression level (TIER-042).
    pub fn open(dir: &Path) -> Result<Self> {
        let objects = dir.join("objects");
        fs::create_dir_all(&objects)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            objects,
            zstd_level: DEFAULT_ARTIFACT_ZSTD_LEVEL,
            pins: Mutex::new(HashSet::new()),
        })
    }

    /// Set the zstd level for newly written artifacts
    /// (`storage.checkpoint_compression_level`, TIER-042). Reading is
    /// level-agnostic; changing the level never invalidates existing
    /// checkpoints (unchanged chunks written at another level simply hash
    /// as new objects).
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    /// The repository directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write a checkpoint of `snapshot` covering every transaction
    /// `<= last_tx_id` (STG-020/STG-021).
    ///
    /// Non-blocking by construction (STG-022): the input is a lock-free
    /// [`Snapshot`] — no store lock is held for any part of the write, so
    /// reducer execution proceeds while objects hit disk. Two-phase
    /// crash-safe: objects first (each durable before the manifest names
    /// it), then the fsynced manifest as the commit record. Unchanged
    /// chunks are shared with earlier checkpoints by content hash, never
    /// rewritten. `last_tx_id` must strictly increase past the newest
    /// existing checkpoint (STG-020 cadence is monotone).
    pub fn write(
        &self,
        snapshot: &Snapshot,
        shard_id: u32,
        last_tx_id: u64,
        epoch: u64,
    ) -> Result<CheckpointStats> {
        if last_tx_id == 0 {
            return Err(FluxumError::Storage(
                "checkpoint last_tx_id must be >= 1 (there is no tx 0)".into(),
            ));
        }
        if let Some(newest) = self.list(shard_id)?.last()
            && last_tx_id <= newest.last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "checkpoint last_tx_id {last_tx_id} does not strictly increase past the \
                 newest checkpoint at {} (STG-020)",
                newest.last_tx_id
            )));
        }

        let mut stats = CheckpointStats {
            last_tx_id,
            manifest: manifest_path(&self.dir, shard_id, last_tx_id),
            objects_total: 0,
            objects_written: 0,
            objects_shared: 0,
            bytes_written: 0,
        };

        // Deterministic manifest order: tables ascend by id.
        let mut table_ids: Vec<_> = snapshot.state.tables.keys().copied().collect();
        table_ids.sort_unstable();

        let mut tables = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            let table = snapshot.state.table(table_id)?;
            let mut chunks = Vec::new();
            let mut current: Vec<Vec<LogValue>> = Vec::new();
            for (pk, row) in &table.rows {
                current.push(row_to_log(row));
                if current.len() >= CHUNK_MAX_ROWS
                    || crc32(pk.as_bytes()).is_multiple_of(CHUNK_TARGET_ROWS)
                {
                    chunks.push(self.write_chunk(&mut current, &mut stats)?);
                }
            }
            if !current.is_empty() {
                chunks.push(self.write_chunk(&mut current, &mut stats)?);
            }
            tables.push(TableManifest {
                table_id: table_id.as_u32(),
                table_name: table.schema.name.to_string(),
                auto_inc_high_water: table.auto_inc_high_water,
                row_count: table.rows.len() as u64,
                chunks,
            });
        }

        // Every object is durable; the fsynced manifest is the commit record.
        let manifest = Manifest {
            format_version: super::manifest::MANIFEST_VERSION,
            shard_id,
            last_tx_id,
            epoch,
            timestamp: Timestamp::now().as_micros(),
            tables,
        };
        let bytes = compress_artifact(&encode_manifest(&manifest)?, self.zstd_level)?;
        write_durable(&stats.manifest, &bytes)?;
        sync_dir(&self.dir)?;
        Ok(stats)
    }

    /// Encode, zstd-compress (TIER-042), and durably store one row chunk,
    /// sharing it by content hash when an identical object already exists
    /// (STG-021; the hash covers the stored/compressed bytes).
    fn write_chunk(
        &self,
        rows: &mut Vec<Vec<LogValue>>,
        stats: &mut CheckpointStats,
    ) -> Result<serde_bytes::ByteBuf> {
        let encoded = rmp_serde::to_vec(rows)
            .map_err(|e| FluxumError::Storage(format!("checkpoint chunk encoding failed: {e}")))?;
        let bytes = compress_artifact(&encoded, self.zstd_level)?;
        rows.clear();
        let hash = ObjectHash::of(&bytes);
        let path = self.objects.join(hash.to_string());
        stats.objects_total += 1;
        if path.exists() {
            stats.objects_shared += 1;
        } else {
            write_durable(&path, &bytes)?;
            sync_dir(&self.objects)?;
            stats.objects_written += 1;
            stats.bytes_written += bytes.len() as u64;
        }
        Ok(serde_bytes::ByteBuf::from(hash.as_bytes().to_vec()))
    }

    /// List this shard's checkpoints, ascending by `last_tx_id` (manifest
    /// files only — validity is established by [`CheckpointRepo::load`]).
    pub fn list(&self, shard_id: u32) -> Result<Vec<CheckpointRef>> {
        let mut refs = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(r) = parse_manifest_name(name, &entry.path())
                && r.shard_id == shard_id
            {
                refs.push(r);
            }
        }
        refs.sort_by_key(|r| r.last_tx_id);
        Ok(refs)
    }

    /// Load and fully verify one checkpoint: manifest magic + integrity hash,
    /// shard, name/id consistency, then every referenced object's content
    /// hash and the per-table row counts (STG-021: restore verifies the
    /// manifest hash and every object hash before adopting the checkpoint).
    pub fn load(&self, checkpoint: &CheckpointRef) -> Result<LoadedCheckpoint> {
        let manifest = decode_manifest(&fs::read(&checkpoint.path)?)?;
        if manifest.shard_id != checkpoint.shard_id || manifest.last_tx_id != checkpoint.last_tx_id
        {
            return Err(FluxumError::Storage(format!(
                "checkpoint manifest {}: body (shard {}, tx {}) disagrees with the file name",
                checkpoint.path.display(),
                manifest.shard_id,
                manifest.last_tx_id
            )));
        }
        let mut tables = Vec::with_capacity(manifest.tables.len());
        for table in &manifest.tables {
            let mut rows: Vec<Row> = Vec::new();
            for hash in table.chunk_hashes()? {
                let bytes = fs::read(self.objects.join(hash.to_string()))?;
                if ObjectHash::of(&bytes) != hash {
                    return Err(FluxumError::Storage(format!(
                        "checkpoint object {hash}: content hash mismatch"
                    )));
                }
                // Hash verified over the stored bytes; decompress after
                // (raw pre-compression objects pass through, TIER-042).
                let bytes = decompress_artifact(&bytes)
                    .map_err(|e| FluxumError::Storage(format!("checkpoint object {hash}: {e}")))?;
                let chunk: Vec<Vec<LogValue>> = rmp_serde::from_slice(&bytes).map_err(|e| {
                    FluxumError::Storage(format!("checkpoint object {hash}: decode failed: {e}"))
                })?;
                for values in &chunk {
                    let row = values
                        .iter()
                        .map(LogValue::to_row_value)
                        .collect::<Result<Vec<_>>>()?;
                    rows.push(Row::new(row));
                }
            }
            if rows.len() as u64 != table.row_count {
                return Err(FluxumError::Storage(format!(
                    "checkpoint table `{}`: {} rows restored but the manifest declares {}",
                    table.table_name,
                    rows.len(),
                    table.row_count
                )));
            }
            tables.push(LoadedTable {
                table_id: table.table_id,
                table_name: table.table_name.clone(),
                auto_inc_high_water: table.auto_inc_high_water,
                rows,
            });
        }
        Ok(LoadedCheckpoint {
            last_tx_id: manifest.last_tx_id,
            epoch: manifest.epoch,
            tables,
        })
    }

    /// The newest checkpoint whose manifest verifies, if any (cadence resume
    /// point for the [`super::SnapshotWorker`]; objects are only verified on
    /// [`CheckpointRepo::load`]).
    pub fn latest_verified_tx(&self, shard_id: u32) -> Result<Option<u64>> {
        for r in self.list(shard_id)?.iter().rev() {
            if fs::read(&r.path)
                .map_err(FluxumError::Io)
                .and_then(|bytes| decode_manifest(&bytes))
                .is_ok()
            {
                return Ok(Some(r.last_tx_id));
            }
        }
        Ok(None)
    }

    /// Pin a checkpoint (by `last_tx_id`) against pruning — e.g. while it
    /// seeds a new replica (STG-023: a checkpoint being transferred is never
    /// deleted until the transfer completes).
    pub fn pin(&self, last_tx_id: u64) {
        self.pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(last_tx_id);
    }

    /// Release a [`CheckpointRepo::pin`].
    pub fn unpin(&self, last_tx_id: u64) {
        self.pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&last_tx_id);
    }

    /// Prune checkpoints beyond the retention window (STG-023): keep the
    /// newest `retention` (>= 2 always) plus every pinned checkpoint, delete
    /// older manifests, then garbage-collect objects no retained manifest
    /// references. Returns the removed manifest paths.
    ///
    /// Object GC is conservative: if any retained manifest fails to decode,
    /// object deletion is skipped entirely (its references are unknowable).
    pub fn prune(&self, shard_id: u32, retention: usize) -> Result<Vec<PathBuf>> {
        if retention < 2 {
            return Err(FluxumError::Storage(format!(
                "checkpoint retention {retention} is below the minimum of 2 (STG-023)"
            )));
        }
        let refs = self.list(shard_id)?;
        if refs.len() <= retention {
            return Ok(Vec::new());
        }
        let pins = self
            .pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let cutoff = refs.len() - retention;
        let mut removed = Vec::new();
        for r in &refs[..cutoff] {
            if pins.contains(&r.last_tx_id) {
                continue;
            }
            fs::remove_file(&r.path)?;
            removed.push(r.path.clone());
        }
        if removed.is_empty() {
            return Ok(Vec::new());
        }

        // Object GC: collect every hash the remaining manifests reference.
        let mut referenced: HashSet<String> = HashSet::new();
        for r in self.list(shard_id)? {
            let manifest = match fs::read(&r.path)
                .map_err(FluxumError::Io)
                .and_then(|bytes| decode_manifest(&bytes))
            {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        manifest = %r.path.display(),
                        error = %e,
                        "retained checkpoint manifest is unreadable; skipping object GC"
                    );
                    return Ok(removed);
                }
            };
            for table in &manifest.tables {
                for hash in table.chunk_hashes()? {
                    referenced.insert(hash.to_string());
                }
            }
        }
        for entry in fs::read_dir(&self.objects)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if is_hash_name(name) && !referenced.contains(name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(removed)
    }
}

/// Durable file create: temp write + fsync + rename, so a crash never leaves
/// a half-written file under a valid name (same discipline as the blob
/// store, STG-021 two-phase creation).
fn write_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

/// The manifest file path of `(shard_id, last_tx_id)` — zero-padded so
/// directory order equals checkpoint order.
fn manifest_path(dir: &Path, shard_id: u32, last_tx_id: u64) -> PathBuf {
    dir.join(format!("ckpt-{shard_id:010}-{last_tx_id:020}.manifest"))
}

/// Parse `ckpt-<shard:010>-<tx:020>.manifest` back into a [`CheckpointRef`].
fn parse_manifest_name(name: &str, path: &Path) -> Option<CheckpointRef> {
    let rest = name.strip_prefix("ckpt-")?.strip_suffix(".manifest")?;
    let (shard, tx) = rest.split_at_checked(10)?;
    let tx = tx.strip_prefix('-')?;
    if shard.len() != 10 || tx.len() != 20 {
        return None;
    }
    Some(CheckpointRef {
        path: path.to_path_buf(),
        shard_id: shard.parse().ok()?,
        last_tx_id: tx.parse().ok()?,
    })
}

/// Whether `name` looks like a content-hash object file (64 hex chars).
fn is_hash_name(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())
}
