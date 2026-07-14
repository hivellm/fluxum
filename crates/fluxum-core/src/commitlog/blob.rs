//! [`BlobStore`] — per-shard content-addressed, reference-counted storage
//! for large out-of-row values (STG-041).
//!
//! Identical values share one stored copy (the content hash IS the object
//! key), and physical reclamation is gated on retention holds: a blob's
//! bytes may be deleted only when its refcount is zero **and** no retained
//! checkpoint or commit-log segment has registered a hold on its hash
//! (STG-041 / STG-023). Holds are registered by the checkpoint machinery
//! (T2.3) and the replication retention logic; each holder uses its own key.
//!
//! Refcounts are an in-memory index over the on-disk object set: on open,
//! existing objects load with refcount 0 and recovery (checkpoint restore +
//! log replay, T2.3) re-establishes live counts before any reclamation runs.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use sha2::{Digest, Sha256};

use crate::error::{FluxumError, Result};

use super::segment::sync_dir;

/// Content hash of a stored blob (SHA-256; BLAKE3-class per STG-041 — the
/// algorithm is an implementation detail until the G5 checkpoint-format
/// freeze). Displays as 64 lowercase hex characters, which is also the
/// object file name.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    /// Hash `bytes` into their content address.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// The raw 32 hash bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobHash({self})")
    }
}

fn parse_hash(name: &str) -> Option<BlobHash> {
    if name.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in name.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).ok()?;
        bytes[i] = u8::from_str_radix(hex, 16).ok()?;
    }
    Some(BlobHash(bytes))
}

#[derive(Debug, Default)]
struct Inner {
    /// Live row references per hash. 0 = present on disk but unreferenced
    /// (reclaim candidate once no hold covers it).
    refcounts: HashMap<BlobHash, u64>,
    /// Retention holds: holder key (checkpoint id, segment id, transfer id…)
    /// → hashes it pins (STG-041 GC gate).
    holds: HashMap<u64, HashSet<BlobHash>>,
}

/// Per-shard content-addressed, refcounted blob store (STG-041).
#[derive(Debug)]
pub struct BlobStore {
    dir: PathBuf,
    inner: Mutex<Inner>,
}

impl BlobStore {
    /// Open (or create) the store in `dir`, indexing existing objects with
    /// refcount 0 (recovery re-establishes live counts).
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let mut refcounts = HashMap::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(hash) = entry.file_name().to_str().and_then(parse_hash) {
                refcounts.insert(hash, 0);
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            inner: Mutex::new(Inner {
                refcounts,
                holds: HashMap::new(),
            }),
        })
    }

    fn object_path(&self, hash: &BlobHash) -> PathBuf {
        self.dir.join(hash.to_string())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Store `bytes`, incrementing the refcount. Identical values are stored
    /// once — a second `put` of the same bytes only bumps the count.
    pub fn put(&self, bytes: &[u8]) -> Result<BlobHash> {
        let hash = BlobHash::of(bytes);
        let path = self.object_path(&hash);
        let mut inner = self.lock();
        let count = inner.refcounts.entry(hash).or_insert(0);
        if *count == 0 && !path.exists() {
            // Durable create: temp write + fsync + rename, so a crash never
            // leaves a half-written object under a valid hash name.
            let tmp = self.dir.join(format!("{hash}.tmp"));
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&tmp, &path)?;
            sync_dir(&self.dir)?;
        }
        *count += 1;
        Ok(hash)
    }

    /// Fetch a blob's bytes, if the hash is known.
    pub fn get(&self, hash: &BlobHash) -> Result<Option<Vec<u8>>> {
        if !self.lock().refcounts.contains_key(hash) {
            return Ok(None);
        }
        Ok(Some(fs::read(self.object_path(hash))?))
    }

    /// The current refcount of `hash` (`None` if unknown).
    pub fn refcount(&self, hash: &BlobHash) -> Option<u64> {
        self.lock().refcounts.get(hash).copied()
    }

    /// Drop one row reference. The bytes stay on disk until [`Self::reclaim`]
    /// runs *and* no retention hold covers the hash.
    pub fn unref(&self, hash: &BlobHash) -> Result<()> {
        let mut inner = self.lock();
        let count = inner
            .refcounts
            .get_mut(hash)
            .ok_or_else(|| FluxumError::Storage(format!("unref of unknown blob {hash}")))?;
        *count = count.saturating_sub(1);
        Ok(())
    }

    /// Register a retention hold: `holder` (a checkpoint id, retained
    /// segment id, or replica-transfer id) pins every hash in `hashes` —
    /// their bytes are never reclaimed while the hold exists (STG-041).
    pub fn hold(&self, holder: u64, hashes: impl IntoIterator<Item = BlobHash>) {
        self.lock().holds.entry(holder).or_default().extend(hashes);
    }

    /// Release `holder`'s retention hold (e.g. its checkpoint aged out of
    /// the STG-023 retention window).
    pub fn release_hold(&self, holder: u64) {
        self.lock().holds.remove(&holder);
    }

    /// Physically delete every blob whose refcount is zero and whose hash no
    /// retained hold references (STG-041). Returns the reclaimed hashes.
    pub fn reclaim(&self) -> Result<Vec<BlobHash>> {
        let mut inner = self.lock();
        let held: HashSet<BlobHash> = inner.holds.values().flatten().copied().collect();
        let candidates: Vec<BlobHash> = inner
            .refcounts
            .iter()
            .filter(|(hash, count)| **count == 0 && !held.contains(hash))
            .map(|(hash, _)| *hash)
            .collect();
        for hash in &candidates {
            let path = self.object_path(hash);
            if path.exists() {
                fs::remove_file(&path)?;
            }
            inner.refcounts.remove(hash);
        }
        Ok(candidates)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn object_count(dir: &Path) -> usize {
        fs::read_dir(dir).unwrap().count()
    }

    #[test]
    fn identical_values_are_stored_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        let big = vec![7u8; 8192];
        let a = store.put(&big).unwrap();
        let b = store.put(&big).unwrap();
        assert_eq!(a, b);
        assert_eq!(object_count(dir.path()), 1);
        assert_eq!(store.refcount(&a), Some(2));
        assert_eq!(store.get(&a).unwrap().unwrap(), big);
        assert_eq!(store.get(&BlobHash::of(b"other")).unwrap(), None);
    }

    #[test]
    fn reclaim_is_gated_on_refcount_and_holds() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        let hash = store.put(b"large value").unwrap();

        // Referenced: never reclaimed.
        assert!(store.reclaim().unwrap().is_empty());

        // Unreferenced but held by a retained checkpoint: never reclaimed
        // while any retained checkpoint references the hash (STG-041).
        store.unref(&hash).unwrap();
        store.hold(1, [hash]);
        assert!(store.reclaim().unwrap().is_empty());
        assert!(dir.path().join(hash.to_string()).exists());

        // Hold released: bytes may go.
        store.release_hold(1);
        assert_eq!(store.reclaim().unwrap(), vec![hash]);
        assert!(!dir.path().join(hash.to_string()).exists());
        assert_eq!(store.refcount(&hash), None);
    }

    #[test]
    fn reopen_indexes_existing_objects() {
        let dir = tempfile::tempdir().unwrap();
        let hash = {
            let store = BlobStore::open(dir.path()).unwrap();
            store.put(b"survives restart").unwrap()
        };
        let store = BlobStore::open(dir.path()).unwrap();
        // Present with refcount 0 until recovery re-references it.
        assert_eq!(store.refcount(&hash), Some(0));
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"survives restart");
        // A put of the same content dedupes against the existing object.
        assert_eq!(store.put(b"survives restart").unwrap(), hash);
        assert_eq!(store.refcount(&hash), Some(1));
        assert!(store.unref(&BlobHash::of(b"unknown")).is_err());
    }
}
