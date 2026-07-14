//! Checkpoint manifest (STG-021): the fsynced commit record of a checkpoint,
//! naming every content-addressed object it references, carrying an
//! integrity hash over its own serialized bytes.
//!
//! A checkpoint whose manifest is absent or fails verification does not
//! exist — objects are written (and fsynced) first, the manifest last, so
//! creation is two-phase crash-safe.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::{Digest, Sha256};

use crate::error::{FluxumError, Result};

/// Manifest file magic: identifies the format before any decoding runs.
pub(crate) const MANIFEST_MAGIC: &[u8; 8] = b"FLXCKPT1";

/// Current manifest body format version.
pub(crate) const MANIFEST_VERSION: u32 = 1;

/// Content hash of a checkpoint object (SHA-256; BLAKE3-class per STG-021 —
/// the algorithm is an implementation detail until the G5 checkpoint-format
/// freeze, mirroring [`crate::commitlog::BlobHash`]). Displays as 64
/// lowercase hex characters, which is also the object file name.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    /// Hash `bytes` into their content address.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// The raw 32 hash bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Wrap raw hash bytes (e.g. decoded from a manifest).
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ObjectHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectHash({self})")
    }
}

/// The MessagePack manifest body (STG-021): `shard_id`, `last_tx_id`, the
/// epoch, the timestamp, and the content hashes of every referenced object.
/// rmp-serde serializes positionally: field order freezes at gate G5.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Body format version ([`MANIFEST_VERSION`]).
    pub format_version: u32,
    /// The shard this checkpoint covers.
    pub shard_id: u32,
    /// Every transaction `<= last_tx_id` is covered by this checkpoint.
    pub last_tx_id: u64,
    /// Fencing epoch at checkpoint time (STG-011).
    pub epoch: u64,
    /// Microseconds since the Unix epoch.
    pub timestamp: i64,
    /// One entry per registered table (including empty tables, so the
    /// auto-inc high-water mark always restores).
    pub tables: Vec<TableManifest>,
}

/// One table's checkpointed contents: row-chunk objects in encoded-PK order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableManifest {
    /// Stable table id (`crc32(name)`, STG-050).
    pub table_id: u32,
    /// Table name — restore verifies `crc32(name) == table_id` and resolves
    /// the live schema by name.
    pub table_name: String,
    /// Durable auto-inc high-water mark at checkpoint time (STG-040).
    pub auto_inc_high_water: u64,
    /// Total rows across all chunks (verified on restore).
    pub row_count: u64,
    /// Content hashes of the table's row-chunk objects, in encoded-PK order
    /// (32 bytes each).
    pub chunks: Vec<ByteBuf>,
}

impl TableManifest {
    /// The typed chunk hashes, validating the 32-byte width.
    pub fn chunk_hashes(&self) -> Result<Vec<ObjectHash>> {
        self.chunks
            .iter()
            .map(|raw| {
                let bytes: [u8; 32] = raw.as_slice().try_into().map_err(|_| {
                    FluxumError::Storage(format!(
                        "checkpoint manifest: table `{}` chunk hash must be 32 bytes, got {}",
                        self.table_name,
                        raw.len()
                    ))
                })?;
                Ok(ObjectHash::from_bytes(bytes))
            })
            .collect()
    }
}

/// Serialize a manifest into its on-disk form:
/// `MAGIC | MessagePack body | SHA-256(MAGIC | body)` — the trailing
/// integrity hash covers everything before it (STG-021).
pub(crate) fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>> {
    let body = rmp_serde::to_vec(manifest)
        .map_err(|e| FluxumError::Storage(format!("checkpoint manifest encoding failed: {e}")))?;
    let mut out = Vec::with_capacity(MANIFEST_MAGIC.len() + body.len() + 32);
    out.extend_from_slice(MANIFEST_MAGIC);
    out.extend_from_slice(&body);
    let digest: [u8; 32] = Sha256::digest(&out).into();
    out.extend_from_slice(&digest);
    Ok(out)
}

/// Decode and verify a manifest file's bytes: magic, integrity hash, body
/// decode, format version. Any failure means the checkpoint does not exist
/// (restore falls back to an older retained checkpoint, STG-021).
pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<Manifest> {
    let corrupt = |reason: &str| FluxumError::Storage(format!("checkpoint manifest: {reason}"));
    if bytes.len() < MANIFEST_MAGIC.len() + 32 {
        return Err(corrupt(&format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC {
        return Err(corrupt("bad magic"));
    }
    let (covered, stored_hash) = bytes.split_at(bytes.len() - 32);
    let digest: [u8; 32] = Sha256::digest(covered).into();
    if digest != stored_hash {
        return Err(corrupt("integrity hash mismatch"));
    }
    let manifest: Manifest = rmp_serde::from_slice(&covered[MANIFEST_MAGIC.len()..])
        .map_err(|e| corrupt(&format!("body decode failed: {e}")))?;
    if manifest.format_version != MANIFEST_VERSION {
        return Err(corrupt(&format!(
            "unsupported format version {}",
            manifest.format_version
        )));
    }
    Ok(manifest)
}
