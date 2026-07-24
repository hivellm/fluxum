//! Seekable-zstd artifact framing (SPEC-025 OPS-010): independently
//! decodable zstd blocks plus a trailing index, so a byte window of the RAW
//! payload can be recovered by range-reading only the compressed blocks that
//! cover it — the format PITR uses to fetch just the cut window of the
//! target segment from object storage instead of the whole artifact.
//!
//! Layout:
//!
//! ```text
//! ┌──────────┬───────────────┬───────────────┬─────────────────────────────┐
//! │ magic 8  │ zstd frame 0  │ zstd frame 1… │ index | index_len u32 LE |  │
//! │ FLXZSEG1 │ (block 0 raw) │               │ magic 8 (FLXZIDX1)          │
//! └──────────┴───────────────┴───────────────┴─────────────────────────────┘
//! ```
//!
//! The index is MessagePack: `Vec<(comp_off, comp_len, raw_off, raw_len)>`.
//! A reader that knows the artifact's total size range-reads the trailer
//! (index tail) first, then exactly the frames it needs. A whole-artifact
//! read never needs the index: the frames are plain zstd back to back, so
//! [`decode_all`] simply decompresses until the index magic.

use crate::error::{FluxumError, Result};

/// Leading artifact magic.
pub const SEEKABLE_MAGIC: &[u8; 8] = b"FLXZSEG1";
/// Trailing index magic (the last 8 bytes of the artifact).
pub const INDEX_MAGIC: &[u8; 8] = b"FLXZIDX1";

/// Raw bytes per compressed block. 256 KiB: large enough that zstd ratios
/// stay near whole-file compression on segment payloads, small enough that
/// a PITR window fetches kilobytes, not the artifact.
pub const DEFAULT_BLOCK_RAW_BYTES: usize = 256 * 1024;

/// One block's placement: where its compressed frame lives in the artifact
/// and which raw window it decodes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    /// Offset of the zstd frame within the artifact.
    pub comp_off: u64,
    /// Compressed frame length.
    pub comp_len: u64,
    /// Raw offset this block decodes to.
    pub raw_off: u64,
    /// Raw length of this block.
    pub raw_len: u64,
}

/// Encode `raw` as a seekable artifact with `block_raw_bytes` per block.
///
/// # Errors
/// zstd compression or index encoding failures.
pub fn encode(raw: &[u8], block_raw_bytes: usize, level: i32) -> Result<Vec<u8>> {
    let block = block_raw_bytes.max(1);
    let mut out = Vec::with_capacity(raw.len() / 2 + 64);
    out.extend_from_slice(SEEKABLE_MAGIC);
    let mut index: Vec<(u64, u64, u64, u64)> = Vec::new();
    let mut raw_off = 0usize;
    while raw_off < raw.len() || (raw.is_empty() && raw_off == 0) {
        let end = (raw_off + block).min(raw.len());
        let frame = zstd::stream::encode_all(&raw[raw_off..end], level)
            .map_err(|e| FluxumError::Storage(format!("seekable zstd block failed: {e}")))?;
        index.push((
            out.len() as u64,
            frame.len() as u64,
            raw_off as u64,
            (end - raw_off) as u64,
        ));
        out.extend_from_slice(&frame);
        raw_off = end;
        if raw.is_empty() {
            break;
        }
    }
    let index_bytes = rmp_serde::to_vec(&index)
        .map_err(|e| FluxumError::Storage(format!("seekable index encoding failed: {e}")))?;
    let index_len = u32::try_from(index_bytes.len())
        .map_err(|_| FluxumError::Storage("seekable index too large".into()))?;
    out.extend_from_slice(&index_bytes);
    out.extend_from_slice(&index_len.to_le_bytes());
    out.extend_from_slice(INDEX_MAGIC);
    Ok(out)
}

/// Decode a whole seekable artifact back to its raw bytes.
///
/// # Errors
/// A missing magic, a corrupt index, or a corrupt block.
pub fn decode_all(artifact: &[u8]) -> Result<Vec<u8>> {
    let index = parse_index(artifact_tail(artifact)?, artifact.len() as u64)?;
    let mut raw = Vec::new();
    for block in &index {
        raw.extend_from_slice(&decode_block(artifact_slice(artifact, block)?)?);
    }
    Ok(raw)
}

/// The index of a seekable artifact, given ONLY its trailing bytes and the
/// artifact's total size — what a remote reader gets from one small range
/// read at the tail. `tail` must include at least the whole index trailer;
/// [`TAIL_HINT_BYTES`] is a safe amount to fetch blind.
///
/// # Errors
/// A short tail, a bad magic, or an undecodable index.
pub fn parse_index(tail: &[u8], artifact_len: u64) -> Result<Vec<BlockRef>> {
    let corrupt = |reason: &str| FluxumError::Storage(format!("seekable artifact: {reason}"));
    if tail.len() < 12 + INDEX_MAGIC.len() - 8 {
        return Err(corrupt("tail too short for the index trailer"));
    }
    let magic_at = tail.len() - INDEX_MAGIC.len();
    if &tail[magic_at..] != INDEX_MAGIC {
        return Err(corrupt("bad index magic"));
    }
    let len_at = magic_at - 4;
    let index_len = u32::from_le_bytes([
        tail[len_at],
        tail[len_at + 1],
        tail[len_at + 2],
        tail[len_at + 3],
    ]) as usize;
    let Some(index_at) = len_at.checked_sub(index_len) else {
        return Err(corrupt("index longer than the fetched tail"));
    };
    let entries: Vec<(u64, u64, u64, u64)> = rmp_serde::from_slice(&tail[index_at..len_at])
        .map_err(|e| corrupt(&format!("index decode failed: {e}")))?;
    let blocks: Vec<BlockRef> = entries
        .into_iter()
        .map(|(comp_off, comp_len, raw_off, raw_len)| BlockRef {
            comp_off,
            comp_len,
            raw_off,
            raw_len,
        })
        .collect();
    for block in &blocks {
        if block.comp_off.saturating_add(block.comp_len) > artifact_len {
            return Err(corrupt("index references bytes past the artifact"));
        }
    }
    Ok(blocks)
}

/// How many trailing bytes to fetch blind to be sure of holding the whole
/// index trailer (segments produce a handful of blocks; 64 KiB of index is
/// thousands of entries).
pub const TAIL_HINT_BYTES: u64 = 64 * 1024;

/// The blocks whose raw windows intersect `[raw_start, raw_start + raw_len)`,
/// in order — what a range reader must fetch.
pub fn blocks_covering(index: &[BlockRef], raw_start: u64, raw_len: u64) -> Vec<BlockRef> {
    let raw_end = raw_start.saturating_add(raw_len);
    index
        .iter()
        .copied()
        .filter(|b| b.raw_off < raw_end && b.raw_off + b.raw_len > raw_start)
        .collect()
}

/// Decode one block's compressed frame.
///
/// # Errors
/// zstd decompression failures (a corrupt or truncated frame).
pub fn decode_block(frame: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(frame)
        .map_err(|e| FluxumError::Storage(format!("seekable zstd block decode failed: {e}")))
}

/// The total raw length the index describes.
pub fn raw_len(index: &[BlockRef]) -> u64 {
    index.last().map_or(0, |b| b.raw_off + b.raw_len)
}

fn artifact_tail(artifact: &[u8]) -> Result<&[u8]> {
    if !artifact.starts_with(SEEKABLE_MAGIC) {
        return Err(FluxumError::Storage(
            "seekable artifact: bad leading magic".into(),
        ));
    }
    Ok(artifact)
}

fn artifact_slice<'a>(artifact: &'a [u8], block: &BlockRef) -> Result<&'a [u8]> {
    let start = usize::try_from(block.comp_off)
        .map_err(|_| FluxumError::Storage("seekable block offset out of range".into()))?;
    let len = usize::try_from(block.comp_len)
        .map_err(|_| FluxumError::Storage("seekable block length out of range".into()))?;
    artifact
        .get(start..start + len)
        .ok_or_else(|| FluxumError::Storage("seekable block out of bounds".into()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample(len: usize) -> Vec<u8> {
        // Compressible but position-dependent, so block mixups are caught.
        (0..len).map(|i| (i / 7 % 251) as u8).collect()
    }

    #[test]
    fn round_trips_across_block_boundaries() {
        for len in [0usize, 1, 1024, 4096, 10_000] {
            let raw = sample(len);
            let artifact = encode(&raw, 4096, 3).unwrap();
            assert_eq!(decode_all(&artifact).unwrap(), raw, "len {len}");
        }
    }

    #[test]
    fn a_range_read_touches_only_covering_blocks() {
        let raw = sample(20_000);
        let artifact = encode(&raw, 4096, 3).unwrap();
        let index = parse_index(&artifact, artifact.len() as u64).unwrap();
        assert_eq!(index.len(), 5, "20000/4096 → 5 blocks");
        assert_eq!(raw_len(&index), 20_000);

        // A window inside blocks 2..=3.
        let covering = blocks_covering(&index, 9_000, 4_000);
        assert_eq!(covering.len(), 2);
        let mut recovered = Vec::new();
        for block in &covering {
            recovered.extend_from_slice(
                &decode_block(artifact_slice(&artifact, block).unwrap()).unwrap(),
            );
        }
        let base = covering[0].raw_off as usize;
        assert_eq!(&recovered[9_000 - base..13_000 - base], &raw[9_000..13_000]);
    }

    #[test]
    fn the_index_parses_from_a_blind_tail_fetch() {
        let raw = sample(50_000);
        let artifact = encode(&raw, 4096, 3).unwrap();
        let tail_start = artifact.len().saturating_sub(TAIL_HINT_BYTES as usize);
        let index = parse_index(&artifact[tail_start..], artifact.len() as u64).unwrap();
        assert_eq!(raw_len(&index), 50_000);
    }

    #[test]
    fn corruption_is_detected() {
        let raw = sample(10_000);
        let mut artifact = encode(&raw, 4096, 3).unwrap();
        // Bad index magic.
        let last = artifact.len() - 1;
        artifact[last] ^= 0xFF;
        assert!(parse_index(&artifact, artifact.len() as u64).is_err());
        artifact[last] ^= 0xFF;
        // A flipped byte inside a block fails that block's decode.
        artifact[16] ^= 0xFF;
        assert!(decode_all(&artifact).is_err());
    }
}
