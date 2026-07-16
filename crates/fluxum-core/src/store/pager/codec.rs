//! Page-payload compression codecs (SPEC-015 TIER-040/041/044) and the zstd
//! artifact codec shared by checkpoints and backups (TIER-042, reused by
//! T7.3).
//!
//! # Where compression sits in the tier (TIER-044)
//!
//! The buffer pool holds only **uncompressed** page images; a pool hit never
//! touches a codec. Compression runs exactly once on the spill/flush path
//! ([`compress_image`]) and decompression exactly once on the fault-in path
//! ([`decompress_image`]), after the mandatory CRC32C verification — the CRC
//! stored in a compressed page covers the **stored** (compressed) image, so
//! corruption is detected before any codec runs.
//!
//! # Stored-payload layout (freeze surface, versioned with the page format)
//!
//! A page whose TIER-021 header codec bits are non-zero stores its payload
//! as (all integers little-endian):
//!
//! | Offset | Size | Field | Notes |
//! |---|---|---|---|
//! | 0 | 4 | `raw_len: u32` | uncompressed payload length |
//! | 4 | … | block | LZ4 block (codec `1`) or zstd frame (codec `2`) decoding to exactly `raw_len` bytes |
//!
//! The header's `payload_len` counts the stored bytes (prefix + block). A
//! page whose codec bits are `0` stores its payload verbatim — the writer
//! falls back to raw whenever the payload is smaller than
//! `storage.compression_min_bytes` or compression saves less than 12.5%
//! (TIER-040), so every page is self-describing and files with mixed codecs
//! read correctly (TIER-041).
//!
//! Reconstruction is exact: decompressing and re-encoding with the codec
//! bits cleared reproduces the original pool image bit-identically
//! (round-trip property, TIER-044), which keeps TIER-063 content hashes
//! stable across evict/fault cycles.
//!
//! # Checkpoint/backup artifacts (TIER-042)
//!
//! Checkpoint manifests and content-addressed objects — and later the T7.3
//! backup archives — are compressed as whole zstd frames via
//! [`compress_artifact`]/[`decompress_artifact`]. Artifacts are
//! self-describing through the zstd frame magic (`28 B5 2F FD`): the
//! pre-compression formats (`FLXCKPT1` manifests, MessagePack array chunk
//! objects) can never start with that byte sequence, so raw artifacts
//! written before compression landed keep reading correctly.

use std::borrow::Cow;

use crate::config::PageCompression;
use crate::crypto::Keyring;
use crate::error::{FluxumError, Result};

use super::format::{self, FLAG_CODEC_MASK, FLAG_ENCRYPTED, PAGE_HEADER_LEN, PageHeader};

/// The AEAD associated data binding a sealed page payload to its identity
/// (SEC-010): shard, table, page id, and the stored flag bits (codec,
/// encrypted, index/overflow, and version). A page sealed for one position
/// can never authenticate in another.
fn page_aad(shard_id: u32, header: &PageHeader) -> [u8; 18] {
    let mut aad = [0u8; 18];
    aad[0..4].copy_from_slice(&shard_id.to_le_bytes());
    aad[4..8].copy_from_slice(&header.table_id.to_le_bytes());
    aad[8..16].copy_from_slice(&header.page_id.to_le_bytes());
    aad[16..18].copy_from_slice(&header.flags.to_le_bytes());
    aad
}

/// zstd level used for compressed page payloads (fixed — TIER-041 exposes
/// the codec choice, not a per-page level; zstd pages trade fault-in latency
/// for ratio at the default level).
const PAGE_ZSTD_LEVEL: i32 = 3;

/// LZ4 pages are written with **LZ4-HC** at this level: spills run in the
/// background evictor/flush, never on the writer or read hot path, so the
/// writer-side CPU buys ~15% better ratio at identical (format-compatible,
/// codec-bit-1) fast decompression on fault-in. Level 4 is within 1% of
/// HC 9's ratio on the reference corpus at roughly twice the speed.
const PAGE_LZ4_HC_LEVEL: i32 = 4;

/// Default zstd level for checkpoint/backup artifacts
/// (`storage.checkpoint_compression_level`, TIER-042).
pub const DEFAULT_ARTIFACT_ZSTD_LEVEL: i32 = 3;

/// The zstd frame magic (RFC 8878), little-endian on disk.
pub const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// A page compression codec — the TIER-021 header flag bits 0–1. Bit value
/// `3` is reserved and rejected on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCodec {
    /// Payload stored verbatim (codec bits `0`).
    None,
    /// LZ4 block (codec bits `1`) — the TIER-040 default.
    Lz4,
    /// zstd frame (codec bits `2`) — higher ratio, slower fault-in.
    Zstd,
}

impl PageCodec {
    /// The TIER-021 flag bits of this codec.
    pub const fn bits(self) -> u16 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Zstd => 2,
        }
    }

    /// Decode header codec bits; `None` for the reserved value `3`.
    pub const fn from_bits(bits: u16) -> Option<Self> {
        match bits {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }
}

impl From<PageCompression> for PageCodec {
    /// The codec selected by `storage.page_compression` (TIER-041).
    fn from(config: PageCompression) -> Self {
        match config {
            PageCompression::Lz4 => Self::Lz4,
            PageCompression::Zstd => Self::Zstd,
            PageCompression::None => Self::None,
        }
    }
}

/// Compress one page payload for storage (TIER-040).
///
/// Returns the stored-payload bytes (`raw_len` prefix + codec block), or
/// `None` when the page should be stored raw: the codec is
/// [`PageCodec::None`], the payload is smaller than `min_bytes`, or the
/// compressed form saves less than 12.5% of the payload.
pub fn compress_payload(
    codec: PageCodec,
    payload: &[u8],
    min_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    if codec == PageCodec::None || payload.len() < min_bytes {
        return Ok(None);
    }
    let raw_len = u32::try_from(payload.len()).map_err(|_| {
        FluxumError::Storage(format!(
            "page payload of {} bytes exceeds the u32 raw_len field",
            payload.len()
        ))
    })?;
    let block = match codec {
        PageCodec::None => return Ok(None),
        PageCodec::Lz4 => lz4::block::compress(
            payload,
            Some(lz4::block::CompressionMode::HIGHCOMPRESSION(
                PAGE_LZ4_HC_LEVEL,
            )),
            false,
        )
        .map_err(|e| FluxumError::Storage(format!("LZ4 page compression failed: {e}")))?,
        PageCodec::Zstd => zstd::bulk::compress(payload, PAGE_ZSTD_LEVEL)
            .map_err(|e| FluxumError::Storage(format!("zstd page compression failed: {e}")))?,
    };
    let stored_len = 4 + block.len();
    // TIER-040: keep the compressed form only when it saves >= 12.5%
    // (stored <= 7/8 of raw); otherwise the raw bytes win.
    if stored_len.saturating_mul(8) > payload.len().saturating_mul(7) {
        return Ok(None);
    }
    let mut stored = Vec::with_capacity(stored_len);
    stored.extend_from_slice(&raw_len.to_le_bytes());
    stored.extend_from_slice(&block);
    Ok(Some(stored))
}

/// Decompress one stored page payload (`raw_len` prefix + codec block) back
/// to its raw bytes. `max_raw` bounds the allocation (a page payload can
/// never exceed the page size); the decoded length must equal `raw_len`
/// exactly.
pub fn decompress_payload(codec: PageCodec, stored: &[u8], max_raw: usize) -> Result<Vec<u8>> {
    if codec == PageCodec::None {
        return Err(FluxumError::Storage(
            "decompress_payload called for an uncompressed page".into(),
        ));
    }
    let Some((prefix, block)) = stored.split_at_checked(4) else {
        return Err(FluxumError::Storage(format!(
            "compressed page payload of {} bytes is shorter than its raw_len prefix",
            stored.len()
        )));
    };
    let raw_len = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
    if raw_len > max_raw {
        return Err(FluxumError::Storage(format!(
            "compressed page declares a {raw_len}-byte payload, above the {max_raw}-byte \
             page bound"
        )));
    }
    let raw_len_i32 = i32::try_from(raw_len).map_err(|_| {
        FluxumError::Storage(format!("compressed page raw_len {raw_len} overflows i32"))
    })?;
    let raw = match codec {
        PageCodec::None => unreachable!("rejected above"),
        PageCodec::Lz4 => lz4::block::decompress(block, Some(raw_len_i32))
            .map_err(|e| FluxumError::Storage(format!("LZ4 page decompression failed: {e}")))?,
        PageCodec::Zstd => zstd::bulk::decompress(block, raw_len)
            .map_err(|e| FluxumError::Storage(format!("zstd page decompression failed: {e}")))?,
    };
    if raw.len() != raw_len {
        return Err(FluxumError::Storage(format!(
            "compressed page decoded to {} bytes but declared raw_len {raw_len}",
            raw.len()
        )));
    }
    Ok(raw)
}

/// Compress an uncompressed pool image into its stored form (spill path,
/// TIER-040): `None` means "store the image verbatim". When `Some`, the
/// returned image carries the codec flag bits and a CRC32C over the stored
/// bytes.
pub fn compress_image(image: &[u8], codec: PageCodec, min_bytes: usize) -> Result<Option<Vec<u8>>> {
    let (header, payload) = format::trusted_header(image)?;
    if header.codec() != 0 {
        return Err(FluxumError::Storage(format!(
            "pool image of page {} already carries codec bits {} — frames must hold \
             uncompressed images (TIER-044)",
            header.page_id,
            header.codec()
        )));
    }
    let Some(stored_payload) = compress_payload(codec, payload, min_bytes)? else {
        return Ok(None);
    };
    let stored_header = PageHeader {
        flags: header.flags | codec.bits(),
        ..header
    };
    Ok(Some(format::encode_page(&stored_header, &stored_payload)?))
}

/// Rebuild the uncompressed pool image from a CRC-verified stored page whose
/// codec bits are non-zero (fault-in path, TIER-044): decompress the stored
/// payload, clear the codec bits, and re-encode — bit-identical to the image
/// that was spilled.
pub fn decompress_image(
    header: &PageHeader,
    stored_payload: &[u8],
    max_raw: usize,
) -> Result<Vec<u8>> {
    let codec = PageCodec::from_bits(header.codec()).ok_or_else(|| {
        FluxumError::Storage(format!(
            "page {} carries reserved codec bits 3 (TIER-021)",
            header.page_id
        ))
    })?;
    let raw = decompress_payload(codec, stored_payload, max_raw)?;
    let raw_header = PageHeader {
        flags: header.flags & !FLAG_CODEC_MASK,
        ..*header
    };
    format::encode_page(&raw_header, &raw)
}

/// Prepare an uncompressed pool image for cold storage (spill path):
/// compress per the codec (TIER-040), then — when a keyring is present —
/// AEAD-encrypt the stored payload under the active key (SEC-010), setting
/// [`FLAG_ENCRYPTED`]. The CRC32C in the returned image covers the stored
/// (ciphertext) bytes, so fault-in verifies integrity *before* decrypting
/// (SEC-011).
///
/// Returns `None` only when the image is stored verbatim — possible solely
/// when no keyring is configured *and* compression did not help; with
/// encryption on, every page is sealed, so the result is always `Some`.
pub fn encode_for_storage(
    image: &[u8],
    codec: PageCodec,
    min_bytes: usize,
    shard_id: u32,
    keyring: Option<&Keyring>,
) -> Result<Option<Vec<u8>>> {
    let (header, payload) = format::trusted_header(image)?;
    if header.codec() != 0 || header.is_encrypted() {
        return Err(FluxumError::Storage(format!(
            "pool image of page {} already carries codec/encryption bits — frames must \
             hold plain uncompressed images (TIER-044)",
            header.page_id
        )));
    }
    let compressed = compress_payload(codec, payload, min_bytes)?;
    let codec_bits = if compressed.is_some() {
        codec.bits()
    } else {
        0
    };
    let stored_payload: &[u8] = compressed.as_deref().unwrap_or(payload);

    match keyring {
        None => {
            let Some(stored_payload) = &compressed else {
                return Ok(None); // verbatim: uncompressed, unencrypted
            };
            let stored_header = PageHeader {
                flags: header.flags | codec_bits,
                ..header
            };
            Ok(Some(format::encode_page(&stored_header, stored_payload)?))
        }
        Some(ring) => {
            let flags = header.flags | codec_bits | FLAG_ENCRYPTED;
            let stored_header = PageHeader { flags, ..header };
            let sealed = ring.seal(stored_payload, &page_aad(shard_id, &stored_header))?;
            Ok(Some(format::encode_page(&stored_header, &sealed)?))
        }
    }
}

/// Rebuild the uncompressed pool image from a CRC-verified stored page (fault
/// path, TIER-044): decrypt the stored payload if [`FLAG_ENCRYPTED`] is set
/// (SEC-011 — the CRC already passed), then decompress if the codec bits say
/// so, then clear both the codec and encryption bits and re-encode. The
/// result is bit-identical to the image originally spilled.
///
/// A page whose bytes were sealed under a retired (`previous`) key still
/// opens (SEC-012 lazy rotation); it re-seals under the active key the next
/// time it is modified and spilled.
pub fn open_image(
    header: &PageHeader,
    stored_payload: &[u8],
    max_raw: usize,
    shard_id: u32,
    keyring: Option<&Keyring>,
) -> Result<Vec<u8>> {
    // 1. Decrypt (SEC-011): only reached after CRC verification.
    let payload: Cow<'_, [u8]> = if header.is_encrypted() {
        let ring = keyring.ok_or_else(|| {
            FluxumError::Storage(format!(
                "page {} is encrypted at rest but no keyring is configured (SEC-010)",
                header.page_id
            ))
        })?;
        let (plain, _active) = ring.open(stored_payload, &page_aad(shard_id, header))?;
        Cow::Owned(plain)
    } else {
        Cow::Borrowed(stored_payload)
    };

    // 2. Decompress if the codec bits are non-zero.
    let codec = PageCodec::from_bits(header.codec()).ok_or_else(|| {
        FluxumError::Storage(format!(
            "page {} carries reserved codec bits 3 (TIER-021)",
            header.page_id
        ))
    })?;
    let raw = if codec == PageCodec::None {
        payload.into_owned()
    } else {
        decompress_payload(codec, &payload, max_raw)?
    };

    // 3. Clear codec + encryption bits and re-encode the plain pool image.
    let raw_header = PageHeader {
        flags: header.flags & !(FLAG_CODEC_MASK | FLAG_ENCRYPTED),
        ..*header
    };
    format::encode_page(&raw_header, &raw)
}

/// The stored payload length of an encoded image (bytes after the header) —
/// the `fluxum_page_compression_ratio` denominator input (TIER-080).
pub(crate) fn payload_len(image: &[u8]) -> u64 {
    image.len().saturating_sub(PAGE_HEADER_LEN) as u64
}

/// Magic prefix of an encrypted artifact envelope (SEC-010): the sealed
/// `[nonce ++ ciphertext ++ tag]` follows. Distinct from the zstd frame
/// magic and every plaintext artifact magic (`FLXCKPT1`, MessagePack), so
/// artifacts stay self-describing across the encryption boundary.
pub const ARTIFACT_ENC_MAGIC: [u8; 8] = *b"FLXENC01";

/// The AEAD associated data binding an artifact envelope to the artifact
/// domain (so a page envelope can never be replayed as an artifact and vice
/// versa; pages bind an 18-byte position tuple instead).
const ARTIFACT_AAD: &[u8] = b"fluxum-at-rest-artifact-v1";

/// Compress a checkpoint/backup artifact (manifest, content-addressed
/// object, backup archive member) as one zstd frame (TIER-042), then — when
/// a keyring is present — AEAD-encrypt it under the active key (SEC-010),
/// prefixed with [`ARTIFACT_ENC_MAGIC`]. `level` is
/// `storage.checkpoint_compression_level` (default
/// [`DEFAULT_ARTIFACT_ZSTD_LEVEL`]).
pub fn compress_artifact(bytes: &[u8], level: i32, keyring: Option<&Keyring>) -> Result<Vec<u8>> {
    let compressed = zstd::stream::encode_all(bytes, level)
        .map_err(|e| FluxumError::Storage(format!("zstd artifact compression failed: {e}")))?;
    match keyring {
        None => Ok(compressed),
        Some(ring) => {
            let sealed = ring.seal(&compressed, ARTIFACT_AAD)?;
            let mut out = Vec::with_capacity(ARTIFACT_ENC_MAGIC.len() + sealed.len());
            out.extend_from_slice(&ARTIFACT_ENC_MAGIC);
            out.extend_from_slice(&sealed);
            Ok(out)
        }
    }
}

/// Decompress an artifact written by [`compress_artifact`], decrypting first
/// when the [`ARTIFACT_ENC_MAGIC`] envelope is present (SEC-011 — a wrong or
/// absent key is an authentication failure, never silent garbage), then
/// passing raw (pre-compression) artifacts through unchanged. Self-describing
/// via the artifact and zstd magics.
pub fn decompress_artifact<'a>(
    bytes: &'a [u8],
    keyring: Option<&Keyring>,
) -> Result<Cow<'a, [u8]>> {
    let inner: Cow<'a, [u8]> = if bytes.starts_with(&ARTIFACT_ENC_MAGIC) {
        let ring = keyring.ok_or_else(|| {
            FluxumError::Storage(
                "artifact is encrypted at rest but no keyring is configured (SEC-010)".into(),
            )
        })?;
        let (plain, _active) = ring.open(&bytes[ARTIFACT_ENC_MAGIC.len()..], ARTIFACT_AAD)?;
        Cow::Owned(plain)
    } else {
        Cow::Borrowed(bytes)
    };
    if inner.starts_with(&ZSTD_MAGIC) {
        let raw = zstd::stream::decode_all(inner.as_ref()).map_err(|e| {
            FluxumError::Storage(format!("zstd artifact decompression failed: {e}"))
        })?;
        Ok(Cow::Owned(raw))
    } else {
        Ok(inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::pager::format::{FLAG_INDEX, decode_page, encode_page};

    fn compressible(len: usize) -> Vec<u8> {
        // Repeating phrase: compresses well under both codecs.
        b"the quick brown fox jumps over the lazy dog -- "
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect()
    }

    #[test]
    fn codec_bits_round_trip_and_reserve_3() {
        for codec in [PageCodec::None, PageCodec::Lz4, PageCodec::Zstd] {
            assert_eq!(PageCodec::from_bits(codec.bits()), Some(codec));
        }
        assert_eq!(PageCodec::from_bits(3), None);
    }

    #[test]
    fn payloads_below_the_threshold_stay_raw() {
        let payload = compressible(512);
        for codec in [PageCodec::Lz4, PageCodec::Zstd] {
            let stored = compress_payload(codec, &payload, 1024).unwrap_or_else(|e| panic!("{e}"));
            assert!(stored.is_none(), "sub-threshold payload was compressed");
        }
    }

    #[test]
    fn incompressible_payloads_stay_raw() {
        // splitmix64 output: effectively random, saving < 12.5%.
        let mut state = 7u64;
        let mut payload = Vec::with_capacity(8192);
        while payload.len() < 8192 {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            payload.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
        }
        for codec in [PageCodec::Lz4, PageCodec::Zstd] {
            let stored = compress_payload(codec, &payload, 1024).unwrap_or_else(|e| panic!("{e}"));
            assert!(stored.is_none(), "random payload beat the 12.5% gate");
        }
    }

    #[test]
    fn payload_round_trips_bit_identically() {
        let payload = compressible(8000);
        for codec in [PageCodec::Lz4, PageCodec::Zstd] {
            let stored = compress_payload(codec, &payload, 1024)
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("compressible payload stored raw ({codec:?})"));
            assert!(stored.len() < payload.len());
            let raw = decompress_payload(codec, &stored, 16384).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(raw, payload, "{codec:?} round trip diverged");
        }
    }

    #[test]
    fn image_round_trips_through_the_stored_form() {
        let payload = compressible(4000);
        let original = encode_page(&PageHeader::new(9, 0xAB, 17, FLAG_INDEX), &payload)
            .unwrap_or_else(|e| panic!("{e}"));
        for codec in [PageCodec::Lz4, PageCodec::Zstd] {
            let stored = compress_image(&original, codec, 1024)
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("compressible image stored raw ({codec:?})"));
            // The stored image is a valid page in its own right: CRC over
            // the stored bytes, codec bits set, index flag preserved.
            let (header, stored_payload) =
                decode_page(&stored, 0, 0xAB, 9).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(header.codec(), codec.bits());
            assert!(header.is_index());
            assert_eq!(header.row_count, 17);
            // Fault-in reconstruction is bit-identical (TIER-044/063).
            let rebuilt =
                decompress_image(&header, stored_payload, 8192).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(rebuilt, original, "{codec:?} image round trip diverged");
        }
    }

    #[test]
    fn declared_raw_len_is_bounded_by_the_page_size() {
        let payload = compressible(6000);
        let stored = compress_payload(PageCodec::Lz4, &payload, 1024)
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("compressible payload stored raw"));
        let err = match decompress_payload(PageCodec::Lz4, &stored, 4096) {
            Ok(_) => panic!("oversized raw_len was allocated"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("page bound"), "{err}");
    }

    #[test]
    fn decompress_payload_rejects_the_none_codec() {
        let err = match decompress_payload(PageCodec::None, &[0u8; 8], 4096) {
            Ok(_) => panic!("codec None must be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("uncompressed page"), "{err}");
    }

    #[test]
    fn stored_payload_shorter_than_the_prefix_is_rejected() {
        let err = match decompress_payload(PageCodec::Lz4, &[1u8, 2], 4096) {
            Ok(_) => panic!("2-byte stored payload decoded"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("shorter than its raw_len prefix"),
            "{err}"
        );
    }

    #[test]
    fn raw_len_overflowing_i32_is_rejected_without_allocating() {
        // raw_len = u32::MAX passes the max_raw bound (usize::MAX) but must
        // fail the i32 conversion the LZ4 API needs.
        let mut stored = u32::MAX.to_le_bytes().to_vec();
        stored.extend_from_slice(&[0u8; 4]);
        let err = match decompress_payload(PageCodec::Lz4, &stored, usize::MAX) {
            Ok(_) => panic!("oversized raw_len decoded"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("overflows i32"), "{err}");
    }

    #[test]
    fn decoded_length_must_match_the_declared_raw_len() {
        let payload = compressible(6000);
        let mut stored = compress_payload(PageCodec::Zstd, &payload, 1024)
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("compressible payload stored raw"));
        // Tamper the raw_len prefix upward by one: zstd decodes to the
        // original 6000 bytes, which no longer equals the declared length.
        let declared = u32::from_le_bytes([stored[0], stored[1], stored[2], stored[3]]) + 1;
        stored[..4].copy_from_slice(&declared.to_le_bytes());
        let err = match decompress_payload(PageCodec::Zstd, &stored, 16384) {
            Ok(_) => panic!("length-mismatched payload decoded"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("but declared raw_len"), "{err}");
    }

    #[test]
    fn compress_image_rejects_an_already_compressed_pool_image() {
        // A pool frame must never hold codec bits (TIER-044).
        let header = PageHeader::new(3, 0xAB, 0, PageCodec::Lz4.bits());
        let image = encode_page(&header, b"stored-form").unwrap_or_else(|e| panic!("{e}"));
        let err = match compress_image(&image, PageCodec::Lz4, 0) {
            Ok(_) => panic!("codec-flagged pool image accepted"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("already carries codec bits"),
            "{err}"
        );
        assert!(err.to_string().contains("TIER-044"), "{err}");
    }

    #[test]
    fn decompress_image_rejects_reserved_codec_bits_3() {
        let header = PageHeader {
            page_id: 9,
            table_id: 1,
            row_count: 0,
            flags: (crate::store::pager::format::FORMAT_VERSION << 8) | 0b11,
        };
        let err = match decompress_image(&header, &[0u8; 8], 4096) {
            Ok(_) => panic!("reserved codec bits decoded"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("reserved codec bits 3"), "{err}");
    }

    #[test]
    fn encode_for_storage_rejects_a_flagged_pool_image() {
        // A pool image must arrive with clean flags (no codec/enc bits).
        let header = PageHeader::new(4, 0xAB, 0, PageCodec::Lz4.bits());
        let image = encode_page(&header, b"already-stored").unwrap_or_else(|e| panic!("{e}"));
        let err = match encode_for_storage(&image, PageCodec::Lz4, 0, 0, None) {
            Ok(_) => panic!("flagged pool image accepted"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("already carries codec/encryption bits"),
            "{err}"
        );
    }

    #[test]
    fn open_image_rejects_reserved_codec_bits_3() {
        let header = PageHeader {
            page_id: 9,
            table_id: 1,
            row_count: 0,
            flags: (crate::store::pager::format::FORMAT_VERSION << 8) | 0b11,
        };
        let err = match open_image(&header, &[0u8; 8], 4096, 0, None) {
            Ok(_) => panic!("reserved codec bits decoded"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("reserved codec bits 3"), "{err}");
    }

    #[test]
    fn encrypted_incompressible_page_seals_raw_and_opens() {
        use crate::crypto::{AtRestKey, Keyring};
        // splitmix64 output: incompressible, so compression is skipped and
        // the raw payload is sealed (codec bits stay 0, FLAG_ENCRYPTED set).
        let mut state = 11u64;
        let mut payload = Vec::with_capacity(2048);
        while payload.len() < 2048 {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            payload.extend_from_slice(&(z ^ (z >> 27)).to_le_bytes());
        }
        let image =
            encode_page(&PageHeader::new(2, 7, 1, 0), &payload).unwrap_or_else(|e| panic!("{e}"));
        let ring = Keyring::new(AtRestKey::new("k", [3u8; 32]), vec![]);

        // No keyring + incompressible ⇒ stored verbatim (None).
        assert!(
            encode_for_storage(&image, PageCodec::Lz4, 0, 0, None)
                .unwrap_or_else(|e| panic!("{e}"))
                .is_none(),
            "incompressible unencrypted page stores verbatim"
        );

        // With a keyring, even an incompressible page is sealed (raw payload).
        let stored = encode_for_storage(&image, PageCodec::Lz4, 0, 0, Some(&ring))
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("encryption always stores"));
        let (header, sealed) = decode_page(&stored, 0, 7, 2).unwrap_or_else(|e| panic!("{e}"));
        assert!(header.is_encrypted());
        assert_eq!(header.codec(), 0, "raw payload sealed, no compression");
        let rebuilt =
            open_image(&header, sealed, 8192, 0, Some(&ring)).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(rebuilt, image);
    }

    #[test]
    fn corrupt_zstd_artifacts_surface_a_decompression_error() {
        // The zstd magic followed by garbage: routed to the codec, which
        // must fail loudly instead of passing bytes through.
        let mut bogus = ZSTD_MAGIC.to_vec();
        bogus.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let err = match decompress_artifact(&bogus, None) {
            Ok(_) => panic!("corrupt zstd frame passed through"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("zstd artifact decompression failed"),
            "{err}"
        );
    }

    #[test]
    fn artifacts_round_trip_and_raw_artifacts_pass_through() {
        let body = compressible(10_000);
        let stored = compress_artifact(&body, DEFAULT_ARTIFACT_ZSTD_LEVEL, None)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(stored.starts_with(&ZSTD_MAGIC));
        assert!(stored.len() < body.len());
        let raw = decompress_artifact(&stored, None).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(raw.as_ref(), body.as_slice());

        // A pre-compression artifact (no zstd magic) passes through borrowed.
        let legacy = b"FLXCKPT1 legacy manifest bytes";
        let out = decompress_artifact(legacy, None).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(out, Cow::Borrowed(_)));
        assert_eq!(out.as_ref(), legacy);
    }
}
