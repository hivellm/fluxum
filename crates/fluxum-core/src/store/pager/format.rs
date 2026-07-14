//! On-disk page and page-file formats (SPEC-015 TIER-021/TIER-023).
//!
//! **Freeze surface (gate G5).** Everything in this module is part of the
//! on-disk format that replication and point-in-time recovery replay; it
//! carries an explicit version and MUST evolve only by version bump.
//!
//! # Page layout (TIER-021)
//!
//! Every on-disk page is a 32-byte header followed by the payload. All
//! integers little-endian:
//!
//! | Offset | Size | Field | Notes |
//! |---|---|---|---|
//! | 0  | 4 | `magic: u32`       | bytes `"FLXP"` (`0x46 0x4C 0x58 0x50`), `0x50584C46` LE |
//! | 4  | 8 | `page_id: u64`     | unique per (shard, table) |
//! | 12 | 4 | `table_id: u32`    | SPEC-002 STG-050 stable id |
//! | 16 | 4 | `row_count: u32`   | rows (leaf) or entries (interior) in this page |
//! | 20 | 2 | `flags: u16`       | see below |
//! | 22 | 2 | `reserved: u16`    | zero; ignored on read |
//! | 24 | 4 | `payload_len: u32` | stored payload bytes (post-compression) |
//! | 28 | 4 | `crc32c: u32`      | CRC32C (Castagnoli) over bytes 0..32 (this field zeroed) + payload |
//! | 32 | …  | payload           | FluxBIN rows / index entries, possibly compressed |
//!
//! `flags` bit assignments:
//!
//! - bits 0–1: compression codec — `0` none, `1` LZ4, `2` zstd, `3` reserved
//!   (rejected on read). A non-zero codec stores the payload as
//!   `raw_len: u32 LE` + codec block — see [`super::codec`] for the full
//!   stored-payload layout (TIER-040). Pages are self-describing, so
//!   mixed-codec files read correctly (TIER-041).
//! - bit 2: index page — interior/leaf B-tree node rather than a data leaf.
//! - bit 3: overflow page (TIER-026).
//! - bits 8–11: page-format version, currently `1`.
//! - all other bits: reserved; a set unknown bit rejects the page as
//!   unreadable (forward-compatibility guard).
//!
//! The `crc32c` field is the per-page integrity hash (hardware-accelerated
//! via [`crate::simd`]); it MUST be verified on every fault-in before the
//! page is served (TIER-032) — a mismatch is [`FluxumError::PageCorrupt`].
//!
//! # Page-file superblock (TIER-023)
//!
//! Each page file (`shard-<shard_id>/table-<table_id>.pages`) begins with a
//! 32-byte superblock; the remainder of its 256-byte extent slot is zero:
//!
//! | Offset | Size | Field | Notes |
//! |---|---|---|---|
//! | 0  | 4 | `magic: u32`     | bytes `"FLXS"`, `0x53584C46` LE |
//! | 4  | 4 | `version: u32`   | file-format version, currently `1` |
//! | 8  | 4 | `page_size: u32` | logical page size, fixed at creation (TIER-022) |
//! | 12 | 4 | `shard_id: u32`  | owning shard |
//! | 16 | 4 | `table_id: u32`  | owning table |
//! | 20 | 8 | reserved         | zero |
//! | 28 | 4 | `crc32c: u32`    | over bytes 0..28 |
//!
//! Stored pages are variable-length physical records (header + payload)
//! allocated at 256-byte granularity ([`EXTENT_ALIGN`], TIER-024).

use crate::error::{FluxumError, Result};

/// Page magic: bytes `"FLXP"` read as a little-endian `u32` (TIER-021).
pub const PAGE_MAGIC: u32 = u32::from_le_bytes(*b"FLXP");

/// Superblock magic: bytes `"FLXS"` read as a little-endian `u32` (TIER-023).
pub const SUPERBLOCK_MAGIC: u32 = u32::from_le_bytes(*b"FLXS");

/// Fixed page-header length in bytes (TIER-021).
pub const PAGE_HEADER_LEN: usize = 32;

/// Superblock length in bytes (TIER-023); its extent slot is [`EXTENT_ALIGN`].
pub const SUPERBLOCK_LEN: usize = 32;

/// Extent allocation granularity in bytes (TIER-024).
pub const EXTENT_ALIGN: u64 = 256;

/// Current page-format version (flags bits 8–11) and file-format version.
pub const FORMAT_VERSION: u16 = 1;

/// Flags bits 0–1: compression codec (`0` none, `1` LZ4, `2` zstd).
pub const FLAG_CODEC_MASK: u16 = 0b0000_0011;
/// Flags bit 2: index page (interior/leaf B-tree node, not a data leaf).
pub const FLAG_INDEX: u16 = 1 << 2;
/// Flags bit 3: overflow page (TIER-026).
pub const FLAG_OVERFLOW: u16 = 1 << 3;
/// Flags bits 8–11: page-format version.
pub const FLAG_VERSION_MASK: u16 = 0b1111 << 8;
/// Every bit meaning something in version 1; set bits outside this mask
/// reject the page as unreadable.
pub const FLAG_KNOWN_MASK: u16 = FLAG_CODEC_MASK | FLAG_INDEX | FLAG_OVERFLOW | FLAG_VERSION_MASK;

/// Decoded page header (the fixed 32 bytes before the payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// Page id, unique per (shard, table).
    pub page_id: u64,
    /// Owning table (STG-050 stable id).
    pub table_id: u32,
    /// Rows (leaf) or entries (interior) in this page.
    pub row_count: u32,
    /// Flag bits (codec, index, overflow, version).
    pub flags: u16,
}

impl PageHeader {
    /// Build a version-1 header. `flags` must not carry version bits — they
    /// are stamped here.
    pub fn new(page_id: u64, table_id: u32, row_count: u32, flags: u16) -> Self {
        debug_assert_eq!(flags & FLAG_VERSION_MASK, 0, "version bits are stamped");
        Self {
            page_id,
            table_id,
            row_count,
            flags: flags | (FORMAT_VERSION << 8),
        }
    }

    /// The compression codec bits (0 = none).
    pub fn codec(&self) -> u16 {
        self.flags & FLAG_CODEC_MASK
    }

    /// Whether the index-page flag (bit 2) is set.
    pub fn is_index(&self) -> bool {
        self.flags & FLAG_INDEX != 0
    }

    /// Whether the overflow-page flag (bit 3) is set.
    pub fn is_overflow(&self) -> bool {
        self.flags & FLAG_OVERFLOW != 0
    }

    /// The page-format version (flags bits 8–11).
    pub fn version(&self) -> u16 {
        (self.flags & FLAG_VERSION_MASK) >> 8
    }
}

/// Encode a full page image — header (with CRC32C stamped) followed by
/// `payload` — ready to be held in a pool frame or written to an extent.
pub fn encode_page(header: &PageHeader, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        FluxumError::Storage(format!(
            "page {} payload of {} bytes exceeds the u32 payload_len field",
            header.page_id,
            payload.len()
        ))
    })?;
    let mut image = Vec::with_capacity(PAGE_HEADER_LEN + payload.len());
    image.extend_from_slice(&PAGE_MAGIC.to_le_bytes());
    image.extend_from_slice(&header.page_id.to_le_bytes());
    image.extend_from_slice(&header.table_id.to_le_bytes());
    image.extend_from_slice(&header.row_count.to_le_bytes());
    image.extend_from_slice(&header.flags.to_le_bytes());
    image.extend_from_slice(&0u16.to_le_bytes()); // reserved
    image.extend_from_slice(&payload_len.to_le_bytes());
    image.extend_from_slice(&0u32.to_le_bytes()); // crc32c, zeroed for hashing
    image.extend_from_slice(payload);
    let crc = page_crc(&image);
    image[28..32].copy_from_slice(&crc.to_le_bytes());
    Ok(image)
}

/// CRC32C over a full page image with the `crc32c` field bytes treated as
/// zero (TIER-021), via the runtime-dispatched kernel.
fn page_crc(image: &[u8]) -> u32 {
    let simd = crate::simd::global();
    let crc = simd.crc32c(&image[..28]);
    let crc = simd.crc32c_extend(crc, &[0, 0, 0, 0]);
    simd.crc32c_extend(crc, &image[PAGE_HEADER_LEN.min(image.len())..])
}

/// Decode and verify a page image faulted from the cold tier: magic, CRC32C
/// (mandatory, TIER-032 — a mismatch is [`FluxumError::PageCorrupt`] and the
/// page is never served), version, unknown flag bits, `payload_len`, and the
/// header's `page_id`/`table_id` against the expected coordinates.
///
/// Returns the header and the payload slice.
pub fn decode_page(
    image: &[u8],
    shard_id: u32,
    table_id: u32,
    page_id: u64,
) -> Result<(PageHeader, &[u8])> {
    let corrupt = || FluxumError::PageCorrupt {
        shard_id,
        table_id,
        page_id,
    };
    if image.len() < PAGE_HEADER_LEN {
        return Err(corrupt());
    }
    // Fixed-width reads from a bounds-checked slice; the closure form keeps
    // every read explicit about its offset.
    let u32_at = |off: usize| {
        u32::from_le_bytes([image[off], image[off + 1], image[off + 2], image[off + 3]])
    };
    let magic = u32_at(0);
    let hdr_page_id = u64::from_le_bytes([
        image[4], image[5], image[6], image[7], image[8], image[9], image[10], image[11],
    ]);
    let hdr_table_id = u32_at(12);
    let row_count = u32_at(16);
    let flags = u16::from_le_bytes([image[20], image[21]]);
    let payload_len = u32_at(24);
    let stored_crc = u32_at(28);

    // CRC first (TIER-021): any bit flip — including in the header fields
    // checked below — is reported as corruption, never as a soft mismatch.
    if page_crc(image) != stored_crc {
        return Err(corrupt());
    }
    if magic != PAGE_MAGIC
        || hdr_page_id != page_id
        || hdr_table_id != table_id
        || image.len() != PAGE_HEADER_LEN + payload_len as usize
    {
        return Err(corrupt());
    }
    if flags & !FLAG_KNOWN_MASK != 0 {
        return Err(FluxumError::Storage(format!(
            "page {page_id} of table {table_id:#010x} carries unknown flag bits \
             {flags:#06x}; refusing to read a future-format page (TIER-021)"
        )));
    }
    let header = PageHeader {
        page_id: hdr_page_id,
        table_id: hdr_table_id,
        row_count,
        flags,
    };
    if header.version() != FORMAT_VERSION {
        return Err(FluxumError::Storage(format!(
            "page {page_id} of table {table_id:#010x} has format version {} \
             (this build reads version {FORMAT_VERSION})",
            header.version()
        )));
    }
    if header.codec() == 0b11 {
        return Err(FluxumError::Storage(format!(
            "page {page_id} of table {table_id:#010x} carries reserved compression \
             codec bits 3 (TIER-021)"
        )));
    }
    Ok((header, &image[PAGE_HEADER_LEN..]))
}

/// Header fields and payload of a **trusted** page image (one produced by
/// [`encode_page`] and held in a pool frame) — no CRC verification, no
/// coordinate checks. Cold-tier reads must go through [`decode_page`].
pub(crate) fn trusted_header(image: &[u8]) -> Result<(PageHeader, &[u8])> {
    if image.len() < PAGE_HEADER_LEN {
        return Err(FluxumError::Storage(format!(
            "trusted page image of {} bytes is shorter than the {PAGE_HEADER_LEN}-byte header",
            image.len()
        )));
    }
    let header = PageHeader {
        page_id: u64::from_le_bytes([
            image[4], image[5], image[6], image[7], image[8], image[9], image[10], image[11],
        ]),
        table_id: u32::from_le_bytes([image[12], image[13], image[14], image[15]]),
        row_count: u32::from_le_bytes([image[16], image[17], image[18], image[19]]),
        flags: u16::from_le_bytes([image[20], image[21]]),
    };
    Ok((header, &image[PAGE_HEADER_LEN..]))
}

/// Encode a page-file superblock (TIER-023).
pub fn encode_superblock(page_size: u32, shard_id: u32, table_id: u32) -> Vec<u8> {
    let mut block = Vec::with_capacity(SUPERBLOCK_LEN);
    block.extend_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    block.extend_from_slice(&u32::from(FORMAT_VERSION).to_le_bytes());
    block.extend_from_slice(&page_size.to_le_bytes());
    block.extend_from_slice(&shard_id.to_le_bytes());
    block.extend_from_slice(&table_id.to_le_bytes());
    block.extend_from_slice(&[0u8; 8]); // reserved
    let crc = crate::simd::global().crc32c(&block);
    block.extend_from_slice(&crc.to_le_bytes());
    block
}

/// Decode and verify a superblock against the expected coordinates.
/// Returns the recorded `page_size`.
pub fn decode_superblock(block: &[u8], shard_id: u32, table_id: u32) -> Result<u32> {
    let fail = |what: &str| {
        FluxumError::Storage(format!(
            "page file of shard {shard_id}, table {table_id:#010x}: bad superblock ({what})"
        ))
    };
    if block.len() < SUPERBLOCK_LEN {
        return Err(fail("truncated"));
    }
    let u32_at = |off: usize| {
        u32::from_le_bytes([block[off], block[off + 1], block[off + 2], block[off + 3]])
    };
    if crate::simd::global().crc32c(&block[..28]) != u32_at(28) {
        return Err(fail("CRC32C mismatch"));
    }
    if u32_at(0) != SUPERBLOCK_MAGIC {
        return Err(fail("magic mismatch"));
    }
    if u32_at(4) != u32::from(FORMAT_VERSION) {
        return Err(fail("unsupported file-format version"));
    }
    if u32_at(12) != shard_id || u32_at(16) != table_id {
        return Err(fail("shard/table coordinates do not match the path"));
    }
    Ok(u32_at(8))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magics_spell_flxp_and_flxs() {
        // TIER-021: "FLXP" reads as 0x50584C46 little-endian.
        assert_eq!(PAGE_MAGIC, 0x5058_4C46);
        assert_eq!(SUPERBLOCK_MAGIC, 0x5358_4C46);
    }

    #[test]
    fn page_round_trips_with_flags() {
        let header = PageHeader::new(7, 0xAB, 3, FLAG_INDEX);
        let image = encode_page(&header, b"payload").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(image.len(), PAGE_HEADER_LEN + 7);
        let (decoded, payload) = decode_page(&image, 0, 0xAB, 7).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(payload, b"payload");
        assert_eq!(decoded.row_count, 3);
        assert!(decoded.is_index());
        assert!(!decoded.is_overflow());
        assert_eq!(decoded.version(), 1);
        assert_eq!(decoded.codec(), 0);
    }

    #[test]
    fn every_bit_flip_is_page_corrupt() {
        let header = PageHeader::new(1, 2, 1, 0);
        let image = encode_page(&header, b"abcdef").unwrap_or_else(|e| panic!("{e}"));
        for byte in 0..image.len() {
            for bit in 0..8 {
                let mut tampered = image.clone();
                tampered[byte] ^= 1 << bit;
                let err = match decode_page(&tampered, 9, 2, 1) {
                    Ok(_) => panic!("tampered page served (byte {byte}, bit {bit})"),
                    Err(e) => e,
                };
                // A flip in the stored CRC or anywhere else must always
                // surface as PageCorrupt with the page coordinates.
                match err {
                    FluxumError::PageCorrupt {
                        shard_id: 9,
                        table_id: 2,
                        page_id: 1,
                    } => {}
                    other => panic!("expected PageCorrupt, got {other}"),
                }
            }
        }
    }

    #[test]
    fn wrong_coordinates_and_truncation_are_corrupt() {
        let header = PageHeader::new(1, 2, 0, 0);
        let image = encode_page(&header, b"x").unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(
            decode_page(&image, 0, 2, 99),
            Err(FluxumError::PageCorrupt { .. })
        ));
        assert!(matches!(
            decode_page(&image, 0, 3, 1),
            Err(FluxumError::PageCorrupt { .. })
        ));
        assert!(matches!(
            decode_page(&image[..10], 0, 2, 1),
            Err(FluxumError::PageCorrupt { .. })
        ));
    }

    #[test]
    fn unknown_flag_bits_reject_the_page_as_unreadable() {
        // Forge a page with a reserved flag bit set and a valid CRC: the
        // guard must fire *after* CRC passes, as a typed Storage error.
        let header = PageHeader {
            page_id: 4,
            table_id: 5,
            row_count: 0,
            flags: (FORMAT_VERSION << 8) | (1 << 6),
        };
        let image = encode_page(&header, b"").unwrap_or_else(|e| panic!("{e}"));
        let err = match decode_page(&image, 0, 5, 4) {
            Ok(_) => panic!("future-format page served"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unknown flag bits"), "{err}");
    }

    #[test]
    fn reserved_codec_bits_3_are_rejected() {
        let header = PageHeader::new(4, 5, 0, 0b11); // reserved codec value
        let image = encode_page(&header, b"z").unwrap_or_else(|e| panic!("{e}"));
        let err = match decode_page(&image, 0, 5, 4) {
            Ok(_) => panic!("reserved-codec page served"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("reserved compression"), "{err}");
    }

    #[test]
    fn known_codec_bits_decode_as_self_describing() {
        // Codec bits 1 (LZ4) and 2 (zstd) are valid version-1 flags; the
        // payload comes back stored-form for [`super::super::codec`] to
        // decompress (TIER-041 mixed-codec files).
        for codec in [0b01u16, 0b10u16] {
            let header = PageHeader::new(4, 5, 0, codec);
            let image = encode_page(&header, b"stored-form").unwrap_or_else(|e| panic!("{e}"));
            let (decoded, payload) = decode_page(&image, 0, 5, 4).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(decoded.codec(), codec);
            assert_eq!(payload, b"stored-form");
        }
    }

    #[test]
    fn trusted_header_matches_decode_page() {
        let header = PageHeader::new(11, 22, 33, FLAG_INDEX);
        let image = encode_page(&header, b"abc").unwrap_or_else(|e| panic!("{e}"));
        let (trusted, payload) = trusted_header(&image).unwrap_or_else(|e| panic!("{e}"));
        let (decoded, _) = decode_page(&image, 0, 22, 11).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(trusted, decoded);
        assert_eq!(payload, b"abc");
        assert!(trusted_header(&image[..10]).is_err());
    }

    #[test]
    fn superblock_round_trips_and_verifies() {
        let block = encode_superblock(8192, 3, 0xCAFE);
        assert_eq!(block.len(), SUPERBLOCK_LEN);
        assert_eq!(
            decode_superblock(&block, 3, 0xCAFE).unwrap_or_else(|e| panic!("{e}")),
            8192
        );
        assert!(decode_superblock(&block, 4, 0xCAFE).is_err());
        let mut tampered = block.clone();
        tampered[8] ^= 0xFF;
        assert!(decode_superblock(&tampered, 3, 0xCAFE).is_err());
    }
}
