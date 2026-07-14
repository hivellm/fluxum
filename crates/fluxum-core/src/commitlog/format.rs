//! On-disk byte formats: the segment header and the STG-011 entry frame.
//!
//! Both formats freeze at gate G5 together with the FluxRPC wire format —
//! the entry frame doubles as the replication stream format (STG-016).
//!
//! # Entry frame (STG-011)
//!
//! ```text
//! ┌──────────────────┬─────────────────┬───────────────────────────┬──────────────────┐
//! │ length: u32 (LE) │ epoch: u64 (LE) │ body: MessagePack bytes   │ crc32c: u32 (LE) │
//! └──────────────────┴─────────────────┴───────────────────────────┴──────────────────┘
//!                      └──── CRC32C covers length + epoch + body ────┘
//! ```
//!
//! The CRC32C covers the length prefix and the epoch as well as the body, so
//! a corrupted length or epoch field is detected as a checksum failure rather
//! than mis-framing the rest of the segment (analysis `spacetimedb-code/03`
//! §"What Fluxum will face" item 3).
//!
//! # Segment header (STG-011 tail)
//!
//! ```text
//! magic [u8; 8] | version u16 LE | checksum u8 | reserved u8 | epoch u64 LE | crc32c u32 LE
//! ```
//!
//! 24 bytes. The header records the log format version, the checksum
//! algorithm, and the epoch at segment creation; its own CRC32C covers the
//! first 20 bytes.

/// Segment file magic (8 bytes).
pub(crate) const SEGMENT_MAGIC: [u8; 8] = *b"FLXMLOG\0";

/// Log format version recorded in every segment header (versioned from day
/// one — replay code accumulates compatibility forever).
pub(crate) const LOG_FORMAT_VERSION: u16 = 1;

/// Checksum algorithm id: 0 = CRC32C (Castagnoli, hardware-accelerated).
pub(crate) const CHECKSUM_CRC32C: u8 = 0;

/// Total segment header length in bytes.
pub(crate) const SEGMENT_HEADER_LEN: usize = 24;

/// Entry envelope overhead: length (4) + epoch (8) + trailing CRC32C (4).
pub(crate) const ENTRY_OVERHEAD: usize = 16;

/// Upper bound on one entry body. Anything larger is framing garbage, not a
/// legitimate transaction (guards allocation on corrupted length prefixes).
pub(crate) const MAX_BODY_LEN: u32 = 1 << 30;

/// Encode a segment header for a segment created under `epoch`.
pub(crate) fn encode_segment_header(epoch: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    buf[0..8].copy_from_slice(&SEGMENT_MAGIC);
    buf[8..10].copy_from_slice(&LOG_FORMAT_VERSION.to_le_bytes());
    buf[10] = CHECKSUM_CRC32C;
    buf[11] = 0; // reserved
    buf[12..20].copy_from_slice(&epoch.to_le_bytes());
    let crc = crc32c::crc32c(&buf[0..20]);
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode and verify a segment header; returns the creation epoch.
pub(crate) fn decode_segment_header(buf: &[u8]) -> Result<u64, String> {
    if buf.len() < SEGMENT_HEADER_LEN {
        return Err(format!(
            "segment shorter than the {SEGMENT_HEADER_LEN}-byte header ({} bytes)",
            buf.len()
        ));
    }
    let crc_stored = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
    if crc32c::crc32c(&buf[0..20]) != crc_stored {
        return Err("segment header CRC32C mismatch".into());
    }
    if buf[0..8] != SEGMENT_MAGIC {
        return Err("bad segment magic".into());
    }
    let version = u16::from_le_bytes([buf[8], buf[9]]);
    if version != LOG_FORMAT_VERSION {
        return Err(format!(
            "unsupported log format version {version} (supported: {LOG_FORMAT_VERSION})"
        ));
    }
    if buf[10] != CHECKSUM_CRC32C {
        return Err(format!("unsupported checksum algorithm id {}", buf[10]));
    }
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&buf[12..20]);
    Ok(u64::from_le_bytes(epoch))
}

/// Frame one entry: `len | epoch | body | crc32c` (STG-011).
pub(crate) fn encode_entry(epoch: u64, body: &[u8]) -> Result<Vec<u8>, String> {
    let len = u32::try_from(body.len())
        .ok()
        .filter(|len| *len <= MAX_BODY_LEN)
        .ok_or_else(|| {
            format!(
                "entry body of {} bytes exceeds the {MAX_BODY_LEN}-byte limit",
                body.len()
            )
        })?;
    let mut buf = Vec::with_capacity(ENTRY_OVERHEAD + body.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&epoch.to_le_bytes());
    buf.extend_from_slice(body);
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// One step of entry-frame scanning at `offset` into a segment buffer.
#[derive(Debug)]
pub(crate) enum ScannedEntry<'a> {
    /// A checksummed entry: its envelope epoch, its body bytes, and the
    /// buffer offset immediately after its trailing CRC.
    Entry {
        /// Envelope epoch (STG-011).
        epoch: u64,
        /// MessagePack body bytes.
        body: &'a [u8],
        /// Offset of the next frame.
        end: usize,
    },
    /// `offset == buf.len()`: the segment ends exactly at an entry boundary.
    CleanEof,
    /// Bytes exist past the boundary but not enough for a whole frame — a
    /// torn write (crash mid-append).
    Torn(String),
    /// A whole frame is present but fails validation (CRC mismatch or a
    /// nonsensical length prefix).
    Corrupt(String),
}

/// Scan the entry frame starting at `offset`, distinguishing clean EOF,
/// torn tail, and corruption (STG-031).
pub(crate) fn scan_entry(buf: &[u8], offset: usize) -> ScannedEntry<'_> {
    let remaining = buf.len().saturating_sub(offset);
    if remaining == 0 {
        return ScannedEntry::CleanEof;
    }
    if remaining < ENTRY_OVERHEAD {
        return ScannedEntry::Torn(format!(
            "{remaining} trailing byte(s) — shorter than the {ENTRY_OVERHEAD}-byte envelope"
        ));
    }
    let len = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]);
    if len > MAX_BODY_LEN {
        return ScannedEntry::Corrupt(format!(
            "length prefix {len} exceeds the {MAX_BODY_LEN}-byte body limit"
        ));
    }
    let body_len = len as usize;
    let frame_len = ENTRY_OVERHEAD + body_len;
    if remaining < frame_len {
        return ScannedEntry::Torn(format!(
            "entry frame of {frame_len} bytes truncated to {remaining}"
        ));
    }
    let end = offset + frame_len;
    let crc_stored = u32::from_le_bytes([buf[end - 4], buf[end - 3], buf[end - 2], buf[end - 1]]);
    if crc32c::crc32c(&buf[offset..end - 4]) != crc_stored {
        return ScannedEntry::Corrupt("entry CRC32C mismatch".into());
    }
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&buf[offset + 4..offset + 12]);
    ScannedEntry::Entry {
        epoch: u64::from_le_bytes(epoch),
        body: &buf[offset + 12..end - 4],
        end,
    }
}

/// Segment file name: `shard-<shard_id>-<first_tx_id>.log` (STG-014), with
/// `first_tx_id` zero-padded to 20 digits so lexicographic directory order
/// equals numeric offset order (analysis `spacetimedb-code/03` item 10).
pub(crate) fn segment_file_name(shard_id: u32, first_tx_id: u64) -> String {
    format!("shard-{shard_id}-{first_tx_id:020}.log")
}

/// Parse a segment file name for `shard_id`; returns its `first_tx_id`.
pub(crate) fn parse_segment_file_name(name: &str, shard_id: u32) -> Option<u64> {
    let rest = name.strip_prefix("shard-")?.strip_suffix(".log")?;
    let (shard, tx) = rest.split_once('-')?;
    if shard.parse::<u32>().ok()? != shard_id {
        return None;
    }
    tx.parse::<u64>().ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn segment_header_roundtrips_and_detects_corruption() {
        let header = encode_segment_header(42);
        assert_eq!(decode_segment_header(&header).unwrap(), 42);
        // Any single corrupted byte is caught (CRC or magic/version check).
        for i in 0..SEGMENT_HEADER_LEN {
            let mut bad = header;
            bad[i] ^= 0xFF;
            assert!(decode_segment_header(&bad).is_err(), "byte {i} undetected");
        }
        assert!(decode_segment_header(&header[..10]).is_err());
    }

    #[test]
    fn entry_roundtrips() {
        let body = b"hello commit log";
        let frame = encode_entry(7, body).unwrap();
        assert_eq!(frame.len(), ENTRY_OVERHEAD + body.len());
        match scan_entry(&frame, 0) {
            ScannedEntry::Entry {
                epoch,
                body: b,
                end,
            } => {
                assert_eq!(epoch, 7);
                assert_eq!(b, body);
                assert_eq!(end, frame.len());
            }
            other => panic!("expected entry, got {other:?}"),
        }
        assert!(matches!(
            scan_entry(&frame, frame.len()),
            ScannedEntry::CleanEof
        ));
    }

    #[test]
    fn crc_covers_length_and_epoch() {
        // Corrupting the length prefix or the epoch must surface as a
        // checksum/framing failure, never as a silently mis-framed entry.
        let frame = encode_entry(9, b"payload").unwrap();
        for i in 0..12 {
            let mut bad = frame.clone();
            bad[i] ^= 0x01;
            assert!(
                matches!(
                    scan_entry(&bad, 0),
                    ScannedEntry::Corrupt(_) | ScannedEntry::Torn(_)
                ),
                "byte {i} undetected"
            );
        }
    }

    #[test]
    fn every_truncation_is_torn_or_clean() {
        let frame = encode_entry(1, b"abcdef").unwrap();
        for cut in 1..frame.len() {
            assert!(
                matches!(scan_entry(&frame[..cut], 0), ScannedEntry::Torn(_)),
                "cut {cut} not detected as torn"
            );
        }
    }

    #[test]
    fn absurd_length_prefix_is_corrupt() {
        let mut frame = encode_entry(1, b"x").unwrap();
        frame[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(scan_entry(&frame, 0), ScannedEntry::Corrupt(_)));
        assert!(encode_entry(1, &vec![0u8; MAX_BODY_LEN as usize + 1]).is_err());
    }

    #[test]
    fn segment_names_roundtrip_and_sort_numerically() {
        let name = segment_file_name(3, 12);
        assert_eq!(name, "shard-3-00000000000000000012.log");
        assert_eq!(parse_segment_file_name(&name, 3), Some(12));
        assert_eq!(parse_segment_file_name(&name, 4), None);
        assert_eq!(parse_segment_file_name("junk.log", 3), None);
        assert_eq!(parse_segment_file_name("shard-3-xyz.log", 3), None);
        // Zero padding keeps lexicographic order == numeric order past 10^10.
        assert!(segment_file_name(0, 9) < segment_file_name(0, 10_000_000_001));
    }
}
