//! T2.9 page-compression acceptance suite (SPEC-015 §6, acceptance 3;
//! checklist 1.1/1.3/1.5):
//!
//! - LZ4 and zstd page round-trips are **bit-identical** under property
//!   testing, at the payload level and through the full stored-image
//!   transform the spill/fault paths apply (TIER-044);
//! - the compression ratio on the SPEC-013 reference corpus over the
//!   canonical demo schema (`User`, `ChatMessage`, `Task`, `Sensor`) is
//!   ≥ 3× for both codecs (TIER-043 — the DAG exit test; the criterion
//!   benchmark in `benches/compression.rs` publishes the same numbers);
//! - the corpus served back through compressed fault-ins matches the
//!   hot-tier oracle exactly, and TIER-063 content hashes survive
//!   evict/fault cycles under compression;
//! - pages below `compression_min_bytes` and incompressible pages stay raw,
//!   and files with mixed codecs read correctly (TIER-040/041).

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../benches/support/corpus.rs"]
mod corpus;

use std::sync::Arc;

use proptest::prelude::*;

use fluxum_core::config::PageCompression;
use fluxum_core::store::pager::codec::{self, PageCodec, compress_payload, decompress_payload};
use fluxum_core::store::pager::format::{self, PageHeader};
use fluxum_core::store::pager::pagefile::PageFile;
use fluxum_core::store::pager::{ColdTable, Pager, PagerOptions};
use fluxum_core::store::{Row, RowValue};

/// A pager with an ample pool (no eviction noise) and the given codec.
fn pager_with(
    dir: &std::path::Path,
    compression: PageCompression,
    compression_min_bytes: usize,
) -> Arc<Pager> {
    Pager::open(
        dir,
        PagerOptions {
            shard_id: 0,
            page_size: 8192,
            pool_capacity_bytes: 2048 * 8192,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression,
            compression_min_bytes,
        },
    )
    .expect("pager opens")
}

/// Spill one corpus table through a fresh pager and return
/// `(raw_payload_bytes, stored_payload_bytes)` from the TIER-080 counters.
fn corpus_ratio_input(
    table: &'static fluxum_core::schema::TableSchema,
    rows: u64,
    compression: PageCompression,
) -> (u64, u64) {
    let dir = tempfile::tempdir().unwrap();
    let (store, table_id) = corpus::populated(table, rows);
    let snap = store.snapshot();
    let pager = pager_with(dir.path(), compression, 1024);
    let _cold = ColdTable::spill_snapshot(&pager, &snap, table_id).expect("spill");
    pager.flush().expect("flush");
    let m = pager.metrics().snapshot();
    (m.compression_raw_bytes, m.compression_stored_bytes)
}

// --- checklist 1.5: the TIER-043 3x DAG exit test ---------------------------

#[test]
fn reference_corpus_compresses_at_least_3x_under_both_codecs() {
    let rows: &[(&'static fluxum_core::schema::TableSchema, u64)] = &[
        (&corpus::USER, 4_000),
        (&corpus::CHAT_MESSAGE, 10_000),
        (&corpus::TASK, 4_000),
        (&corpus::SENSOR, 12_000),
    ];
    for compression in [PageCompression::Lz4, PageCompression::Zstd] {
        let (mut raw_total, mut stored_total) = (0u64, 0u64);
        for &(table, n) in rows {
            let (raw, stored) = corpus_ratio_input(table, n, compression);
            assert!(raw > 0 && stored > 0, "{}: nothing spilled", table.name);
            println!(
                "{compression:?} {}: raw {raw} B, stored {stored} B, ratio {:.2}x",
                table.name,
                raw as f64 / stored as f64
            );
            raw_total += raw;
            stored_total += stored;
        }
        let ratio = raw_total as f64 / stored_total as f64;
        println!("{compression:?} corpus total: ratio {ratio:.2}x");
        assert!(
            ratio >= 3.0,
            "TIER-043 requires >= 3x on the reference corpus; {compression:?} got {ratio:.2}x"
        );
    }
}

// --- checklist 1.1: threshold + self-describing behavior --------------------

#[test]
fn a_threshold_above_every_payload_stores_everything_raw() {
    let dir = tempfile::tempdir().unwrap();
    let (store, table_id) = corpus::populated(&corpus::CHAT_MESSAGE, 2_000);
    let snap = store.snapshot();
    // min_bytes larger than any page payload: TIER-040 stores raw even
    // though the codec is LZ4.
    let pager = pager_with(dir.path(), PageCompression::Lz4, usize::MAX);
    let cold = ColdTable::spill_snapshot(&pager, &snap, table_id).expect("spill");
    pager.flush().expect("flush");
    let m = pager.metrics().snapshot();
    assert_eq!(
        m.compression_raw_bytes, m.compression_stored_bytes,
        "sub-threshold pages must be stored byte-for-byte raw"
    );
    // And they read back fine (codec bits 0).
    pager.evict_all().expect("evict");
    assert_eq!(cold.scan_all().expect("scan").len(), 2_000);
}

#[test]
fn corpus_round_trips_bit_identically_through_compressed_fault_ins() {
    for compression in [PageCompression::Lz4, PageCompression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let (store, table_id) = corpus::populated(&corpus::CHAT_MESSAGE, 6_000);
        let snap = store.snapshot();
        let pager = pager_with(dir.path(), compression, 1024);
        let cold = ColdTable::spill_snapshot(&pager, &snap, table_id).expect("spill");
        pager.flush().expect("flush");

        // TIER-063: the content hash (over the uncompressed image) is
        // unchanged by a spill-compressed round trip.
        let root = cold.primary_tree().root_page_id();
        let before = pager.content_hash(table_id, root).expect("hash");
        pager.evict_all().expect("evict");
        let after = pager.content_hash(table_id, root).expect("hash");
        assert_eq!(before, after, "{compression:?} changed the content hash");

        // Every row is served back identical to the hot-tier oracle after
        // the pool was emptied (all reads decompress on fault-in).
        pager.evict_all().expect("evict");
        let hot: Vec<Row> = snap.scan(table_id).expect("hot scan").cloned().collect();
        let cold_rows = cold.scan_all().expect("cold scan");
        assert_eq!(cold_rows, hot, "{compression:?} scan diverged");
        for id in [0u64, 1, 999, 5_998, 5_999] {
            let got = cold
                .get(&[RowValue::U64(id)])
                .expect("cold get")
                .expect("row exists");
            let want = snap
                .query_pk(table_id, &[RowValue::U64(id)])
                .expect("hot get")
                .expect("row exists hot");
            assert_eq!(got, want, "{compression:?} row {id} diverged");
        }
        // Compression actually happened (chat text beats the 12.5% gate).
        let m = pager.metrics().snapshot();
        assert!(
            m.compression_ratio().unwrap_or(1.0) > 1.5,
            "{compression:?} never compressed: {m:?}"
        );
    }
}

// --- TIER-041: mixed-codec files are self-describing ------------------------

#[test]
fn one_page_file_reads_pages_of_every_codec() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table-7.pages");
    let mut file = PageFile::create(&path, 8192, 0, 7).expect("create");

    let payload: Vec<u8> = b"fluxum compresses cold pages per codec -- "
        .iter()
        .copied()
        .cycle()
        .take(6000)
        .collect();
    let mut originals = Vec::new();
    let mut extents = Vec::new();
    for (page_id, codec) in [
        (1u64, PageCodec::None),
        (2, PageCodec::Lz4),
        (3, PageCodec::Zstd),
    ] {
        let image =
            format::encode_page(&PageHeader::new(page_id, 7, 42, 0), &payload).expect("encode");
        let stored = codec::compress_image(&image, codec, 1024)
            .expect("compress")
            .unwrap_or_else(|| image.clone());
        extents.push((page_id, file.write_page(&stored).expect("write")));
        originals.push(image);
    }

    for ((page_id, extent), original) in extents.into_iter().zip(&originals) {
        let stored = file.read_page(extent).expect("read");
        let (header, stored_payload) = format::decode_page(&stored, 0, 7, page_id).expect("decode");
        let image = if header.codec() != 0 {
            codec::decompress_image(&header, stored_payload, 8192).expect("decompress")
        } else {
            stored.clone()
        };
        assert_eq!(&image, original, "page {page_id} did not self-describe");
    }
}

// --- checklist 1.3: bit-identical round-trip property tests ------------------

/// Compressible byte strings: runs of repeated chunks with varied lengths.
fn compressible_payloads() -> impl Strategy<Value = Vec<u8>> {
    (
        proptest::collection::vec(any::<u8>(), 1..64),
        1usize..400,
        0usize..64,
    )
        .prop_map(|(chunk, repeats, tail)| {
            let mut out = Vec::with_capacity(chunk.len() * repeats + tail);
            for _ in 0..repeats {
                out.extend_from_slice(&chunk);
            }
            out.extend(std::iter::repeat_n(0xEE, tail));
            out
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// TIER-044: whatever the writer decides (compress or store raw), the
    /// reader reproduces the payload bit-identically — for both codecs,
    /// over compressible and incompressible inputs.
    #[test]
    fn payload_round_trips_bit_identically(
        payload in prop_oneof![
            compressible_payloads(),
            proptest::collection::vec(any::<u8>(), 0..8192),
        ],
        codec in prop_oneof![Just(PageCodec::Lz4), Just(PageCodec::Zstd)],
        min_bytes in prop_oneof![Just(0usize), Just(1024usize)],
    ) {
        if let Some(stored) = compress_payload(codec, &payload, min_bytes)
            .map_err(|e| TestCaseError::fail(e.to_string()))?
        {
            let raw = decompress_payload(codec, &stored, payload.len().max(1))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(raw, payload);
        }
        // `None` = stored raw: the round trip is the identity by definition.
    }

    /// The full spill/fault image transform is the identity: compress to
    /// the stored form (self-describing header, CRC over stored bytes),
    /// CRC-verify + decode, decompress — bit-identical pool image out.
    #[test]
    fn stored_image_transform_is_the_identity(
        payload in compressible_payloads(),
        codec in prop_oneof![Just(PageCodec::Lz4), Just(PageCodec::Zstd)],
        page_id in 1u64..1_000_000,
        row_count in 0u32..10_000,
    ) {
        let original = format::encode_page(
            &PageHeader::new(page_id, 0xF00D, row_count, 0),
            &payload,
        )
        .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let stored = compress_image_or_raw(&original, codec)?;
        let (header, stored_payload) = format::decode_page(&stored, 3, 0xF00D, page_id)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let rebuilt = if header.codec() != 0 {
            codec::decompress_image(&header, stored_payload, original.len())
                .map_err(|e| TestCaseError::fail(e.to_string()))?
        } else {
            stored.clone()
        };
        prop_assert_eq!(rebuilt, original);
    }
}

fn compress_image_or_raw(image: &[u8], codec: PageCodec) -> Result<Vec<u8>, TestCaseError> {
    Ok(codec::compress_image(image, codec, 0)
        .map_err(|e| TestCaseError::fail(e.to_string()))?
        .unwrap_or_else(|| image.to_vec()))
}
