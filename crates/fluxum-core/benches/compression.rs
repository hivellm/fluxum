//! T2.9 compression benchmarks (SPEC-015 TIER-043, checklist 1.4).
//!
//! Publishes the compression-ratio artifact — raw vs stored payload bytes
//! per canonical demo table (`User`, `ChatMessage`, `Task`, `Sensor` over
//! the SPEC-013 reference corpus) for LZ4 and zstd — and measures the CPU
//! cost the codecs add: payload compress/decompress throughput and the
//! end-to-end cold fault-in (`pread` + CRC32C + decompress + pool insert)
//! per codec.
//!
//! Run with `cargo bench --bench compression`; the ratio table prints at
//! the head of the run. The `≥ 3x` acceptance itself is asserted by
//! `tests/page_compression.rs` (the DAG exit test), which measures the same
//! corpus through the same spill path.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "support/corpus.rs"]
mod corpus;

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use fluxum_core::config::PageCompression;
use fluxum_core::schema::TableSchema;
use fluxum_core::store::RowValue;
use fluxum_core::store::pager::codec::{PageCodec, compress_payload, decompress_payload};
use fluxum_core::store::pager::{ColdTable, Pager, PagerOptions};

const PAGE_SIZE: usize = 8192;

fn pager_with(dir: &std::path::Path, compression: PageCompression) -> Arc<Pager> {
    Pager::open(
        dir,
        PagerOptions {
            shard_id: 0,
            page_size: PAGE_SIZE,
            pool_capacity_bytes: (2048 * PAGE_SIZE) as u64,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression,
            compression_min_bytes: 1024,
        },
    )
    .expect("pager opens")
}

/// The published ratio artifact: spill the corpus per table per codec and
/// report raw/stored payload bytes from the TIER-080 counters.
fn publish_ratio_report(_c: &mut Criterion) {
    let tables: &[(&'static TableSchema, u64)] = &[
        (&corpus::USER, 4_000),
        (&corpus::CHAT_MESSAGE, 10_000),
        (&corpus::TASK, 4_000),
        (&corpus::SENSOR, 12_000),
    ];
    println!("\n== TIER-043 compression-ratio report (SPEC-013 reference corpus) ==");
    println!(
        "{:<8} {:<12} {:>12} {:>12} {:>8}",
        "codec", "table", "raw B", "stored B", "ratio"
    );
    for compression in [PageCompression::Lz4, PageCompression::Zstd] {
        let (mut raw_total, mut stored_total) = (0u64, 0u64);
        for &(table, rows) in tables {
            let dir = tempfile::tempdir().expect("tempdir");
            let (store, table_id) = corpus::populated(table, rows);
            let snap = store.snapshot();
            let pager = pager_with(dir.path(), compression);
            let _cold = ColdTable::spill_snapshot(&pager, &snap, table_id).expect("spill");
            pager.flush().expect("flush");
            let m = pager.metrics().snapshot();
            println!(
                "{:<8} {:<12} {:>12} {:>12} {:>7.2}x",
                format!("{compression:?}"),
                table.name,
                m.compression_raw_bytes,
                m.compression_stored_bytes,
                m.compression_ratio().unwrap_or(1.0)
            );
            raw_total += m.compression_raw_bytes;
            stored_total += m.compression_stored_bytes;
        }
        println!(
            "{:<8} {:<12} {:>12} {:>12} {:>7.2}x\n",
            format!("{compression:?}"),
            "TOTAL",
            raw_total,
            stored_total,
            raw_total as f64 / stored_total as f64
        );
    }
}

/// A realistic ~8 KiB leaf-like payload: corpus chat text.
fn text_payload() -> Vec<u8> {
    let mut rng = corpus::Rng(11);
    let mut out = Vec::new();
    let mut id = 0u64;
    while out.len() < PAGE_SIZE - 64 {
        for value in corpus::row_for(&corpus::CHAT_MESSAGE, id, &mut rng) {
            if let RowValue::Str(s) = value {
                out.extend_from_slice(s.as_bytes());
                out.push(b' ');
            }
        }
        id += 1;
    }
    out.truncate(PAGE_SIZE - 64);
    out
}

/// Codec throughput on one page payload (the per-page CPU cost the spill
/// and fault-in paths pay).
fn bench_codec_payload(c: &mut Criterion) {
    let payload = text_payload();
    let mut group = c.benchmark_group("codec_payload_8k");
    for codec in [PageCodec::Lz4, PageCodec::Zstd] {
        let stored = compress_payload(codec, &payload, 1024)
            .expect("compress")
            .expect("corpus text must compress");
        group.bench_function(format!("compress/{codec:?}"), |b| {
            b.iter(|| compress_payload(codec, black_box(&payload), 1024).expect("compress"))
        });
        group.bench_function(format!("decompress/{codec:?}"), |b| {
            b.iter(|| decompress_payload(codec, black_box(&stored), PAGE_SIZE).expect("decompress"))
        });
    }
    group.finish();
}

/// End-to-end cold fault-in per codec: directory lookup + one `pread` +
/// CRC32C verify + decompress + pool insert (TIER-032), via 64 point reads
/// of an evicted corpus table.
fn bench_fault_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("fault_in_64_point_reads");
    group.sample_size(20);
    for compression in [
        PageCompression::None,
        PageCompression::Lz4,
        PageCompression::Zstd,
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, table_id) = corpus::populated(&corpus::CHAT_MESSAGE, 6_000);
        let snap = store.snapshot();
        let pager = pager_with(dir.path(), compression);
        let cold = ColdTable::spill_snapshot(&pager, &snap, table_id).expect("spill");
        pager.flush().expect("flush");
        drop(snap);
        group.bench_function(format!("{compression:?}"), |b| {
            b.iter(|| {
                pager.evict_all().expect("evict");
                for i in 0..64u64 {
                    let row = cold
                        .get(&[RowValue::U64(i * 93 % 6_000)])
                        .expect("cold get")
                        .expect("row exists");
                    black_box(row);
                }
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    publish_ratio_report,
    bench_codec_payload,
    bench_fault_in
);
criterion_main!(benches);
