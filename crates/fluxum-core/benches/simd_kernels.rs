//! HWA-060 kernel benchmarks (TST-004, `harness = false`): every dispatched
//! kernel against its scalar reference on representative batch sizes. A SIMD
//! variant that cannot demonstrate a measured speedup here must not ship —
//! parity without performance is dead weight.
#![allow(clippy::unwrap_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fluxum_core::config::SimdMode;
use fluxum_core::simd::{Dispatch, PredOp, Tier, bitmap_words};

fn crc32c(c: &mut Criterion) {
    let scalar = Dispatch::new(SimdMode::Scalar).unwrap();
    let auto = Dispatch::new(SimdMode::Auto).unwrap();
    let tier = auto.selection().crc32c;

    let mut group = c.benchmark_group("crc32c");
    for size in [64usize, 4096, 65536] {
        let data: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("scalar", size), &data, |b, d| {
            b.iter(|| scalar.crc32c(black_box(d)));
        });
        if tier != Tier::Scalar {
            group.bench_with_input(BenchmarkId::new(tier.as_str(), size), &data, |b, d| {
                b.iter(|| auto.crc32c(black_box(d)));
            });
        }
    }
    group.finish();
}

fn hash64(c: &mut Criterion) {
    let scalar = Dispatch::new(SimdMode::Scalar).unwrap();
    let auto = Dispatch::new(SimdMode::Auto).unwrap();
    let tier = auto.selection().hash64;

    let mut group = c.benchmark_group("hash64");
    for size in [8usize, 64, 1024] {
        let data: Vec<u8> = (0..size).map(|i| (i * 131 % 251) as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("scalar", size), &data, |b, d| {
            b.iter(|| scalar.hash64(black_box(d), 0));
        });
        if tier != Tier::Scalar {
            group.bench_with_input(BenchmarkId::new(tier.as_str(), size), &data, |b, d| {
                b.iter(|| auto.hash64(black_box(d), 0));
            });
        }
    }
    group.finish();
}

fn predicate(c: &mut Criterion) {
    let scalar = Dispatch::new(SimdMode::Scalar).unwrap();
    let auto = Dispatch::new(SimdMode::Auto).unwrap();

    const ROWS: usize = 65_536;
    let ints: Vec<i64> = (0..ROWS as i64)
        .map(|i| i.wrapping_mul(2654435761) % 1000)
        .collect();
    let floats: Vec<f64> = ints.iter().map(|&v| v as f64 * 0.25).collect();
    let mut out = vec![0u64; bitmap_words(ROWS)];

    let mut group = c.benchmark_group("predicate_i64");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function(BenchmarkId::new("scalar", ROWS), |b| {
        b.iter(|| scalar.eval_i64(PredOp::Lt, black_box(&ints), 500, &mut out));
    });
    let tier = auto.selection().predicate_i64;
    if tier != Tier::Scalar {
        group.bench_function(BenchmarkId::new(tier.as_str(), ROWS), |b| {
            b.iter(|| auto.eval_i64(PredOp::Lt, black_box(&ints), 500, &mut out));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("predicate_f64");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function(BenchmarkId::new("scalar", ROWS), |b| {
        b.iter(|| scalar.eval_f64(PredOp::Gt, black_box(&floats), 125.0, &mut out));
    });
    let tier = auto.selection().predicate_f64;
    if tier != Tier::Scalar {
        group.bench_function(BenchmarkId::new(tier.as_str(), ROWS), |b| {
            b.iter(|| auto.eval_f64(PredOp::Gt, black_box(&floats), 125.0, &mut out));
        });
    }
    group.finish();
}

criterion_group!(benches, crc32c, hash64, predicate);
criterion_main!(benches);
