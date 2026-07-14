//! Scalar-parity property suite (SPEC-016 HWA-052, DAG T2.10 exit test):
//! every SIMD variant selectable on this machine must be bit-identical to
//! the scalar oracle — over randomized inputs, lane-boundary lengths,
//! misaligned buffers, and NaN / ±0.0 / infinity-heavy float batches.
//!
//! Each forced mode that this CPU supports is exercised (HWA-053: on an ISA
//! matrix of x86-64 + aarch64 runners, that covers every variant a release
//! can select). External references pin the algorithms themselves: the
//! `crc` crate's CRC-32-ISCSI for CRC-32C (HWA-041) and `xxhash-rust` for
//! xxHash64 (HWA-042) — so the oracle is canonical, not merely
//! self-consistent.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::Config;
use fluxum_core::config::SimdMode;
use fluxum_core::hw;
use fluxum_core::simd::{CpuFeatures, Dispatch, PredOp, Tier, bitmap_words, select};
use proptest::prelude::*;

/// HWA-052 boundary lengths: at and around every vector-lane width.
const BOUNDARY_LENS: &[usize] = &[
    0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
];

/// Every dispatch constructible on this machine, labeled by forced mode.
/// `auto` exercises the best tier per kernel; forced tiers exercise the
/// HWA-032 fallback matrix; unsupported forced tiers are skipped (they
/// abort boot by design and are asserted separately).
fn dispatches() -> Vec<(&'static str, Dispatch)> {
    [
        ("auto", SimdMode::Auto),
        ("avx512", SimdMode::Avx512),
        ("avx2", SimdMode::Avx2),
        ("neon", SimdMode::Neon),
        ("scalar", SimdMode::Scalar),
    ]
    .into_iter()
    .filter_map(|(name, mode)| Dispatch::new(mode).ok().map(|d| (name, d)))
    .collect()
}

fn oracle() -> Dispatch {
    Dispatch::new(SimdMode::Scalar).unwrap()
}

/// Independent row-at-a-time reference for predicate bitmaps (HWA-044) —
/// deliberately not the kernel's own scalar implementation.
fn row_at_a_time_i64(op: PredOp, values: &[i64], rhs: i64) -> Vec<u64> {
    let mut out = vec![0u64; bitmap_words(values.len())];
    for (i, &v) in values.iter().enumerate() {
        let hit = match op {
            PredOp::Eq => v == rhs,
            PredOp::Lt => v < rhs,
            PredOp::Gt => v > rhs,
        };
        if hit {
            out[i / 64] |= 1 << (i % 64);
        }
    }
    out
}

fn row_at_a_time_f64(op: PredOp, values: &[f64], rhs: f64) -> Vec<u64> {
    let mut out = vec![0u64; bitmap_words(values.len())];
    for (i, &v) in values.iter().enumerate() {
        let hit = match op {
            PredOp::Eq => v == rhs,
            PredOp::Lt => v < rhs,
            PredOp::Gt => v > rhs,
        };
        if hit {
            out[i / 64] |= 1 << (i % 64);
        }
    }
    out
}

/// Floats biased toward the HWA-052 hard cases: NaN, ±0.0, infinities, and
/// small integers that collide with typical `rhs` values.
fn special_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        any::<f64>(),
        Just(f64::NAN),
        Just(-0.0),
        Just(0.0),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        (-4i32..4).prop_map(f64::from),
    ]
}

/// Integers biased toward collisions with small `rhs` values.
fn biased_i64() -> impl Strategy<Value = i64> {
    prop_oneof![any::<i64>(), -8i64..8, Just(i64::MIN), Just(i64::MAX)]
}

// ---------------------------------------------------------------------------
// CRC-32C
// ---------------------------------------------------------------------------

#[test]
fn crc32c_matches_the_external_reference() {
    let iscsi = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let data: Vec<u8> = (0..1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    for (name, d) in dispatches() {
        assert_eq!(d.crc32c(b"123456789"), 0xE306_9283, "{name}");
        assert_eq!(d.crc32c(b""), 0, "{name}");
        assert_eq!(d.crc32c(&data), iscsi.checksum(&data), "{name}");
    }
}

#[test]
fn crc32c_boundary_lengths_and_misaligned_buffers() {
    let oracle = oracle();
    let raw: Vec<u8> = (0..600u32)
        .map(|i| (i.wrapping_mul(151) >> 3) as u8)
        .collect();
    for (name, d) in dispatches() {
        for &len in BOUNDARY_LENS {
            for offset in 0..4 {
                let slice = &raw[offset..offset + len];
                assert_eq!(
                    d.crc32c(slice),
                    oracle.crc32c(slice),
                    "{name} len={len} offset={offset}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Selection / forced modes (HWA-032, HWA-033, HWA-034)
// ---------------------------------------------------------------------------

#[test]
fn forced_scalar_mode_selects_the_oracle_everywhere() {
    // HWA-034: `simd: scalar` is always valid and fully functional.
    let d = Dispatch::new(SimdMode::Scalar).unwrap();
    let sel = d.selection();
    assert_eq!(
        sel.report(),
        "crc32c=scalar hash64=scalar predicate_i64=scalar predicate_f64=scalar"
    );
    for tier in [sel.crc32c, sel.hash64, sel.predicate_i64, sel.predicate_f64] {
        assert_eq!(tier, Tier::Scalar);
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn forcing_neon_on_x86_64_aborts_boot() {
    let err = Dispatch::new(SimdMode::Neon).unwrap_err().to_string();
    assert!(err.contains("neon"), "{err}");
    assert!(err.contains("HWA-032"), "{err}");
}

#[cfg(target_arch = "aarch64")]
#[test]
fn forcing_avx2_on_aarch64_aborts_boot() {
    let err = Dispatch::new(SimdMode::Avx2).unwrap_err().to_string();
    assert!(err.contains("avx2"), "{err}");
    assert!(err.contains("HWA-032"), "{err}");
}

#[test]
fn selection_appears_in_the_effective_config_boot_event() {
    // HWA-033: the per-kernel selection is part of the serialized
    // effective-configuration event (HWA-012) that boot logs and /health
    // will expose.
    let lookup = |key: &str| (key == "FLUXUM_PROFILE").then(|| "development".to_owned());
    let config = Config::load_with(None, &lookup).unwrap();
    let profile = hw::HardwareProfile {
        logical_cores: 4,
        physical_cores: 4,
        total_ram_bytes: 8 << 30,
        available_ram_bytes: 6 << 30,
        cgroup_cpu_quota: None,
        cgroup_memory_limit_bytes: None,
    };
    let effective = hw::derive(&profile, &config).unwrap();

    let json = serde_json::to_string(&effective).unwrap();
    assert!(json.contains("\"simd_kernels\""), "{json}");
    for kernel in ["crc32c", "hash64", "predicate_i64", "predicate_f64"] {
        assert!(json.contains(kernel), "missing {kernel} in {json}");
    }

    // The logged selection is exactly what dispatch resolves for this mode,
    // and it agrees with the pure selection function on detected features.
    let dispatch = Dispatch::new(config.simd).unwrap();
    assert_eq!(effective.simd_kernels.value, dispatch.selection());
    assert_eq!(
        effective.simd_kernels.value,
        select(config.simd, &CpuFeatures::detect()).unwrap(),
        "HWA-055 self-check unexpectedly demoted a kernel"
    );

    // Emitting the boot event must not panic.
    effective.emit_boot_event();
}

#[test]
fn env_forced_scalar_flows_into_the_selection_with_env_provenance() {
    let lookup = |key: &str| match key {
        "FLUXUM_PROFILE" => Some("development".to_owned()),
        "FLUXUM_SIMD" => Some("scalar".to_owned()),
        _ => None,
    };
    let config = Config::load_with(None, &lookup).unwrap();
    assert_eq!(config.simd, SimdMode::Scalar);
    let profile = hw::HardwareProfile {
        logical_cores: 2,
        physical_cores: 2,
        total_ram_bytes: 4 << 30,
        available_ram_bytes: 3 << 30,
        cgroup_cpu_quota: None,
        cgroup_memory_limit_bytes: None,
    };
    let effective = hw::derive(&profile, &config).unwrap();
    assert_eq!(effective.simd_kernels.value.crc32c, Tier::Scalar);
    assert_eq!(effective.simd_kernels.value.predicate_i64, Tier::Scalar);
    assert_eq!(effective.simd_kernels.source, hw::Provenance::Env);
}

// ---------------------------------------------------------------------------
// Deterministic predicate boundaries
// ---------------------------------------------------------------------------

#[test]
fn predicate_boundary_lengths_and_tail_bits() {
    for &len in BOUNDARY_LENS {
        let ints: Vec<i64> = (0..len as i64).map(|i| i % 5 - 2).collect();
        let floats: Vec<f64> = (0..len as u32)
            .map(|i| match i % 7 {
                0 => f64::NAN,
                1 => -0.0,
                _ => f64::from(i % 5) - 2.0,
            })
            .collect();
        for op in [PredOp::Eq, PredOp::Lt, PredOp::Gt] {
            let want_i = row_at_a_time_i64(op, &ints, 0);
            let want_f = row_at_a_time_f64(op, &floats, 0.0);
            for (name, d) in dispatches() {
                // Pre-poison the buffer: kernels must overwrite every word,
                // including zeroing tail bits past `len`.
                let mut got = vec![u64::MAX; bitmap_words(len)];
                d.eval_i64(op, &ints, 0, &mut got);
                assert_eq!(got, want_i, "{name} i64 {op:?} len={len}");

                let mut got = vec![u64::MAX; bitmap_words(len)];
                d.eval_f64(op, &floats, 0.0, &mut got);
                assert_eq!(got, want_f, "{name} f64 {op:?} len={len}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property suites (HWA-052; failing seeds persist under proptest-regressions/)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn crc32c_parity_and_streaming(
        data in prop::collection::vec(any::<u8>(), 0..512),
        offset in 0usize..8,
        split in 0usize..512,
    ) {
        let oracle = oracle();
        let start = offset.min(data.len());
        let slice = &data[start..]; // misaligned starts
        let want = oracle.crc32c(slice);
        prop_assert_eq!(want, crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(slice));
        for (name, d) in dispatches() {
            prop_assert_eq!(d.crc32c(slice), want, "{}", name);
            let mid = split.min(slice.len());
            let streamed = d.crc32c_extend(d.crc32c(&slice[..mid]), &slice[mid..]);
            prop_assert_eq!(streamed, want, "{} split={}", name, mid);
        }
    }

    #[test]
    fn hash64_parity_and_stability(
        data in prop::collection::vec(any::<u8>(), 0..256),
        seed in any::<u64>(),
    ) {
        // HWA-042: identical across variants AND canonical xxHash64.
        let want = xxhash_rust::xxh64::xxh64(&data, seed);
        for (name, d) in dispatches() {
            prop_assert_eq!(d.hash64(&data, seed), want, "{}", name);
        }
    }

    #[test]
    fn predicate_i64_parity(
        values in prop::collection::vec(biased_i64(), 0..300),
        rhs in biased_i64(),
    ) {
        for op in [PredOp::Eq, PredOp::Lt, PredOp::Gt] {
            let want = row_at_a_time_i64(op, &values, rhs);
            for (name, d) in dispatches() {
                let mut got = vec![u64::MAX; bitmap_words(values.len())];
                d.eval_i64(op, &values, rhs, &mut got);
                prop_assert_eq!(&got, &want, "{} {:?}", name, op);
            }
        }
    }

    #[test]
    fn predicate_f64_parity(
        values in prop::collection::vec(special_f64(), 0..300),
        rhs in special_f64(),
    ) {
        for op in [PredOp::Eq, PredOp::Lt, PredOp::Gt] {
            let want = row_at_a_time_f64(op, &values, rhs);
            for (name, d) in dispatches() {
                let mut got = vec![u64::MAX; bitmap_words(values.len())];
                d.eval_f64(op, &values, rhs, &mut got);
                prop_assert_eq!(&got, &want, "{} {:?}", name, op);
            }
        }
    }
}
