//! Batched predicate evaluation kernel — subscription filters and scans
//! (SPEC-005) over `i64` / `f64` columns.
//!
//! Contract (HWA-044): the LSB-first selection bitmap produced by any
//! variant equals row-at-a-time evaluation for every input — including NaN,
//! ±0.0, and the zeroed tail bits of the last word. Float comparisons use
//! IEEE *ordered* semantics, exactly Rust's scalar `==` / `<` / `>`
//! operators: the AVX2 variant uses the `_CMP_*_OQ` (ordered, quiet)
//! predicate encodings and NEON's `vceqq`/`vcltq`/`vcgtq` match by
//! definition.
//!
//! Every kernel fills `out` completely: `out.len()` must be exactly
//! `bitmap_words(values.len())` (enforced by the safe API in
//! [`Dispatch`](super::Dispatch)).

use super::{CpuFeatures, PredF64Fn, PredI64Fn, PredOp, Tier};

/// Shared scalar bitmap fill.
#[inline]
fn fill<T: Copy>(values: &[T], out: &mut [u64], pred: impl Fn(T) -> bool) {
    let mut chunks = values.chunks(64);
    for word in out.iter_mut() {
        let mut bits = 0u64;
        if let Some(chunk) = chunks.next() {
            for (bit, &value) in chunk.iter().enumerate() {
                bits |= u64::from(pred(value)) << bit;
            }
        }
        *word = bits;
    }
}

/// Scalar reference over `i64` — the permanent oracle (HWA-051).
pub(super) fn scalar_i64(op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
    match op {
        PredOp::Eq => fill(values, out, |v| v == rhs),
        PredOp::Lt => fill(values, out, |v| v < rhs),
        PredOp::Gt => fill(values, out, |v| v > rhs),
    }
}

/// Scalar reference over `f64` — the permanent oracle (HWA-051).
pub(super) fn scalar_f64(op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
    match op {
        PredOp::Eq => fill(values, out, |v| v == rhs),
        PredOp::Lt => fill(values, out, |v| v < rhs),
        PredOp::Gt => fill(values, out, |v| v > rhs),
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use core::arch::x86_64::{
        _CMP_EQ_OQ, _CMP_GT_OQ, _CMP_LT_OQ, _mm256_castsi256_pd, _mm256_cmp_pd, _mm256_cmpeq_epi64,
        _mm256_cmpgt_epi64, _mm256_loadu_pd, _mm256_loadu_si256, _mm256_movemask_pd,
        _mm256_set1_epi64x, _mm256_set1_pd,
    };

    use super::PredOp;

    /// AVX2 `i64` comparisons, 4 lanes per step, scalar tail per 64-row word.
    #[target_feature(enable = "avx2")]
    fn eval_i64(op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
        let rhs_v = _mm256_set1_epi64x(rhs);
        let ptr = values.as_ptr();
        let n = values.len();
        let mut idx = 0usize;
        for word in out.iter_mut() {
            let end = (idx + 64).min(n);
            let mut bits = 0u64;
            let mut i = idx;
            while i + 4 <= end {
                // SAFETY: `i + 4 <= end <= values.len()`, so the unaligned
                // 32-byte load at `ptr + i` reads in-bounds initialized data.
                let v = unsafe { _mm256_loadu_si256(ptr.add(i).cast()) };
                let mask = match op {
                    PredOp::Eq => _mm256_cmpeq_epi64(v, rhs_v),
                    PredOp::Lt => _mm256_cmpgt_epi64(rhs_v, v),
                    PredOp::Gt => _mm256_cmpgt_epi64(v, rhs_v),
                };
                let lanes = _mm256_movemask_pd(_mm256_castsi256_pd(mask)) as u32;
                bits |= u64::from(lanes & 0xF) << (i - idx);
                i += 4;
            }
            while i < end {
                let hit = match op {
                    PredOp::Eq => values[i] == rhs,
                    PredOp::Lt => values[i] < rhs,
                    PredOp::Gt => values[i] > rhs,
                };
                bits |= u64::from(hit) << (i - idx);
                i += 1;
            }
            *word = bits;
            idx = end;
        }
    }

    /// AVX2 `f64` comparisons with `_CMP_*_OQ` (ordered, quiet) — exactly
    /// Rust's scalar operator semantics for NaN and ±0.0 (HWA-044).
    #[target_feature(enable = "avx2")]
    fn eval_f64(op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
        let rhs_v = _mm256_set1_pd(rhs);
        let ptr = values.as_ptr();
        let n = values.len();
        let mut idx = 0usize;
        for word in out.iter_mut() {
            let end = (idx + 64).min(n);
            let mut bits = 0u64;
            let mut i = idx;
            while i + 4 <= end {
                // SAFETY: `i + 4 <= end <= values.len()`, so the unaligned
                // 32-byte load at `ptr + i` reads in-bounds initialized data.
                let v = unsafe { _mm256_loadu_pd(ptr.add(i)) };
                let mask = match op {
                    PredOp::Eq => _mm256_cmp_pd::<_CMP_EQ_OQ>(v, rhs_v),
                    PredOp::Lt => _mm256_cmp_pd::<_CMP_LT_OQ>(v, rhs_v),
                    PredOp::Gt => _mm256_cmp_pd::<_CMP_GT_OQ>(v, rhs_v),
                };
                let lanes = _mm256_movemask_pd(mask) as u32;
                bits |= u64::from(lanes & 0xF) << (i - idx);
                i += 4;
            }
            while i < end {
                let hit = match op {
                    PredOp::Eq => values[i] == rhs,
                    PredOp::Lt => values[i] < rhs,
                    PredOp::Gt => values[i] > rhs,
                };
                bits |= u64::from(hit) << (i - idx);
                i += 1;
            }
            *word = bits;
            idx = end;
        }
    }

    pub(super) fn eval_i64_avx2(op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
        // SAFETY: `variant_i64` hands this wrapper out only after runtime
        // detection proved AVX2 support (HWA-031/HWA-054).
        unsafe { eval_i64(op, values, rhs, out) }
    }

    pub(super) fn eval_f64_avx2(op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
        // SAFETY: `variant_f64` hands this wrapper out only after runtime
        // detection proved AVX2 support (HWA-031/HWA-054).
        unsafe { eval_f64(op, values, rhs, out) }
    }
}

#[cfg(target_arch = "aarch64")]
mod arm {
    use core::arch::aarch64::{
        vceqq_f64, vceqq_s64, vcgtq_f64, vcgtq_s64, vcltq_f64, vcltq_s64, vdupq_n_f64, vdupq_n_s64,
        vgetq_lane_u64, vld1q_f64, vld1q_s64,
    };

    use super::PredOp;

    /// NEON `i64` comparisons, 2 lanes per step, scalar tail per 64-row
    /// word.
    #[target_feature(enable = "neon")]
    fn eval_i64(op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
        let rhs_v = vdupq_n_s64(rhs);
        let ptr = values.as_ptr();
        let n = values.len();
        let mut idx = 0usize;
        for word in out.iter_mut() {
            let end = (idx + 64).min(n);
            let mut bits = 0u64;
            let mut i = idx;
            while i + 2 <= end {
                // SAFETY: `i + 2 <= end <= values.len()`, so the 16-byte
                // load at `ptr + i` reads in-bounds initialized data.
                let v = unsafe { vld1q_s64(ptr.add(i)) };
                let mask = match op {
                    PredOp::Eq => vceqq_s64(v, rhs_v),
                    PredOp::Lt => vcltq_s64(v, rhs_v),
                    PredOp::Gt => vcgtq_s64(v, rhs_v),
                };
                bits |= (vgetq_lane_u64::<0>(mask) & 1) << (i - idx);
                bits |= (vgetq_lane_u64::<1>(mask) & 1) << (i + 1 - idx);
                i += 2;
            }
            while i < end {
                let hit = match op {
                    PredOp::Eq => values[i] == rhs,
                    PredOp::Lt => values[i] < rhs,
                    PredOp::Gt => values[i] > rhs,
                };
                bits |= u64::from(hit) << (i - idx);
                i += 1;
            }
            *word = bits;
            idx = end;
        }
    }

    /// NEON `f64` comparisons — `vceqq`/`vcltq`/`vcgtq` implement IEEE
    /// ordered semantics, matching scalar Rust operators (HWA-044).
    #[target_feature(enable = "neon")]
    fn eval_f64(op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
        let rhs_v = vdupq_n_f64(rhs);
        let ptr = values.as_ptr();
        let n = values.len();
        let mut idx = 0usize;
        for word in out.iter_mut() {
            let end = (idx + 64).min(n);
            let mut bits = 0u64;
            let mut i = idx;
            while i + 2 <= end {
                // SAFETY: `i + 2 <= end <= values.len()`, so the 16-byte
                // load at `ptr + i` reads in-bounds initialized data.
                let v = unsafe { vld1q_f64(ptr.add(i)) };
                let mask = match op {
                    PredOp::Eq => vceqq_f64(v, rhs_v),
                    PredOp::Lt => vcltq_f64(v, rhs_v),
                    PredOp::Gt => vcgtq_f64(v, rhs_v),
                };
                bits |= (vgetq_lane_u64::<0>(mask) & 1) << (i - idx);
                bits |= (vgetq_lane_u64::<1>(mask) & 1) << (i + 1 - idx);
                i += 2;
            }
            while i < end {
                let hit = match op {
                    PredOp::Eq => values[i] == rhs,
                    PredOp::Lt => values[i] < rhs,
                    PredOp::Gt => values[i] > rhs,
                };
                bits |= u64::from(hit) << (i - idx);
                i += 1;
            }
            *word = bits;
            idx = end;
        }
    }

    pub(super) fn eval_i64_neon(op: PredOp, values: &[i64], rhs: i64, out: &mut [u64]) {
        // SAFETY: `variant_i64` hands this wrapper out only after runtime
        // detection proved NEON support (HWA-031/HWA-054).
        unsafe { eval_i64(op, values, rhs, out) }
    }

    pub(super) fn eval_f64_neon(op: PredOp, values: &[f64], rhs: f64, out: &mut [u64]) {
        // SAFETY: `variant_f64` hands this wrapper out only after runtime
        // detection proved NEON support (HWA-031/HWA-054).
        unsafe { eval_f64(op, values, rhs, out) }
    }
}

/// The `i64` variant implementing `tier` on this CPU, if any.
pub(super) fn variant_i64(tier: Tier, features: &CpuFeatures) -> Option<PredI64Fn> {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = features;
    match tier {
        Tier::Scalar => Some(scalar_i64),
        #[cfg(target_arch = "x86_64")]
        Tier::Avx2 if features.avx2 => Some(x86::eval_i64_avx2),
        #[cfg(target_arch = "aarch64")]
        Tier::Neon if features.neon => Some(arm::eval_i64_neon),
        _ => None,
    }
}

/// The `f64` variant implementing `tier` on this CPU, if any.
pub(super) fn variant_f64(tier: Tier, features: &CpuFeatures) -> Option<PredF64Fn> {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = features;
    match tier {
        Tier::Scalar => Some(scalar_f64),
        #[cfg(target_arch = "x86_64")]
        Tier::Avx2 if features.avx2 => Some(x86::eval_f64_avx2),
        #[cfg(target_arch = "aarch64")]
        Tier::Neon if features.neon => Some(arm::eval_f64_neon),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::bitmap_words;
    use super::*;

    #[test]
    fn scalar_i64_sets_the_expected_bits() {
        let values = [3i64, -1, 5, 3, 0];
        let mut out = vec![0u64; bitmap_words(values.len())];
        scalar_i64(PredOp::Eq, &values, 3, &mut out);
        assert_eq!(out, vec![0b01001]);
        scalar_i64(PredOp::Lt, &values, 3, &mut out);
        assert_eq!(out, vec![0b10010]);
        scalar_i64(PredOp::Gt, &values, 3, &mut out);
        assert_eq!(out, vec![0b00100]);
    }

    #[test]
    fn scalar_f64_follows_ieee_ordered_semantics() {
        let values = [f64::NAN, -0.0, 0.0, 1.5, f64::NEG_INFINITY];
        let mut out = vec![0u64; bitmap_words(values.len())];
        // NaN never matches; -0.0 == 0.0.
        scalar_f64(PredOp::Eq, &values, 0.0, &mut out);
        assert_eq!(out, vec![0b00110]);
        scalar_f64(PredOp::Lt, &values, 0.0, &mut out);
        assert_eq!(out, vec![0b10000]);
        scalar_f64(PredOp::Gt, &values, f64::NAN, &mut out);
        assert_eq!(out, vec![0], "nothing is ordered against NaN");
    }

    #[test]
    fn tail_word_bits_beyond_len_are_zero() {
        let values: Vec<i64> = vec![i64::MIN; 65];
        let mut out = vec![u64::MAX; bitmap_words(values.len())];
        scalar_i64(PredOp::Lt, &values, 0, &mut out);
        assert_eq!(out[0], u64::MAX);
        assert_eq!(out[1], 1, "only bit 0 of the tail word may be set");
    }
}
