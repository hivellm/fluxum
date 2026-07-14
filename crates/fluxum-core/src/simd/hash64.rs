//! xxHash64 kernel — partition routing (SPEC-007) and index hashing.
//!
//! Hash stability (HWA-042): the output is identical across all ISA
//! variants, platforms, and endianness — all multi-byte reads are explicitly
//! little-endian, matching canonical xxHash64. A divergent hash would
//! silently misroute rows to the wrong shard, so any future algorithm change
//! must be versioned and migrated, never swapped in place.
//!
//! The scalar implementation is the oracle (HWA-051) and currently the only
//! registered variant: neither AVX2 nor NEON provides the 64-bit lane
//! multiply xxHash64 leans on, so a hand-rolled single-message variant has
//! no plausible HWA-060 speedup to demonstrate. A batched multi-message
//! variant may register here later — with criterion evidence, per HWA-060.

use super::{CpuFeatures, Hash64Fn, Tier};

const PRIME_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME_5: u64 = 0x27D4_EB2F_1656_67C5;

#[inline]
fn read_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}

#[inline]
fn read_u32(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(buf)
}

#[inline]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME_2))
        .rotate_left(31)
        .wrapping_mul(PRIME_1)
}

#[inline]
fn merge_round(acc: u64, val: u64) -> u64 {
    (acc ^ round(0, val))
        .wrapping_mul(PRIME_1)
        .wrapping_add(PRIME_4)
}

/// Scalar reference — canonical xxHash64 and the permanent oracle (HWA-051).
pub(super) fn scalar(data: &[u8], seed: u64) -> u64 {
    let mut rest = data;
    let mut h = if data.len() >= 32 {
        let mut v1 = seed.wrapping_add(PRIME_1).wrapping_add(PRIME_2);
        let mut v2 = seed.wrapping_add(PRIME_2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME_1);
        while rest.len() >= 32 {
            v1 = round(v1, read_u64(rest));
            v2 = round(v2, read_u64(&rest[8..]));
            v3 = round(v3, read_u64(&rest[16..]));
            v4 = round(v4, read_u64(&rest[24..]));
            rest = &rest[32..];
        }
        let acc = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        let acc = merge_round(acc, v1);
        let acc = merge_round(acc, v2);
        let acc = merge_round(acc, v3);
        merge_round(acc, v4)
    } else {
        seed.wrapping_add(PRIME_5)
    };

    h = h.wrapping_add(data.len() as u64);

    while rest.len() >= 8 {
        h ^= round(0, read_u64(rest));
        h = h
            .rotate_left(27)
            .wrapping_mul(PRIME_1)
            .wrapping_add(PRIME_4);
        rest = &rest[8..];
    }
    if rest.len() >= 4 {
        h ^= u64::from(read_u32(rest)).wrapping_mul(PRIME_1);
        h = h
            .rotate_left(23)
            .wrapping_mul(PRIME_2)
            .wrapping_add(PRIME_3);
        rest = &rest[4..];
    }
    for &byte in rest {
        h ^= u64::from(byte).wrapping_mul(PRIME_5);
        h = h.rotate_left(11).wrapping_mul(PRIME_1);
    }

    h ^= h >> 33;
    h = h.wrapping_mul(PRIME_2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME_3);
    h ^ (h >> 32)
}

/// The variant implementing `tier`, if any — scalar only for now (see
/// module docs for the HWA-060 rationale).
pub(super) fn variant(tier: Tier, features: &CpuFeatures) -> Option<Hash64Fn> {
    let _ = features;
    match tier {
        Tier::Scalar => Some(scalar),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_canonical_empty_vector() {
        assert_eq!(scalar(b"", 0), 0xEF46_DB37_51D8_E999);
    }

    #[test]
    fn matches_the_reference_implementation() {
        // `xxhash-rust` is the external algorithm pin (HWA-042): our scalar
        // oracle must be canonical xxHash64, not merely self-consistent.
        let data: Vec<u8> = (0..200u32)
            .map(|i| (i.wrapping_mul(151) >> 2) as u8)
            .collect();
        for len in [
            0, 1, 3, 4, 7, 8, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 200,
        ] {
            for seed in [0u64, 1, 0xDEAD_BEEF, u64::MAX] {
                assert_eq!(
                    scalar(&data[..len], seed),
                    xxhash_rust::xxh64::xxh64(&data[..len], seed),
                    "len={len} seed={seed:#x}"
                );
            }
        }
    }
}
