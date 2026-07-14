//! CRC-32C (Castagnoli) kernel — commit-log entry and page checksums
//! (SPEC-002 STG-011, SPEC-015).
//!
//! Polynomial discipline (HWA-041): every variant computes CRC-32C —
//! reflected polynomial `0x82F63B78`, init `0xFFFFFFFF`, final inversion —
//! exactly. The x86-64 SSE4.2 `crc32` instruction and the aarch64 CRC
//! extension (`__crc32c*`) both implement this polynomial in hardware by
//! definition; a mismatch would corrupt recovery, replication, and PITR.
//!
//! Kernel contract: `(state, bytes) -> state` over the raw (pre-inverted)
//! CRC register; the init/final inversions live in
//! [`Dispatch::crc32c`](super::Dispatch::crc32c) so that streaming
//! (`crc32c_extend`) composes.

use super::{CpuFeatures, Crc32cFn, Tier};

/// CRC-32C reflected polynomial (Castagnoli).
const POLY: u32 = 0x82F6_3B78;

/// Byte-at-a-time lookup table, built at compile time.
static TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Scalar reference — the permanent oracle (HWA-051).
pub(super) fn scalar_update(mut crc: u32, data: &[u8]) -> u32 {
    for &byte in data {
        crc = (crc >> 8) ^ TABLE[((crc ^ byte as u32) & 0xFF) as usize];
    }
    crc
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    /// Hardware CRC-32C via the SSE4.2 `crc32` instruction, 8 bytes per
    /// step, byte tail. The instruction computes the Castagnoli polynomial
    /// by definition (HWA-041).
    #[target_feature(enable = "sse4.2")]
    fn update(crc: u32, data: &[u8]) -> u32 {
        use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
        let (chunks, tail) = data.as_chunks::<8>();
        let mut state = u64::from(crc);
        for chunk in chunks {
            state = _mm_crc32_u64(state, u64::from_le_bytes(*chunk));
        }
        let mut crc = state as u32;
        for &byte in tail {
            crc = _mm_crc32_u8(crc, byte);
        }
        crc
    }

    pub(super) fn update_sse42(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: `variant` hands this wrapper out only after runtime
        // detection proved SSE4.2 support (HWA-031/HWA-054), so the
        // target-feature precondition of `update` holds.
        unsafe { update(crc, data) }
    }
}

#[cfg(target_arch = "aarch64")]
mod arm {
    /// Hardware CRC-32C via the aarch64 CRC extension, 8 bytes per step,
    /// byte tail. `__crc32c*` compute the Castagnoli polynomial by
    /// definition (HWA-041).
    #[target_feature(enable = "crc")]
    fn update(mut crc: u32, data: &[u8]) -> u32 {
        use core::arch::aarch64::{__crc32cb, __crc32cd};
        let (chunks, tail) = data.as_chunks::<8>();
        for chunk in chunks {
            crc = __crc32cd(crc, u64::from_le_bytes(*chunk));
        }
        for &byte in tail {
            crc = __crc32cb(crc, byte);
        }
        crc
    }

    pub(super) fn update_crc_ext(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: `variant` hands this wrapper out only after runtime
        // detection proved the aarch64 CRC extension (HWA-031/HWA-054), so
        // the target-feature precondition of `update` holds.
        unsafe { update(crc, data) }
    }
}

/// The variant implementing `tier` on this CPU, if any. The aarch64
/// hardware CRC registers at the NEON tier slot (the aarch64 accelerated
/// tier) but is additionally gated on the CRC extension being detected.
pub(super) fn variant(tier: Tier, features: &CpuFeatures) -> Option<Crc32cFn> {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = features;
    match tier {
        Tier::Scalar => Some(scalar_update),
        #[cfg(target_arch = "x86_64")]
        Tier::Sse42 if features.sse42 => Some(x86::update_sse42),
        #[cfg(target_arch = "aarch64")]
        Tier::Neon if features.crc => Some(arm::update_crc_ext),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc(data: &[u8]) -> u32 {
        !scalar_update(!0, data)
    }

    #[test]
    fn scalar_matches_the_iscsi_check_vector() {
        // The canonical CRC-32C check value (RFC 3720 / CRC-32-ISCSI).
        assert_eq!(crc(b"123456789"), 0xE306_9283);
        assert_eq!(crc(b""), 0);
        // 32 bytes of zeros — the iSCSI test pattern.
        assert_eq!(crc(&[0u8; 32]), 0x8A91_36AA);
        // 32 bytes of 0xFF.
        assert_eq!(crc(&[0xFFu8; 32]), 0x62A8_AB43);
    }

    #[test]
    fn scalar_update_streams() {
        let data = b"the quick brown fox jumps over the lazy dog";
        for split in 0..data.len() {
            let whole = scalar_update(!0, data);
            let streamed = scalar_update(scalar_update(!0, &data[..split]), &data[split..]);
            assert_eq!(streamed, whole, "split={split}");
        }
    }

    #[test]
    fn hardware_variant_matches_scalar_when_available() {
        let features = CpuFeatures::detect();
        let tier = if cfg!(target_arch = "x86_64") {
            Tier::Sse42
        } else {
            Tier::Neon
        };
        let Some(hw) = variant(tier, &features) else {
            return; // no hardware CRC on this machine — parity CI covers it
        };
        let data: Vec<u8> = (0..255u8).collect();
        for len in [0, 1, 7, 8, 9, 63, 64, 65, 255] {
            assert_eq!(
                hw(!0, &data[..len]),
                scalar_update(!0, &data[..len]),
                "len={len}"
            );
        }
    }
}
