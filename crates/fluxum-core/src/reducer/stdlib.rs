//! Determinism-preserving reducer standard library (SPEC-026 SEC-020/021).
//!
//! Reducers must be **deterministic**: the same inputs produce the same writes
//! on every shard and every re-execution (DST, SPEC-011). Two ambient sources
//! break that — the OS RNG and the wall clock — so a reducer that needs
//! randomness or time-bucketing reaches for these helpers instead of
//! `rand`/`SystemTime`:
//!
//! - [`Rng`] — a small-state PRNG seeded **from the transaction**
//!   (`tx_id`, `shard_id`), with no OS entropy. The same transaction always
//!   produces the same sequence, and two shards replaying the same logical
//!   transaction agree. Exposed as `ctx.rng()`.
//! - Logical-time helpers ([`ReducerContext::time_bucket`] and friends) derive
//!   from `ctx.timestamp` (the commit timestamp, itself deterministic under
//!   DST) — never from `SystemTime::now()`.
//!
//! The whole call tree of one transaction (a reducer plus every reducer it
//! calls, RED-005) shares **one** [`Rng`] stream, so nested calls draw distinct
//! values yet the combined sequence stays reproducible.

use std::cell::Cell;

/// Derive the deterministic per-transaction RNG seed from `(tx_id, shard_id)`
/// (SEC-020). A SplitMix64 finalizer over the two mixed inputs, so adjacent
/// tx ids and shard ids still yield well-separated seeds.
pub(crate) fn seed_from(tx_id: u64, shard_id: u32) -> u64 {
    // Mix the shard into the high bits so shard 0 vs 1 diverge immediately.
    let mixed = tx_id
        ^ (u64::from(shard_id)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left(32));
    let mut z = mixed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A deterministic, seedable SplitMix64 generator (SEC-020) — no OS entropy,
/// interior-mutable so `&self` reducer code can advance it. One instance backs
/// the whole transaction call tree via `ctx.rng()`.
#[derive(Debug)]
pub struct Rng {
    state: Cell<u64>,
}

impl Rng {
    /// Seed the generator (internal — reducers get a seeded [`Rng`] from the
    /// engine via `ctx.rng()`; tests may seed one directly).
    pub fn new(seed: u64) -> Self {
        Self {
            state: Cell::new(seed),
        }
    }

    /// The next 64 uniform random bits (SplitMix64), advancing the state.
    pub fn next_u64(&self) -> u64 {
        let z0 = self.state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.state.set(z0);
        let mut z = z0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform `u32`.
    pub fn next_u32(&self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// A uniform value in `[0, n)` (SEC-020). Returns `0` for `n == 0`. Uses
    /// Lemire's multiply-shift to avoid modulo bias.
    pub fn below(&self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        // 128-bit multiply, take the high 64 bits: uniform in [0, n) with only
        // negligible bias (no rejection loop — determinism over perfection).
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// A uniform value in `[low, high)` (SEC-020). Returns `low` when the range
    /// is empty (`high <= low`).
    pub fn range(&self, low: i64, high: i64) -> i64 {
        if high <= low {
            return low;
        }
        let span = high.wrapping_sub(low) as u64;
        low.wrapping_add(self.below(span) as i64)
    }

    /// A fair coin.
    pub fn bool(&self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// A uniform `f64` in `[0.0, 1.0)` (53-bit mantissa precision).
    pub fn f64(&self) -> f64 {
        // Top 53 bits / 2^53.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Fill `buf` with deterministic random bytes (SEC-020).
    pub fn fill(&self, buf: &mut [u8]) {
        let mut chunks = buf.chunks_exact_mut(8);
        for chunk in &mut chunks {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let tail = chunks.into_remainder();
        if !tail.is_empty() {
            let bytes = self.next_u64().to_le_bytes();
            tail.copy_from_slice(&bytes[..tail.len()]);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_reproduces_the_sequence() {
        let a = Rng::new(seed_from(42, 7));
        let b = Rng::new(seed_from(42, 7));
        let seq_a: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b, "same (tx_id, shard) ⇒ same stream");
    }

    #[test]
    fn different_tx_or_shard_diverge() {
        assert_ne!(seed_from(42, 7), seed_from(43, 7), "tx id matters");
        assert_ne!(seed_from(42, 7), seed_from(42, 8), "shard matters");
        let a: Vec<u64> = {
            let r = Rng::new(seed_from(1, 0));
            (0..8).map(|_| r.next_u64()).collect()
        };
        let b: Vec<u64> = {
            let r = Rng::new(seed_from(2, 0));
            (0..8).map(|_| r.next_u64()).collect()
        };
        assert_ne!(a, b);
    }

    #[test]
    fn below_is_bounded_and_zero_safe() {
        let r = Rng::new(123);
        for _ in 0..1000 {
            assert!(r.below(10) < 10);
        }
        assert_eq!(r.below(0), 0);
        assert_eq!(r.below(1), 0, "the only value in [0,1)");
    }

    #[test]
    fn range_is_bounded_and_empty_safe() {
        let r = Rng::new(9);
        for _ in 0..1000 {
            let v = r.range(-5, 5);
            assert!((-5..5).contains(&v));
        }
        assert_eq!(r.range(3, 3), 3, "empty range returns low");
        assert_eq!(r.range(10, 2), 10, "reversed range returns low");
    }

    #[test]
    fn f64_is_in_unit_interval_and_fill_is_deterministic() {
        let r = Rng::new(55);
        for _ in 0..1000 {
            let v = r.f64();
            assert!((0.0..1.0).contains(&v));
        }
        let mut a = [0u8; 20];
        let mut b = [0u8; 20];
        Rng::new(77).fill(&mut a);
        Rng::new(77).fill(&mut b);
        assert_eq!(a, b, "same seed fills identically");
        // A different seed almost certainly differs.
        let mut c = [0u8; 20];
        Rng::new(78).fill(&mut c);
        assert_ne!(a, c);
    }
}
