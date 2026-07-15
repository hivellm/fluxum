//! Seeded deterministic RNG (splitmix64) — every simulation decision and
//! every fault probability derives from the run seed alone (TST-131), so a
//! failure reproduces from its seed with no other state.

/// Deterministic PRNG: splitmix64. Small, fast, dependency-free, and stable
/// across platforms and releases — the reproducibility contract of TST-131.
#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// A generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Derive an independent stream (e.g. the fault plan) from this seed:
    /// same seed ⇒ same fork, different `stream` ⇒ decorrelated sequence.
    pub fn fork(&self, stream: u64) -> Self {
        let mut forked = Self {
            state: self.state ^ stream.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        };
        forked.next_u64(); // decorrelate from the parent state
        forked
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `0..n` (`n` must be > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next_u64() % n
    }

    /// Uniform `usize` index in `0..n`.
    pub fn index(&mut self, n: usize) -> usize {
        usize::try_from(self.below(n as u64)).unwrap_or(0)
    }

    /// Bernoulli draw: true with probability `percent`/100.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = SimRng::new(42);
        let mut b = SimRng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn forks_are_decorrelated_but_deterministic() {
        let base = SimRng::new(7);
        let mut f1 = base.fork(1);
        let mut f2 = base.fork(2);
        let mut f1b = SimRng::new(7).fork(1);
        assert_ne!(f1.next_u64(), f2.next_u64());
        let _ = f1b.next_u64();
        assert_eq!(f1.next_u64(), f1b.next_u64());
    }
}
