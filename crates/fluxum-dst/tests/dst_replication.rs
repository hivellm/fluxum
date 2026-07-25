//! Replication deterministic simulation (SPEC-014 §3/§5; TST-134; G7 input).
//!
//! A seeded primary produces a commit-log stream; a replica applies it
//! through the real [`fluxum_core::store::MemStore::apply_replica_record`]
//! under seeded message faults — dropped batches recovered by an offset
//! resync (REP-013) and duplicated batches (idempotent replay, REP-014).
//! Every run asserts the replication contract: over the delivered prefix
//! the replica's `CommittedState` equals the primary's, and its commit log
//! is byte-identical (REP-010). Each seed executes twice for the TST-130
//! determinism check.
//!
//! Per-PR: a bounded multi-seed run. Nightly → G7: raise the volume via
//! `FLUXUM_DST_SEEDS` / `FLUXUM_DST_OPS`. A failure prints the seed;
//! reproduce it with `FLUXUM_DST_SEED=<n> cargo test -p fluxum-dst
//! --test dst_replication`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_dst::replication::run_seed_checked;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ops() -> usize {
    usize::try_from(env_u64("FLUXUM_DST_OPS", 90)).unwrap()
}

/// One pinned seed via `FLUXUM_DST_SEED`, or `FLUXUM_DST_SEEDS` (default 10)
/// sequential seeds off a fixed base distinct from the storage sim's.
fn seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("FLUXUM_DST_SEED") {
        return vec![seed.parse().expect("FLUXUM_DST_SEED must be a u64")];
    }
    let count = env_u64("FLUXUM_DST_SEEDS", 10);
    (0..count).map(|i| 0xB2_0000 + i).collect()
}

#[test]
fn replication_converges_under_message_faults() {
    let ops = ops();
    let (mut commits, mut resyncs, mut duplicates) = (0u64, 0u64, 0u64);
    for seed in seeds() {
        // Every invariant (convergence + byte-identity + determinism) is
        // asserted inside the run; here we only tally what the faults did.
        let report = run_seed_checked(seed, ops);
        commits += report.commits;
        resyncs += report.resyncs;
        duplicates += report.duplicates;
    }
    // Prove the fault paths were actually taken — the invariants held
    // THROUGH the faults, not because the fault never fired.
    assert!(commits > 0, "the primary committed nothing");
    assert!(
        resyncs > 0,
        "no dropped-batch resync was exercised (REP-013)"
    );
    assert!(
        duplicates > 0,
        "no duplicate delivery was exercised (REP-014)"
    );
}
