//! The storage/commitlog DST suite (SPEC-013 TST-130..TST-134; T2.7
//! checklist 1.4) — the G2-gating run.
//!
//! Per-PR: a bounded multi-seed run (each seed executed twice for the
//! TST-130 determinism check). Nightly / G2: raise the volume via
//! `FLUXUM_DST_SEEDS` / `FLUXUM_DST_OPS`. A failure prints the seed;
//! reproduce it exactly with `FLUXUM_DST_SEED=<n> cargo test -p fluxum-dst`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::panic::AssertUnwindSafe;

use fluxum_dst::{run_seed, run_seed_checked};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ops() -> usize {
    usize::try_from(env_u64("FLUXUM_DST_OPS", 220)).unwrap()
}

/// The seeds this run covers: one pinned seed via `FLUXUM_DST_SEED`, or
/// `FLUXUM_DST_SEEDS` (default 6) sequential seeds off a fixed base.
fn seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("FLUXUM_DST_SEED") {
        return vec![seed.parse().expect("FLUXUM_DST_SEED must be a u64")];
    }
    let count = env_u64("FLUXUM_DST_SEEDS", 6);
    (0..count).map(|i| 0xF1_0000 + i).collect()
}

#[test]
fn dst_storage_commitlog_with_model_oracle() {
    let ops = ops();
    for seed in seeds() {
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| run_seed_checked(seed, ops)));
        match outcome {
            Ok(report) => {
                // Every run must actually exercise the machinery it claims
                // to: commits, rejections (acceptance parity), and at least
                // the mandatory final crash/recovery cycle.
                assert!(report.commits > 0, "seed {seed}: no commits");
                assert!(report.crashes > 0, "seed {seed}: no crash/recovery cycles");
                eprintln!(
                    "dst seed {seed}: {} commits, {} aborts, {} rejections, {} checkpoints, \
                     {} crashes, {} trace checkpoints",
                    report.commits,
                    report.aborts,
                    report.rejections,
                    report.checkpoints,
                    report.crashes,
                    report.trace.len()
                );
            }
            Err(cause) => {
                eprintln!(
                    "DST FAILURE — reproduce with: FLUXUM_DST_SEED={seed} FLUXUM_DST_OPS={ops} \
                     cargo test -p fluxum-dst"
                );
                std::panic::resume_unwind(cause);
            }
        }
    }
}

/// The determinism property in isolation: two runs of one seed produce
/// identical traces and identical reports (TST-130).
#[test]
fn same_seed_produces_identical_traces() {
    let a = run_seed(0xDE7E_2401, 120);
    let b = run_seed(0xDE7E_2401, 120);
    assert_eq!(a, b, "same seed must reproduce the identical run");
    assert!(!a.trace.is_empty());
}
