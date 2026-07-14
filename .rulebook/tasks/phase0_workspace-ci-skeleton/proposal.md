# Proposal: phase0_workspace-ci-skeleton

## Why
DAG task T0.1 (Cargo workspace + CI skeleton) had no owning rulebook task - found by the plan-vs-tasks coverage audit. Most of it already landed (5 crates, 3-OS nextest CI, lint workflow, codespell, CHANGELOG), but the ROADMAP M0 layout also names `crates/fluxum-bench` and `sdks/rust`, and CI pins stable while the PRD mandates the nightly toolchain.

## What Changes
Add the `fluxum-bench` and `sdks/rust` placeholder crates to the workspace, align CI with `rust-toolchain.toml` (nightly), and verify the full quality pipeline (fmt + clippy -D warnings + nextest on 3 OSes) is green on the skeleton.

## Impact
- DAG task: T0.1
- Affected specs: SPEC-013 (quality gates TST-006, TST-010..012)
- PRD requirements: NFR-09
- Affected code: Cargo.toml (workspace members), crates/fluxum-bench, sdks/rust, .github/workflows
- Depends on: nothing (first task)
- Breaking change: NO
- User benefit: every later task lands on a complete workspace with the family quality gate enforced from day one
