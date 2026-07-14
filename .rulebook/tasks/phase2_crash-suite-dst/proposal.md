# Proposal: phase2_crash-suite-dst

## Why
Crash safety is the product's non-negotiable (PRD design philosophy 4); the kill -9 matrix, corruption drills, and deterministic simulation are what prove zero committed-tx loss before anything is built on top.

## What Changes
Build the kill -9 harness across all commit and checkpoint boundaries, CRC bit-flip drills on log and pages, the 10 GB recovery benchmark, the seeded DST suite with model oracle, and process-level restart drills.

## Impact
- DAG task: T2.7
- Affected specs: SPEC-013 (TST-020..026, TST-130..141), SPEC-002, SPEC-015
- PRD requirements: FR-13, NFR-06, NFR-08
- Affected code: crates/fluxum-core tests, test harness crates
- Depends on: T2.2, T2.3, T2.8
- Breaking change: NO
- User benefit: provable durability - the G2 gate blocks everything downstream until this is green
