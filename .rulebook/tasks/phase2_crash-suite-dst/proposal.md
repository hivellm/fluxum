# Proposal: phase2_crash-suite-dst

## Why
Durability claims are worthless without adversarial proof; the crash suite and deterministic simulation are the permanent evidence that no committed transaction is ever lost.

## What Changes
Build the crash suite: kill -9 harness at every commit boundary, CRC bit-flip drills (log and pages), a 10 GB recovery benchmark, and a deterministic simulation (DST) suite for storage/commitlog with seeded runtime, fault injection, and a model oracle.

## Impact
- DAG task: T2.7
- Affected specs: SPEC-013 (testing and conformance)
- PRD requirements: FR-13, NFR-06
- Affected code: crates/fluxum-server tests, CI workflows
- Depends on: T2.2 (phase2_commitlog), T2.3 (phase2_checkpoints), T2.8 (phase2_paged-cold-tier-buffer-pool)
- Breaking change: NO
- User benefit: proven zero-loss durability and bounded recovery time
