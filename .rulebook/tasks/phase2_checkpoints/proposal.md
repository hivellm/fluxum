# Proposal: phase2_checkpoints

## Why
Replaying the full log from genesis does not scale; incremental content-addressed checkpoints bound recovery time and let the log be truncated.

## What Changes
Implement checkpoints as incremental, content-addressed pages (unchanged data shared between checkpoints) with a manifest integrity hash; recovery = latest checkpoint + log replay; plus log truncation.

## Impact
- DAG task: T2.3
- Affected specs: SPEC-002 (storage engine)
- PRD requirements: FR-13, FR-14
- Affected code: crates/fluxum-server (storage/checkpoint)
- Depends on: T2.2 (phase2_commitlog)
- Breaking change: NO
- User benefit: fast restarts with bounded recovery time regardless of dataset age
