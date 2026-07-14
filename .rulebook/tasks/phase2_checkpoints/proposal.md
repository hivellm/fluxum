# Proposal: phase2_checkpoints

## Why
Without checkpoints, recovery time and log size grow without bound; incremental content-addressed checkpoints avoid the full-dump scaling cliff and enable log compaction and later backups.

## What Changes
Implement incremental content-addressed checkpoints with manifest integrity, checkpoint+replay recovery with fallback to older checkpoints, and log truncation routed through an archival hook (PITR prerequisite).

## Impact
- DAG task: T2.3
- Affected specs: SPEC-002 (STG-020..022), SPEC-014 (PITR source), SPEC-015 (shared page objects)
- PRD requirements: FR-13, FR-14, FR-104 (prerequisite)
- Affected code: crates/fluxum-core (checkpoint)
- Depends on: T2.2
- Breaking change: NO
- User benefit: fast recovery and bounded disk usage without blocking writers
