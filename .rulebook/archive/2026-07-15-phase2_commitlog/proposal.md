# Proposal: phase2_commitlog

## Why
The append-only CommitLog is the durability story and, later, the replication protocol itself - its entry format freezes at G5, so it must be right early.

## What Changes
Implement the CRC32C+epoch entry format, the group-commit flush actor with a published durable offset, segment rotation, and replay with non-destructive torn-tail repair.

## Impact
- DAG task: T2.2
- Affected specs: SPEC-002 (STG-010..015, STG-031, STG-040/041), SPEC-014 (stream format)
- PRD requirements: FR-10, FR-13, NFR-08
- Affected code: crates/fluxum-core (commitlog)
- Depends on: T2.1
- Breaking change: NO
- User benefit: zero committed-tx loss with fsync cost amortized by group commit
