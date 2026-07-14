# Proposal: phase2_commitlog

## Why
Durability rests entirely on the commit log; group commit amortizes fsync cost and torn-tail repair is what makes crash recovery trustworthy.

## What Changes
Implement CommitLog: append-only "u32 LE + MessagePack + CRC32C" entries with epoch, a group-commit flush actor, segment rotation, and replay with non-destructive torn-tail repair.

## Impact
- DAG task: T2.2
- Affected specs: SPEC-002 (storage engine); entry format freezes with the wire at G5
- PRD requirements: FR-10, FR-13
- Affected code: crates/fluxum-server (storage/commitlog)
- Depends on: T2.1 (phase2_memstore-mvcc)
- Breaking change: NO
- User benefit: committed transactions survive crashes without stalling the write path
