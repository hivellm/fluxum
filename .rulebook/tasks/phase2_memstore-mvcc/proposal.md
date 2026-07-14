# Proposal: phase2_memstore-mvcc

## Why
The in-memory store is the hot path of the whole database; MVCC separation of committed vs in-flight state is what makes lock-free reads and clean rollback possible.

## What Changes
Implement MemStore: CommittedState (BTreeMap per table) + TxState (in-flight inserts/deletes), MVCC merge on commit, discard on rollback, and lock-free committed reads.

## Impact
- DAG task: T2.1
- Affected specs: SPEC-002 (storage engine)
- PRD requirements: FR-10, FR-12
- Affected code: crates/fluxum-server (storage module)
- Depends on: G1
- Breaking change: NO
- User benefit: sub-millisecond reads that never block behind writers
