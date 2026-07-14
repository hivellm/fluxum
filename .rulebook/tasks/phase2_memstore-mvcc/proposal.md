# Proposal: phase2_memstore-mvcc

## Why
MemStore is the hot tier every transaction, index, and subscription reads through; MVCC with a single writer per shard is what makes lock-free reads and clean rollback possible.

## What Changes
Implement `CommittedState` + `TxState` with MVCC merge-on-commit / discard-on-rollback, lock-free committed reads, and the rollback correctness rules (delete-then-reinsert cancellation, index rebuild equivalence).

## Impact
- DAG task: T2.1
- Affected specs: SPEC-002 (storage engine)
- PRD requirements: FR-10, FR-12, NFR-02
- Affected code: crates/fluxum-core (storage)
- Depends on: G1
- Breaking change: NO
- User benefit: sub-microsecond hot reads with ACID semantics
