## 1. Implementation
- [ ] 1.1 Implement `CommittedState` (BTreeMap per table) and `TxState` (in-flight inserts/deletes) in fluxum-core (STG-001..004)
- [ ] 1.2 Implement MVCC merge of `TxState` into `CommittedState` on commit and discard on rollback; committed state never holds a partial transaction (STG-005/STG-006, FR-12)
- [ ] 1.3 Lock-free committed reads: readers never block on the single writer per shard (FR-10, FR-12)
- [ ] 1.4 Rollback correctness rules: delete-then-reinsert cancels to a no-op; unique-constraint checks ignore committed rows tx-deleted in the same transaction; after rollback every index is bit-identical to a fresh rebuild (STG-007, SPEC-002 acceptance 7)
- [ ] 1.5 Criterion benchmark: committed-state point lookup < 1 microsecond on a hot (buffer-pool) hit (NFR-02)
- [ ] 1.6 Verification (DAG exit test): ACID unit tests - insert/delete/query_pk/scan

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
