## 1. Implementation
- [ ] 1.1 Implement the commit pipeline: validate -> merge CommittedState -> append CommitLog -> respond; rollback discards TxState (FR-11, TXN-001..)
- [ ] 1.2 Monotonic gap-free `tx_id` per shard; after kill -9 + replay the next commit uses last_replayed_tx_id + 1 (TXN-030)
- [ ] 1.3 Constraints: PK uniqueness (single + composite) with descriptive conflict errors, `#[unique]` checks, auto-inc assignment returned pre-commit with no reuse of rolled-back IDs, upsert semantics (TXN-040..TXN-042, FR-15)
- [ ] 1.4 Single-writer serialization: bounded reducer queue; filling past capacity returns 503 "shard busy" immediately without blocking the transport (TXN-010/TXN-011)
- [ ] 1.5 Atomicity property test (proptest): reducers ending in Err or panic leave CommittedState byte-identical; successful reducers apply exactly their write set (SPEC-003 acceptance 1)
- [ ] 1.6 Criterion benchmark: commit p99 of the small-write reducer < 1 ms with async log writes (NFR-03, SPEC-003 acceptance 10)
- [ ] 1.7 Verification (DAG exit test): concurrent-read/sequential-write harness (serial commit history under N concurrent clients)
- [ ] 1.8 Gate G3 input: rollback suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
