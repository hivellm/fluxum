## 1. Implementation
- [ ] 1.1 Implement `ReducerContext` (identity, connection_id, timestamp, shard info) handed to every reducer (RED-001/RED-002)
- [ ] 1.2 Implement `TxHandle`: insert / delete / upsert / query_pk / scan / scan_where (FR-20)
- [ ] 1.3 Implement intra-transaction reads: scan_pending / scan_all (committed union pending, deduplicated by PK) / count_pending (FR-17, RED-003, TXN-050/TXN-051)
- [ ] 1.4 Implement nested reducer calls `ctx.tx.call` sharing one transaction: callee Err propagated = both write sets roll back; handled error = caller commits (RED-005)
- [ ] 1.5 Verification (DAG exit test): TxHandle drives all reducer tests; intra-tx visibility suite (scan excludes pending, scan_pending exact, scan_all deduplicated union)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
