## 1. Implementation
- [ ] 1.1 Implement single-column B-tree secondary indexes maintained on commit (FR-16 P0)
- [ ] 1.2 Implement multi-column composite indexes with order-preserving key encoding, supporting prefix scans (FR-16 P1)
- [ ] 1.3 Index maintenance on rollback: after any rollback the index is bit-identical to a freshly rebuilt index over CommittedState (STG-007)
- [ ] 1.4 Design index pages to be paged/evictable under the memory budget from day one (integration lands with T2.8 paged cold tier; SPEC-015 TIER-050)
- [ ] 1.5 Verification (DAG exit test): index consistency property tests - equality/range scans on single indexes and prefix scans on composite indexes return exactly the rows a full scan would (SPEC-001 acceptance 7)
- [ ] 1.6 Gate G2 input: index suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
