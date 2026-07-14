## 1. Implementation
- [x] 1.1 Implement the QuadTree spatial index (BTreeMap-backed, no pointer chasing): insert, query_point, query_radius, delete (FR-60, SPX-001..)
- [x] 1.2 Configurable bucket size, default 8; non-default bucket size produces identical query results (SPX-003)
- [x] 1.3 Update coherence: after an upsert moving a row's coordinates, old-location queries no longer return the row and new-location queries do - no stale entries (SPX-032)
- [x] 1.4 Advisory event-stream lint: `#[spatial]` on append-heavy log-like tables emits the non-fatal advisory (SPX-040)
- [x] 1.5 Verification (DAG exit test): proptest correctness vs a brute-force O(n) reference over randomized insert/delete/update workloads, including boundary rows (points on edges, distance exactly r)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
