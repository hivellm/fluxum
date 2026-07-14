## 1. Implementation
- [x] 1.1 Implement the R-tree bounding-box index: insert/delete/update, query_region (FR-61)
- [x] 1.2 Implement `IN REGION (x, y, w, h)` and `WITHIN RADIUS r OF (x, y)` predicate evaluation resolved via the spatial index, never a full scan (FR-62): bounding-box prefilter + exact Euclidean post-filter for radius (SPEC-008 acceptance 3)
- [x] 1.3 Error paths: spatial predicate on a table without `#[spatial]` returns 400; negative w/h/r rejected with 400; spatial queries during post-crash index rebuild return 503 "spatial index not ready" and the shard defers ReducerCalls until rebuild completes (SPEC-008 acceptance 4/5)
- [x] 1.4 Verification (DAG exit test): criterion benchmark - 1M indexed rows, IN REGION selecting ~1000 rows at least 10x faster than the O(n) full scan, scaling consistent with O(log n + k)
- [x] 1.5 Gate G2 input (and unblocks T4.1 spatial SQL predicates)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
