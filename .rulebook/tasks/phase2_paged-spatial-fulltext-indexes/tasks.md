## 1. Implementation
- [ ] 1.1 Design doc: linear-quadtree key mapping for QuadTree points and a packed/linearized layout (or documented fallback) for R-tree extents over `PagedTree`; term/postings + document-statistics page layout for full-text (SPEC-008, SPEC-019, TIER-051)
- [ ] 1.2 Page the spatial index: inserts/removes copy-on-write inside the commit merge, region/radius/point queries decompose into key-range scans with exact-filter semantics preserved (SPX-020/021/023); SPX-031 rebuild lifecycle unchanged
- [ ] 1.3 Page the full-text index: posting lists and BM25 statistics fault/evict under the budget with identical scores (FTS-030); FTS-022 rebuild lifecycle unchanged
- [ ] 1.4 Verification: a spatial-dominated and an FTS-dominated dataset >10x the memory budget stay within budget end-to-end through reducers (TIER-070); spatial e2e + FTS suites green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation (drop the TIER-051 residency caveat from `store/mod.rs` / SPEC-015 notes)
- [ ] 2.2 Write tests covering the new behavior (paged spatial/FTS MVCC equivalence + eviction-under-budget)
- [ ] 2.3 Run tests and confirm they pass
