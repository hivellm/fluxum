## 1. Implementation
- [ ] 1.1 Extend CompiledPlan additively: access (FullScan | IndexScan{index_id, bounds}), residual filter, ordered_by_index, cursor (QP-050; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.2 Rule-based index selection at compile time: longest equality prefix + single next-column range; score by prefix len, range-binds-next, order-served; deterministic tie-break by declaration order; fall back to FullScan (QP-001)
- [ ] 1.3 WHERE split into index bound conditions vs residual conditions; encode bounds for Snapshot::index_scan (QP-010; committed.rs:325, btree.rs:223)
- [ ] 1.4 IN expansion to per-value bounded index scans up to index_in_expansion_max (default 128), else residual fallback (QP-011)
- [ ] 1.5 Snapshot evaluator branches on access path: FullScan = scan().filter(); IndexScan = index_scan(bounds) then residual + RLS (QP-050; subscription/mod.rs:462)
- [ ] 1.6 Index-ordered ORDER BY: when index order matches (incl. DESC via reverse scan), set ordered_by_index and skip the in-RAM sort (QP-020)
- [ ] 1.7 Index-ordered top-N: with ordered_by_index + LIMIT n, stop after n rows passing residual + RLS; apply RLS/masking within the scan before counting toward LIMIT (QP-021/022)
- [ ] 1.8 Transparency property test: IndexScan result set byte-identical to forced FullScan across a generated query corpus; access path deterministic across recompiles (QP-002)
- [ ] 1.9 /query/explain (or fluxum query explain): chosen index, bounds, residual, index-served-order flag (QP-051)
- [ ] 1.10 Verification: rows-scanned + sort-invoked counters prove range pushdown and no-sort top-N on the marketplace query; SUB correctness property test (10k mutations) still green with planner enabled

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
