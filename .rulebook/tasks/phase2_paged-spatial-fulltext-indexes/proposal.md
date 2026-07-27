# Proposal: phase2_paged-spatial-fulltext-indexes

## Why

`phase2_tiered-live-store-integration` moved the primary row map, the
secondary B-tree indexes, and the `#[unique]` constraint maps onto the paged
cold tier (`PagedTree`, SPEC-015 TIER-050): they fault in and evict under
`memory.budget`, so row- and btree-index-dominated datasets are bounded by
the budget (TIER-004/070).

Two index families intentionally stayed **resident**:

- **Spatial** (SPEC-008, `crates/fluxum-core/src/index/` QuadTree/R-tree):
  heap structures with geometric query algorithms (region/radius/point).
- **Full-text** (SPEC-019, `crates/fluxum-core/src/index/fulltext.rs`):
  posting lists plus BM25 document statistics.

Both are unpersisted rebuild-from-rows structures (SPX-031 / FTS-022), so
their memory scales with the number of spatial/full-text-indexed rows, not
with the budget. A dataset dominated by spatial or full-text entries can
therefore still exceed `memory.budget` (TIER-051 gap). This was re-scoped
out of the parent task because each family needs a genuine redesign, not a
substrate swap:

- Spatial paging needs the SPEC-008 linear-quadtree key mapping onto the
  paged B-tree (the `pager/tree.rs` module docs and the cold spill target
  already anticipate the key form), with region/radius decomposition into
  key-range scans; the R-tree needs a packed/linearized layout or an
  explicit fallback.
- Full-text paging needs term→posting-list pages plus paged document
  statistics for BM25, or a bounded-cache design over the paged substrate.

## What Changes

Serve spatial and full-text indexes through the buffer pool so their memory
counts against the one `memory.budget` (TIER-051, TIER-070), preserving the
exact query semantics (SPX-020/021/023 results, FTS-030 scores) and the
SPX-031/FTS-022 rebuild lifecycle. Same MVCC discipline as the other paged
structures: copy-on-write versions, superseded pages retired through the
TIER-061 reclaimer.

## Impact

- Affected specs: SPEC-015 (TIER-051/070), SPEC-008, SPEC-019
- Affected code: `crates/fluxum-core/src/index/` (mod, fulltext),
  `crates/fluxum-core/src/store/` (committed, memstore, pager/cold)
- PRD requirements: NFR-12, NFR-13 (for spatial/FTS-dominated datasets)
- Depends on: phase2_tiered-live-store-integration (substrate + discipline)
- Breaking change: NO (internal storage substitution)
- Risk: MEDIUM-HIGH — geometric/scoring correctness must be preserved
  exactly; the spatial e2e and FTS suites are the guardrails
- User benefit: the bounded-RSS pillar holds for *every* index family, not
  only rows + B-tree/unique

## Notes

Until this lands, the TIER-070 "index-dominated dataset stays bounded"
guarantee holds for B-tree/unique indexes only; `store/mod.rs` and the
SPEC-015 conformance notes state the limitation explicitly.
