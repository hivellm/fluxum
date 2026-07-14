# Proposal: phase2_paged-cold-tier-buffer-pool

## Why
Datasets bounded by disk instead of RAM is a core pillar vs SpacetimeDB; the own page format, buffer pool, and paged evictable indexes are the novel storage work, and the page format freezes at G5.

## What Changes
Implement the page format (FluxBIN rows + per-page CRC32C), clock-LRU buffer pool with pin/unpin and fault-in, `memory.budget` enforcement, and paged evictable indexes, plus the 10x-dataset correctness suite.

## Impact
- DAG task: T2.8
- Affected specs: SPEC-015 (TIER-*), SPEC-016 (budget from probe)
- PRD requirements: FR-18, FR-110, NFR-02, NFR-07, NFR-12
- Affected code: crates/fluxum-core (pager, bufferpool)
- Depends on: T2.1
- Breaking change: NO
- User benefit: a 200 GB dataset runs on a 4 GB VM without OOM
