# Proposal: phase2_paged-cold-tier-buffer-pool

## Why
Serving datasets far larger than RAM under a hard memory.budget is a core differentiator (SpacetimeDB is RAM-bound); paged evictable indexes are the novel piece.

## What Changes
Implement the paged cold tier + buffer pool: on-disk page format (FluxBIN rows + per-page checksum), clock-LRU eviction, memory.budget enforcement, fault-in/evict paths, and paged evictable indexes.

## Impact
- DAG task: T2.8
- Affected specs: SPEC-015 (tiered storage); page format freezes at G5
- PRD requirements: FR-18, FR-110, NFR-12
- Affected code: crates/fluxum-server (storage/tier)
- Depends on: T2.1 (phase2_memstore-mvcc)
- Breaking change: NO
- User benefit: datasets 10x beyond RAM served correctly on small machines, memory budget never exceeded
