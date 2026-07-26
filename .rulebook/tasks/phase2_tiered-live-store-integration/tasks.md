## 1. Implementation
- [ ] 1.1 Design the MVCC-safe paged snapshot: a cheap, consistent immutable view over the paged store (copy-on-write page roots / versioned page directory) that backs `snapshot()`, `snapshot_as_of()` (SPEC-022), and subscription `InitialData` with the same lock-free-read contract the `imbl::OrdMap` gives today (SPEC-002, TIER-061)
- [ ] 1.2 Back `CommittedState`'s primary row map with the paged B-tree (`pager::tree`/`pager::cold::ColdTable`) instead of `imbl::OrdMap`: point lookup, ordered range scan, and the commit merge fault pages in through the `BufferPool` and evict under `memory.budget` (TIER-003/004, FR-18)
- [ ] 1.3 Page every secondary/spatial index the same way (TIER-050/051), so index memory counts against the one budget and an index-dominated dataset stays bounded (TIER-070)
- [ ] 1.4 Wire construction: `MemStore` opens a `Pager`/`BufferPool` sized from the effective `memory.budget` (already on `EffectiveConfig`), one page file per table per shard (TIER-023); integrate with the T2.3 checkpoint/recovery path (checkpoint root + log replay) so durability is unchanged
- [ ] 1.5 Verification: RSS is bounded by the budget under a >10x-RAM live workload driven end-to-end through reducers (not the pager in isolation) — the `fluxum-bench soak` within-budget assertion passes; DST/crash and subscription-correctness suites stay green (ACID + MVCC unchanged)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation (reconcile the SPEC-015 / store `mod.rs` "tiered live store" claims with reality)
- [ ] 2.2 Write tests covering the new behavior (paged-store MVCC/snapshot equivalence + eviction-under-budget; the soak within-budget path)
- [ ] 2.3 Run tests and confirm they pass
