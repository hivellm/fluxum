# Proposal: phase2_tiered-live-store-integration

## Why

The billion-row soak (T7.7, `phase7_billion-row-soak-droplet-validation`)
empirically fails the TIER-004/NFR-13 "bounded RSS" guarantee, and the cause is
a real architectural gap, not a test artifact:

- The paged cold tier (T2.8, `crates/fluxum-core/src/store/pager/`) is built and
  unit-tested as a standalone module — buffer pool (clock-LRU, pin/unpin,
  fault-in/evict), the on-disk page format, paged B-trees for the primary map
  and every index, and `memory.budget` enforcement. Its module doc claims
  "steady-state RSS is a function of the budget, never of rows on disk."
- **But it is never wired into the live serving path.** `Pager::open` /
  `BufferPool::new` are constructed only inside the `pager` module and its
  tests. `store/memstore.rs`, `store/committed.rs`, and `txn/mod.rs` never
  reference the pager, `ColdStore`, or `cold::`.
- The live committed state is `CommittedState.rows: imbl::OrdMap<PkBytes, Row>`
  (`committed.rs:34`) — **every committed row fully resident in RAM**. The
  budget bounds the cold tier's page cache, which the live read/write path never
  serves through. The cold tier (`pager/cold.rs` `ColdTable::spill_snapshot`) is
  populated only as the **T2.3 checkpoint spill target** (durability/recovery),
  and spilling does **not** evict from the in-memory map.

**Evidence (real run, 2026-07-26, AMD Ryzen 9 7950X3D, release build):**
`fluxum-bench soak --rows 1000000 --duration-secs 60 --shards 2
--memory-budget 512MiB` — `/health` confirmed the budget applied
(`memory_budget_bytes` = 536870912 env, `bufferpool_capacity_bytes` = 429 MiB).
Result: idle RSS 11.8 MiB (< 100 MB, TST-111 ok), but **peak RSS 1147 MiB vs a
512 MiB budget** — the RSS samples climb linearly with the row count
(423 -> 478 -> ... -> 1147 MiB), i.e. RSS tracks the dataset, not the budget.
`within_budget: false`.

Datasets bounded by disk instead of RAM is a **core pillar vs SpacetimeDB**
(the T2.8 "why"). Until the live store serves *through* the buffer pool, that
pillar and NFR-12/NFR-13 are unmet, and the T7.7 soak / gate G7 cannot pass.

## What Changes

Wire the T2.8 paged cold tier into the live `MemStore` serving path so committed
rows and indexes fault in / evict under `memory.budget`, making steady-state RSS
a function of the budget (TIER-004) rather than the resident row count. The
central difficulty is **preserving MVCC snapshot semantics**: the current
`imbl::OrdMap` gives O(1) structurally-shared immutable snapshots that back
`snapshot()`, `snapshot_as_of()` (SPEC-022 AS OF), and subscription
`InitialData`; a paged store needs an equivalent cheap, consistent snapshot
mechanism (e.g. copy-on-write page roots / a versioned page directory) so
lock-free committed reads, the commit merge, and point-in-time reads keep their
contract. Secondary/spatial index pages must page and evict against the same
budget (TIER-050/051). Behaviour (ACID, MVCC, subscription correctness, crash
recovery) must stay identical; this is an internal storage substitution, not a
semantic change.

## Impact

- Affected specs: SPEC-015 (TIER-001..070), SPEC-002 (storage engine / MVCC)
- Affected code: `crates/fluxum-core/src/store/` (memstore, committed, tx,
  pager/cold + pager/tree integration), `crates/fluxum-core/src/txn`
- PRD requirements: FR-18, FR-110, NFR-02, NFR-07, NFR-12, NFR-13
- Depends on: T2.8 (paged cold tier — the substrate exists), T2.1 (MemStore)
- Blocks: `phase7_billion-row-soak-droplet-validation` (T7.7) and gate G7
- Breaking change: NO (internal; the on-disk page format already froze at G5)
- Risk: HIGH — the MVCC commit path, snapshots, and eviction are the subtlest,
  most correctness-critical code in the system. Must not be rushed; the
  DST/crash/subscription-correctness suites are the guardrails.
- User benefit: a dataset far larger than RAM actually runs within the memory
  budget on the live server — the SpacetimeDB-differentiating pillar, and the
  precondition for the billion-row soak and the 1 vCPU/512 MB droplet to pass.

## Notes

The T2.8 checklist item 1.8 ("dataset 10x the memory budget served correctly …
budget never exceeded") was verified against the pager module in isolation, not
end-to-end through `MemStore` + reducers — which is why the end-to-end T7.7 soak
is the first thing to surface the missing integration. Update SPEC-015 / the
T2.8 record if the "served correctly" claim needs re-scoping to the module.
