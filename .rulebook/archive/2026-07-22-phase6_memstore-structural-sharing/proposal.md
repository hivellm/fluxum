# Proposal: phase6_memstore-structural-sharing

## Why

The first honest parity run (T6.3, `docs/parity/report-v0.1.0.md`, 2026-07-21) failed the two
NFR-11 ratios that matter most — write throughput 0.30× (target ≥ 10×) and e2e p99 4.9×
(target ≥ 10×) — and the investigation found a single structural root cause:
**`MemStore::commit` deep-clones every touched table on every commit.**

`Tx::commit` (crates/fluxum-core/src/store/memstore.rs:1501) builds the next `CommittedState`
by cloning the base's `tables` map (Arc bumps, fine) and then `Arc::make_mut(slot)` per touched
table. Because the transaction's own base snapshot holds a second `Arc` reference to every
table, `make_mut` **always** deep-clones: `TableState` is `rows: BTreeMap<PkBytes, Row>` plus
every secondary B-tree index, unique map, spatial and full-text index — all cloned wholesale.
Commit cost is therefore **O(table size)**, measured at ~1.13 ms mean per commit with the
demo `Task` table at ~10⁵ rows (via `fluxum_reducer_duration_us`). The NFR-03 bench
(`txn_commit`) passes only because its table has 1k rows.

Consequences while this stands:
- NFR-11 write ≥ 10× and e2e ≥ 10× are unreachable (T6.3 blocked at items 1.6/1.7);
- NFR-01 (≥ 100k small-write tx/s on one shard, T6.6's load test) is mathematically
  impossible on any table that grows;
- write latency degrades linearly with data volume — the opposite of the product's pitch.

## What Changes

Replace wholesale-clone copy-on-write in `TableState` with **structural sharing**: a commit
touching k rows of an n-row table must do O(k · log n) work, independent of n.

- `rows` moves from `std::collections::BTreeMap` to a persistent ordered map with Arc-shared
  nodes (path-copying on write), preserving ordered iteration, range scans, and point lookups.
- Secondary B-tree indexes, `#[unique]` maps, spatial and full-text state get the same
  treatment or an equivalent sharing strategy — whatever each structure's access pattern
  needs; the investigation item decides per structure.
- The dependency question (a vetted persistent-collections crate vs a hand-rolled Arc-node
  ordered map) is decided in item 1.1 with measurements, not taste. Family convention prefers
  few, well-justified dependencies.
- **No behavioral change**: MVCC single-writer semantics, snapshot isolation, eager
  TXN-040/041 constraint checks, STG-005/STG-007 row/index consistency, deterministic
  iteration order, and every public API stay exactly as they are. The entire existing suite —
  subscription property suite, crash suites, DST, pager 10×, conformance corpus — is the
  regression harness.

## Impact

- Affected specs: SPEC-002 (storage, STG-002 CommittedState), SPEC-003 (NFR-03 commit
  latency) — semantics unchanged, an implementation-note update at most
- PRD requirements: NFR-01, NFR-03, NFR-11 (unblocks), G3
- Affected code: crates/fluxum-core/src/store/{committed,memstore,tx}.rs, crates/fluxum-core/src/index/*
- Unblocks: phase6_postgres-parity-harness items 1.6/1.7 (re-verify with
  `fluxum-bench report`), phase6_load-test-security-audit (T6.6 NFR-01)
- Breaking change: NO (internal representation only)
- User benefit: write latency independent of table size — the property a realtime database
  is bought for
