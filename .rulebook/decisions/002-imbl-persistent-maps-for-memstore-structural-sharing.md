# 2. imbl persistent maps for MemStore structural sharing

**Status**: accepted
**Date**: 2026-07-22
**Related Tasks**: phase6_memstore-structural-sharing

## Context

`MemStore::commit` deep-cloned every touched table per commit (`Arc::make_mut` with the Tx's
base snapshot holding a second ref ⇒ CoW always clones), making commit cost O(table size) and
NFR-01/NFR-03/NFR-11-write structurally unreachable. Decision bench
`crates/fluxum-core/benches/table_clone.rs` (item 1.1), Win10 bench box, 2026-07-22:

| shape (std::BTreeMap) | clone cost |
|---|---|
| rows 10k / 100k / 1M | 148 µs / 7.10 ms / **85.8 ms** (linear) |
| secondary btree index 100k | 36.97 ms |
| unique map 100k | 7.30 ms |

| imbl::OrdMap (v7.0.1) | cost |
|---|---|
| clone 100k / 1M | **3.1 ns / 3.0 ns** (O(1) handle) |
| worst-case shared insert 100k / 1M | 797 ns / 1.04 µs (O(log n) path copy) |
| point get 100k (vs std 69 ns) | 50 ns (faster) |
| range-100 scan 100k (vs std 159 ns) | 231 ns (1.45×) |
| full iteration 100k (vs std 116 µs) | 231 µs (2×) |

## Decision

Adopt **`imbl` 7.0.1** (persistent ordered maps/sets, Arc-shared chunks, path-copying) for
every write-path structure the commit merge clones: `TableState::rows`, secondary
`BTreeIndex` maps/sets, `UniqueIndex` maps, and full-text `postings`/`doc_len`. Hand-rolling
an Arc-node ordered map was rejected: imbl matches or beats std on the hot read path, the 2×
full-iteration cost only touches async checkpoint/snapshot scans, and the crate is
maintained, widely used, and MPL-2.0 (file-level copyleft, linked unmodified — compatible
with Apache-2.0; recorded in the workspace Cargo.toml comment).

**Spatial indexes (`QuadTree`/`RTree`) stay deep-cloned** — recorded residual: they are
arena-backed trees that do not map onto chunked path-copying; sharing them is a redesign
(persistent/generational arena), not a swap. Cost is paid only by tables that declare
`#[spatial(...)]` (none in the parity demo); a table with a large spatial index still pays
O(n_spatial) per touched commit. Follow-up owns it when a spatial-heavy workload materializes.

## Consequences

Commit merge cost drops from O(n) per touched table (85.8 ms at 1M rows) to O(k·log n)
(≈ 1 µs per touched row) — `txn_commit` now asserts p99 < 1 ms at 1k **and** 1M rows (item
1.4). Snapshot/checkpoint full scans pay ≤ 2× iteration; measured NFR-11 effect: write ratio
0.30× → 25.66× in `docs/parity/report-v0.1.0.md`.
