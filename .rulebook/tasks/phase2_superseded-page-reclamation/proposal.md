# Proposal: phase2_superseded-page-reclamation

## Why

The buffer pool fills with **superseded copy-on-write page versions** far
faster than the TIER-061 reclaimer retires them. Once occupancy reaches the
eviction watermark the pool starts spilling those dead versions to disk, and
sustained write throughput collapses by more than 10x.

Measured (2026-07-27, `MemStore::new` defaults: 256 MiB pool, 4 KiB pages,
watermarks 0.90/0.95; 100 000 rows of a two-column table, no indexes,
inserted through `store.begin()`/`commit()`):

| rows per tx | rows/s | page writes | evictions | pool occupancy |
|---|---|---|---|---|
| 200 | 52 984 | **0** | **0** | 153 MiB |
| 300 | 53 032 | **0** | **0** | 225 MiB |
| 400 | 4 815 | 64 293 | 65 580 | 228 MiB |
| 800 | 1 623 | 201 078 | 206 577 | 241 MiB |

Two things stand out. First, the live data is ~5 MB (100 000 rows of a `u64`
plus a short string) while the pool holds **153-241 MiB** - roughly **30x the
live set**, essentially all of it superseded versions. Second, the cliff sits
exactly where occupancy crosses the 0.90 low watermark (230 MiB of 256 MiB):
below it, zero page writes; above it, the pool pays disk I/O to *write out
garbage* it is about to discard.

Attribution is settled, not inferred:

- **Not the commit log** - the same cliff appears via `store.begin()`/`commit()`
  with no `TxPipeline` and no `CommitLog` in the picture.
- **Not key order** - scattered keys (`i * 0x9E3779B97F4A7C15`) behave
  identically to sequential ones, so it is not rightmost-leaf splitting.
- **Not the network, the driver, or fsync** - group-commit fsync is already
  off the commit path, and neither process saturates CPU.

The consequences reach past this benchmark. It is why the T7.7 soak's load
phase runs at ~4 000 rows/s under a small budget and degrades as the dataset
grows, and it puts a floor under NFR-01 (>= 100 000 tx/s per shard; the current
path measures ~11 000-17 000 tx/s end-to-end) that no amount of client
concurrency can lift.

## What Changes

Make superseded-page reclamation keep pace with the copy-on-write churn that
produces it, so pool occupancy tracks the **live** working set rather than the
accumulated version history, and eviction only ever spills pages that are
still reachable.

The shape of the fix is deliberately not pinned here - item 1.1 is to
establish *why* reclamation lags before choosing between eagerly retiring a
commit's superseded pages once no live version can reach them, dropping
unreachable pages without writing them (a clean drop is free; a spill is not),
running the reclaimer on a tighter cadence, or bounding a transaction's
un-retirable CoW churn. What is fixed is the observable outcome: no page write
for a version nothing can reach, and no throughput cliff at the watermark.

MVCC correctness is the hard constraint. A snapshot pinned on an older version
must keep reading a consistent tree (TIER-061), so nothing may be retired
while any live version can still reach it - the existing `store_acid`,
`temporal_as_of`, and `txn_atomicity` suites are the guardrails.

## Impact

- Affected specs: SPEC-015 (TIER-004/TIER-061), SPEC-002 (STG-005 commit merge)
- Affected code: `crates/fluxum-core/src/store/pager/` (reclaimer, pool,
  eviction policy), `crates/fluxum-core/src/store/memstore.rs` (commit merge's
  superseded handoff)
- PRD requirements: NFR-01 (throughput), NFR-12/NFR-13 (bounded RSS at scale)
- Depends on: nothing - the paged live store and TIER-061 reclaimer are landed
- Breaking change: NO (internal storage behaviour)
- Risk: HIGH - retiring a page one version too early is silent data corruption
  for any reader holding that snapshot, and the failure would surface far from
  the change. Treat the MVCC regression suites as the gate, not a formality.
- User benefit: sustained write throughput stops falling off a cliff, RSS
  reflects live data instead of version garbage, and the billion-row soak
  becomes a practical run rather than a multi-day one

## Notes

Reproduction is a short standalone probe against `MemStore` - insert N rows in
batches of K through `store.begin()`/`commit()` and print
`store.pager().metrics().snapshot()`. The cliff appears between K = 300 and
K = 400 at 100 000 rows; both K and total table size move it, because both
change how much superseded churn accumulates before the reclaimer catches up.

Two measurement notes for whoever picks this up:

- The stored criterion baselines under `target/criterion/` predate the paged
  live store, so `cargo bench` reports a large "regression" (+98% on
  `txn_commit_update_reading_1000`, +294% on the 1M case) that is really the
  accepted cost of paging. The absolute NFR-03 gate still passes comfortably
  (p99 220 us at 1M rows against a 1 ms target). Refresh the baselines as part
  of this work so the guard means something again.
- Benchmarks on the development workstation need the machine quiet; a
  contended run reported roughly half the throughput of the committed parity
  report and made `txn_commit` fail its p99 assertion spuriously.
