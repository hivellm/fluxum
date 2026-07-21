# 02 — Root causes: why the numbers are (were) bad

### F-003 — `MemStore::commit` was O(table size): every commit deep-cloned each touched table
- **Evidence:** `crates/fluxum-core/src/store/memstore.rs:1504` clones the
  committed table map (`self.base.tables.clone()` — Arc bumps only), then
  `:1517` `let table = Arc::make_mut(slot)`. Because the committing `Tx` holds
  its own `Arc` reference to the same base snapshot, the refcount is ≥ 2, so
  `make_mut` **always** deep-clones the whole `TableState` — its `rows`
  (`BTreeMap<PkBytes, Row>`), every secondary index, unique map, spatial and
  full-text state. Commit cost therefore grew with table size, not with the
  k rows the transaction touched. Confirmed at runtime via
  `fluxum_reducer_duration_us` (engine mean **1.13 ms/commit under load**);
  documented in `phase6_postgres-parity-harness/tasks.md:6` (task 1.5).
- **Impact:** This is the direct cause of the committed report's failures:
  write throughput **941 ops/s (0.30×)**, mixed/write **257 ops/s**,
  mixed/e2e **p99 123 ms** (chat commits queued behind O(table) clones). It
  also makes NFR-01 (≥ 100 000 tx/s, `docs/PRD.md:363`) and NFR-03 (commit p99
  < 1 ms independent of table size, `docs/PRD.md:365`) structurally
  unreachable on any non-trivial table — the existing `txn_commit` bench only
  passed because its table is 1 000 rows.
- **Confidence:** High (code + measured + already root-caused by the team).

### F-004 — The in-flight fix works but is partial: indexes/unique/spatial/FTS still clone
- **Evidence:** working tree swaps `TableState::rows` to a persistent map
  (`crates/fluxum-core/src/store/committed.rs`: `rows: imbl::OrdMap<PkBytes,
  Row>`, O(1) structural-sharing clone; `imbl` added at
  `crates/fluxum-core/Cargo.toml:18`). This alone moved write to **36 111
  ops/s (11.97×)**, mixed/write to **31 175 ops/s** (121×), and mixed/e2e p99
  from 123 ms → **4.8 ms** (26×). **But** `TableState` still holds
  `indexes: BTreeMap<IndexId, BTreeIndex>`, `unique`, `spatial`, `fulltext` as
  eagerly-cloned structures, so `Arc::make_mut` still deep-copies index state
  on every commit — commit stays O(index size) for indexed tables. The demo
  schema's tables are lightly indexed, so the bench does not expose it; a
  100k–1M-row indexed table will. This is exactly what
  `phase6_memstore-structural-sharing/tasks.md:4` (1.3) and `:5` (1.4, "commit
  p99 < 1 ms independent of table size at ≥ 1M rows") still leave open.
- **Impact:** Write/mixed pass *today* on the demo schema, but NFR-01/NFR-03
  are not yet proven at scale, and a future report on a larger schema could
  regress. The fix must be finished (indexes structurally shared or
  generationally rebuilt) and committed before the artifact is trustworthy.
- **Confidence:** High (code: `TableState` fields still eager; tasks open).

### F-005 — e2e p99 (0.9 ms) cannot reach 10× vs LISTEN/NOTIFY, and it is not a fan-out-evaluation problem
- **Evidence:** The subscription engine already evaluates and encodes each
  query's delta **once** and shares the bytes to a bucket of subscribers who
  see identical rows — `crates/fluxum-core/src/subscription/mod.rs:12`
  ("a query's delta is evaluated and encoded **once**"), `:319` ("every
  subscriber in the bucket because they all see the same rows"), `:387`
  (`SharedDelta`, "shared, once-encoded table update"). In the e2e workload
  all 50 subscribers share one query (`SELECT * FROM ChatMessage WHERE channel
  = X`, `crates/fluxum-bench/src/fluxum_side.rs:129`), so encoding is not
  duplicated. The residual p99 is therefore (a) **serial per-subscriber socket
  enqueue/flush** on the server and (b) **driver-side scheduling** of 50
  subscriber threads timestamping receipt in one process
  (`crates/fluxum-bench/src/workload.rs:658-685`, one thread per subscriber on
  the same 32-core box as the server). To beat PG's 4.39 ms by 10× Fluxum
  needs p99 ≤ 0.44 ms fan-out to 50 sockets (~9 µs/socket) — the tail is
  dominated by the 50th socket write and the 50th thread wake, not by query
  work.
- **Impact:** The remaining NFR-11 miss (F-002). It will not close by
  optimizing subscription evaluation (already optimal); it needs either
  parallel/batched socket writes on the server, fewer commit→push hops, or a
  measurement that isolates server emit latency from driver thread scheduling.
- **Confidence:** Medium (fan-out sharing confirmed in code; the split between
  server socket serialization and client-thread scheduling is inferred, not
  yet instrumented — the first plan task is to measure it).

### F-006 — Fan-out degrades 5× under contention because commits are single-writer
- **Evidence:** standalone e2e p99 `0.899 ms` vs mixed/e2e p99 `4.802 ms`
  (`report-v0.1.0.md`), a 5.3× degradation, leaving only **1.68×** over PG's
  mixed/e2e `8.060 ms`. The chat commit that feeds fan-out shares the
  single-writer commit path with the write/read load
  (`memstore.rs` commit is serialized; STG-005 single-writer). Under 4 writers
  + 4 readers hammering, a chat commit waits behind them before it can fan out.
- **Impact:** The "realistic deployment" number (mixed) is where Fluxum's
  live-query latency advantage nearly vanishes. Even once standalone e2e is
  fixed, contention will pull mixed/e2e back unless commit admission
  prioritizes latency-sensitive small commits or fan-out runs off the commit
  critical path.
- **Confidence:** Medium (report + single-writer design; interaction inferred).

### F-007 — Write throughput is round-trip-bound, not commit-bound — the gap to NFR-01 / SpacetimeDB-class is in the client path
- **Evidence:** `crates/fluxum-bench/src/fluxum_side.rs:95-99`
  (`add_task` → `connection.call_reducer(...)`) is a **synchronous** call: one
  in-flight op per client, blocking until the ack. The write loop keeps one op
  outstanding per client (`crates/fluxum-bench/src/workload.rs:118-131`). With
  8 clients the working tree hits 36 111 ops/s ≈ 4.5k/client/s ≈ **222 µs/op**,
  while the commit itself is now ~165 µs p50 — i.e. throughput is set by
  round-trip latency × client count, not by engine commit cost. NFR-01 asks for
  ≥ 100 000 tx/s per shard (`docs/PRD.md:363`, `SPEC-013 TST-060`), and
  SpacetimeDB's published numbers are in-process (no socket) vs SQLite
  (`docs/analysis/spacetimedb-code/09-ops-testing-bench.md:334`, §4.5).
- **Impact:** Even with the storage engine fixed, the harness cannot
  demonstrate NFR-01 or a SpacetimeDB-class absolute number, because a single
  client is latency-limited and the SDK offers no request **pipelining /
  batching** (multiple in-flight reducer calls per connection). The 10× *ratio*
  vs PG is met; the *absolute* 100k/s target is a separate, unmet claim.
- **Confidence:** Medium-high (client code is synchronous; arithmetic from the
  report; NFR-01 measured elsewhere via T6.6, not this harness).
