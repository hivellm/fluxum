# Proposal: phase0_parity-write-pipelining

## Why

With the MVCC structural-sharing fix in flight, write throughput passes the NFR-11 *ratio*
(11.97× vs PG) but the *absolute* number — 36 111 ops/s — is nowhere near NFR-01's
≥ 100 000 small-write tx/s per shard (`docs/PRD.md:363`, SPEC-013 TST-060), and the gap is
**not in the engine**. Analysis: `docs/analysis/parity-benchmark-corrections/` finding
**F-007**:

- `crates/fluxum-bench/src/fluxum_side.rs:95-99` — `call_reducer` is **synchronous**: one
  in-flight op per client, blocking until the ack (`workload.rs:118-131` keeps exactly one op
  outstanding).
- 8 clients × ~4.5k ops/client/s ≈ 222 µs/op round trip, while the engine commit itself is
  now ~165 µs p50 — throughput is set by round-trip latency × client count, not commit cost.
- SpacetimeDB's published comparisons are in-process (no socket) vs SQLite
  (`docs/analysis/spacetimedb-code/09-ops-testing-bench.md` §4.5) — a "SpacetimeDB-class"
  absolute claim over a real socket needs pipelining to be demonstrable at all.

Without this, T6.6's load test (`phase6_load-test-security-audit`, NFR-01) starts blind: it
cannot distinguish an engine ceiling from a protocol ceiling.

## What Changes

- SDK/protocol: allow multiple in-flight reducer calls per connection — pipelined request IDs
  with futures/callbacks matched to acks (the wire framing already carries request IDs; this
  is a client-connection concurrency change, not a wire-format change if IDs suffice).
- Bench: add a `--pipeline N` (or batched-write) mode to the write workload so the report can
  show **both** numbers without conflating them: acked-serial latency (today's honest number)
  and pipelined throughput (the NFR-01 number).
- Record precisely what caps the pipelined number (network, single-writer commit, ack path)
  so T6.6 starts from a known ceiling.

## Impact

- Affected specs: SPEC-011 (client SDK / connection protocol), SPEC-013 (TST-060 methodology)
- PRD requirements: NFR-01 (≥ 100k tx/s per shard); feeds `phase6_load-test-security-audit`
- Affected code: Rust SDK connection/reducer-call path, crates/fluxum-bench/src/{workload,fluxum_side}.rs,
  report generator (two write columns)
- Depends on: `phase6_memstore-structural-sharing` (engine must be O(k log n) first, or the
  pipeline just queues behind table clones)
- Breaking change: NO (synchronous call remains the default; pipelining is opt-in)
- User benefit: a single connection can saturate the engine — realistic ingestion workloads
  (ETL, telemetry, migration) stop being round-trip-bound
