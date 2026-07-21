# Proposal: phase0_parity-fanout-latency

## Why

After the in-flight MVCC structural-sharing fix, e2e change→subscriber p99 is the **sole
remaining NFR-11 miss**: working-tree `ratios.e2e_p99 = 4.89` vs target ≥ 10×
(`docs/parity/report-v0.1.0.json`; Fluxum e2e p99 0.899 ms vs PG LISTEN/NOTIFY 4.392 ms).
Analysis: `docs/analysis/parity-benchmark-corrections/` findings **F-002, F-005, F-006**.

Two root causes, neither of which is subscription evaluation (already optimal — the engine
evaluates and encodes each query's delta **once** per bucket, `subscription/mod.rs:12,319,387`,
`SharedDelta`):

1. **F-005 — the residual tail is fan-out delivery, not query work.** With 50 subscribers on
   one query, beating PG by 10× requires p99 ≤ 0.44 ms to 50 sockets (~9 µs/socket). The tail
   is dominated by (a) serial per-subscriber socket enqueue/flush on the server and/or (b)
   driver-side scheduling of 50 subscriber threads timestamping receipt in one process on the
   same 32-core box (`crates/fluxum-bench/src/workload.rs:658-685`). The split between the two
   has **not been instrumented** — measuring it is the first item.
2. **F-006 — fan-out degrades 5.3× under contention** (standalone e2e p99 0.899 ms → mixed/e2e
   4.802 ms, only 1.68× over PG's 8.060 ms). The chat commit that feeds fan-out shares the
   single-writer commit path (STG-005) with the write/read load, so a small latency-sensitive
   commit queues behind bulk writes before it can fan out.

This gates `phase6_postgres-parity-harness` exit items 1.6/1.7 and Gate G6.

## What Changes

Measure first, then fix the side the measurement blames:

- Server-side instrumentation splitting e2e latency into commit → subscription-eval →
  per-socket enqueue → socket flush, vs driver-side thread-wake→callback, so the 0.9 ms p99
  is attributed instead of guessed.
- If server-bound: make per-subscriber fan-out concurrent/batched (write the shared,
  once-encoded bytes to all bucket sockets without serializing behind each flush) and move
  fan-out off the commit critical path so small commits are not queued behind write load.
- If driver-bound: fix the harness — pin server vs driver to disjoint cores, cap subscriber
  threads, or measure server-emit timestamp for the fan-out component, documented honestly
  per TST-091.

## Impact

- Affected specs: SPEC-005 (subscriptions/fan-out), SPEC-013 (TST-091/TST-093 methodology
  notes if driver-side)
- PRD requirements: NFR-11 (e2e_p99 ≥ 10×), NFR-02 (fan-out latency); unblocks Gate G6
- Affected code: crates/fluxum-core/src/subscription/*, server socket write path,
  crates/fluxum-bench/src/{workload,fluxum_side}.rs
- Depends on: `phase6_memstore-structural-sharing` (engine fix must land first so measurements
  are on the final commit path)
- Breaking change: NO
- User benefit: the product's headline property — sub-millisecond live-query delivery that
  holds under realistic mixed load, not only in an idle benchmark
