## 1. Implementation

Analysis: `docs/analysis/fanout-event-batching/` (F-001..F-020). Two
increments, both wire-compatible; P1 lands before P2 so the burst bench
attributes each win separately.

- [ ] 1.1 Burst-mode fan-out bench FIRST (F-015): a bench/e2e mode that
      drives the writer queue non-empty (bursts of ≥ 32 queued frames,
      plus a sustained 1k+ commits/s profile with N subscribers), and
      records frames-per-write, syscalls/s, and the parity p50/p99. The
      existing 10–20 msg/s benches keep running as the latency guard —
      batching must measure as a no-op there. Baseline captured before
      any writer change.
- [ ] 1.2 P1a — TCP writer coalescing (F-001, `tcp.rs:673-696`): after
      the first `recv()`, drain what is already queued (`recv_many`
      with a bounded batch, e.g. 64 frames / 256 KiB) and write the
      whole batch in one syscall (single buffer or vectored write once
      `MaybeTls` grows `poll_write_vectored`). No timer, no artificial
      delay: an empty queue behaves exactly as today (F-018). QueueWait
      and Flush stage metrics keep their meaning (record per frame).
- [ ] 1.3 P1b — HTTP chunk writer (F-002, `http/wire.rs:227-233` +
      `http.rs:1273-1308`): assemble chunk header + payload + CRLF in
      one buffer — one write + one flush per batch, not 3 writes +
      flush per frame; drain the mpsc opportunistically like the TCP
      side. Record the missing Flush stage on the HTTP path (today
      TCP-only).
- [ ] 1.4 OBS-024 metrics: frames-per-write histogram + coalesced
      bytes/frames counters per transport, exposed in `/metrics` and
      visible in the console Metrics dashboard, so the natural batch
      factor is observable in production.
- [ ] 1.5 P2 — one `TxUpdate` per (commit, connection) (F-005):
      group a commit's `QueryDelta`s per connection by equivalence
      class, populate `tables: Vec<TableUpdate>` (RPC-033 already
      declares it; RPC-032 `query_id` already correlates), preserve the
      encode-once/`Arc` sharing (SUB-024), and remove the per-group
      deep clone at `manager.rs:811`. Resume windows (CS-021) keep
      their per-query granularity — only transport framing merges.
- [ ] 1.6 SDK proof: conformance corpus re-run across the five SDKs
      against the merging server (F-017 says the readers accept it with
      no negotiation — prove it, don't assume it); one added corpus
      scenario with a multi-query subscriber receiving a merged frame.
- [ ] 1.7 Before/after numbers on the MMO-shape workload
      (`demo/mmo_bots.py` at 99 bots × 10 Hz + N browser-equivalent
      subscribers): writes/s, frames-per-write, e2e p50/p99 — the
      2026-07-27 baseline (e2e p50 5.2 ms / p99 7.9 ms @ ~980
      commits/s) must not regress.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
      (SPEC-006 note: multi-table TxUpdate now actually emitted;
      SPEC-012 OBS-024 metrics; analysis README cross-link)
- [ ] 2.2 Write tests covering the new behavior (writer drain unit
      tests incl. the empty-queue no-op, merged-TxUpdate framing tests,
      burst bench assertions)
- [ ] 2.3 Run tests and confirm they pass (workspace suites + SDK
      conformance corpus + coverage floor)
