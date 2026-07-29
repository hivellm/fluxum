## 1. Implementation

Analysis: `docs/analysis/fanout-event-batching/` (F-001..F-020). Two
increments, both wire-compatible; P1 lands before P2 so the burst bench
attributes each win separately.

- [x] 1.1 Burst-mode fan-out bench FIRST (F-015) — `fluxum-bench fanout-burst`
      (`--clients W --rate R --subscribers N --duration-secs S`): W writer
      identities fire simultaneously every `W/R` seconds, so every round lands a
      W-commit burst in each subscriber's outbound queue while each identity
      stays under the demo module's 20/s send_chat admission (the command
      refuses a rate the fleet cannot carry). Per-delivery e2e latency via the
      parity e2e's embedded-send-instant trick; frames-per-write scraped from
      /metrics once OBS-024 exists (`null` on the baseline server — that IS the
      pre-change record), with the Content-Length HTTP read (the phase8
      read-to-EOF hang, relearned once). The existing paced `fanout` command
      stays as the low-rate latency guard. **Baseline captured 2026-07-28
      (release, loopback), before any writer change:** 1,000 commits/s in
      64-commit bursts × 50 subscribers = 50k deliveries/s, zero loss over
      1,001,600 deliveries, e2e p50 **9.64 ms** / p95 13.9 ms / p99
      **14.9 ms** — the queue-backlog cost of one write per frame, exactly
      what coalescing must cut; frames/write = 1.0 by construction.
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
