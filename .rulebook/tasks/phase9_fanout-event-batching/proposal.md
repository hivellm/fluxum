# Proposal: phase9_fanout-event-batching

## Why

The realtime push path delivers every `TxUpdate` as its own unbuffered
socket write. Under MMO-style load (the `demo/mmo.html` sample: ~1,000
commits/s fanned out to every subscriber) that means one syscall and
typically one TCP packet per event per subscriber — and on the HTTP
transport, **three** `write_all` calls plus a `flush` per frame
(chunk-size line, data, CRLF). The per-event cost multiplies by
`updates/s × subscribers`: at 1k upd/s × 100 subscribers that is 100k
writes/s of pure overhead before bandwidth even matters.

The full analysis is `docs/analysis/fanout-event-batching/` (findings
F-001..F-020). The headline findings:

- **F-001** — the TCP writer (`tcp.rs:679-694`) and the HTTP GET stream
  writer (`http.rs:1278-1291`) `recv()` ONE frame per await and write it
  immediately: no `BufWriter`, no `recv_many` drain, no vectored write
  (`MaybeTls` does not implement `poll_write_vectored`). Frames already
  sitting in the queue are written one syscall each.
- **F-002** — `http/wire.rs:227-233` is 3 writes + flush per chunk. With
  `TCP_NODELAY` on (deliberate, NFR-04) that is up to 3 segments — or 3
  TLS records — per TxUpdate, on exactly the transport the browser SDK
  cannot avoid. ~10-line fix.
- **F-005** — a connection subscribed to K matched queries receives **K
  separate frames per commit**: `manager.rs:811` always builds
  `tables: vec![one]` even though RPC-033 already declares
  `TxUpdate.tables: Vec<TableUpdate>` and RPC-032 correlates by
  `query_id`. The merged form is legal on the wire TODAY — every SDK's
  incremental frame reader accepts it with no negotiation (F-017).
- **F-004** — the archived `phase0_parity-fanout-latency` stage split
  blames the per-frame legs: `queue_wait` 107–115 µs and `flush`
  41–46 µs **per frame**, vs `eval` ~13 µs per commit. The overhead IS
  where batching helps.
- **F-015** — the existing fan-out benches run at 10–20 msg/s where the
  queue is always empty, so batching measures as a no-op by
  construction. Without a burst bench the change cannot prove itself.

Design stance (F-018): **opportunistic drain only, no time-window
batching by default.** Draining what is ALREADY queued is latency-
neutral — it groups frames that would otherwise each pay a syscall,
and the batch factor grows automatically with load. A flush timer
(game-server tick) pays p99 latency to group frames that do not exist
yet, against the very metric the parity harness defends. Rejected as a
default; a conflated/snapshot subscription mode (last-write-wins per
row) is real spec work against the CS-020/021 resume contract and the
SDK-045 per-commit cache consistency promise — follow-up, not this
task (see Notes).

## What Changes

Two increments, neither changing the wire protocol:

1. **Write coalescing (P1)** — both stream writers drain
   opportunistically (`recv_many` / `try_recv` loop after the first
   frame) and write the batch in one buffered syscall; the HTTP chunk
   writer assembles chunk header + data + CRLF into one buffer (one
   write + one flush per BATCH, not per frame). New OBS-024 metrics:
   frames-per-write histogram + coalesced-bytes counter, so the batch
   factor is observable. A burst-mode fan-out bench (F-015) proves the
   win and guards the regression.

2. **TxUpdate merge (P2)** — one `TxUpdate` per (commit, connection):
   group a commit's `QueryDelta`s by equivalence class so the
   encode-once sharing (SUB-024) is preserved, and populate
   `tables: Vec<TableUpdate>` instead of emitting K single-table
   frames. Removes the per-group deep clone at `manager.rs:811`.
   Legal on the wire already; the SDK conformance corpus re-run proves
   the five SDKs agree.

## Impact

- Affected specs: SPEC-006 (framing — no change, but document the
  multi-table TxUpdate as now actually emitted), SPEC-012 (new OBS-024
  metrics), SPEC-005/021 (no contract change; resume windows unchanged).
- Affected code: `crates/fluxum-server/src/tcp.rs` (writer task),
  `crates/fluxum-server/src/http.rs` + `http/wire.rs` (GET stream
  writer), `crates/fluxum-server/src/lib.rs` (fan-out grouping),
  `crates/fluxum-core/src/subscription/manager.rs` (`tx_update`),
  `crates/fluxum-core/src/metrics.rs` (OBS-024), fan-out benches.
- Breaking change: NO — wire-legal today; SDKs use incremental frame
  readers (F-017).
- Risk: MEDIUM — the writer loop is the realtime hot path; the burst
  bench plus the parity latency harness gate both directions (throughput
  up, p50/p99 not worse).
- User benefit: syscalls and packets per subscriber drop by the natural
  batch factor under load; the MMO-shape workload (many subscribers ×
  high update rate) stops paying per-event transport overhead.

## Notes

Follow-up explicitly out of scope here (spec first, separate task):
**subscriber send-buffer policy + conflation** — wire the dormant
byte-budget 3-tier `SubscriberBuffer` (implemented + tested but
unplumbed, F-009: the delivery path is a 1024-frame mpsc with
deliver-or-kill; `subscriptions.send_buffer_bytes` is parsed, plumbed,
hot-reloadable and read by nobody) and add an opt-in conflated
subscription mode for snapshot-semantics tables (game positions), which
must be reconciled with CS-020/021 resume and SDK-045 consistency.
Also fix silently-lagged commit broadcasts (F-012: `Lagged(_) =>
continue` with no metric, capacity 256 hardcoded).

Measured baseline to beat (2026-07-27, loopback, release): e2e p50
5.2 ms / p99 7.9 ms at ~980 commits/s with fan-out stage totals of
~19 µs/event; the coalescing must keep those and cut writes/frame.
