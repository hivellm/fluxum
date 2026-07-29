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
- [x] 1.2 P1a — TCP writer coalescing (F-001) — `writer_task` now `recv_many`s up to 64
      frames, applies the per-frame RPC-008 transform where armed, assembles one buffer
      (flushing early past 256 KiB) and issues ONE `write_all` per batch. No timer: an empty
      queue is the single-frame path of before, byte for byte (F-018) — the paced guards
      measured identical before/after (p50 4.73→4.69 ms, p99 9.50→9.11 ms, noise). QueueWait
      stays per frame; Flush stays "time inside `write_all`", now per batch, documented in
      OBS-023. (Vectored writes deferred: `MaybeTls` has no `poll_write_vectored`; the single
      assembled buffer already collapses the syscalls.)
- [x] 1.3 P1b — HTTP chunk writer (F-002) — `push_chunk` assembles header + payload + CRLF
      into a caller buffer (the 3-writes+flush per frame are gone; `write_chunk` keeps its
      signature for the one-off callers); the GET stream drains the mpsc opportunistically
      under the same 64-frame/256 KiB budget and writes one buffer + one flush per batch.
      The Flush stage is now recorded on the HTTP path too (was TCP-only).
- [x] 1.4 OBS-024 — `fluxum_writer_writes_total` / `_coalesced_frames_total` /
      `_coalesced_bytes_total` per transport (tcp|http) + the
      `fluxum_writer_frames_per_write` histogram (buckets 1..64), specified in SPEC-012,
      exported in `/metrics`, charted in the Grafana overview (the dashboard-coverage test
      enforces the families) and as a console Metrics tile ("Frames/write" batch factor).
- [x] 1.5 P2 — one `TxUpdate` per (commit, connection) (F-005) — the fan-out groups
      connections by their (delta, query_id) signature and builds `tables: Vec<TableUpdate>`
      per equivalence class: the common fleet is one class (one encode, SUB-024 preserved),
      a mixed fleet pays one encode per distinct signature, never per connection, and the
      per-query-id deep-clone loop is gone from the hot path (the `tx_update` helper stays
      for core tests). Composes with RPC-035: full and light bodies are built per class.
      Resume windows keep per-query granularity — only transport framing merges. Frame shape
      pinned by `tests/merged_frames.rs` (two lanes, own query_ids, same rows, NO second
      frame; a single-query bystander gets exactly its own lane).
- [x] 1.6 SDK proof — new corpus scenario `merged-txupdate`: two overlapping queries, one
      commit → one merged frame; the row is owned by both query_ids, so unsubscribing one
      handle keeps it and unsubscribing the last removes it (SDK-044 refcount through the
      merged lanes). Green on all five runners against the merging server (Rust corpus
      TCP+HTTP, TS 125, Python 13, Go `-count=1`, C# 15) — F-017 proven, not assumed.
- [x] 1.7 Before/after on the burst profile (the attribution rig of 1.1; same command,
      same box, release, loopback): e2e p50 **9.64 → 4.68 ms (−51%)**, p95 13.9 → 8.2 ms,
      p99 **14.9 → 9.54 ms (−36%)**, zero loss over 1,001,600 deliveries both runs,
      frames/write **2.00** observed at 50k deliveries/s (report committed at
      `docs/reports/fanout-report.json`). The paced guards did not move: `fanout`
      (1,000 subscribers @ 10/s) measured p50 4.73/p99 9.50 ms on the pre-change HEAD
      server and 4.69/9.11 ms after — identical within noise (its NFR-04 "p99 < 5 ms"
      verdict is NOT MET on this workstation in BOTH runs; a pre-existing box condition
      recorded honestly, not a regression). The 2026-07-27 MMO-shape baseline (p50 5.2 /
      p99 7.9 ms @ ~980 commits/s, paced) is guarded by exactly that unchanged paced path.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — SPEC-006 RPC-033 now
      states the multi-table form is actually emitted (and that decoders must not assume one
      lane); SPEC-012 gains OBS-024 with the coalescing contract (64/256 KiB budgets, no
      timer) and the OBS-023 Flush clarification; the analysis README carries the outcome
      block with the measured numbers, cross-linked both ways.
- [x] 2.2 Write tests covering the new behavior — `tests/merged_frames.rs` (merged framing,
      class partition, no-second-frame, OBS-024 families exported); the empty-queue no-op is
      guarded behaviorally by the unchanged paced benches plus every existing e2e suite
      running through the coalesced writers; `fanout-burst` doubles as the burst assertion
      (zero-loss delivery check exits non-zero on a drop).
- [x] 2.3 Run tests and confirm they pass — full workspace green through the coalesced
      writers and merged fan-out; 5-SDK corpus green including both new scenarios; clippy
      --all-features --all-targets, fmt, codespell clean; coverage gate re-run with the
      parity rig live (figure in the archive note).
