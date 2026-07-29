# Fan-out event batching / frame coalescing

**Slug:** `fanout-event-batching` · **Date:** 2026-07-27 ·
**Scope:** reducing per-packet and per-syscall overhead on the realtime push path
(commit → `TxUpdate` → subscriber socket). Findings numbered globally **F-001..F-020**.

> **Outcome (2026-07-28, phase9_fanout-event-batching):** implemented as analyzed.
> Writer coalescing (F-001/F-002: opportunistic drain, 64 frames / 256 KiB per buffered
> write, HTTP chunks assembled into one buffer) + the merged multi-table `TxUpdate`
> (F-005/F-017, proven by the 5-SDK corpus `merged-txupdate` scenario) + the OBS-024
> batch-factor metrics + the `fluxum-bench fanout-burst` rig (F-015). Measured on the
> burst profile (1k commits/s in 64-commit bursts × 50 subscribers, release, loopback):
> e2e p50 9.64 → 4.68 ms (−51%), p99 14.9 → 9.54 ms (−36%), zero loss, frames/write 2.0
> observed; the paced low-rate guards are byte-identical to the pre-change path (empty
> queue ⇒ no coalescing, by construction). The sibling layers (RPC-035 light + RPC-008
> stream-deflate) landed first in `phase9_delta-compression`; batch × light × compression
> compose.

## Executive summary

The fan-out path above the socket is already well engineered: one plan compilation per
query, one delta evaluation per unique query, one encode per query shared to every
subscriber by `Arc` clone (SUB-020/021/024). **Everything this analysis finds is below that
line** — in how many times those shared bytes are handed to the kernel.

The measured stage split from the predecessor task (`phase0_parity-fanout-latency`) is
unambiguous: the two **per-frame** legs dominate (`queue_wait` 107–115 µs/frame, `flush`
41–46 µs/frame) while the two **per-commit** legs do not (`eval` ~13 µs, `enqueue` ~36 µs).
The 1→50 subscriber slope is +287 µs on p99 — pure delivery serialization. That is the
arithmetic signature of a workload batching helps.

Yet **nothing coalesces anywhere**:

1. **One `write_all` per frame per subscriber** (`tcp.rs:679-694`, `http.rs:1278-1291`) —
   no `BufWriter`, no `recv_many`, no vectored write. `recv()` returns one frame even when
   50 are queued behind it (**F-001**).
2. **The HTTP push stream issues three writes and a flush per frame** — chunk header, body,
   CRLF (`http/wire.rs:227-233`). With `TCP_NODELAY` on and possibly TLS, that is up to
   3 segments / 3 TLS records per `TxUpdate` on the transport browsers cannot avoid
   (**F-002**).
3. **A connection receives K frames per commit, one per matched subscription**
   (`lib.rs:1289-1344`, `manager.rs:800-813` hardcodes `tables: vec![one]`) — even though
   RPC-033 already models `tables: Vec<TableUpdate>` and RPC-032 already makes `query_id`
   the client's correlation handle. The merged form is legal today (**F-005**).

Two adjacent problems surfaced while tracing the path, both of which batching work must
either fix or work around:

- **SUB-042's three-tier backpressure is implemented and disconnected.**
  `SubscriberBuffer` (byte-budgeted, tick-aware, priority-aware) is referenced only by
  tests and doc links; the live path is a 1024-**frame** `mpsc` with a binary
  deliver-or-kill policy. `subscriptions.send_buffer_bytes` is parsed, plumbed, made
  hot-reloadable, asserted by tests — and read by nobody on the delivery path (**F-009**).
  Since a coalescing writer *needs* byte accounting to size a batch, fixing this and adding
  batching are the same work.
- **A lagging fan-out silently drops commits for every subscriber on the shard**, with no
  metric and no log (`lib.rs:1237`, capacity hardcoded at 256, `boot.rs:533`) — ~4 ms of
  slack at the measured 64k commits/s ceiling (**F-012**).

The good news dominates the risk assessment: **transport-level coalescing is wire-transparent
to all five shipped SDKs** (Rust/TS/Python/Go/C# all use incremental buffer-draining frame
readers, and RPC-004/005 already mandate back-to-back frames) — so the highest-value work
needs no protocol change, no negotiation, and no SDK release (**F-017**).

### What to do, and what not to do

**Do** — opportunistic drain (`recv_many` → one write per batch), never a timed window: it
is weakly better in every regime, equalling today's behaviour when there is nothing to
batch and self-tuning its batch size to the actual backlog. A timed window would pay
latency on p99 to batch frames that do not exist yet, against the one number the entire
parity harness exists to defend (**F-018**).

**Don't** re-try parallel/chunked enqueue or a direct-socket write path — both were
measured and honestly reverted in `phase0_parity-fanout-latency` item 1.2. Neither was
batching: both kept **one write per frame** and only moved who performed it. Coalescing is
the untried axis.

**Measure first.** The existing fan-out benchmarks run at 10–20 commits/s, where the queue
is always empty when a frame arrives — batching is a no-op there **by construction**, and a
reviewer running only those benches will correctly conclude the change did nothing
(**F-015**). No metric counts syscalls or frames-per-write either (**F-014**). The plan
therefore ships the burst bench and the counters *with* the first change, not after it.

### Findings roll-up

| Severity | Findings |
|---|---|
| High | F-001, F-002, F-005, F-009, F-012 |
| Medium | F-006, F-010, F-013, F-014, F-015, F-016 |
| Low | F-007, F-008, F-011 |
| Context / positive | F-003 (`TCP_NODELAY` is right — it just makes userspace batching the only lever), F-004, F-017, F-018, F-019, F-020 |

### Plan at a glance

| Task | Levers | Closes | Protocol change |
|---|---|---|---|
| **P1** `phase0_fanout-write-coalescing` | buffered chunk write + opportunistic drain + metrics + burst bench | F-001, F-002, F-012, F-014, F-015 | none |
| **P2** `phase0_fanout-txupdate-merge` | one `TxUpdate` per (commit, connection) | F-005, F-006, F-007, F-008 | none (already legal) |
| **P3** `phase8_subscriber-send-buffer-policy` | byte budget + graded tiers + conflation | F-009, F-010, F-011, F-013 | additive |

P1 gates both: it delivers the byte accounting P3 needs and the measurement P2's value has
to be argued with.

## Reading order

1. [01 — The realtime push path as it exists today](01-current-push-path.md) *(F-001..F-004)*
2. [02 — Why there are more frames than there need to be](02-frame-multiplication.md) *(F-005..F-008)*
3. [03 — Backpressure that is not wired, and loss that is not visible](03-backpressure-and-loss.md) *(F-009..F-013)*
4. [04 — What we cannot currently see or measure](04-observability-and-benchmarks.md) *(F-014..F-016)*
5. [05 — The design space and its constraints](05-design-options.md) *(F-017..F-020)*
6. [06 — Execution plan](06-execution-plan.md)

## Related material

- Predecessor task (the measurements this analysis builds on):
  `.rulebook/archive/2026-07-23-phase0_parity-fanout-latency/`
- [Parity benchmark corrections](../parity-benchmark-corrections/README.md) — F-005/F-006
  there first identified fan-out delivery as the residual NFR-11 e2e gap
- Normative specs: [SPEC-005 §subscriptions](../../specs/SPEC-005-subscriptions.md)
  (SUB-021/024/042/043), [SPEC-006 FluxRPC](../../specs/SPEC-006-protocol-fluxrpc.md)
  (RPC-001/004/005/008/033/061), [SPEC-012 observability](../../specs/SPEC-012-observability.md)
  (OBS-021/022/023)
