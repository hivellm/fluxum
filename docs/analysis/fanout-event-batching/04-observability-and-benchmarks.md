# 04 — What we cannot currently see, and what the benchmark cannot currently show

*Findings F-014..F-016.* A batching change that cannot be measured will be reverted the
next time someone re-reads the code, exactly as the parallel-enqueue attempt was (F-004).
This file establishes what has to exist *before* the work, not after.

---

## F-014 — No metric counts frames per write, writes per commit, or bytes per write

**Evidence** — the full fan-out metric surface:

- `fluxum_fanout_messages_total`, `fluxum_fanout_rows_total` (OBS-021,
  `docs/specs/SPEC-012-observability.md:86-91`; `crates/fluxum-core/src/metrics.rs:866-872`)
- `fluxum_subscriber_drops_total{reason}` (OBS-022, SPEC-012:96-101;
  `metrics.rs:878-886`) — reasons `buffer_full`, `blocked_timeout`, `frame_too_large`
  (`metrics.rs:52-69`)
- `fluxum_fanout_stage_us{stage}` (OBS-023, SPEC-012:107-115;
  `metrics.rs:316-357`) — six stages, all **latency**

Every series is a count of *messages/rows* or a *duration*. Nothing counts **syscalls**, and
nothing relates frames to writes. `FanoutStage::Flush` (`metrics.rs:327-328`) records the
duration of one `write_all` — after a coalescing change its meaning silently shifts from
"one frame" to "one batch", and the historical series becomes uncomparable without a
companion batch-size series to normalize it.

**What is missing, concretely:**

| Proposed series | Answers |
|---|---|
| `fluxum_fanout_batch_frames{shard}` (histogram) | how many frames one socket write carried — the whole point of the change |
| `fluxum_fanout_socket_writes_total{shard}` (counter) | the syscall rate; ratio against `fluxum_fanout_messages_total` is the coalescing factor |
| `fluxum_fanout_batch_bytes{shard}` (histogram) | whether batches approach MSS/record size, and whether they risk `max_frame_bytes` pressure |
| `fluxum_fanout_lagged_total{shard}` (counter) | F-012's invisible shard-wide loss |

**Impact.** Without `batch_frames` and `socket_writes_total` there is no way to state "the
change reduced writes by N×" in a parity report (TST-091 requires methodology be part of
the published configuration), no way for the TST-064 regression guard to notice batching
silently turning itself off, and no way for an operator to distinguish "quiet" from
"coalescing hard because the consumer is slow".

**Confidence:** high.

---

## F-015 — The fan-out benchmark is constructed so that batching can never appear

**Evidence** — `crates/fluxum-bench/src/load.rs:231-250`:

```rust
impl Default for FanoutConfig {
    fn default() -> Self {
        Self { subscribers: 1_000, messages: 200, rate_per_sec: 20 }
    }
}
```

and the driver loop at `load.rs:303-308`, which sleeps `1/rate_per_sec` between commits.

At 20 commits/s — one every 50 ms — the per-connection queue is **always empty** when a
frame arrives. `recv()` returns immediately, there is never a second frame to coalesce
with, and a batching writer would measure byte-for-byte identical to today's. The parity
harness's e2e workload runs at a similar cadence (the archived stage split at
`.rulebook/archive/2026-07-23-phase0_parity-fanout-latency/tasks.md:1.1` is explicitly
"50 subscribers @ **10 msg/s**", and notes "the reader is always parked when the frame
lands").

The measured stage split confirms it: `queue_wait` of 107–115 µs at 10 msg/s is pure
**task-wake** latency, not queueing — a queue with one item in it.

**Impact.** The project's two fan-out measurements (TST-061 and the NFR-11 e2e class) both
sit in the regime where coalescing is a no-op by construction. That is not a flaw in them —
TST-061 measures the latency floor, which is what NFR-04 is about — but it means:

1. A coalescing change will show **no improvement** on the existing benches, and a reviewer
   who only runs them will conclude it did nothing (the same trap that made the
   parallel-enqueue experiment inconclusive).
2. The regime where it *does* pay — bursty commits, high commit rate, many matched
   subscriptions per connection, slow/remote consumers — is **not benchmarked at all**.

A burst scenario is needed: `FanoutConfig` with `rate_per_sec` high enough that the writer
queue depth exceeds 1 (e.g. a commit burst of B at full speed, then idle), plus a variant
with K > 1 subscriptions per connection to exercise F-005. The right headline number for
such a bench is not p99 latency but **writes per delivered message** and **bytes on the
wire per delivered row**, with p99 held as a non-regression guard.

**Confidence:** high.

---

## F-016 — RPC-008 server→client compression is specified but not implemented

**Evidence** — SPEC-006 RPC-008 (`docs/specs/SPEC-006-protocol-fluxrpc.md:159-181`)
specifies `none | gzip | brotli` negotiation, a 1-byte compression tag prefixed to every
server→client frame body, a `compression_threshold_bytes` (default 1024), and the rule that
compression runs in the per-connection send path and is never shared across subscribers.

What exists: `pub compression: Option<String>` on `Authenticate`
(`crates/fluxum-protocol/src/messages.rs:140`), always constructed as `None`
(`crates/fluxum-server/src/election.rs:635`, `replication.rs:781`). There is no encoder, no
tag byte, no `?compression=` query-parameter handling, and no
`compression_threshold_bytes` key in `config/config.example.yml`. A search for
`compression` under `crates/fluxum-server/src` returns only page-compression (storage)
metrics.

**Impact.** Adjacent to this analysis rather than central — compression reduces *bytes*,
batching reduces *packets and syscalls*, and F-004 shows the per-frame syscall/wake legs
dominate the current tail. But the two compound in one specific way worth recording:
RPC-008's 1024-byte threshold means **individual small `TxUpdate` frames (~200 bytes,
F-006) would never be compressed at all**. A coalesced batch of 8–20 such frames clears the
threshold easily and compresses extremely well, because F-006's ~100-byte envelope block
repeats near-identically in every frame. Batching is what makes RPC-008 worth implementing
for the push path; implementing RPC-008 first, without batching, would leave it inert on
exactly the traffic that matters.

**Confidence:** high for the absence; medium-high for the compression-ratio claim (the spec
itself cites "measured 7–10× on large subscription updates" at brotli quality 1, which is
consistent).

---

Next: [05 — The design space and its constraints](05-design-options.md)
