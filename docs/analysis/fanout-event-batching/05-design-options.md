# 05 — The design space and its constraints

*Findings F-017..F-020.* Four independent levers, ordered by (benefit × safety) ÷ cost.
They compose; none conflicts with another.

| # | Lever | Reduces | Protocol change | Latency risk |
|---|---|---|---|---|
| **A** | One buffered write per chunk (HTTP) | writes ×3 → ×1 | none | none |
| **B** | Opportunistic drain: one write per batch of queued frames | writes per burst | none | none |
| **C** | Merge a commit's deltas into one `TxUpdate` per connection | frames ×K → ×1 | none (already legal) | none |
| **D** | Conflation of last-value-wins updates under pressure | frames + bytes | additive (opt-in policy) | none (only engages when behind) |
| *E* | *Timed coalescing window* | writes at low rate | none | **yes — rejected as a default** |

---

## F-017 — Transport-level coalescing is wire-transparent to all five shipped SDKs

**Evidence.** Writing several complete frames in one `write_all` produces byte-identical
output to writing them one at a time — the stream is the same, only the syscall boundaries
move. Every shipped client is an incremental, buffer-draining frame reader that already
handles arbitrary chunk boundaries, including several frames arriving together:

| SDK | Reader | Drains all buffered frames |
|---|---|---|
| Rust | `sdks/rust/src/client/runtime.rs:384-407` | yes — inner `loop` "Drain every complete frame currently buffered before reading" |
| TypeScript | `sdks/typescript/src/protocol.ts:74-101` (wraps `@hivehub/thunder` `FrameReader`) | yes — `push()`/`nextBody()` partial-buffer state machine |
| Python | `sdks/python/fluxum/client.py:257-262`, `protocol.py:72-90` | yes — `while True: next_body()` after each `push(chunk)` |
| Go | `sdks/go/fluxum/client.go:267,306`, `protocol.go:65-73` | yes — `frameReader.nextBody()` loop |
| C# | `sdks/csharp/Fluxum.Sdk/Connection.cs:216,235`, `Protocol.cs:120-129` | yes — `while (_frames.NextBody() is { } body)` |

The spec anticipates it explicitly for HTTP: RPC-004/RPC-005
(`docs/specs/SPEC-006-protocol-fluxrpc.md:117-119, 132-139`) already require request and
response bodies to carry "one or more standard FluxRPC frames … **concatenated
back-to-back**". Multiple frames inside one HTTP chunk is likewise unremarkable — chunk
boundaries are transport framing beneath the FluxRPC frame layer and carry no semantics.

**Impact.** Levers A and B need **no version negotiation, no capability flag, and no SDK
release**. They are pure server-side changes that existing deployed clients cannot observe
except as lower latency. This is the single most important property in this analysis: it
makes the highest-value work the lowest-risk work.

**Confidence:** high — verified reader-by-reader across all five SDKs.

---

## F-018 — A timed coalescing window would trade the product's headline metric; opportunistic drain dominates it

Two ways to batch:

- **Timed window** — hold outgoing frames for `W` µs, then flush whatever accumulated.
  Classic Nagle-in-userspace. Guarantees batching even at low commit rates.
- **Opportunistic drain** — never wait; when the writer wakes, take *everything already
  queued* (`mpsc::Receiver::recv_many`, available in the pinned `tokio 1.52.3`,
  `Cargo.lock:3651-3653`) and issue one write.

The arithmetic decides it:

| Regime | Timed window (W) | Opportunistic drain |
|---|---|---|
| Queue has 1 frame (the TST-061/parity regime, F-015) | **adds up to W to p99** | identical to today — `recv_many` returns 1 |
| Queue has N frames (burst) | 1 write per W | 1 write per wake, ~N frames |
| Consumer slow / socket backed up | 1 write per W | batch grows automatically as the queue grows |

Opportunistic drain is **weakly better in every regime**: it equals the timed window when
batching is possible and equals today's behaviour when it is not. Its batch size
self-tunes to exactly the backlog, which is the definition of the right amount of
coalescing. A timed window's only advantage is batching frames that have not been produced
yet — and paying `W` on the p99 to get it is directly adverse to NFR-04 (< 5 ms fan-out
p99), NFR-11 (e2e ≥ 10×), and the marketing claim the whole parity harness exists to
defend.

**Recommendation:** implement opportunistic drain as the default and *only* mechanism.
If a timed window is ever wanted (a high-fan-out, latency-tolerant deployment shape), add
it later as an explicitly opt-in `subscriptions.coalesce_window_us` defaulting to `0`, and
document it as a throughput-for-latency trade. Do **not** ship it enabled.

**Impact.** Prevents the most likely way this work could regress the product's headline
number.

**Confidence:** high — this is arithmetic on the measured stage split (F-004), not
speculation.

---

## F-019 — The invariants a batching writer must preserve

Enumerated so the implementation has an explicit contract to test against:

1. **Per-connection FIFO.** Frames must reach the socket in enqueue order. A drain-then-
   concatenate writer preserves this trivially (`recv_many` yields in order); anything that
   reorders or parallelizes *within* one connection does not. The archived
   `phase0_parity-fanout-latency` item 1.2 already learned this lesson for the enqueue side.
2. **`tx_id` order across commits.** Guaranteed upstream: the single writer publishes to
   the broadcast in `tx_id` order at commit visibility (`lib.rs:1231-1245`, and SPEC-005
   SUB-021 "Delivery visibility semantics"). Batching downstream of that cannot disturb it.
3. **One `tx_id` per `TxUpdate`.** Lever C merges *tables within one commit* into one
   envelope. It must **not** merge across commits — `TxUpdate.tx_id`, `timestamp`,
   `reducer_name`, `caller`, `duration_us` and `tx_offset`
   (`crates/fluxum-protocol/src/messages.rs:313-348`) are per-commit provenance that
   SPEC-021 CS-011 optimistic reconciliation and CS-020 resume both depend on. Cross-commit
   coalescing happens at the **write** layer (Lever B), never at the envelope layer.
4. **`max_frame_bytes` is per frame, not per write.** RPC-061's 16 MiB cap
   (`crates/fluxum-protocol/src/frame.rs:41-42`) bounds one frame; a batch of many frames
   legitimately exceeds it and clients will not (and must not) reject it — every reader
   above checks the *per-frame* prefix. Lever C, which grows a single frame, **is** bound by
   it and must fall back to splitting when a merged envelope would exceed the cap.
5. **The batch buffer needs its own byte cap.** An unbounded drain-and-concatenate turns a
   1024-frame backlog of 16 MiB frames into a 16 GB `Vec` (F-009 §2). The batch must stop
   at a byte budget — the natural value is the one that already exists and is currently
   inert, `subscriptions.send_buffer_bytes` (2 MiB default) — and loop. This is also why
   fixing F-009 and implementing Lever B are the same work: batching *requires* byte
   accounting.
6. **Metrics semantics shift.** `FanoutStage::Flush` becomes per-batch; `queue_wait` should
   continue to be recorded **per frame** (each frame's own `enqueued_at`), or the stage
   loses its meaning. See F-014.

**Confidence:** high.

---

## F-020 — Where each lever pays, quantified against the measured baseline

Using F-004's measured per-frame legs (`flush` 41–46 µs, `queue_wait` 107–115 µs) and
F-006's ~100–130 byte envelope:

**Lever A (HTTP `write_chunk`)** — 3 writes + 1 flush → 1 write + 1 flush per frame.
Saves 2 syscalls/frame on the browser transport unconditionally, plus ~42 bytes of TLS
record overhead per frame when TLS is on. Applies at *every* rate, including the
TST-061/parity regime where B and C do nothing. **The single highest
certainty-to-effort item in this analysis** — it is a ten-line change to
`crates/fluxum-server/src/http/wire.rs:227-233`.

**Lever B (opportunistic drain)** — at a queue depth of D frames, writes drop from D to 1
and the wall time of the write leg from `D × 41 µs` to roughly `41 µs + D × memcpy`. It is
a **no-op at low rate by design** (F-018), so its value must be argued and measured on the
burst bench F-015 asks for, not on TST-061. Its secondary effect is the one that matters
operationally: a slow consumer's backlog now drains in O(1) syscalls per wake instead of
O(N), which is what turns a transient stall into a recovery instead of a SUB-042 kill.

**Lever C (per-connection `TxUpdate` merge)** — divides frames by K, the number of a
connection's subscriptions matched by one commit. At K = 1 (today's parity workload) it
changes nothing; at K = 5 it removes 4 wakes, 4 syscalls and ~500 bytes of envelope per
commit per connection. It is the only lever that helps in the **low-rate** regime that
TST-061 and NFR-11 measure, because it reduces frames per commit rather than writes per
burst. It is also the one that requires care to preserve SUB-024's encode-once
(equivalence-class grouping, F-005).

**Lever D (conflation)** — engages only when a subscriber is behind. Converts today's
"kill the connection" (F-010) into "deliver current state", which is both cheaper and a
better product behaviour. Not a throughput optimization; a resilience one.

**Combined expectation.** For the reference workload (K≈1, 20 msg/s), A alone is
measurable and B/C are not. For a burst or high-K workload the three compose
multiplicatively: `writes = commits × subscribers` becomes
`writes ≈ ceil(burst_frames / batch) × subscribers`, and each write carries K merged
tables instead of one. No single number can be promised before the F-015 bench exists —
which is why the plan puts the bench first.

**Confidence:** medium-high — the mechanism is certain, the magnitude is workload-dependent
and deliberately not asserted.

---

Next: [06 — Execution plan](06-execution-plan.md)
