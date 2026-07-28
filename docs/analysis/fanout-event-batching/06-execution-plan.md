# 06 — Execution plan

Three tasks, strictly sequenced. Each is independently shippable and independently
revertible. The ordering is not arbitrary: **P1 builds the measurement that justifies P2
and P3**, because the predecessor task (`phase0_parity-fanout-latency`) demonstrated that
un-measured fan-out changes get reverted, correctly, for lack of evidence.

Naming follows the precedent set by the NFR-11 parity work: fan-out latency remediation
lives in `phase0_*`.

---

## P1 — `phase0_fanout-write-coalescing` *(P0, no dependencies)*

**Closes:** F-001, F-002, F-012 (metric), F-014, F-015.
**Levers:** A (buffered chunk write) + B (opportunistic drain).
**Protocol impact:** none. No SDK change, no negotiation (F-017).

### 1. Implementation

- **1.1 Collapse `write_chunk` to one write.** `crates/fluxum-server/src/http/wire.rs:227-233`:
  build `hex-len + CRLF + data + CRLF` into a single reusable buffer, one `write_all`, one
  `flush`. Drop the per-frame `format!` allocation. *(Lever A — F-002.)*
- **1.2 Opportunistic drain in the TCP writer.** `crates/fluxum-server/src/tcp.rs:679-694`:
  replace `recv()` with `recv_many()` into a reusable `Vec<OutFrame>`, concatenate into a
  reusable scratch buffer bounded by a byte budget, one `write_all` per batch. Record
  `queue_wait` **per frame** (each `OutFrame::enqueued_at`) and `flush` per batch.
  *(Lever B — F-001, F-019.6.)*
- **1.3 Same drain in the HTTP push loop.** `crates/fluxum-server/src/http.rs:1278-1291`:
  drain the receiver and emit **one HTTP chunk carrying N frames** (legal per RPC-004/005,
  F-017). Keep the `select!` arms for keep-alive/idle/shutdown unchanged.
- **1.4 Batch byte budget.** Cap the scratch buffer; stop the drain when it is reached and
  loop. Use `ShardContext::send_buffer_bytes()` (`lib.rs:601-608`) — the currently inert
  operator knob (F-009) — so the first real consumer of the config key arrives with this
  change. *(F-019.5.)*
- **1.5 Metrics (SPEC-012 OBS-024, new).** `fluxum_fanout_batch_frames{shard}` histogram,
  `fluxum_fanout_batch_bytes{shard}` histogram, `fluxum_fanout_socket_writes_total{shard}`
  counter. Plus `fluxum_fanout_lagged_total{shard}` and a rate-limited `WARN` on the
  discarded `Lagged(n)` at `lib.rs:1237` — F-012's silent shard-wide loss. *(F-014.)*
- **1.6 Burst fan-out bench.** Extend `crates/fluxum-bench/src/load.rs:231-250`
  (`FanoutConfig`) with a burst mode: B commits at full speed then idle, so the writer queue
  depth exceeds 1. Report **writes per delivered message** and **bytes per delivered row**
  alongside p99, sourced from 1.5's counters. Keep the existing 20 msg/s scenario unchanged
  as the latency-floor guard. *(F-015.)*

### 2. Exit criteria

- Existing TST-061 fan-out p99 and the parity e2e/mixed-e2e classes **do not regress**
  (this is the guard, not the target — F-018 predicts no change at 20 msg/s).
- On the new burst bench: `fluxum_fanout_socket_writes_total / fluxum_fanout_messages_total`
  drops measurably below 1.0, and the HTTP transport's write count per frame drops from 3
  to 1 at every rate.
- Loopback suites (`crates/fluxum-server/tests/{tcp_loopback,http_loopback,fanout}.rs`) and
  the SDK conformance corpus pass unchanged — the proof that coalescing is wire-transparent.
- A new test asserts per-connection FIFO across a batch boundary (F-019.1) and that a batch
  larger than the byte budget splits rather than growing unbounded (F-019.5).

### 3. Risk

Low. Both writers are small and self-contained; the change is invisible on the wire; the
byte budget bounds the new buffer. Main hazard is the metric-semantics shift on
`FanoutStage::Flush` (per-frame → per-batch) making historical `flush` series
uncomparable — mitigated by shipping 1.5 in the same change and noting it in SPEC-012.

---

## P2 — `phase0_fanout-txupdate-merge` *(P1, depends on P1)*

**Closes:** F-005, F-006 (amortized), F-007, F-008.
**Lever:** C.
**Protocol impact:** none — the merged form is already legal (RPC-033 `tables: Vec<TableUpdate>`,
RPC-032 `TableUpdate.query_id` correlation). Behavioural change to clients, so it needs
conformance coverage.

### 1. Implementation

- **2.1 Equivalence-class grouping.** Restructure `crates/fluxum-server/src/lib.rs:1289-1344`
  from `for delta { for query_id group { for conn } }` to: build, per connection, the set of
  `(delta, query_id)` pairs matched by this commit; group connections by that signature;
  encode **once per class**. In the common case (identical subscription lists) there is one
  class, so SUB-024's encode-once holds exactly as today. *(F-005.)*
- **2.2 Merged envelope builder.** Replace
  `SubscriptionManager::tx_update` (`manager.rs:800-813`) with a builder taking
  `&[(Arc<TableUpdate>, u32 /*query_id*/)]` that serializes **by reference** — removing the
  `(*delta.update).clone()` deep copy of `rows_data`. *(F-007.)*
- **2.3 Resolve connection handles once per commit.** Hoist
  `ctx.connections.handles_for(...)` out of the per-group loop. *(F-008.)*
- **2.4 `max_frame_bytes` fallback.** If a merged envelope would exceed RPC-061's cap, split
  it back into per-table frames rather than emitting an undeliverable frame. *(F-019.4.)*
- **2.5 Spec + conformance.** Add a normative note to SPEC-005 SUB-021 and SPEC-006 RPC-033
  stating that one `TxUpdate` MAY carry several `TableUpdate`s from several queries of the
  same commit, and that clients MUST route by `TableUpdate.query_id`. Add a corpus case to
  the SDK conformance suite exercising a multi-table/multi-query `TxUpdate`, and verify all
  five SDK caches apply it correctly (including SPEC-021 CS-011 optimistic-overlay
  reconciliation, which now sees one update where it previously saw K).

### 2. Exit criteria

- A commit matching K subscriptions of one connection produces **one** frame, verified by a
  loopback test at K = 3 and by `fluxum_fanout_messages_total` on the K > 1 bench variant.
- All five SDKs pass the extended conformance corpus.
- No regression on the K = 1 parity classes (the path must stay byte-identical when K = 1).

### 3. Risk

Medium — this is the only lever that changes what a client observes. The mitigation is that
the observed form is already mandated by the spec and already exercised by `InitialData`
(which has always carried `tables: Vec<TableUpdate>`). Grouping bugs that break the
encode-once property would show up as a `fluxum_fanout_stage_us{stage="enqueue"}`
regression proportional to subscriber count — an explicit thing to watch on the bench.

---

## P3 — `phase8_subscriber-send-buffer-policy` *(P1, depends on P1)*

**Closes:** F-009, F-010, F-011, F-013.
**Lever:** D, plus the SUB-042 compliance debt that batching exposes.

### 1. Implementation

- **3.1 Byte-budgeted send path.** Adopt `SubscriberBuffer`'s policy on the real transport
  queue so `subscriptions.send_buffer_bytes` governs delivery. **Change its API to
  `Arc<Vec<u8>>` first** — `enqueue`'s `bytes.to_vec()` (`sendbuffer.rs:235`) would
  otherwise reintroduce an O(subscribers) copy and undo SUB-024. *(F-009, F-011.)*
- **3.2 Restore the Pressured tier.** Today's policy is binary (deliver / kill,
  `lib.rs:1199-1213`). Reinstate the 50–90% graded tier and the 5 s blocked-send trigger,
  with `fluxum_subscriber_drops_total{reason}` already in place to observe it.
- **3.3 `send_priority` in the table macro.** SUB-043 (`SPEC-005:360-372`) is unimplemented
  — `crates/fluxum-macros` never parses the attribute, and `high_priority`/`tick_sourced`
  are never set anywhere (F-010). Implement both flags and thread them to the offer path.
- **3.4 Conflation.** For last-value-wins updates under pressure, collapse queued frames per
  `(query_id, primary key)` to the latest instead of dropping the connection. Must be
  **declared** (an opt-in table/subscription policy), never silent — `TxUpdate` gaps are
  already recoverable via the CS-020 resume cursor, but only a declared policy makes the
  gap contractual. *(F-010.)*
- **3.5 Make `send_queue_depth` configurable** or delete it in favour of the byte budget.
  `boot.rs:530-533` reasons that two knobs for one queue would disagree — resolve that by
  keeping **one**: bytes. *(F-013.)*

### 2. Exit criteria

- `crates/fluxum-server/tests/config_hot_reload.rs` gains an assertion that changing
  `subscriptions.send_buffer_bytes` changes actual delivery behaviour, not just a stored
  atomic.
- The SUB-042 slow-consumer stress test (SPEC-005 acceptance 6) runs against the **real**
  transport rather than the standalone buffer.
- A conflation scenario: a subscriber stalled for N ticks reconnects to correct current
  state without a full resync and without being dropped.

### 3. Risk

Medium — it changes drop semantics on a live path. Sequencing it after P1 means the batch
byte-accounting already exists to build on, and the OBS-024 counters already exist to watch
it with.

---

## Explicitly out of scope / already disproven

- **A timed coalescing window enabled by default.** Rejected in F-018: it adds latency in
  exactly the regime the product's headline metric measures. Revisit only as an opt-in
  `subscriptions.coalesce_window_us = 0` knob after P1 lands.
- **Parallel/chunked per-subscriber enqueue.** Tried and reverted in
  `phase0_parity-fanout-latency` item 1.2 (measured no better, convoy risk). Do not retry.
- **A direct-socket write path bypassing the writer task.** Discarded by arithmetic in the
  same task's item 1.2. Do not retry.
- **RPC-008 compression (F-016).** A separate task. Note the dependency direction: it is
  worth doing *after* P1, because the 1024-byte threshold means individual `TxUpdate`
  frames would never be compressed anyway, while coalesced batches clear it easily.
- **Fan-out offload to replicas** (DAG T7.2) — orthogonal; it changes *who* fans out, not
  how many packets each fan-out costs.

---

## Sequencing summary

```
P1 phase0_fanout-write-coalescing   ──┬──►  P2 phase0_fanout-txupdate-merge
   (A + B + metrics + burst bench)    │
                                      └──►  P3 phase8_subscriber-send-buffer-policy
                                             (D + SUB-042/043 compliance)
```

P1 is the prerequisite for both because it delivers the byte accounting P3 needs and the
measurement P2's value has to be argued with.
