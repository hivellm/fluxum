# 03 — Backpressure that is not wired, and loss that is not visible

*Findings F-009..F-013.* Batching changes what "the queue is full" means, so the queue's
current semantics have to be understood before changing them. They turn out to be weaker
than the spec, and weaker than the code's own documentation claims.

---

## F-009 — `SubscriberBuffer` (SUB-042's three-tier policy) is not wired to any transport

**Evidence**

`crates/fluxum-core/src/subscription/sendbuffer.rs` implements SUB-042 in full: a
byte-budgeted buffer with Normal (<50%) / Pressured (50–90%) / Full (>90% or blocked >5 s)
tiers, tick-sourced skipping, SUB-043 high-priority exemption, and a reason-labelled drop
counter. It is exported at `crates/fluxum-core/src/subscription/mod.rs:34,39`.

Its only callers are its own unit tests and
`crates/fluxum-core/tests/subscriber_backpressure.rs`. A repo-wide search for
`SubscriberBuffer` outside `sendbuffer.rs` returns three **doc-comment links** in
`crates/fluxum-server/src/lib.rs:358, 603, 606` and nothing else.

What the transports actually use is a plain frame-counted `tokio::sync::mpsc`:

- `crates/fluxum-server/src/conn.rs:36-41` — `ConnHandle { sink: mpsc::Sender<OutFrame>, shutdown }`
- `crates/fluxum-server/src/tcp.rs:354` and `http.rs:1082` — `mpsc::channel(send_queue_depth)`
- `crates/fluxum-server/src/lib.rs:1199-1213` — `deliver_frame`: `try_send` → `Ok` deliver,
  `Full` → **drop the connection immediately**, `Closed` → deregister.

The consequences:

1. **`subscriptions.send_buffer_bytes` has no effect on the send path.** It is parsed
   (`config/mod.rs:876,884`, default 2 MiB), plumbed into `ShardContext`
   (`lib.rs:357-361, 489-491`), made hot-reloadable (`config/reload.rs:44`,
   `lib.rs:774-775`), exposed by `ShardContext::send_buffer_bytes()` (`lib.rs:601-608`) and
   asserted by `crates/fluxum-server/tests/config_hot_reload.rs:195-242` — and then read by
   nobody on the delivery path. The operator knob is inert.
2. **The real bound is frames, not bytes.** `send_queue_depth = 1024` frames (F-013) means
   the actual memory ceiling per slow consumer is `1024 × frame_size` — anywhere from
   ~200 KB (small deltas) to **16 GB** at the RPC-061 `max_frame_bytes` of 16 MiB. Under a
   bulk-insert fan-out this is a genuine memory-exhaustion path, and it is the direct
   inverse of what SUB-042 specifies.
3. **The graded tiers do not exist.** There is no Pressured tier — the policy is binary:
   deliver, or kill the connection. A subscriber that is 51% full is treated identically to
   one that is 89% full.
4. **The 5-second blocked-send trigger does not exist** on the real path; only queue
   fullness kills.

**Impact.** High. This is a specified P0 control (SUB-042) whose implementation is complete,
tested, and disconnected. It also blocks the natural fix for batching: a coalescing writer
needs **byte** accounting to size a batch, which is exactly what `SubscriberBuffer` already
tracks and the `mpsc` does not.

**Confidence:** high — verified by exhaustive search; the only hits outside the module are
doc links.

---

## F-010 — There is no conflation: stale updates are queued, then the connection is killed

**Evidence** — `deliver_frame` (`lib.rs:1199-1213`) has exactly two outcomes for a
non-closed connection: enqueue, or drop the connection with `DropReason::BufferFull`. The
core policy's softer middle option (`Offered::SkippedPressured`,
`sendbuffer.rs:206-208`) is unreachable (F-009), and even that only *skips* — it never
*replaces*.

Neither of the two flags the policy keys on is ever produced: a repo-wide search for
`tick_sourced`, `high_priority` and `send_priority` outside `sendbuffer.rs` returns only
`crates/fluxum-core/tests/subscriber_backpressure.rs:24-25`. SUB-043's
`#[fluxum::table(public, send_priority = …)]` attribute (SPEC-005:360-372) is **not
implemented in the table macro** — `crates/fluxum-macros` never parses it.

**Impact.** The classic realtime lever this analysis is named after — *frame coalescing*,
where a slow consumer receives the **latest** state of an entity rather than every
intermediate one — is unavailable. For a `#[fluxum::tick]`-driven workload (positions,
counters, sensor readings) a subscriber that falls 300 frames behind is currently killed
and forced through a full resubscribe/resync, when collapsing those 300 frames to the last
one per (query, primary key) would have kept it alive and current. Conflation is also
strictly cheaper than delivery: it *reduces* both frames and bytes.

Note the correctness boundary: conflation is only sound for updates whose semantics are
"latest value wins". `TxUpdate` is documented as a cache delta, not a durability receipt
(SPEC-005 SUB-021 "Delivery visibility semantics"), and clients already tolerate gaps via
the `tx_id`/`tx_offset` resume cursor (RPC-062, SPEC-021 CS-020) — so a *declared*,
opt-in conflation policy is consistent with the protocol, but silent conflation of
arbitrary deltas is not.

**Confidence:** high for the absence; medium for the applicability breadth (depends on how
many real tables are last-value-wins).

---

## F-011 — `SubscriberBuffer::enqueue` copies the bytes its own docs promise not to copy

**Evidence** — `crates/fluxum-core/src/subscription/sendbuffer.rs:228-236`:

```rust
fn enqueue(&mut self, bytes: &[u8], now: Timestamp) {
    …
    self.queued_bytes += bytes.len();
    self.queue.push_back(bytes.to_vec());   // ← full copy, per subscriber
}
```

against its module doc at `sendbuffer.rs:8-10` and `84-90`:

> The fan-out enqueues shared, already-encoded bytes into it … the buffer only tracks the
> length, **never copies**.

and against SUB-024 (SPEC-005:254-258), which requires per-subscriber fan-out work to be a
refcount bump.

**Impact.** Latent, not live — the type is unused (F-009). But it is a trap: wiring
`SubscriberBuffer` in as-is to fix F-009 would silently introduce an **O(subscribers) byte
copy** on the fan-out path, undoing the single most important property of the current
design (`lib.rs:1332,1341` shares one `Arc` across all subscribers). The signature must
become `Arc<Vec<u8>>`/`Bytes`, not `&[u8]`, before it is adopted.

**Confidence:** high.

---

## F-012 — A lagging fan-out drops commits for every subscriber, silently

**Evidence** — `crates/fluxum-server/src/lib.rs:1233-1239`:

```rust
recv = commits.recv() => match recv {
    Ok(entry) => entry,
    // Lagged: the fan-out fell behind; clients recover on reconnect via
    // the tx_id gap (SPEC-006 acceptance 14).
    Err(broadcast::error::RecvError::Lagged(_)) => continue,
    Err(broadcast::error::RecvError::Closed) => break,
},
```

The `Lagged(n)` count is discarded. There is **no metric, no log line, and no event** — the
skipped commits vanish. Contrast `crates/fluxum-server/src/http.rs:717, 859`, where the
console's log/CDC streams do handle `Lagged(n)` explicitly with a marker.

The channel is `broadcast::channel(COMMIT_BROADCAST_CAPACITY)` with
`COMMIT_BROADCAST_CAPACITY = 256` hardcoded (`crates/fluxum-server/src/boot.rs:533`,
consumed at `lib.rs:446` and `namespace.rs:98`).

**Impact.** The measured single-writer ceiling is ~64k commits/s
(SPEC-013 TST-060 note). At that rate a 256-slot buffer gives the fan-out task **~4 ms** of
slack before it starts discarding commits for *all* subscribers on the shard at once.
Recovery is correct-by-protocol (clients detect the `tx_id` gap and resync) but expensive
and — critically — **invisible**: an operator cannot tell a healthy shard from one silently
shedding every subscriber's updates. Every SUB-042 subscriber drop is metered
(`fluxum_subscriber_drops_total`); this much larger, shard-wide loss mode is not.

This is directly relevant to batching: a fan-out loop that drains several commits per wake
(and merges them per connection) has strictly more headroom against `Lagged` than one that
processes exactly one commit per wake.

**Confidence:** high.

---

## F-013 — `send_queue_depth` is hardcoded in both transports

**Evidence** — `crates/fluxum-server/src/tcp.rs:45,56` and
`crates/fluxum-server/src/http.rs:74,96` both define `send_queue_depth: usize` in their
options struct with `1024` as the `Default`. No config key maps to it: it does not appear in
`config/config.example.yml`, in `crates/fluxum-core/src/config/mod.rs`, or in the
hot-reload allowlist (`config/reload.rs`). `boot.rs:530-533` explicitly reasons that
`send_queue_depth` is *the* backpressure knob ("Two knobs for one queue would only let them
disagree") — but the knob it defers to is the one that cannot be set.

**Impact.** The only live bound on per-subscriber buffering is a compile-time constant, and
the documented operator control for it (`subscriptions.send_buffer_bytes`) is inert
(F-009). A 512 MB droplet (`config/config.droplet-1vcpu-512mb.yml`) and a 128 GB reference
box get the same 1024-frame queue per connection.

**Confidence:** high.

---

Next: [04 — What we cannot currently see or measure](04-observability-and-benchmarks.md)
