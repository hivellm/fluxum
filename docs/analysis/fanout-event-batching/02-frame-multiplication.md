# 02 — Why there are more frames than there need to be

*Findings F-005..F-008.* File 01 showed that each frame costs a wake and a syscall. This
file shows the frame count itself is higher than the protocol requires.

---

## F-005 — One `TxUpdate` frame per *(query delta × connection)*, not per *(commit × connection)*

**Evidence** — the fan-out loop, `crates/fluxum-server/src/lib.rs:1289-1344`:

```rust
for delta in deltas {                                   // one per matched UNIQUE query
    let mut by_query_id: BTreeMap<u32, Vec<u128>> = …;  // group conns by their query_id
    for (query_id, conns) in by_query_id {
        let mut tx_update = SubscriptionManager::tx_update(&diff, &delta);
        …
        let bytes = Arc::new(codec.encode(&body)?);
        for (conn_id, handle) in ctx.connections.handles_for(&conns).await {
            deliver_frame(&ctx, conn_id, &handle, OutFrame::now(Arc::clone(&bytes)), rows).await;
        }
    }
}
```

and `crates/fluxum-core/src/subscription/manager.rs:800-813`:

```rust
pub fn tx_update(diff: &TxDiff, delta: &QueryDelta) -> TxUpdate {
    TxUpdate { tx_id: diff.tx_id, …, tables: vec![(*delta.update).clone()] }
    //                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ always exactly one
}
```

The loop is `for delta { for group { for conn { send } } }`. A connection that holds **K**
subscriptions matched by a single commit therefore receives **K separate `TxUpdate` frames
carrying the same `tx_id`**, each with a one-element `tables` vector.

The wire format does not require this. RPC-033 (`docs/specs/SPEC-006-protocol-fluxrpc.md:387-409`)
declares `pub tables: Vec<TableUpdate>`, and RPC-032 already makes `TableUpdate.query_id`
the client's correlation handle:

> Clients SHALL use `TableUpdate.query_id` to correlate subscriptions with `Unsubscribe`
> messages. — SPEC-006:384

So the merged form — one `TxUpdate` per (commit, connection) carrying K `TableUpdate`s with
K distinct `query_id`s — is **already legal today** and already what every SDK is required
to handle.

**Impact.** For a connection with K matched subscriptions per commit, this multiplies every
per-frame cost in F-004 by K: K wakes, K syscalls, K envelope headers (F-006), K
`queue_wait` samples. Realistic K for the demo/parity workload is 1–2, but any application
that subscribes per entity/room/document (the canonical Fluxum shape) sits at K = 3–20.
It also multiplies the *semantic* work on the client: K cache-apply passes and K UI
notifications for what was one transaction, which SPEC-021's optimistic-overlay reconcile
(CS-011) has to handle K times.

**Constraint on the fix.** Merging naively costs the SUB-024 share: today the envelope is
encoded once per `(delta, query_id)` group and `Arc`-shared across every connection in it.
Merging *per connection* would make the encode O(subscribers). The fix is to merge per
**equivalence class**: connections whose matched set of `(delta, query_id)` pairs is
identical share one encode. In the common case — clients running the same subscription list
in the same order, which the code already calls out at `lib.rs:1293-1294` ("in the common
case … there is exactly one group") — there is exactly one class, so the merged path
encodes once and shares to everyone, exactly as today, but emits **one frame instead of K**.

**Confidence:** high for the behaviour; high for legality of the merged form (spec text is
explicit); medium for the K distribution in real deployments (workload-dependent).

---

## F-006 — Every frame carries ~100+ bytes of fixed envelope regardless of payload

**Evidence** — the on-wire cost of one `TxUpdate` frame, assembled from
`crates/fluxum-protocol/src/frame.rs:39` (`FRAME_HEADER_LEN = 4`),
`crates/fluxum-protocol/src/tagged.rs:12-40` (`["Tag", payload]` fixarray[2]) and
`crates/fluxum-protocol/src/messages.rs:294-348`:

| Component | Bytes |
|---|---|
| Frame length prefix (`u32` LE) | 4 |
| Envelope `fixarray[2]` + `"TxUpdate"` str | ~10 |
| `TxUpdate` fixarray header | 1 |
| `tx_id` u64 | 1–9 |
| `timestamp` i64 (µs epoch, ~1.7e15 → always wide) | 9 |
| `reducer_name` String (e.g. `"send_chat"`) | 1 + len |
| `caller` `bin32` (RPC-033 provenance) | **34** |
| `duration_us` u32 | 1–5 |
| `shard_id`, `tx_offset` (additive tail) | 2–14 |
| `TableUpdate` header + `table_id` + `table_name` + `query_id` | ~16 + len |
| 2 × `RowList` (`row_count`, `size_hint` tagged enum, `rows_data` bin header) | ~14 |

That totals roughly **100–130 bytes of metadata before a single row byte**. A small delta —
one inserted chat row, ~90 bytes FluxBIN — is therefore a ~200–220-byte frame that is
**over half metadata**, and on a real network another 54–66 bytes of Ethernet/IPv4/TCP
headers ride on top because F-003 guarantees one frame is one segment.

**Impact.** This is the quantitative case for F-005's merge: the ~100-byte envelope is paid
**once per frame**, not once per table. Merging K table updates into one frame collapses K
envelopes to 1 and K packet headers to 1 (assuming the merged frame still fits one MSS,
which it does for small deltas). It is also the case for compression (F-016): the
metadata block is highly repetitive across consecutive frames and compresses far better in
a batch than one frame at a time.

**Confidence:** medium-high — the field list and encodings are read directly, but the
totals are analytic (rmp-serde positional-array encoding per `messages.rs:329-335`), not
measured from a captured frame. A one-off `encode()` size assertion should confirm the
constant before it is quoted in a report.

---

## F-007 — The shared `TableUpdate` is deep-cloned per query-id group

**Evidence** — `crates/fluxum-core/src/subscription/manager.rs:811`:

```rust
tables: vec![(*delta.update).clone()],
```

`QueryDelta::update` is an `Arc<TableUpdate>` (`manager.rs:731, 741-745`) precisely so the
encoded rows are shared (SUB-024). `tx_update` dereferences and **clones** it, which copies
both `RowList::rows_data` buffers (inserts and deletes) and the `Offsets` vector.

**Impact.** Bounded but real: one full copy of the delta's row bytes per `(delta, query_id
group)` per commit, on the fan-out task, before the MessagePack encode copies them a second
time. In the common single-group case that is one extra copy per delta per commit — small
next to the 150 µs/frame delivery legs, which is why it has not shown up in profiles, but
it scales with delta size (a bulk insert fanned out to a subscribed query copies the whole
delta twice).

It matters more *after* the F-005 merge: a merged builder that takes `&[Arc<TableUpdate>]`
and serializes by reference avoids the clone entirely, so the merge is an opportunity to
delete this rather than multiply it.

**Confidence:** high.

---

## F-008 — The connection-registry mutex is taken once per group inside the hot loop

**Evidence** — `crates/fluxum-server/src/lib.rs:1340` calls
`ctx.connections.handles_for(&conns).await` inside the `for (query_id, conns)` loop, and
`crates/fluxum-server/src/conn.rs:78-84` locks a `tokio::sync::Mutex<HashMap<…>>` and
clones a `ConnHandle` (two `Arc` bumps) per target. `deliver_frame` then re-acquires the
same mutex on every drop/close path (`lib.rs:1208, 1211`).

**Impact.** `deltas × query_id_groups` async lock acquisitions per commit on the single
fan-out task, plus one handle clone per subscriber per group. This is inside the ~36 µs
`enqueue` stage, so it is not today's bottleneck — but it is O(groups), and the F-005 merge
naturally collapses it to **one lock acquisition per commit** by resolving all targets once
before building frames. Worth doing as part of that change, not on its own.

**Confidence:** high.

---

Next: [03 — Backpressure that is not wired, and loss that is not visible](03-backpressure-and-loss.md)
