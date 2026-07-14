# UzDB — Gaps Analysis & Architecture Improvements

**Date:** 2026-04-14
**Based on:** Full SpacetimeDB analysis (01-overview through 10-tml-stdlib)
**Status:** Applied — changes reflected in specs 02, 04, 05, 06, 07, 10, 13 and architecture.md

---

## Summary

Cross-referencing the SpacetimeDB protocol analysis (especially 06-subscriptions, 07-protocol,
05-transactions) against our initial architecture and specs revealed 4 critical gaps and
5 high-priority improvements. All have been applied.

---

## Critical gaps (C-series)

### C1 — Row encoding: MessagePack is wrong for the hot path

**What we had:** architecture.md chose MessagePack for the entire UzRPC body,
citing "mature TML libs." TableUpdate rows were listed as "BSATN or MsgPack-encoded" (ambiguous).

**Problem:** Protocol analysis (07-protocol) shows:
- BSATN: ★★★★★ size, ★★★★★ speed
- MessagePack: ★★★ size, ★★★★ speed

MessagePack encodes field names or type tags even for well-typed data. For `TableUpdate.rows`
(the hot path: pushed to every subscriber on every commit), schema is known on both sides —
field tags are pure overhead. At 100k tx/s × 1000 subscribers, this is ~40% more bandwidth.

**Fix:** Two-layer encoding:
- **Message envelope** stays MessagePack (flexible, debuggable with msgpack-inspect)
- **Row data inside `TableUpdate.inserts/deletes`** uses `UzBIN` (BSATN-equivalent: sequential
  little-endian field values, no names, no tags, schema-driven)

`UzBIN` matches the `ADAPT` decision in 09-tml-mapping.md ("BSATN wire encoding → UzBIN").
This was already the intended design; the architecture.md was inconsistent.

**Files changed:** architecture.md §UzRPC, spec 07 §6.

---

### C2 — TxUpdate carries no context

**What we had:** `TxUpdate { tx_id: U64, tables: List[TableUpdate] }`

**Problem:** SpacetimeDB's `TransactionUpdate` carries:
- `caller_identity` — who triggered the change
- `reducer_name` — which reducer caused it
- `timestamp` — when it happened
- `host_execution_duration_micros` — for profiling

Without these, clients cannot:
- Show "Player X attacked you" — no `caller_identity`
- Filter events for sound/VFX — no `reducer_name`
- Display timestamps — no `timestamp`
- Profile slow reducers — no `duration_us`

**Fix:** Enrich `TxUpdate`:
```
TxUpdate {
    tx_id:         U64,
    timestamp:     I64,          // added
    reducer_name:  Str,          // added (empty string for system-initiated commits)
    caller:        Identity,     // added (zeroed for system commits)
    duration_us:   U32,          // added (reducer execution time)
    tables:        List[TableUpdate],
}
```

**Files changed:** architecture.md §UzRPC, spec 07 REQ-07-15.

---

### C3 — No composite primary keys

**What we had:** `@pk` on a single column only.

**Problem:** Terrain chunks naturally key on `(chunk_x, chunk_y)`. Without composite PKs,
games must add a synthetic U64 ID column to every natural multi-key table, then maintain
a secondary index on the natural key — doubling storage and adding a lookup hop.
SpacetimeDB also lacks this. It is an explicit IMPROVE opportunity from the analysis.

**Fix:** Add `@compositePk(cols: ["col1", "col2"])` table-level annotation:
```tml
@table @public @compositePk(cols: ["chunk_x", "chunk_y"])
type TerrainChunk {
    chunk_x:   I32,
    chunk_y:   I32,
    tile_data: Buffer,
    version:   U64,
}
```

The composite PK is stored as a `BTreeMap[(PK1, PK2), Row]` where the key is a tuple.
Subscription deletes carry both PK values.

**Files changed:** spec 02 §2, spec 03 §6.

---

### C4 — Fan-out backpressure not specified

**What we had:** Fan-out delivers `TxUpdate` to all matching subscribers. No mention of what
happens when a client's TCP send buffer is full.

**Problem:** If one slow client blocks the broadcast loop, every other client is delayed.
This is a DoS vector: one client with a slow connection or intentionally blocked receive
can stall game state delivery to everyone.

**Fix:** Per-client send buffer with three-tier policy:
1. **Normal** (buffer < 50%): deliver immediately
2. **Pressured** (buffer 50–90%): deliver inserts only, skip `@tick`-sourced updates
3. **Full** (buffer > 90% or > 5s stall): drop connection, log warning

The broadcast loop SHALL NOT block for any individual client. Each client's send buffer is
checked non-blockingly; if full, the tier policy applies.

**Files changed:** spec 06 §6, spec 08 §3.

---

## High-priority improvements (H-series)

### H1 — @tick drift handling underspecified

**What we had:** "approximately the specified rate" with no definition of approximately.

**Problem:** Game loops must not accumulate backlog. If a 60Hz tick takes 20ms, naive "wait
16ms between ticks" causes unbounded queue growth. The correct behavior is the "fixed timestep
with missed frame detection" pattern used by every game engine.

**Fix:** Define precise semantics:
- Track `next_target_time = start + N * period` (absolute clock, not relative to last completion)
- If tick finishes before `next_target_time`: sleep until then
- If tick finishes after `next_target_time`: schedule next tick immediately, log missed tick
- If `now > next_target_time + (3 * period)`: log WARNING "tick budget exceeded"; reset `next_target_time = now`

**Files changed:** spec 04 REQ-04-12.

---

### H2 — TxHandle missing intra-transaction reads

**What we had:** Reads in reducers always go to `CommittedState` (REQ-03-4). No way to read
in-flight inserts within the same transaction.

**Problem:** Common game logic pattern:
```tml
ctx.tx.insert[Item]({ owner: ctx.identity, kind: SWORD })
let count = ctx.tx.scan[Item]().filter(i -> i.owner == ctx.identity).len()
// count does NOT include the just-inserted sword — bugs game logic
```

**Fix:** Add `scan_pending[T]()` to `TxHandle` that reads from `TxState.inserts`:
```tml
ctx.tx.scan_pending[T]()  // O(n) over in-flight inserts for this table
ctx.tx.count_pending[T](pred)  // count matching pending inserts
```

Combined read (committed + pending): `ctx.tx.scan_all[T]()` = committed + pending.

**Files changed:** spec 04 REQ-04-3, spec 05 §4.

---

### H3 — No rate limiting for player-callable reducers

**What we had:** No rate limiting. A client can call any reducer at unbounded rate.

**Problem:** `send_chat_message` at 10,000/s can saturate a shard even if each call is cheap.
SpacetimeDB has an energy system (complex). We can do better with a simple declarative annotation.

**Fix:** Add `@reducer(max_rate: "10/s")` annotation:
- Runtime tracks per-(identity, reducer) call counts in a rolling 1-second window
- Exceeding the limit returns `Error { code: 429, message: "rate limit exceeded" }` without executing the reducer
- The rate check happens before TxState is created (zero overhead for allowed calls)

**Files changed:** spec 04 §2, spec 07 §4.

---

### H4 — Server-to-server identity not addressed

**What we had:** Auth spec only describes player (game client) authentication.

**Problem:** The C++ game server calls UzDB reducers as a privileged peer (e.g., `create_item_batch`,
`award_quest_completion`). It needs:
- A server-level identity that differs from player identities
- Bypass of `@visibility(rule: "owner_only")` filters
- Long-lived token that doesn't expire on reconnect

**Fix:** Add `server_token` auth mode. Server-level identities occupy a reserved namespace:
`Identity = SHA-256("SERVER:" + server_name)`. Server connections bypass RLS filters.
The server token is configured via `config.yml` and is long-lived (or manually rotated).

**Files changed:** spec 10 §6.

---

### H5 — SubscribeSingle missing, Unsubscribe too coarse

**What we had:** `Subscribe { queries: List[Str] }` batches all queries.
`Unsubscribe { query_ids: List[U32] }` cancels by server-assigned IDs.

**Problem:** To add one subscription, clients must send all queries again (re-registers everything).
`query_id` is assigned by the server and returned in `InitialData` — but spec 07 doesn't make
this assignment explicit, making `Unsubscribe` hard to use.

**Fix:**
- Add `SubscribeSingle { id, query: Str }` — subscribes to exactly one query
- `InitialData.query_id` (per table) makes the server-assigned ID unambiguous
- `Unsubscribe { query_ids }` already takes a list — clarify it works for both `Subscribe` batch and `SubscribeSingle` queries

**Files changed:** spec 07 §4, spec 07 §5.

---

## Minor fixes (M-series)

### M1 — Spec 06 REQ numbering bug
`REQ-07-16` in spec 06 should be `REQ-06-16`. Fixed.

### M2 — ORDER BY + LIMIT subscription semantics
Analysis explicitly flagged: ORDER BY / LIMIT in subscription queries applies to `InitialData`
only — subsequent `TxUpdate` diffs are unordered and unlimited. Spec now says this explicitly.

### M3 — Missing observability spec
New spec 13 added: metrics catalogue, Prometheus endpoint, key counters and histograms.

---

## What was intentionally left unchanged

| Decision | Rationale |
|----------|-----------|
| MessagePack for message envelope | Remains — debuggable, flexible, mature TML libs |
| Single-writer per shard | Correct — microsecond transactions make it sufficient |
| No cross-shard transactions | Correct — eliminates distributed transaction complexity |
| No full-text search | Out of scope |
| No JOIN in subscriptions | Correct — denormalization is the intended pattern |
| @visibility over imperative RLS | Improvement over SpacetimeDB already in spec |
| @tick over manual schedule_reducer | Improvement over SpacetimeDB already in spec |
