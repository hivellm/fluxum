# SpacetimeDB Code Analysis — 06: Client Protocol & API Surface

Deep implementation analysis of the real SpacetimeDB source, focused on the wire encoding
(SATS/BSATN), the WebSocket protocol message set (v1/v2/v3), the `client-api` axum surface,
per-connection server infrastructure, compression at fan-out, and the Postgres wire adapter.
Written to inform Fluxum's SPEC-006 (FluxRPC: `u32 LE + MessagePack` envelope, FluxBIN rows,
TCP + Streamable HTTP, no WebSocket).

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` |
| **Crates analyzed** | `crates/sats` (~14k LoC), `crates/client-api-messages`, `crates/client-api` (~7k LoC), `crates/pg`, plus `crates/core/src/client/*` and `crates/core/src/subscription/websocket_building.rs` for the serialize/fan-out path |
| **Key files** | `sats/src/bsatn/{ser,de}.rs`, `sats/src/buffer.rs`, `sats/src/algebraic_type.rs`, `client-api-messages/src/websocket/{common,v1,v2,v3}.rs`, `client-api/src/routes/subscribe.rs` (2,682 lines), `client-api/src/routes/database.rs`, `client-api/src/auth.rs`, `core/src/client/{messages,client_connection}.rs`, `core/src/subscription/module_subscription_manager.rs`, `pg/src/pg_server.rs` |
| **Fluxum counterpart** | `docs/specs/SPEC-006-protocol-fluxrpc.md` (RPC-001..RPC-063) |

---

## 1. SATS: how types are described

`crates/sats/src/algebraic_type.rs` defines `AlgebraicType`, a **structural** (not nominal)
type system with 22 variants: `Ref(AlgebraicTypeRef)`, `Sum(SumType)`, `Product(ProductType)`,
`Array(ArrayType)`, `String`, `Bool`, and the full integer/float ladder `I8/U8 … I256/U256,
F32, F64`. Two design points matter:

- **`Ref` + `Typespace`** (`sats/src/typespace.rs`): recursive/shared types are not inlined;
  they are indices into a `Typespace` (a `Vec<AlgebraicType>`). Every schema shipped to a
  client is a typespace plus root refs. This is what makes their generated SDKs and the
  `/schema` endpoint possible — the type description is itself a SATS value (`MetaType`
  gives `AlgebraicType` a self-describing meta-type, so schemas travel over the same codec).
- **Special types are structural newtypes, not new variants**
  (`sats/src/product_type.rs`, `sats/src/sum_type.rs`): `Identity` is
  `Product { "__identity__": U256 }`, `ConnectionId` is `Product { "__connection_id__": U128 }`,
  `Timestamp` / `TimeDuration` are `Product { "__timestamp_micros_since_unix_epoch__": I64 }` /
  `{ "__time_duration_micros__": I64 }`, `UUID` similarly. `Option<T>` is
  `Sum { some(T) = tag 0, none = tag 1 }`, `Result` is `Sum { ok = 0, err = 1 }`. Because
  products have zero encoding overhead (§2), these wrappers are **free on the wire** — an
  `Identity` encodes as exactly 32 raw bytes, a `Timestamp` as 8. The "specialness" lives
  entirely in the type layer (magic field-name tags recognized by `is_identity()` etc.), so the
  value codec stays a 6-rule recursion.

Serde is bridged, not used natively: SATS has its own `Serialize`/`Deserialize` traits
(`sats/src/ser.rs`, `sats/src/de.rs`) mirroring serde's design, plus adapters
(`sats/src/ser/serde.rs`, `sats/src/de/serde.rs`, `SerializeWrapper`) so any SATS value can be
fed to `serde_json` for the JSON protocol and HTTP responses. BSATN is a direct implementation
of the SATS `Serializer` trait — no serde in the binary hot path. This mirrors Fluxum's
decision to hand-roll FluxBIN without serde (RPC-040).

## 2. BSATN: exact encoding rules and perf machinery

From `sats/src/bsatn/ser.rs` + `sats/src/buffer.rs` (`BufWriter` writes all integers
`to_le_bytes`):

| Type | Encoding |
|---|---|
| `bool` | 1 byte, `0x00`/`0x01`; decoder **rejects** 2–255 (`DecodeError::InvalidBool`) |
| `u8..u256`, `i8..i256` | fixed-width little-endian (`buffer.rs::put_uN`) |
| `f32`/`f64` | IEEE-754 bit pattern as LE `u32`/`u64` |
| `String` / `&[u8]` | `u32` LE length + raw bytes (`put_len` errors if > `u32::MAX`) |
| array (`Vec<T>`) | `u32` LE element **count** + elements back-to-back |
| product (struct) | fields in declaration order, **no header, no names, no separators**; named products serialize identically to unnamed (`ForwardNamedToSeqProduct`) |
| sum (enum) | `u8` tag + payload (`serialize_variant`), max 256 variants |

This is byte-for-byte the FluxBIN design (RPC-040) with three deltas Fluxum should note:

1. **Option tag order is inverted**: BSATN `Option` = `some` tag `0`, `none` tag `1`
   (`sum_type.rs::OPTION_SOME_TAG` order); FluxBIN uses `0x00 = None`, `0x01 + payload = Some`.
   Same size, pure convention — but Fluxum's convention has the nice property that `None`
   is a zero byte, matching C-style "null = 0" intuition. No reason to change; just don't
   copy their SDK test vectors blindly.
2. **BSATN has 128/256-bit integers** (needed for `Identity` = u256). Fluxum's `Identity` is
   `[u8; 32]` raw, which encodes identically; FluxBIN needs no u256 arithmetic type.
3. **Strict decode validation** is a first-class concern: `bsatn.rs` proptests
   round-trips *and* rejects (bad bools, unknown sum tags with
   `InvalidTag { tag, sum_name }`, truncation with typed `BufferLength` errors). FluxBIN's
   acceptance tests (golden vectors) should copy the negative cases too.

**Perf machinery worth copying:**

- `bsatn::to_len()` (`bsatn.rs`) computes encoded size via a `CountWriter` without
  allocating — used to pre-size buffers and to make compression decisions
  (`message_size()` in `subscribe.rs`).
- `ToBsatn` trait (`bsatn.rs`): `static_bsatn_size() -> Option<u16>` reports when a row type
  has a fixed encoded size (backed by `StaticLayout` in `sats/src/layout.rs`); when it does,
  rows are memcpy'd from page storage into the wire buffer without per-field dispatch, via
  `to_bsatn_extend` + `BufReservedFill::reserve_and_fill` (write into `spare_capacity_mut`,
  `set_len` after — no zeroing).
- `serialize_bsatn_in_chunks` / `serialize_str_in_chunks`: rows that live discontiguously in
  page storage are emitted as chunk iterators, avoiding an intermediate concatenation
  (debug-asserted valid, `unsafe` trusted in release).

## 3. Row batching: `BsatnRowList` and `RowSizeHint`

The single most copy-worthy structure in the protocol
(`client-api-messages/src/websocket/common.rs`):

```rust
pub struct BsatnRowList {
    size_hint: RowSizeHint,   // FixedSize(u16) | RowOffsets(Arc<[u64]>)
    rows_data: Bytes,         // ALL rows flattened into one buffer
}
```

Rows in a table update are **not** `Vec<Vec<u8>>`. They are one flat `Bytes` buffer plus an
out-of-band boundary hint: `FixedSize(n)` when every row encodes to the same `n` bytes
(the common case for numeric tables — zero per-row overhead, row count = `len / n`, O(1)
random access, trivially parallel decode), else `RowOffsets` (start offsets only; ends
inferred from the next start). The builder (`core/src/subscription/websocket_building.rs`,
`BsatnRowListBuilder`) starts optimistic (`FixedSizeStatic` if `static_bsatn_size()` is
`Some`, else `FixedSizeDyn` from the first row) and degrades to offsets only on the first
size mismatch, retroactively synthesizing the offset table. Buffers come from a
`BsatnRowListBuilderPool` and are reclaimed after the message is written
(`msg.consume_each_list(...)` in `core/src/client/messages.rs`).

**Fluxum contrast**: SPEC-006 RPC-032 specifies `inserts: Vec<Vec<u8>>` — one allocation and
one MessagePack `bin` header per row. At the stated design point (100k tx/s × 1k subscribers)
that is exactly the overhead `BsatnRowList` exists to kill. Adopting a
`(size_hint, rows_data)` shape inside `TableUpdate` costs nothing in MessagePack (two fields)
and removes per-row allocation on encode *and* decode, enables `Bytes`-slice zero-copy on the
server, and gives browsers a single `Uint8Array` subarray per row.

## 4. WS protocol v1 → v2 → v3: the message set and its lessons

### v1 (`websocket/v1.rs`, subprotocols `v1.json.spacetimedb` and `v1.bsatn.spacetimedb`)

- **Client → server**: `CallReducer{reducer, args, request_id, flags}`, `Subscribe`
  (replaces the *entire* query set), `SubscribeSingle`, `SubscribeMulti`, `Unsubscribe`,
  `UnsubscribeMulti`, `OneOffQuery{message_id: bytes, query_string}`, `CallProcedure`.
- **Server → client**: `InitialSubscription`, `TransactionUpdate`, `TransactionUpdateLight`,
  `IdentityToken`, `OneOffQueryResponse`, `SubscribeApplied`, `UnsubscribeApplied`,
  `SubscriptionError`, `SubscribeMultiApplied`, `UnsubscribeMultiApplied`, `ProcedureResult`.
- The whole tree is generic over `WebsocketFormat` (`BsatnFormat` | `JsonFormat`) and every
  server path carries a `FormatSwitch<Bsatn, Json>` enum (`core/src/client/messages.rs`
  `SwitchedServerMessage`). The dual-format requirement **infects everything**: double
  memoized encodes in fan-out (`memo_encode::<BsatnFormat>` and `::<JsonFormat>` in
  `module_subscription_manager.rs`), `ToProtocol` conversions on every message type,
  `protocol.assert_matches_format_switch()` runtime assertions, and a `JsonRowListBuilderFakePool`.
- `TransactionUpdate` (full) carries: `status` (Committed(DatabaseUpdate)/Failed(msg)/OutOfEnergy),
  `timestamp`, `caller_identity`, `caller_connection_id`, `reducer_call: ReducerCallInfo
  {reducer_name, reducer_id, args (re-encoded per format!), request_id}`, `energy_quanta_used`,
  `total_host_execution_duration`. A code comment admits the energy field is "lying"
  (a unit-converted CPU figure kept for compatibility). Another `NOTE(centril, 1.0)` warns that
  broadcasting `reducer_name` per commit pushes bandwidth-constrained users toward one-letter
  reducer names.
- `TransactionUpdateLight` = `{request_id, update: DatabaseUpdate}` — nothing else. Selected
  per-connection via `?light=true` at the WS URL (`SubscribeQueryParams` in
  `subscribe.rs`); the reducer *caller* always gets the full update, and failed reducers
  always produce full `TransactionUpdate`s (the error must reach the caller).
  `CallReducerFlags::NoSuccessNotify` additionally suppresses success echo for unsubscribed
  callers.
- `TableUpdate` = `{table_id, table_name, num_rows, updates: SmallVec<[QueryUpdate; 1]>}`;
  `QueryUpdate` = `{deletes: RowList, inserts: RowList}` — deletes are **full rows**, not PKs.

### v2 (`websocket/v2.rs`, subprotocol `v2.bsatn.spacetimedb`) — the corrections release

- **JSON is gone.** Binary only; text frames are rejected at decode
  (`message_handlers_v2.rs`: "v2 websocket does not support text messages"). The
  `FormatSwitch` machinery survives only in the v1 compatibility path. This is the strongest
  external validation of Fluxum's binary-only choice — SpacetimeDB shipped dual-format,
  paid the generic-infection tax across ~10 crates, and walked it back.
- **Uniform `request_id: u32` on every client message** and echoed in every response —
  exactly Fluxum's RPC-002 multiplexing id (v1's `OneOffQuery` had used a byte-blob
  `message_id` instead).
- The five subscribe variants collapse to `Subscribe{request_id, query_set_id, query_strings}` /
  `Unsubscribe{request_id, query_set_id, flags}` over client-chosen `QuerySetId(u32)`.
  `UnsubscribeFlags::SendDroppedRows` makes the expensive "return the rows you should evict"
  behavior opt-in (in v1 `UnsubscribeApplied` always carried them, with a TODO lamenting the
  cost).
- **`TransactionUpdate` was stripped to nothing but rows**: `{query_sets: Box<[QuerySetUpdate]>}`,
  each `{query_set_id, tables: [{table_name, rows: [PersistentTable{inserts, deletes} |
  EventTable{events}]}]}`. Caller identity, timestamp, reducer name, energy — all removed from
  the broadcast; reducer metadata moved to the caller-only `ReducerResult{request_id,
  timestamp, result}` where `ReducerOutcome::Ok(ReducerOk{ret_value, transaction_update})`
  inlines the caller's own tx update, `OkEmpty` is a dedicated variant that exists purely to
  save 8 bytes of length prefixes, and `Err(Bytes)` carries a **typed, BSATN-encoded error
  value** (structured reducer errors, not strings).
- Updates are grouped **per query set** (client knows which subscription matched without
  re-evaluating), and `EventTable` rows are distinguished from persistent-table diffs in the
  type, with an explicit comment reserving a future variant for in-place PK updates.

### v3 (`websocket/v3.rs`, subprotocol `v3.bsatn.spacetimedb`)

Fourteen lines: no new schema — a v3 binary payload is simply **one or more concatenated
BSATN v2 `ServerMessage`s**. It exists because WebSocket gave them 1-message-per-payload
semantics and coalescing needed a version bump. The server coalesces up to
`V3_MAX_UNCOMPRESSED_PAYLOAD_SIZE = 512 KiB` per payload (`subscribe.rs`), and payloads at or
above that size are encoded on rayon (`maybe_spawn_encode` / `spawn_rayon`) instead of the
tokio worker. Fluxum's framing (RPC-001/RPC-005: frames concatenated back-to-back in one HTTP
body) has v3 semantics from day one — no version bump will ever be needed for batching.

**Protocol negotiation** (`subscribe.rs::handle_websocket`): the four subprotocols are offered
via `Sec-WebSocket-Protocol` in preference order v3-bin, v2-bin, v1-bin, v1-text; per-connection
knobs ride the query string: `?compression=None|Brotli|Gzip` (default **Brotli**),
`?light=true`, `?confirmed=true|false` (deliver updates only after durability confirmation —
default on for v2/v3). Fluxum equivalents: negotiate nothing (single codec), but `light`,
`confirmed`, and `compression` as per-session settings are features worth stealing.

## 5. Compression: what, where, and the fan-out lesson

Mechanics (`core/src/subscription/websocket_building.rs`,
`core/src/client/messages.rs`):

- Algorithms: **Brotli quality 1** (measured 7:1–10:1 on large subscription updates) and
  **gzip fast**; enum `Compression { None, Brotli (default), Gzip }`.
- Threshold: `decide_compression()` — compress only above **1024 bytes** uncompressed
  (comment: "chosen without measurement. TODO(perf): measure!").
- Framing: every binary server payload is prefixed with a **1-byte compression tag**
  (`0 = none, 1 = brotli, 2 = gzip`, `SERVER_MSG_COMPRESSION_TAG_*`). The tag is written
  first into the uncompressed buffer so no byte-shifting is needed if compression is skipped
  (`SerializeBuffer::write_with_tag` / `compress_with_tag` — two pooled `BytesMut`, one for
  uncompressed, one for compressed output).
- JSON protocol is never compressed (relies on nothing; `compressed_capacity = 0`).

**Where compression happens is the interesting history.** v1's
`CompressableQueryUpdate { Uncompressed | Brotli(Bytes) | Gzip(Bytes) }` allowed compressing
each query update **once, inside the tx pipeline, shared across all subscribers** of that
query. They abandoned that. The long comment in
`module_subscription_manager.rs::eval_updates_sequential_inner` (~line 1590) records the
trade-off: shared compression means (1) holding the tx lock while compressing, (2) worse
ratios (small units), (3) per-query decompression overhead on clients. Current design:
rows are **encoded once per format and shared** (`memo_encode` memoizes the BSATN bytes; the
per-client clone is a `Bytes` refcount bump), but **compression runs per-client, per-payload,
in each client's send path** — off the tx lock, over the whole message, on the client's own
schedule. For Fluxum's fan-out (SPEC-005 C4): share the FluxBIN row encoding across
subscribers, never share compression; if compression is added to FluxRPC, apply it per-frame
in the per-session writer with a size threshold and a leading tag byte.

## 6. `client-api`: routes, auth, energy

Router (`client-api/src/routes/mod.rs`, `database.rs::into_router`) — everything under `/v1`,
permissive CORS, plus a separate `/internal` tree:

| Route | Purpose |
|---|---|
| `/v1/database/:name_or_identity` PUT/GET/DELETE, `/names`, `/reset`, `/lock`, `/unlock`, `/pre_publish` | database lifecycle (publish is a PUT of the wasm module) |
| `/v1/database/:n/subscribe` GET | **the** WebSocket upgrade endpoint |
| `/v1/database/:n/call/:reducer` POST | call reducer/procedure over plain HTTP (JSON args in, JSON out) |
| `/v1/database/:n/sql` POST | one-off SQL; body = SQL text, response = JSON `Vec<SqlStmtResult{schema: ProductType, rows, total_duration_micros, stats}>` + `SpacetimeExecutionDurationMicros` header |
| `/v1/database/:n/schema?version=10` GET | full module schema (typespace) as JSON |
| `/v1/database/:n/logs`, `/unstable/timestamp` | ops |
| `/v1/database/:n/route/*path` | **unauthenticated** user-defined HTTP handlers (webhooks) — deliberately outside the auth middleware |
| `/v1/identity/*` | identity mint/verify (`websocket-token` short-lived tokens) |
| `/v1/energy/:identity` GET/PUT/POST | energy balance as JSON (`i128` serialized **as a string** to avoid JS truncation) |
| `/v1/ping`, `/v1/metrics`, `/v1/prometheus` | health/metrics |

Auth (`client-api/src/auth.rs`): a JWT bearer token in `Authorization: Bearer` **or**
`?token=` query param (WS browser clients can't set headers). Anonymous connections are
allowed: `anon_auth_middleware` mints a fresh identity + self-signed token
(`SpacetimeAuth::alloc`, issuer/subject → `Identity::from_claims`). The token is returned
in response headers *and* re-sent as the first WS message (`IdentityToken` /
`InitialConnection`) because "some client libraries are unable to access the http response
headers" — Fluxum's `AuthResult` frame (RPC-030) already does the in-band thing; keep it.
Claims cache keys validators per issuer; energy/budget checks live in the host, not
middleware — HTTP-level limits are effectively just the WS incoming queue plus the module's
energy budget.

## 7. Per-connection server infrastructure (the cost of WebSocket)

For **each** connected client (`subscribe.rs` + `core/src/client/client_connection.rs`):

- **Tasks (5)**: main `select!` actor (`ws_main_loop`: ping interval, idle timer, module
  hotswap watcher, close-handshake sequencing), send loop (`ws_send_loop`), encode task
  (`ws_encode_task`, spawned separately so encoding never blocks the socket writer), recv
  queue pump (`ws_recv_queue`), plus the `ClientConnection` message-handler invocations.
- **Channels**: outbound `mpsc::channel(CLIENT_CHANNEL_CAPACITY = 16 * 1024)` of
  `ClientUpdate`; inbound `mpsc::channel(incoming_queue_length = 16384)`; three
  **unbounded** channels (unordered control messages, encode input, encoded frames).
- **Backpressure policy is disconnect, not blocking**: if the outbound channel hits 16,384
  backed-up messages, `ClientConnectionSender::send` **aborts the client**
  ("exceeded channel capacity … kicking", `outgoing_queue_disconnects` metric). If the
  inbound queue overflows, the server initiates a close with reason "too many requests".
  Subscription fan-out must never block on a slow consumer.
- **Buffers**: `SerializeBufferPool` (16 pairs of uncompressed/compressed `BytesMut`,
  reclaimed via `Bytes::is_unique()` once the socket has flushed), plus the shared
  `BsatnRowListBuilderPool`.
- **Liveness**: server ping every 15 s (skipped if the last one wasn't ponged), idle timeout
  30 s (reset on any receive, implemented as a `watch`-fed resettable sleep), close-handshake
  drain timeout 250 ms, plus ~15 lines of commentary on RFC 6455 close-handshake corner cases
  and manual frame-level writes (`FRAME_BATCH_SIZE = 8` frames ≈ 32 KiB between control-message
  checks so pings can interleave with a large message on a slow link).
- **Metrics per connection**: queue-length gauges, request sizes, payload row counts,
  idle-timeout and closed-connection counters (`WORKER_METRICS`).

Roughly a third of `subscribe.rs`'s 2,682 lines exist to manage WebSocket-specific concerns:
subprotocol negotiation, ping/pong, the close handshake, frame splitting, and draining rules.
Fluxum's Streamable HTTP drops all of that (keep-alive = zero-length frame; teardown = end the
response body / expire the session) but must build what WS gave them for free: the **session
registry** (RPC-007) mapping `Fluxum-Session` → identity + subscriptions + outbound queue,
survival of the outbound queue across `GET /rpc` reconnects, and expiry sweeping. The channel
sizing, kick-don't-block policy, encode-off-the-writer-task split, and buffer pooling all
transfer unchanged.

## 8. The `pg` crate: a Postgres wire front-end for one-off queries

`crates/pg` (~700 lines) is a **pgwire**-based server exposing SpacetimeDB to `psql` and any
Postgres-compatible tool:

- **Subset**: startup + cleartext-password auth + **simple query protocol only**. No extended
  protocol (prepared statements are a `TODO`), no TLS (`tls_acceptor: None`), no user system
  (`database` startup param = SpacetimeDB database name; the **password field carries the
  JWT token**, validated by the same `validate_token` as HTTP).
- **Implementation**: each query is routed through the *same* code path as `POST /sql`
  (`database::sql_direct` with `confirmed: Some(true)`), then `SqlStmtResult<ProductValue>`
  rows are re-encoded to Postgres `DataRow`s via a `PsqlFormatter`/`TypedSerializer` over the
  SATS `ProductType` schema (`pg/src/encoder.rs`); DML returns a Postgres command tag with
  rows inserted/deleted/updated and server timing.

What it implies: one-off query ergonomics matter enough that they built an entire foreign
wire protocol rather than asking users to `curl` JSON — the payoff is the whole
psql/DataGrip/BI ecosystem for free, at the cost of a permanent second protocol surface
(auth duplication, type-mapping table, feature-subset support matrix). For Fluxum, the
cheaper 80% is making `POST /query` (RPC-050) excellent — schema-typed responses like
`SqlStmtResult` (schema + rows + stats + duration), not bare JSON rows — and treating a
pgwire adapter as a later, self-contained crate exactly as they did (it depends only on the
public SQL entry point, nothing internal).

---

## What Fluxum will face

1. **FluxBIN is validated — steal the negative tests, not the format.** BSATN and FluxBIN are
   byte-identical in philosophy (LE, `u32` length/count prefixes, `u8` sum tags, header-less
   structs, bool 0/1). Keep FluxBIN's Option convention (`0x00 = None`), but copy BSATN's
   strict-decode discipline: reject bools ∉ {0,1}, unknown enum tags, and truncated buffers
   with typed errors, and proptest both round-trip *and* rejection (`sats/src/bsatn.rs` tests).

2. **Replace `inserts: Vec<Vec<u8>>` with a flattened row list.** `BsatnRowList`
   (`size_hint: FixedSize(u16) | RowOffsets`, `rows_data: Bytes`) is the highest-leverage
   protocol structure in the codebase: zero per-row overhead for fixed-size rows, one
   allocation per table update, zero-copy slicing, parallel-decode-friendly. It composes with
   Fluxum's `static size` knowledge from the schema exactly as their `StaticLayout` /
   `static_bsatn_size()` does. This is a wire-format change — it must land before the G5
   freeze.

3. **Binary-only is the proven endpoint — don't backslide.** SpacetimeDB shipped
   JSON+BSATN, let `FormatSwitch` generics infect ten crates and force double encodes at
   fan-out, then deleted JSON in v2. Fluxum's JSON surface should remain confined to the
   HTTP admin envelope (RPC-050), never entering `fluxum-protocol`.

4. **Decide the enriched-`TxUpdate` bandwidth question consciously.** Fluxum's RPC-033
   broadcasts `reducer_name`, `caller`, `duration_us` to every subscriber per commit — that is
   v1 `TransactionUpdate`, which SpacetimeDB explicitly regretted (naming-pressure NOTE, the
   "lying" energy field) and stripped in v2 to rows-only, moving metadata to the caller-only
   response. Options: keep enrichment but adopt their `light` per-session flag (v1's
   `TransactionUpdateLight` ≙ rows + request_id) so constrained clients can opt out, and/or
   intern reducer names (id ↔ name map at subscribe time). Also consider v2's `OkEmpty`-style
   empty-result optimization and typed (FluxBIN-encoded) reducer error values instead of
   `Result<(), String>` in RPC-031.

5. **Group updates by subscription in the wire format.** v2 wraps table updates in
   `QuerySetUpdate{query_set_id, tables}` so clients know *which* subscription matched.
   Fluxum's `TableUpdate.query_id` covers this, but verify the semantics when one commit
   matches multiple queries over the same table (SpacetimeDB allows repeated `TableUpdate`s
   per table and documents duplicate row delivery for overlapping queries).

6. **Fan-out: share encoding, never compression; compress per-session off the hot path.**
   Their memoized once-per-format row encode + `Bytes` refcount clone per subscriber, with
   per-client whole-payload compression in the send task, is the settled answer after trying
   the alternative. If FluxRPC adds compression (currently absent from SPEC-006 — at 1k
   subscribers × large `InitialData`, brotli-1's 7–10× is hard to ignore), use: negotiated
   per-session algorithm, ≥1 KiB threshold, one tag byte per frame, brotli quality 1.

7. **Per-session infra: budget it now.** Expect per client: bounded outbound queue
   (~16k messages) with **kick-on-overflow** (never block the fan-out), bounded inbound queue
   with close-on-overflow, a dedicated encode task so serialization+compression never stalls
   the socket writer, pooled serialize buffers reclaimed by `Bytes` uniqueness, coalescing up
   to ~512 KiB per HTTP chunk with rayon offload for large payloads, and queue-length /
   disconnect-reason metrics from day one. Fluxum's framing already gives v3-style coalescing
   for free — batch many frames per chunked-body write.

8. **Streamable HTTP trades WS ceremony for session-registry work.** ~900 lines of
   `subscribe.rs` (subprotocol negotiation, ping/pong, RFC 6455 close handshake, frame
   interleaving) disappear; in exchange Fluxum must own: session ↔ outbound-queue binding
   across `GET /rpc` reconnects (their WS gets a fresh subscription state per socket —
   Fluxum sessions outlive transport connections, closer to their `confirmed`/durability
   plumbing than to their connection model), idle expiry sweeping, and the single-active-push-
   stream rule (RPC-006). Their module-hotswap watcher (swap module on live connections,
   close with "module exited" on delete) is a lifecycle Fluxum's session actor also needs.

9. **Auth in-band and out.** Copy their pragmatism: token in header *or* query param, and the
   identity/token echoed in-band as the first frame (Fluxum's `AuthResult` already is).
   Consider their short-lived `websocket-token` pattern (pre-authenticate over HTTPS, connect
   with a 60 s single-use token) so long-lived tokens never appear in `/rpc` URLs or logs.

10. **One-off queries deserve typed results; pgwire can wait.** Return
    `{schema, rows, stats, duration}` from both `OneOffQuery` (RPC-025) and `POST /query`,
    like `SqlStmtResult`. A future `fluxum-pg` crate is viable precisely because their `pg`
    crate is a thin adapter over the public SQL entry point — keep Fluxum's SQL execution
    reachable behind one function (`sql_direct`-equivalent) so a wire adapter never needs
    internal APIs.
