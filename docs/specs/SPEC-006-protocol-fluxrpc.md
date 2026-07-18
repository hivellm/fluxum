# SPEC-006 — FluxRPC Protocol

| | |
|---|---|
| **Status** | Draft — frozen at gate G5 (wire freeze, [DAG](../DAG.md)) |
| **Phase / tasks** | Phase 1 · T1.2 + Phase 5 · T5.1–T5.3 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-40..FR-46, FR-91; FR-88 (P2, HTTP/3/WebTransport path) |
| **Requirement prefix** | `RPC-` |
| **Source** | UzDB spec 07, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `RPC-xxx`. Integers little-endian unless stated otherwise. Message and codec
types live in `fluxum-protocol` (pure wire layer, no storage deps); transports live in
`fluxum-server`.

---

## 1. Overview

FluxRPC is the native binary protocol of Fluxum. It is derived from the **SynapRPC** protocol
(implemented in `hivellm/synap`) and uses the **HiveLLM wire-framing standard** —
`u32 LE length prefix + MessagePack body` — shared with SynapRPC (Synap), VectorizerRPC
(Vectorizer), and the Nexus binary RPC. The commit log (SPEC-002) reuses the same framing, so the
stack has one codec, one decoder, one debugging tool.

FluxRPC runs over two transports; the HTTP server additionally exposes a JSON admin surface:

- **TCP** (:15801) — primary transport for trusted backend services (privileged server peers,
  often same host / loopback)
- **Streamable HTTP** (:15800, path `/rpc`) — browser and mobile clients (TypeScript SDK); the
  same binary frames carried in HTTP request/response bodies (§2), no WebSocket involved
- **HTTP/JSON** (:15800) — admin tooling only, separate envelope (not FluxRPC binary)

### Two-layer encoding model

FluxRPC uses different encodings at two layers:

| Layer | Encoding | Why |
|-------|----------|-----|
| Message envelope | **MessagePack** (`rmp-serde`) | Flexible, debuggable, mature tooling, handles tagged variants |
| Row data in `TableUpdate.inserts`/`deletes` | **FluxBIN** | Schema-driven, no field names/tags → ~40% smaller than MessagePack for typed rows |

The envelope uses MessagePack tagged variants for message types. Row data inside
`TableUpdate.inserts`/`deletes` uses FluxBIN (see §6). FluxBIN is a BSATN-equivalent encoding:
at 100k tx/s × 1,000 subscribers, row-encoding overhead compounds — FluxBIN is mandatory for the
hot path.

---

## 2. Wire framing and transports

- **RPC-001** [P0] **Frame format.** Every FluxRPC message in both directions SHALL use the
  following binary frame:

  ```
  ┌───────────────────┬────────────────────────────────────────────┐
  │ length: u32 (LE)  │ body: MessagePack bytes (message envelope) │
  └───────────────────┴────────────────────────────────────────────┘
       4 bytes                    `length` bytes
  ```

  - `length` is a 32-bit unsigned integer in little-endian byte order.
  - `length` counts the body bytes only (excludes the 4-byte length field).
  - `body` is a MessagePack-encoded message object (envelope only).
  - Row data embedded in `TableUpdate` uses FluxBIN encoding (see §6).
  - Both transports — TCP and Streamable HTTP — use identical framing.
  - A frame with `length = 0` (no body) is a **keep-alive frame**: receivers SHALL ignore it. It
    is used by the Streamable HTTP push stream (RPC-006) and MAY be used on TCP.

  This framing is the HiveLLM family wire standard (SPEC-001, `thunder`), not a Fluxum invention.
  Implementations SHALL delegate the length prefix, the `max_frame_bytes` check (RPC-061) and the
  body slicing to the family wire layer wherever it exposes them, rather than re-implementing
  them; the bytes above are frozen and any divergence is a bug in the implementation, not a
  Fluxum dialect. Fluxum owns only what sits *above* the frame boundary: the `[tag, payload]`
  envelope catalog (§3), RowList slicing, and FluxBIN (§6).

  Current state: the TypeScript SDK delegates to `@hivehub/thunder`'s `FrameReader`. The Rust
  `fluxum-protocol` still carries its own codec because `thunder::wire` decodes a frame only by
  deserializing the body into its own message types and has no borrowed-body variant, which
  Fluxum's zero-copy sans-IO decoding requires (tracked upstream as hivellm/thunder#6). That
  codec is pinned byte-for-byte against Thunder's encoder by
  `crates/fluxum-protocol/tests/thunder_parity.rs`, so the two cannot drift while the gap lasts.
  The zero-length keep-alive is the one place Fluxum currently extends the family layer — the
  same issue asks for it to be defined in SPEC-001 instead.

- **RPC-002** [P0] **Multiplexing.** Every message SHALL carry an `id: u32` field chosen by the
  sender. Server responses SHALL echo the `id` of the corresponding request. Multiple in-flight
  requests on a single connection SHALL be supported; responses MAY arrive out of order. (This is
  what lets a client pipeline many calls on one connection instead of opening one connection per
  request.)

- **RPC-003** [P0] **Transports and ports.** The server SHALL expose FluxRPC over TCP on
  `tcp_port` (default **15801**) and over Streamable HTTP at path `/rpc` on `http_port` (default
  **15800**), carrying identical messages and identical framing on both. The HTTP/JSON admin API
  (§7) SHALL be served on the same `http_port`. Ports are configured in `config.yml` under
  `server:` and MAY be overridden via `FLUXUM_`-prefixed environment variables.

  | Port | Default | Carries |
  |------|---------|---------|
  | `http_port` | 15800 | HTTP/JSON admin API (§7) + FluxRPC Streamable HTTP at `/rpc` (RPC-004..RPC-007) |
  | `tcp_port` | 15801 | FluxRPC over raw TCP |

- **RPC-004** [P0] **Streamable HTTP transport.** The server SHALL expose FluxRPC over
  Streamable HTTP at path `/rpc` on `http_port`, via two methods: `POST /rpc` (client-initiated
  requests with streamed responses, RPC-005) and `GET /rpc` (server-initiated push stream,
  RPC-006). The design follows the MCP Streamable HTTP pattern already shipped by the HiveLLM
  family (rmcp in Synap/Vectorizer), but is **binary end-to-end**:

  - Request and response bodies SHALL use `Content-Type: application/x-fluxum` and SHALL carry
    one or more standard FluxRPC frames (RPC-001: `u32 LE length + MessagePack body`),
    concatenated back-to-back.
  - No SSE, no base64, no JSON on this path. Browsers consume response bodies incrementally via
    the fetch `ReadableStream` API; no WebSocket is involved.
  - The message layer is identical to TCP: same message types (§4/§5), same `FluxValue` (§3),
    same FluxBIN row encoding (§6), same multiplexing semantics (RPC-002), same
    `max_frame_bytes` limit (RPC-061, default 16 MB), same error codes (RPC-034). Only the
    carrier differs.
  - The server SHALL reject `/rpc` requests whose `Content-Type` is not `application/x-fluxum`
    with HTTP status 415.

  Rationale: Streamable HTTP traverses standard HTTP infrastructure (reverse proxies, load
  balancers, corporate middleboxes) that routinely interferes with WebSocket upgrades, gains
  HTTP/2 multiplexing over a single connection for free, and provides a natural upgrade path to
  HTTP/3 and WebTransport (PRD FR-88, P2).

- **RPC-005** [P0] **POST /rpc.** The request body SHALL contain one or more client → server
  frames (`Authenticate`, `ReducerCall`, `Subscribe`, `SubscribeSingle`, `Unsubscribe`,
  `OneOffQuery`). The server SHALL respond with a streamed body (chunked transfer encoding or
  HTTP/2 data stream) containing exactly the response frames for those requests, correlated by
  `id` (RPC-002) and written as each request completes — delivery out of order by `id` is
  allowed, exactly as on TCP. The response body ends when every request in the POST body has
  been answered.

- **RPC-006** [P0] **GET /rpc push stream.** `GET /rpc` SHALL open a long-lived binary response
  stream (chunked transfer encoding or HTTP/2 data stream) carrying server-initiated frames —
  `InitialData`, `TxUpdate`, and `Error` — for the session identified by the `Fluxum-Session`
  request header (RPC-007), using the same framing (RPC-001) as every other carrier. The server
  SHALL write a keep-alive frame (`length = 0`, RPC-001) every `http_keepalive_s` (default:
  15 s, configurable) of stream inactivity so intermediaries do not reap the connection. The
  stream is terminated by the idle timeout (RPC-060). At most one `GET /rpc` stream SHALL be
  active per session; opening a new one SHALL close the previous one.

- **RPC-007** [P0] **Session binding.** The HTTP response carrying the `AuthResult` frame for the
  first successful `Authenticate` on `POST /rpc` SHALL include a `Fluxum-Session` response header
  containing an opaque session id. The client SHALL echo this id in a `Fluxum-Session` request
  header on every subsequent `POST /rpc` and on the `GET /rpc` stream. Server-side, the session
  holds the authenticated identity and the set of registered subscriptions. Sessions expire on
  idle timeout (RPC-060). Requests bearing an unknown or expired session id SHALL receive HTTP
  status 404; the client then re-authenticates and re-subscribes per RPC-062 (the SDK does this
  automatically). Non-`Authenticate` frames on a POST without a valid session SHALL receive
  `Error { code: 401, message: "unauthenticated" }` (RPC-020).

- **RPC-008** [P1] **Server→client compression negotiation.** The server SHALL support
  per-connection compression of server→client frames, negotiated at connection setup with one of
  three algorithms: `none` (default) | `gzip` | `brotli`. The client selects the algorithm via
  the `compression` field of `Authenticate` (RPC-020) or, for Streamable HTTP, the
  `?compression=` query parameter on `/rpc` (the `Authenticate` field wins if both are present).
  Rules: *(adopted from SpacetimeDB analysis, file 06)*

  - When any algorithm other than `none` is negotiated, every server→client frame body SHALL
    begin with a 1-byte compression tag (`0x00` = uncompressed, `0x01` = gzip, `0x02` = brotli)
    followed by the (possibly compressed) MessagePack envelope; the `u32 LE` length prefix
    (RPC-001) counts tag + payload. The tag is written first into the uncompressed buffer so
    that skipping compression requires no byte-shifting. When `none` is negotiated (or nothing
    was negotiated), no tag byte is present and framing is exactly RPC-001.
  - Compression SHALL be applied only to payloads whose uncompressed body size is at or above
    `compression_threshold_bytes` (default: 1,024, configurable) — in practice large `TxUpdate`
    and `InitialData` payloads; smaller frames are sent with tag `0x00`. Recommended encoder
    setting: brotli quality 1 (measured 7–10× on large subscription updates).
  - Client→server messages SHALL always be uncompressed.
  - Compression SHALL execute in the per-connection send path, off the commit path, and SHALL
    never be shared across subscribers (SPEC-005 SUB-024: share the row encoding, never the
    compression).

---

## 3. Value type: FluxValue

- **RPC-010** [P0] **FluxValue variants.** All reducer arguments and return values SHALL be
  encoded as `FluxValue`:

  ```rust
  pub enum FluxValue {
      Null,
      Bool(bool),
      I64(i64),
      F64(f64),
      Bytes(Vec<u8>),
      Str(String),
      Array(Vec<FluxValue>),
      Map(Vec<(FluxValue, FluxValue)>),
      Identity([u8; 32]),   // stable 256-bit client identity
      EntityId(u64),        // row/entity primary key
      Timestamp(i64),       // microseconds since Unix epoch
  }
  ```

- **RPC-011** [P0] **MessagePack encoding of FluxValue.** `FluxValue` SHALL be encoded using
  MessagePack tagged variants (the same pattern as SynapValue). The tag SHALL be the first field
  in a 2-element array:

  | FluxValue variant | MessagePack encoding |
  |-------------------|----------------------|
  | `Null` | `nil` |
  | `Bool(b)` | `bool` |
  | `I64(n)` | `int` (compact) |
  | `F64(f)` | `float 64` |
  | `Bytes(b)` | `bin` (length-prefixed) |
  | `Str(s)` | `str` |
  | `Array(v)` | `array` of encoded FluxValues |
  | `Map(kv)` | `array` of `[key, value]` pairs |
  | `Identity(b)` | `fixarray[2]` of `["Identity", bin32]` |
  | `EntityId(n)` | `fixarray[2]` of `["EntityId", uint64]` |
  | `Timestamp(t)` | `fixarray[2]` of `["Timestamp", int64]` |

- **RPC-012** [P0] **No vector/geometry variants.** Dedicated vector types (2-D/3-D tuples,
  geometry objects) SHALL NOT be part of `FluxValue`. Persistent spatial coordinates (e.g.
  `Sensor.x` / `Sensor.y`) SHALL be stored as raw `f32`/`f64` columns in the table schema, where
  the geospatial indexes (SPEC-008) consume them.

---

## 4. Client → Server messages

- **RPC-020** [P0] **Authenticate.**

  ```rust
  pub struct Authenticate {
      pub id: u32,
      pub token: Vec<u8>,              // opaque auth token (JWT, API key, or custom token)
      pub compression: Option<String>, // "none" | "gzip" | "brotli" — RPC-008; default "none"
      pub tx_updates: Option<String>,  // "full" | "light" — RPC-035; default "full"
  }
  ```

  The `compression` and `tx_updates` fields set the per-connection options of RPC-008 and
  RPC-035; `None` means the default. An unrecognized value SHALL be rejected with
  `Error { code: 400 }`. *(amended per SpacetimeDB analysis, file 06)*

  The server SHALL respond with `AuthResult`. The client MUST authenticate before sending any
  other message. Unauthenticated messages (except `Authenticate`) SHALL receive
  `Error { code: 401, message: "unauthenticated" }`. Identity derivation and token validation are
  specified in SPEC-009.

- **RPC-021** [P0] **ReducerCall.**

  ```rust
  pub struct ReducerCall {
      pub id: u32,
      pub reducer: String,          // reducer function name, e.g. "send_chat"
      pub version: Option<u32>,     // reducer version (None for latest)
      pub args: Vec<FluxValue>,     // positional arguments (after ReducerContext)
  }
  ```

  The server SHALL execute the named reducer atomically (SPEC-004) and respond with
  `ReducerResult`.

- **RPC-022** [P0] **Subscribe.**

  ```rust
  pub struct Subscribe {
      pub id: u32,
      pub queries: Vec<String>,   // batch: one or more SQL query strings (SPEC-005 SQL subset)
  }
  ```

  The server SHALL respond with `InitialData` (one `TableUpdate` per query) and register all
  queries for ongoing `TxUpdate` delivery. Each query is assigned a server-generated `query_id`
  returned in `InitialData.tables[n].query_id`.

- **RPC-023** [P1] **SubscribeSingle.**

  ```rust
  pub struct SubscribeSingle {
      pub id: u32,
      pub query: String,   // exactly one SQL query string
  }
  ```

  The server SHALL respond with `InitialData` for the single query and register it for `TxUpdate`
  delivery. The assigned `query_id` appears in `InitialData.tables[0].query_id`.

  Use `SubscribeSingle` when adding an individual subscription without re-sending the full batch.
  Functionally equivalent to `Subscribe { queries: vec![query] }`.

- **RPC-024** [P0] **Unsubscribe.**

  ```rust
  pub struct Unsubscribe {
      pub id: u32,
      pub query_ids: Vec<u32>,   // server-assigned query IDs from InitialData.tables[n].query_id
  }
  ```

  Works for query IDs from both `Subscribe` (batch) and `SubscribeSingle`.

- **RPC-025** [P1] **OneOffQuery.**

  ```rust
  pub struct OneOffQuery {
      pub id: u32,
      pub sql: String,   // read-only SQL query
  }
  ```

  The server SHALL execute the query against `CommittedState` (SPEC-002) and return `InitialData`
  with matching rows. No subscription is registered.

---

## 5. Server → Client messages

- **RPC-030** [P0] **AuthResult.**

  ```rust
  pub struct AuthResult {
      pub id: u32,              // echoes Authenticate.id
      pub identity: [u8; 32],   // derived 256-bit identity (SPEC-009)
      pub token: Vec<u8>,       // refreshed/rotated token (MAY be same as input)
  }
  ```

- **RPC-031** [P0] **ReducerResult.**

  ```rust
  pub struct ReducerResult {
      pub id: u32,                       // echoes ReducerCall.id
      pub outcome: Result<(), String>,   // MessagePack-encoded as ["Ok", null] or ["Err", "message"]
  }
  ```

- **RPC-032** [P0] **InitialData / TableUpdate.**

  ```rust
  pub struct InitialData {
      pub id: u32,                    // echoes Subscribe.id, SubscribeSingle.id, or OneOffQuery.id
      pub schema_version: u32,        // server's current schema version
      pub tables: Vec<TableUpdate>,
  }

  pub struct TableUpdate {
      pub table_id: u32,
      pub table_name: String,
      pub query_id: u32,              // server-assigned ID for this subscription query
      pub inserts: RowList,           // FluxBIN-encoded rows, flat (see below and §6)
      pub deletes: RowList,           // rows_data holds FluxBIN PK field(s) only (RPC-042)
  }

  /// Flat row list: one contiguous buffer + out-of-band boundaries — NOT Vec<Vec<u8>>.
  pub struct RowList {
      pub row_count: u32,
      pub size_hint: RowSizeHint,     // how to slice rows_data into rows
      pub rows_data: Vec<u8>,         // ALL rows' FluxBIN bytes, back-to-back (one bin field)
  }

  pub enum RowSizeHint {
      /// Every row encodes to exactly n bytes: row i = rows_data[i*n .. (i+1)*n].
      /// Zero per-row overhead, O(1) random access. row_count MUST equal len/n.
      Fixed(u16),
      /// Variable-size rows: start offset of each row into rows_data; a row's end is the
      /// next row's start (or the end of rows_data for the last row).
      Offsets(Vec<u64>),
  }
  ```

  Row batches SHALL use this flat layout in both `InitialData` and `TxUpdate`: one allocation
  per table update instead of one per row, no per-row MessagePack `bin` header, zero-copy
  slicing on the server (refcounted buffer, SPEC-005 SUB-024) and on the client (one
  `Uint8Array` subarray per row in browsers), and parallel-decode-friendly. Encoders SHALL emit
  `Fixed(n)` whenever the table's schema yields a statically known row size, MAY start
  optimistically from the first row's actual size otherwise, and SHALL degrade to `Offsets` on
  the first size mismatch (retroactively synthesizing the offset table). Decoders SHALL reject a
  `RowList` whose `row_count`, `size_hint`, and `rows_data` length are mutually inconsistent
  with a 400 error (RPC-034). *(adopted from SpacetimeDB analysis, file 06 — their
  `BsatnRowList` lesson; pre-G5-freeze wire change)*

  Clients SHALL use `TableUpdate.query_id` to correlate subscriptions with `Unsubscribe`
  messages.

- **RPC-033** [P0] **TxUpdate.** `TxUpdate` is server-initiated (no `id` field). It carries full
  commit context so clients can drive UI updates, notifications, and client-side event routing
  without issuing follow-up queries.

  ```rust
  pub struct TxUpdate {
      pub tx_id: u64,               // monotonically increasing per shard
      pub timestamp: i64,           // microseconds since Unix epoch (reducer commit time)
      pub reducer_name: String,     // name of the reducer that caused this commit;
                                    // "" for system-initiated commits (#[fluxum::on_init],
                                    // #[fluxum::tick], scheduled reducers, etc.)
      pub caller: [u8; 32],         // Identity of the calling client (32 zero bytes for system)
      pub duration_us: u32,         // reducer execution time in microseconds
      pub tables: Vec<TableUpdate>,
  }
  ```

  `TxUpdate` is NOT correlated to any client request. Clients SHALL apply these as incremental
  diffs to their local cache, using `tx_id` to detect missed updates. Enrichment rationale:
  `caller` lets clients attribute changes ("Alice edited this document"); `reducer_name` drives
  client-side event routing; `timestamp` orders events; `duration_us` enables client-side
  profiling.

- **RPC-034** [P0] **Error** *(amended by [SPEC-028](SPEC-028-error-catalog.md): the
  HTTP-compatible code table is replaced by the stable machine-readable catalog)*.

  ```rust
  pub struct Error {
      pub id: Option<u32>,        // echoes request id if applicable; None for server-initiated
      pub code: u16,              // stable SPEC-028 catalog code (per-subsystem ranges)
      pub name: String,           // stable SCREAMING_SNAKE catalog name
      pub message: String,        // human-readable; never the machine interface
      pub severity: Severity,     // error | fatal (fatal = connection closes after this frame)
      pub retryable: bool,        // SPEC-028 §4 retry semantics
      pub retry_after_ms: Option<u32>,
      pub sqlstate: Option<String>, // SQL-range codes only (PostgreSQL-compatible)
      pub details: Vec<(String, FluxValue)>, // keys documented per code in the catalog
  }
  ```

  Codes this spec's own conditions emit (the full registry lives in
  `fluxum-protocol/src/codes.rs`; the generated reference is `docs/errors.md`):

  | Code | Name | Mandated by |
  |------|------|-------------|
  | 1000 | `PROTO_MALFORMED` | RPC-001 |
  | 1003 | `PROTO_FRAME_TOO_LARGE` | RPC-061 |
  | 1004 | `PROTO_IDLE_TIMEOUT` | RPC-060 |
  | 1005 | `PROTO_SESSION_EXPIRED` | RPC-007 |
  | 2000 | `AUTH_REQUIRED` | RPC-020 |
  | 5005 | `REDUCER_RATE_LIMITED` | RPC-021 (SPEC-004 RED-050) |
  | 6000 | `SUB_LIMIT_EXCEEDED` | SPEC-005 SUB-044 |
  | 8000 | `CLUSTER_SHARD_UNAVAILABLE` | SPEC-007 / TXN-011 |

  The Streamable HTTP transport derives its response status from the entry's `http_status`
  (SPEC-028 §7), preserving the previous externally observable semantics.

- **RPC-035** [P1] **TxUpdate metadata mode (`tx_updates: full | light`).** The default remains
  the enriched `TxUpdate` of RPC-033 (FR-43). A connection MAY opt out via the per-connection
  option `tx_updates: light`, set in `Authenticate` (RPC-020) or, for Streamable HTTP, via the
  `?tx_updates=` query parameter on `/rpc`. Under `light`, the server SHALL send `TxUpdateLight`
  instead of `TxUpdate`, omitting `reducer_name`, `caller`, and `duration_us` for
  bandwidth-critical clients:

  ```rust
  pub struct TxUpdateLight {
      pub tx_id: u64,               // as RPC-033
      pub timestamp: i64,           // as RPC-033
      pub tables: Vec<TableUpdate>,
  }
  ```

  `TxUpdateLight` carries identical row-diff semantics (SPEC-005 SUB-003) and the same `tx_id`
  gap-detection contract (RPC-062). The mode affects only broadcast updates: the caller of a
  reducer always receives its `ReducerResult` (RPC-031) regardless of mode. Lesson: SpacetimeDB
  broadcast v1-style enriched updates, regretted the per-commit metadata fan-out cost (bandwidth
  pressure even pushed users toward one-letter reducer names), and stripped metadata entirely in
  its v2 protocol; Fluxum keeps enrichment as the default but makes it opt-out per connection
  instead. *(adopted from SpacetimeDB analysis, file 06)*

---

## 6. FluxBIN row encoding

- **RPC-040** [P0] **FluxBIN encoding rules.** Row data in `TableUpdate.inserts` and
  `TableUpdate.deletes` SHALL use **FluxBIN** — a schema-driven binary encoding equivalent to
  BSATN. No field names. No per-value type tags. The schema (known to both sides) provides all
  type context. The codec is hand-rolled in `fluxum-protocol` (no serde on this path).

  ```
  FluxBIN encoding:
    bool          → 1 byte: 0x00 false | 0x01 true
    u8 / i8       → 1 byte
    u16 / i16     → 2 bytes little-endian
    u32 / i32     → 4 bytes little-endian
    u64 / i64     → 8 bytes little-endian
    f32           → 4 bytes IEEE 754 little-endian
    f64           → 8 bytes IEEE 754 little-endian
    String        → u32 LE length + UTF-8 bytes
    Vec<u8>       → u32 LE length + raw bytes
    Vec<T>        → u32 LE count + N × encode(T)
    Option<T>     → 0x00 (None) | 0x01 + encode(T)
    Identity      → 32 bytes raw (no prefix)
    ConnectionId  → 16 bytes raw (the u128 in little-endian, no prefix)
    EntityId      → 8 bytes LE (u64 newtype)
    Timestamp     → 8 bytes LE (i64 µs since Unix epoch)
    struct        → fields encoded in declaration order, no separators, no names
    enum          → u8 tag + encode(variant payload)
  ```

- **RPC-041** [P0] **Insert row encoding.** Each insert row SHALL be encoded as sequential
  FluxBIN field values in column declaration order; the rows of a table update are concatenated
  back-to-back into `RowList.rows_data` (RPC-032), which travels as a single `bin` field of the
  MessagePack envelope — there is no per-row length header on the wire *(amended per SpacetimeDB
  analysis, file 06)*:

  ```rust
  // #[fluxum::table(public, primary_key(grid_x, grid_y))]
  // #[spatial(quadtree(x, y))]
  // pub struct Sensor { grid_x: i32, grid_y: i32, x: f32, y: f32, reading: f64, updated_at: Timestamp }
  //
  // Insert: Sensor { grid_x: 5, grid_y: 3, x: 12.5, y: 8.0, reading: 21.7, updated_at: t }
  //
  // FluxBIN bytes: [5 i32 LE][3 i32 LE][12.5 f32 LE][8.0 f32 LE][21.7 f64 LE][t i64 LE]
  //                 4 bytes   4 bytes   4 bytes      4 bytes     8 bytes      8 bytes  = 32 bytes total
  //
  // vs a self-describing MessagePack map {grid_x: 5, grid_y: 3, ...} ≈ 68 bytes
  // (field names + per-value type tags overhead)
  ```

- **RPC-042** [P0] **Delete row encoding.** Delete entries carry only the primary key field(s) in
  FluxBIN:

  ```rust
  // Single PK: Task deleted, pk = 42
  // FluxBIN bytes: [42 u64 LE]  = 8 bytes

  // Composite PK: Sensor deleted, grid_x = 5, grid_y = 3
  // FluxBIN bytes: [5 i32 LE][3 i32 LE]  = 8 bytes
  ```

- **RPC-043** [P0] **Schema version validation.** `InitialData.schema_version` SHALL be verified
  by the client SDK against the expected schema version embedded in the generated SDK code
  (SPEC-011). A mismatch triggers automatic schema refresh and SDK reconnect before delivering
  `InitialData` to application code.

---

## 7. HTTP/JSON admin transport

- **RPC-050** [P0] **HTTP endpoints.** The server SHALL expose the following HTTP endpoints on
  the configured `http_port` (default 15800), served by axum:

  | Method | Path | Description |
  |--------|------|-------------|
  | `GET` | `/health` | Server and shard status: `{"status": "ok", "shards": N}` |
  | `GET` | `/metrics` | Prometheus text format (`fluxum_*` metrics, SPEC-012) |
  | `GET` | `/schema` | Full schema as JSON — tables, reducers, types (SPEC-011) |
  | `POST` | `/reducer/:name` | Call a reducer (JSON body, JSON response) |
  | `POST` | `/query` | One-off read-only SQL query |
  | `GET` | `/view/:name` | Call a `#[fluxum::view]` function |
  | `POST` | `/procedure/:name` | Call a `#[fluxum::procedure]` function (admin) [P2] |

  Paths are unversioned — there is no `/v1` prefix. The `/rpc` path on the same port belongs to
  the binary Streamable HTTP transport (RPC-004..RPC-007) and does NOT use the JSON envelopes
  below.

- **RPC-051** [P1] **HTTP request envelope.**

  ```json
  {
    "request_id": "uuid-v4",
    "payload": { ... }
  }
  ```

- **RPC-052** [P1] **HTTP response envelope.** Success:

  ```json
  {
    "success": true,
    "request_id": "uuid-v4",
    "payload": { ... }
  }
  ```

  Error:

  ```json
  {
    "success": false,
    "request_id": "uuid-v4",
    "error": "error message"
  }
  ```

- **RPC-053** [P0] **Health latency.** `GET /health` MUST respond in < 50 ms and MUST NOT take
  storage locks (FR-91). Normative metric and health-payload details live in SPEC-012.

---

## 8. Connection management

- **RPC-060** [P1] **Idle connection timeout.** Connections that send no messages for more than
  `idle_timeout_s` (default: 60 s, configurable) SHALL be closed by the server. The server SHALL
  send `Error { code: 408, message: "idle timeout" }` before closing. For Streamable HTTP, the
  same timeout governs session expiry (RPC-007): a session with no `POST /rpc` activity and no
  open `GET /rpc` stream for `idle_timeout_s` SHALL be discarded; if a `GET /rpc` stream is open
  when the session expires, the 408 `Error` frame SHALL be written to it before the stream is
  terminated. Keep-alive frames (RPC-006) do not count as client activity.

- **RPC-061** [P1] **Maximum frame size.** The server SHALL reject frames with
  `length > max_frame_bytes` (default: 16 MB, configurable). Oversized frames SHALL receive
  `Error { code: 413, message: "frame too large" }`.

- **RPC-062** [P1] **Reconnection and resync.** Clients that reconnect after a disconnect (TCP)
  or after session loss (Streamable HTTP, RPC-007) SHALL re-authenticate and re-subscribe; the
  SDKs do this automatically. The `InitialData` response on re-subscribe provides a fresh
  snapshot. Clients SHOULD use the `tx_id` from the last received `TxUpdate` to detect whether
  any updates were missed during the disconnect window.

- **RPC-063** [P2] **TLS.** The server SHALL support optional TLS on the TCP transport and HTTPS
  on the HTTP port (FR-46), enabled via configuration. The Streamable HTTP transport then runs
  over `https://`; framing and messages are unchanged under TLS.

- **RPC-064** [P0] **Bounded per-connection queues, kick on overflow.** Every per-connection (or
  per-session, for Streamable HTTP) outgoing queue SHALL be bounded
  (`outgoing_queue_frames` in `config.yml`, default: 16,384 frames) — an unbounded outgoing
  queue is prohibited. When an enqueue would exceed the bound, the server SHALL disconnect
  (kick) that client immediately — it SHALL NEVER block the subscription fan-out loop on a slow
  consumer — logging `WARN "subscriber dropped: outgoing queue overflow"` and incrementing
  `fluxum_subscriber_drops_total{shard, reason="queue_overflow"}` (SPEC-012). This is the
  transport-level enforcement of the SPEC-005 SUB-042 backpressure tiers: the byte-based
  three-tier policy operates within the bounded queue, and a frame-count overflow maps to the
  Full tier's drop behavior. Inbound queues SHALL likewise be bounded
  (`incoming_queue_frames`, default: 16,384); on inbound overflow the server SHALL send
  `Error { code: 429, message: "too many requests" }` and close the connection. Lesson:
  SpacetimeDB uses 16k bounded channels with disconnect-on-overflow on both directions —
  fan-out must never wait on one client. *(adopted from SpacetimeDB analysis, file 06)*

---

## Acceptance criteria

Exit tests for T1.2 (codec) and T5.1–T5.3 (transports); wire freeze at G5 requires all of these
green.

1. **Frame codec round-trip** — every message type in §4/§5 encodes to `u32 LE + MessagePack` and
   decodes back identically (proptest round-trip property over all message types, per DAG T1.2).
2. **FluxBIN golden vectors** — for every FluxBIN type in RPC-040 (all primitives, `String`,
   `Vec<u8>`, `Vec<T>`, `Option<T>`, `Identity`, `ConnectionId`, `EntityId`, `Timestamp`, struct,
   enum), fixed input → fixed expected bytes; the RPC-041 `Sensor` row encodes to exactly the
   32 bytes shown, and the RPC-042 delete examples to exactly 8 bytes each. **Flat row list**
   (RPC-032): three `Sensor` rows encode as `RowList { row_count: 3, size_hint: Fixed(32),
   rows_data: 96 bytes }` with zero per-row overhead; a batch with variable-size rows (e.g.
   `ChatMessage`) degrades to `Offsets` with correct start offsets; a `RowList` with
   inconsistent `row_count`/`size_hint`/`rows_data` length is rejected with a 400 error.
   *(adopted from SpacetimeDB analysis, file 06)*
3. **FluxBIN size advantage** — encoding the canonical `Sensor` and `ChatMessage` rows in FluxBIN
   is measurably smaller than the equivalent self-describing MessagePack map (target ~40% for
   typed rows).
4. **Multiplexing out-of-order** — a client pipelines N concurrent `ReducerCall`s with distinct
   `id`s on one TCP connection; every `ReducerResult`/`Error` echoes the correct `id`, with
   responses deliberately delivered out of order.
5. **Auth gate** — any non-`Authenticate` message on a fresh connection receives
   `Error { code: 401, message: "unauthenticated" }`.
6. **Transport equivalence** — the same byte-identical frames drive an identical
   auth → subscribe → reducer → `TxUpdate` session over TCP :15801 and Streamable HTTP
   `POST`/`GET /rpc` on :15800; a `/rpc` request without `Content-Type: application/x-fluxum` is
   rejected with HTTP 415.
7. **Browser fetch-stream integration** — from a browser environment, the TypeScript SDK
   authenticates via `POST /rpc` (binary `Authenticate` frame in, `AuthResult` frame plus
   `Fluxum-Session` header out), opens the `GET /rpc` stream, subscribes, and receives binary
   `TxUpdate` frames incrementally via fetch `ReadableStream` after a committed reducer call;
   zero-length keep-alive frames arrive at the configured `http_keepalive_s` interval and are
   ignored by the SDK. No SSE parsing, base64, or JSON appears anywhere on the path.
8. **Session expiry / reauth drill** — after `idle_timeout_s` with no activity, the session is
   discarded (408 `Error` frame on any open `GET /rpc` stream); a subsequent POST with the stale
   `Fluxum-Session` id receives HTTP 404; the SDK automatically re-authenticates, obtains a new
   session id, re-subscribes, receives a fresh `InitialData` snapshot, and detects any missed
   updates via the `tx_id` gap.
9. **TxUpdate enrichment** — after a committed `send_chat` call, subscribers receive a `TxUpdate`
   with correct `tx_id` (monotonic per shard), `timestamp`, `reducer_name = "send_chat"`,
   `caller` = the caller's Identity, plausible `duration_us`, and FluxBIN diffs in `tables`; a
   system commit (e.g. `#[fluxum::tick]`) yields `reducer_name = ""` and a 32-zero-byte `caller`.
10. **Subscription lifecycle** — `Subscribe` (batch) and `SubscribeSingle` both return
    `InitialData` with per-query `query_id`s; `Unsubscribe` with those IDs stops further
    `TxUpdate` delivery for exactly those queries; `OneOffQuery` returns rows without registering
    a subscription.
11. **Schema version mismatch** — a client SDK built against schema version N connecting to a
    server at N+1 refreshes the schema and reconnects before surfacing `InitialData`.
12. **Connection limits** — an idle connection is closed after `idle_timeout_s` with a prior 408
    `Error`; a frame with `length` > `max_frame_bytes` (default 16 MB) receives a 413 `Error` on
    both TCP and `/rpc`.
13. **HTTP admin (curl suite, per DAG T5.3)** — all RPC-050 endpoints respond with the RPC-051/052
    envelopes; `/health` returns `{"status": "ok", "shards": N}` in < 50 ms without taking
    storage locks; `/metrics` serves Prometheus text format.
14. **Reconnect resync (TCP)** — after a forced disconnect, a client re-authenticates,
    re-subscribes, receives a fresh `InitialData` snapshot, and detects a missed update via the
    `tx_id` gap.
15. **Proxy compatibility** — the full Streamable HTTP flow (POST authenticate, GET stream,
    streamed `TxUpdate` delivery, keep-alive messages) works unmodified through a standard HTTP reverse
    proxy (e.g. nginx/HAProxy) with response buffering disabled for `/rpc`; the required proxy
    configuration is documented as a deployment note.
16. **Compression negotiation** (RPC-008) *(adopted from SpacetimeDB analysis, file 06)* — a
    connection negotiating `brotli` (via `Authenticate` field and, separately, via
    `?compression=` on `/rpc`) receives large `TxUpdate`/`InitialData` frames tagged `0x02` and
    correctly decompressed by the SDK; frames below `compression_threshold_bytes` arrive tagged
    `0x00`; a `none` (default) connection sees no tag byte and byte-identical RPC-001 framing;
    client→server frames are never compressed; an unrecognized algorithm is rejected with a 400
    error.
17. **TxUpdate light mode** (RPC-035) *(adopted from SpacetimeDB analysis, file 06)* — with two
    subscribers to the same query, one `tx_updates: full` and one `tx_updates: light`, a
    committed reducer call delivers the enriched `TxUpdate` (criterion 9) to the first and a
    `TxUpdateLight` (same `tx_id`, `timestamp`, and row diffs; no `reducer_name`/`caller`/
    `duration_us`) to the second; the reducer's caller receives its `ReducerResult` in both
    modes.
18. **Queue overflow kick** (RPC-064) *(adopted from SpacetimeDB analysis, file 06)* — a
    subscriber whose outgoing queue is driven past `outgoing_queue_frames` is disconnected
    without any measurable stall of fan-out to other subscribers (cross-checked against the
    SPEC-005 acceptance criterion 6 stress test); the WARN line is logged and
    `fluxum_subscriber_drops_total{reason="queue_overflow"}` increments; flooding the inbound
    queue past `incoming_queue_frames` yields a 429 `Error` followed by connection close.
