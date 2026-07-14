# SPEC-011 — SDK Code Generation

| | |
|---|---|
| **Status** | Draft — `/schema` JSON freezes at T6.1 |
| **Phase / tasks** | Phase 6 · T6.1–T6.4 + Phase 7 · T7.4–T7.6 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-81, FR-82, FR-83, FR-84, FR-85, FR-86, FR-87, FR-88 |
| **Requirement prefix** | `SDK-` |
| **Source** | UzDB spec 12, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `SDK-xxx`. Defines schema introspection (`GET /schema`), the `fluxum generate`
and `fluxum schema export` CLI commands, the five-language SDK surface (TypeScript, Python, Go,
Rust, C#) plus the P2 C++ codegen target, and the Rust client SDK crate
(`fluxum-sdk`). Wire-level message and encoding references (`ReducerCall`,
`InitialData`, `TxUpdate`, FluxValue, FluxBIN) are normative in [SPEC-006](SPEC-006-protocol-fluxrpc.md);
subscription semantics in [SPEC-005](SPEC-005-subscriptions.md); schema versioning and migrations in
[SPEC-010](SPEC-010-schema-migration.md).

## 1. Overview

SDK code generation eliminates an entire class of client-side bugs by ensuring that a client
application's type definitions for tables, reducers, and FluxValue variants always match the
server's schema.

Fluxum generates type-safe client SDK bindings from the live schema exposed at `GET /schema`
(HTTP admin transport, port 15800). The `fluxum generate` CLI command is the entry point. The
demo application (chat + presence + per-user tasks, T6.5) consumes the generated TypeScript SDK
end-to-end and is the reference consumer for this spec.

The `/schema` JSON document is part of the **module API freeze** at T6.1: after the freeze,
changes to the document format MUST be additive.

### 1.1 Target-language matrix

Five SDKs are the minimum competitive surface. TypeScript and Rust ship with 0.1.0 (MVP, G6);
Python, Go, and C# ship with 0.2.0 ("competitive launch", G7). C++ is P2 and unscheduled.

| Target | Priority | Task | PRD | Notes |
|--------|----------|------|-----|-------|
| JavaScript/TypeScript | **P0** | T6.2 | FR-82 | Reference target; **browser-native** (binary FluxRPC over Streamable HTTP, SDK-080..084); powers the MVP demo app (T6.5) |
| Python | P1 | T7.4 | FR-83 | asyncio-first |
| Go | P1 | T7.5 | FR-85 | context-aware, idiomatic errors |
| Rust | P1 | T6.4 | FR-84 | `fluxum-sdk` crate; shares `fluxum-protocol` (§7) |
| C# | P1 | T7.6 | FR-86 | async/await, NuGet |
| C++ | P2 | — | FR-87 | Typed structs + reducer helpers only (SDK-063) |

Every P0/P1 SDK MUST pass the **same shared conformance corpus** (SPEC-013) in CI — see SDK-064.
The architecture for every language is identical: generated code (types, decoders, reducer
wrappers, cache/event accessors) layered on a thin hand-written transport/runtime package per
language (published names in SDK-071). Generated code MUST NOT reimplement protocol semantics.

## 2. Schema introspection

- **SDK-001** [P0] The server SHALL expose the full schema as a JSON document at `GET /schema`
  (HTTP admin, default `http://host:15800/schema`):

  ```json
  {
    "schema_version": 3,
    "tables": [
      {
        "id": 2847362341,
        "name": "Task",
        "public": true,
        "global": false,
        "partition_by": null,
        "columns": [
          { "name": "id",    "type": "U64",      "pk": true, "auto_inc": true },
          { "name": "owner", "type": "Identity", "pk": false },
          { "name": "title", "type": "Str",      "pk": false },
          { "name": "done",  "type": "Bool",     "pk": false }
        ],
        "indexes": [
          { "type": "btree", "columns": ["owner"] }
        ],
        "spatial_index": null,
        "visibility": { "rule": "owner_only", "owner_field": "owner" }
      }
    ],
    "reducers": [
      {
        "name": "send_chat",
        "version": 2,
        "params": [
          { "name": "channel", "type": "U32" },
          { "name": "text",    "type": "Str" }
        ],
        "return_type": "Result[Unit, Str]"
      }
    ],
    "views": [ ... ],
    "procedures": [ ... ]
  }
  ```

  Column and parameter `type` values are FluxValue wire-type names as defined in SPEC-006
  (`Bool`, `I8`–`I64`, `U8`–`U64`, `F32`/`F64`, `Str`, `Buffer`, `Identity`, `EntityId`,
  `Timestamp`, `Option[T]`, `List[T]`). Composite primary keys appear as multiple columns with
  `"pk": true`, in declaration order. Tables declared `#[fluxum::table(partition_by(field))]`
  report the partition column in `partition_by` (SPEC-007); non-partitioned tables report `null`.
  Entries under `views` and `procedures` carry the same signature shape as reducers
  (`name`, `params`, `return_type`).

- **SDK-002** [P0] The schema document SHALL include `schema_version: U32` — the current version
  from the schema registry (SPEC-001, SPEC-010). Generated SDK code SHALL embed the
  `schema_version` of the document it was generated from; runtime verification against
  `InitialData.schema_version` is specified in SDK-043.

## 3. `fluxum generate` CLI

- **SDK-010** [P0] The `fluxum generate` CLI command SHALL accept:

  ```
  fluxum generate --lang <lang> --schema <url_or_file> --out <dir>

  Arguments:
    --lang    Target language: typescript | python | go | rust | csharp | cpp
    --schema  URL of the /schema endpoint or path to a saved schema JSON file
    --out     Output directory for generated files
  ```

  Generation from a schema file MUST behave identically to generation from a live URL, and MUST
  work fully offline (no running server required) — see SDK-070.

- **SDK-011** [P0] For each target language, the generator SHALL produce the following files in
  `<out>/<lang>/`:

  ```
  <out>/
    typescript/
      index.ts            // re-exports
      tables.ts           // table types (interfaces)
      reducers.ts         // reducer call functions
      client.ts           // typed FluxumClient class

    python/
      __init__.py         # re-exports
      tables.py           # table dataclasses + FluxBIN row decoders
      reducers.py         # async reducer call functions
      client.py           # typed FluxumClient wrapper (asyncio)

    go/
      tables.go           // table structs + FluxBIN row decoders
      reducers.go         // typed reducer call functions
      client.go           // typed FluxumClient wrapper

    rust/
      mod.rs              // re-exports
      tables.rs           // table structs + FluxBIN row decode impls
      reducers.rs         // typed reducer call helpers

    csharp/
      Fluxum.Tables.cs
      Fluxum.Reducers.cs
      Fluxum.Client.cs

    cpp/                  // P2 target (SDK-063)
      Fluxum.h            // typed structs + reducer helpers in one header
      Fluxum.cpp          // implementation
  ```

  The Rust target emits no `client.rs`: the client runtime is provided by the handwritten
  `fluxum-sdk` crate (§7); generated Rust bindings layer on top of it (SDK-051). Every other
  P0/P1 target follows the same pattern: connection, auth, subscription management, and diff
  application live in the hand-written per-language runtime package (SDK-064, SDK-071), and the
  generated `client.*` file is a typed wrapper over it.

- **SDK-012** [P1] The CLI SHALL support exporting the schema to a JSON file for offline SDK
  generation:

  ```bash
  fluxum schema export --server http://localhost:15800 --out ./schema.json
  ```

  The exported file SHALL be byte-identical to the body served by `GET /schema`, so it can be
  committed to a repository, diffed in review, and used as the golden file for the T6.1 schema
  freeze test.

## 4. Generated types (per table)

- **SDK-020** [P0] For each table declared `#[fluxum::table(public)]`, the generator SHALL
  produce a typed struct/interface with all columns typed using the target language's equivalent
  of the FluxValue wire types (SDK-021).

  TypeScript example for `Task`:

  ```typescript
  export interface Task {
      id:    bigint;      // U64 → bigint
      owner: Uint8Array;  // Identity → Uint8Array (32 bytes)
      title: string;      // Str → string
      done:  boolean;     // Bool → boolean
  }
  ```

  C++ example:

  ```cpp
  struct Task {
      uint64_t id;
      std::array<uint8_t, 32> owner;   // Identity
      std::string title;
      bool done;
  };
  ```

  Rust example (generated by `fluxum generate --lang rust`):

  ```rust
  #[derive(Debug, Clone, PartialEq)]
  pub struct Task {
      pub id: u64,
      pub owner: fluxum_protocol::Identity,
      pub title: String,
      pub done: bool,
  }
  ```

- **SDK-021** [P0] Type mapping table:

  | FluxValue type | TypeScript | Python | Go | Rust | C# | C++ (P2) |
  |----------------|-----------|--------|----|------|----|----------|
  | `Bool` | `boolean` | `bool` | `bool` | `bool` | `bool` | `bool` |
  | `I8`–`I64` | `number`/`bigint` | `int` | `int8`–`int64` | `i8`–`i64` | `sbyte`–`long` | `int8_t`–`int64_t` |
  | `U8`–`U64` | `number`/`bigint` | `int` | `uint8`–`uint64` | `u8`–`u64` | `byte`–`ulong` | `uint8_t`–`uint64_t` |
  | `F32`/`F64` | `number` | `float` | `float32`/`float64` | `f32`/`f64` | `float`/`double` | `float`/`double` |
  | `Str` | `string` | `str` | `string` | `String` | `string` | `std::string` |
  | `Buffer` | `Uint8Array` | `bytes` | `[]byte` | `Vec<u8>` | `byte[]` | `std::vector<uint8_t>` |
  | `Identity` | `Uint8Array` (32) | `bytes` (32) | `Identity` (`[32]byte`) | `Identity` (newtype) | `byte[32]` | `std::array<uint8_t,32>` |
  | `EntityId` | `bigint` | `int` | `EntityId` (`uint64`) | `EntityId` (newtype) | `ulong` | `uint64_t` |
  | `Timestamp` | `bigint` | `int` | `Timestamp` (`int64`) | `Timestamp` (newtype) | `long` | `int64_t` |
  | `Option[T]` | `T \| null` | `T \| None` | `*T` (`nil` = absent) | `Option<T>` | `T?` | `std::optional<T>` |
  | `List[T]` | `T[]` | `list[T]` | `[]T` | `Vec<T>` | `List<T>` | `std::vector<T>` |

  In TypeScript, 64-bit integer types (`I64`, `U64`, `EntityId`, `Timestamp`) SHALL map to
  `bigint` to avoid precision loss; 32-bit-and-smaller integers map to `number`. The Rust
  newtypes (`Identity([u8; 32])`, `EntityId(u64)`, `Timestamp(i64)`) come from
  `fluxum-protocol` — generated Rust code SHALL reuse them, never redefine them. In Python,
  all integer wire types map to `int` (arbitrary precision); the encoder SHALL range-check
  values against the wire type on serialization. In Go, `Identity`, `EntityId`, and
  `Timestamp` are named types provided by the Go runtime package (SDK-071), and `Option[T]`
  maps to a pointer (`nil` = absent).

## 5. Generated reducer calls

- **SDK-030** [P0] For each `#[fluxum::reducer]`, the generator SHALL produce a typed call
  function that serializes arguments to FluxValue and sends a `ReducerCall` message (SPEC-006):

  TypeScript example:

  ```typescript
  export async function sendChat(client: FluxumClient, channel: number, text: string): Promise<void> {
      const result = await client.call("send_chat", [
          { type: "U32", value: channel },
          { type: "Str", value: text },
      ]);
      if (result.outcome === "Err") throw new Error(result.message);
  }
  ```

  C++ example:

  ```cpp
  std::future<void> send_chat(FluxumClient& client, uint32_t channel, const std::string& text) {
      return client.call("send_chat", {
          FluxValue::U32(channel),
          FluxValue::Str(text),
      });
  }
  ```

  Rust example (layered on `fluxum-sdk`):

  ```rust
  pub async fn send_chat(client: &FluxumClient, channel: u32, text: String) -> Result<(), String> {
      client
          .call("send_chat", &[FluxValue::U32(channel), FluxValue::Str(text)])
          .await
  }
  ```

  An `Err` outcome in `ReducerResult` SHALL surface as the target language's idiomatic error
  (thrown `Error` in TypeScript, raised exception in Python, non-nil `error` return in Go,
  `Err(String)` in Rust, thrown exception in C#, failed future/exception in C++).

- **SDK-031** [P1] When the schema document records a reducer `version` greater than 1, the
  generated wrapper SHOULD pin that version via `ReducerCall.version` (omitted for version 1),
  so a stale SDK never silently invokes a newer, signature-incompatible reducer revision
  (reducer versioning: SPEC-004).

## 6. Generated client runtime (local cache, events, schema check)

- **SDK-040** [P0] The generated `FluxumClient` SHALL maintain a local cache of all subscribed
  table rows, mirroring the server state selected by the client's subscriptions (SPEC-005). The
  cache SHALL be updated automatically by applying `TxUpdate` diffs.

  TypeScript example:

  ```typescript
  class FluxumClient {
      // Auto-generated typed caches (one per public table)
      readonly tasks:        Map<bigint, Task>        = new Map();
      readonly chatMessages: Map<bigint, ChatMessage> = new Map();

      private applyTxUpdate(update: TxUpdate): void {
          for (const tableUpdate of update.tables) {
              if (tableUpdate.tableName === "Task") {
                  for (const row of tableUpdate.inserts) this.tasks.set(row.id, row);
                  for (const pk of tableUpdate.deletes) this.tasks.delete(pk.id);
              }
              // ... one branch per generated table
          }
      }
  }
  ```

  Cache keys are the table's primary key. Non-integer primary keys (e.g. `Identity` in
  `OnlineUser`) SHALL be encoded to a stable string key; composite primary keys
  (`primary_key(a, b)`) SHALL be encoded as a stable tuple key in column declaration order.
  Per SPEC-006, `deletes` entries carry primary-key fields only.

- **SDK-041** [P0] Rows in `InitialData` and `TxUpdate` arrive as FluxBIN-encoded buffers
  (SPEC-006). For each table, generated SDKs SHALL include a FluxBIN row decoder — full row for
  `inserts`, primary-key-only for `deletes` — derived at generation time from the schema's
  column order and types. Decoding SHALL be straight-line generated code: no runtime reflection
  and no runtime schema interpretation. (Reducer arguments are FluxValue values inside the
  MessagePack envelope, not FluxBIN — see SDK-030.)

- **SDK-042** [P1] The generated client SHALL support typed event callbacks triggered by cache
  updates:

  ```typescript
  const db = await FluxumClient.connect("http://localhost:15800");

  db.on("Task:insert", (row: Task) => { ... });
  db.on("Task:delete", (row: Task) => { ... });
  db.on("Task:update", (old: Task, updated: Task) => { ... });
  ```

  `update` callbacks are emitted when a row with the same primary key appears in both `deletes`
  and `inserts` within the same `TxUpdate` (delete old + insert new = update); the SDK SHALL
  emit one `:update` instead of a `:delete`/`:insert` pair. The row passed to `:delete` (and
  `old` in `:update`) is the previously cached copy, resolved before removal — the wire carries
  only the primary key.

- **SDK-043** [P0] Generated SDK code SHALL verify the embedded `schema_version` (SDK-002)
  against `InitialData.schema_version` on connection. On mismatch the SDK SHALL trigger an
  automatic schema refresh (re-fetch `GET /schema`) and reconnect before delivering any data
  to the application (SPEC-006). Because generated types cannot change at runtime, if the
  refreshed schema is incompatible with the embedded bindings the SDK SHALL surface a typed
  `SchemaMismatch` error/event so the application can prompt for regeneration
  (`fluxum generate`) instead of operating on mistyped rows.

## 7. Rust client SDK — `fluxum-sdk`

- **SDK-050** [P1] The workspace crate `sdks/rust` (`fluxum-sdk`) SHALL provide the Rust client
  runtime: an async `FluxumClient` (tokio) that connects via FluxRPC TCP
  (`fluxum://host:15801`) or Streamable HTTP (`http(s)://host:15800`, `/rpc` — SPEC-006),
  authenticates (SPEC-009), calls reducers, manages subscriptions
  (`Subscribe`/`SubscribeSingle`/`Unsubscribe`/`OneOffQuery`), and maintains the local cache
  with the same diff-application, update-coalescing, and schema-version-check semantics as
  SDK-040–SDK-043. It SHALL reuse `fluxum-protocol` for FluxValue, the FluxBIN codec, message
  types, and the `Identity`/`ConnectionId`/`EntityId`/`Timestamp` newtypes — no duplicated wire
  code. Trusted backend services (privileged server peers) use this same SDK with a
  server-to-server identity (SPEC-009).

- **SDK-051** [P1] `fluxum generate --lang rust` SHALL emit bindings that layer on `fluxum-sdk`:
  table structs with FluxBIN decode impls (SDK-020, SDK-041), typed reducer wrappers (SDK-030),
  and typed cache/event accessors:

  ```rust
  let db = FluxumClient::connect("fluxum://localhost:15801").await?;
  db.subscribe(["SELECT * FROM Task WHERE owner = :me"]).await?;
  db.on_insert::<Task>(|row| { /* update UI state */ });
  send_chat(&db, 1, "hello".to_string()).await?;
  ```

## 8. Language targets & idioms

Each SDK MUST feel native in its language while preserving identical semantics (types per
SDK-020/021, reducer wrappers per SDK-030/031, cache/events/schema check per SDK-040–SDK-043).

**JavaScript/TypeScript (P0, T6.2, FR-82)** — the reference target: typed event callbacks
(SDK-042), Promise-based reducer and connection API (SDK-030, SDK-040), typed local cache
(SDK-040). Powers the demo app (T6.5). SDK-020/021/030/040–043 are normative for it, plus the
browser-native requirements below — the browser talks the **binary high-performance RPC directly
to the database**; there is no JSON fallback and no intermediate gateway.

- **SDK-080** [P0] **Browser-native binary runtime.** The JS/TS SDK MUST run in evergreen
  browsers with no polyfills and no bundler-specific transforms, connecting over **Streamable
  HTTP** (`http(s)://host:15800`, `POST /rpc` for client→server frames, `GET /rpc` binary push
  stream — SPEC-006). The push stream SHALL be consumed via fetch `ReadableStream`; all hot-path
  work — frame parsing, MessagePack envelope decode, FluxBIN row decode (SDK-041) — SHALL
  operate directly on `ArrayBuffer`/`DataView`/typed arrays. JSON, SSE, base64, or string
  round-trips MUST NOT appear anywhere on the message hot path.

- **SDK-081** [P0] **Plain-JavaScript consumable.** The package SHALL ship compiled JavaScript
  (ESM and CJS) with `.d.ts` type declarations: TypeScript is a development convenience, never a
  requirement — vanilla-JS applications consume the same package with full runtime behavior.
  The runtime SHALL have zero runtime dependencies and be importable via npm **and** directly in
  a browser via `<script type="module">` / ESM CDN without a build step.

- **SDK-082** [P0] **Dual environment, one package.** The same package SHALL run in Node.js
  (FluxRPC TCP via `fluxum://host:15801`, or Streamable HTTP) and in browsers (Streamable HTTP
  via `http(s)://host:15800`). Transport selection follows the URI scheme; using `fluxum://`
  (TCP) in a browser SHALL fail fast with an actionable error naming the `http(s)://` endpoint
  form. Session re-establishment with exponential backoff and automatic re-authentication +
  resubscription (SPEC-006 session semantics, SPEC-014 client behavior) apply identically in
  both environments.

- **SDK-083** [P1] **Footprint.** The hand-written runtime (excluding generated code) SHALL be
  tree-shakeable (side-effect-free ESM) and ≤ 50 KB minified + gzipped; the size is asserted in
  CI so regressions fail the build.

- **SDK-084** [P1] **Browser CI.** The shared conformance corpus (SDK-064) SHALL run for this
  SDK in two environments in CI: Node.js and a real headless browser (Chromium), covering the
  Streamable HTTP binary path (`POST /rpc` + `GET /rpc` fetch stream) end-to-end against a live
  server.

- **SDK-085** [P2] **WebTransport (FR-88).** When the server offers a WebTransport (HTTP/3 /
  QUIC) endpoint, the browser runtime MAY prefer it over Streamable HTTP for lower latency; the
  message layer and FluxBIN encoding are unchanged. Post-0.2.0, unscheduled.

**Rust (P1, T6.4, FR-84)** — tokio async runtime, typed table/event handles, shares
`fluxum-protocol`: see §7 (SDK-050, SDK-051).

- **SDK-061** [P1] **Python SDK (T7.4, FR-83).** `fluxum generate --lang python` plus the
  hand-written runtime SHALL be asyncio-first: every network operation is a coroutine, full
  type hints on all public APIs (`py.typed` marker), and async context managers for connection
  and subscription lifecycle:

  ```python
  async with FluxumClient.connect("http://localhost:15800") as db:
      await db.subscribe(["SELECT * FROM Task WHERE owner = :me"])
      db.on_insert(Task, lambda row: ...)
      await send_chat(db, channel=1, text="hello")
  ```

- **SDK-062** [P1] **Go SDK (T7.5, FR-85).** The Go SDK SHALL be context-aware and use
  idiomatic errors: every blocking operation takes `context.Context` as its first parameter,
  subscription and row-event delivery use channels, and failures are wrapped `error` values
  (compatible with `errors.Is`/`errors.As`) — no panics for protocol or reducer errors:

  ```go
  db, err := fluxum.Connect(ctx, "fluxum://localhost:15801")
  events, err := db.SubscribeTasks(ctx, "SELECT * FROM Task WHERE owner = :me")
  for ev := range events { /* typed TaskEvent */ }
  ```

- **SDK-060** [P1] **C# SDK (T7.6, FR-86).** `fluxum generate --lang csharp` SHALL emit
  `Fluxum.Tables.cs`, `Fluxum.Reducers.cs`, and `Fluxum.Client.cs` with the same feature set
  as the TypeScript target. The API SHALL be async/await throughout (`Task`-returning calls)
  and expose subscriptions and row events as `IAsyncEnumerable<T>` streams. Distributed via
  NuGet as `Fluxum.Sdk` (SDK-071). Ships in 0.2.0.

- **SDK-063** [P2] **C++ target (FR-87).** `fluxum generate --lang cpp` SHALL emit typed
  structs and reducer helpers only (`Fluxum.h`/`Fluxum.cpp` per SDK-020/021/030). A full C++
  client runtime (cache, events, schema check per SDK-040–SDK-043) is out of scope for 0.2.0;
  the C++ target is not part of the five-SDK conformance gate. Post-0.2.0, unscheduled.

- **SDK-064** [P1] **Shared conformance corpus.** Every P0/P1 SDK (TypeScript, Python, Go,
  Rust, C#) MUST pass the same shared conformance corpus (SPEC-013) in its language's CI job:
  identical wire fixtures, cache-application scenarios, update-coalescing cases, and
  schema-mismatch drills. Each SDK SHALL consist of generated code plus a thin hand-written
  transport/runtime layer per language; protocol semantics (FluxBIN decode, diff application,
  coalescing, version checks) MUST NOT fork per language beyond that thin layer.

## 9. Distribution & regeneration workflow

- **SDK-070** [P1] The recommended SDK regeneration workflow SHALL be:

  ```bash
  # Fetch the schema from a running server and regenerate the SDK
  fluxum generate --lang typescript --schema http://localhost:15800/schema --out ./sdk

  # Or from a saved schema file (offline codegen — CI without a running server)
  fluxum generate --lang go --schema ./schema.json --out ./client/generated
  ```

  This workflow ensures the client SDK always reflects the current server schema. Generated
  files SHALL NOT be hand-edited — they are generated artifacts and SHALL carry a
  "generated — do not edit" header naming the source `schema_version`. Generation SHALL be
  deterministic: the same schema document produces byte-identical output, so CI can regenerate
  and diff to detect drift.

- **SDK-071** [P1] The hand-written per-language runtime packages (SDK-064) SHALL be published
  under the HiveLLM family naming precedent:

  | Registry | Package |
  |----------|---------|
  | npm | `@hivehub/fluxum-sdk` |
  | PyPI | `fluxum-sdk` |
  | crates.io | `fluxum-sdk` |
  | Go module | `github.com/hivellm/fluxum-sdk-go` |
  | NuGet | `Fluxum.Sdk` |

## Acceptance criteria

1. **Schema golden file (T6.1 freeze gate):** `fluxum schema export` against the demo-app module
   matches the committed golden `schema.json` byte-for-byte; any diff fails CI. The document
   contains tables (columns, types, pk/auto_inc, indexes, spatial_index, partition_by,
   visibility), reducers (name, version, params, return_type), views, procedures, and
   `schema_version`.
2. **JavaScript/TypeScript target (T6.2):** `fluxum generate --lang typescript` against the demo
   schema produces code that compiles under `tsc --strict` with zero manual stubs; `Task`,
   `ChatMessage`, `OnlineUser`, and `User` interfaces and all reducer wrappers (`sendChat`,
   `completeTask`, …) are emitted and typed per SDK-021.
   **Browser-native checks:** the conformance corpus passes in headless Chromium against a live
   server over Streamable HTTP — `POST /rpc` + `GET /rpc` consumed via fetch `ReadableStream`
   (SDK-080, SDK-084); a vanilla-JS smoke test loads the runtime via `<script type="module">`
   with no build step, connects, subscribes, and receives a typed `TxUpdate` (SDK-081); the
   packaged runtime is ≤ 50 KB min+gzip (SDK-083); `fluxum://` in a browser fails fast with the
   actionable error (SDK-082).
3. **Rust SDK (T6.4):** `fluxum-sdk` plus `--lang rust` bindings pass the client conformance
   subset (SPEC-013); a symbol/type audit confirms all wire types come from `fluxum-protocol`.
4. **Python SDK (T7.4):** `--lang python` bindings plus the asyncio runtime pass the shared
   conformance corpus in Python CI; public API is fully type-hinted (`py.typed`) and
   connection/subscription lifecycle works via async context managers (SDK-061).
5. **Go SDK (T7.5):** `--lang go` bindings plus the Go runtime pass the shared conformance
   corpus in Go CI; all blocking calls take `context.Context`, subscriptions deliver over
   channels, and errors are idiomatic wrapped values (SDK-062).
6. **C# SDK (T7.6):** `--lang csharp` output plus the .NET runtime pass the shared conformance
   corpus in .NET CI; subscriptions surface as `IAsyncEnumerable<T>` and the package builds as
   NuGet `Fluxum.Sdk` (SDK-060).
7. **Five-SDK conformance gate (G7, release 0.2.0):** all five SDKs — TypeScript, Python, Go,
   Rust, C# — pass the shared conformance corpus (SPEC-013) in CI from the same fixture set
   (SDK-064).
8. **End-to-end round trip:** the demo app (chat + presence + per-user tasks) runs on the
   generated TypeScript SDK: a `send_chat` reducer call produces a `TxUpdate`, the local cache
   reflects the new `ChatMessage` row, and the `ChatMessage:insert` callback fires with a fully
   typed row decoded from FluxBIN. A delete+insert of the same `Task` primary key within one
   `TxUpdate` fires exactly one `Task:update` callback.
9. **Schema-mismatch drill:** bump the server `schema_version` (SPEC-010 migration); a client
   built from the previous schema detects the mismatch on `InitialData.schema_version`,
   refreshes the schema and reconnects, and surfaces a typed `SchemaMismatch` error without
   delivering mistyped rows to the application.
10. **Offline CI codegen:** on a machine with no running server, `fluxum generate` from the
    exported `schema.json` produces output byte-identical to URL-based generation against the
    live server.
11. **Determinism:** regenerating twice from the same schema yields byte-identical files; every
    generated file carries the "generated — do not edit" header with the source `schema_version`.
12. **C++ target (P2, post-0.2.0):** when scheduled, generated `Fluxum.h`/`Fluxum.cpp` (typed
    structs + reducer helpers only, SDK-063) compile cleanly in the C++ test harness.
