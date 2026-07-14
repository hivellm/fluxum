# 07 — SDKs, Code Generation, and CLI

Deep implementation analysis of the real SpacetimeDB source, focused on the client-facing
surface: the codegen crate, the four client SDKs, the `spacetime` CLI, and the template/test
module corpus. Written to inform Fluxum's SPEC-011 (SDK codegen) and SPEC-013 (conformance).

## Source

| | |
|---|---|
| **Project** | SpacetimeDB (Clockwork Labs) |
| **Version** | 2.7.0 |
| **Commit** | `1a8df2a` (2026-07-13) |
| **Areas analyzed** | `crates/codegen` (~13.3k LOC), `crates/cli` (~18.2k LOC), `sdks/rust` (~5.0k LOC, 14 files), `sdks/csharp` (~6.3k LOC src + tests), `sdks/typescript` → `crates/bindings-typescript` (~20.2k LOC src), `sdks/unreal` (~17k LOC plugin), `templates/` (25 templates), `modules/` (38 test modules) |
| **Compared against** | Fluxum `docs/specs/SPEC-011-sdk-codegen.md` (SDK-001..085), SPEC-013 |

---

## 1. `crates/codegen` — one trait, five backends

### 1.1 The `Lang` trait

`crates/codegen/src/lib.rs` (only 71 lines) is the entire orchestration layer:

```rust
pub fn generate(module: &ModuleDef, lang: &dyn Lang, options: &CodegenOptions) -> Vec<OutputFile> {
    itertools::chain!(
        util::iter_tables(module, options.visibility).map(|tbl| lang.generate_table_file(module, tbl)),
        module.views().map(|view| lang.generate_view_file(module, view)),
        module.types().flat_map(|typ| lang.generate_type_files(module, typ)),
        util::iter_reducers(module, options.visibility).map(|r| lang.generate_reducer_file(module, r)),
        util::iter_procedures(module, options.visibility).map(|p| lang.generate_procedure_file(module, p)),
        lang.generate_global_files(module, options),
    ).collect()
}

pub trait Lang {
    fn generate_table_file_from_schema(&self, module: &ModuleDef, tbl: &TableDef, schema: TableSchema) -> OutputFile;
    fn generate_type_files(&self, module: &ModuleDef, typ: &TypeDef) -> Vec<OutputFile>;
    fn generate_reducer_file(&self, module: &ModuleDef, reducer: &ReducerDef) -> OutputFile;
    fn generate_procedure_file(&self, module: &ModuleDef, procedure: &ProcedureDef) -> OutputFile;
    fn generate_global_files(&self, module: &ModuleDef, options: &CodegenOptions) -> Vec<OutputFile>;
    // provided: generate_table_file (validates TableSchema), generate_view_file
}
```

Key observations:

- **Input is the validated `ModuleDef`** from `spacetimedb_schema::def` — the same structure the
  host uses — not a bespoke schema JSON. Views are lowered to tables for codegen purposes
  (`generate_view_file` builds a `TableSchema::from_view_def_for_codegen` and reuses the table
  path), so **views get the same generated client-cache handles as tables** for free.
- **File granularity is per-entity**: one file per table, per type, per reducer, per procedure,
  plus "global files" (the `DbConnection`/`RemoteTables`/`RemoteReducers` glue). Fluxum's
  SPEC-011 SDK-011 chooses per-*category* files (`tables.ts`, `reducers.ts`) instead — fewer
  files, but SpacetimeDB's per-entity layout makes stale-file cleanup and diffs easier.
- **Visibility filtering**: `CodegenOptions { visibility: CodegenVisibility::OnlyPublic }` is the
  default; private tables are skipped and reported (`util::private_table_names`,
  `crates/cli/src/subcommands/generate.rs` prints "Skipping private tables during codegen").
- **`AUTO_GENERATED_PREFIX`** (`crates/codegen/src/util.rs`): every emitted file starts with a
  magic marker comment; the CLI uses it to identify and delete stale generated files (§6.2).
  This is exactly Fluxum's SDK-070 "generated — do not edit" header, but load-bearing.

### 1.2 Backends and their sizes

| Backend | File | LOC | Wired into `spacetime generate` |
|---|---|---|---|
| Rust | `crates/codegen/src/rust.rs` | 2,195 | yes (`--lang rust`) |
| C# | `crates/codegen/src/csharp.rs` | 1,652 | yes (`--lang csharp`, `--namespace`) |
| TypeScript | `crates/codegen/src/typescript.rs` | 1,318 | yes (`--lang typescript`) |
| Unreal C++ | `crates/codegen/src/unrealcpp.rs` | **7,081** | yes (`--lang unrealcpp`, needs `--uproject-dir` + `--unreal-module-name`) |
| Plain C++ | `crates/codegen/src/cpp.rs` | 632 | not exposed via `pub use`; server-module oriented |

The Unreal backend alone is bigger than the other three combined — generating UCLASS/USTRUCT
Blueprint-compatible wrappers, `.generated.h` includes, and delegate plumbing is enormously
expensive. Lesson: **an engine-native C++ target is a whole product, not a fifth backend.**
Fluxum's decision to keep C++ at "typed structs + reducer helpers only" (SDK-063) is validated.

What gets generated per entity (consistent across backends, verified in the TS output at
`crates/bindings-typescript/src/sdk/client_api/index.ts` and C# example
`sdks/csharp/examples~/regression-tests/client/module_bindings/SpacetimeDBClient.g.cs`):

- **Per table**: a row struct/class with typed fields + BSATN (de)serialization registration; a
  table-handle accessor exposing `count()`, `iter()`, `on_insert`/`on_delete`/`on_update`
  (update only when a PK exists), and typed unique-index point-lookup accessors
  (`user.identity().find(...)`).
- **Per reducer**: an args struct plus a typed call method on `RemoteReducers`. Callback style
  diverges by language: Rust emits `{reducer}()` fire-and-forget plus `{reducer}_then(args,
  callback)` completion callbacks (`rust.rs:463-600`) with a global `enum Reducer` for event
  dispatch; C# emits a per-reducer `public event {R}Handler On{R}` (`csharp.rs:819-921`).
- **Per type**: standalone (de)serializable type definitions (sum/product types).
- **Globals**: `DbConnection` (a thin subclass of the runtime's `DbConnectionImpl`/
  `DbConnectionBase`), `RemoteTables`, `RemoteReducers`, `SubscriptionBuilder`,
  `RemoteModule` metadata object, and typed `EventContext` aliases.
- **Procedures** (request/response functions with return values, unlike reducers) are
  first-class in codegen — `invoke_procedure_with_callback::<_, Ret>` in Rust, `Promise`-returning
  accessors in TS. **Row-level security has zero codegen involvement** (enforced purely
  server-side); scheduled tables surface only as a `ScheduleAt` column type. Private tables are
  filtered out but their *row types* are still emitted (types are never visibility-filtered).
- **Version guard**: the TS `REMOTE_MODULE` carries `versionInfo.cliVersion`; the runtime
  enforces a minimum CLI version (`src/sdk/version.ts::ensureMinimumVersionOrThrow`, min 1.4.0)
  and throws on stale generated code. This is codegen-version pinning, not schema-version
  pinning — SpacetimeDB has **no runtime schema_version handshake** like Fluxum's SDK-043.

---

## 2. Rust SDK (`sdks/rust`, `spacetimedb-sdk`, ~5,008 LOC / 14 files)

Files: `db_connection.rs`, `client_cache.rs` (637), `websocket.rs` (649), `subscription.rs`
(566), `callbacks.rs`, `spacetime_module.rs` (325), `table.rs` (211), `event.rs`,
`credentials.rs`, `compression.rs`, `db_context.rs`, `error.rs`, `metrics.rs`, `lib.rs`.

### 2.1 Client cache — the part worth copying

`sdks/rust/src/client_cache.rs` is the reference implementation all other SDKs mirror:

```rust
pub struct TableCache<Row> {
    /// Keys are BSATN-serialized representations of the values.
    pub(crate) entries: HashMap<Bytes, RowEntry<Row>>,
    pub(crate) unique_indices: HashMap<&'static str, Box<dyn UniqueIndexDyn<Row = Row>>>,
}
pub(crate) struct RowEntry<Row> { row: Row, ref_count: u32 }
```

Five design decisions, each with an explicit rationale in the source:

1. **Rows are keyed by their raw BSATN bytes**, not by PK and not by the row value. The doc
   comment: this gives `HashMap` semantics "even when `Row` is not `Hash + Eq`, e.g. for row
   types which contain floats", and byte-slice hashing "can be implemented directly via SIMD
   without skipping padding or branching on enum variants". The server already sent the bytes;
   they are kept alongside the decoded row (`WithBsatn<Row>`, `spacetime_module.rs`).
2. **Reference counting for overlapping subscriptions.** `RowEntry.ref_count` counts how many
   live queries cover the row. An insert on an existing key bumps the count silently; a delete
   decrements and only produces a *semantic* delete event at `ref_count == 0`
   (`handle_insert`/`handle_delete`, `client_cache.rs:203-259`). This is how N overlapping
   `SELECT` queries dedupe into one cached row and one callback.
3. **Inserts are applied before deletes** within a `TableUpdate`
   (`TableCache::apply_diff`, `client_cache.rs:269`) — required so a refcount never transiently
   hits zero (which would fire a spurious delete+insert pair) and so unique-index updates don't
   collide. A long comment (`:230-244`) documents why the host can legitimately send
   `[delete r0, insert r0]` for the *same row* under join semantics — the apply logic cancels
   the pair.
4. **Update derivation is a post-pass by primary key.** `apply_diff` returns only semantic
   inserts/deletes; the *generated* code then calls
   `TableAppliedDiff::with_updates_by_pk(|row| &row.pk_field)` (`client_cache.rs:131`), which
   uses `HashMap::extract_if` to move matching delete+insert pairs into
   `update_deletes`/`update_inserts`. Tables without a PK never get update events. This is
   precisely Fluxum's SDK-042 coalescing rule, implemented as a pluggable projection so the
   runtime stays schema-agnostic (thin-runtime compatible).
5. **Unique "indexes" are duplicated `HashMap<Col, Row>`s** (`UniqueIndexImpl`,
   `client_cache.rs:591`) — each unique index stores a full clone of every row, with a comment
   acknowledging the hack ("HashMap does not expose stable indices"). Non-unique/BTree client
   indexes don't exist here; in the TS SDK they exist but are **full scans** with a TODO.

### 2.2 Threading, callbacks, transport

- **Single-threaded apply, message-passing everywhere.** All mutations — including callback
  *registration* — are funneled through an unbounded `futures_channel::mpsc` of
  `PendingMutation` messages (`db_connection.rs`), drained by one advance loop
  (`run_threaded`, `run_async`, or manual `frame_tick()`). `TableHandle`s hold
  `Arc<Mutex<ClientCache>>` and take the lock only per-operation (`client_cache.rs:404-437`).
  Callbacks are invoked **after** the cache mutation for the whole transaction is applied.
- **Transport**: WebSocket, BSATN subprotocol only — there is no JSON client protocol in v2.
  Server→client compression (brotli/gzip) negotiated via a `?compression=` query parameter
  (`websocket.rs`, `compression.rs`).
- **Reducer calls**: fire-and-forget `CallReducer` with request-id tracking for matching the
  resulting `ReducerEvent`; flags hardcoded to `Default` in v2 (parity comment appears in all
  SDKs).
- **No auto-reconnect.** On disconnect, `on_disconnect` fires and the connection is dead; the
  cache is **not cleared**. Identity continuity is manual: save the token from `on_connect`,
  pass it to the next builder (`credentials.rs`).

---

## 3. C# SDK (`sdks/csharp`, v2.7.0, ~6,300 LOC src)

- **Packaging**: `netstandard2.1` library (`SpacetimeDB.ClientSDK.csproj`) **and** a Unity UPM
  package (`package.json` = `com.clockworklabs.spacetimedbsdk`) in the same tree, with paired
  `.meta` files, a Godot csproj variant, and a WebGL `WebSocket.jslib` shim. One codebase,
  three engines.
- **Main-thread pump**: `IDbConnection.FrameTick()` (`src/SpacetimeDBClient.cs:986`). In Unity a
  `MonoBehaviour` singleton (`src/SpacetimeDBNetworkManager.cs`) auto-pumps every `Update()`.
  `DbConnection` is single-thread-affine — only the `FrameTick` thread may touch `conn.Db`
  (documented in `DEVELOP.md`).
- **Pipeline**: network thread → `_parseQueue` (`BlockingCollection`) → dedicated parse thread
  (decompress + BSATN decode + build `MultiDictionaryDelta` per table, entirely off the main
  thread) → `_applyQueue` → main-thread apply. On WebGL (no threads) parsing runs as a
  coroutine.
- **Cache structure**: `MultiDictionary<object, Row>` (`src/MultiDictionary.cs`, 725 LOC) — a
  multiset `Dictionary<TKey,(Value, uint Multiplicity)>`; multiplicity plays the ref_count role.
  Key = PK value if the table has one, **else the row object itself** (structural
  `Equals`/`GetHashCode` generated for every `[SpacetimeDB.Type]`) — content-addressed rather
  than byte-addressed.
- **Three-phase apply across all tables** (`ApplyUpdate`, `SpacetimeDBClient.cs:661`):
  `PreApply` (fire `OnBeforeDelete`) → `Apply` (mutate multiset + fix unique/btree indexes) →
  `PostApply` (fire `OnInsert`/`OnUpdate`/`OnDelete`). Guarantees callbacks never observe a
  half-applied transaction — the strongest atomic-visibility statement of any SDK here.
- **Compression**: brotli (default) and gzip via a 1-byte per-message tag
  (`src/CompressionHelpers.cs:52`), with IL2CPP-driven perf workarounds documented inline.
- **Reconnection**: none (grep confirms); token persisted to Unity `PlayerPrefs` / Godot
  `ConfigFile` / `~/.spacetime_csharp_sdk/settings.ini` (`src/AuthToken.cs`) for manual
  reconnect.
- **Tests**: ~1,190 LOC — snapshot tests (Verify), randomized property tests of the multiset
  (`tests~/MultiDictionaryTests.cs`), query-builder tests.

---

## 4. TypeScript SDK (`sdks/typescript` → `crates/bindings-typescript`, npm `spacetimedb` v2.7.0)

Note: `sdks/typescript` is an unmaterialized git symlink (a text file containing
`../crates/bindings-typescript`). The SDK moved into `crates/` because the **same package is
also the server-side module bindings** — one npm package `spacetimedb` with subpath exports:
`.` / `./sdk` (client) / `./server` (module runtime, `src/server/runtime.ts`) / `./react` /
`./vue` / `./svelte` / `./solid` / `./angular` / `./tanstack`.

### 4.1 Browser story

- **Transport**: WebSocket only; subprotocols `v3.bsatn.spacetimedb` preferred, `v2.bsatn.…`
  fallback (`src/sdk/websocket_protocols.ts`). No JSON protocol, no HTTP streaming. v3 is a
  micro-batching frame layer (`websocket_v3_frames.ts`, outbound frames ≤ 256 KB).
- **Token hygiene**: one `fetch` POST to `v1/identity/websocket-token` swaps the long-lived
  bearer token for a short-lived one placed in the WS URL query — the real token never appears
  in a URL.
- **Compression in the browser**: native Web Streams `DecompressionStream`
  (`src/sdk/decompress.ts`); gzip default, brotli only where the runtime supports
  `DecompressionStream('brotli')` (throws at config time otherwise). No JS brotli decoder is
  shipped.
- **Node support**: global `WebSocket` (Node ≥ 22) or lazy `import('undici')` behind a
  bundler-invisible `new Function(...)` (`src/sdk/ws.ts`); `undici` is an optional peer dep.
- **Bundle size**: size-limit CI budgets — minified browser ESM **30 KB brotli / 34 KB gzip /
  134 KB raw**; React bindings **10 KB brotli** (`package.json` size-limit config). This is the
  competitive number Fluxum's SDK-083 (≤ 50 KB min+gzip) must beat or match — note SpacetimeDB's
  30 KB is *brotli* and its gzip figure (34 KB) is comfortably inside Fluxum's budget.
- **Build**: tsup (esbuild) with ~19 entries, ESM+CJS+browser conditions, `tsc` for `.d.ts`.

### 4.2 Cache and runtime

- `TableCacheImpl.rows: Map<ComparablePrimitive, [Row, refCount]>` (`src/sdk/table_cache.ts`).
  Row identity: PK value via `AlgebraicType.intoMapKey` (primitives direct; composite →
  BSATN+base64); **no PK → base64 of the row's raw BSATN bytes** — the same byte-identity trick
  as Rust, adapted to JS `Map` key constraints.
- `#mergeTableUpdates` guarantees at most one update batch per table per transaction;
  `applyOperations` coalesces same-PK delete+insert into `update` events with a
  `refCountDelta`, and returns `PendingCallback[]` — **all tables are mutated first, then all
  callbacks dispatched** (`db_connection_impl.ts`), matching C#'s atomic-visibility rule.
- Event tables (insert-only, never cached) are first-class (`isEvent`), as in Rust
  (`TableAppliedDiff::from_event_inserts`).
- **Serialization without codegen'd codecs**: generated code describes types in a fluent
  builder DSL `t` (`src/lib/type_builders.ts`, ~3.5k lines); the runtime compiles
  (de)serializer closures per type at connect time (`AlgebraicType.makeSerializer/…`,
  `src/lib/algebraic_type.ts`). This is a *runtime-interpreted* schema — the opposite of
  Fluxum's SDK-041 "straight-line generated code, no runtime schema interpretation" rule.
  Trade-off: SpacetimeDB's generated TS files are tiny and the runtime is big; Fluxum inverts it.
- **Reconnection**: core `DbConnectionImpl` never reconnects; the framework layer's
  `ConnectionManager` (`src/sdk/connection_manager.ts`) adds ref-counted connection sharing +
  exponential backoff (`min(1000·2^n, 30000)` ms) — but only for the React/Vue/etc. hooks.
- Known gaps admitted in source: one-off queries unimplemented (logs a warning on
  `OneOffQueryResult`); client indexes are full scans (`// TODO: build proper index
  structures`); an error on an already-applied subscription is **fatal** ("Once we have
  per-query storage, this won't be fatal" — evicting one query's rows requires per-query row
  bookkeeping the refcount scheme can't do).

---

## 5. Unreal SDK (`sdks/unreal`, plugin v1.0, ~17k LOC)

- A UE 5.6 Runtime plugin (`src/SpacetimeDbSdk/SpacetimeDbSdk.uplugin`), C++20, with a
  from-scratch BSATN implementation (`Public/BSATN/`), i128–u256 types, and heavy Blueprint
  exposure (162 `UFUNCTION`/`UPROPERTY` sites; connection is a `UObject`, wire structs are
  `USTRUCT(BlueprintType)`).
- Cache: `FTableCache<Row>` = `TMap<TArray<uint8>, FRowEntry<Row>>` — **keyed by raw BSATN
  bytes with a RefCount**, i.e. a literal port of the Rust design (`Public/DBCache/`).
- Threading: `Async(EAsyncExecution::Thread)` for parse/decompress + a **reorder buffer**
  (`NextPreprocessId`/`NextReleaseId`, `DbConnectionBase.cpp:210`) to re-sequence out-of-order
  background parses before the game-thread `FrameTick()` drains them. Optional auto-tick via
  `FTSTicker`.
- Rough edges: **brotli decompression is stubbed** (`DecompressBrotli` logs "unavailable" and
  fails, `DbConnectionBase.cpp:501`) so Unreal clients must negotiate gzip; one table instance
  per row type; token stored in `GameUserSettings.ini` via `GConfig` with a TODO about multiple
  tokens (`Private/Connection/Credentials.cpp`).
- Tests: four full UE test projects (~8.7k LOC) driven by a **Rust harness**
  (`tests/sdk_unreal_harness.rs`, `sdk-unreal-test-harness` crate).

Maturity ranking of the four SDKs: **C# ≥ Rust > TypeScript (recently rewritten, broad but with
admitted gaps) > Unreal (v1.0, functional but rough)**.

---

## 6. CLI (`crates/cli`, ~18.2k LOC)

### 6.1 Command surface

Registered in `src/lib.rs::get_subcommands()`:
`publish, delete, logs, call, describe, dev, sql, rename, generate, list, login, logout, init,
build, server, subscribe, start, lock, unlock, version`. Several are explicitly marked
UNSTABLE (`call`, `describe`, `sql`, `list`, `subscribe`, the whole `server` group) — a useful
DX pattern: ship the command, print an instability warning to stderr.

Day-1 developer loop is exactly five commands: `init` → `start` → `publish` → `generate` →
`logs` (plus `sql` for poking at data). Everything else is management surface.

### 6.2 `spacetime generate` consumes the *module artifact*, not a server

`src/subcommands/generate.rs` (1,398 LOC). Schema source precedence:

1. `--module-def <json>` (hidden): raw `RawModuleDef` JSON from file/stdin.
2. `--bin-path <wasm>` / `--js-path <js>`: inspect a prebuilt artifact.
3. Default: **build the module from source** (`build::exec_with_argstring`), then extract the
   schema by spawning the sibling binary **`spacetimedb-standalone extract-schema <artifact>`**
   (`extract_descriptions`, `generate.rs:734`), which runs `__describe_module__` inside the
   artifact and prints a `RawModuleDef`.

So codegen **never contacts a running server** — it is offline-by-construction, but it requires
the module *toolchain* (Rust/C#/TS compilers) on the machine. Fluxum's SDK-010 flips this:
generate from `GET /schema` or a saved JSON file, requiring neither toolchain nor server binary
— strictly better for client-team ergonomics (client developers usually don't have the module
source). SpacetimeDB's hidden `--module-def` path shows they retrofitted the same idea.

Other generate behaviors worth stealing:
- Default output dirs per language (`src/module_bindings` for Rust/TS, `module_bindings` for
  C#, `generate.rs:423`).
- **Stale-file GC**: walks the out dir, finds files starting with `AUTO_GENERATED_PREFIX` that
  weren't regenerated, confirms, deletes (`generate.rs:550-586`).
- Post-format via `rustfmt`/`dotnet format` (TS formatting is a TODO).
- Language auto-detection from the client project (`package.json`→TS, `Cargo.toml`→Rust,
  `*.csproj`→C#, `generate.rs:392`).

### 6.3 publish / dev / start / login

- **`publish`** (`publish.rs`, 1,366 LOC): build → `PUT /v1/database/{name}` (named) or
  `POST /v1/database` (anonymous, server-assigned identity). Migration safety is CLI-side: a
  pre-publish endpoint returns a migration plan; destructive changes require `--delete-data`
  or typed confirmation; breaking client changes gated by `--break-clients`; granular
  `--yes=all,remote,migrate,break-clients,…`.
- **`dev`** (`dev.rs`, 2,100 LOC): the flagship DX command — scaffolds if needed, then loops
  **generate → build → publish**, streams database logs, launches the client dev server with
  `SPACETIMEDB_DB_NAME`/`SPACETIMEDB_HOST` injected, and watches files (`notify`, 500 ms,
  gitignore-aware). It does *not* autostart the local server — `spacetime start` is separate.
- **`start`** (`start.rs`): exec-replaces into the sibling `spacetimedb-standalone start
  --data-dir … --jwt-key-dir …`. `spacetime version` delegates to a `spacetimedb-update`
  multicall binary for version management. `spacetime server` manages a named-server list in
  `cli.toml` (built-ins: `local` = `127.0.0.1:3000`, `maincloud`; **default = maincloud**).
- **`login`** (`login.rs`): browser flow against `https://spacetimedb.com` — request a token,
  open `…/login/cli?token=…`, poll `/api/auth/cli/status` 1 s until approved, exchange the web
  session for a SpacetimeDB token; both stored in `cli.toml`. Escape hatches: `--token`,
  `--server-issued-login` (direct `POST /v1/identity`), `--no-browser`.
- **`sql`/`call`/`logs`** are plain HTTP (`POST …/sql`, `POST …/call/{fn}`, `GET …/logs`);
  `call` fetches the schema first to validate args and suggest reducer names by edit distance
  (`edit_distance.rs`). `subscribe` is the only WS command. `sql --interactive` opens a
  `rustyline` REPL with syntect SQL highlighting.
- A layered project-config system (`spacetime_config.rs`, **3,212 LOC** — the biggest file in
  the CLI) supports `spacetime.json` + `spacetime.local.json` + env overlays, multi-target
  publish/generate, and glob database filtering.

---

## 7. `templates/` and `modules/` — quickstarts and the conformance (non-)story

### 7.1 Templates

25 templates at repo root: `basic-{rs,cs,ts,cpp}`, `chat-console-{rs,cs}`, `chat-react-ts`,
`browser-ts`, `nodejs-ts`, `bun-ts`, `deno-ts`, and framework-complete coverage
(`react/angular/vue/svelte/solid/nextjs/nuxt/remix/astro/tanstack`-ts), plus showcases
(`hangman-react-ts`, `llm-chat-ts`, `money-exchange-react-ts`). Mechanics:

- Templates are **embedded into the `spacetime` binary at build time**:
  `crates/cli/build.rs` reads `templates/templates-list.json` and `include_str!`s every file
  into a generated `embedded_templates.rs` consumed by `init.rs`. `spacetime init --template`
  also accepts `owner/repo` GitHub refs.
- Templates ship with **generated `module_bindings` committed**, so `init` → run works before
  the user ever invokes `generate`.
- Each template pairs a server module (default path `<project>/spacetimedb`) with a client, and
  `spacetime dev` picks both up by convention.

### 7.2 Test modules — hand-mirrored, not a shared corpus

38 modules under `modules/`. The SDK test story: `sdk-test` (Rust), `sdk-test-cs`,
`sdk-test-ts`, `sdk-test-cpp` are **hand-written mirrors of the same module in each language**,
with READMEs stating they "must be kept in sync" — the same for every feature variant
(`sdk-test-connect-disconnect-*`, `sdk-test-view-pk-*`, `sdk-test-procedure-*`, …). Assertions
are driven by a **single Rust test-client binary** using counter-style assertions against each
module, not by shared golden wire fixtures. There is **no cross-language conformance corpus** —
each SDK's behavior is validated by its own harness (C# snapshot tests, TS vitest, Unreal's
Rust harness) plus these mirrored modules.

This is the strongest validation of Fluxum's SDK-064/SPEC-013 bet: SpacetimeDB pays an O(SDKs ×
features) synchronization tax (visible in the 4-way duplicated module directories), and the
divergences it failed to catch are visible in shipped behavior — Unreal's stubbed brotli, TS's
missing one-off queries, per-SDK cache key strategies (bytes vs PK-or-row-object vs
ComparablePrimitive).

---

## 8. Answers to the key Fluxum questions

**Generated-code ↔ runtime boundary vs SDK-064.** SpacetimeDB already follows the thin-runtime
rule structurally: generated `DbConnection` is a trivial subclass; all protocol semantics
(diff application, refcounting, coalescing) live in the hand-written runtime; generated code
contributes only types, per-entity accessors, and *projections* (e.g. the `derive_pk` closure
passed to `with_updates_by_pk`, the `GetPrimaryKey` override in C#). Where it deviates from
Fluxum: (a) TS decoders are runtime-compiled from a generated type DSL rather than straight-line
generated code (vs SDK-041); (b) there is no schema_version handshake (vs SDK-043) — only a
codegen-tool version floor; (c) semantics *did* fork per language anyway (cache keying, apply
phasing) because nothing forces convergence — which is exactly what a shared corpus is for.

**Client cache details worth copying** (consolidated): BSATN-bytes row identity for PK-less
tables; per-row **refcount for overlapping subscriptions**; inserts-before-deletes apply order
with delete/insert cancellation (the join-driven `[delete r0, insert r0]` case is real);
update = PK-matched delete+insert post-pass; **mutate-all-then-callback-all** (C#'s three-phase
PreApply/Apply/PostApply is the cleanest form, and `OnBeforeDelete` is a nice extra hook);
insert-only event tables bypassing the cache; parse/decompress off the hot thread with an
in-order handoff (C# BlockingCollection pipeline, Unreal reorder buffer); callbacks registered
through the same serialized mutation queue as data (Rust `PendingMutation`) so registration
races are impossible. One warning: per-query row eviction is unsolvable with refcounts alone —
SpacetimeDB's TS SDK makes subscription errors *fatal* for this reason; Fluxum should decide
early whether SPEC-005 needs per-query row bookkeeping.

**TS browser story.** WebSocket-only, BSATN-only, native `DecompressionStream`, short-lived WS
token via one HTTP call, 30 KB-brotli minified browser bundle with CI size budgets, six
first-party framework bindings in one package, reconnection only in the framework layer.
Fluxum's Streamable-HTTP approach (SDK-080) has no counterpart here — SpacetimeDB never ran in
a WS-hostile environment, but also never solved proxies/HTTP-3; that remains open water.

**Why no Python/Go SDK.** Nothing in the tree even hints at them (no crates, no codegen
backends, no test modules). Structural reasons are visible: every SDK is a full reimplementation
of cache + BSATN + protocol (5–20k LOC each, kept in sync by hand), so each new language costs a
team; and their market (game engines: Unity → C#, Unreal → C++, web → TS, servers → Rust)
doesn't demand Python/Go. Fluxum's general-purpose positioning makes Python/Go the gap to own —
and the shared-corpus + thin-runtime architecture is what makes five languages affordable where
SpacetimeDB stopped at four.

**CLI DX lessons.** (1) `dev` (watch → regenerate → rebuild → republish → stream logs → run
client) is the single highest-leverage command; Fluxum has no equivalent specced. (2) Embedding
templates in the binary with committed bindings makes `init` work offline and instantly.
(3) Stale-generated-file GC via a magic header prefix. (4) UNSTABLE warnings let commands ship
early. (5) Language auto-detection and per-language default output dirs remove flags from the
happy path. (6) Generate-from-artifact forces module toolchains onto client machines — Fluxum's
generate-from-`/schema` is the right call; keep a `--schema file.json` offline path (they
retrofitted one as hidden `--module-def`). (7) `call` validating args against the fetched schema
with edit-distance suggestions is cheap and delightful.

---

## What Fluxum will face

1. **The cache is the SDK.** Transport and codegen are the easy 30%; the client cache
   (refcounts, apply ordering, coalescing, callback visibility, thread handoff) is where
   SpacetimeDB spent its complexity budget in *every* language, and where its languages
   silently diverged. Fluxum must write the cache semantics down as normative,
   fixture-testable rules (SPEC-013) *before* the second SDK exists — SpacetimeDB shows what
   happens otherwise: four cache implementations, three keying strategies, one fatal-error
   workaround.

2. **Overlapping subscriptions force refcounted rows — decide now.** SDK-040's
   `Map<pk, Row>` sketch is insufficient the moment two subscriptions can return the same row.
   Fluxum needs either SpacetimeDB-style refcounts (cheap, but makes per-query unsubscribe
   eviction impossible without server help) or per-query row sets (heavier, but unsubscribe
   and partial-error recovery become trivial). SpacetimeDB is stuck mid-migration on this
   ("Once we have per-query storage…"); choosing per-query bookkeeping in SPEC-005/SDK-040 up
   front avoids their fatal-subscription-error wart.

3. **PK-less tables and float columns will break naive keying.** Fluxum's SDK-040 assumes a
   primary key. SpacetimeDB's answer — identity = serialized row bytes — is elegant and
   Fluxum should adopt it for FluxBIN (row bytes as cache key when no PK; PK projection
   otherwise), since it sidesteps float hashing and composite-key encoding in one move.

4. **Reconnection is genuinely hard, which is why SpacetimeDB doesn't do it.** No SDK
   auto-reconnects at the core layer; only the TS framework wrapper retries, and none of them
   resubscribe or reconcile the stale cache. Fluxum's SDK-082 (backoff + re-auth +
   resubscription in the core runtime, identically across environments) is a real
   differentiator — but it must specify what happens to the cache and to event callbacks on
   re-`InitialData` (clear-and-replay? diff against stale state?). Nobody has prior art here;
   budget for it.

5. **The five-language promise is a factory problem.** SpacetimeDB's per-language cost
   (5k–20k LOC runtimes, hand-mirrored test modules, 7k LOC for one engine backend) is the
   cautionary tale. Fluxum's mitigations — thin runtimes (SDK-064), generated straight-line
   decoders instead of per-language codec libraries (SDK-041), one shared wire-fixture corpus
   (SPEC-013) — are correct, but the corpus must cover *cache application scenarios*
   (insert-before-delete, join-driven delete/insert of the same row, overlap refcounts,
   update coalescing), not just encode/decode round-trips, or the divergence will happen in
   exactly the same place theirs did.

6. **Bundle-size and browser claims need CI teeth.** SpacetimeDB asserts 30 KB brotli in CI via
   size-limit and still ships a runtime-interpreted type system to get there. Fluxum's 50 KB
   gzip budget (SDK-083) is achievable with straight-line generated decoders only if the
   runtime stays free of a type-interpretation layer — enforce the size gate from the first
   commit, and add headless-Chromium conformance runs (SDK-084) early, since the
   `DecompressionStream('brotli')` portability trap SpacetimeDB hit is exactly the kind of
   thing only a real-browser CI catches.

7. **A `fluxum dev` command should be specced.** The largest DX asset in SpacetimeDB's CLI is
   not any protocol feature — it's the 2,100-line `dev` loop plus embedded templates. SPEC-011
   covers `generate` and `schema export` but no inner-loop command; matching `spacetime dev`
   (watch → regenerate → republish → tail logs → run client) is table stakes for the
   quickstart experience Fluxum's demo app (T6.5) will be judged by.
