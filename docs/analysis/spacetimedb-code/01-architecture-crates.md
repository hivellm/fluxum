# SpacetimeDB Implementation Analysis — 01: Architecture & Crate Map

| | |
|---|---|
| **Source repo** | github.com/clockworklabs/SpacetimeDB |
| **Version** | 2.7.0 |
| **Commit** | `1a8df2a` |
| **Date** | 2026-07-13 (analyzed 2026-07-14) |

**Fluxum relevance:** SpacetimeDB is the direct reference for Fluxum's "database that is also a server" model. Its 45-crate workspace (~237k Rust LOC in `crates/` alone, plus ~91k LOC of C++/C#/TypeScript module bindings) shows exactly where a 6-crate plan will come under pressure, which boundaries were drawn early vs. carved out painfully later, and which whole subsystems Fluxum's native-module decision deletes.

---

## 1. Workspace shape

- **Toolchain** (`rust-toolchain.toml`): pinned stable `1.93.0`, edition **2024**, `rust-version = "1.93.0"` in `[workspace.package]`, extra target `wasm32-unknown-unknown` (modules are built by the same toolchain), `rust-src` component.
- **Resolver**: `resolver = "3"` set explicitly (workspace manifests have no edition).
- **Members**: 40 crates listed in `Cargo.toml` members + implicit path-dep members (e.g. `crates/codegen` is pulled in via `crates/cli`), plus `sdks/rust`, `sdks/unreal`, 14 test/benchmark modules under `modules/`, 2 templates, 10 SDK test clients, and 9 `tools/*` crates. `default-members = ["crates/cli", "crates/standalone", "crates/update"]` — a plain `cargo build` produces exactly the three shipped binaries.
- **Dependency centralization**: every external dep is in `[workspace.dependencies]` (200+ entries), including every internal crate pinned `=2.7.0` — internal crates are also published to crates.io, so paths carry exact versions. Notable pins: `v8 = "=145.0.0"` (Nix flake hashes depend on it), `wasmtime = "39"` (default-features off, cranelift + pooling + async), `deno_core_icudata` kept in lockstep with V8's ICU.
- **Profiles**: `release` uses `lto = "thin"`, `codegen-units = 16`, `panic = "unwind"` (reducer panics must unwind, not abort); `test` uses `opt-level = 1`; a custom **`profiling`** profile = release + debug symbols for flamegraphs/Tracy.
- **Lints**: `[workspace.lints]` is thin (`unexpected_cfgs` check for `tokio_unstable`, `clippy::result_large_err = allow`), but `clippy.toml` bans `print!/println!/eprint!/eprintln!/dbg!` workspace-wide via `disallowed-macros` — everything must go through `log`/`tracing` so output is filterable.
- **Polyglot root**: `pnpm-workspace.yaml` + `package.json` + `tsconfig.json` + `eslint.config.js` (TypeScript bindings/SDK), `global.json` (C# SDK), `flake.nix` (Nix builds), `git-hooks/`, `Dockerfile` + `docker-compose.yml`.

---

## 2. Crate map (all 45 under `crates/`)

LOC = physical lines in `.rs` files (incl. tests/comments); bindings-cpp/csharp/typescript counted in their own languages. Deps column lists **internal** crates only (dev-deps marked).

### Foundation (no internal deps, or leaf-only)

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `primitives` | 1,400 | Copy-type IDs (`TableId`, `ColId`, `IndexId`, `SequenceId`… in `crates/primitives/src/ids.rs`), inline-packed `ColList`, constraint/attr flags, errno table. The bottom of everything. | memory-usage |
| `memory-usage` | 149 | Single trait `MemoryUsage::heap_usage()` implemented across the tree for heap accounting/metering (`crates/memory-usage/src/lib.rs`). | — |
| `data-structures` | 3,270 | Assorted containers: `error_stream.rs` (accumulate many errors instead of failing on first — used heavily by schema validation), `map.rs` (hash wrappers), `object_pool.rs`, `slim_slice.rs`, `small_map.rs`, `nstr.rs`. | memory-usage |
| `metrics` | 201 | Prometheus macro helpers (typed metric group declarations). | — |
| `paths` | 946 | The **entire on-disk directory layout as a type hierarchy** — every path (commitlog dir, snapshot dir, module logs…) is a distinct newtype, so functions can't be handed the wrong directory. | — |
| `fs-utils` | 1,070 | Filesystem helpers (lockfiles, atomic renames, compression helpers). | — |
| `runtime` | 2,597 | **Not the module runtime.** The determinism boundary for DST: `Handle::Tokio` vs `Handle::Simulation`, a single-threaded `no_std+alloc` simulator (`src/sim/`: executor, virtual time, seeded RNG, `buggify` fault injection, node scheduling), `sync` wrappers, `check_determinism` replay harness. See `crates/runtime/README.md` and `DETERMINISM_COVERAGE.md`. | — |
| `guard` | 495 | Test-only process guard: finds the built CLI/standalone binaries, spawns a server on a `portpicker` port, waits for HTTP readiness, correlates logs by spawn ID (`crates/guard/src/lib.rs`). Used by smoketests/testing. | — |

### Type system & schema

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `sats` | 14,150 | **S**pacetime **A**lgebraic **T**ype **S**ystem: `AlgebraicType/Value`, BSATN binary codec, JSON codec, type-space, `u256`/`i256`. The value layer shared by server, modules, and clients. | bindings-macro (derives!), primitives, memory-usage, metrics |
| `lib` | 5,481 | The "common" grab-bag: `Identity`, `ConnectionId`, `Timestamp`, `Address`, module-def raw types, operator types. Sits above sats; nearly everything depends on it. | sats, bindings-macro, primitives, metrics, memory-usage |
| `schema` | 15,653 | Validated schema layer: `ModuleDef` → `TableSchema`/indexes/constraints/sequences, schema validation & auto-migration planning (dev-deps: testing). | lib, sats, primitives, data-structures, sql-parser |
| `client-api-messages` | 2,624 | Wire schema of the WebSocket/HTTP client protocol (BSATN + JSON forms), energy/db-update messages. Shared with the Rust SDK. | lib, sats, primitives |

### Storage engine

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `table` | 21,100 | In-memory row storage: 64 KiB pages + page pool, BFLATN row format (`bflatn_from/to.rs`), var-len blob store, pointer map, btree/unique/direct indexes (`table_index/`), static row layouts, row hashing. The performance core. | schema, sats, lib, primitives, data-structures, memory-usage |
| `commitlog` | 9,155 | Segmented append-only commit log: varint framing, CRC, offset index (`index/`), segment repo abstraction (`repo/`), async streaming (`stream/`), fallocate support. Generic over payload — reusable standalone. | sats, paths, fs-utils, primitives |
| `durability` | 1,070 | `Durability` trait (append TX, offset watermarks) + single-node `Local` impl over commitlog; runtime-aware for DST. | commitlog, paths, fs-utils, runtime, sats |
| `snapshot` | 2,954 | On-disk snapshot capture/restore of datastore state, dedup'd by blob hash, zstd-framed; used for fast restart + log truncation. | table, datastore(core), durability, paths, runtime, … |
| `datastore` | 15,634 | The transactional datastore: `locking_tx_datastore/` (MVCC-ish tx machine: committed state + tx state + locks), system tables, `execution_context.rs` (workload classification), traits, metrics. | table, commitlog, durability, snapshot, schema, sats, lib |
| `engine` | 6,767 | **`RelationalDB`** (`crates/engine/src/relational_db.rs`): ties datastore + durability + snapshots + commitlog into one database object; persistence orchestration, restore-from-snapshot/replay, size-on-disk resources, metrics-recorder actor, SQL glue (`src/sql/`), auto-update of database schema (`src/update.rs`). Carved out of `core` so DST can drive a real database without the host stack. | datastore, table, commitlog, durability, snapshot, runtime, schema, expr |

### Query pipeline (5 crates + parser)

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `sql-parser` | 1,638 | SQL AST + parser (two dialects: subscription SQL and general SQL), built on `sqlparser`. | lib |
| `expr` | 2,917 | Typed logical expression tree + type-checking of parsed SQL against schema (dev-dep: bindings). | sql-parser, schema, lib, sats, primitives, data-structures |
| `physical-plan` | 3,645 | Physical plan representation + rewrites (index selection, join ordering). | expr, sql-parser, schema, table, lib |
| `execution` | 1,664 | Iterators/executors over physical plans against `table` (the actual query engine loop). | physical-plan, expr, table, sql-parser, sats |
| `query` | 101 | Tiny façade: "top level crate for invoking the query engine and optimizer" — compile pipeline entry point. | execution, physical-plan, expr, sql-parser, table, schema |
| `subscription` | 689 | Subscription compiler: turns a subscription query into `SubscriptionPlan`s with pruning metadata for incremental eval. | query, execution, physical-plan, expr, schema |
| `query-builder` | 965 | Typed Rust query-builder DSL (used by module authors, e.g. for views/RLS). | lib |

### Host & network

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `core` | 41,483 | The kitchen-sink host: **module hosts** (`src/host/`: wasmtime backend, V8/JS backend, scheduler, host controller), client connections + message handlers v1–v3 (`src/client/`), subscription actor/manager + delta eval (`src/subscription/`), energy accounting (`src/energy.rs`), database logger, SQL entry points (`src/sql/`), replica context. Everything above engine and below HTTP. | engine, datastore, query, subscription, client-api-messages, commitlog, durability, snapshot, runtime, auth, … (24 internal deps) |
| `auth` | 214 | JWT/OIDC token validation helpers (`jsonwebtoken`). | lib, data-structures |
| `client-api` | 7,101 | Axum HTTP + WebSocket API: routes for database CRUD/call/sql/subscribe, identity, energy, metrics, health (`src/routes/`). Generic over a `ControlStateDelegate` trait so the same router serves standalone and (closed-source) cloud. | core, client-api-messages, datastore, auth, lib, paths |
| `pg` | 714 | **Postgres wire-protocol front-end** built on `pgwire`: `src/pg_server.rs` (`start_pg` on a `TcpListener`, auth via spacetime tokens), `src/encoder.rs` (SATS→pg type/value mapping). Lets `psql` and BI tools query SpacetimeDB directly. | client-api, client-api-messages, auth, lib |
| `standalone` | 2,330 | The `spacetimedb-standalone` binary: single-node server; control-plane database (`control_db.rs`, sled-backed), wires client-api + pg + core host controller; subcommands (start, extract-schema). | client-api, core, pg, datastore, paths |

### Module authoring (guest side) & codegen

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `bindings-sys` | 1,658 | Raw WASM ABI: `extern "C"` imports of the host syscall surface (`spacetime_10.0` ABI), buffer/iterator handles. | primitives |
| `bindings-macro` | 3,430 | Proc-macros: `#[table]`, `#[reducer]`, `#[view]`, SATS derives (`SpacetimeType`). Note: it is a **dependency of `sats` and `lib`** — the derive macros live at the bottom of the graph, not the top. | primitives |
| `bindings` | 6,752 | The `spacetimedb` crate module authors depend on: typed table handles, reducer context, iterators, log integration, view/procedure API. Compiled to `wasm32-unknown-unknown`. | bindings-macro, bindings-sys, lib, primitives, query-builder |
| `codegen` | 13,582 | Client/module codegen from `ModuleDef` for **Rust, C#, TypeScript, C++, Unreal C++** (`src/{rust,csharp,typescript,cpp,unrealcpp}.rs`). Used by `spacetime generate`. | schema, lib, primitives, data-structures |
| `bindings-cpp` | ~29,400 (C++) | C++ module bindings: headers + sources + CMake, own ARCHITECTURE/REFERENCE docs. Not a Rust crate. | — |
| `bindings-csharp` | ~30,900 (C#) | C# module bindings: `BSATN.Codegen`, `BSATN.Runtime`, Roslyn source generators (`Codegen/`), NativeAOT-LLVM notes. Not a Rust crate. | — |
| `bindings-typescript` | ~31,100 (TS) | TypeScript **module** bindings + framework adapters (`src/{server,react,angular,solid,svelte,tanstack}/`). Its `test-*/server` dirs are tiny Rust `cdylib` shims that are workspace members. | — |

### CLI, ops & test infrastructure

| Crate | LOC | Purpose | Key internal deps |
|---|---|---|---|
| `cli` | 18,744 | `spacetime` CLI: init/build/publish/call/sql/subscribe/generate/login, module build orchestration (Rust→wasm, TS via bundled **rolldown**, C#), config management. | codegen, auth, client-api-messages, schema, lib, paths, fs-utils |
| `update` | 1,350 | Rustup-style **multicall binary**: `spacetime` argv0 dispatch → version manager (`spacetimedb-update`), proxies subcommands to the per-version CLI binary (`src/proxy.rs`), self-install (`spacetime-install.sh/.ps1`), update notices. A default-member; it *is* the `spacetime` users install. | paths |
| `testing` | 1,483 | Test harness lib: compile test modules, boot in-memory server configs (`CompiledModule`, `start_runtime`). | core, standalone, cli, client-api, guard |
| `smoketests` | 14,822 (117 files) | End-to-end smoke tests (Rust port of former Python suite): drives real server + CLI via `guard`. Own excluded sub-workspace `crates/smoketests/modules`. | core, guard |
| `sqltest` | 1,038 | `sqllogictest`-based SQL conformance runner (compares vs sqlite/postgres engines). | core, lib, sats |
| `bench` | 3,886 | Criterion + callgrind benchmark suite (generic DB ops, special workloads), vs-sqlite comparisons. | core, datastore, execution, query, table, standalone, testing… |
| `index-scan-gate` | 82 | A single-file CI **performance gate binary**: compiles the `perf-test` module, runs 4 index-scan reducers 31× (5 warmups) and fails if the median exceeds 100 µs (`crates/index-scan-gate/src/main.rs`). | testing, sats |
| `dst` | 2,186 | Deterministic-simulation test suites (`spacetimedb-dst-lib`): generic `TargetDriver`/`Properties`/`TestSuite` traits (`src/traits.rs`), an engine workload model + properties (`src/engine/`), simulated commitlog (`src/sim/`). Drives `engine` under `runtime` with `features = ["simulation"]`. | engine, datastore, commitlog, durability, runtime(sim), table, schema |

Total: ~237k Rust LOC under `crates/`, plus ~91k LOC of non-Rust bindings, plus `sdks/` (Rust, C#, TypeScript, Unreal client SDKs — the Rust and Unreal ones are workspace members).

---

## 3. Layered dependency diagram (major crates)

Arrows point downward = "depends on". Dev-dep cycles (schema→testing, expr→bindings, codegen→testing) are allowed by Cargo and used deliberately; the *normal* graph is a strict DAG.

```
                 BINARIES ──────────────────────────────────────────────
                 standalone          cli            update (spacetime launcher)
                    │  │              │                │
                    │  └────────┐     ├── codegen      └── paths
        ┌───────────┤           │     │   (rust/cs/ts/cpp/unreal)
        ▼           ▼           ▼     ▼
       pg ───► client-api    auth   schema
 (pgwire front)     │
                    ▼
 HOST ───────────  core  ◄──────────────────────────── testing / smoketests / bench
 (module hosts:     │ │ │                                    (via guard: spawns real
  wasmtime + V8,    │ │ │                                     server processes)
  clients, subs     │ │ └───────────────┐
  actor, energy)    │ │                 │
                    │ │                 ▼
                    │ │        QUERY PIPELINE
                    │ │        subscription ─► query ─► execution ─► physical-plan
                    │ │                                                │
                    │ │                                                ▼
                    │ │                                       expr ─► sql-parser
                    ▼ ▼
 DB ENGINE ──────  engine (RelationalDB) ◄─── dst (deterministic sim tests)
                    │        │
        ┌───────────┤        └── runtime (Tokio ⟷ deterministic simulator)
        ▼           ▼
 TX STORE ───────  datastore ──► snapshot
                    │    │           │
                    ▼    ▼           ▼
 ROW STORE ──────  table    commitlog ◄── durability
                    │           │
                    ▼           ▼
 SCHEMA/TYPES ───  schema ─► lib ─► sats ──► bindings-macro (derives)
                    │                 │
                    ▼                 ▼
 FOUNDATION ─────  primitives · data-structures · memory-usage · metrics · paths · fs-utils

 MODULE GUEST (wasm32) ──  bindings ─► bindings-sys / bindings-macro / query-builder / lib
 WIRE SCHEMA ────────────  client-api-messages ─► lib/sats   (shared with sdks/rust)
```

Layering rules visible in the graph: proc-macros and IDs at the very bottom; value types (`sats`) below schema; row storage (`table`) below transactions (`datastore`) below database (`engine`) below host (`core`) below HTTP (`client-api`) below binaries; the query pipeline is a parallel stack that touches `table` but not `datastore`; test infra sits on top of everything and reaches the server only through spawned processes (`guard`).

---

## 4. Module-language story

**Two in-process hosts, both inside `core` — there is no separate "wasm-host" crate:**

- **WASM host** — `crates/core/src/host/wasmtime/` (wasmtime 39, pooling allocator, `wasm_instance_env.rs` implements the syscall surface) plus `crates/core/src/host/wasm_common/` (ABI versioning `abi.rs`, `module_host_actor.rs`, instrumentation). Rust/C#/C++ modules compile to `wasm32-unknown-unknown` against `bindings`/`bindings-csharp`/`bindings-cpp`.
- **V8 host** — `crates/core/src/host/v8/` (rusty_v8 pinned `=145.0.0`). TypeScript modules are bundled by the CLI (rolldown) and run in V8 isolates. The topology (documented in `crates/core/src/host/v8/mod.rs`) has two lanes: a **single-threaded main lane** (one mpsc queue → one OS thread → one isolate) for reducers/subscriptions/one-off queries, and a **bounded pool of procedure isolates** for async procedures; isolates are replaced inline after traps/heap retirement. Per-call budgets in `budget.rs`, serde bridges in `de.rs`/`ser.rs`/`to_value.rs`/`from_value.rs`, syscalls in `syscall/`.
- Both hosts plug into a common `ModuleHost` abstraction (`src/host/module_host.rs`, `module_common.rs`, `host_controller.rs`) — this trait seam is what made adding a second language runtime tractable.
- **Note:** the crate named `runtime` is *not* this — it is the deterministic-simulation boundary (§2). The module runtimes live in `core`.

**Guest-side surface** is huge: `bindings`(+`-sys`,`-macro`) for Rust, `bindings-csharp` (Roslyn codegen + BSATN runtime), `bindings-cpp` (CMake package), `bindings-typescript` (server API + React/Angular/Solid/Svelte/TanStack client adapters). `codegen` generates client types/reducer stubs for 5 targets from a `ModuleDef`.

---

## 5. Non-Rust surface

- `modules/` — ~38 test/benchmark modules in 4 languages (`sdk-test`, `sdk-test-cs`, `sdk-test-ts`, `sdk-test-cpp`, procedure/view/pk variants, `perf-test`, `benchmarks*`, `keynote-benchmarks`). Rust ones are workspace members; every SDK feature is tested in every module language.
- `templates/` — 25 starter templates (`basic-{rs,cs,cpp,ts}`, `chat-console-*`, plus framework-specific TS templates: react, nextjs, nuxt, svelte, solid, astro, angular, remix, tanstack, deno, bun, llm-chat…). This is the `spacetime init` catalog — onboarding is a product surface.
- `tools/` — 9 internal Rust tools (workspace members): `ci`, `release`, `regen` (snapshot regen), `gen-bindings`, `generate-client-api` (wire-schema codegen), `license-check`, `keynote-bench-harness`, `xtask-llm-benchmark` (LLM one-shot module-writing benchmark), plus shell scripts (`perf.sh`, `publish-crates.sh`).
- `demo/Blackholio/` — the flagship multiplayer game demo kept in-tree.
- `sdks/` — client SDKs for Rust (workspace member), C#, TypeScript, Unreal; 10 Rust test-client crates paired with `modules/sdk-test-*`.
- `skills/` — in-repo agent/LLM skill definitions.

---

## 6. Comparison with Fluxum's planned 6-crate workspace

Fluxum plan (`E:\HiveLLM\Fluxum\docs\ARCHITECTURE.md` §Workspace layout): `fluxum-core`, `fluxum-macros`, `fluxum-protocol`, `fluxum-server`, `fluxum-cli`, `fluxum-bench`.

### Mapping and split pressure

| Fluxum crate | SpacetimeDB equivalent(s) | Split pressure |
|---|---|---|
| `fluxum-core` | primitives + data-structures + memory-usage + sats + lib + schema + table + commitlog + durability + snapshot + datastore + engine + sql-parser + expr + physical-plan + execution + query + subscription + core(runtime parts) ≈ **19 crates, ~150k LOC** | **Certain to split.** The first fault lines to draw *now*, even if only as modules with enforced one-way imports: (1) **types/values** (Identity, FluxValue, FluxBIN) — must be usable by `fluxum-macros`, `fluxum-protocol`, and SDKs without dragging storage in; (2) **commitlog** — SpacetimeDB's is payload-generic and fully reusable; (3) **row store vs. transaction machine** (`table` vs `datastore`) — the perf-critical page/index layer has a different change cadence than tx logic; (4) **query pipeline** — parser → typed logical → physical → execution as separate layers, since subscriptions, one-off SQL, RLS, and the pg front-end all re-enter it at different stages. |
| `fluxum-macros` | bindings-macro | Matches 1:1 — but note SpacetimeDB puts the macro crate **below `sats`** so value types can `#[derive(SpacetimeType)]`. `fluxum-macros` must similarly avoid depending on `fluxum-core`; it should depend only on a tiny types crate (or nothing) and emit registry entries. |
| `fluxum-protocol` | client-api-messages (+ the codec half of sats) | Holds as one crate. SpacetimeDB keeps wire schema separate from HTTP server precisely so SDKs (`sdks/rust`) share it — Fluxum's plan already has this right. |
| `fluxum-server` | client-api + core(host/clients/subscription-actor) + standalone + pg + auth ≈ 51k LOC | Will split at least into "session/subscription runtime" vs "transport" once a second front-end appears (Fluxum plans TCP + Streamable HTTP from day one; SpacetimeDB's `pg` shows a third front-end costs only ~700 LOC *if* the API layer is generic over a context trait like `client-api`'s delegate). `auth` as a leaf crate (214 LOC) is cheap and keeps JWT deps out of the core graph. |
| `fluxum-cli` | cli + codegen + update ≈ 33k LOC | `codegen` (client SDK generation for TS/Python/Go/C#) should be a separate lib from day one — SpacetimeDB's is 13.6k LOC and also consumed by tools/tests, not just the CLI. A `spacetime update`-style self-updating multicall launcher is a separate small binary concern. |
| `fluxum-bench` | bench + sqltest + index-scan-gate + smoketests + testing + guard + dst ≈ **7 crates, ~24k LOC** | Fluxum's parity-harness plan covers `bench`; the rest (process-guard e2e tests, SQL logic tests, CI perf gates, deterministic simulation) are separate concerns that will not fit in a bench crate. |

### SpacetimeDB crates with no Fluxum equivalent — and why

- **`bindings`, `bindings-sys`, `bindings-cpp`, `bindings-csharp`, `bindings-typescript` (~70k LOC + ABI)** — the guest side of the WASM/V8 sandbox. Fluxum's native-module model (app = Rust crate linked into the server binary) deletes the entire guest ABI, the syscall surface, per-language module bindings, and FFI marshaling. This is the single biggest scope reduction Fluxum buys — but it also deletes multi-language *modules*; only Rust apps exist.
- **`core/src/host/wasmtime` + `core/src/host/v8`** — no sandbox hosts. Fluxum's equivalent is a dispatch table + `catch_unwind`; the lesson to keep is the `ModuleHost` **trait seam**: reducer dispatch, energy/budget, and scheduling behind one interface, so a future sandboxed backend (or out-of-process module) remains possible.
- **`update`** — version-manager/proxy makes sense when the CLI drives a hosted server; with Fluxum, the app binary *is* the deployment, so a launcher that multiplexes CLI versions is unnecessary (a plain self-update may still be wanted).
- **`energy` (in core)** — metering exists to bill/limit untrusted sandboxed modules; native trusted reducers don't need per-instruction budgets (but do need per-reducer wall-clock timeouts, which Fluxum plans via the tx layer).
- **wasm32 toolchain target, rolldown bundler in CLI, module templates in 4 languages** — all sandbox-driven.

### Boundary lessons worth adopting early

1. **Macros at the bottom, not the top.** `bindings-macro → primitives` only; `sats` and `lib` *consume* the macro crate. If `fluxum-macros` ever needs `fluxum-core` types, the graph deadlocks.
2. **A `paths`-style typed filesystem layout crate** (946 LOC) eliminates a whole class of ops bugs and is nearly free.
3. **`memory-usage` as a 149-LOC leaf trait** lets every layer report heap usage without a metrics dependency — Fluxum's memory-budget/eviction design (SPEC-015) needs exactly this and should define it before the buffer pool, not after.
4. **`error_stream` (in data-structures)** — schema validation that reports *all* errors, not the first one. Do this before writing the schema validator.
5. **Determinism boundary (`runtime`) must exist before the code that needs it.** SpacetimeDB is retrofitting DST (`DETERMINISM_COVERAGE.md` tracks what still leaks host behavior); every direct `tokio::spawn`/`Instant::now`/`rand` call is later debt. Fluxum should route time/rng/spawn through a handle from the first commit of `fluxum-core`.
6. **Engine vs host split.** `engine` (RelationalDB) was extracted from `core` late so DST could drive a database without the network/host stack. Fluxum should keep "open a database, run a tx, subscribe to changes" fully constructible without `fluxum-server`.
7. **Dev-dep cycles are a feature**: `schema` dev-depends on `testing`, `expr` dev-depends on `bindings` — integration-grade tests live next to the code without polluting the normal DAG.
8. **API layer generic over a control-plane trait** (`client-api`'s delegate) is how one router serves standalone and cloud; Fluxum's single-binary model may want the same seam for embedded vs. clustered.

---

## What Fluxum will face

- **Scale gap**: matching SpacetimeDB's server feature set is ~237k Rust LOC across 45 crates; the 6-crate plan is a starting topology, not an end state. Plan for `fluxum-core` to fracture along types / commitlog / row-store / tx / query / subscription seams — draw those as internal module boundaries with enforced one-way imports on day one, so later crate extraction is `git mv`, not surgery.
- **The query pipeline alone is 5 crates / ~10k LOC** (`sql-parser`→`expr`→`physical-plan`→`execution`→`query`, + `subscription` compiler) *for a deliberately small SQL subset*. Fluxum's subscription SQL compiler is the same shape of work; budget for it as a subsystem, not a module.
- **The row store is the biggest single investment**: `table` is 21k LOC of pages, BFLATN layout, blob store, and index machinery — and Fluxum additionally needs the paged cold tier + eviction SpacetimeDB doesn't have. Expect `table`+`pager` to be Fluxum's largest component.
- **Native modules delete ~70k LOC of guest bindings + two sandbox hosts + energy metering** — the decisive scope win — but client-side codegen remains: SpacetimeDB generates for 5 targets (13.6k LOC `codegen`); Fluxum's 4 planned SDK languages imply a comparable generator that should be a library, not CLI-internal code.
- **Keep a `ModuleHost`-like trait seam** around reducer dispatch even with native modules; it's what let SpacetimeDB add V8 alongside WASM without touching the engine, and it's Fluxum's escape hatch if sandboxed/out-of-process modules ever return.
- **DST is a before-the-fact architecture decision**: SpacetimeDB's `runtime` handle + `dst` suites + `check_determinism` replay require every core crate to take time/rng/spawn by injection. Retrofit is visibly painful (their coverage doc tracks leaks). Fluxum should ship the equivalent of `runtime::Handle` in 0.1.
- **Test infrastructure is ~7 crates / ~24k LOC**: process-guard e2e (`guard`+`smoketests`), sqllogictest conformance (`sqltest`), CI perf gates with hard µs thresholds (`index-scan-gate`), criterion+callgrind benches, DST. The planned `fluxum-bench` covers one of these five categories.
- **A Postgres wire front-end is cheap once the query engine exists** (`pg` is 714 LOC on `pgwire`) and instantly unlocks psql/BI tooling — worth a slot on Fluxum's roadmap after SQL lands.
- **Workspace hygiene to copy**: pinned toolchain + edition 2024 + resolver 3; all deps in `[workspace.dependencies]`; `default-members` = shipped binaries; a `profiling` profile (release+debug); `clippy.toml` `disallowed-macros` banning `println!`; `panic = "unwind"` in release because reducer panics must roll back, not abort.
- **Onboarding is a product surface**: 25 templates, 38 test modules, and a flagship demo live in-tree and are CI'd. Fluxum's "3-line main.rs" story needs the same treatment — templates and example apps as workspace members from early on.
