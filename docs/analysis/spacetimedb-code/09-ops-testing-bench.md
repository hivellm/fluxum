# SpacetimeDB Code Analysis — 09: Operations, Testing Infrastructure, Benchmarks

Deep implementation analysis of the real SpacetimeDB source, focused on the standalone server
(what a single node actually wires together and what is visibly missing vs. their cloud), the
self-updating CLI, the metrics/telemetry stack, module logging, the smoketest harness, the
deterministic-simulation-testing (DST) crate, the SQL:2016 conformance runner, the criterion/
callgrind benchmark suite, CI workflows, and the Docker deployment story. Written to inform
Fluxum's SPEC-012 (observability), SPEC-013 (testing & conformance, incl. the PostgreSQL parity
harness NFR-11/TST-090…096) and SPEC-014 (replication).

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` |
| **Crates analyzed** | `crates/standalone`, `crates/update`, `crates/metrics`, `crates/core/src/worker_metrics`, `crates/datastore/src/db_metrics`, `crates/engine/src/metrics.rs`, `crates/core/src/database_logger.rs`, `crates/core/src/startup.rs`, `crates/smoketests` (14,822 LoC of Rust), `crates/dst`, `crates/runtime/src/sim`, `crates/sqltest`, `crates/bench`, `crates/testing`, `TESTING.md`, `.github/workflows/*`, `Dockerfile`, `docker-compose.yml` |
| **Date analyzed** | 2026-07-14 |

---

## 1. `crates/standalone` — the single-node server

`spacetimedb-standalone` is a thin composition root (~1,300 LoC across `src/lib.rs`,
`src/main.rs`, `src/subcommands/start.rs`, `src/control_db.rs`). `StandaloneEnv::init`
(`src/lib.rs:63`) wires together:

- **`ControlDb`** (`src/control_db.rs`) — a **sled** key-value store at
  `{data-dir}/control-db` holding the control-plane records: `Database`, `Replica`, `Node`,
  energy balances, TLD/domain-name registrations, and database locks. Values are BSATN-encoded;
  identities stored little-endian.
- **`HostController`** (from `crates/core`) — module lifecycle, with a
  `LocalPersistenceProvider` (commitlog + snapshots on local disk), `NullEnergyMonitor`
  (energy metering is stubbed in OSS — `withdraw_energy` is a no-op with the comment "The
  energy balance code is obsolete"), a `DiskStorage` program store (`{data-dir}/program-bytes`),
  and `JobCores` for core pinning.
- **Prometheus registry** — registers exactly four metric groups: `WORKER_METRICS`,
  `ENGINE_METRICS`, `DB_METRICS`, `DATA_SIZE_METRICS` (`src/lib.rs:98-102`).
- **JWT `CertificateAuthority`** — self-issued ECDSA keys (`get_or_create_keys`), the
  standalone auth provider.
- **`PidFile`** — data-dir lock; a second `StandaloneEnv::init` on the same dir fails (tested
  in `ensure_init_grabs_lock`).
- **`metadata.toml`** — records `edition = "standalone"` and version; refuses to open a data
  dir written by an incompatible edition/version (`MetadataFile::check_compatibility_and_update`).

**Config**: `spacetime start --data-dir <dir>` (`src/subcommands/start.rs`) with flags
`--listen-addr` (default `0.0.0.0:3000`), `--in-memory`, `--page_pool_max_size` (default 8 GiB),
`--pg-port` (opt-in **Postgres wire protocol server**, `spacetimedb_pg::pg_server`),
`--jwt-{pub,priv}-key-path`, `--enable-tracy`. On first start it materializes a default
`config.toml` into the data dir (template at `crates/standalone/config.toml`) with sections
`[logs]` (env-filter directives, hot-reloaded in debug builds), `[websocket]`
(idle-timeout, close-handshake-timeout), `[wasm]`/`[v8]` (procedure instance pool sizes, V8
heap policy), and `[commitlog]` (segment size, offset-index interval, fsync requirements,
preallocation, write buffer). Interactive port-conflict resolution (offers the next free port
unless `--non-interactive`).

**Data-dir layout** (`crates/paths/src/server.rs`, `path_type!` newtypes):

```
{data-dir}/
  config.toml, metadata.toml, spacetime.pid
  logs/                       # server tracing logs, daily-rolled
  control-db/                 # sled
  program-bytes/              # content-addressed module WASM/JS
  cache/wasmtime/             # compiled-artifact cache
  replicas/{replica_id}/      # per-database state — note the plural name
    clog/                     # commitlog segments
    snapshots/{offset:020}.snapshot_dir/
    logs/                     # module logs, ModuleLogsDir, daily files
```

The `replicas/$replica_id/` directory name is the single clearest cloud fingerprint in the
on-disk format: every database is stored as "replica N" even though standalone only ever
creates one.

### 1.1 Replication is vocabulary, not machinery, in OSS

The control-plane **schema** is replication-shaped, but the **implementation** is hard-wired to
one node and one replica:

- `crates/core/src/messages/control_db.rs` defines `Replica { id, database_id, node_id, leader:
  bool }`, `Node { id, unschedulable, advertise_addr, pg_addr }`, `ReplicaStatus`, `NodeStatus`
  (with a comment linking to the Kubernetes NodeStatus API as the model).
- `crates/core/src/messages/control_worker_api.rs` defines the **control-node ↔ worker-node
  protocol** (`WorkerBoundMessage::ScheduleState/ScheduleUpdate`, `ControlBoundMessage::
  EnergyWithdrawals`) — schedule state is a full `{replicas, databases, nodes}` snapshot plus
  insert/update/delete deltas. **Nothing in the OSS tree sends or receives these messages**;
  the crate compiles them for the (closed) cloud control plane.
- `crates/standalone/src/lib.rs:281-282`: `// standalone does not support replication.`
  `let num_replicas = 1;` — the publish path then contains fully general scale-up/scale-down
  logic over that constant (dead in OSS). `get_node_id()` always returns `Some(0)`;
  `get_node_by_id` fabricates a static `Node { advertise_addr: "node:80", pg_addr: "node:5432" }`.
- The `NodeDelegate::leader(database_id)` trait method (implemented at `src/lib.rs:177`) and
  `GetLeaderHostError::is_misdirected()` (`MaybeMisdirected` — an HTTP-routing concept for
  "this node doesn't host that replica, redirect") exist so `client-api` can be shared between
  standalone and cloud; standalone's implementation is just "look up the one replica flagged
  `leader` in sled and launch it locally".
- **No consensus anywhere**: `grep -rn "raft|paxos|quorum|leader.election"` over `crates/`
  yields zero implementation hits. Leader election, log shipping between nodes, failover — all
  absent from OSS.
- **Leader/follower hooks that leak from the cloud** into shared crates:
  - `crates/engine/src/relational_db.rs:254`: the `DiskSizeFn` "must report zero if this
    database is a follower instance"; `:677`: replay "may be lifted in the future to allow for
    'live' followers".
  - `crates/engine/src/snapshot.rs:58`: `SnapshotWorker` is designed to be reused across
    `RelationalDB::open` calls "for replicated databases when transitioning between the leader
    and follower states, to preserve event subscriptions".
  - `crates/engine/src/persistence.rs:85`: durability may be "exclusively remote" — the
    `Durability` trait object (`crates/durability/src/lib.rs`) with its `durable_tx_offset() ->
    DurableOffset` watch-channel handle is the seam where the cloud plugs in replicated
    durability.
  - **Confirmed reads** (`crates/core/src/client/client_connection.rs:79`
    `ClientConfig::confirmed_reads`): clients can request that subscription updates be held
    until the tx offset is durable. The smoketest
    (`crates/smoketests/tests/smoketests/confirmed_reads.rs`) opens with: *"We only test that
    we can pass a `--confirmed` flag and that things appear to work as if we hadn't. Without
    controlling the server, we can't test that there is any difference in behavior."* — i.e.
    the feature only becomes observable under (cloud) replicated durability.

**Verdict for SPEC-014**: yes — replication is genuinely absent from OSS SpacetimeDB. What OSS
ships is the *interface layer* (control-DB vocabulary, leader lookup, durability abstraction,
follower-aware snapshot workers, confirmed-read client protocol) around a closed-source
implementation. An open replication implementation is a real differentiator, and SpacetimeDB's
seams (durable-offset watch channel; snapshot-worker handoff at role transition; "commit log is
the unit of replication") independently validate SPEC-014's central design decision that the
commit log *is* the replication protocol.

---

## 2. `crates/update` — self-updating CLI / version manager

The shipped `spacetime` binary is **not** the CLI — it's `spacetimedb-update`, a
rustup/nvm-style multiplexer (~1,350 LoC):

- `src/main.rs`: if invoked as `spacetime`, it parses just enough args to find `--root-dir`,
  then `proxy::run_cli` **exec-replaces** itself with
  `{bin-dir}/{current-version}/spacetimedb-cli` (`execvp` on Unix, emulated on Windows). A
  `spacetime version …` subcommand family is handled by the update binary itself.
- `src/cli/`: `install`, `uninstall`, `upgrade`, `use`, `list`, `link` (dev builds), and
  `self-install` (the binary doubles as its own installer when named `spacetime-install`,
  with `spacetime-install.sh` / `.ps1` bootstrap scripts).
- `src/update_notice.rs`: before exec'ing, prints an update notice (rate-limited via state in
  the CLI config dir) — the "only chance to print" since exec replaces the process.
- Side-by-side versioned installs under `SpacetimePaths` (`crates/paths`), with a `current`
  symlink/marker. When run from a cargo `target/` dir it proxies to the locally built CLI
  instead (dev ergonomics).

This exists because modules pin host versions and the CLI must match the server; Fluxum's
single-binary story (no separate module toolchain version matrix at 0.1) makes this crate
mostly irrelevant until there is a hosted service with server-version skew.

---

## 3. Metrics & telemetry

### 3.1 `crates/metrics` — typed Prometheus macro layer

A tiny crate (2 files) exporting `metrics_group!` / `metrics_vec!` / `metrics_histogram_vec!`
(`src/typed_prometheus.rs`). It generates **statically typed label methods**: each metric
declares `#[name = …] #[help = …] #[labels(db: Identity, reducer: &str)] #[buckets(…)]`, and the
macro emits a wrapper struct whose `with_label_values(...)` takes typed arguments instead of
`&[&str]`. All four registries below are built on it. There is **no OpenTelemetry anywhere**
(zero `opentelemetry` deps in any Cargo.toml) — telemetry is Prometheus pull + `tracing`.

### 3.2 The four metric groups (all registered in `StandaloneEnv::init`)

Served at `GET /metrics` (`crates/client-api/src/routes/metrics.rs`, nested by
`routes/mod.rs:37`) via `NodeDelegate::gather_metrics()`.

- **`WORKER_METRICS`** (`crates/core/src/worker_metrics/mod.rs`, ~70 metrics) — the
  host/process view: connected clients & WebSocket lifecycle counters
  (`spacetime_worker_connected_clients`, `…ws_clients_spawned/aborted/closed_connection/
  idle_timed_out_total`), message sizes in/out, **jemalloc** gauges, **page-pool** gauges
  (resident bytes, pages reused/dropped — fed by `spawn_page_pool_stats`), a very complete
  **tokio runtime** set (~20 gauges: queue depths, steal counts, busy ratios, polls-per-park —
  `spawn_tokio_stats`), per-reducer queueing (`spacetime_worker_instance_operation_queue_length
  {_histogram}`, `spacetime_reducer_wait_time_sec`), WASM/V8 memory and heap-limit gauges,
  request RTT (`spacetime_request_round_trip_time`), `spacetime_reducer_plus_query_duration_sec`
  (labeled by workload), subscription send-queue lengths, and subscription-eval instrumentation
  (`spacetime_subscription_rows_examined`, `…query_execution_time_micros`, `…queries_total`).
- **`DB_METRICS`** (`crates/datastore/src/db_metrics/mod.rs`, ~40 metrics) — per-database
  datastore view: rows inserted/deleted/scanned, bytes scanned/written, index seeks, txn
  counts, `spacetime_txn_elapsed_time_sec` / `spacetime_txn_cpu_time_sec` histograms (labeled
  db/workload/reducer), commitlog size, **reducer timing split** (`reducer_wasmtime_fuel_used`,
  `reducer_wasm_time_usec`, `reducer_abi_time_usec`, plus `reducer_duration_usec` recorded in
  `crates/core/src/host/wasm_common/module_host_actor.rs:1634-1673`), delta-query/subscription
  evaluation counters (`spacetime_num_delta_queries_evaluated/matched`, duplicate-row counters),
  subscription registry gauges (connections, sets, queries), subscription-lock contention
  (`spacetime_subscription_lock_num_waiters`, `…wait_time_sec`), and procedure HTTP-egress
  counters.
- **`ENGINE_METRICS`** (`crates/engine/src/metrics.rs`) — durability view: bytes sent to
  clients, **replay** timings on startup (total, snapshot read/hash/restore, commitlog replay
  time and commit count), **snapshot creation/compression** timings (incl. fsync time, objects
  compressed vs hardlinked), and `spacetime_durability_blocking_send_duration_sec`.
- **`DATA_SIZE_METRICS`** (`crates/datastore/src/db_metrics/data_size.rs`) — per-table/blob
  gauges: rows, bytes-by-rows, index rows/key bytes, blob counts/bytes.

Notable: metrics use **label cardinality per (db, reducer/table)** freely — acceptable for a
single-tenant node, dangerous for multi-tenant clouds; several metrics exist specifically to
debug fan-out (duplicate rows sent) and lock contention, i.e. they were added after production
incidents.

### 3.3 Tracing / profiling (`crates/core/src/startup.rs`)

`configure_tracing(TracingOptions)` builds a `tracing-subscriber` stack: env-filter directives
from `config.toml` `[logs]` (with a background thread **hot-reloading the filter** when the file
changes, debug builds/`reload_config`), daily-rolling file logs to `{data-dir}/logs/`
(disable via `SPACETIMEDB_DISABLE_DISK_LOGGING`), optional **Tracy** profiler layer
(`--enable-tracy` / `SPACETIMEDB_TRACY`), and optional **flamegraph** layer
(`SPACETIMEDB_FLAMEGRAPH[_PATH]`, folded stacks; `d3-flamegraph-base.html` at repo root renders
them). This matches SPEC-012's direction; the hot-reloadable filter and the "profiling built
into the server binary" posture are worth copying.

### 3.4 Module logs & log streaming to clients

`crates/core/src/database_logger.rs`: each replica has a `DatabaseLogger` writing structured
records (level, target, filename/line, message, optional backtrace) to
`replicas/{id}/logs/{date}.log`, with an in-memory variant for tests. Key capabilities:

- `tail(n, follow)` — async command-channel API; `follow: true` returns a `LogStream` that
  snapshots the persistent tail then **streams new records as they are appended**.
- HTTP: `GET /v1/database/:name_or_identity/logs?num_lines&follow`
  (`crates/client-api/src/routes/database.rs:610`) — owner-only, streamed as a chunked body;
  if the module isn't running it still serves logs **from disk** (`read_latest_on_disk`).
  The CLI surfaces this as `spacetime logs -f` — clients get `tail -f` of their module's logs.
- `SystemLogger` injects host-side messages (e.g. panics, migration output) into the same
  stream, so a module developer sees one merged log.

---

## 4. Testing infrastructure

### 4.1 `TESTING.md` — the suite map

Dated 2025-06-25 (updated 2026-04-14), it admits coverage is "rather haphazardly spread across
several suites" and describes four families:

1. **SDK tests** (`crates/testing/src/sdk.rs`): publish a fresh module → run `spacetime
   generate` codegen → compile a client project → run it as a subprocess with the DB name in an
   env var → exit code = pass/fail. Cross-language module families (`modules/sdk-test*` in
   Rust/C#/TypeScript/C++) run against the same Rust test client (`sdks/rust/tests/test.rs`,
   `declare_tests_with_suffix!` per language).
2. **Schema parity tests** (`crates/schema/tests/ensure_same_schema.rs`): compares *extracted
   schemas* of equivalent modules across languages — "often the first place where casing,
   indexes, primary keys … drift apart".
3. **Smoketests** (below).
4. **Standalone integration test** (`crates/testing/tests/standalone_integration_test.rs`):
   publishes `modules/module-test{,-cs,-ts,-cpp}`, invokes reducers, asserts on module logs.

### 4.2 `crates/smoketests` — 14,822 LoC Rust end-to-end harness

Formerly a Python suite; v2.7.0 is a pure-Rust harness (`src/lib.rs`, 1,961 LoC) + ~55 test
files under `tests/smoketests/` + ~60 prebuilt module fixtures under `modules/`. Architecture:

- **One server per test**: each `Smoketest::builder().build()` spawns its own
  `spacetimedb-standalone` on a random port with an isolated temp data dir and isolated CLI
  config, then publishes a module. Modules come either from inline source strings
  (`module_code(…)` — written to a temp cargo project, compiled to WASM per test, ~12 s) or
  from `precompiled_module("name")` referencing the shared `modules/` workspace.
- **Drives the real CLI as a subprocess** (`run_cmd`): `publish` (a `PublishBuilder` with
  `.name() .clear() .break_clients() .num_replicas() .organization() .force() .stdin()` — note
  `num_replicas`, used against maincloud), `call`, `sql`, `sql_confirmed`, `logs`, `subscribe`
  (a `SubscribeBuilder` with `.expect_rows() .confirmed() .background()` returning a
  `SubscriptionHandle` for later `.collect()`), plus raw HTTP via `ApiResponse`.
- **Assertions** are mostly string/JSON equality on CLI output (`assert_sql("SELECT …",
  "value\n-----\n42")`).
- **Remote mode**: the same suite runs against any standalone-compatible server
  (`cargo ci smoketests --server https://…`), including maincloud with SpacetimeAuth tokens
  (`--auth-host`); tests needing throwaway identities call `require_server_issued_login!()` to
  self-skip. This is how they get *some* cloud coverage from the OSS suite.
- **Prebuilt-binary discipline** (`DEVELOP.md`): tests never rebuild the CLI/server (parallel
  `cargo build` inside tests caused Windows "Access denied" races); `cargo smoketest` is an
  xtask that rebuilds then runs via nextest. Each test costs ~15–20 s (WASM compile dominates).
- **Scenario coverage** (test-file names are the spec): CLI auth/dev/generate/list/publish/
  server; auto-inc; auto-migration & views auto-migration; C# modules (incl. AOT); change host
  type (WASM↔JS); client connection errors (reject/panic/HTTP-cancel); confirmed reads;
  database lock; delete/reset database; describe; DML; domains/namespaces; failed initial
  publish recovery; filtering & RLS (row-level security, with/without filters); HTTP egress
  from procedures; hotswap; module log level filtering; nested module ops; panics (incl.
  panic-then-recover); permissions & lifecycle; **pg-wire** (`psql` against `--pg-port`,
  gated on `have_psql()`); publish-upgrade prompts; **quickstart replay** (runs the actual
  documentation quickstart for Rust/C#/TypeScript — docs-as-tests); server restart with
  connected clients; scheduled reducers (cancel/volatile/subscribe); SQL formatting; detection
  of wasm-bindgen misuse; version-upgrade fixtures (`fixtures/stale-view-backing-table-v2.6.0/`
  — a checked-in v2.6.0 data dir with `control-db/` + `replicas/1/clog/`, and
  `upgrade_old_module_v1.wasm`) exercising **on-disk upgrade compatibility**.

### 4.3 `crates/dst` — deterministic simulation testing (nascent but real)

`spacetimedb-dst-lib` (~2,200 LoC) is a **property-based deterministic simulation harness for
the storage engine**, not a distributed-systems simulator (yet). Structure:

- **`src/traits.rs`** — the clean core abstraction:
  `TargetDriver<I>` (system under test: `execute(&interaction) -> Observation`),
  `Properties<I, O>` (`observe(&interaction, &observation)` validates), and
  `TestSuite` (`build(rng) -> (Interactions, Target, Properties)`; `run(rng, max_interactions)`
  is the generic driver loop).
- **`crates/runtime/src/sim/`** — a full **deterministic simulated runtime** underneath:
  seeded `Rng` (SplitMix64) with a `DeterminismLog` that **panics with "non-determinism
  detected for seed N at checkpoint M"** if two runs of the same seed diverge; a deterministic
  executor (`Runtime`, `Node`/`NodeBuilder` — multi-node vocabulary already present); simulated
  time; and **`buggify.rs`** — FoundationDB-style probabilistic fault injection
  (`should_inject_fault_with_prob(runtime, p)`, explicitly citing
  transactional.blog/simulation/buggify). `spacetimedb-runtime` has a `simulation` feature and
  `Handle::simulation(...)` so *engine code runs unmodified* on either tokio or the sim executor.
- **`src/engine/`** — the first (only) suite, `EngineTest`: `schema.rs` generates a random
  schema plan; `workload.rs` generates weighted interactions
  `{BeginMutTx, Insert, Delete, CommitTx, Replay}`; `model.rs` is an in-memory **model oracle**
  (tracks expected rows incl. unique-constraint acceptance/rejection); `properties.rs` checks
  each observation against the model (`insert_matches`: "target accepted row rejected by
  model" / vice versa; `commit_matches` compares full `CommitDelta`s; `Replay` compares
  post-replay row counts).
- **`src/sim/commitlog.rs`** — an **in-memory `Durability`/`History` implementation**
  (`InMemoryCommitlog`) so the engine's real commit/replay path runs against simulated storage;
  the `Replay` interaction drops the DB and reopens it from the commitlog — i.e. **crash-replay
  equivalence is the flagship property**.
- **Maturity**: no crate in the workspace depends on `spacetimedb-dst-lib`; no CI workflow
  mentions `dst`; only in-crate unit tests exist. This is early scaffolding — the deterministic
  runtime + buggify + multi-node executor strongly suggests the (closed) replication layer is
  the intended target.

### 4.4 `crates/sqltest` — SQL:2016 conformance with SQLite *and Postgres* oracles

A conformance runner over YAML test files organized by **SQL:2016 feature code**
(`standards/2016/E/E011-01.tests.yml` …). `src/main.rs` takes `--engine
{SpacetimeDB|Sqlite|Postgres}` (`src/space.rs`, `src/sqlite.rs`, `src/pg.rs` — Postgres needs a
`TestSpace` database) plus `--override`/`--format` modes that regenerate expected outputs from a
chosen engine. `build_standard.py` generated the corpus; the README documents divergences found
by cross-running (e.g. "`CHAR (8 CHARACTERS)` is not implemented by neither PG nor Sqlite",
"`CURRENT_TIME` marked Don't-use-EVER by PG"). **Not wired into CI** (no workflow references
it) — it's a development-time oracle, not a gate. So SpacetimeDB *does* have a
Postgres-as-correctness-oracle harness for the SQL surface, but no Postgres *performance*
comparison anywhere, and the conformance runner covers only SQL evaluation — not subscriptions,
reducers, or durability semantics.

### 4.5 `crates/bench` — criterion + callgrind; baseline is SQLite, not Postgres

- **Three backends** behind a `BenchDatabase` trait (`src/database.rs`): `spacetime_raw.rs`
  (datastore called directly), `spacetime_module.rs` (through a published WASM module — measures
  "latency to call a reducer without network delays", incl. arg serde), and **`sqlite.rs`** —
  the only external comparison. The README states it plainly: "Provides comparisons between the
  underlying spacetime datastore, spacetime modules, and sqlite."
- **Benchmark matrix** (`benches/generic.rs`): per `[db]/[disk|mem]`: `empty_transaction`,
  `insert_1`, `insert_bulk`, `insert_large_value`, `iterate`, `filter[string,u64][index?]`,
  `find_unique`, `delete` — crossed with schema (`person`=2 ints+string, `location`=3 ints),
  index type (unique/non_unique/multi), and preloaded row counts. Plus `benches/special.rs`
  (serde: bsatn/json/product_value; `stdb_module/print_bulk`, 64 KiB args; **`db_game/`
  workloads** — `circles` (Boids-like) and `ia_loop`, "designed to simulate realistic workloads
  commonly found in games"), `benches/subscription.rs` (subscription evaluation), and
  `benches/index.rs` (with a comment linking to real BitCraft game code as the motivating
  schema).
- **Callgrind** (`benches/callgrind.rs` via iai-callgrind, `callgrind-docker.sh`) for
  instruction-count benchmarks — deliberately excludes async plumbing (README documents the
  limitation).
- **CI integration** (`.github/workflows/benchmarks.yml`): comment **"benchmarks please"** (or
  "callgrind please") on a PR → runs on a dedicated `benchmarks-runner` with CPU-boost toggling
  (`Enable CPU boost` / `Disable CPU boost` steps around the build), compares against a
  baseline via `src/bin/summarize.rs` (pack/markdown-report), uploads raw criterion output to
  DigitalOcean Spaces, and posts the markdown diff as a PR comment (external
  `benchmarks-viewer` app). There is also a scheduled `llm-benchmark-periodic.yml` (LLM codegen
  quality benchmarks — `tools/xtask-llm-benchmark`) and a `Keynote Bench` job in `ci.yml` on a
  `spacetimedb-benchmark-runner` (`tools/keynote-bench-harness`) — a curated headline-number
  harness.
- `hyper_cmp.py` — a small hyperfine-based CLI-latency comparator between branches.

**Answer for NFR-11**: SpacetimeDB has *no* Postgres performance-parity harness. Their
published comparisons are vs. SQLite (embedded, in-process — a flattering baseline for an
in-process datastore) and vs. themselves (regression tracking). Fluxum's SPEC-013 §10
(same application implemented twice, equal hardware, tuned Postgres, honesty rules TST-091,
versioned report, CI ratio guard) has no equivalent here and is a genuine edge — with the
caveat that SpacetimeDB's *SQL-correctness*-vs-Postgres oracle (`sqltest`) covers a parity
dimension SPEC-013 should keep distinct from performance parity.

### 4.6 CI (`.github/workflows/`, 18 workflows)

`ci.yml`: merge-queue no-op detection (tree-hash comparison to skip duplicate runs);
**Smoketests on Linux + Windows matrix** (nextest, `--test-threads=1`, with Node/pnpm,
emscripten, .NET workloads, psql installed — the full polyglot toolchain); **Test Suite** on a
self-hosted `spacetimedb-new-runner-2` (`cargo ci test` xtask); Keynote Bench. Notable
recurring pain visible in comments: sccache/V8 build flakiness (`cargo clean --release -p v8 ||
true` workaround), wasm-bindgen version pinning. Other workflows: `benchmarks.yml`
(comment-triggered, above), `release.yml`/`tag-release.yml`/`package.yml`/
`attach-artifacts.yml`, `docs-publish.yaml` + `docs-update-llms.yaml` (llms.txt), CLA gates,
`internal-tests.yml` (triggers the private repo's suite — the cloud tests OSS PRs against
closed code), `check-merge-labels.yml`, `pr_approval_check.yml`. There is no coverage gate, no
fuzzing workflow, no sanitizer matrix visible in OSS CI.

### 4.7 Docker deployment story

- **Root `Dockerfile`** (the shipped image): 2-stage `rust:bookworm` build of
  `spacetimedb-standalone` + `spacetimedb-cli` — but the runtime image then installs **the
  entire module toolchain**: .NET 8 + `wasi-experimental` workload, Rust `wasm32-unknown-unknown`
  target, binaryen — because publishing a module compiles it server-side-adjacent. The image is
  a dev workstation, not a minimal server.
- **`docker-compose.yml`** (dev only): single `node` service running `cargo watch … -x 'run
  start --data-dir=/stdb/data … --pg-port 5432'` with live-mounted source crates, ports 3000
  (HTTP/WS), 5432 (pg-wire), 8086 (**Tracy profiler**), `SPACETIMEDB_FLAMEGRAPH_PATH` +
  `SPACETIMEDB_TRACY=1`, `privileged: true`. No multi-node compose exists — consistent with
  §1.1: there is nothing multi-node to compose.

---

## 5. Comparison snapshot vs. Fluxum specs

| Area | SpacetimeDB OSS v2.7.0 | Fluxum spec | Delta |
|---|---|---|---|
| Replication | Vocabulary + hooks only; `num_replicas = 1` hard-coded (`crates/standalone/src/lib.rs:282`); consensus/failover closed-source | SPEC-014: open replica sets, commit-log streaming, ack modes, failover, PITR | SPEC-014 is a true OSS differentiator; SpacetimeDB's seams validate "commit log = replication protocol" |
| Metrics | 4 Prometheus groups, ~120 metrics, typed-label macro; no OTel | SPEC-012 `fluxum_` metrics on admin port | Near-parity in philosophy; copy: typed `metrics_group!`, replay/snapshot timing metrics, subscription-eval + lock-contention metrics, tokio/allocator gauges |
| Logging | Per-module daily log files, merged system+module stream, HTTP `?follow` streaming, hot-reloadable filter | SPEC-012 §9 structured logging | Module-log streaming to clients (`spacetime logs -f`) is a DX feature SPEC-012 doesn't currently spec |
| E2E harness | 14.8k-LoC Rust smoketest harness, one server per test, CLI-subprocess-driven, remote-server mode, docs-as-tests | SPEC-013 conformance corpus | Architecture worth copying (see below) |
| DST | Engine-only, model-oracle + crash-replay property, deterministic runtime + buggify; not in CI | SPEC-013 §3 crash suite (kill-harness), §4 subscription proptest | Adopt the *runtime seam* idea, not just the harness |
| SQL conformance | SQL:2016 YAML corpus, 3 engines incl. Postgres oracle; not in CI | SPEC-013 §6 protocol conformance | Distinct correctness-parity dimension worth adding |
| Benchmarks | criterion + callgrind vs. SQLite + self; comment-triggered PR bot with baseline diff | SPEC-013 §10 Postgres parity harness (NFR-11) | No Postgres perf comparison exists — NFR-11 edge confirmed |
| Quality gates | fmt/clippy/nextest, Linux+Windows smoketests; no coverage/fuzz/sanitizers in OSS CI | SPEC-013 §2 NFR-09 3-OS matrix | Fluxum's stated gates are already stricter than SpacetimeDB's visible ones |

---

## What Fluxum will face

1. **Replication absence confirmed — but so is the pattern for hiding it.** SpacetimeDB keeps
   OSS honest by shipping the full *interface* (control-DB `Replica`/`Node` records, worker
   protocol enums, `NodeDelegate::leader`, follower-aware `DiskSizeFn`/`SnapshotWorker`,
   confirmed-reads protocol) with a closed implementation. Fluxum, implementing SPEC-014 in the
   open, inherits the harder problem SpacetimeDB deferred: the smoketest comment "*without
   controlling the server, we can't test that there is any difference in behavior*" is exactly
   the trap — replication features that aren't observable from a single node need a multi-node
   test harness from day one (SPEC-013 §13 drills must land *with* SPEC-014, not after).

2. **Adopt the DST seam before writing the replication layer, not after.** SpacetimeDB
   retrofitted a `simulation` feature onto `spacetimedb-runtime` (`Handle::simulation`,
   `Node`/`NodeBuilder`, buggify, determinism-log panics) so engine code runs unmodified on a
   seeded executor — and their engine suite's flagship property (drop DB, replay commitlog,
   compare against a model oracle) is precisely SPEC-013 §3's crash suite, minus the process
   kills. Recommendation: SPEC-013 should add a DST layer for **storage and replication**
   (deterministic executor + simulated commitlog + model oracle + buggify-style fault points),
   keep the OS-level kill-harness (T2.7) as the non-simulated backstop, and require the
   determinism check (same seed ⇒ identical trace) as a CI property. The cost SpacetimeDB paid
   — a whole abstract-runtime crate threaded through the engine — is far cheaper at Fluxum's
   current stage than at theirs (their DST still isn't in CI).

3. **Copy the smoketest harness shape, skip its tax.** The wins to replicate in Fluxum's
   conformance corpus: one isolated server + data dir per test; drive the *real* CLI/client as
   a subprocess (tests the actual product surface, catches CLI regressions free); builders for
   publish/subscribe with background subscription handles; a `--server URL` remote mode so the
   same corpus runs against any deployment; checked-in **old-version data-dir fixtures** for
   upgrade testing; and quickstart-docs-as-tests. The tax to avoid: 12 s of per-test WASM
   compilation (Fluxum's native modules compile once as normal Rust — precompiled fixture
   modules should be the default, inline-source modules the exception) and the stale-prebuilt-
   binary footgun their `DEVELOP.md` shouts about (make the test runner hash-check binaries).

4. **The NFR-11 edge is real; hold both parity dimensions.** SpacetimeDB benchmarks against
   SQLite and itself, never Postgres — their `sqltest` uses Postgres only as a *correctness*
   oracle, and even that isn't in CI. Fluxum's SPEC-013 §10 (equal-hardware, tuned-Postgres,
   versioned-report performance parity) is uncontested in this codebase. But add the missing
   mirror: a Postgres/SQLite *correctness* oracle for the query surface (SpacetimeDB's YAML-
   per-feature-code corpus with `--override` regeneration is a good, cheap format), so
   performance claims and semantics claims are separately falsifiable.

5. **Observability: SPEC-012 is directionally right; steal four specifics.** (a) the typed
   `metrics_group!` macro pattern — compile-time-checked label arity costs nothing and prevents
   the classic label-drift bug; (b) startup **replay/snapshot timing metrics**
   (`spacetime_replay_*`, `spacetime_snapshot_*` incl. fsync time) — Fluxum's SPEC-002/014
   recovery paths need these to make failover time measurable; (c) subscription-eval forensic
   counters (rows examined, duplicate rows sent, subscription-lock wait time) — these are
   post-incident metrics SpacetimeDB clearly needed; (d) module-log streaming with `?follow`
   and merged system+module records — a small feature with outsized DX value. Also note what
   they *don't* have: no OpenTelemetry traces; if SPEC-012 stays Prometheus-only, Fluxum is at
   parity, and OTel spans over reducer→fan-out would exceed it.

6. **Operational surface creep is the standalone crate's real lesson.** Even "single node, no
   replication" accreted: pid-file locking, `metadata.toml` edition/version compatibility
   gating, first-run config materialization, interactive port-conflict resolution, hot-reload
   log filters, jemalloc/tokio/page-pool stat exporters, a version-manager binary in front of
   the CLI, and a Docker image that ships a whole compiler toolchain. Fluxum should budget for
   the first five now (they are cheap early, breaking later) and let its native-module model
   explicitly kill the last two — no server-side module toolchain, no CLI/server version
   multiplexer until a hosted service exists.
