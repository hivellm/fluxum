# SpacetimeDB Deep-Dive 04 — Module Host & ABI

Analysis of SpacetimeDB v2.7.0 (commit `1a8df2a`) module hosting: lifecycle, instance pools,
reducer call path, the WASM (wasmtime) and JS (V8) engines, traps/panics, hot-publish +
auto-migration, the guest bindings/macros, the raw ABI, the 15.6k-LOC schema crate, energy
accounting, and the reducer context surface. Written for Fluxum, which replaces all of this
guest/host machinery with **native static Rust modules** (docs/ARCHITECTURE.md "native module
model", SPEC-001, SPEC-004).

## Sources

| Area | Path (under `crates/`) | Size |
|---|---|---|
| Host controller / lifecycle | `core/src/host/host_controller.rs` | 1,607 LOC |
| ModuleHost, instance pools | `core/src/host/module_host.rs` | 3,655 LOC |
| Runtime-agnostic reducer path | `core/src/host/wasm_common/module_host_actor.rs` | 2,008 LOC |
| Wasmtime engine setup | `core/src/host/wasmtime/mod.rs` | 386 LOC |
| Wasmtime instance + call path | `core/src/host/wasmtime/wasmtime_module.rs` | 991 LOC |
| Host-side ABI functions | `core/src/host/wasmtime/wasm_instance_env.rs` | 2,081 LOC |
| ABI-neutral DB env (chunking) | `core/src/host/instance_env.rs` | 2,215 LOC |
| ABI names/signatures/version | `core/src/host/wasm_common.rs`, `wasm_common/abi.rs` | 453 + 56 LOC |
| V8/JS runtime | `core/src/host/v8/` (`mod.rs` 2,187; `syscall/` ~3,500; `budget.rs` 122) | ~7,600 LOC |
| Scheduler | `core/src/host/scheduler.rs` | 844 LOC |
| Energy trait | `core/src/energy.rs`, `client-api-messages/src/energy.rs` | small |
| Guest raw ABI | `bindings-sys/src/lib.rs` | 1,658 LOC |
| Guest runtime & registration | `bindings/src/rt.rs`, `bindings/src/lib.rs`, `bindings/src/table.rs` | 1,436 + 2,118 + 1,449 LOC |
| Proc macros | `bindings-macro/` (`table.rs` 1,379; `reducer.rs` 204; `sats.rs` 752) | ~3,400 LOC |
| Schema/ModuleDef/validation | `schema/src/def.rs` 2,177; `def/validate/v9.rs` 2,442; `v10.rs` 2,519; `auto_migrate.rs` 2,625; `schema.rs` 1,519 | ~15.6k LOC |
| Deterministic-sim runtime | `runtime/src/` (NOT the JS runtime — see §5) | ~2.6k LOC |

---

## 1. Host lifecycle: `HostController` → `Host` → `ModuleHost`

`HostController` (`host_controller.rs:176`) is the registry of running databases, keyed by
`replica_id` in `Hosts = Arc<Mutex<IntMap<u64, Arc<AsyncRwLock<Option<Host>>>>>>`. It is cheap to
clone (all `Arc`s) and owns:

- `program_storage: Arc<dyn ExternalStorage>` — content-addressed program bytes by `Hash`.
- `energy_monitor: Arc<dyn EnergyMonitor>` — pluggable metering (see §7).
- `runtimes: Arc<HostRuntimes>` — exactly two engine singletons: `WasmtimeRuntime` and `V8Runtime`
  (`host_controller.rs:203`).
- `db_cores: JobCores` — pinned CPU cores handed to each module's executor thread.
- shared `PagePool` and a `BsatnRowListBuilderPool`.

Launch path (`Host::try_init`, `host_controller.rs:1037`):
1. Open `RelationalDB` (replay commitlog / snapshot).
2. Load program bytes: from the DB's `st_module` system table if initialized, else from
   `ProgramStorage` by `database.initial_program` hash.
3. `make_module_host` dispatches on `HostType::{Wasm, Js}`; wasm compilation runs under
   `asyncify` because cranelift compile of a large module can take ~1s.
4. If fresh: `init_database` — creates all tables/views/RLS from the `ModuleDef` in one tx, then
   invokes the `Lifecycle::Init` reducer (`module_host.rs:573-664`).
5. Replays `client_disconnected` for clients recorded in `st_client` at crash time; starts the
   scheduler (`scheduler_starter.start`), disk-metrics task, view-cleanup task.

The live module is held in a `watch::Sender<ModuleHost>` inside `Host` (`host_controller.rs:1010`)
— subscribers (websocket handlers) observe hot-swaps through the watch channel.

### `ModuleHost` internals (`module_host.rs:1435`)

```
ModuleHost { info: Arc<ModuleInfo>, inner: Arc<ModuleHostInner>, on_panic, closed: AtomicBool }
ModuleHostInner::Wasm(WasmtimeModuleHost) | Js(V8ModuleHost)
```

`ModuleInfo` (`module_host.rs:221`) carries `Arc<ModuleDef>`, owner/database identities, module
hash, and the `ModuleSubscriptions` handle — i.e., the validated schema is pinned to the running
module, not looked up from the datastore.

### Instance pools & threading

- **WASM main lane**: `WasmtimeModuleHost` (`module_host.rs:417`) = one `SingleThreadedExecutor`
  on a dedicated pinned OS thread (`wasm-<id-suffix>` name) holding exactly ONE
  `ModuleInstance`. Reducers are **serialized** — there is no reducer concurrency per database.
  After each job, `WasmtimeModuleState::with_instance` checks `needs_replacement()` (trapped) and
  synchronously rebuilds the instance (`module_host.rs:397-407`).
- **WASM procedure lane**: `ModuleInstanceManager<Arc<ProcedureModule>>` (`module_host.rs:1186`)
  — bounded pool (tokio `Semaphore` + `VecDeque`), instances created lazily on demand, leased for
  the whole (async, suspendable) procedure call, and *dropped instead of returned* if they
  trapped (`return_instance`, `module_host.rs:1405`).
- **JS**: one shared main worker thread per DB with a mpsc queue (`SharedJsMainInstanceManager`),
  plus a bounded pool of procedure isolates on their own OS threads; a trapped/heap-limited
  isolate is replaced inline in the worker (`v8/mod.rs` module doc).

Every entry point goes through the `call`/`call_pooled` helpers which start queue-time metrics
(`reducer_wait_time`, `instance_queue_length`) and install `scopeguard::defer_on_unwind` calling
`on_panic()` — which unregisters the whole host from the controller (`unregister_fn`,
`host_controller.rs:732`), so a host-side panic tears the database host down rather than
corrupting state.

## 2. Reducer call path (the hot path)

`ModuleHost::call_reducer` (`module_host.rs:2332`):

1. **Lookup + typecheck**: `module_def.reducer_full(name)` → `(ReducerId, &ReducerDef)`; rejects
   lifecycle reducers and private reducers for non-owners. `FunctionArgs::{Json,Bsatn,Nullary}`
   are deserialized against the reducer's `ProductType` param signature into an `ArgsTuple`
   (a `ProductValue` with memoized bsatn/json forms) (`host/mod.rs:41-109`).
2. **Enqueue** onto the module's single executor thread.
3. `InstanceCommon::call_reducer_with_tx_offset` (`module_host_actor.rs:963`) — the
   runtime-agnostic core shared by WASM and V8:
   - Begin (or adopt) a `Serializable` `MutTxId` with `Workload::Reducer(ReducerContext)`.
   - Park the tx in the instance's `TxSlot` (`instance_env.rs:92` — a
     `Arc<Mutex<Option<MutTxId>>>` the ABI syscalls pull the tx out of).
   - `call_function` (`module_host_actor.rs:1132`): ask `EnergyMonitor::reducer_budget(fingerprint)`
     for a `FunctionBudget`, invoke the VM, then `record_reducer(fingerprint, used, duration)`.
   - Classify the result: `ExecutionError::User` (module returned `Err(msg)`) →
     `EventStatus::FailedUser`; `Recoverable`/`Trap` → traceback + `FailedInternal` or
     `OutOfEnergy` if remaining fuel == 0 (`handle_outer_error`, `module_host_actor.rs:1118`).
   - On success, re-evaluate any materialized views whose read-sets the tx touched
     (`call_views_with_tx`), then `commit_and_broadcast_event` — commit the tx and fan out
     subscription updates in one step.
4. Result: `ReducerCallResult { outcome: Committed|Failed|BudgetExceeded, execution_budget_used,
   execution_duration }` plus a `trapped: bool` that drives instance replacement.

### WASM invocation detail (`wasmtime_module.rs:621-665`)

`__call_reducer__(id: u32, sender_0..3: u64, conn_id_0..1: u64, timestamp: u64,
args: BytesSource, error: BytesSink) -> i32` — identity is smeared across 4 `u64` params,
connection id across 2 (little-endian `bytemuck` casts), args are **not** written into guest
memory up front; instead the host registers a `BytesSource` handle that the guest drains via
`bytes_source_read`, and errors flow back through a `BytesSink` handle. Return code 0 = ok,
`HOST_CALL_FAILURE` (errno 1) = user error in the sink, anything else = recoverable error;
a wasmtime trap surfaces as `ExecutionError::Trap`.

## 3. The ABI (host↔guest boundary)

### Surface

Import modules are versioned `spacetime_10.0` … `spacetime_10.5` (`wasm_common.rs:405-453`,
guest side `bindings-sys/src/lib.rs`). The host checks each import's module string, computes the
max requested minor version, and rejects unsupported ones (`abi::determine_spacetime_abi`,
`verify_supported`; `WasmtimeModule::IMPLEMENTED_ABI = 10.5`). The full syscall list (~30 calls,
tagged by `AbiCall` in `host/mod.rs:169`):

- **Datastore**: `table_id_from_name`, `index_id_from_name`, `datastore_table_row_count`,
  `datastore_table_scan_bsatn`, `datastore_index_scan_{point,range}_bsatn`,
  `datastore_insert_bsatn`, `datastore_update_bsatn`,
  `datastore_delete_by_index_scan_{point,range}_bsatn`, `datastore_delete_all_by_eq_bsatn`,
  `datastore_clear`, `row_iter_bsatn_advance`, `row_iter_bsatn_close`.
- **Byte pipes**: `bytes_source_read`, `bytes_source_remaining_length`, `bytes_sink_write`.
- **Misc**: `console_log`, `console_timer_start/end`, `identity`, `get_jwt`,
  `volatile_nonatomic_schedule_immediate`.
- **Procedures (10.3, async lane only)**: `procedure_sleep_until`, `procedure_http_request`
  (linked as trapping sync stubs in the main lane), and sync
  `procedure_{start,commit,abort}_mut_tx`.

Guest exports validated at load (`FuncNames::check_required`, `wasm_common.rs:234`): `memory`,
`__call_reducer__`, `__describe_module__`, optional `__call_procedure__`, `__call_view__`,
`__call_view_anon__`, `__call_http_handler__`, `__setup__`, and any number of `__preinit__*`
functions (registration hooks, run in sorted order).

### Marshaling cost model (what the boundary actually costs)

Everything crossing is **BSATN bytes copied through linear memory**:

- **Writes**: guest serializes the row to bsatn in its own buffer, host reads it out of wasm
  memory (`MemView::deref_slice`), decodes to `ProductValue`, inserts; for auto-inc columns the
  host **writes generated column values back into the guest's row buffer** (`datastore_insert_bsatn`
  returns via `row_ptr/row_len` in-place).
- **Reads / iteration**: scans are *eagerly* executed host-side into pooled ~chunk-sized bsatn
  buffers (`ChunkedWriter::collect_iter`, `instance_env.rs:196`; `ROW_ITER_CHUNK_SIZE`, pool
  capped at 32 chunks × 4×chunk-size). The guest gets a `RowIter` handle in a `ResourceSlab` and
  pulls chunks with `row_iter_bsatn_advance` into a guest buffer, then deserializes each row from
  bsatn. So every scanned row is: encode-to-bsatn (host) → copy into wasm memory → decode (guest).
- **Every host call** is wrapped in `cvt/cvt_ret` (`wasm_instance_env.rs:452-508`) which records a
  per-`AbiCall` timing span (`CallTimes`, reported as "abi_duration") and converts `NodesError`
  into stable errnos (`err_to_errno`, `wasm_common.rs:357`).
- Args/results also pass through `BytesSource`/`BytesSink` handle indirection rather than direct
  pointers.

This is precisely the overhead Fluxum's in-process modules eliminate: no linear-memory copies, no
bsatn round-trip per row, no handle slabs, no errno mapping — a reducer can hold typed `RowRef`s.

### Guest-side dispatch (`bindings/src/rt.rs`)

- `#[reducer]` (macro, `bindings-macro/src/reducer.rs:95`) generates: a unit struct named after
  the fn, an `impl FnInfo` (NAME, LIFECYCLE, ARG_NAMES, INVOKE fn-pointer), a monomorphized
  `invoke` that deserializes args and calls the user fn, and an
  `#[export_name = "__preinit__20_register_describer_<name>"]` extern fn calling
  `rt::register_reducer`.
- `#[table]` (`bindings-macro/src/table.rs`) similarly derives SATS
  serialize/deserialize/`SpacetimeType`, a `<name>__TableHandle` implementing
  `spacetimedb::table::TableInternal` (table name, indexes, unique columns, sequences, schedule),
  typed index accessor structs, and a `__preinit__` registering `rt::register_table::<T>()`.
- At module load the host calls each `__preinit__*` export; each pushes a closure into a global
  `DESCRIBERS: Mutex<Vec<Box<dyn DescriberFn>>>` (`rt.rs:939`). Then `__describe_module__` runs
  all describers against a `RawModuleDefV10Builder`, bsatn-serializes `RawModuleDef::V10`,
  writes it to a sink, and freezes global dispatch tables `REDUCERS/PROCEDURES/VIEWS/...:
  OnceLock<Vec<fn>>` (`rt.rs:977-1000`). `__call_reducer__` then is just
  `reducers[id](&ctx, args)` (`rt.rs:1035-1064`). Reducer IDs = declaration order in the
  `ModuleDef` (IndexMap), preserved across host and guest.

So the "schema registry" is: link-time collection of exported preinit symbols → runtime
`ModuleBuilder` → bsatn `RawModuleDef` → host-side validation into `ModuleDef`. Fluxum can do
the same trick natively with `inventory`/`linkme` distributed slices — no serialization step
needed, the registrations can produce the validated def types directly.

## 4. Traps, panics, isolation

- Guest Rust panics become wasm traps (`unreachable`), surfacing as `ExecutionError::Trap`. The
  open tx (still in `TxSlot`) is taken back by the host and rolled back; the event is recorded as
  `FailedInternal` (or `OutOfEnergy` if fuel hit 0). The instance is flagged `trapped` and
  replaced after the call; the store/instance are cheap to rebuild from `InstancePre`.
- Host-side panics inside a module job are caught by `defer_on_unwind` → `on_panic()` →
  host unregistered from the controller (next request relaunches it from persistent state).
- Epoch interruption (`EPOCH_TICK_LENGTH = 10ms`, ticker task incrementing both engines' epochs)
  is used **only for logging long-running calls** — the deadline callback logs a warning and
  returns `UpdateDeadline::Continue` (`wasmtime_module.rs:354-363`). Actual termination comes
  from fuel exhaustion.
- Procedures that abandon an anonymous tx get it force-aborted after the call
  (`terminate_dangling_anon_tx`).

Fluxum note: with native modules a reducer panic unwinds through host frames. SpacetimeDB's
design shows the invariant to preserve: *tx lives in a slot owned by the host, never by the
module*, so any failure path (panic, error, budget) rolls back centrally. Native Fluxum needs
`catch_unwind` at the dispatch boundary + abort-on-double-panic policy, and must accept that a
truly wild module (UB, segfault) takes the process down — the isolation SpacetimeDB buys with
wasm is exactly what restart-based deploys and process supervision must cover.

## 5. The two engines

### Wasmtime (`host/wasmtime/`)

- Two `Engine`s per process: sync (reducers/views — no fiber overhead) and async (procedures,
  `async_support(true)`, pooled fiber stacks on unix). Config: `cranelift_opt_level(Speed)`,
  `consume_fuel(true)`, `epoch_interruption(true)`, backtrace details on, on-disk compile cache
  under the data dir (`wasmtime_config`, `mod.rs:90-132`).
- Same program bytes are compiled **twice** (once per engine); `InstancePre` amortizes linking.

### V8 (`host/v8/`, NOT `crates/runtime`)

Important correction to the task brief: **`crates/runtime` is not the JS runtime** — it is
`spacetimedb_runtime`, a tokio-vs-deterministic-simulation abstraction (`Handle::Tokio |
Simulation`, sim executor/time/rng/buggify — FoundationDB-style DST plumbing, `runtime/src/lib.rs`).
The TypeScript/JS module runtime lives in `core/src/host/v8/` using the `v8` crate (rusty_v8
`=145.0.0`, pinned in workspace `Cargo.toml`) + `deno_core_icudata`; there is no deno_core — they
built raw isolates:

- Synthetic ES modules `spacetime:sys@1.0/1.1/1.2` expose the same syscall surface as the wasm
  ABI (`v8/syscall/v1.rs`, `v2.rs`, `common.rs` ~3,400 LOC); the JS SDK registers hook objects
  (`__call_reducer__`, `__call_view__`, …) via `register_hooks`.
- Values crossing are still bsatn `Uint8Array`s plus dedicated ser/de into JS values
  (`v8/ser.rs`, `de.rs`).
- **Energy**: V8 has no fuel. `v8/budget.rs` converts budget→wall-clock deadline enforced by
  `IsolateHandle::terminate_execution()` from a ticker thread — but it is currently **stubbed
  out** ("TODO(v8): This currently leads to UB as there are bugs in the v8 crate"), with
  `budget_to_duration = Duration::MAX` and used-energy reported as `ZERO`. i.e., JS modules run
  effectively unmetered today.
- Isolate heap limits + trap recovery replace instance replacement; a JS "trap" does not poison
  a pool instance (`GenericModuleInstance for JsProcedureInstance::trapped() = false`).

## 6. Publish, hotswap, auto-migration

Publish flow (`client-api/src/routes/database.rs` → controller):

1. **Fresh publish**: `check_module_validity` (`host_controller.rs:480`) spins up a **throwaway
   in-memory database** with the candidate program, runs full init (schema creation, RLS
   typechecking, `__init__`), then discards it. Validation-by-execution.
2. **Update publish**: `update_module_host` (`host_controller.rs:513`) takes the replica's write
   lock, builds a *new* `ModuleHost` from the new program (new wasm compile, new `ModuleDef`),
   then `Host::update_module`:
   - `MigrationPolicy::{Compatible, BreakClients(token)}.try_migrate(old_def, new_def)` →
     `MigratePlan` via `ponder_migrate` (`schema/src/auto_migrate.rs:469`) — a pure diff of the
     two `ModuleDef`s.
   - `InstanceCommon::update_database` (`module_host_actor.rs:643`): one serializable tx —
     `update_program` (swap `st_module` row) + execute the plan steps against the datastore;
     rollback on any error, leaving the **old module running** (`UpdateDatabaseResult::
     {AutoMigrateError, ErrorExecutingMigration}`).
   - On success: swap the `watch::Sender<ModuleHost>`, start new scheduler, `old_module.exit()`;
     if the plan `breaks_client`, disconnect all clients (they reconnect against the new module).
3. **Dry-run**: a separate `migrate_plan` endpoint extracts the candidate's `ModuleDef` by
   running `__describe_module__` in a dummy in-memory host (`extract_schema`,
   `host_controller.rs:1526-1574`) and pretty-prints the plan without applying.

`AutoMigrateStep` covers: add/remove table, add columns (with defaults), change columns
(`ChangeColumns`/layout-compatible only), add/remove index, constraint, sequence, schedule, view,
RLS, change table access, change primary key, update view, `ReschemaEventTable`. The error enum
is exhaustive about *why* a change is rejected (dozens of `ChangeColumnType*` variants: fewer
variants, renamed field, size mismatch, align mismatch…) — errors are accumulated in an
`ErrorStream`, not fail-fast, and there is a human-readable plan formatter (+ colored terminal
variant).

Fluxum chose restart-based deploys, so the watch-channel hot-swap, dual-module handoff, and
client-disconnect orchestration disappear. What does **not** disappear: `ponder_migrate`-style
schema diffing must still run at startup (old def from stored catalog vs. new def compiled into
the binary), with the same "reject on incompatible change unless forced" policy — that's the
core of SPEC-010.

## 7. Energy / budget accounting

- Host trait `EnergyMonitor` (`core/src/energy.rs`): `reducer_budget(&FunctionFingerprint) ->
  FunctionBudget` before each call; `record_reducer(fingerprint, used, duration)` after; plus
  periodic `record_disk_usage`/`record_memory_usage`. Fingerprint = module hash + module identity
  + caller identity + function name. OSS standalone ships only `NullEnergyMonitor` (infinite
  default budget, records nothing) — real billing lives in their hosted control plane.
- `FunctionBudget` (`client-api-messages/src/energy.rs:130`) is a `u64` that is **literally
  wasmtime fuel 1:1** (≈1 wasm instruction). Constants: `PER_EXECUTION_SEC = 2e9` (assumes 2GHz
  1-IPC), `DEFAULT_BUDGET` = 60s worth. `EnergyQuanta` (u128 "eV") is the billing-side unit with
  disk/memory byte·second conversions; fuel→quanta conversion is external to this crate.
- Mechanics: `store.set_fuel(budget)` before the call, `get_fuel()` after; used = budget −
  remaining (`prepare_store_for_call` / `finish_opcall`, `wasmtime_module.rs:884-952`). Fuel
  exhaustion = trap with remaining==0 → `EventStatus::OutOfEnergy`. Comments warn the u64 width
  is load-bearing (a past bug produced u64::MAX usage for no-op reducers).
- Per-call `ExecutionStats` also splits `total_duration` vs `abi_duration` (sum of per-AbiCall
  spans) — i.e., they explicitly measure how much of a reducer is spent inside the host boundary.
- Procedures/HTTP handlers get a budget but usage is not yet recorded (`TODO(procedure-energy)`).
- V8: unimplemented (see §5).

Fluxum implication: with native code there is no instruction counter. The only real options are
the ones SpacetimeDB uses for V8 — wall-clock deadlines + a watchdog — or cooperative checkpoints.
Notably SpacetimeDB *cannot preempt wasm either*; fuel only stops execution at function/loop
granularity, and their epoch system just logs. A `FunctionBudget`-like per-call time budget with
the `EnergyMonitor` trait shape (budget-before, record-after, fingerprint keying) is directly
reusable and keeps the metering pluggable without the fuel machinery.

## 8. Reducer context surface (what modules can do)

From `bindings/src/lib.rs` + `table.rs` + `rng.rs`:

- `ReducerContext { sender: Identity, connection_id: Option<ConnectionId>, timestamp: Timestamp,
  identity() (database identity), db: Local, rng(), sender_auth: AuthCtx (JWT claims),
  new_uuid_v4/v7() }`.
- `ctx.db.<table>()` → generated table handle: `count()`, `iter()`, `insert()` (panics on
  constraint violation), `try_insert() -> Result<Row, TryInsertError>` (typed
  `UniqueConstraintViolation` / `AutoIncOverflow`), `delete(row)`; per-unique-column
  `UniqueColumn::{find, delete, update, insert_or_update}` (update restricted to primary-key
  columns via a `PrimaryKey` marker trait); `PointIndex::{filter, delete}` and
  `RangedIndex::{filter(range), delete(range)}` with typed multi-column prefix arguments.
  Everything routes through the bsatn ABI calls of §3.
- **RNG**: `ctx.rng()` = `StdbRng`, a `StdRng` **seeded from the reducer-call timestamp**
  (`rng.rs:49-51`) — deterministic given the recorded event, so commitlog replay reproduces it.
- **Timestamps**: `ctx.timestamp` is host-supplied per call (part of `__call_reducer__` args);
  guests have no clock syscall in the reducer lane.
- Scheduling: `#[table(scheduled(reducer_name))]` scheduled tables; rows with `ScheduleAt::
  {Time, Interval}` drive `scheduler.rs` (min-heap over tx-committed schedule rows); plus the
  unstable `volatile_nonatomic_schedule_immediate`.
- Logging via `console_log` with level + file/line, `log_stopwatch` timers.
- Procedures additionally get `ProcedureContext::{with_tx/try_with_tx (anonymous committed txs),
  sleep_until, http_request}` — suspendable, off the main lane.
- Views (`#[view]`), HTTP handlers/routers, `#[client_visibility_filter]` RLS SQL constants.

This is a close match to Fluxum SPEC-004's `ReducerContext`/`TxHandle` plan; the notable design
choices worth copying are (a) timestamp-seeded RNG for replay determinism, (b) `try_insert` with
typed constraint errors while `insert` panics, (c) the `PrimaryKey`-marker-gated `update`.

## 9. Schema crate: why 15.6k LOC, and what Fluxum actually needs

Breakdown of `crates/schema`:

- `def.rs` (2,177): `ModuleDef` — validated, canonicalized, immutable: `IdentifierMap<TableDef>`,
  ordered `IndexMap`s for reducers/procedures/views/http handlers (order = wire ID), lifecycle
  reducer map, `Typespace` + `TypespaceForGenerate` (client codegen form), global-namespace map
  (`stored_in_table_def`), raw RLS. Sub-defs: TableDef → ColumnDef/IndexDef/ConstraintDef/
  SequenceDef/ScheduleDef, ReducerDef, ProcedureDef, ViewDef, TypeDef.
- `def/validate/v9.rs` + `v10.rs` (~5k): raw→validated conversion for TWO wire versions of
  `RawModuleDef`, each checking identifier validity (unicode rules, `identifier.rs`), name
  collisions, type references, index/constraint/sequence coherence, scheduled-table shapes, etc.
  Errors accumulate in `ErrorStream` (all problems reported at once).
- `auto_migrate.rs` + formatters (~3.7k): the diff planner of §6.
- `schema.rs` (1,519): conversion from defs to datastore `TableSchema` etc.
- `type_for_generate.rs` (909): typespace normalization for SDK codegen.

Half of this crate exists because the module def crosses a **serialization boundary with two
historical versions** (v9 bsatn, v10 bsatn) authored by untrusted guests. A native Fluxum module
constructs defs in-process from macro-generated code, so: no raw/validated split across a wire
format, no multi-version deserialization, no "is this identifier valid UTF-8" class of checks on
its own output. What remains genuinely necessary (~⅓): semantic validation (collision checks,
index/column coherence, scheduled-table shape), the canonical `ModuleDef` model itself, the
migration differ, and the codegen typespace. Budget roughly 4–6k LOC, not 15k.

---

## What Fluxum will face

1. **The boundary tax we skip is real and measured.** SpacetimeDB itself tracks `abi_duration`
   per reducer because it matters: every row read is host-side bsatn encode → linear-memory copy
   → guest decode (chunked through a pooled `ChunkedWriter`), every write is the reverse, plus
   handle slabs, errno mapping, and fuel bookkeeping (`set_fuel`/`get_fuel` per call). Native
   Fluxum reducers can receive borrowed typed rows and skip all of it — but we should keep the
   *shape* of their instrumentation (per-syscall timing, total vs. datastore-time split) as
   first-class metrics, since it's how they diagnose module performance.

2. **We must rebuild, natively: registration → validation → dispatch.**
   - *Macro → registry*: their `__preinit__*` export + `DESCRIBERS` + `__describe_module__`
     pipeline maps to `linkme`/`inventory` distributed slices in a static binary: each
     `#[fluxum::table]`/`#[fluxum::reducer]` registers a typed descriptor + an
     `fn(&ReducerContext, &[u8]) -> Result` (or fully typed fn pointer — we don't even need the
     byte-level indirection). Keep their invariant: reducer IDs are stable positional indices in
     a canonically ordered def, shared with the client protocol.
   - *Validation*: even with trusted native modules, semantic validation (duplicate names,
     index/column coherence, lifecycle uniqueness, scheduled-table shape) is needed at startup,
     with `ErrorStream`-style accumulate-all-errors reporting. Skip the raw-def wire versions.
   - *Dispatch table*: `ModuleInfo { Arc<ModuleDef>, dispatch: Vec<ReducerFn> }` pinned per
     module; args deserialization from JSON/wire happens once host-side against the def's
     `ProductType` (their `ArgsTuple` with memoized encodings is worth copying for event
     logging/broadcast).

3. **Panic isolation without a sandbox.** Copy the tx-ownership discipline: the tx lives in a
   host-owned slot (`TxSlot`), the module never owns commit/rollback; wrap dispatch in
   `catch_unwind`; treat a panic as their `Trap` (rollback, log traceback, `FailedInternal`,
   count `wasm_instance_errors`-equivalent). Their "trapped instance is discarded" has no native
   analog — a panicked native module may have poisoned its own globals; Fluxum's answer is
   (a) strongly discourage module global state, (b) escalate repeated panics to process restart
   (which is our deploy unit anyway). Memory/UB safety is simply not recoverable in-process:
   document it as the explicit trade of the native model.

4. **Budgeting must be time-based.** Fuel is a wasm-only luxury; even SpacetimeDB's V8 lane falls
   back to wall-clock deadlines (and today ships it stubbed/unmetered). Adopt the
   `EnergyMonitor`-shaped trait (budget-before, record-after, fingerprint keying, Null impl for
   OSS) with `FunctionBudget` as a duration; enforcement = watchdog + cooperative checks at
   datastore-call boundaries (we control the host API the module calls into). Accept that a pure
   CPU loop in a reducer can only be caught by watchdog + restart.

5. **Restart-based deploys still need the migration machinery, minus the choreography.** What we
   drop: dual-module hot-swap via `watch::Sender`, in-memory throwaway validation instance
   (`check_module_validity` — our compile step + startup validation covers it), client-disconnect
   orchestration on breaking updates. What we keep from `auto_migrate.rs`: diff old stored
   catalog vs. new binary's `ModuleDef` at startup **before** opening for traffic; execute the
   plan in a single serializable tx; on failure, refuse to start (their equivalent: keep old
   module running). Their two-tier policy (`Compatible` vs. `BreakClients(token)`) and the
   exhaustive, human-readable rejection reasons (`ChangeColumnTypeSizeMismatch`, …) plus a
   `migrate-plan` dry-run CLI are directly worth porting — the dry-run becomes a subcommand of
   the Fluxum binary itself (`fluxum migrate --plan` against the data dir).

6. **Single-threaded reducer lane is a load-bearing simplification.** Per database: one pinned
   OS thread, strictly serialized reducers, no instance pool on the hot path; concurrency exists
   only for suspendable procedures (bounded semaphore pool). Fluxum's SPEC-003/004 should
   consciously decide whether to inherit this (simple, matches serializable tx + commit-order
   broadcast) before designing anything fancier; everything in their subscription/commit
   pipeline (`commit_and_broadcast_event` under the same call) assumes it.

7. **Context-surface parity checklist** (SPEC-001/004): timestamp injected per call;
   RNG seeded from that timestamp (replay determinism); `insert` panics / `try_insert` returns
   typed `UniqueConstraintViolation`/`AutoIncOverflow`; `update` only via unique/PK column
   handles; point vs. range index handles with typed prefix args; scheduled tables as
   rows-with-`ScheduleAt`; auto-inc writeback into the inserted row; JWT claims on the context.
   SpacetimeDB's API here is mature and battle-tested — diverge deliberately, not accidentally.
