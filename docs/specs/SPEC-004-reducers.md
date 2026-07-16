# SPEC-004 — Reducers

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 3 · T3.2–T3.5 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-01, FR-17, FR-20..FR-27 |
| **Requirement prefix** | `RED-` |
| **Source** | UzDB spec 04, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `RED-xxx`. Transaction machinery (`TxState`/`CommittedState`, commit pipeline,
rollback) is [SPEC-003](SPEC-003-transactions.md); the `ReducerCall`/`ReducerResult` wire messages
and the HTTP admin endpoints are [SPEC-006](SPEC-006-protocol-fluxrpc.md); identity derivation is
[SPEC-009](SPEC-009-authentication.md); spatial queries are [SPEC-008](SPEC-008-spatial-indexes.md).

## 1. Overview

Application logic in Fluxum is expressed as three kinds of server-side functions:

| Kind | Attribute | Caller | Mutates state | Description |
|------|-----------|--------|---------------|-------------|
| Reducer | `#[fluxum::reducer]` | Any authenticated client or trusted backend service | Yes | Primary mutation API |
| View | `#[fluxum::view]` | HTTP admin/tools (`GET /view/:name`) | No | Read-only computed query |
| Procedure | `#[fluxum::procedure]` | HTTP only (`POST /procedure/:name`) | Yes | Admin and server-to-server operations |

All three are plain Rust functions in the application crate. They are collected at link time and
registered with the Fluxum runtime at startup via `fluxum::ServerBuilder` — no source scanning, no
WASM sandbox, no dynamic loading (FR-01, FR-03). Lifecycle hooks (§3) and scheduled reducers (§4)
are specializations of the reducer form.

## 2. Reducers

- **RED-001** Reducer declaration [P0] — A function annotated with `#[fluxum::reducer]` SHALL be
  callable by authenticated clients via a `ReducerCall` message (FluxRPC TCP :15801, Streamable
  HTTP `/rpc` on :15800, or `POST /reducer/:name` on the HTTP admin API — SPEC-006). The function SHALL
  receive a `&ReducerContext` as its first parameter, followed by any user-supplied arguments.
  The `#[fluxum::reducer]` macro SHALL generate the dispatch glue that decodes the `FluxValue`
  argument list of the `ReducerCall` into the declared parameter types; an argument count or type
  mismatch SHALL fail the call with an error before any transaction is started.

  ```rust
  #[fluxum::reducer]
  fn update_reading(ctx: &ReducerContext, grid_x: i32, grid_y: i32, value: f64) -> Result<(), String> {
      let mut sensor = ctx.tx.query_pk::<Sensor>((grid_x, grid_y))
          .ok_or_else(|| "unknown sensor".to_string())?;
      sensor.reading = value;
      sensor.updated_at = ctx.timestamp;
      ctx.tx.upsert::<Sensor>(sensor)?;
      Ok(())
  }
  ```

- **RED-002** ReducerContext [P0] — The system SHALL provide a `ReducerContext` to every reducer
  at call time:

  ```rust
  pub struct ReducerContext {
      pub identity:      Identity,     // 256-bit caller identity, stable across sessions (SPEC-009)
      pub connection_id: ConnectionId, // ephemeral per-connection identifier (u128)
      pub timestamp:     Timestamp,    // call timestamp (µs since Unix epoch)
      pub shard_id:      u32,          // shard this reducer runs on
      pub tx:            TxHandle,     // read/write handle bound to this call's transaction
  }
  ```

  The UzDB `entity_id` field was specific to its original domain and does not exist in Fluxum;
  rows are addressed exclusively through table primary keys.

- **RED-003** TxHandle operations [P0] — `TxHandle` SHALL expose the following operations for use
  within a reducer:

  ```rust
  impl TxHandle {
      // --- Writes ---
      pub fn insert<T: Table>(&mut self, row: T) -> Result<(), String>;    // error on PK conflict
      pub fn upsert<T: Table>(&mut self, row: T) -> Result<(), String>;    // insert or replace by primary key
      pub fn delete<T: Table>(&mut self, pk: T::PrimaryKey) -> Result<(), String>;  // delete row by primary key
      pub fn delete_where<T: Table>(&mut self, pred: impl Fn(&T) -> bool) -> Result<u64, String>; // rows deleted

      // --- Committed-state reads (CommittedState only — pre-transaction snapshot) ---
      pub fn query_pk<T: Table>(&self, pk: T::PrimaryKey) -> Option<T>;    // lookup row by pk
      pub fn query_index<T: Table>(&self, column: &str, value: FluxValue) -> Vec<T>; // lookup by indexed column
      pub fn scan<T: Table>(&self) -> Vec<T>;                              // full table scan (CommittedState)
      pub fn scan_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Vec<T>;      // filtered scan

      // --- Intra-transaction reads (TxState only — sees in-flight inserts) ---
      pub fn scan_pending<T: Table>(&self) -> Vec<T>;                      // rows inserted in THIS transaction
      pub fn count_pending<T: Table>(&self, pred: impl Fn(&T) -> bool) -> u64; // count pending inserts matching pred

      // --- Combined reads (CommittedState + TxState inserts, deduplicated by PK) ---
      pub fn scan_all<T: Table>(&self) -> Vec<T>;                          // committed rows + pending inserts
      pub fn scan_all_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Vec<T>;

      // --- Spatial (tables with #[spatial(...)] — SPEC-008) ---
      pub fn spatial_radius<T: Table>(&self, x: f32, y: f32, r: f32) -> Vec<T>;
      pub fn spatial_region<T: Table>(&self, x1: f32, y1: f32, x2: f32, y2: f32) -> Vec<T>;

      // --- Reducer delegation (same transaction — RED-005) ---
      pub fn call(&mut self, reducer: &str, args: Vec<FluxValue>) -> Result<(), String>;
  }
  ```

  **Note on read isolation (FR-17):** `scan::<T>()` and `query_pk::<T>()` read from
  `CommittedState` only (the pre-transaction snapshot). Use `scan_pending::<T>()` to read rows
  inserted in the current transaction, or `scan_all::<T>()` for the combined view. This
  distinction prevents accidental self-referential loops in recursive reducers.

- **RED-004** Reducer atomicity [P0] — Every reducer call SHALL execute inside exactly one
  database transaction (FR-11). If the reducer returns `Err`, or panics, the transaction SHALL be
  fully rolled back. No partial writes SHALL be visible.

  ```
  Given  a reducer inserts row A, then returns Err("reason")
  When   the ReducerResult is sent to the caller
  Then   row A is NOT present in CommittedState
  And    the caller receives ReducerResult { outcome: Err("reason") }
  ```

- **RED-005** Reducer-calls-reducer (shared transaction) [P0] — A reducer MAY call another
  reducer via `ctx.tx.call(name, args)`. The inner call SHALL execute within the same transaction
  as the outer call. The inner call SHALL NOT start a new transaction; it shares the outer
  `TxState`. If the inner call returns `Err`, the error SHALL propagate upward — the calling
  reducer MAY handle it or let it bubble up to cause a full rollback.

  ```
  Given  reducer A calls reducer B in the same transaction
  When   reducer B returns Err("task not found")
  And    reducer A does not handle the error
  Then   the entire transaction (A's and B's writes) is rolled back
  ```

- **RED-006** Reducer registration & dispatch [P0] — Reducer functions SHALL be collected at link
  time (`inventory`/`linkme`) and registered at startup by `fluxum::ServerBuilder` into a
  `HashMap<String, ReducerFn>` keyed by function name. Duplicate reducer names SHALL be a startup
  error. A `ReducerCall` naming an unregistered reducer SHALL be rejected with an error, without
  starting a transaction.

- **RED-007** Reducer versioning [P2] — A reducer MAY declare a version:
  `#[fluxum::reducer(version = 2)]`. The system SHALL maintain reducers under a composite key
  `(name, version)`. Calling `ReducerCall { reducer: "update_reading", version: 1, .. }` SHALL
  invoke the v1 implementation. If `version` is omitted from the call, the latest version SHALL
  be used. This enables clients using older SDKs to continue calling deprecated reducer versions
  without breaking (FR-27; evolution policy in [SPEC-010](SPEC-010-schema-migration.md)).

## 3. Lifecycle reducers

- **RED-010** `#[fluxum::on_init]` [P0] — A function annotated with `#[fluxum::on_init]` SHALL be
  called exactly once, the first time a shard starts with an empty `CommittedState` (no snapshot
  and no commit log). It SHALL NOT be called on recovery restarts. Typical use: insert seed data,
  initialize application configuration and constants.

  ```rust
  #[fluxum::table(private)]
  pub struct AppConfig {
      #[primary_key] pub id: u32,
      pub max_sessions: u32,
      pub default_channel: u32,
  }

  #[fluxum::on_init]
  fn init(ctx: &ReducerContext) -> Result<(), String> {
      ctx.tx.insert::<AppConfig>(AppConfig { id: 0, max_sessions: 10_000, default_channel: 1 })?;
      Ok(())
  }
  ```

- **RED-011** `#[fluxum::on_connect]` [P0] — A function annotated with `#[fluxum::on_connect]`
  SHALL be called when an authenticated client establishes a connection. The function SHALL
  receive the client's `Identity` and `ConnectionId` through the `ReducerContext`. It SHALL run
  inside a transaction (full rollback on error).

  ```rust
  #[fluxum::on_connect]
  fn on_connect(ctx: &ReducerContext) -> Result<(), String> {
      ctx.tx.upsert::<OnlineUser>(OnlineUser {
          identity:      ctx.identity,
          connection_id: ctx.connection_id,
          connected_at:  ctx.timestamp,
      })?;
      Ok(())
  }
  ```

- **RED-012** `#[fluxum::on_disconnect]` [P0] — A function annotated with
  `#[fluxum::on_disconnect]` SHALL be called when a client's connection is dropped (clean close
  or TCP timeout). It SHALL run inside a transaction.

  ```rust
  #[fluxum::on_disconnect]
  fn on_disconnect(ctx: &ReducerContext) -> Result<(), String> {
      ctx.tx.delete::<OnlineUser>(ctx.identity)?;
      Ok(())
  }
  ```

- **RED-013** `#[fluxum::on_shard_start]` [P1] — A function annotated with
  `#[fluxum::on_shard_start]` SHALL be called every time a shard starts, including recovery
  restarts. It SHALL run after the `CommittedState` has been fully recovered but before the shard
  accepts new `ReducerCall` messages. Typical use: warm caches, rebuild derived in-memory state,
  log shard startup.

## 4. Scheduled reducers

- **RED-020** `#[fluxum::tick]` — declarative periodic execution [P0] — A function annotated with
  `#[fluxum::tick(rate = N)]` SHALL be called by the runtime at the specified rate (in Hz) using
  a **fixed-timestep, no-accumulation** scheduler (FR-21). Each invocation SHALL execute as a
  full reducer transaction. Typical use: high-frequency simulation steps, rolling aggregation,
  expiry sweeps.

  ```rust
  #[fluxum::tick(rate = 60)]
  fn refresh_alerts(ctx: &ReducerContext) -> Result<(), String> {
      for sensor in ctx.tx.scan_where::<Sensor>(|s| s.reading > ALERT_THRESHOLD) {
          // fold into aggregates, raise or refresh alert rows
      }
      Ok(())
  }
  ```

  **Precise drift semantics (fixed-timestep clock):**

  The scheduler tracks an absolute `next_target_time` clock:

  ```
  period_us = 1_000_000 / rate
  next_target_time = startup_time_us

  loop:
      now = clock_us()
      if now < next_target_time:
          sleep(next_target_time - now)   // sleep until scheduled time
      run_tick_transaction()
      next_target_time += period_us       // advance by exactly one period
      if now > next_target_time + (3 * period_us):
          LOG WARNING "tick budget exceeded by >3 periods"
          next_target_time = clock_us()   // reset — do not accumulate backlog
  ```

  This is the classic fixed timestep with missed-deadline detection used by realtime schedulers:

  - If a tick finishes early: sleep until the next scheduled slot (smooth, regular cadence).
  - If a tick takes slightly longer than the period: the next tick fires immediately (no sleep).
  - If a tick exceeds 3× the period: log a warning and reset the clock (no unbounded backlog).

  The system SHALL guarantee at most one concurrent `#[fluxum::tick]` execution per function per
  shard.

- **RED-021** `#[fluxum::schedule]` — one-shot deferred reducer [P0] — A function annotated with
  `#[fluxum::schedule(delay_ms = N)]` (enqueued at shard start), or any reducer scheduled
  dynamically via `ctx.schedule_after(delay, reducer, args)`, SHALL be enqueued for execution
  after the specified delay (FR-22).

  ```rust
  impl ReducerContext {
      /// Enqueue `reducer` for execution after `delay`. The enqueue is part of the
      /// current transaction: if it rolls back, the scheduled call is discarded.
      pub fn schedule_after(&self, delay: Duration, reducer: &str, args: Vec<FluxValue>)
          -> Result<(), String>;
  }
  ```

  Scheduled entries SHALL be stored in the system table `__schedule__`:

  ```rust
  #[fluxum::table(private, name = "__schedule__")]
  pub struct ScheduleEntry {
      #[primary_key] #[auto_inc]
      pub id:            u64,
      pub reducer_name:  String,
      pub args:          Vec<u8>,  // MessagePack-encoded FluxValue list
      pub execute_at_us: i64,      // microseconds since Unix epoch
      pub shard_id:      u32,
  }
  ```

  A `ScheduleWorker` per shard SHALL poll `__schedule__` and fire due entries
  (`execute_at_us <= now`). Each fired entry SHALL execute as a full reducer transaction and its
  row SHALL be removed from `__schedule__`. Because `__schedule__` is a persisted table, pending
  scheduled calls survive crash recovery.

  **Rollback safety SHALL be enforced at fire time**: when a timer fires, the `ScheduleWorker`
  SHALL re-read the committed `__schedule__` row before executing. If the row is absent — because
  the scheduling transaction rolled back, or the entry was deleted since — the firing SHALL be a
  no-op. Scheduling a deferred reducer inside a transaction that subsequently fails MUST have no
  effect: the committed row is the sole source of truth, and cancellation requires no timer
  unhooking — deleting the row cancels the schedule.
  *(adopted from SpacetimeDB analysis, file 08)*

- **RED-023** Delivery semantics: at-least-once, restart rescan, no backfill [P0] — Scheduled
  reducers SHALL have at-least-once delivery semantics. The `__schedule__` row SHALL be deleted
  (or, for recurring entries, rescheduled) only AFTER the scheduled reducer transaction has
  executed successfully; the removal SHALL be part of the same transaction as the execution, so
  that on success delivery is exactly-once, while a crash before the fired transaction commits
  leaves the row in place for re-delivery. On shard restart, the `ScheduleWorker` SHALL rescan
  all `__schedule__` entries and re-enqueue every row, deduplicating on re-enqueue (a schedule
  `id` already in the in-memory timer queue SHALL NOT be enqueued twice). Past-due entries
  (`execute_at_us < now` at recovery time) SHALL fire once, immediately — the system SHALL NOT
  backfill missed interval occurrences (no catch-up burst of missed ticks).
  *(adopted from SpacetimeDB analysis, file 08)*

- **RED-024** Interval rescheduling is anti-drift [P0] — When a recurring scheduled entry is
  rescheduled after execution, the next `execute_at_us` SHALL be computed from the INTENDED tick
  time of the entry that just fired (`intended_time + period`), NOT from the completion time of
  the handler. This keeps recurring schedules drift-free and is consistent with the
  fixed-timestep, absolute-target clock of `#[fluxum::tick]` (RED-020). The no-backfill rule of
  RED-023 still applies: if the intended time is already in the past by more than one period at
  reschedule time, the next occurrence SHALL be rebased to the present rather than accumulating
  a backlog. *(adopted from SpacetimeDB analysis, file 08)*

- **RED-025** Scheduled execution context and client-call rejection [P0] — Executions triggered
  by `#[fluxum::tick]` (RED-020) or by the `__schedule__` scheduler (RED-021..024) SHALL run
  under the server (database) identity ([SPEC-009](SPEC-009-authentication.md) §8) with a nil
  `ConnectionId` (`ConnectionId(0)`, reserved — never assigned to a real connection). Reducers
  that are schedule-only — declared with `#[fluxum::tick]` or `#[fluxum::schedule]` — SHALL NOT
  be callable by clients via `ReducerCall`: such calls SHALL be rejected with
  `Error { code: 403, message: "schedule-only reducer" }` before any transaction is started,
  unless the reducer explicitly opts in to client calls (e.g.
  `#[fluxum::schedule(..., client_callable = true)]`). Default rejection is a deliberate
  improvement over SpacetimeDB, where scheduled reducers remain client-callable unless module
  code checks the caller. *(adopted from SpacetimeDB analysis, file 08)*

- **RED-022** Recurring schedule [P0] — A reducer MAY reschedule itself to create a recurring
  pattern. This is the manual alternative to `#[fluxum::tick]` for low-frequency periodic work.

  ```rust
  const SESSION_TTL: Duration = Duration::from_secs(30 * 60);

  #[fluxum::reducer]
  fn purge_expired_sessions(ctx: &ReducerContext) -> Result<(), String> {
      let cutoff = ctx.timestamp - SESSION_TTL;
      ctx.tx.delete_where::<OnlineUser>(|u| u.connected_at < cutoff)?;
      // re-arm: a one-shot chain forms a recurring schedule
      ctx.schedule_after(Duration::from_secs(60), "purge_expired_sessions", vec![])?;
      Ok(())
  }
  ```

## 5. Views

- **RED-030** `#[fluxum::view]` declaration [P1] — A function annotated with `#[fluxum::view]`
  SHALL be callable via `GET /view/:name` as a read-only HTTP endpoint (FR-26). Views SHALL
  NOT receive a handle that permits writes. Views SHALL read directly from `CommittedState`.

  ```rust
  #[derive(Serialize)]
  pub struct ChannelStats { pub channel: u32, pub messages: u64 }

  #[fluxum::view]
  fn busiest_channels(ctx: &ViewContext, top_n: u32) -> Vec<ChannelStats> {
      let mut counts = HashMap::<u32, u64>::new();
      for msg in ctx.tx.scan::<ChatMessage>() {
          *counts.entry(msg.channel).or_default() += 1;
      }
      let mut stats: Vec<ChannelStats> = counts
          .into_iter()
          .map(|(channel, messages)| ChannelStats { channel, messages })
          .collect();
      stats.sort_unstable_by(|a, b| b.messages.cmp(&a.messages));
      stats.truncate(top_n as usize);
      stats
  }
  ```

- **RED-031** ViewContext [P1] — Views SHALL receive a `ViewContext` instead of a
  `ReducerContext`:

  ```rust
  pub struct ViewContext {
      pub timestamp: Timestamp,
      pub shard_id:  u32,
      pub tx:        ReadOnlyTxHandle, // only read operations permitted
  }
  ```

  `ReadOnlyTxHandle` SHALL expose only the read operations of RED-003 (`query_pk`, `query_index`,
  `scan`, `scan_where`, `spatial_radius`, `spatial_region`); write methods MUST NOT exist on the
  type, so a view that attempts to write fails to compile.

## 6. Procedures

- **RED-040** `#[fluxum::procedure]` declaration [P1] — A function annotated with
  `#[fluxum::procedure]` SHALL be callable via `POST /procedure/:name` as an HTTP endpoint
  (FR-26). Procedures can mutate state (they receive a full `TxHandle` through a
  `ReducerContext`). They are NOT callable by client applications via the FluxRPC protocol — only
  via HTTP. Typical use: admin tools, webhook handlers, server-to-server integration.

  ```rust
  #[fluxum::table(private)]
  pub struct SuspendedUser {
      #[primary_key] pub identity: Identity,
      pub reason: String,
      pub suspended_at: Timestamp,
  }

  #[fluxum::procedure]
  fn suspend_user(ctx: &ReducerContext, target: Identity, reason: String) -> Result<(), String> {
      ctx.tx.upsert::<SuspendedUser>(SuspendedUser {
          identity: target,
          reason,
          suspended_at: ctx.timestamp,
      })?;
      Ok(())
  }
  ```

## 7. Rate limiting

- **RED-050** Per-reducer rate limit annotation [P0] — A reducer MAY declare a call rate limit
  with `#[fluxum::reducer(max_rate = "N/s")]` (FR-24). The runtime SHALL enforce a rolling-window
  limit per `(Identity, reducer_name)` pair. Calls that exceed the limit SHALL be rejected with
  `Error { code: 429 }` **before** any `TxState` is created — zero storage cost for rejected
  calls.

  ```rust
  #[fluxum::reducer(max_rate = "5/s")]
  fn send_chat(ctx: &ReducerContext, channel: u32, text: String) -> Result<(), String> {
      // at most 5 calls per second per caller identity
      ctx.tx.insert::<ChatMessage>(ChatMessage {
          id: 0, // auto_inc
          sender: ctx.identity,
          channel,
          content: text,
          sent_at: ctx.timestamp,
      })?;
      Ok(())
  }

  #[fluxum::reducer(max_rate = "1/s")]
  fn rename_user(ctx: &ReducerContext, new_name: String) -> Result<(), String> {
      // at most 1 call per second per caller identity
      /* ... */
      Ok(())
  }
  ```

- **RED-051** Rate limit implementation [P0] — The rate limit counter SHALL be stored per shard
  in a `HashMap<(Identity, String), TokenBucket>` in the shard's memory (NOT in `CommittedState`
  — it is ephemeral, not persisted). `TokenBucket` tracks calls in a 1-second sliding window
  using a token-bucket algorithm:

  - Capacity = `max_rate` tokens.
  - Refill = 1 token per `1/max_rate` seconds.
  - On each call: consume 1 token; if the bucket is empty, reject with 429.

  Server-to-server identities (`SHA-256("SERVER:" + name)` — SPEC-009) are exempt from rate
  limits.

- **RED-052** Global shard-level rate limit [P1] — Each shard SHALL have a configurable global
  rate limit (`shard_max_reducers_per_sec`, default: 200,000). When the shard's total reducer
  throughput exceeds this limit, new `ReducerCall` messages SHALL receive
  `Error { code: 503, message: "shard overloaded" }`.

## 8. Error handling

- **RED-060** Reducer return type [P0] — All `#[fluxum::reducer]` and `#[fluxum::procedure]`
  functions MUST return `Result<T, String>` (`Result<(), String>` for void reducers). Returning
  `Err(msg)` SHALL trigger a full transaction rollback and send the error string to the caller.
  *(Amended by [SPEC-028](SPEC-028-error-catalog.md) ERR-012: the string reaches the caller as a
  structured `ReducerResult` outcome `[code, app_code, message]` with code 5001
  `REDUCER_USER_ERROR` and the message verbatim; a RED-061 panic is 5002 `REDUCER_PANIC`, never
  5001; other system-caused failures — unknown reducer, argument mismatch, rate limit,
  schedule-only — keep their own catalog codes as `Error` frames.)*

  ```rust
  #[fluxum::reducer]
  fn complete_task(ctx: &ReducerContext, task_id: u64) -> Result<(), String> {
      let task = ctx.tx.query_pk::<Task>(task_id)
          .ok_or_else(|| "task not found".to_string())?;
      if task.owner != ctx.identity {
          return Err("not the owner of this task".into()); // → full rollback, string sent to caller
      }
      ctx.tx.upsert::<Task>(Task { done: true, ..task })?;
      Ok(())
  }
  ```

- **RED-061** Panic isolation [P0] — Reducers SHALL run under `std::panic::catch_unwind`. If a
  reducer panics (index out of bounds, arithmetic error, explicit `panic!`, or any other
  unwinding panic), the runtime SHALL catch the unwind, discard the `TxState` (rolling back the
  transaction), and return `ReducerResult { outcome: Err("internal error: <details>") }` to the
  caller. The server SHALL NOT crash (FR-25). The panic SHALL be logged at ERROR level. Because
  `CommittedState` is never mutated before the commit point (SPEC-003), discarding the `TxState`
  restores all invariants, keeping the reducer path unwind-safe. Known limitation: a panic while
  already panicking (double panic) aborts the process — the workspace lints
  (`unwrap_used`/`expect_used` denied) exist to keep panics out of reducer code in the first
  place.

## Acceptance criteria

1. **Rollback on error** (RED-004): a reducer inserts a row then returns `Err` → the row is
   absent from `CommittedState` and the caller receives the exact error string; the same holds
   when the reducer panics instead of returning `Err`.
2. **Nested-call rollback** (RED-005): reducer A calls reducer B via `ctx.tx.call`; B returns
   `Err` and A propagates it → both A's and B's writes are rolled back; when A handles the error
   and returns `Ok`, A's writes commit.
3. **Intra-transaction read semantics** (RED-003, FR-17): within one reducer, insert *k* rows →
   `scan` excludes them, `scan_pending` returns exactly those *k* rows, `count_pending`
   matches its predicate, and `scan_all` returns committed ∪ pending deduplicated by primary key.
4. **Panic isolation soak** (RED-061): a deliberately panicking reducer called 10,000 times
   returns an internal-error result on every call while interleaved healthy calls keep
   succeeding; the process never exits and memory stays stable.
5. **Tick cadence and drift** (RED-020): a 60 Hz tick over 10 s executes 600 ± 1 times with no
   cumulative drift (absolute-target clock); an injected stall of 1–3 periods causes an immediate
   re-fire with no warning; a stall > 3 periods logs exactly one warning and resets the clock
   with no catch-up burst; the same tick function never runs concurrently with itself on a shard.
6. **Scheduling** (RED-021/022/023): `ctx.schedule_after` fires once after the delay and removes
   its `__schedule__` row in the same transaction as the execution; scheduling inside a
   rolled-back transaction never fires — the worker re-reads the committed row at fire time and
   an absent row is a no-op; pending entries survive `kill -9` + recovery: on restart every
   `__schedule__` row is re-enqueued exactly once (no duplicate enqueue for the same id), and a
   past-due entry fires once immediately with no backfill of missed occurrences; the
   self-rescheduling `purge_expired_sessions` example runs for at least 10 consecutive cycles.
   *(adopted from SpacetimeDB analysis, file 08)*
7. **Rate limiting** (RED-050/051): a 10-call burst against `max_rate = "5/s"` yields 5 accepted
   calls and 5 rejections with code 429 and zero `TxState` allocations; buckets are independent
   per `(Identity, reducer)`; refill restores capacity after the window; server-to-server
   identities are never limited.
8. **Shard overload** (RED-052): synthetic load above `shard_max_reducers_per_sec` receives
   `503 "shard overloaded"` on the excess calls only.
9. **Lifecycle** (RED-010..013): a fresh shard (no snapshot, no commit log) runs `on_init`
   exactly once; a restart from snapshot/log does not run `on_init` but runs `on_shard_start`
   after recovery and before the first accepted `ReducerCall`; connect/disconnect drive the
   `OnlineUser` presence rows end to end (PRD UC-1).
10. **Registry and versioning** (RED-006/007): duplicate reducer names abort startup with a clear
    error; a `ReducerCall` naming an unknown reducer returns an error without starting a
    transaction; a call with `version: 1` invokes the v1 implementation while an unversioned call
    invokes the latest.
11. **Recurring anti-drift** (RED-024): a recurring 1 s schedule whose handler takes 300 ms
    fires at t+1s, t+2s, t+3s (intended-time rebase, no 300 ms/cycle drift); a handler stalled
    past its intended slot rebases to the present without a catch-up burst.
    *(adopted from SpacetimeDB analysis, file 08)*
12. **Scheduled execution context** (RED-025): a fired schedule/tick observes
    `ctx.identity == server identity` and `ctx.connection_id == ConnectionId(0)`; a client
    `ReducerCall` naming a schedule-only reducer is rejected with code 403 and no transaction,
    while one marked `client_callable = true` is accepted.
    *(adopted from SpacetimeDB analysis, file 08)*
13. **Views and procedures** (RED-030/031/040): `ReadOnlyTxHandle` exposes no write methods
    (compile-fail test); `GET /view/:name` returns computed results; procedures execute via
    `POST /procedure/:name` and are rejected when invoked as FluxRPC `ReducerCall`s.
