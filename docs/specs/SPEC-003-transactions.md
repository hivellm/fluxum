# SPEC-003 — Transactions

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 3 · T3.1 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-11, FR-12, FR-15; NFR-03 |
| **Requirement prefix** | `TXN-` |
| **Source** | UzDB spec 05, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `TXN-xxx`. `MemStore`, `CommittedState`, `TxState`, and the `CommitLog` are
specified in [SPEC-002](SPEC-002-storage-engine.md); the `ReducerContext`/`TxHandle` API surface
in [SPEC-004](SPEC-004-reducers.md); the `TxUpdate` subscription message in
[SPEC-005](SPEC-005-subscriptions.md) and [SPEC-006](SPEC-006-protocol-fluxrpc.md).

## 1. Model

Fluxum uses a simplified MVCC model adapted from SpacetimeDB's proven design. The key
simplification: there is exactly **one writer per shard** at any time (single-writer guarantee).
This eliminates write-write conflicts and reduces MVCC to a simple **snapshot + delta** approach
rather than full MVCC with version chains:

- **Snapshot** — `CommittedState`: the last fully committed state, read lock-free by views and
  by default reducer reads.
- **Delta** — `TxState`: the in-flight transaction's private write set, merged into
  `CommittedState` atomically at commit or discarded at rollback.

```rust
pub struct TxState {
    pub tx_id: u64,
    pub inserts: HashMap<TableId, Vec<Row>>,
    pub deletes: HashMap<TableId, Vec<PrimaryKey>>,
}
```

## 2. ACID guarantees

- **TXN-001** Atomicity [P0]. Every reducer call SHALL be atomic: either all writes performed
  through the transaction's `TxHandle` commit to `CommittedState`, or none do.

  **Scenario: partial write rejection**
  ```
  Given  a reducer inserts rows A and B, then calls ctx.tx.insert::<C>() which fails
  When   the error propagates and the reducer returns Err
  Then   rows A, B, and C are NOT present in CommittedState
  ```

- **TXN-002** Consistency [P0]. After every commit, `CommittedState` SHALL satisfy all declared
  constraints:
  - primary key uniqueness (including composite primary keys, SPEC-001);
  - auto-increment counter monotonicity;
  - unique constraint violations are detected before commit and trigger rollback (§6).

- **TXN-003** Isolation [P0]. Readers SHALL always see the last fully committed state. A
  transaction in progress SHALL NOT be visible to any other reader (views, subscription
  evaluation, other clients). Within a single reducer, the default read methods do NOT see the
  transaction's own in-flight writes; read-your-own-writes is available only through the explicit
  `scan_pending` / `scan_all` family (§7, SPEC-004).

- **TXN-004** Durability [P0]. A committed transaction SHALL be appended to the `CommitLog`
  before the `ReducerResult` is returned to the caller. After that point, the transaction SHALL
  survive a process crash and be replayed on recovery (SPEC-002).

  **Durability gap (explicit design choice):** the `CommitLog` write is async — no fsync on the
  commit path. Worst-case data loss on an OS crash is bounded by the OS write-behind buffer
  (~50 ms). This trade buys the NFR-03 target (reducer commit p99 < 1 ms) and is acceptable for
  realtime application workloads; a process crash alone loses nothing already handed to the OS.

## 3. Single-writer per shard

- **TXN-010** Single-writer guarantee [P0]. Each shard SHALL process at most one reducer call at
  a time. Incoming `ReducerCall` messages for a shard SHALL be queued in arrival order and
  processed sequentially. There SHALL be at most one active `TxState` per shard at any time.

  ```rust
  // ShardHost main loop — the single writer for its shard
  loop {
      let call: ReducerCall = incoming_queue.recv().await; // bounded MPSC channel
      begin_transaction(&call);                            // TXN-020
      let outcome = execute_reducer(&call);                // SPEC-004
      commit_or_rollback(outcome);                         // TXN-021 / TXN-022
      send_result(call.id);
  }
  ```

- **TXN-011** Queue backpressure [P1]. The incoming reducer queue SHALL have a configurable
  bounded capacity (default: 1,000 entries). When the queue is full, the server SHALL return
  `Error { code: 503, message: "shard busy" }` (SPEC-006) immediately rather than blocking the
  accepting task.

- **TXN-012** No cross-shard transactions [P0]. A reducer running on shard N SHALL NOT directly
  invoke a reducer on shard M within the same transaction. Cross-shard state changes require two
  separate reducer calls (one per shard), each with its own transaction boundary; atomic
  migration of a row set between shards goes through entity handoff
  ([SPEC-007](SPEC-007-sharding.md)).

  This is an explicit design choice to avoid distributed transaction complexity (two-phase
  commit, distributed deadlock detection).

  **Anti-pattern (forbidden):**
  ```rust
  #[fluxum::reducer]
  fn relocate_user_cross_shard(ctx: &ReducerContext, target_shard: u32) -> Result<(), String> {
      // FORBIDDEN: calling a reducer on another shard inside the same transaction.
      // No such API exists on TxHandle by design.
      ctx.tx.call_remote_shard(target_shard, "receive_user", args)?; // NOT ALLOWED
      Ok(())
  }
  ```

## 4. Transaction lifecycle

- **TXN-020** Transaction begin [P0]. A transaction SHALL begin when a `ReducerCall` is dequeued
  for execution. The runtime SHALL:
  1. Allocate a new `TxState { tx_id, inserts: HashMap::new(), deletes: HashMap::new() }`.
  2. Assign the next monotonic `tx_id` (per-shard counter, starting at 1).
  3. Set `MemStore.tx = Some(tx_state)`.
  4. Pass the `TxHandle` (wrapping `TxState`) to the reducer via `ReducerContext`.

- **TXN-021** Transaction commit sequence [P0]. On successful reducer completion:

  ```
  1.  Validate constraints (pk uniqueness, unique indexes)          — §6
  2.  If validation fails: rollback (TXN-022) and return Err to caller
  3.  Acquire CommittedState write lock
  4.  Apply TxState.inserts → CommittedState
  5.  Apply TxState.deletes → CommittedState
  6.  Update B-tree indexes
  7.  Update spatial indexes (if applicable)                        — SPEC-008
  8.  Release CommittedState write lock
  9.  Enqueue TxRecord on the CommitLog writer channel              — SPEC-002
  10. Evaluate subscriptions: SubscriptionManager::on_commit(&tx_state) — SPEC-005
  11. Set MemStore.tx = None
  12. Send ReducerResult { outcome: Ok(...) } to caller
  ```

  Steps 9 and 10 happen concurrently: the `CommitLog` write and the subscription fan-out are
  both async operations. Step 12 (result to caller) happens after step 8 (in-memory commit) but
  SHALL NOT wait for the disk write to complete or for subscription delivery. The merge
  (steps 3–8) SHALL be atomic from the perspective of other readers — no intermediate state
  SHALL be observable.

- **TXN-022** Transaction rollback sequence [P0]. On reducer error — `Err` return value, or a
  panic caught by the runtime's `std::panic::catch_unwind` boundary (SPEC-004):

  ```
  1. Discard TxState (all inserts and deletes are discarded)
  2. Set MemStore.tx = None
  3. Send ReducerResult { outcome: Err(message) } to caller
  4. No CommitLog entry is written
  5. No subscription events are generated
  ```

  A panic SHALL produce the same rollback as an `Err` and SHALL NOT crash the shard.

## 5. Transaction ID (`tx_id`)

- **TXN-030** Monotonic `tx_id` per shard [P0]. Each shard SHALL maintain a `u64` transaction
  counter. Every committed transaction SHALL receive a unique, monotonically increasing `tx_id`.
  Rolled-back transactions SHALL NOT consume a `tx_id`.

  The counter SHALL be persisted via the `CommitLog`: on recovery, the counter is set to the
  `tx_id` of the last replayed entry (SPEC-002).

- **TXN-031** `tx_id` in `TxUpdate` [P0]. Every `TxUpdate` message sent to subscribers SHALL
  carry the `tx_id` of the committed transaction (SPEC-005, SPEC-006). Clients MAY use `tx_id`
  to detect gaps (missed updates) and request resynchronisation.

## 6. Constraint checking

- **TXN-040** Primary key uniqueness [P0]. Before applying inserts (step 1 of the commit
  sequence), the runtime SHALL check that no inserted row's primary key — single-column or
  composite (`#[fluxum::table(primary_key(a, b))]`, SPEC-001) — already exists in
  `CommittedState`. Violation SHALL trigger rollback with
  `Err("primary key conflict: table=<name> pk=<value>")`.

  Exception: `ctx.tx.upsert::<T>` SHALL replace the existing row rather than erroring.

- **TXN-041** Unique constraints [P1]. Before applying inserts, the runtime SHALL check all
  declared unique constraints (`#[unique]` columns, SPEC-001). Violation SHALL trigger rollback
  with `Err("unique constraint violation: ...")`.

- **TXN-042** `auto_inc` assignment timing [P0]. `#[auto_inc]` IDs SHALL be assigned during the
  `TxState` write (when `ctx.tx.insert` is called), not during commit. This ensures the reducer
  can observe the assigned ID immediately after the insert call (e.g., to link related rows):

  ```rust
  #[fluxum::reducer]
  fn send_chat(ctx: &ReducerContext, channel: u32, text: String) -> Result<(), String> {
      let msg = ctx.tx.insert::<ChatMessage>(ChatMessage {
          id: 0, // auto_inc placeholder
          sender: ctx.identity,
          channel,
          content: text,
          sent_at: ctx.timestamp,
      })?;
      debug_assert_ne!(msg.id, 0); // ID assigned at insert time, not at commit
      Ok(())
  }
  ```

  IDs assigned inside a transaction that subsequently rolls back are NOT reused; `auto_inc`
  guarantees uniqueness and monotonicity, not density.

## 7. Intra-transaction visibility

- **TXN-050** Default reads see the pre-transaction snapshot [P0]. The default read methods —
  `query_pk::<T>`, `scan::<T>`, `scan_where::<T>`, and index lookups — SHALL read from
  `CommittedState` only. They SHALL NOT see uncommitted inserts or deletes from the same
  transaction's `TxState`. This gives every reducer consistent snapshot semantics and prevents
  accidental self-referential loops in reducers that call other reducers.

  **Scenario: default read isolation**
  ```
  Given  a transaction has inserted row R into TxState
  When   the same transaction calls ctx.tx.scan::<T>() on the same table
  Then   row R is NOT visible — only CommittedState rows are returned
  ```

- **TXN-051** Explicit intra-transaction reads [P0]. Read-your-own-writes SHALL be available
  only through the explicit `TxHandle` methods (full signatures in SPEC-004):
  - `scan_pending::<T>()` — rows inserted in THIS transaction (reads `TxState.inserts` only);
  - `count_pending::<T>(pred)` — count of pending inserts matching a predicate;
  - `scan_all::<T>()` / `scan_all_where::<T>(pred)` — combined view: `CommittedState` rows plus
    pending inserts, deduplicated by primary key (a pending insert or upsert wins over the
    committed row with the same primary key).

  The visibility split is part of the module API: which method a reducer uses is an explicit,
  reviewable statement of whether it intends to observe its own in-flight writes.

## 8. Read-only transactions (views)

- **TXN-060** View reads do not block writes [P0]. `#[fluxum::view]` functions read directly
  from `CommittedState` without acquiring the write lock. View execution and reducer execution
  MAY overlap concurrently.

- **TXN-061** View snapshot isolation [P0]. A view function SHALL see a consistent snapshot of
  `CommittedState` at the moment it begins reading. If a reducer commits while a view is
  executing, the view SHALL NOT see the partial or post-commit state — it sees the snapshot from
  when it started.

  Implementation: the view reads under a read lock held for the duration of the view function.
  The `CommittedState` write lock (taken by commits, TXN-021 steps 3–8) is exclusive, so a view
  either starts before a commit (and reads the pre-commit state) or after it (and reads the
  post-commit state) — never a mix.

## Acceptance criteria

1. **Atomicity property test** (proptest): reducers performing arbitrary sequences of
   inserts/deletes/upserts that end in `Err` or panic leave `CommittedState` byte-identical to
   the pre-transaction state; successful reducers apply exactly their write set (TXN-001,
   TXN-022).
2. **Constraint suite**: duplicate single-column and composite PK inserts roll back with
   `primary key conflict: table=<name> pk=<value>`; `#[unique]` violations roll back with the
   unique-constraint error; `upsert` on an existing PK replaces the row and commits (TXN-040,
   TXN-041).
3. **`auto_inc` timing**: the row value returned by `ctx.tx.insert` carries the assigned ID
   before commit; IDs consumed by rolled-back transactions are not reused (TXN-042).
4. **Single-writer serialization**: N concurrent clients firing reducers at one shard produce a
   serial commit history (per-`tx_id` order with no interleaved effects); filling the reducer
   queue past its configured capacity returns `503 "shard busy"` immediately without blocking
   the transport (TXN-010, TXN-011).
5. **`tx_id` monotonicity across recovery**: a workload mixing commits and rollbacks yields
   gap-free, strictly increasing `tx_id`s in the `CommitLog`; after kill-9 and replay, the next
   commit uses `last_replayed_tx_id + 1` (TXN-030).
6. **Durability**: kill-9 the process immediately after a `ReducerResult` is received — the
   committed transaction is present after recovery (process-crash case; the ~50 ms OS
   write-behind gap applies only to OS crashes) (TXN-004).
7. **Intra-transaction visibility**: within one reducer, `scan` never returns pending rows,
   `scan_pending` returns exactly the pending rows, and `scan_all` returns the deduplicated
   union (TXN-050, TXN-051; FR-17 tests live with SPEC-004).
8. **View isolation under load**: views running concurrently with a committing write workload
   always observe either the full pre-commit or full post-commit state, never a mix
   (TXN-060, TXN-061).
9. **Panic isolation**: a deliberately panicking reducer rolls back, returns an error to the
   caller, writes no `CommitLog` entry, emits no subscription events, and the shard continues
   serving subsequent calls (TXN-022).
10. **NFR-03 benchmark** (criterion): commit p99 of the `update_reading` small-write reducer
    < 1 ms with async log writes enabled (TXN-004, TXN-021).
