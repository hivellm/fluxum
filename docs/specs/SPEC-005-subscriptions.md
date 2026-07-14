# SPEC-005 — Subscriptions

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 4 · T4.1–T4.4 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-30, FR-31, FR-32, FR-33, FR-34, FR-35; NFR-04, NFR-10 |
| **Requirement prefix** | `SUB-` |
| **Source** | UzDB spec 06, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `SUB-xxx`. Subscriptions live in `fluxum-core` (plan compilation, registration,
post-commit fan-out); message delivery rides the transports defined in
[SPEC-006](SPEC-006-protocol-fluxrpc.md). The wire encoding of every message shown here
(`Subscribe`, `InitialData`, `TxUpdate`, …) is normatively defined in SPEC-006; this spec defines
their semantics.

## 1. Overview

Subscriptions are the realtime data delivery mechanism. Clients subscribe to SQL queries and
receive automatic incremental diffs (`TxUpdate` messages) after every committed transaction that
touches their subscribed tables.

Clients never poll. The server pushes — this is the fundamental difference from REST+polling
architectures, and the core of the database-as-a-server model.

Cross-shard behavior (a query spanning multiple partitions) is handled by the ShardCoord
subscription aggregation defined in [SPEC-007](SPEC-007-sharding.md); this spec describes the
per-shard mechanism.

## 2. Subscription lifecycle

- **SUB-001** [P0] **Subscribe message.** A client sends a `Subscribe` message with one or more SQL
  query strings. The server SHALL: (1) parse and compile each query into a `CompiledPlan`
  (SUB-020); (2) assign each query a `query_id` (unique per connection) and register the plans
  under the client's `ConnectionId`; (3) immediately evaluate the plans against `CommittedState`
  ([SPEC-002](SPEC-002-storage-engine.md)) and send `InitialData` with all currently matching rows.
  The assigned `query_id`s are the handles later used by `Unsubscribe` (SUB-004).

  ```rust
  pub struct Subscribe {
      pub id: u32,              // message ID for correlation
      pub queries: Vec<String>, // SQL query strings
  }
  ```

- **SUB-002** [P0] **InitialData response.** After registering subscriptions, the server SHALL send
  one `InitialData` message containing the current rows for all subscribed queries. The client's
  local cache is initialized from `InitialData`; from this point forward, the client maintains the
  cache solely by applying `TxUpdate` diffs.

  ```rust
  pub struct InitialData {
      pub id: u32,                  // echoes Subscribe.id / SubscribeSingle.id
      pub tables: Vec<TableUpdate>,
  }

  pub struct TableUpdate {
      pub table_id: u32,
      pub table_name: String,
      pub inserts: Vec<Row>,        // current rows matching this query
      pub deletes: Vec<Row>,        // empty for InitialData
  }
  ```

- **SUB-003** [P0] **TxUpdate push.** After every committed transaction, the system SHALL:
  (1) determine which committed mutations (inserts and deletes) match each registered plan;
  (2) apply the `#[visibility]` / RLS filter (§5) for each matching row and each subscribed client;
  (3) for each client with at least one matching row, send one `TxUpdate` message. If a commit
  produces zero matching rows for a client, no `TxUpdate` SHALL be sent to that client for that
  commit.

  ```rust
  pub struct TxUpdate {
      pub tx_id: u64,               // monotonically increasing transaction ID
      pub tables: Vec<TableUpdate>,
  }
  ```

- **SUB-004** [P0] **Unsubscribe.** A client sends an `Unsubscribe` message to cancel one or more
  subscriptions by their server-assigned `query_id`s. The server SHALL remove the specified
  `CompiledPlan` entries and stop delivering `TxUpdate` events for those queries.

  ```rust
  pub struct Unsubscribe {
      pub id: u32,
      pub query_ids: Vec<u32>,
  }
  ```

- **SUB-005** [P0] **Disconnect cleanup.** When a client disconnects (clean or timeout), the server
  SHALL remove all `CompiledPlan` entries for that `ConnectionId` from the `SubscriptionManager`.
  No `TxUpdate` events SHALL be delivered after disconnect.

- **SUB-006** [P1] **SubscribeSingle for granular subscription management.** In addition to
  `Subscribe { queries: Vec<String> }`, the server SHALL accept
  `SubscribeSingle { id: u32, query: String }` to subscribe to exactly one query. The server SHALL
  return `InitialData` with a single `TableUpdate` for the query, including the assigned
  `query_id`. This enables clients to add or remove individual subscriptions without
  re-registering the entire subscription set.

## 3. SQL subscription queries

- **SUB-010** [P0] **Supported SQL subset.** Subscription queries SHALL support the following SQL
  operations:

  ```sql
  SELECT * FROM <table>
  SELECT * FROM <table> WHERE <column> = <value>
  SELECT * FROM <table> WHERE <column> IN (<value>, ...)
  SELECT * FROM <table> WHERE <column> BETWEEN <v1> AND <v2>
  SELECT * FROM <table> WHERE <column> = <v1> AND <column2> = <v2>
  SELECT * FROM <table> ORDER BY <column> [ASC|DESC] LIMIT <n>
  ```

- **SUB-011** [P0] **Geospatial SQL extensions.** For tables with `#[spatial(quadtree(x, y))]` or
  `#[spatial(rtree(...))]`, the following spatial predicates SHALL be supported (grammar and index
  evaluation are defined in [SPEC-008](SPEC-008-spatial-indexes.md)):

  ```sql
  SELECT * FROM <table> IN REGION (<x>, <y>, <w>, <h>)
  SELECT * FROM <table> WITHIN RADIUS <r> OF (<x>, <y>)
  ```

  Spatial predicates SHALL be evaluated through the table's spatial index so that only candidate
  rows near the region/center are touched (see SUB-022). A subscriber whose anchor point moves
  (e.g., a vehicle-tracking dashboard following a vehicle) re-issues the radius query with new
  coordinates; `SubscribeSingle` + `Unsubscribe` (SUB-004/SUB-006) make this cheap.

- **SUB-012** [P0] **Unsupported SQL.** The following SQL constructs SHALL NOT be supported in
  subscription queries:

  - `JOIN` (use separate subscriptions per table)
  - `GROUP BY`, `HAVING`, aggregate functions (use `#[fluxum::view]` for aggregates)
  - `INSERT`, `UPDATE`, `DELETE` (subscriptions are read-only)
  - Subqueries
  - `WITH` (CTEs)

  A query using unsupported syntax SHALL be rejected at subscription time with
  `Error { code: 400, message: "unsupported query syntax: ..." }` (error message type per
  SPEC-006).

- **SUB-013** [P0] **ORDER BY and LIMIT apply to InitialData only.** `ORDER BY` and `LIMIT` clauses
  in subscription queries apply **only to `InitialData`** (the initial snapshot). They do NOT apply
  to subsequent `TxUpdate` diffs.

  Rationale: `TxUpdate` delivers a delta (rows added or removed in a single transaction), not a
  full ordered result set. Applying ORDER BY/LIMIT to diffs would require materializing the full
  result set on every commit to determine what is "in" vs "out" of the top-N — a prohibitive cost.

  Clients that need ordered, bounded sets SHALL apply sorting and limiting client-side to their
  local cache after receiving `TxUpdate` diffs.

  **Scenario: ORDER BY semantics**
  ```
  Given  a client subscribes to "SELECT * FROM ChatMessage WHERE channel = 1 ORDER BY sent_at DESC LIMIT 50"
  When   InitialData is sent
  Then   rows are ordered by sent_at DESC and at most 50 rows are returned
  When   a new ChatMessage is committed
  Then   TxUpdate.inserts contains the new row (unordered, no limit applied)
  And    the client is responsible for inserting it correctly in its local sorted cache
  ```

## 4. CompiledPlan

- **SUB-020** [P0] **Query compilation.** At subscription time, the server SHALL compile each SQL
  query string into a `CompiledPlan` exactly once. Subsequent transaction commits SHALL re-use the
  compiled plan rather than re-parsing the SQL.

  ```rust
  pub type FilterFn = Box<dyn Fn(&Row) -> bool + Send + Sync>;
  pub type RlsFn = Box<dyn Fn(&Row, &Identity) -> bool + Send + Sync>;

  pub struct CompiledPlan {
      pub query_id: u32,               // assigned by the server at Subscribe time
      pub table_ids: Vec<TableId>,     // which tables this plan reads from
      pub filter: Option<FilterFn>,    // compiled predicate function
      pub rls: Option<RlsFn>,          // row-level visibility filter (from #[visibility])
      pub order_by: Option<OrderSpec>, // InitialData only (SUB-013)
      pub limit: Option<u32>,          // InitialData only (SUB-013)
  }
  ```

- **SUB-021** [P0] **Plan evaluation on commit.** After every commit, the `SubscriptionManager`
  SHALL evaluate each registered plan against the delta rows (inserts and deletes in the committed
  `TxState`). The evaluation algorithm SHALL be:

  ```text
  for each plan registered by connected clients:        // candidates via table_watchers (SUB-040)
      if plan.table_ids ∩ tx_state.mutated_tables == ∅:
          skip                                          // fast path — no relevant table changed
      matching_inserts = []
      matching_deletes = []
      for row in tx_state.inserts[plan.table_ids]:
          if plan.filter(row) && plan.rls(row, client_identity):
              matching_inserts.push(row)
      for pk in tx_state.deletes[plan.table_ids]:
          // deletes are always sent if the client was subscribed to the row
          // (checked by looking up the pre-commit CommittedState)
          if client_was_subscribed_to(pk, plan, client_identity):
              matching_deletes.push(pk)
      if !matching_inserts.is_empty() || !matching_deletes.is_empty():
          enqueue TxUpdate for this client              // delivery subject to SUB-041/SUB-042
  ```

- **SUB-022** [P0] **Fan-out complexity.** The worst-case fan-out per commit SHALL be O(C × R),
  where C is the number of subscribed clients and R is the number of delta rows in the commit. For
  geospatial queries, the QuadTree/R-tree index SHALL reduce R to only nearby rows, bounding
  practical fan-out to O(C_local × R_local).

## 5. Row-level security (visibility)

- **SUB-030** [P1] **`#[visibility]` filter enforcement.** For tables with
  `#[visibility(owner_only(owner))]`, the subscription system SHALL automatically apply the filter
  before sending rows:

  ```text
  if row.owner != client.identity:
      do NOT include this row in TxUpdate for this client
  ```

  This check SHALL apply to both `InitialData` and all subsequent `TxUpdate` events.

  **Scenario: owner_only visibility**
  ```
  Given  table Task has #[visibility(owner_only(owner))]
  And    client Alice (identity A) subscribes to "SELECT * FROM Task"
  When   a reducer inserts Task { owner: B, title: "write report", done: false }
  Then   Alice's subscription does NOT receive the new row
  When   a reducer inserts Task { owner: A, title: "review PR", done: false }
  Then   Alice's subscription DOES receive the new row
  ```

- **SUB-031** [P1] **Server-peer bypass.** Connections authenticated with a server-to-server
  identity (`SHA-256("SERVER:" + name)`, [SPEC-009](SPEC-009-authentication.md)) SHALL bypass all
  `#[visibility]` filters: a trusted backend service subscribing to a table receives every row
  matching the plan's predicate, regardless of ownership, in both `InitialData` and `TxUpdate`.
  Because user tokens can never produce an identity in the `SERVER:` namespace (SPEC-009), the
  bypass cannot be forged from a client credential.

- **SUB-032** [P2] **Custom visibility function.** A table MAY declare
  `#[visibility(custom(my_filter))]` where `my_filter` is a Rust function with signature
  `fn(row: &T, ctx: &VisibilityContext) -> bool`. The function SHALL be called for each candidate
  row during plan evaluation.

## 6. SubscriptionManager structure

- **SUB-040** [P0] **SubscriptionManager layout.**

  ```rust
  pub struct SubscriptionManager {
      /// Registered plans per client connection.
      plans: HashMap<ConnectionId, Vec<CompiledPlan>>,
      /// Index: table_id → set of ConnectionIds with plans touching that table.
      table_watchers: HashMap<TableId, BTreeSet<ConnectionId>>,
  }
  ```

  The `table_watchers` index enables the fast-path skip in SUB-021: only clients watching a
  mutated table need plan evaluation.

- **SUB-041** [P0] **Thread safety and non-blocking commit path.** The `SubscriptionManager` SHALL
  be protected by an async mutex (`tokio::sync::Mutex`). `on_subscribe`, `on_unsubscribe`, and
  `on_commit` operations SHALL acquire this mutex. The commit path SHALL hold the mutex only for
  the duration of plan evaluation, then release it before sending FluxRPC messages — so
  network I/O to subscribers never blocks subsequent commits.

- **SUB-042** [P0] **Fan-out backpressure — per-client send buffer policy.** The broadcast fan-out
  loop SHALL NEVER block waiting for a single client's TCP/Streamable HTTP send buffer to drain. Each client
  connection SHALL have an independent send buffer with a three-tier policy based on buffer
  occupancy:

  | Tier | Condition | Behaviour |
  |------|-----------|-----------|
  | **Normal** | buffer < 50% | Deliver all `TxUpdate` messages immediately |
  | **Pressured** | buffer 50–90% | Deliver inserts only; skip tick-sourced updates (diffs produced by `#[fluxum::tick]` reducers, [SPEC-004](SPEC-004-reducers.md)) |
  | **Full** | buffer > 90% OR send blocked for > 5 s | Drop connection; log `WARN "subscriber dropped: send buffer full"`; increment `fluxum_subscriber_drops_total{shard, reason}` ([SPEC-012](SPEC-012-observability.md)) |

  The default per-client send buffer size is configurable (`subscription_send_buffer_bytes` in
  `config.yml`, default: 2 MB).

  **Rationale:** Without this policy, one client with a slow or intentionally blocked receive
  (e.g., a client on a poor network, or a malicious connection) can stall the fan-out loop for all
  other subscribers, introducing latency spikes for the entire server.

  **Scenario: slow client isolation**
  ```
  Given  1,000 clients are subscribed to Sensor updates
  And    client X has a full send buffer (bad network)
  When   a reducer commits a Sensor reading update
  Then   999 clients receive the TxUpdate within 1 ms
  And    client X is dropped after 5 s of failed delivery
  And    the other 999 clients are NOT affected
  ```

- **SUB-043** [P1] **Per-table send priority.** Delivery priority under pressure SHALL be
  configurable per public table via the `send_priority` annotation:

  ```rust
  #[fluxum::table(public, send_priority = high)]   // never dropped, even under pressure
  pub struct Notification { /* ... */ }

  #[fluxum::table(public, send_priority = normal)] // default — skipped in Pressured tier when tick-sourced
  pub struct Sensor { /* ... */ }

  #[fluxum::table(public, send_priority = low)]    // dropped first under any pressure
  pub struct ChatMessage { /* ... */ }
  ```

## 7. Materialized views (subscribable computed views)

- **SUB-050** [P2] **`#[fluxum::materialized_view]`.** A Rust function annotated with
  `#[fluxum::materialized_view]` SHALL define a computed view whose results are cached in a
  virtual table. Clients MAY subscribe to a materialized view as if it were a regular public
  table. The system SHALL recompute the view after each commit that touches any table the view
  reads from, and deliver incremental diffs to subscribers.

  ```rust
  #[fluxum::materialized_view]
  fn channel_activity() -> Vec<ChannelActivityRow> {
      // computed from the ChatMessage table
      // ...
  }
  ```

  **Status:** P2 — design decision deferred to post-launch. SpacetimeDB lacks this and forces
  denormalization; Fluxum should support it, but it is not on the critical path.

## Acceptance criteria

1. **Lifecycle round-trip** (SUB-001…SUB-006): `Subscribe` returns `InitialData` identical to a
   direct query of `CommittedState`; a subsequent commit produces exactly one `TxUpdate` per
   affected client; `Unsubscribe` by `query_id` and disconnect both stop delivery — no `TxUpdate`
   is ever observed after either event. `SubscribeSingle` returns the assigned `query_id`, and
   that id round-trips through `Unsubscribe`.
2. **SQL subset parser corpus** (SUB-010…SUB-012): every supported form compiles to a
   `CompiledPlan`; each unsupported construct (`JOIN`, `GROUP BY`/`HAVING`/aggregates, DML,
   subqueries, CTEs) is rejected with error code 400; an injection-attempt corpus (malformed and
   hostile query strings) never crashes the parser or produces a plan (DAG T4.1).
3. **ORDER BY/LIMIT semantics** (SUB-013): the SUB-013 scenario passes — `InitialData` ordered and
   limited, `TxUpdate` diffs unordered and unlimited.
4. **Compile-once check** (SUB-020): commit-path profiling shows zero SQL parsing after
   registration; plans are reused across commits.
5. **RLS matrix** (SUB-030/SUB-031, DAG T4.3): for an `owner_only` table, the matrix
   {owner, other user, server peer} × {`InitialData`, `TxUpdate`} yields: owner sees own rows
   only, other users see nothing, server-peer identity sees all rows (bypass verified in an
   integration test).
6. **Slow-consumer stress test** (SUB-041/SUB-042, DAG T4.4): with 1,000 subscribers and one
   client whose socket is blocked, the remaining 999 receive `TxUpdate` with p99 delivery
   latency < 5 ms (NFR-04); commit throughput is unaffected while the socket stays blocked
   (non-blocking guarantee); the blocked client is dropped after 5 s, the WARN line is logged, and
   `fluxum_subscriber_drops_total` increments.
7. **Fast-path skip** (SUB-021/SUB-040): a commit touching no watched tables performs no plan
   evaluation (verified via the `table_watchers` index).
8. **Subscription correctness property test** (NFR-10; runs under
   [SPEC-013](SPEC-013-testing-conformance.md), DAG T4.5): generate 10,000 random mutations
   (inserts/updates/deletes across random tables, including `owner_only` tables) against a
   population of clients holding random subscriptions; every client cache — initialized from
   `InitialData` and maintained solely by applying `TxUpdate` diffs — MUST equal the server-side
   query result for its subscriptions after every commit. Required accuracy: 100%.
