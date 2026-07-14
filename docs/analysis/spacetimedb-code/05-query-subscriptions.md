# 05 — Query Engine & Subscription Evaluation

Deep implementation analysis of the real SpacetimeDB source, focused on the SQL pipeline and the
incremental subscription (IVM) machinery. Compared throughout against Fluxum
[SPEC-005](../../specs/SPEC-005-subscriptions.md) (subscriptions), with implications for SPEC-002/008/012.

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` |
| **Analyzed crates** | `crates/sql-parser` (~1.6k LoC), `crates/expr` (~2.9k), `crates/physical-plan` (~3.6k), `crates/execution` (~1.7k), `crates/query` (101), `crates/subscription` (689), `crates/engine` (~6.8k), `crates/query-builder` (~1k), `crates/index-scan-gate` (82) |
| **Core modules** | `crates/core/src/subscription/` — `module_subscription_manager.rs` (3,220), `module_subscription_actor.rs` (4,329), `delta.rs`, `tx.rs`, `query.rs`, `execution_unit.rs`, `websocket_building.rs`; `crates/core/src/sql/execute.rs` (1,689); `crates/core/src/estimation.rs` |
| **Fluxum spec** | SPEC-005 (SUB-xxx requirements) |
| **Date** | 2026-07-14 |

---

## 1. Pipeline overview

The query/subscription pipeline is split across seven crates, one stage each:

```
SQL text
  │  crates/sql-parser        parse (sqlparser + PostgreSqlDialect), two grammars:
  │                           parser/sub.rs (subscriptions) and parser/sql.rs (one-off SQL)
  ▼
Untyped AST (SqlSelect / SqlAst)
  │  crates/expr              type checking → typed AST (RelExpr / ProjectName / ProjectList),
  │                           RLS resolution (rls.rs) → UNION of query fragments
  ▼
Typed logical plan fragments
  │  crates/physical-plan     compile.rs lowers to PhysicalPlan; rules.rs rewrites
  │                           (filter pushdown, index selection, hash→index join, semijoins)
  ▼
Optimized PhysicalPlan (ProjectPlan / ProjectListPlan)
  │  crates/execution         push-based pipelined executors (pipelined.rs);
  │                           Datastore (committed) + DeltaStore (tx deltas) traits
  ▼
crates/subscription           SubscriptionPlan: base plan + delta "Fragments" (the IVM algebra)
  ▼
crates/core/src/subscription  SubscriptionManager (registration, dedup, pruning, fan-out),
                              ModuleSubscriptions (commit hook, initial eval), SendWorker
```

`crates/engine` (`spacetimedb-engine`) is the `RelationalDB` layer extracted out of `core` (re-exported
by `core/src/db/mod.rs`); it owns `SchemaViewer` (`engine/src/sql/ast.rs`), which implements
`SchemaView` for the type checker and reads RLS rules from the `st_row_level_security` system table.
`crates/query` (101 lines) is the thin glue: `compile_subscription`, `compile_sql_stmt`,
`execute_select_stmt`, `execute_dml_stmt`.

The single most important architectural fact: **the same physical plan and the same executors run both
the initial subscription evaluation and the incremental (per-commit) evaluation**. Incremental mode is
just the same plan with `TableScan.delta = Some(Delta::Inserts | Delta::Deletes)` flipped on selected
scans, executed against a `DeltaTx` that routes those scans to the transaction's write set instead of
committed state. There is no separate "diff engine."

---

## 2. The supported SQL subset

### 2.1 Subscription grammar (`sql-parser/src/parser/sub.rs`, `ast/sub.rs`)

Effective grammar (from the module doc, lines 3–54, and enforcement code):

```
query      = SELECT projection FROM relation [ WHERE predicate ]
projection = STAR | ident '.' STAR
relation   = table | relation [AS ident] { [INNER] JOIN relation [AS ident] ON lhs.col = rhs.col }
predicate  = expr | predicate AND predicate | predicate OR predicate
op         = '=' | '<' | '>' | '<=' | '>=' | '!=' | '<>'
literal    = INTEGER | FLOAT | STRING | HEX | TRUE | FALSE
```

Allowed:
- `SELECT *` / `SELECT t.*` projections only (whole rows of exactly one table).
- **Inner equi-joins are allowed** — but the `ON` clause must be a *single* column-to-column equality
  (`parser/mod.rs:50`); `ON a.x = b.y AND …`, `ON a.x = 5`, and all outer joins are rejected with
  `"Non-inner joins are not supported"`. Cross joins and `JOIN` without `ON` parse, but die later.
- `WHERE` with `=, <, >, <=, >=, !=, <>`, `AND`, `OR`, parentheses, literals, and exactly one
  parameter: **`:sender`** (`Parameter::Sender`, resolved to the caller `Identity`).
- In any join, every column reference must be qualified (`"Names must be qualified when using joins"`).

Rejected at parse time (each with a specific error): DML (`"Unsupported: Non-SELECT queries"`),
`ORDER BY`, `LIMIT`, `OFFSET`, `FETCH`, `WITH`/CTEs, locking clauses, `UNION`/`INTERSECT`/`EXCEPT`,
`DISTINCT`, `GROUP BY`, `HAVING`, `QUALIFY`, window functions, implicit comma joins
(`"Implicit joins are not supported"`), subqueries in `FROM`, `IN`, `BETWEEN`, `LIKE`, `IS NULL`,
arithmetic between columns, and any function call in `WHERE`.

Rejected at type-check time (`expr/src/check.rs:167`, `expect_table_type`): column projections and
aggregates — `"Column projections are not supported in subscriptions; Subscriptions must return a
table type"`. `SELECT *` over a join → `"SELECT * is not supported for joins"` (must name `t.*`).

Rejected at subscription-compile time (`crates/subscription/src/lib.rs:648-654`):
- any join that did not optimize into an **index join**: `"Subscriptions require indexes on join columns"`
  (a `HashJoin`/`NLJoin` surviving optimization is fatal);
- event tables as the lookup side of a join.

And in `Fragments::compile_from_plan` (`subscription/src/lib.rs:236`): **more than 2 tables →
`"Invalid number of tables in subscription"`**. So the real incremental engine supports exactly
single-table queries and two-table index equi-joins. Nothing else.

Special form: `SELECT * FROM *` (regex-matched in `core/src/subscription/query.rs:15`) subscribes to
every readable user table by generating one `SELECT * FROM t` plan per table
(`subscription/subscription.rs::get_all`).

### 2.2 One-off SQL grammar (`sql-parser/src/parser/sql.rs`)

Superset of the subscription grammar:
- `SELECT` with **column lists** (`Project::Exprs`), `COUNT(*) AS alias` (the *only* aggregate;
  `SUM`, `COUNT(col)`, `COUNT(DISTINCT …)` rejected despite being listed in the doc comment),
  integer-literal `LIMIT`. **`ORDER BY` is not supported at all, anywhere** (the grammar comment
  advertises it; the code requires `order_by.is_empty()`).
- DML: `INSERT INTO t [(cols)] VALUES (literals…)` (no `INSERT…SELECT`, no `ON CONFLICT`),
  `UPDATE t SET col = literal [WHERE …]` (single table, literal RHS only),
  `DELETE FROM t [WHERE …]` (single table). DML on views rejected.
- `SET var = n` / `SHOW var` — rewritten into `st_var` system-table access; the only variable is
  `row_limit` (`expr/src/statement.rs:261`).

### 2.3 Hard limits

| Limit | Value | Where |
|---|---|---|
| Max SQL length | 50,000 UTF-8 bytes | `query/src/lib.rs:24` (`MAX_SQL_LENGTH`, "DIRTY HACK" against parser stack overflow) |
| Parser expr recursion | 1,600 | `sql-parser/src/parser/recursion.rs:8` |
| Type-checker recursion | 2,500 | same file, checked in `expr/src/lib.rs:90` |
| Index columns usable per scan | 3 | `physical-plan/src/rules.rs:398` (`MAX_EXACT_INDEX_COLS`) |
| Join count / RLS fan-out | none explicit | bounded only by SQL length + recursion + RLS cycle detection |

---

## 3. Typed AST and RLS resolution (`crates/expr`)

### 3.1 Typed AST

- `RelExpr` (`expr/src/expr.rs:242`): `RelVar | Select(input, Expr) | LeftDeepJoin | EqJoin(join, lhs_field, rhs_field)`
  — join trees are strictly **left-deep**.
- `ProjectName` (`expr.rs:42`): a query returning whole rows of one relvar — the only legal
  subscription shape. `ProjectList` (`expr.rs:164`) adds `List` (column projection), `Limit`, `Agg(Count)`
  for one-off SQL. Crucially, `ProjectList::Name(Vec<ProjectName>)` is a **vector** because RLS turns
  one query into a UNION of fragments.
- Type checking is bidirectional (`expr/src/lib.rs::_type_expr`); literals are typed from column
  context (a bare literal with no expected type is an error). `:sender` types as `Identity` or bytes.

### 3.2 RLS: filters become inlined joins, rules union (`expr/src/rls.rs`)

SpacetimeDB RLS rules (`#[client_visibility_filter]`) are **SQL strings in the subscription dialect**,
stored per-table in `st_row_level_security` and fetched via `SchemaView::rls_rules_for_table`. The
resolution algorithm (`resolve_views_for_expr`, `rls.rs:206`) is the most sophisticated part of the
front-end:

1. **Owner bypass:** `auth.bypass_rls()` (database owner) returns the query untouched.
2. Collect every table referenced by the query (skipping the rule's own return table when expanding a
   rule — rules are not self-recursive).
3. **Cycle detection** via a persistent linked list of in-flight `TableId`s (`ResolveList`);
   a cyclic rule chain fails with `"Discovered cyclic dependency when resolving RLS rules…"`.
4. Each rule is parsed with `parse_and_type_sub` — i.e., **RLS rules obey the exact subscription
   subset** — and recursively expanded.
5. **Inlining:** the rule body is grafted into the referencing query's left-deep tree in place of the
   protected table's leaf (`expand_leaf`, `rls.rs:448`). If the leaf was the RHS of an `EqJoin`, the
   join becomes a `Select` over an extended tree — the "semijoin-style" transformation. Aliases are
   alpha-renamed with unique `_<n>` suffixes to avoid capture (`alpha_rename_fragments`).
6. **Multiple rules on one table = UNION.** N rules on table A × M rules on table B produce N×M
   fragments (`expand_views`, `rls.rs:291`). Each fragment compiles to a separate physical plan;
   results are concatenated with **bag semantics** downstream.
7. A rule using `:sender` marks the whole query parameterized (`has_param = true`), which changes how
   it is hashed/shared (§7.3).

Practical consequence: **any RLS-protected table makes every subscription against it a join** (the
rule's `WHERE identity = :sender` joins the protected table against whatever the rule references), and
multiple rules multiply plan fragments per query. This is why the incremental engine had to support
joins at all.

---

## 4. Physical plans and the optimizer (`crates/physical-plan`)

### 4.1 Operators (`plan.rs:284`)

```rust
enum PhysicalPlan {
    TableScan(TableScan, Label),          // schema, limit: Option<u64>, delta: Option<Delta>
    IxScan(IxScan, Label),                // + index_id, probe: IndexProbe::Point(expr)
    IxJoin(IxJoin, Semi),                 // streamed lhs, index-probed rhs, unique: bool
    HashJoin(HashJoin, Semi),             // build rhs hash table, stream lhs, unique: bool
    NLJoin(Box<..>, Box<..>),             // cross product fallback
    Filter(Box<..>, PhysicalExpr),
}
```

`Semi::{Lhs, Rhs, All}` marks semijoin projection. `IndexProbe::Range` exists but is **never
constructed** — only exact point probes are emitted today (a `NOTE(centril)` comment documents why:
partial index prefixes are actually ranged scans, deliberately unsupported). Projection is layered on
top: `ProjectPlan` (whole rows — subscriptions) and `ProjectListPlan` (columns/limit/count — one-off
SQL). Scalar `PhysicalExpr` includes `Param(ParamSlot, ty)` with three predefined slots
(`PARAM_SENDER`, plus two view-arg-hash slots) resolved at runtime by `ExecutionParams` — this is how
one compiled plan is shared across senders.

### 4.2 Optimization pipeline (`plan.rs::optimize`, `rules.rs`)

Fixed rule sequence, purely structural, **no cost model, no statistics**:

1. `expand_views` — inject `arg_hash = <param>` filters over view-backed tables.
2. `canonicalize` — literals to RHS of comparisons, flatten AND/OR, orient equijoins.
3. `PushConstAnd` / `PushConstEq` — push constant equality filters down onto table scans.
4. `ReorderDeltaJoinRhs` — **swap hash-join sides so a delta scan is always the streamed LHS**
   (critical for incremental evaluation).
5. `ReorderHashJoin` — put base-table scans on the stream side, subtrees on the build side.
6. `HashToIxJoin` — "always prefer an index join": if the RHS has an index whose *last* column is the
   join key and whose leading columns are pinned by pushed equality constants, rewrite to `IxJoin`.
7. `IxScanFromPredicates` — index selection for scans: gather top-level `col = const/param`
   conjuncts; an index qualifies only if **every** index column (≤3) is covered; among candidates the
   **widest fully-covered index wins**; residual conjuncts stay as a `Filter`. Partial prefixes are
   rejected (a filter on `y` alone will not use a `(y, z)` index).
8. `UniqueIxJoinRule` / `UniqueHashJoinRule` — mark `unique: true` when the probed column has a
   unique constraint (enables ≤1-row probe fast paths and better row estimates).
9. `introduce_semijoins` — convert joins to `Semi::Lhs/Rhs` based on which labels the projection needs.
10. `ComputePositions` — resolve symbolic `Label`s to positional tuple indices; hard-fails if any
    position is unresolved (`"Could not compute positional arguments during query planning"`).

All equijoins are compiled as `HashJoin` and only *become* index joins via rule 6 — and subscriptions
then reject whatever is still a hash/NL join. Elegant: one compiler, and the subscription restriction
is a post-hoc check, not a separate code path.

### 4.3 Cardinality estimation (`core/src/estimation.rs`)

`estimate_rows_scanned` / `row_estimate` walk the physical plan: table scans use live row counts;
point index scans use `row_count / num_distinct_values(index cols)`; unique joins O(lhs); non-unique
joins multiply. Used **only as an admission gate**, not for plan choice: `check_row_limit` rejects a
new query when the estimate exceeds the `row_limit` st_var (error
`"Estimated cardinality (N rows) exceeds limit (M rows)"`), unless `auth.exceed_row_limit()`.

---

## 5. Execution engine (`crates/execution/src/pipelined.rs`)

**Push-based, tuple-at-a-time, non-interruptible.** Every executor implements
`execute(tx, params, metrics, f: &mut dyn FnMut(row) -> Result<()>)` and pushes rows into the
callback. No batching, no vectorization, no early termination (`PipelinedLimit` keeps consuming input
after the limit — a known wart; also `COUNT(*)` over a bare table scan is answered from table
metadata as an in-executor hack).

Two datastore traits (`execution/src/lib.rs`):
- `Datastore` — committed state: `table_scan`, `index_scan_point`, `index_scan_range`, `row_count`.
- `DeltaStore` — tx write set: `inserts_for_table` / `deletes_for_table` (slices of `ProductValue`),
  `index_scan_{point,range}_for_delta`, `has_inserts/deletes`.

A `Row<'a>` is either `Ptr(RowRef)` (committed, zero-copy) or `Ref(&ProductValue)` (delta). Join
outputs are `Tuple::Join(Vec<Row>)` indexed positionally via the precomputed `label_pos`.

Runtime join algorithms: **index join** (fully pipelined; 6 specializations over
`unique × {Lhs,Rhs,All}`), **hash join** (pipeline breaker, builds RHS `HashMap`/`HashSet`;
`Semi::Lhs` builds only key→count), **nested-loop** (materializes RHS `Vec`). Every scan/probe
dispatches on `IndexSource::Base | Delta(..)` — the same executor runs against committed state or
the delta tables.

`ExecutionMetrics` (rows_scanned, bytes_scanned, index_seeks, bytes_sent_to_clients,
delta_queries_evaluated/matched, duplicate_rows_evaluated/sent…) is threaded through every call.

---

## 6. Incremental evaluation — the actual IVM algorithm

### 6.1 The delta algebra (`crates/subscription/src/lib.rs`)

`SubscriptionPlan::compile` produces, per RLS fragment:
- `base_plan: PipelinedProject` — the full plan (initial evaluation);
- `fragments: Fragments { insert_plans, delete_plans }` — the incremental plans.

`Fragments::compile_from_plan` (lib.rs:158) implements textbook delta-query derivation, documented
in a 60-line comment deriving, for `V = R ⋈ S`:

```
dv(+) = R'ds(+) ∪ dr(+)S' ∪ dr(+)ds(-) ∪ dr(-)ds(+)
dv(-) = R'ds(-) ∪ dr(-)S' ∪ dr(+)ds(+) ∪ dr(-)ds(-)
```

Mechanically: clone the optimized plan, flip `TableScan.delta = Some(Inserts|Deletes)` on the chosen
relvar(s) (`mut_plan`), re-optimize, wrap as `PipelinedProject`. Result:

- **Single table:** 1 insert plan (`delta = Inserts`) + 1 delete plan (`delta = Deletes`).
- **Two-table join:** **4 insert plans + 4 delete plans** (the eight terms above; `R'`/`S'` means the
  post-commit committed state, which is exactly what a read-only tx sees after `commit_tx_downgrade`).
- **>2 tables: `bail!`** — this is the hard scope wall of the whole system.

Because delta scans sit on the streamed side (rule `ReorderDeltaJoinRhs`) and `plan.is_empty(tx)`
short-circuits fragments whose delta side is empty, a commit touching only `R` executes only the
`dr` fragments; the `ds` fragments are skipped in O(1).

### 6.2 DeltaTx and per-commit delta indexes (`core/src/subscription/tx.rs`)

`DeltaTx<'a>` wraps the post-commit read tx (`&TxId`) + the commit's `TxData` and implements both
`Datastore` (delegating to committed state) and `DeltaStore` (serving the write set). The subtle part:
**join fragments probe *into* delta tables by index** (`R'ds(+)`: scan committed R', probe inserts of
S by join key). For that, `DeltaTableIndexes::from_tx_data` builds, *per commit*, a
`BTreeMap<AlgebraicValue, SmallVec<[usize;1]>>` over the delta rows for **exactly the indexes that
live subscriptions actually use** — tracked by `QueriedTableIndexIds` (ref-counted
`(TableId → IndexId → count)` map maintained on subscribe/unsubscribe). No subscription on an index →
no per-commit index build cost.

### 6.3 Bag semantics and duplicate elimination (`core/src/subscription/delta.rs::eval_delta`)

Single-table fragments cannot produce duplicates, so rows are streamed straight into the update.
Join fragments **can** (the 4 terms overlap, e.g., a row inserted on both sides), so `eval_delta`
maintains `insert_counts` / `delete_counts: HashMap<RelValue, i64>` and cancels matched
insert/delete pairs, emitting each net row with its multiplicity. The header comment is explicit:
*"This does and must implement bag semantics… a client needs to know for each row in R how many rows
it joins with in S"* — deletes are only sent when the join count truly reaches zero. Metrics
`duplicate_rows_evaluated` / `duplicate_rows_sent` track the overhead.

There is no per-row "was the client subscribed to this row?" lookup (Fluxum's SUB-021 sketch):
a deleted row is reported iff the delete fragment — the same predicate + RLS plan, run over
`Delta::Deletes` with the row's *old* values — matches it. Correctness falls out of running the query
itself against the delta.

---

## 7. SubscriptionManager: registration, dedup, pruning, fan-out

(`core/src/subscription/module_subscription_manager.rs`)

### 7.1 Data structures

```rust
pub struct SubscriptionManager {
    clients: HashMap<ClientId, ClientInfo>,             // ClientId = (Identity, ConnectionId)
    queries: HashMap<QueryHash, QueryState>,            // one entry per UNIQUE query text
    tables:  IntMap<TableId, HashSet<QueryHash>>,       // inverted index (fallback pruning)
    indexes: QueriedTableIndexIds,                      // ref-counted indexes used by subs (§6.2)
    search_args: SearchArguments,                       // value-level pruning for equality filters
    join_edges: JoinEdges,                              // value-level pruning for join queries
    send_worker_queue: BroadcastQueue,                  // mpsc to the SendWorker task
}
struct QueryState {
    query: Arc<Plan>,                                   // compiled once, shared by all subscribers
    legacy_subscribers: HashSet<ClientId>,              // Subscribe (whole-set) API
    subscriptions: HashSet<ClientId>,                   // v1 SubscribeSingle/Multi
    v2_subscriptions: HashSet<(ClientId, QuerySetId)>,  // v2 protocol
}
struct ClientInfo {
    outbound_ref: Arc<ClientConnectionSender>,
    v1_subscriptions: HashMap<(ClientId, QueryId), HashSet<QueryHash>>,
    v2_subscriptions: HashMap<(ClientId, QuerySetId), HashSet<QueryHash>>,
    subscription_ref_count: HashMap<QueryHash, usize>,  // same query via N handles
    dropped: Arc<AtomicBool>,                           // set by SendWorker on send failure
}
```

The whole manager sits behind a `parking_lot::RwLock` (not an async mutex): reads for commit-path
evaluation, writes for subscribe/unsubscribe. Lock ordering is documented and enforced by
convention: *db lock first, then subscriptions lock* (`module_subscription_actor.rs:61`).

### 7.2 Multi-client dedup

`QueryHash` (`execution_unit.rs`) = **blake3 of the SQL text** — so a query subscribed by N clients
exists once, is compiled once, and per commit is **evaluated once and encoded once**, then the encoded
bytes are cheaply cloned per subscriber. If the query is parameterized (`:sender` anywhere, including
inside an RLS rule, or reads a client-specific view), the hash is
`blake3(sql ‖ caller_identity)` — per-identity sharing instead of global. On subscribe,
`compile_queries` checks the cache under a **read** lock and only compiles misses
(`num_new_queries_subscribed` metric counts cache misses).

### 7.3 Query pruning — the answer to the fan-out cliff

On each commit, candidate queries are the union of three sources (`queries_for_table_update`):

1. **`SearchArguments`** — if a query's plan has a single-column equality (`t.id = 5` or a join
   `… WHERE s.x = 5`), it is *removed* from the `tables` inverted index and registered under
   `args: BTreeMap<(TableId, ColId, AlgebraicValue) → HashSet<QueryHash>>`. Per delta row, the
   manager projects the row's value for each parameterized column and looks up exact matches.
   The doc example: *1,000 clients subscribed to `SELECT * FROM t WHERE id = ?` with distinct values;
   a 1-row commit evaluates exactly 1 query* — O(1) instead of O(clients).
2. **`JoinEdges`** — for a two-table query `SELECT a.* FROM a JOIN b ON a.id = b.id WHERE b.x = v`
   (requires: unique join index, single-column point filter on b, no self-join), a sorted
   `BTreeMap<JoinEdge, HashMap<AlgebraicValue, HashSet<QueryHash>>>` is kept. For a delta row of `a`,
   the manager follows the join edge (`find_rhs_val`: index lookup of the matching `b` row, read
   `b.x`) and prunes to queries whose `v` matches. Same O(1)-per-row effect for the join workload
   (the doc example is 1,000 players each subscribed to entities in *their* region).
3. **`tables` inverted index** — everything else: any query on a mutated table without a usable
   search arg/join edge is evaluated unconditionally. This is the slow tier — an unindexed
   subscription on a hot table is evaluated on **every** commit touching that table.

### 7.4 Commit-path sequence (`module_subscription_actor.rs::commit_and_broadcast_event`, line 1736)

```
reducer holds MutTxId (db write lock)
1. take subscriptions READ lock  (before commit — prevents duplicate/missed updates)
2. commit_tx_downgrade(tx)       → (TxData, read-only TxId)  — db lock swapped for read tx
3. DeltaTx::new(read_tx, tx_data, manager.index_ids_for_subscriptions())
        └ builds per-commit BTree indexes over delta rows (§6.2)
4. manager.eval_updates_sequential(...)          — SINGLE-THREADED, on the commit thread:
     for each mutated table → queries_for_table_update (pruning §7.3), dedup by hash
     for each surviving query → for each RLS fragment → eval_delta (§6.3)
     encode once per wire format (BSATN / JSON), memoized; clone bytes per subscriber
     on eval error: log, queue error message, mark subscription for removal
5. send SendWorkerMessage::Broadcast { tx_offset, ComputedQueries } → return
   (datastore unlocked; next reducer can run)
6. SendWorker task (async, ordered mpsc):
     awaits tx_offset (durability) — gates delivery for confirmed-reads clients
     aggregates per (client, table) → per client one TransactionUpdate
     caller always gets a full update (even if empty); others get "light" updates if configured
     per-client websocket task compresses the outer message (Brotli quality 1 / Gzip, only if
     >1 KiB — decide_compression, websocket_building.rs:200) and writes to the socket
     send failure → set ClientInfo.dropped; manager reaps via remove_dropped_clients
```

Two deliberate perf decisions are documented in comments:
- **Rayon was removed from the update path** (`eval_updates_sequential` doc): parallel evaluation
  cost more in thread-switching than it saved for the common case of small commits. Initial
  subscription evaluation still uses rayon (`execute_plans` uses `par_iter`).
- **Compression moved off the tx lock**: query updates inside a transaction update are *not*
  compressed individually anymore (they used to be, to share compression across subscribers);
  instead the outer message is compressed per client on the websocket worker. Holding the tx lock
  to compress was worse than redundant per-client compression.

The `SendWorker` exists explicitly to (a) keep serialization order of updates across commits,
(b) get network fan-out off the commit thread, and (c) avoid waking hundreds of per-client tokio
tasks while the datastore is still locked. Its queue depth is a gauge
(`spacetime_subscription_send_queue_length`). Note: the queue is **unbounded** — backpressure is
handled per client at the websocket layer (drop-and-mark, not block), never at the fan-out loop.

### 7.5 Initial evaluation (subscribe path)

`add_multi_subscription_inner` (actor line 1426) shows the full sequence: compile-or-reuse plans
(read lock) → register (write lock, brief) → materialize any views + downgrade tx →
**row-limit gate** (`check_new_query_row_limit`, §4.3 — a too-broad query is rejected *before*
scanning) → `evaluate_queries` (rayon `execute_plans` over the read tx; plans whose tables are empty
are skipped) → enqueue the `InitialData` message on the SendWorker **while still holding the read
tx**, which guarantees a client never observes a TxUpdate ordered before its InitialData.
Unsubscribe reuses the same machinery with `TableUpdateType::Unsubscribe` — the final matching rows
are sent as **deletes**, so the client cache empties itself without special-casing.

### 7.6 Metrics on slow queries

Per-plan classification is computed **at compile time** (`SubscriptionPlanMetrics`,
`crates/subscription/src/lib.rs:295`): `scan_type ∈ {fully_indexed, indexed_with_filter, sequential,
mixed}` plus a comma-joined list of `unindexed_columns` extracted from residual filters. At runtime
these become labels on three Prometheus series (`core/src/worker_metrics/mod.rs:394-409`):
`spacetime_subscription_rows_examined` (histogram), `…_query_execution_time_micros` (histogram),
`…_queries_total` (counter) — so "which subscriptions are doing table scans, on which columns" is
directly queryable in ops dashboards. Additional gauges/counters: lock waiters + lock wait time and
compile time per workload (subscribe/unsubscribe/update), number of unique queries, connections,
subscription sets, `delta_queries_evaluated` / `delta_queries_matched`, duplicate-row counters, and
send-queue length. There is no per-query kill/timeout — the executor is non-interruptible; the only
guards are the compile-time row estimate and the metrics.

---

## 8. One-off SQL, query-builder, index-scan-gate

- **One-off SQL** (`core/src/sql/execute.rs::run_inner`): parse+type in a *mutable* tx (so DML can
  proceed), RLS applied to selects via `resolve_views_for_sql`; selects downgrade to a read tx,
  pass the `check_row_limit` estimate gate, and execute via `ProjectListExecutor` collecting
  `ProductValue`s. DML requires `auth.has_write_access()`, executes via `MutExecutor`
  (update = delete + reinsert with columns substituted), then — the nice touch — **commits through
  `commit_and_broadcast_event` with a synthetic `ModuleEvent`**, so ad-hoc DML feeds subscriptions
  exactly like a reducer. No RLS on DML; no timeout.
- **`query-builder`** is not part of the server pipeline: it is a typed Rust DSL (used by
  `bindings`/SDK for **views**) that renders SQL strings (`Query::into_sql`), with compile-time
  guarantees (joins only on indexed columns via `IxCol`, event tables can't be lookup tables via the
  `CanBeLookupTable` marker). The generated SQL re-enters the normal parser.
- **`index-scan-gate`** is a CI perf gate, not a static analyzer: it runs 4 index-scan reducers
  against a populated module, 5 warmups + 31 measured runs each, and **fails CI if any median
  ≥ 100 µs** (`MEDIAN_THRESHOLD`) — a regression tripwire for "the planner stopped using the index."

---

## 9. Views (new in this version — relevant to SPEC-005 §7)

v2.7 has server-side **incremental views**: subscriptions can target view-backed tables
(`SubscriptionPlan::is_view`, `ViewId`, `reads_anonymous_view`), views are materialized on first
subscribe (`materialize_views_and_downgrade_tx` runs module code to populate backing tables),
ref-counted per subscriber, and unsubscribed views are cleaned up asynchronously. View rows carry
private leading columns (arg-hash etc.) that are stripped at the wire boundary
(`project_product(num_private_cols..num_cols)`). Client-specific views act like `:sender`-
parameterized queries for hashing. **SPEC-005's SUB-050 note that "SpacetimeDB lacks this" is now
outdated** — they shipped it, including incremental maintenance, and it visibly complicated every
layer (extra plan metadata, extra tx work on subscribe/unsubscribe, module callbacks inside the
subscription path).

---

## What Fluxum will face

**1. Single-table scope (SUB-010/SUB-012) is defensible — but only with closure-based RLS, and the
join wall must be understood.** SpacetimeDB's engine looks join-capable, yet the incremental core
supports exactly **one or two tables** (`Fragments::compile_from_plan` bails at >2), only inner
equi-joins, only with an index on the join column, and pays 8 delta fragments + per-commit delta
index builds + bag-semantics refcounting for every join. They needed even that limited join support
for two reasons Fluxum should check against its own requirements: (a) **RLS rules are SQL and become
joins** — Fluxum's `RlsFn` closure (SUB-030) sidesteps this entirely, but also means Fluxum's RLS
cannot express "visible if a row in *another* table says so" (SpacetimeDB can: `SELECT i.* FROM item i
JOIN player p ON i.owner = p.id WHERE p.identity = :sender`); (b) the canonical game/collab query
"rows of X that belong to my Y" is a semijoin. If Fluxum keeps single-table plans, it should say
explicitly that relational visibility and semijoin subscriptions are done client-side or via
denormalized owner columns — that's where SpacetimeDB users would hit the wall first.

**2. Adopt the "same plan, delta-flagged scans" IVM design.** The most valuable idea in the codebase:
no separate diff evaluator. One compiled plan; incremental mode = clone with `delta = Inserts/Deletes`
on a scan + a datastore trait (`DeltaStore`) that serves the write set. For Fluxum's single-table scope
this collapses to exactly 2 fragments (insert/delete) and makes SUB-021's "deletes: check if client
was subscribed" unnecessary — run the same filter+RLS over the deleted rows' old values. Simpler and
provably consistent with InitialData.

**3. SPEC-005's fan-out algorithm is O(clients); SpacetimeDB's is O(distinct queries after pruning).**
Three upgrades to SUB-040/SUB-021 worth specifying now:
- **Dedup by query hash** (`HashMap<QueryHash, QueryState>`): evaluate once per unique query, encode
  once per format, clone bytes per client. SPEC-005's `plans: HashMap<ConnectionId, Vec<CompiledPlan>>`
  re-evaluates per client. Hash = blake3(sql), + identity when the plan depends on the caller.
- **Value-level pruning** (`SearchArguments`): `(table, col, value) → queries` for equality-filtered
  subscriptions. This — not the per-table `table_watchers` index of SUB-040 — is what prevents the
  "1,000 clients, 1,000 different `WHERE id = ?` values, every commit evaluates 1,000 plans" cliff.
  Fluxum's spatial subscriptions (SUB-011) need the spatial analogue: a region index over subscribed
  regions so a moved entity prunes to the few clients whose region contains it, which SPEC-008's
  quadtree can serve.
- **Detach network from evaluation** via an ordered send-worker queue (SPEC-005's SUB-041 gets the
  non-blocking part right; SpacetimeDB adds strict cross-commit ordering and durability gating by
  `tx_offset` for confirmed reads — Fluxum will need the same hook once SPEC-014 replication lands).

**4. Perf-cliff lessons, in their own words (comments in the source):**
- Parallel (rayon) evaluation of per-commit updates was a **net loss**; they went single-threaded on
  the commit path and kept parallelism only for initial subscribes. Don't build SUB-022 around a
  thread pool for the delta path.
- Compressing per-query updates while holding the tx lock was a net loss; compress the outer message
  per client, off the lock, above a 1 KiB threshold (Brotli level 1, 7–10× on large updates).
- Unindexed subscriptions are the dominant operational hazard: every commit on the table pays a full
  filter evaluation. SpacetimeDB's answer is compile-time plan classification (`scan_type`,
  `unindexed_columns`) exported as metric labels, plus a CI gate for index-scan latency. Fluxum's
  SPEC-012 should reserve those exact labels, and subscription compile should *warn* on sequential
  scans.
- The subscribe path itself is guarded by a **cardinality-estimate row limit** (`row_limit` st_var,
  `"Estimated cardinality (N) exceeds limit (M)"`) — SPEC-005 has no admission control for a
  `SELECT * FROM huge_table` subscription; add one (estimate = table row count for single-table
  plans; trivial with SPEC-002's row counts).
- Slow-client handling: SpacetimeDB never blocks fan-out; a failed send sets a `dropped` flag reaped
  later. Fluxum's three-tier buffer policy (SUB-042) is *more* specified than what they ship — keep it.

**5. Scope confirmations from their restrictions.** SpacetimeDB rejects ORDER BY/LIMIT even for
initial subscription data — SPEC-005's SUB-013 (ORDER BY/LIMIT on InitialData only) already exceeds
parity; fine, but be aware nothing in their engine needed ordering, and their executor cannot even
short-circuit a LIMIT. No `IN`/`BETWEEN` anywhere in their subscription dialect — SPEC-005 SUB-010
promises both, which is a (small) superset requiring set/range predicates in the filter compiler and,
for `IN`, either multi-point pruning entries or fallback to the table tier. Their `SELECT * FROM *`
subscribe-all form is worth copying for tooling. And two hygiene items worth stealing verbatim: a
max SQL length (50 KB) + recursion caps to keep hostile queries from blowing the parser stack
(SPEC-005 acceptance §2's "hostile corpus" needs these), and error handling on incremental eval =
send error + auto-unsubscribe that query, never poison the commit loop.

**6. Where Fluxum can be simpler.** No wire-format duality (BSATN+JSON memoized twice per update),
no three protocol generations (legacy/v1/v2 sets tracked in parallel in every structure — a large
fraction of the manager's 3.2k lines is compatibility), no module-thread round-trips in the subscribe
path (views). Fluxum's single format + single protocol keeps the manager at roughly the SPEC-005
sketch's complexity *if* it adds the three structures from point 3. Realistic size estimate based on
this codebase: the manager+actor+delta machinery is ~9k lines of subtle, comment-heavy Rust even
before views — SPEC-005 is indeed the most underestimated area, and the estimate should assume the
pruning indexes and send-worker are core deliverables, not optimizations to defer.
