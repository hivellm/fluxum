# SPEC-018 — Index-Aware Query Planner, Range Operators & Keyset Pagination

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 4 (extends T4.1 SQL compiler + T4.2 subscription manager) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-30, FR-31, FR-34 (extends); new: FR-93 (index-aware query planning), FR-94 (keyset pagination) |
| **Requirement prefix** | `QP-` |
| **Source** | New (Fluxum-native). Closes the gap between the built B-tree index ([SPEC-001](SPEC-001-data-model.md) DM-030/031, `crates/fluxum-core/src/index/btree.rs`) and the subscription/query evaluator, which currently full-scans. |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`QP-xxx` are stable. Priority tags: `[P0]` MVP · `[P1]` competitive launch · `[P2]` post-launch.

## 1. Scope & problem statement

Fluxum already has a complete secondary index: composite B-tree with memcomparable, order-preserving
keys, equality/range/prefix scans, MVCC-consistent maintenance, and a public
`Snapshot::index_scan` API (`crates/fluxum-core/src/store/committed.rs:325`;
`crates/fluxum-core/src/index/btree.rs`). **Nothing in the query path uses it.** Non-spatial `WHERE`
evaluation — both `InitialData` at subscribe time and one-off queries — iterates the entire primary
row map and applies a per-row predicate closure (`crates/fluxum-core/src/subscription/mod.rs:482`);
`ORDER BY` sorts the fully materialized result in RAM and `LIMIT` truncates it. `CompiledPlan`
carries no index reference (`crates/fluxum-core/src/sql/mod.rs:94`). The result: a filtered read such
as a marketplace listing (`category = X AND price BETWEEN a AND b ORDER BY price LIMIT 50`) costs
O(rows in table) instead of O(log n + k), even with `#[index(btree(category, price))]` declared.

This spec adds:

- **§2 Index selection** — a rule-based planner that picks a usable index for a `WHERE`/`ORDER BY`.
- **§3 Range pushdown** — index-bounded scans plus a residual filter for the rest.
- **§4 Index-ordered `ORDER BY`/`LIMIT`** — when the index order matches, skip the sort and stop early.
- **§5 Range operators** — extend the SQL subset with `<`, `>`, `<=`, `>=` (freeze-sensitive, T6.1).
- **§6 Keyset pagination** — cursor/seek pagination for stable, index-accelerated windows.

**Out of scope (unchanged non-goals):** `JOIN`, `OR`, `GROUP BY`/aggregates, subqueries
([SPEC-005](SPEC-005-subscriptions.md) SUB-012 stand). `ORDER BY`/`LIMIT` on `TxUpdate` diffs remain
unsupported (SUB-013): the planner accelerates the **snapshot** read (`InitialData`, one-off query),
not the incremental diff. Incremental/materialized top-N is `#[fluxum::materialized_view]` (SUB-050,
P2) and is the follow-up track, not this spec. No statistics/cost model — selection is **rule-based**.

## 2. Index selection (planner)

- **QP-001** [P0] The planner SHALL run once at query compile time and annotate the `CompiledPlan`
  with an optional **access path**: either a `FullScan` or an `IndexScan { index_id, bounds }`.
  Selection is rule-based over the query's top-level `AND` conditions and `ORDER BY`:

  1. For each declared B-tree index on the table, compute the longest prefix of its key columns
     satisfied by **equality** conditions (`col = v`, or `col IN (…)` treated as a set of equality
     probes — QP-011), followed by **at most one** range condition (`BETWEEN`/`<`/`>`/`<=`/`>=`,
     §5) on the next key column.
  2. Score each index by (equality-prefix length, whether a range binds the next column, whether the
     remaining index order satisfies `ORDER BY` — §4).
  3. Choose the highest-scoring index whose equality prefix length ≥ 1 **or** whose leading columns
     satisfy `ORDER BY`; otherwise choose `FullScan`.

  The chosen path SHALL be deterministic for a given schema + query (ties broken by index declaration
  order) so the same normalized query always compiles to the same plan (dedup stability, SUB-020).

- **QP-002** [P0] The planner SHALL be transparent: for any query, `IndexScan` and `FullScan` MUST
  return the **same row set** (the residual filter, QP-010, guarantees this). Index selection is an
  optimization only; correctness never depends on it. A property test SHALL assert
  `plan_with_index(q) == plan_forced_fullscan(q)` for a generated query corpus.

- **QP-003** [P1] The planner SHALL apply to **one-off queries** (`OneOffQuery`, SPEC-006) and
  **subscription `InitialData`** (SUB-001). It SHALL NOT change `TxUpdate` delta evaluation, which
  already scans only the commit's delta rows, nor the value-level fan-out pruning (SUB-023) — those
  are orthogonal.

## 3. Range pushdown & residual filter

- **QP-010** [P0] For an `IndexScan`, the planner SHALL split the `WHERE` conditions into:
  - **bound conditions** — the equality prefix + the single next-column range → encoded into the
    `index_scan` prefix and lower/upper bounds (`crates/fluxum-core/src/index/btree.rs:223`);
  - **residual conditions** — every remaining condition (a second range column, a non-indexed
    column) → retained as the per-row predicate closure applied to each row the index yields.

  Example: `category = X AND price BETWEEN a AND b AND listed_at >= t` with
  `#[index(btree(category, price))]` → bound = `category = X` prefix + `price ∈ [a,b]`; residual =
  `listed_at >= t`. The scan touches only rows in the `(X, [a,b])` key range, then filters
  `listed_at`.

- **QP-011** [P0] `col IN (v1, …, vk)` on an index's leading column SHALL be executed as `k` bounded
  index scans (one per value) merged in key order, not as a full scan with a residual `IN` closure,
  when `k` is at or below a configurable `index_in_expansion_max` (default 128); above the threshold
  it falls back to a residual filter over a broader scan. `IN` on a non-leading or non-indexed column
  is always a residual filter.

- **QP-012** [P1] The planner SHALL respect the existing single-range-column limit of the index
  (`committed.rs:88`): only the column immediately after the equality prefix may be a bound; any
  further range is residual. This spec does **not** add multi-range index scans.

## 4. Index-ordered ORDER BY / LIMIT

- **QP-020** [P0] When the chosen index's residual key order matches the query's `ORDER BY` (same
  column, compatible direction — a `DESC` order served by a reverse index scan), the planner SHALL
  mark the plan `ordered_by_index = true`, **skip the in-RAM sort**, and stream rows already in
  order. Postgres analog: an index scan satisfying `ORDER BY` without a `Sort` node.

- **QP-021** [P0] When `ordered_by_index = true` **and** a `LIMIT n` is present, evaluation SHALL
  **stop after the first `n` rows that pass the residual filter and RLS** — an index-ordered
  top-N that reads O(n + skipped) rows instead of materializing and sorting the whole result set.
  When the order is **not** index-served, behavior is unchanged (materialize filtered set, sort,
  truncate — SUB-013).

- **QP-022** [P1] RLS (`#[visibility]`, SUB-030) and column masking ([SPEC-017](SPEC-017-column-transforms.md)
  CT-041) SHALL be applied **within** the index-ordered scan, before counting toward `LIMIT`, so a
  top-N never returns fewer authorized rows than exist (no "hole" from post-limit filtering).

## 5. Range operators (SQL subset extension)

- **QP-030** [P0] The subscription/query SQL subset (SUB-010) SHALL be extended with the comparison
  operators `<`, `>`, `<=`, `>=` as top-level `AND` conditions:

  ```sql
  SELECT * FROM Item
  WHERE category = 'weapon' AND price >= 100 AND price <= 500 AND listed_at > 1719000000000000
  ORDER BY price ASC LIMIT 50
  ```

  Two range conditions on the **same** column (`price >= 100 AND price <= 500`) SHALL be recognized
  and folded into a single closed/half-open interval equivalent to `BETWEEN` for pushdown (QP-010).
  `!=`, `OR`, `LIKE`, `NULL`/`IS` remain unsupported and rejected with the existing 400 diagnostic
  (SUB-012; `crates/fluxum-core/src/sql/parse.rs:95`).

- **QP-031** [P0] The new operators are an **additive** change to the module API / query surface that
  freezes at T6.1. They MUST land before that freeze or defer to a post-freeze additive revision.
  SDK codegen and the `/schema` query documentation ([SPEC-011](SPEC-011-sdk-codegen.md)) SHALL
  reflect the extended operator set.

- **QP-032** [P1] Operator type-checking SHALL reuse the existing schema-typed coercion
  (`sql/mod.rs:338`, int→float widening only) and SHALL reject a comparison operator on `Bool`,
  `Option`, or `List` columns at compile time (consistent with the current `BETWEEN` rule,
  `sql/mod.rs:273`).

## 6. Keyset (cursor) pagination

- **QP-040** [P1] The query SQL subset SHALL support keyset pagination via an `AFTER` cursor clause,
  usable **only** together with `ORDER BY` on an indexed column:

  ```sql
  SELECT * FROM Item WHERE category = 'weapon' ORDER BY price ASC, id ASC LIMIT 50 AFTER (250, 8123)
  ```

  The cursor `(250, 8123)` is the `(ORDER BY value, primary key)` of the last row of the previous
  page. The planner SHALL translate `AFTER` into an index lower bound (`price > 250 OR (price = 250
  AND id > 8123)`), so page N+1 is an O(log n + page) seek — never O(N · page) as `OFFSET` would be.
  `OFFSET` SHALL NOT be added (it is linear and unstable under concurrent writes); keyset is the
  sanctioned pagination primitive. Postgres analog: keyset/seek pagination over a matching index.

- **QP-041** [P1] `ORDER BY` for a stable cursor SHALL append the primary key as the final sort term
  (implicitly if the client omits it) so the order is total and the cursor is unambiguous across rows
  with equal `ORDER BY` values.

- **QP-042** [P1] Keyset pagination applies to the **snapshot** (`InitialData`/one-off) only,
  consistent with QP-003 and SUB-013. A paginated subscription delivers the first page as
  `InitialData`; subsequent pages are fetched via new `SubscribeSingle`/`OneOffQuery` calls with an
  advanced cursor. Live diffs for a paginated window are out of scope (materialized views, SUB-050).

## 7. CompiledPlan extension

- **QP-050** [P0] `CompiledPlan` (`sql/mod.rs:94`) SHALL be extended additively:

  ```rust
  pub struct CompiledPlan {
      // ... existing fields (query_id, table_ids, filter, rls, order_by, limit,
      //     equalities, spatial, query_hash, normalized) ...
      pub access: AccessPath,          // NEW: FullScan | IndexScan { index_id, bounds }
      pub residual: Option<FilterFn>,  // NEW: conditions not covered by the index bounds
      pub ordered_by_index: bool,      // NEW: skip the sort + enable index-ordered top-N (QP-020/021)
      pub cursor: Option<Keyset>,      // NEW: AFTER cursor bounds (QP-040)
  }

  pub enum AccessPath {
      FullScan,
      IndexScan { index_id: IndexId, bounds: ScanBounds },   // feeds Snapshot::index_scan
  }
  ```

  `filter` remains the full predicate for `FullScan`; for `IndexScan`, `residual` carries only the
  not-pushed-down conditions. The snapshot evaluator (`subscription/mod.rs:462`) SHALL branch on
  `access`: `FullScan` → today's `scan().filter()`; `IndexScan` → `index_scan(bounds)` then
  `residual` + RLS.

- **QP-051** [P0] The access path SHALL be reflected in a debug/introspection surface — a
  `GET /query/explain?q=…` admin endpoint (or `fluxum query explain`) that returns the chosen index,
  bounds, residual conditions, and whether the sort is index-served — so operators can verify a query
  is index-accelerated. Postgres analog: `EXPLAIN`.

## 8. Acceptance criteria

1. **Index selection & transparency (QP-001/002):** for a generated corpus of `WHERE`/`ORDER BY`
   queries, the `IndexScan` result set is byte-identical to the forced-`FullScan` result set; the
   chosen access path is deterministic and stable across recompiles of the same normalized query.
2. **Range pushdown (QP-010/011):** `category = X AND price BETWEEN a AND b AND listed_at >= t` with
   `#[index(btree(category, price))]` touches only rows in the `(X,[a,b])` key range (verified by a
   rows-scanned counter) and applies `listed_at` as residual; `IN (…)` under the threshold expands to
   per-value index scans, above it falls back — both returning the correct set.
3. **Index-ordered top-N (QP-020/021):** `… ORDER BY price ASC LIMIT 50` served by
   `btree(category, price)` performs no in-RAM sort (sort-invoked counter = 0) and reads
   O(50 + filtered-out) rows, not the whole table; a non-index-served order still sorts+truncates.
4. **RLS/masking within the scan (QP-022):** an `owner_only` table with a top-N returns exactly the
   authorized top-N (no short page from post-limit filtering).
5. **Range operators (QP-030/032):** `<`, `>`, `<=`, `>=` compile; same-column pairs fold to an
   interval and push down; operators on `Bool`/`Option`/`List` are rejected at compile time; `OR`,
   `!=`, `LIKE` still return 400.
6. **Keyset pagination (QP-040/041):** paging through a large table with `AFTER` returns each row
   exactly once with no gaps/overlaps under a stable snapshot; page N+1 is a bounded index seek
   (rows-scanned ≈ page size, independent of N); the implicit PK tiebreak makes equal-value cursors
   unambiguous.
7. **Explain (QP-051):** `GET /query/explain` reports the index, bounds, residual, and
   index-served-order flag matching the executed plan.
8. **No regression:** the SPEC-005 subscription correctness property test (SUB, 10,000 random
   mutations) still passes with the planner enabled — client caches equal server query results;
   dedup and value-level fan-out pruning are unchanged.
