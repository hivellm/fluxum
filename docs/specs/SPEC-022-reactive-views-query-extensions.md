# SPEC-022 — Reactive Views & Query Extensions

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 3 (constraints, computed columns) · Phase 4 (reactive views, temporal, relational RLS) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-30, FR-32, FR-34, FR-11 (extends); new: FR-124 (reactive materialized views), FR-125 (temporal AS OF queries), FR-126 (declarative constraints/triggers), FR-127 (relational row visibility), FR-128 (computed columns) |
| **Requirement prefix** | `RV-` |
| **Source** | New (Fluxum-native). Materializes the `SUB-050` placeholder that [SPEC-018](SPEC-018-query-planner.md) and [SPEC-019](SPEC-019-fulltext-search.md) both defer to. Aggregates are a PRD non-goal for *ad-hoc SQL*, but live counters/leaderboards/dashboards are core realtime needs that today force client-side recomputation over diffs. |

Keywords are RFC 2119. Requirement IDs `RV-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

Extends the read/subscription surface with capabilities that the current delta engine cannot
express: incrementally-maintained **reactive views** (aggregates and sorted top-N pushed to
subscribers), **temporal `AS OF`** reads over retained MVCC versions, **declarative integrity**
(CHECK / references / per-table triggers) so mutation rules stop living only in hand-written reducer
code, **relational row visibility** (RLS predicated on another table), and **computed columns**
derived from siblings. All of these ride the existing post-commit delta fan-out
([subscription/mod.rs](../../crates/fluxum-core/src/subscription/mod.rs)) and tx pipeline.

## 2. Reactive materialized views (`RV-01x`)

### Requirement: Incrementally-maintained pushed views
- **RV-010** [P1] A `#[fluxum::view(materialized)]` declaration SHALL define a view over one table with
  an aggregate (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`) and optional `GROUP BY`, maintained incrementally on
  each commit from the delta rows — never a full re-scan.
- **RV-011** [P1] A materialized view MUST be subscribable; subscribers receive the changed view rows
  (per group) as a `TxUpdate`, with fan-out cost O(affected groups), not O(rows).
- **RV-012** [P1] A sorted top-N view (`ORDER BY col LIMIT n`) SHALL maintain a live ordered window,
  emitting enter/leave/reorder deltas as underlying rows change (the live-leaderboard case).
- **RV-013** [P1] View state MUST be crash-consistent: rebuilt from the base table on recovery or
  persisted and validated against a bit-identical recompute.

#### Scenario: Live leaderboard
Given a `top_scores` materialized view `ORDER BY score DESC LIMIT 10` with 10 subscribers
When a player's score update pushes them from rank 12 to rank 4
Then subscribers receive a bounded reorder delta and no full re-evaluation of the score table runs.

## 3. Temporal AS OF queries (`RV-02x`)

### Requirement: Point-in-time reads
- **RV-020** [P1] The storage engine SHALL retain superseded row versions for a configurable temporal
  window (bounded by budget / checkpoint horizon), tagged by committing `tx_id`/timestamp.
- **RV-021** [P1] `OneOffQuery` and `/query` SHALL accept `AS OF (tx_id | timestamp)` returning the
  committed state at that point; requests older than the retained window return a typed error.
- **RV-022** [P2] `AS OF` reads MUST honor RLS and column masking exactly as live reads do.

#### Scenario: Audit an earlier state
Given retention covers the last 24h
When a client runs `SELECT * FROM Order WHERE id = 7 AS OF timestamp '...'`
Then it receives the order exactly as it existed at that instant.

## 4. Declarative constraints & triggers (`RV-03x`)

### Requirement: Integrity without hand-written reducer guards
- **RV-030** [P1] Table attributes `#[check(expr)]`, `#[references(Table(col), on_delete=...)]`, and
  `#[not_null]` (for non-`Option` semantics) SHALL be validated in the commit pipeline before merge;
  a violation aborts the transaction with a typed error, exactly like a panic rollback.
- **RV-031** [P2] Declarative per-table hooks `#[fluxum::on_insert(Table)]` / `on_update` / `on_delete`
  SHALL run inside the same transaction as the mutation that triggers them (cascade/derive), reusing
  reducer isolation.
- **RV-032** [P1] `on_delete` referential actions (`restrict` default, `cascade`, `set_null`) MUST be
  applied atomically within the triggering transaction.

#### Scenario: Cascade delete
Given `LineItem` references `Order(id) on_delete=cascade`
When an `Order` row is deleted
Then its `LineItem` rows are deleted in the same transaction and both changes fan out together.

## 5. Relational row visibility (`RV-04x`)

### Requirement: Visibility predicated on another table
- **RV-040** [P1] `#[visibility(member_of(Table, key))]` SHALL make a row visible to an identity iff a
  matching row exists in the referenced membership table, evaluated for initial data and diffs.
- **RV-041** [P1] The evaluator MUST index membership so per-row visibility stays sub-linear; wire the
  currently-unimplemented `VisibilityRule::Custom` seam ([sql/mod.rs](../../crates/fluxum-core/src/sql/mod.rs)).

#### Scenario: Project-scoped rows
Given `Document` is `member_of(ProjectMember, project_id)`
When a user joins a project's membership table
Then they immediately receive that project's documents via their existing subscription.

## 6. Computed / generated columns (`RV-05x`)

### Requirement: Derived stored columns
- **RV-050** [P1] `#[computed(expr)]` SHALL derive a column value from sibling columns on write; the
  stored/indexed/replicated value is the computed result; it is read-only to reducers.
- **RV-051** [P1] Computed columns MAY be indexed and used in `WHERE`/`ORDER BY` like any column.

#### Scenario: Derived total
Given `Invoice` has `#[computed(qty * unit_price)] total`
When a reducer inserts an invoice with qty and unit_price
Then `total` is stored, indexable, and pushed to subscribers without reducer code setting it.

## 7. Non-goals

- Ad-hoc `GROUP BY`/aggregates in one-off SQL (materialized views are the sanctioned path).
- Multi-table JOIN views (single base table per view; denormalize or use typed edges SPEC-023).
- Unbounded temporal history (retention is windowed; PITR remains the long-horizon tool).
