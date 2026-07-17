# Proposal: phase3_computed-generated-columns

## Why
Derived values such as `total = qty * unit_price` are recomputed by hand in every
reducer that writes the row and again by every client that reads it, so the
derivation drifts and cannot be indexed or filtered on server-side. `ColumnSchema`
in `crates/fluxum-core/src/schema/mod.rs` (line 63) has no notion of a computed
column, and the table macro `crates/fluxum-macros/src/table.rs` has no attribute
to declare one. A stored, indexed, replicated generated column derived on write
removes the hand-recomputation and makes the derived value first-class in
`WHERE`/`ORDER BY`. The gap: no `#[computed(expr)]` attribute and no
compute-on-write hook in the tx/reducer path.

## What Changes
Add `#[computed(expr)]` deriving a column value from sibling columns on write. The
stored/indexed/replicated value is the computed result; it is read-only to
reducers (reducers cannot set it). Computed columns may be indexed and used in
`WHERE`/`ORDER BY` like any other column.

## Impact
- Governing spec: docs/specs/SPEC-022-reactive-views-query-extensions.md
- Related specs: SPEC-003 (schema/data model); SPEC-018 (query planner, indexing)
- New PRD requirements: FR-128 (computed columns)
- Requirements covered: RV-050, RV-051
- Affected code: crates/fluxum-macros/src/table.rs (computed attr parsing); crates/fluxum-core/src/schema/mod.rs (ColumnSchema computed flag + expr, line 63); crates/fluxum-core/src/txn/mod.rs (compute value on write, before merge); crates/fluxum-core/src/index/mod.rs (index integration for computed columns)
- Depends on: phase1 macros (archived); phase3 tx (archived)
- Breaking change: NO
- User benefit: derived columns are computed once on write, stored, indexed, and pushed to subscribers, so clients and reducers stop recomputing them and can filter/sort on them server-side.
