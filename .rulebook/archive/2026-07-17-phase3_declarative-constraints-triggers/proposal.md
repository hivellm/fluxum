# Proposal: phase3_declarative-constraints-triggers

## Why
Today reducers are the only mutation path, and the commit pipeline
(`crates/fluxum-core/src/txn/mod.rs`, validate → merge into `CommittedState`)
enforces nothing beyond unique constraints
(`crates/fluxum-core/src/store/unique.rs`). Every integrity rule — foreign keys,
value checks, not-null on non-`Option` columns, cascade deletes — is hand-written
inside each reducer, so auditors miss cases and rules drift between reducers.
Moving these into the schema, validated in the pipeline before merge, makes
integrity declarative and uniform. The gap: no `#[check]`/`#[references]`/`#[not_null]`
table attributes and no declarative per-table triggers exist in
`crates/fluxum-macros/src/table.rs` or `crates/fluxum-macros/src/reducer.rs`.

## What Changes
Add table attributes `#[check(expr)]`, `#[references(Table(col), on_delete=...)]`,
and `#[not_null]` validated in the commit pipeline before merge; a violation
aborts the transaction with a typed error, exactly like a panic rollback. Add
declarative per-table hooks `#[fluxum::on_insert(Table)]` / `on_update` /
`on_delete` that run inside the same transaction as the triggering mutation,
reusing reducer isolation. `on_delete` referential actions (`restrict` default,
`cascade`, `set_null`) are applied atomically within the triggering transaction.

## Impact
- Governing spec: docs/specs/SPEC-022-reactive-views-query-extensions.md
- Related specs: SPEC-004 (reducer engine/isolation); SPEC-003 (schema/data model)
- New PRD requirements: FR-126 (declarative constraints/triggers)
- Requirements covered: RV-030, RV-031, RV-032
- Affected code: crates/fluxum-macros/src/table.rs (check/references/not_null attr parsing); crates/fluxum-macros/src/reducer.rs (on_insert/on_update/on_delete hook macros); crates/fluxum-core/src/txn/mod.rs (constraint validation before merge + cascade actions); crates/fluxum-core/src/reducer/ (trigger dispatch); crates/fluxum-core/src/schema/mod.rs (constraint metadata on TableSchema/ColumnSchema)
- Depends on: phase3 reducer engine + tx pipeline (archived)
- Breaking change: NO
- User benefit: integrity rules live in the schema and are enforced uniformly before merge, so cascade/foreign-key/check violations abort transactions consistently instead of relying on hand-written reducer guards.
