# Proposal: phase4_reactive-materialized-views

## Why
Ad-hoc `GROUP BY`/aggregates are a deliberate PRD non-goal, yet live counters,
leaderboards, and dashboards are core realtime needs. Today the delta engine in
`crates/fluxum-core/src/subscription/mod.rs` only fans out per-row diffs
(`QueryDelta`/`TxUpdate`, ~line 128/449), so every client must recompute
aggregates over those diffs itself. The `#[fluxum::view]` machinery in
`crates/fluxum-core/src/reducer/view.rs` is read-only pull (admin `GET /view/:name`),
not an incrementally-maintained pushed view. SPEC-018 (query planner) and
SPEC-019 (full-text) both explicitly defer the `SUB-050` materialized-views
placeholder — this task materializes it. The gap: no server-maintained
aggregate/top-N view that pushes bounded deltas to subscribers.

## What Changes
Add `#[fluxum::view(materialized)]` defining a view over one table with a single
aggregate (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`) and optional `GROUP BY`, maintained
incrementally from the commit's delta rows — never a full re-scan. The view is
subscribable through the existing fan-out with cost O(affected groups), emitting
changed view rows as `TxUpdate`. A sorted top-N view (`ORDER BY col LIMIT n`)
maintains a live ordered window, emitting enter/leave/reorder deltas as the
underlying rows change (live-leaderboard case). View state is crash-consistent:
rebuilt from the base table on recovery or persisted and validated against a
bit-identical recompute.

## Impact
- Governing spec: docs/specs/SPEC-022-reactive-views-query-extensions.md
- Related specs: SPEC-018 (query planner, defers SUB-050); SPEC-019 (full-text, defers SUB-050); SPEC-004 (reducers/views)
- New PRD requirements: FR-124 (reactive materialized views)
- Requirements covered: RV-010, RV-011, RV-012, RV-013
- Affected code: crates/fluxum-core/src/subscription/mod.rs (view registry + fan-out); crates/fluxum-core/src/sql/mod.rs (aggregate/top-N plan compile); crates/fluxum-macros/src/reducer.rs (view macro, materialized variant); crates/fluxum-core/src/reducer/view.rs (view surface)
- Depends on: phase4 subscription manager (T4.2, archived); phase4 SQL compiler (T4.1, archived)
- Breaking change: NO
- User benefit: live counters, dashboards, and leaderboards are pushed and maintained by the server with bounded deltas, eliminating client-side recomputation over diffs.
