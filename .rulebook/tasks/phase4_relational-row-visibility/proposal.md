# Proposal: phase4_relational-row-visibility

## Why
Today only `owner_only` RLS is enforced (an O(1) column-equals-viewer check);
`compile_visibility` in `crates/fluxum-core/src/sql/mod.rs` (~line 230) compiles
`VisibilityRule::Custom(_)` to `None`, i.e. no filter — a documented gap. But
"a row is visible iff a membership row exists in another table"
(guild/project/tenant membership) is a very common pattern the current
`VisibilityRule` in `crates/fluxum-core/src/schema/mod.rs` (~line 177) cannot
express. The `Custom` seam exists but is unwired. The gap: no relational
visibility rule and no wiring of the `Custom` variant into an actual per-row
filter applied to initial data and diffs in
`crates/fluxum-core/src/subscription/mod.rs`.

## What Changes
Add `#[visibility(member_of(Table, key))]` making a row visible to an identity
iff a matching row exists in the referenced membership table, evaluated for both
initial data and diffs. Index the membership table so per-row visibility stays
sub-linear. Wire the currently-unimplemented `VisibilityRule::Custom` seam so the
compiled membership rule produces a real `RlsFn` applied by the subscription
fan-out.

## Impact
- Governing spec: docs/specs/SPEC-022-reactive-views-query-extensions.md
- Related specs: SPEC-005 (row-level visibility / RLS); SPEC-018 (query planner)
- New PRD requirements: FR-127 (relational row visibility)
- Requirements covered: RV-040, RV-041
- Affected code: crates/fluxum-core/src/schema/mod.rs (VisibilityRule member_of variant); crates/fluxum-core/src/sql/mod.rs (compile membership rule to an RlsFn, wiring the Custom seam ~line 230); crates/fluxum-core/src/subscription/mod.rs (apply the relational filter to initial data + diffs); crates/fluxum-core/src/index/ (membership index for sub-linear lookup)
- Depends on: phase4 visibility/RLS (T4.3, archived)
- Breaking change: NO
- User benefit: rows scoped by membership (project/guild/tenant) are delivered automatically — a user who joins a membership table immediately receives that scope's rows through their existing subscription, without O(rows) evaluation.
