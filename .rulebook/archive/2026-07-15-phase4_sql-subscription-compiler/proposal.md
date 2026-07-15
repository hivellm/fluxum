# Proposal: phase4_sql-subscription-compiler

## Why
Subscriptions are declared in a SQL subset; compiling them once into a CompiledPlan is what makes per-commit evaluation cheap and injection-safe.

## What Changes
Implement the SQL subset compiler: SELECT * FROM T [WHERE pred] [IN REGION ...] [WITHIN RADIUS ...] compiled to a CompiledPlan (table filter, spatial constraint, visibility rule).

## Impact
- DAG task: T4.1
- Affected specs: SPEC-005 (subscriptions)
- PRD requirements: FR-30, FR-35
- Affected code: crates/fluxum-server (sql/subscription module)
- Depends on: G3, T2.6 (phase2_rtree-spatial-predicates)
- Breaking change: NO
- User benefit: expressive, safe subscription queries including spatial filters
