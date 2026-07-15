# Proposal: phase3_schema-migration

## Why
Schemas evolve; without versioned migrations and auto-diff, every deploy against existing data is a gamble. Incompatible changes must abort startup, not corrupt data.

## What Changes
Implement the #[fluxum::migration(version)] runner: __schema_meta__ system table, auto-diff of declared vs stored schema, safe auto-apply for additive changes, abort on incompatible schema.

## Impact
- DAG task: T3.6
- Affected specs: SPEC-010 (schema migration)
- PRD requirements: FR-80
- Affected code: crates/fluxum-server (migration runner), crates/fluxum-macros (#[migration])
- Depends on: T3.1 (phase3_transactions)
- Breaking change: NO
- User benefit: safe schema evolution with additive changes applied automatically and destructive ones blocked
