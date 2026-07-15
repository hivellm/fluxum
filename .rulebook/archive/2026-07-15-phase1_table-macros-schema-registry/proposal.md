# Proposal: phase1_table-macros-schema-registry

## Why
The `#[fluxum::table]` macro surface is the module API developers write against; the schema registry is what every storage, subscription, and codegen component introspects.

## What Changes
Implement the `#[fluxum::table]` proc macro with `#[primary_key]`, `#[auto_inc]`, `#[index(btree(...))]`, composite PKs, and `#[spatial]`/`#[visibility]`/`partition_by` attribute parsing, plus the link-time schema registry (inventory) and `TableSchema` introspection.

## Impact
- DAG task: T1.1
- Affected specs: SPEC-001 (data model)
- PRD requirements: FR-15, FR-16, FR-81
- Affected code: crates/fluxum-macros, crates/fluxum-core
- Depends on: G0
- Breaking change: NO
- User benefit: declarative table definitions with compile-time schema validation and zero manual registration
