# Proposal: phase6_schema-export-api-freeze

## Why
Every SDK generator consumes the /schema JSON; finalizing it and shipping fluxum schema export is the module API freeze event that all codegen depends on.

## What Changes
Finalize the /schema JSON document and implement the fluxum schema export CLI command; this is the module API freeze (T6.1) — after it, #[fluxum::*] surface and schema JSON changes must be additive.

## Impact
- DAG task: T6.1
- Affected specs: SPEC-011 (SDK codegen); freezes SPEC-001/004/011 module API surface
- PRD requirements: FR-81
- Affected code: crates/fluxum-server (/schema), crates/fluxum-cli (schema export)
- Depends on: G5
- Breaking change: NO (defines the compatibility contract going forward)
- User benefit: a stable, machine-readable schema contract for all current and future SDKs
