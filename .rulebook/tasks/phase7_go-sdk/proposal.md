# Proposal: phase7_go-sdk

## Why
Go dominates backend/infrastructure services; a context-aware SDK completes coverage of the main server-side ecosystems for the competitive launch.

## What Changes
Implement the Go SDK (context-aware) over FluxRPC with generated typed bindings, validated by the shared conformance corpus.

## Impact
- DAG task: T7.5
- Affected specs: SPEC-011 (SDK codegen)
- PRD requirements: FR-85
- Affected code: sdks/go, crates/fluxum-cli (generate --lang go)
- Depends on: G6
- Breaking change: NO
- User benefit: idiomatic Go client with context cancellation across all operations
