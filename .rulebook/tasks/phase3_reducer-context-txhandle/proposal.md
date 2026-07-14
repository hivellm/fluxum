# Proposal: phase3_reducer-context-txhandle

## Why
ReducerContext/TxHandle is the API every module author touches; intra-transaction reads (scan_pending/scan_all) are a differentiator SpacetimeDB lacks.

## What Changes
Implement ReducerContext, the full TxHandle write/read surface, intra-tx visibility, and nested reducer calls sharing one transaction.

## Impact
- DAG task: T3.2
- Affected specs: SPEC-004 (RED-001..005), SPEC-003 (TXN-050/051)
- PRD requirements: FR-17, FR-20
- Affected code: crates/fluxum-core (reducer api)
- Depends on: T3.1
- Breaking change: NO
- User benefit: ergonomic, fully transactional module API
