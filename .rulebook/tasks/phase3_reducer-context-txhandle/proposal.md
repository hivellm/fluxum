# Proposal: phase3_reducer-context-txhandle

## Why
ReducerContext + TxHandle is the entire data API reducers see; intra-tx reads (scan_pending/scan_all) are what let reducers act on their own uncommitted writes.

## What Changes
Implement ReducerContext + TxHandle: insert/delete/upsert/query_pk/scan/scan_where plus intra-tx scan_pending/scan_all/count_pending.

## Impact
- DAG task: T3.2
- Affected specs: SPEC-004 (reducers)
- PRD requirements: FR-17, FR-20
- Affected code: crates/fluxum-server (reducer module), crates/fluxum-core (context types)
- Depends on: T3.1 (phase3_transactions)
- Breaking change: NO
- User benefit: a complete, typed data API inside reducers including reads of pending writes
