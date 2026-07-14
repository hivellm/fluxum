# Proposal: phase3_transactions

## Why
Reducers, subscriptions, and replication all hinge on one correct commit pipeline with strict constraint enforcement and monotonic transaction ids.

## What Changes
Implement the transaction pipeline: validate -> merge CommittedState -> append CommitLog -> respond; rollback discards TxState; monotonic tx_id per shard; PK-uniqueness and auto-inc constraints.

## Impact
- DAG task: T3.1
- Affected specs: SPEC-003 (transactions)
- PRD requirements: FR-11, FR-15
- Affected code: crates/fluxum-server (txn module)
- Depends on: G2
- Breaking change: NO
- User benefit: ACID commits with clear constraint errors and deterministic ordering per shard
