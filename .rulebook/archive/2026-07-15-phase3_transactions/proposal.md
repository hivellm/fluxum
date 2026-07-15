# Proposal: phase3_transactions

## Why
The commit pipeline is where ACID becomes real: every reducer call must be one atomic transaction with monotonic tx_ids, constraints, and full rollback.

## What Changes
Implement validate -> merge -> append -> respond, gap-free tx_id across recovery, PK/unique/auto-inc constraints, the bounded reducer queue (503 on overflow), and the NFR-03 commit-latency benchmark.

## Impact
- DAG task: T3.1
- Affected specs: SPEC-003 (TXN-*)
- PRD requirements: FR-11, FR-15, NFR-03
- Affected code: crates/fluxum-core (txn)
- Depends on: G2
- Breaking change: NO
- User benefit: partial writes never reach clients; commit p99 under 1 ms
