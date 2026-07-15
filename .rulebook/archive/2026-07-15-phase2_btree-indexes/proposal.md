# Proposal: phase2_btree-indexes

## Why
Secondary indexes make WHERE-filtered subscriptions and scans O(log n); composite indexes cover the (channel, sent_at)-style access paths the demo and parity workloads depend on.

## What Changes
Implement single-column and composite B-tree secondary indexes maintained on commit, with order-preserving key encoding designed for paged eviction (T2.8).

## Impact
- DAG task: T2.4
- Affected specs: SPEC-001 (indexes), SPEC-002 (maintenance on commit), SPEC-015 (paged indexes)
- PRD requirements: FR-16
- Affected code: crates/fluxum-core (index)
- Depends on: T2.1
- Breaking change: NO
- User benefit: indexed scans return exactly what full scans would, at O(log n)
