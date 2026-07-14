# Proposal: phase2_btree-indexes

## Why
Non-PK lookups need secondary indexes maintained transactionally; subscriptions and the SQL subset depend on them for anything beyond PK access.

## What Changes
Implement secondary B-tree indexes (single-column and composite) declared via #[index(btree(...))], maintained on commit.

## Impact
- DAG task: T2.4
- Affected specs: SPEC-001 (data model), SPEC-002 (storage engine)
- PRD requirements: FR-16
- Affected code: crates/fluxum-server (storage/index)
- Depends on: T2.1 (phase2_memstore-mvcc)
- Breaking change: NO
- User benefit: fast equality/range queries on any indexed column, kept consistent with commits
