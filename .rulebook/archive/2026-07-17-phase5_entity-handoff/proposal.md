# Proposal: phase5_entity-handoff

## Why
Entities must move between shards (rebalancing, locality) without losing rows or breaking live subscriptions; the atomic handoff protocol plus cross-shard aggregation makes sharding transparent to clients.

## What Changes
Implement entity handoff (the 11-step atomic row-set migration between shards) and cross-shard subscription aggregation.

## Impact
- DAG task: T5.5
- Affected specs: SPEC-007 (sharding)
- PRD requirements: FR-52, FR-54
- Affected code: crates/fluxum-server (shard module)
- Depends on: T5.4 (phase5_shardcoord-shardhost)
- Breaking change: NO
- User benefit: shard topology changes are invisible to clients; no data loss, no dropped subscriptions
