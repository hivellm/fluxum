# Proposal: phase5_shardcoord-shardhost

## Why
Horizontal scale requires partitioning writes across shards while keeping small reference tables available everywhere; ShardCoord/ShardHost is that split.

## What Changes
Implement ShardCoord (partition-key routing, shard registry, #[table(global)] replication to all shards) and the ShardHost per-partition loop (MemStore + CommitLog + SubscriptionManager per shard).

## Impact
- DAG task: T5.4
- Affected specs: SPEC-007 (sharding)
- PRD requirements: FR-50, FR-51, FR-53
- Affected code: crates/fluxum-server (shard module)
- Depends on: G4
- Breaking change: NO
- User benefit: write throughput scales by adding shards; global tables stay readable on every shard
