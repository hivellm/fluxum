## 1. Implementation
- [ ] 1.1 Implement `ShardCoord`: partition-key routing for hash, range, and geospatial-region strategies; shard registry (FR-50, FR-51); decide OQ-2 (tasks vs processes) and OQ-6 (default partitioning when partition_by is absent)
- [ ] 1.2 Implement `ShardHost` per-partition loop owning its MemStore + CommitLog + SubscriptionManager (independent storage per shard)
- [ ] 1.3 Routing determinism: golden vectors - the same key resolves to the same shard across restarts and platforms; region resolution matches the floor(x / region_size) grid at cell boundaries (SHD-003)
- [ ] 1.4 `#[fluxum::table(global)]` read-only replication to all shards: write on non-authoritative shard errors (SHD-031); committed authoritative write readable on every shard before ReducerResult returns; replicas write no CommitLog entries for replicated mutations (FR-53, SHD-030)
- [ ] 1.5 Shard independence: panicking reducer + saturated queue on shard 0 leave shard 1 throughput/latency unaffected; per-shard ticks fire independently (SHD-020..SHD-022)
- [ ] 1.6 Lifecycle: per-shard kill -9 recovery via SPEC-002 replay; graceful shutdown drains in-flight calls, writes a final snapshot, closes the log (SHD-061)
- [ ] 1.7 Verification (DAG exit test): single- and multi-shard boot tests

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
