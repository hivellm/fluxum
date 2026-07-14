## 1. Implementation
- [ ] 1.1 Implement the 11-step atomic entity handoff protocol (row-set migration when a partition key changes shard) with the `__handoff__` marker (FR-52, SHD-041)
- [ ] 1.2 Fault injection at each of the 11 steps: after retry or abort back to the source shard, the entity is readable and consistent on exactly one shard - no lost or duplicated rows (SHD acceptance 4)
- [ ] 1.3 Client continuity during handoff: continuous ReducerCall stream executes each call exactly once (none dropped, none duplicated); subscribed clients observe no absence interval and no duplicate rows (SHD-042/SHD-044)
- [ ] 1.4 Cross-shard subscription aggregation by ShardCoord: TxUpdate tagged with the correct shard_id, per-shard ordering preserved (FR-54, SHD-051)
- [ ] 1.5 Verification (DAG exit test): 2-shard handoff test with zero data loss - post-handoff rows byte-identical (FluxBIN) to the pre-handoff export; source shard retains nothing
- [ ] 1.6 Gate G5 input: 2-shard handoff green (also PRD 12.1 MVP criterion)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
