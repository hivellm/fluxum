## 1. Implementation
- [ ] 1.1 Implement commit-log streaming as the replication protocol (log format = stream format, frozen at G5): batches with epoch numbers (FR-100, REP-010..)
- [ ] 1.2 Full sync: empty replica converges via checkpoint transfer + log tail (REP-012); partial sync: rejoining replica converges from its log offset (REP-013)
- [ ] 1.3 Async mode (default) + semi-synchronous quorum acknowledgment mode; semi-sync visibility barrier - no ReducerResult and no TxUpdate delivered before quorum append confirmed (REP-020/REP-021)
- [ ] 1.4 Epoch fencing: a partitioned stale primary's batches are rejected, it demotes and truncates its diverged suffix; fluxum_replication_fenced_total increments (REP-031)
- [ ] 1.5 Replication config surface: replica-set membership, mode (async|semi_sync), quorum size, max_staleness_ms - documented in the config reference
- [ ] 1.6 Replication metrics: fluxum_replication_offset/lag/semi_sync_wait_us and peers gauges (FR-105 with T7.2)
- [ ] 1.7 Verification (DAG exit test): replica converges from cold AND from offset (CommittedState equal, log byte-identical over shared tx_id range); semi-sync ack test; fencing test

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
