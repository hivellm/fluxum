## 1. Implementation
- [ ] 1.1 Consensus-based primary election per replica set (decide OQ-8: openraft vs custom Raft); automatic failover on primary failure (FR-101, REP-030/REP-032)
- [ ] 1.2 Stale-primary handling: demote + diverged-suffix truncation on rejoin; no write from a stale epoch survives in the set (REP-031, REP-013)
- [ ] 1.3 SDK transparent failover: clients reconnect, re-authenticate, resubscribe; reconciled caches match the new primary exactly (REP-033; SDK-047)
- [ ] 1.4 Read + subscription fan-out offload to replicas: replica TxUpdate streams content-identical to the primary incl. RLS filtering and TxMeta (FR-102, REP-043); bounded observable staleness - replica past max_staleness_ms rejects new admissions with ReplicaStale (REP-041)
- [ ] 1.5 Replication observability: /health reports role/epoch/replicas/offset/lag (REP-080) within the OBS-061 latency bound; all REP-081 metrics + REP-082 structured events through a scripted election + resync sequence (FR-105)
- [ ] 1.6 Replication DST scenarios green (TST-134)
- [ ] 1.7 Verification (DAG exit test): failover drill - kill -9 the primary of a 3-member semi-sync set under sustained writes: a replica promotes and every client-observed transaction is present; zero committed-tx loss; fan-out offload verified (1,000 subscribers moved to a replica reduce primary fan-out with write throughput unchanged)
- [ ] 1.8 Gate G7 input (PRD 12.2 failover criterion)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
