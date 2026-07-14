## 1. Implementation
- [ ] 1.1 Implement the full P0 Prometheus metrics catalogue (OBS-010..OBS-051) with `fluxum_` prefix, specified types/labels: reducer calls (outcome labels ok/err/rate_limited), duration histogram, tx commits/rollbacks, fan-out, memstore bytes, subscriber drops, buffer-pool gauges, shard state (FR-90; replication gauges land with T7.1/T7.2)
- [ ] 1.2 `fluxum_reducer_duration_us` exposes exactly the buckets [50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000] (SPEC-012 acceptance 3)
- [ ] 1.3 Structured logging via tracing: log_format json|pretty, configurable level, env override (FR-92; every json line parses with a level field)
- [ ] 1.4 Slow-reducer WARN above configurable threshold (default 5 ms / 5000 us) with shard/reducer/duration_us fields (FR-93); reducer panic produces an ERROR log with backtrace while the shard keeps serving (SPEC-012 acceptance 7/8)
- [ ] 1.5 /health payload: per-shard id/state/tx_id/queue_depth + effective-config exposure (probe inputs, derived values with sources, SIMD selection) within the 50 ms budget (FR-113, HWA-013/HWA-033)
- [ ] 1.6 Verification (DAG exit test): metrics endpoint (valid Prometheus text format, catalogue complete under demo load) + log format tests
- [ ] 1.7 Gate G5 input

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
