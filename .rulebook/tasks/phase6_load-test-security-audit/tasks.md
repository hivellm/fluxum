## 1. Implementation
- [ ] 1.1 Build the load generator and run the sustained load test: >= 100,000 small-write reducer calls/s on one shard, measured via fluxum_reducer_calls_total; produce the load report (NFR-01, TST-060)
- [ ] 1.2 Fan-out latency measurement under load: TxUpdate delivery p99 < 5 ms with 1,000 subscribers (NFR-04, TST-061)
- [ ] 1.3 Security audit: auth bypass paths, RLS bypass paths, SQL injection against the subscription compiler (reusing the T4.1 injection corpus), rate-limit bypass; document findings - no P0 findings allowed (TST-070..TST-072)
- [ ] 1.4 Grafana dashboard: JSON committed to the repo covering ALL P0 metrics (reducer throughput/duration, fan-out, memstore/bufferpool, drops, shard state) + provisioning instructions (PRD 12.1 criterion)
- [ ] 1.5 Verification (DAG exit test): load report + audit report with no P0 findings; dashboard renders all P0 metrics against a running server
- [ ] 1.6 Gate G6 input (PRD 12.1: throughput benchmark + metrics/dashboard criteria)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
