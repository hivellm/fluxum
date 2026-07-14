## 1. Implementation
- [ ] 1.1 Build the `fluxum-bench` workload driver: demo workload (chat + presence + tasks) with configurable client population, write rates, subscription counts (NFR-11; crate bootstrapped in phase0_workspace-ci-skeleton)
- [ ] 1.2 Build the parity baseline app: functionally identical application on app-server + PostgreSQL (decide OQ-9: axum+sqlx vs Node/Express+pg, possibly both) plus the SQLite variant; same schema, same operations, same client behavior
- [ ] 1.3 Honest-comparison protocol: equal hardware, tuned Postgres (indexes, prepared statements), durability settings documented both ways (synchronous_commit on/off), LISTEN/NOTIFY fan-out for the e2e latency comparison; publish harness + configs with the repo
- [ ] 1.4 Comparative report generator producing the release artifact (report is regenerated per release: v1 at 0.1.0/G6, v2 at 0.2.0/G7)
- [ ] 1.5 Measure all four NFR-11 ratios: write throughput >= 10x, end-to-end change-to-subscriber p99 >= 10x lower, hot reads >= 50x lower latency, cold (page-in) reads within 2x of PostgreSQL
- [ ] 1.6 Verification (DAG exit test): report v1 meets the write, e2e-latency, and cold-read thresholds
- [ ] 1.7 Gate G6 input: parity report v1 (PRD 12.1)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
