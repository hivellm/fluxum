## 1. Implementation
- [ ] 1.1 Implement fluxum backup create: hot backup from checkpoint pages + log segments, zstd-compressed, without stalling the writer
- [ ] 1.2 Implement fluxum backup verify: integrity check of manifest hash, page checksums, and log CRCs
- [ ] 1.3 Implement fluxum backup restore into a fresh data directory
- [ ] 1.4 Implement commit-log segment archival and PITR replay to a target timestamp or tx_id
- [ ] 1.5 Verification (DAG exit test): backup + restore + PITR round-trip green in CI
- [ ] 1.6 Gate G7 input: PRD section 12.2 all green - failover + PITR + 5 SDKs + 1B-row soak + parity report v2 (release 0.2.0)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
