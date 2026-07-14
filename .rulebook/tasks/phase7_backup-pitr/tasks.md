## 1. Implementation
- [ ] 1.1 Implement commit-log segment archival: truncated/compacted segments archived (not deleted) under a retention config - the PITR replay source (FR-104 prerequisite; hook added in phase2_checkpoints)
- [ ] 1.2 Implement `fluxum backup create`: hot backup (latest checkpoint + archived segments) with no writer stall (throughput within noise), zstd-compressed, integrity manifest (FR-103, REP-060..)
- [ ] 1.3 Implement `fluxum backup verify`: full integrity check; a single injected bit-flip in a segment fails verify with a precise report (REP-064)
- [ ] 1.4 Implement `fluxum backup restore`: reproduces the exact state at the backup head (REP-063)
- [ ] 1.5 Implement PITR: replay archived segments to `--to-timestamp` / `--to-tx-id` (inclusive target, boundary reported); the restored node refuses to partial-sync into the old replica set and seeds a new one (FR-104, REP-070..REP-072)
- [ ] 1.6 Verification (DAG exit test): backup + restore + PITR round-trip in CI under sustained writes
- [ ] 1.7 Gate G7 input (PRD 12.2 backup/PITR criterion)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
