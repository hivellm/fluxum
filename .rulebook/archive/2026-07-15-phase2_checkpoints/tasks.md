## 1. Implementation
- [x] 1.1 Implement incremental, content-addressed checkpoints: unchanged page/objects shared with the previous checkpoint by content hash; manifest with integrity hash (STG-020/STG-021, FR-14)
- [x] 1.2 Recovery = latest checkpoint + log replay; corrupted manifest or object hash detected on restore falls back to an older retained checkpoint (STG-021)
- [x] 1.3 Checkpoint writes never block reducer execution (verified under sustained write load, STG-022)
- [x] 1.4 Log truncation/compaction of segments fully covered by a completed checkpoint - routed through an archival hook so segments are archived (not deleted) when log archival is enabled (prerequisite for PITR, SPEC-014 REP-070; FR-104)
- [x] 1.5 Periodic checkpoint cadence every N committed transactions, configurable, with adaptive default from the hardware profile (FR-14, FR-113)
- [x] 1.6 Verification (DAG exit test): checkpoint + restore equivalence vs full-log replay; incremental checkpoint after small mutation writes only changed objects (no full-dump scaling cliff); recovery succeeds after compaction

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
