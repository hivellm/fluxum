## 1. Implementation
- [ ] 1.1 Implement the append-only CommitLog entry format: `u32 LE + MessagePack + CRC32C` with epoch field (format freezes at G5 - replication and PITR replay it) (STG-010/STG-011)
- [ ] 1.2 Implement the group-commit flush actor: batched fsync, published durable offset advancing monotonically; fsync count far below tx count under load (STG-012, SPEC-002 acceptance 8)
- [ ] 1.3 Implement segment rotation + retention policy (decide OQ-5 defaults: segment size, rotation triggers)
- [ ] 1.4 Implement replay with non-destructive torn-tail repair: stop at first corrupt entry, quarantine the torn tail to a byte-identical sidecar file (never destructive truncation), resume appends at the last valid boundary, report last recovered tx_id + operator notification (STG-031, FR-13)
- [ ] 1.5 Blob-store handling for large values: identical large values stored once; blob bytes never reclaimed while any retained checkpoint references their hash (STG-041)
- [ ] 1.6 Verification (DAG exit test): write/replay tests over arbitrary insert/delete interleavings incl. torn-tail quarantine; tx_id strictly increasing across restart; auto-inc counters resume without reuse (STG-015, STG-040)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
