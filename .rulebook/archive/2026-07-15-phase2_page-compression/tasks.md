## 1. Implementation
- [x] 1.1 Implement LZ4 compression for cold pages with a compressibility threshold; zstd optional per config (FR-19, TIER-040..)
- [x] 1.2 Implement zstd compression for checkpoints (and later backups - reused by T7.3) (FR-19)
- [x] 1.3 Roundtrip property tests: LZ4 and zstd page round-trips bit-identical (TIER-043/TIER-044)
- [x] 1.4 Compression-ratio benchmark on the SPEC-013 reference corpus over the canonical demo schema, published as a bench artifact
- [x] 1.5 Verification (DAG exit test): ratio at least 3x on typical row data (SPEC-015 acceptance 3)
- [x] 1.6 Gate G2 input: compression suite + ratio bench green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
