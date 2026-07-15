## 1. Implementation
- [ ] 1.1 Add an object-store client abstraction (S3-compatible: put/get/get-range/list/head) with endpoint/bucket/prefix/credentials from config; keep a local-fs backend so the trait is target-agnostic (OPS-010; new module + crates/fluxum-core/src/config)
- [ ] 1.2 Write archived commit-log segments with seekable-zstd framing (independently decodable blocks + frame index) so a byte range can be range-read without whole-segment download (OPS-010; crates/fluxum-core/src/commitlog/segment.rs)
- [ ] 1.3 Extend `fluxum backup create` to upload the latest checkpoint + newly sealed segments to the object-store target, recording each artifact's content hash and object key in the backup manifest (OPS-010/OPS-011; crates/fluxum-cli/src, crates/fluxum-core/src/checkpoint/manifest.rs)
- [ ] 1.4 Incremental scheduled archival: upload only new/changed checkpoint pages and freshly sealed segments, driven off the checkpoint worker so writers never stall (throughput within noise) (OPS-011; crates/fluxum-core/src/checkpoint/worker.rs, repo.rs)
- [ ] 1.5 Download-side integrity: on restore/PITR fetch, re-hash each artifact and fail with a precise report on any mismatch; a single injected bit-flip in an uploaded segment fails verify (OPS-011; crates/fluxum-core/src/checkpoint/recover.rs)
- [ ] 1.6 PITR range-read: replay resolves the target tx_id/timestamp to a segment + byte window and range-reads only that window from object storage (OPS-010; crates/fluxum-core/src/commitlog/replay.rs)
- [ ] 1.7 Verification: backup → object store → restore + PITR round-trip against a local S3-compatible fixture under sustained writes reproduces the exact head state and the boundary target record

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
