## 1. Implementation
- [ ] 1.1 Add a `Blob` FluxType whose row value is a content-hash reference, not the inline bytes (DMX-040; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.2 Build a content-addressed blob store over the commit-log blob-overflow path, keyed by content hash (DMX-040; crates/fluxum-core/src/commitlog/blob.rs)
- [ ] 1.3 Reuse the pager overflow-page handling to persist blob payloads outside the row (DMX-040; crates/fluxum-core/src/store/pager/)
- [ ] 1.4 Maintain a per-blob refcount that tracks how many rows reference each content hash (DMX-040; crates/fluxum-core/src/commitlog/blob.rs)
- [ ] 1.5 Add a dedicated admin/transport streaming upload/download endpoint for blob bytes, outside the 16 MB frame (DMX-041; crates/fluxum-server/src/admin.rs)
- [ ] 1.6 Garbage-collect a blob when its refcount reaches zero (last referencing row deleted) (DMX-041; crates/fluxum-core/src/commitlog/blob.rs)
- [ ] 1.7 Verification: a `User.avatar: Blob` column compiles; a 4 MB upload streams out of band and the row carries a content-hash reference; deleting all referencing rows reclaims the blob

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
