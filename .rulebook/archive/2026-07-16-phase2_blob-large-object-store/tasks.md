## 1. Implementation
- [x] 1.1 `Blob` FluxType whose row value is a 32-byte content-hash reference (`types::BlobRef`), never the inline bytes; FluxBIN = 32 raw bytes; threaded through RowValue/LogValue/StoredType/memcomparable/JSON; `BlobRef` parses/displays as 64-hex (DMX-040; crates/fluxum-core/src/{schema/mod.rs,types.rs,store/row.rs})
- [x] 1.2 Content-addressed store over the existing `BlobStore` (SHA-256 object files, durable tmp+fsync+rename create, dedup by hash) extended with `stage`/`contains`/`incref`/`rebuild_refcounts`/`gc` (DMX-040; crates/fluxum-core/src/commitlog/blob.rs)
- [x] 1.3 Blob payloads persist **outside the row and outside pages entirely**: rows carry only the 32-byte ref, so the pager never sees payload bytes — the content-addressed object files subsume the overflow-page role (and checkpoints already pin them via STG-041 holds); no pager change needed (DMX-040)
- [x] 1.4 Refcount = row references: write-time validation rejects a `Blob` value naming no stored object (or with no store attached); the commit merge increfs inserted rows' blobs and unrefs deleted rows' (increments before decrements, under the writer lock); `MemStore::attach_blob_store` rebuilds counts from the live snapshot after recovery (DMX-040; crates/fluxum-core/src/store/memstore.rs)
- [x] 1.5 Dedicated out-of-band HTTP endpoints on :15800 — `POST /blob` (raw body up to 256 MiB → `{"hash": ...}`, staged under an upload lease) and `GET /blob/:hash` (octet-stream); 404 with no store installed, 400 bad hash/empty body; `ShardContext::set_blob_store` wires store + endpoints (DMX-041; crates/fluxum-server/src/{http.rs,lib.rs})
- [x] 1.6 GC: `reclaim()` deletes refcount-0 unheld objects; upload leases (per-hash holds) protect staged-but-unreferenced bytes until the first row reference releases them or `gc(orphan_age)` collects the orphan (DMX-041; crates/fluxum-core/src/commitlog/blob.rs)
- [x] 1.7 Verification: `User.avatar: BlobRef` compiles (trybuild pass, incl. `Option<BlobRef>`); a 4 MB upload streams out of band and rows carry the hash reference; shared blob counted per row; update swaps references; deleting all referencing rows reclaims the bytes (crates/fluxum-core/tests/blob_store.rs — 4 tests; crates/fluxum-server/tests/blob_endpoints.rs — 2 tests)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
