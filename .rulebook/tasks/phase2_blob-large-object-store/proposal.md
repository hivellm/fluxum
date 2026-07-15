# Proposal: phase2_blob-large-object-store

## Why
Attachments, avatars, and images are routinely referenced by rows but exceed the 16 MB transport frame, so they cannot travel as an ordinary `Bytes` column. The plumbing for large values already half-exists: the commit log has a blob-overflow module (crates/fluxum-core/src/commitlog/blob.rs) and the pager has overflow-page handling (crates/fluxum-core/src/store/pager/), but there is no first-class file API — no `Blob` column type, no content-addressed store, and no streaming upload/download path. This adds a first-class blob store so rows can reference large objects by content hash.

## What Changes
Add a `Blob` column type whose value stores large bytes in a content-addressed, refcounted blob store, keeping only a reference (content hash) inline in the row — reusing the existing commit-log blob-overflow path as the backing store. Upload and download stream over a dedicated admin/transport endpoint rather than through the 16 MB frame. Blobs are reference-counted per referencing row and garbage-collected when their refcount reaches zero. Existing `Bytes` columns for small inline payloads are unchanged.

## Impact
- Governing spec: SPEC-023 §5 (Blob / large-object store, DMX-040..041) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (FluxType/ColumnSchema — new Blob type), and the phase-2 storage / admin-transport specs the overflow and streaming paths derive from
- New PRD requirements: FR-132 (blob / large-object store)
- Requirements covered: DMX-040, DMX-041
- Affected code: crates/fluxum-core/src/commitlog/blob.rs (content-addressed, refcounted backing store), crates/fluxum-core/src/store/pager/ (overflow page reuse), crates/fluxum-core/src/schema/mod.rs (Blob FluxType + inline reference), crates/fluxum-server/src/admin.rs (streaming upload/download endpoint)
- Depends on: phase-2 storage (commit-log blob overflow, pager) — archived
- Breaking change: NO (new opt-in column type; Bytes and existing tables unaffected)
- User benefit: store and serve avatars, attachments, and images above the frame limit by content-hash reference, with automatic reclamation when the last referencing row is deleted
