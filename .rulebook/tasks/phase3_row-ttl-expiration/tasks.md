## 1. Implementation
- [ ] 1.1 Parse `#[ttl(field)]` on a table and validate the named column is a `Timestamp`; carry a TTL descriptor on TableSchema (DMX-020; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 Parse `#[ttl(after = "30m")]` duration form and reject declaring both forms on one table or an unknown/zero duration (DMX-020; crates/fluxum-macros/src/table.rs)
- [ ] 1.3 Register a scheduler sweep worker that periodically scans TTL tables for expired rows (DMX-020; crates/fluxum-core/src/scheduler/mod.rs)
- [ ] 1.4 Delete expired rows in normal transactions that emit delete diffs so subscribers receive the removals (DMX-020; crates/fluxum-core/src/reducer)
- [ ] 1.5 Make deletion at-least-once and idempotent: re-sweeping an already-deleted row is a no-op, not an error (DMX-020; crates/fluxum-core/src/reducer)
- [ ] 1.6 Bound and batch each sweep pass (max rows per transaction, yield between batches) so TTL never stalls the single writer (DMX-021; crates/fluxum-core/src/scheduler/mod.rs)
- [ ] 1.7 Verification: a `Session` table with `#[ttl(after="30m")]` compiles; aged rows are deleted by background transactions and subscribers receive the deletes; a large expired backlog drains in bounded batches without blocking foreground writes

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
