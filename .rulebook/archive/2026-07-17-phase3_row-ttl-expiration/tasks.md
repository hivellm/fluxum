## 1. Implementation
- [x] 1.1 `#[ttl(col)]` parsed on a table; the named column is validated to be a `Timestamp` at compile time; carried as a link-time `TtlDef { TtlKind::Field }` side registry (DMX-020; crates/fluxum-macros/src/table.rs, crates/fluxum-core/src/schema/mod.rs)
- [x] 1.2 `#[ttl(after = "30m")]` duration form parsed via parse_duration_us; two `#[ttl]` on one table rejected; unknown/zero/negative duration rejected (compile-time + schema-assembly backstop) (DMX-020; crates/fluxum-macros/src/table.rs, crates/fluxum-core/src/schema/mod.rs)
- [x] 1.3 `TtlSweeper` (scheduler) scans a wait-free snapshot of every registered TTL table for expired rows — field mode compares the Timestamp column to now; sliding mode uses an in-memory identity witness like the DMX-011 ephemeral sweeper (DMX-020; crates/fluxum-core/src/scheduler/mod.rs)
- [x] 1.4 Expired rows are deleted in one ordinary transaction whose delete diffs fan out to subscribers; started per shard on serve via `ShardContext::start_ttl_sweeper` on both transports, publishing the diffs (DMX-020; crates/fluxum-core/src/scheduler/mod.rs, crates/fluxum-server/src/{lib.rs,http.rs,tcp.rs})
- [x] 1.5 At-least-once + idempotent: each doomed row is re-verified inside the delete transaction (field: still past; sliding: identity unchanged) so a racing write wins and a re-sweep of an already-deleted/refreshed row is a no-op (DMX-020; crates/fluxum-core/src/scheduler/mod.rs)
- [x] 1.6 Bounded/batched: each pass deletes at most TTL_SWEEP_BATCH (1024) rows and reports `more_pending`; the server loop keeps sweeping while capped so a large backlog drains across passes without one giant delete or a writer stall (DMX-021; crates/fluxum-core/src/scheduler/mod.rs, crates/fluxum-server/src/lib.rs)
- [x] 1.7 Verification: a `Session` with `#[ttl(after="30m")]`/`#[ttl(expires_at)]` compiles (trybuild pass; non-Timestamp column is a compile error); absolute expiry deletes only past rows and emits delete diffs; a refreshed row survives; a 2500-row backlog drains in bounded passes; sliding TTL expires since last write and a rewrite refreshes the window (crates/fluxum-core/tests/row_ttl.rs — 5 tests; crates/fluxum-macros/tests/ui/{pass,fail}/ttl_*.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
