# Proposal: phase4_temporal-as-of-queries

## Why
The MVCC committed state (`crates/fluxum-core/src/store/committed.rs`,
`crates/fluxum-core/src/store/memstore.rs`), the commit log
(`crates/fluxum-core/src/commitlog/`), and checkpoints
(`crates/fluxum-core/src/checkpoint/`) already exist, but point-in-time recovery
is an operator-only, whole-database rewind. There is no way for a client to read
the committed state as of an earlier `tx_id`/timestamp. Exposing point-in-time
reads to clients yields cheap undo, audit, and debugging over the versions the
engine already tracks. The gap: `OneOffQuery` and the admin `/query` endpoint
(`crates/fluxum-server/src/admin.rs`) and the SQL plan
(`crates/fluxum-core/src/sql/mod.rs`) have no `AS OF` surface, and superseded row
versions are not retained past compaction.

## What Changes
Retain superseded row versions for a configurable temporal window (bounded by a
memory budget / checkpoint horizon), each tagged by the committing
`tx_id`/timestamp. `OneOffQuery` and `/query` accept `AS OF (tx_id | timestamp)`
and return the committed state at that point; a request older than the retained
window returns a typed error. `AS OF` reads honor RLS and column masking exactly
as live reads do.

## Impact
- Governing spec: docs/specs/SPEC-022-reactive-views-query-extensions.md
- Related specs: SPEC-002 (storage engine/MVCC); SPEC-018 (query planner)
- New PRD requirements: FR-125 (temporal AS OF queries)
- Requirements covered: RV-020, RV-021, RV-022
- Affected code: crates/fluxum-core/src/store/memstore.rs and crates/fluxum-core/src/store/committed.rs (windowed version retention tagged by tx_id/timestamp); crates/fluxum-core/src/sql/mod.rs (AS OF parse + plan); crates/fluxum-server/src/admin.rs (/query AS OF); crates/fluxum-core/src/subscription/mod.rs (snapshot read at an offset)
- Depends on: phase2 storage (archived); phase4 query (T4.1, archived)
- Breaking change: NO
- User benefit: clients get undo, audit, and time-travel debugging by reading the exact committed state at any tx_id/timestamp within the retention window, with RLS/masking preserved.
