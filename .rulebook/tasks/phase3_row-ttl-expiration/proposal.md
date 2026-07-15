# Proposal: phase3_row-ttl-expiration

## Why
Sessions, rate-limit buckets, verification codes, and other short-lived durable rows should disappear on their own; expiring them by hand in application code is error-prone and universal. Fluxum has no time-to-live seam today: the macro parses only `#[primary_key]`/`#[auto_inc]`/`#[default]`/`#[rename]` per field (crates/fluxum-macros/src/table.rs:268-296) with no `#[ttl]`, and the scheduler (crates/fluxum-core/src/scheduler/mod.rs) has no expiry sweep. This adds declarative row TTL that a background worker enforces as ordinary delete transactions.

## What Changes
Admit `#[ttl(field)]` (expire when a `Timestamp` column is in the past) and `#[ttl(after = "30m")]` (expire N after row age) on a table. The schedule worker deletes expired rows in normal transactions that emit delete diffs; deletion is at-least-once and idempotent, so a redelivered sweep on an already-deleted row is a no-op. Sweeps are batched and bounded so they never stall the single writer: each pass deletes at most a bounded batch and yields. Non-TTL tables are untouched and pay nothing.

## Impact
- Governing spec: SPEC-023 §3 (Row TTL, DMX-020..021) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (table macro surface, Timestamp column type), and the phase-3 scheduler spec the sweep worker derives from
- New PRD requirements: FR-130 (row TTL expiration)
- Requirements covered: DMX-020, DMX-021
- Affected code: crates/fluxum-macros/src/table.rs (#[ttl(field)] / #[ttl(after=...)] parsing + validation), crates/fluxum-core/src/scheduler/mod.rs (batched, bounded expiry sweep), crates/fluxum-core/src/reducer (batched delete transactions emitting delete diffs)
- Depends on: phase-3 scheduler — archived
- Breaking change: NO (opt-in per-table attribute; tables without #[ttl] are unaffected)
- User benefit: sessions, rate buckets, and temporary rows expire automatically as background transactions, with subscribers receiving the deletes and no writer stalls
