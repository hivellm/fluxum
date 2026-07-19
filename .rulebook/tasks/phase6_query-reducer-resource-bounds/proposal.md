# Proposal: phase6_query-reducer-resource-bounds

## Why
OWASP Top 10:2025 findings **F-014 / F-015 (High, A06/A10)** and **F-016 / F-017
(Medium)**: there is no query `LIMIT` ceiling, no per-query row-scan budget, and
no wall-clock execution deadline; a reducer has no execution-time or per-transaction
memory bound; subscriptions/one-off queries are not per-identity rate-limited and
the global shard limiter is bypassable via token rotation; `idempotency_key` has no
length cap at decode. Because a shard has a single writer, one caller can stall it
for every tenant on that shard — a design-level availability gap that the
connection-level `phase6_ddos-overload-resilience` task does not close (that task
guards *connections*, not *query/reducer execution*).

## What Changes
Configurable query bounds (default + max `LIMIT`, row-scan budget, plan deadline);
reducer bounds (cooperative execution deadline + per-transaction allocation
ceiling, reusing the panic→rollback path); an identity/connection-keyed rate bucket
in front of subscription registration and one-off queries; and a decode-time cap on
`idempotency_key`.

## Impact
- Affected specs: SPEC query engine, SPEC reducer engine, SPEC-012 (metrics); new
  SEC-04x availability requirements.
- Affected code: `fluxum-core/src/sql/mod.rs`, `subscription/mod.rs::query_json`,
  `reducer/engine.rs`, `reducer/ratelimit.rs`, `quota.rs`.
- Breaking change: NO by default (bounds default to generous/current behavior;
  operators tighten per deployment).
- User benefit: a single caller can no longer exhaust a shard's writer or memory;
  noisy-neighbor isolation on shared shards.

## Notes
Scoped to the query/reducer execution layer. Connection-level flood/admission
control (ConnGuard, socket knobs, shed states) lives in
`phase6_ddos-overload-resilience`; per-IP global caps/blocklist in
`phase6_ip-blocklist-global-caps`. This task depends conceptually on
`phase6_proxy-aware-client-ip` for the resolved-IP keying of the secondary bucket.
