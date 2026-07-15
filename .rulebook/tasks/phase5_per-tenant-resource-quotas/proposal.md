# Proposal: phase5_per-tenant-resource-quotas

## Why
Once one binary hosts multiple namespaces (phase5_database-namespaces-multitenancy), a single noisy tenant can starve the others: the reducer rate limiter's token buckets are per-(Identity, reducer) with only a global shard guard (crates/fluxum-core/src/reducer/ratelimit.rs, RED-052), the buffer pool enforces one process-wide memory budget with no per-tenant share (crates/fluxum-core/src/store/pager/pool.rs, TIER-003 BufferPoolExhausted), and subscription counts and storage bytes are unbounded per tenant. There is no per-namespace ceiling, so tenant A can exhaust memory or saturate the reducer path and degrade tenant B. SPEC-025 OPS-060/061 add per-tenant quotas so isolation is enforced, not just structural — foreshadowing HiveHub.Cloud. Marked P2.

## What Changes
Add per-namespace (or per-identity) quotas bounding: memory budget share (a fraction/cap of the buffer pool), reducer call rate, concurrent subscription count, and storage bytes. Exceeding any quota yields a typed error to the offending tenant — a retryable 429-style code for rate, a distinct exhaustion error for memory/storage/subscriptions — without affecting other tenants' latency or admission. Current usage against each quota is exported as `fluxum_tenant_*` metrics so operators can see headroom and set alerts. Quotas are optional config; an unquotaed namespace behaves as today.

## Impact
- Governing spec: SPEC-025 §7 Per-tenant quotas (OPS-060, OPS-061) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-004 (reducer rate limiting), SPEC-015 (memory budget / buffer pool), SPEC-005 (subscriptions), SPEC-012 (metrics)
- New PRD requirements: FR-144 (per-tenant quotas)
- Requirements covered: OPS-060, OPS-061
- Affected code: crates/fluxum-core/src/reducer/ratelimit.rs (per-namespace reducer-rate quota), crates/fluxum-core/src/store/pager/pool.rs (per-namespace memory-budget share), crates/fluxum-core/src/subscription (subscription-count cap), storage-bytes accounting, crates/fluxum-server metrics (fluxum_tenant_* series)
- Depends on: phase5_database-namespaces-multitenancy
- Breaking change: NO (quotas optional; unquotaed namespace = current behavior)
- User benefit: noisy-neighbor isolation — one tenant hitting a quota is contained without affecting others, with per-tenant usage visible in metrics
- Priority: P2
