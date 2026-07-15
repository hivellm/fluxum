## 1. Implementation
- [ ] 1.1 Define a per-namespace (or per-identity) quota config: memory-budget share, reducer rate, max concurrent subscriptions, storage bytes; optional per namespace (OPS-060; crates/fluxum-core/src/config, quota model)
- [ ] 1.2 Per-namespace reducer-rate quota layered above the existing per-(Identity, reducer) buckets; exceeding yields a retryable 429-style typed error (OPS-060; crates/fluxum-core/src/reducer/ratelimit.rs)
- [ ] 1.3 Per-namespace memory-budget share against the buffer pool: a tenant over its share hits a typed exhaustion error without forcing eviction from other tenants' frames (OPS-060; crates/fluxum-core/src/store/pager/pool.rs)
- [ ] 1.4 Per-namespace subscription-count cap: a new subscription beyond the cap is refused with a typed error (OPS-060; crates/fluxum-core/src/subscription)
- [ ] 1.5 Per-namespace storage-bytes accounting and cap, with a typed error on exceed (OPS-060; crates/fluxum-core/src/store)
- [ ] 1.6 Export usage-vs-quota per tenant as fluxum_tenant_* metrics (rate, memory share, subscriptions, storage bytes) (OPS-061; crates/fluxum-server metrics)
- [ ] 1.7 Verification: tenant A saturating its reducer-rate quota receives 429s while tenant B's latency and admission are unaffected; a memory/subscription/storage over-quota yields the typed error only to the offending tenant; fluxum_tenant_* reflects usage

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
