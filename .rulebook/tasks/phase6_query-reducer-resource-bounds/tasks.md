## 1. Implementation
- [ ] 1.1 Query bounds: configurable default + max `LIMIT` (queries without a limit get the default; over-max is clamped or rejected by config), a per-query row-scan budget, and a wall-clock deadline that aborts the plan cleanly (`sql/mod.rs`, `subscription/mod.rs::query_json`)
- [ ] 1.2 Reducer bounds: a cooperative execution deadline checked at stdlib/host-call boundaries + a per-transaction allocation ceiling; breach → rollback + counter, reusing the existing panic→rollback path (`reducer/engine.rs`)
- [ ] 1.3 Per-identity / per-connection rate limits in front of subscription registration and one-off queries; make the global shard guard mandatory-on; add an IP/connection-keyed secondary bucket so token rotation cannot mint fresh budget (`reducer/ratelimit.rs`, `quota.rs`)
- [ ] 1.4 Cap `idempotency_key` length at decode (F-017): reject over-length keys before they reach the dedup map
- [ ] 1.5 Metrics: `fluxum_query_aborted_total{reason}` (`limit`, `scan_budget`, `deadline`), `fluxum_reducer_aborted_total{reason}` (`deadline`, `alloc`), rate-limit rejection counters; wire into SPEC-012
- [ ] 1.6 Spec: SPEC query/reducer engine bounds; new SEC-04x availability requirements; SPEC-012 gains the new metrics
- [ ] 1.7 Verification: an unbounded/over-max `LIMIT` is clamped or rejected; a runaway scan hits the row-scan budget; a slow query/reducer is aborted at the deadline with rollback and a counter; a token-rotating caller cannot exceed the IP-keyed bucket; an over-length `idempotency_key` is refused

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
