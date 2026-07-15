## 1. Implementation
- [x] 1.1 Parse `max_rate = "N/s"` on `#[fluxum::reducer]` and build token buckets per (Identity, reducer) (FR-24) — macro parses the `"N/s"` literal into `ReducerDef.max_rate_per_sec`; `reducer::ratelimit::RateLimiter` holds lazy per-pair `TokenBucket`s (capacity N, continuous refill at N/s) in shard memory only, never in `CommittedState`
- [x] 1.2 Reject before TxState creation with error 429 and zero allocations on the reject path (RED-050) — engine admission order: name/callability (404/403) → rate (503/429) → args (400) → pipeline; conformance test proves gap-free tx ids and exactly the accepted calls in the commit log
- [x] 1.3 Bucket independence + refill: buckets independent per (Identity, reducer); capacity restored after the window; server-to-server identities never rate-limited (RED-051, AUTH-062) — exemption set on the `RateLimiter` (engine auto-exempts its own server identity; assembly adds peers via `with_rate_limiter`)
- [x] 1.4 Shard overload guard: load above `shard_max_reducers_per_sec` receives 503 "shard overloaded" on the excess calls only (RED-052) — global token bucket, default 200,000/s, `0` disables
- [x] 1.5 Verification (DAG exit test): rate-limit conformance tests (10-call burst vs "5/s" = 5 accepted + 5 rejected 429) — `tests/rate_limit.rs` + `ratelimit` unit suite
- [ ] 1.6 Gate G3 input: rate-limit suite green — full workspace suite green locally; CI validation pending (GitHub Actions quota exhausted this month — jobs refuse to start); tick on the first green run after quota/billing recovers (`gh workflow run rust-test.yml` for the full matrix)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (module docs on `reducer::ratelimit`; `max_rate` rustdoc on the `#[fluxum::reducer]` attribute)
- [x] 2.2 Write tests covering the new behavior (6 unit + 4 engine-level integration tests; macro parse tests incl. invalid forms)
- [x] 2.3 Run tests and confirm they pass (full workspace suite green locally; fmt + clippy clean)
