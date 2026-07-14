## 1. Implementation
- [ ] 1.1 Parse `max_rate = "N/s"` on `#[fluxum::reducer]` and build token buckets per (Identity, reducer) (FR-24)
- [ ] 1.2 Reject before TxState creation with error 429 and zero allocations on the reject path (RED-050)
- [ ] 1.3 Bucket independence + refill: buckets independent per (Identity, reducer); capacity restored after the window; server-to-server identities never rate-limited (RED-051, AUTH-062)
- [ ] 1.4 Shard overload guard: load above `shard_max_reducers_per_sec` receives 503 "shard overloaded" on the excess calls only (RED-052)
- [ ] 1.5 Verification (DAG exit test): rate-limit conformance tests (10-call burst vs "5/s" = 5 accepted + 5 rejected 429)
- [ ] 1.6 Gate G3 input: rate-limit suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
