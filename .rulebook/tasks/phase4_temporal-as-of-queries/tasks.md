## 1. Implementation
- [ ] 1.1 Retain superseded row versions in a bounded temporal window, tagged by committing tx_id/timestamp (RV-020; crates/fluxum-core/src/store/committed.rs)
- [ ] 1.2 Bound retention by a configurable budget / checkpoint horizon and prune versions past the window (RV-020; crates/fluxum-core/src/store/memstore.rs)
- [ ] 1.3 Add an AS OF snapshot read that resolves a tx_id or timestamp to the committed state at that point (RV-021; crates/fluxum-core/src/store/committed.rs)
- [ ] 1.4 Parse `AS OF (tx_id | timestamp)` and thread it into the query plan (RV-021; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.5 Accept AS OF on OneOffQuery and the `/query` admin endpoint (RV-021; crates/fluxum-server/src/admin.rs)
- [ ] 1.6 Return a typed error when the requested point is older than the retained window (RV-021; crates/fluxum-core/src/store/committed.rs)
- [ ] 1.7 Apply RLS and column masking to AS OF reads exactly as for live reads (RV-022; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.8 Reuse the AS OF snapshot for subscription initial-data reads at an offset (RV-021; crates/fluxum-core/src/subscription/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
