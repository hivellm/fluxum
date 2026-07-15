## 1. Implementation

- [ ] 1.1 Add an optional Postgres wire-protocol listener that completes the startup/auth handshake and is disabled by default, gated behind auth + config (PGW-001, PGW-004; crates/fluxum-server)
- [ ] 1.2 Parse incoming simple/extended-query `SELECT` and route it through the existing `compile()` → `CompiledPlan` engine (PGW-001; crates/fluxum-core/src/sql/mod.rs, crates/fluxum-server)
- [ ] 1.3 Execute reads via the index-aware planner (SPEC-018) reusing the `/query` read semantics (`query_json`) and stream result rows in the Postgres RowDescription/DataRow format (PGW-001; crates/fluxum-server/src/admin.rs, crates/fluxum-server)
- [ ] 1.4 Enforce read-only: reject `INSERT`/`UPDATE`/`DELETE`/DDL and multi-statement transactions with a clear Postgres error response (PGW-002; crates/fluxum-server)
- [ ] 1.5 Expose an `information_schema`/catalog subset reflecting Fluxum tables, views, and column types for BI schema discovery (PGW-003; crates/fluxum-server, crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.6 Apply per-connection auth identity plus RLS and column masking to every read served over the wire (PGW-004; crates/fluxum-core/src/auth, crates/fluxum-server)
- [ ] 1.7 Optionally surface `AS OF` point-in-time snapshots (SPEC-022) so BI reads are point-in-time consistent (PGW-005; crates/fluxum-server, crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.8 Wire configuration + feature gate so the endpoint is off by default and documented as read-only interop (PGW-001, PGW-004; crates/fluxum-server)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
