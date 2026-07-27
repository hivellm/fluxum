## 1. Implementation
- [ ] 1.1 Foundation: SPA skeleton embedded in the binary (asset pipeline + `statics.rs` embedding, `/console/*` routing, admin-auth gate with the `console_unauthenticated` dev escape), connected via the browser JS SDK over `/rpc`
- [ ] 1.2 Overview + Schema browser: health/hardware/budget/shard/replication panels; full schema rendering from `GET /schema` (tables, indexes incl. spatial/fulltext/unique, FKs, visibility, migrations)
- [ ] 1.3 Data explorer: SQL console with keyset pagination + EXPLAIN display; live mode (query → subscription → TxUpdate-driven updates); row inspector
- [ ] 1.4 Reducer console: signature-driven typed argument forms, invoke/result, rate-limit display, recent invocations from the audit trail
- [ ] 1.5 Sessions & subscriptions: connected-client list with queue/backpressure state, active queries, kick, bans/blocklist management (additive JSON endpoints where today CLI-only)
- [ ] 1.6 Observability: metrics dashboards parsed from `/metrics`, `/logs` tail, audit-trail viewer with filters
- [ ] 1.7 Ops: config view/edit + hot-reload apply, checkpoint trigger, drain, backup create/verify + PITR restore-point browser (incl. S3 archive), replication status/promote, namespaces + quotas
- [ ] 1.8 Hardening: every mutating action audited; CSP with no external origins; dashboard e2e smoke against a live server (spawned like the SDK conformance runners)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation (dashboard guide in docs/, SPEC addition/update for the console contract)
- [ ] 2.2 Write tests covering the new behavior (route/auth tests, state/watch endpoints, e2e smoke)
- [ ] 2.3 Run tests and confirm they pass
