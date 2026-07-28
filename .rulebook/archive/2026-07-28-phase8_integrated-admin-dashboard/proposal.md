# Proposal: phase8_integrated-admin-dashboard

## Why

Fluxum ships as a single binary that already exposes everything an operator
needs — `/health` (effective config + hardware probe), `/metrics`, `/schema`,
`/logs`, the admin surface (audit trail, bans/blocklist, checkpoint trigger,
config hot-reload, drain, EXPLAIN), backup/PITR via the CLI, replication and
namespace/quota management — but each through a different raw endpoint or CLI
invocation. The only visual surface is the minimal `/console`
(`crates/fluxum-server/src/console.{rs,html}`: a 500-line table watcher with
`/console/state` and `/console/watch`). There is no single place where an
operator can *manage the complete database*: browse schema, explore and watch
live data, invoke reducers, watch sessions/subscriptions, drive backups, edit
config, or see replication/shard health. Competing products (SpacetimeDB
dashboard, Convex dashboard, SurrealDB Surrealist) treat this as a core
adoption surface.

## What Changes

Grow `/console` into an integrated admin dashboard — a self-contained SPA
embedded in the server binary (same single-binary philosophy: no external
hosting, no CDN, assets served like `console.html`/`statics.rs` today). The
frontend talks to the database exactly like any client — the browser JS SDK
over Streamable HTTP `/rpc` for live data (subscriptions ARE the refresh
mechanism) — plus the existing JSON admin endpoints for management actions.

Capability areas (each a checklist item):

1. **Overview** — health, version, hardware probe, `memory.budget` /
   buffer-pool occupancy, shard map, replication roles/lag, uptime.
2. **Schema browser** — tables, columns/types, PKs, indexes (btree / spatial /
   full-text / `#[unique]`), foreign keys, visibility rules, migration
   history; rendered from `GET /schema`.
3. **Data explorer** — SQL console with keyset pagination and EXPLAIN
   (planner access-path display); **live mode**: any query result becomes a
   subscription and updates via TxUpdate diffs; row inspector. Writes go
   through reducers only — the explorer never bypasses the reducer model.
4. **Reducer console** — list reducers with signatures from the schema,
   typed argument forms, invoke + result/error display, declared rate limits,
   recent invocations from the audit trail.
5. **Sessions & subscriptions** — connected clients (identity, connection id,
   transport), per-client queue depth / backpressure tier, active
   subscription queries, kick; bans/blocklist management.
6. **Observability** — metrics dashboards rendered from `/metrics`
   (tx rate, fan-out latency, buffer-pool hit rate, RSS vs budget), `/logs`
   tail with level filter, audit-trail viewer with entity/actor filters.
7. **Ops** — config viewer/editor with hot-reload apply + degraded-state
   display, checkpoint trigger, graceful drain, backup create/verify +
   restore-point browser (PITR targets incl. S3 archive), replication
   status/promote, namespaces/tenants + quota editing.
8. **Security** — the dashboard sits behind admin auth (the existing
   `console_unauthenticated` development flag stays for dev mode); every
   management action goes through the audited admin paths so the audit trail
   captures dashboard-driven changes like any API call.

## Impact

- Affected specs: SPEC-012 (observability/admin), SPEC-006 (transport /rpc),
  SPEC-011 (browser SDK reuse), SPEC-025 (ops: backup/drain/checkpoint),
  SPEC-009 (admin auth); likely a new SPEC for the dashboard contract.
- Affected code: `crates/fluxum-server/src/console.rs` (routes),
  `console.html` → SPA assets under `crates/fluxum-server/assets/console/`
  (embedded via the `statics.rs` include machinery), possibly small additive
  JSON admin endpoints where a capability has CLI-only coverage today
  (backup/restore points, session list).
- Depends on: browser JS SDK (shipped), admin endpoints (shipped),
  audit trail (shipped), namespaces/quotas (shipped).
- Breaking change: NO — additive; `/console` URLs stay.
- Risk: MEDIUM — large frontend surface, but read paths dominate; every
  mutating action reuses existing audited admin endpoints (no new write
  authority is invented).
- User benefit: one screen to operate the entire database — the adoption
  surface every comparable product treats as table stakes, powered by
  Fluxum's own differentiator (live subscriptions make the dashboard itself
  realtime, no polling).

## Notes

Frontend build: keep the zero-external-dependency stance of the demo/browser
SDK (esbuild bundle checked in like `demo/fluxum.min.js`, or a build step in
the repo — decide in 1.1). No JS framework requirement is imposed by this
proposal; the constraints are: assets embedded in the binary, total size in
the hundreds of KB at most, no runtime CDN fetches (CSP-friendly, air-gapped
deploys work).
