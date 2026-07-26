# Proposal: phase7_pgwire-readonly-endpoint

## Why

Fluxum already compiles its SQL subset into a `CompiledPlan` exactly once
(`crates/fluxum-core/src/sql/mod.rs:94` / `compile()` at `:150`) and serves one-off
read-only SQL as JSON through the admin `POST /query` path
(`crates/fluxum-server/src/admin.rs:229`, `query_json` at `:245`). That surface is only
reachable over the bespoke HTTP/JSON admin API, so standard BI/SQL tools (psql, Grafana,
Metabase, Superset) cannot connect without a custom connector. The gaps analysis lists a
Postgres-wire front-end as a roadmap candidate to unlock that ecosystem. The gap: no
wire-protocol listener speaks the Postgres frontend/backend protocol on top of the existing
compiled query + index-aware planner (SPEC-018), and there is no catalog/`information_schema`
reflection for tool discovery. Read-only by design so Fluxum stays a realtime DB, not an OLAP
engine — all mutation continues to flow through reducers.

## What Changes

Add an optional Postgres wire-protocol listener that authenticates a connection, answers
`SELECT` over exposed tables/views by reusing the compiled query + planner (SPEC-018), and
streams result rows in the Postgres wire format. The endpoint is strictly read-only:
`INSERT`/`UPDATE`/`DELETE`/DDL and multi-statement transactions are rejected with a clear
error. An `information_schema`/catalog subset reflects Fluxum tables, views, and column types
for BI discovery. Reads honor RLS (SPEC-005), column masking (SPEC-017 CT-041), and the
per-connection auth identity (`crates/fluxum-core/src/auth`). The listener is disabled by
default and gated behind auth + config. `AS OF` point-in-time snapshots (SPEC-022) MAY be
surfaced for consistent BI reads.

## Impact

- **Governing spec:** docs/specs/SPEC-027-analytics-interop.md
- **Related specs:** SPEC-018 (query-planner), SPEC-022 (reactive-views / AS OF), SPEC-017 (column-transforms), SPEC-005 (subscriptions / RLS)
- **New PRD requirements:** FR-148
- **Requirements covered:** PGW-001, PGW-002, PGW-003, PGW-004, PGW-005
- **Affected code:** new Postgres wire listener in `crates/fluxum-server`; `crates/fluxum-core/src/sql/mod.rs` (reuse `compile`/`CompiledPlan`); `crates/fluxum-server/src/admin.rs` read path (`query_json`) as the shared read semantics; catalog/`information_schema` introspection over `Schema`; auth in `crates/fluxum-core/src/auth`
- **Depends on:** phase4 SQL compiler + index-aware query planner (SPEC-018); phase4_temporal-as-of-queries (SPEC-022, optional — only for PGW-005)
- **Breaking change:** NO
- **User benefit:** connect psql, Grafana, Metabase, and Superset directly to Fluxum for point-in-time analytics with no bespoke connector, while keeping the database realtime and mutation-safe (reads honor RLS/masking; writes stay in reducers).
