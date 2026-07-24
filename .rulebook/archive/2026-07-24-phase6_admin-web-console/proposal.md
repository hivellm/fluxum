# Proposal: phase6_admin-web-console

## Why
Convex and SpacetimeDB ship dashboards, but Fluxum only exposes raw JSON admin endpoints
today — `crates/fluxum-server/src/admin.rs` serves `/health`, `/metrics`, `/schema`,
`/query`, and `/view/:name` with no human-facing surface. A built-in data browser plus a
live subscription viewer is a large DX win, and because Fluxum is a single binary it can
serve the console itself with no extra deployment. The commit broadcast already exists
(`ShardContext::publish_commit` / `subscribe_commits` in `crates/fluxum-server/src/lib.rs`),
so a live diff viewer can ride the existing transport. The `/health` path is contractually
lock-free (`RPC-053`, `< 50 ms`), so the console must add no storage locks that violate it.

## What Changes
Serve a self-contained web console on the HTTP admin port: browse tables, run `/query`
(read-only), watch a subscription's live diffs, and view `/metrics` and `/schema`. The
console honors auth (no anonymous access outside the `development` profile), takes no
storage locks that break the `/health` latency budget, and shows reducer invocation logs
plus slow-reducer warnings.

## Impact
- Governing spec: docs/specs/SPEC-024-developer-experience-tooling.md
- Related specs: docs/specs/SPEC-011 (schema), Phase 5 transports/admin specs
- New PRD requirements: FR-137 (admin web console)
- Requirements covered: DEV-030, DEV-031, DEV-032
- Affected code: crates/fluxum-server/src/admin.rs (serve console + existing endpoints), a
  subscription viewer over the existing commit broadcast transport
  (ShardContext::subscribe_commits in lib.rs), self-contained static assets
- Depends on: phase5 transports + admin (archived), phase6
- Breaking change: NO
- User benefit: developers browse data, run read-only queries, and watch live diffs and
  reducer logs from a single-binary built-in console with no separate dashboard to deploy.
