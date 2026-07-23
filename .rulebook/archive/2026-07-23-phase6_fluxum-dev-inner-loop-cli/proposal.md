# Proposal: phase6_fluxum-dev-inner-loop-cli

## Why
The gaps analysis names the `spacetime dev` inner loop "the single highest-leverage DX
asset", yet Fluxum's CLI is an empty ~8-line stub (`crates/fluxum-cli/src/main.rs` only
prints "bootstrap skeleton, subcommands land per docs/DAG.md") and the server binary is
likewise a stub (`crates/fluxum-server/src/main.rs` prints "transports land in Phase 5").
There is no way for a module author to edit a reducer, rebuild, and see the change without
manual, multi-step ceremony. The edit → rebuild → reload → logs loop makes or breaks
adoption, so `fluxum dev` / `fluxum init` / `fluxum logs -f` are the first CLI surface to
land. The server already exposes structured tracing logs and admin JSON endpoints
(`crates/fluxum-server/src/admin.rs`: `/health`, `/metrics`, `/schema`, `/query`,
`/view/:name`) and assembles a shard via `ShardContext::new` / `ShardContext::with_views`
(`crates/fluxum-server/src/lib.rs`), which the reload path drives.

## What Changes
Implement the developer inner loop in `crates/fluxum-cli`: `fluxum dev` watches the module
crate, rebuilds on change, restarts the server via snapshot + log replay, regenerates SDK
bindings, and streams merged module + system logs; `fluxum init [--template]` scaffolds a
runnable module (schema + reducers + config + a client) that boots with one command; and
`fluxum logs -f` streams structured logs over the admin transport with level/format
filters.

## Impact
- Governing spec: docs/specs/SPEC-024-developer-experience-tooling.md
- Related specs: docs/specs/SPEC-011 (schema), Phase 5 transports/admin specs
- New PRD requirements: FR-135 (dev inner-loop CLI)
- Requirements covered: DEV-010, DEV-011, DEV-012
- Affected code: crates/fluxum-cli (all subcommands), crates/fluxum-server (restart hooks
  around ShardContext::new/with_views in lib.rs; snapshot + log replay), SDK codegen
  (bindings regeneration), structured tracing log stream over admin transport
- Depends on: phase6 SDK codegen (T6.1/T6.2)
- Breaking change: NO
- User benefit: one command runs the full edit-save-see loop, removing the biggest
  friction to writing and iterating on Fluxum modules.
