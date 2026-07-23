# Proposal: phase6_seed-fixtures-migrate-plan

## Why
Developers need fixtures to bring an instance to a known state and a safe way to preview a
migration before it mutates data, but neither exists today. The migration machinery is
already present — `crates/fluxum-core/src/migration/` holds the diff computation
(`diff.rs`), the runner (`runner.rs`), the catalog (`catalog.rs`), and context — yet there
is no dry-run entry point and no seed command. The CLI that would host these is an empty
stub (`crates/fluxum-cli/src/main.rs`). Without a `--plan` preview, authors cannot see the
computed schema diff or the auto-apply (safe/additive vs. requires-migration) decision
before it runs, and without `seed` they hand-roll fixtures each time.

## What Changes
Add two CLI commands: `fluxum seed <file>` loads fixture rows/reducer calls into a running
or fresh instance for development and tests; `fluxum migrate --plan` prints the computed
schema diff and the auto-apply decision (safe/additive vs. requires-migration) without
mutating state. This requires exposing the migration diff-planning path so the plan can be
computed read-only, and applying fixtures through reducers on the server side.

## Impact
- Governing spec: docs/specs/SPEC-024-developer-experience-tooling.md
- Related specs: docs/specs/SPEC-011 (schema), Phase 3 schema migration spec
- New PRD requirements: FR-138 (seeding & migrate dry-run)
- Requirements covered: DEV-040, DEV-041
- Affected code: crates/fluxum-cli (seed, migrate --plan subcommands),
  crates/fluxum-core/src/migration/ (expose diff planning read-only via diff.rs/runner.rs),
  crates/fluxum-server (apply fixtures via reducers)
- Depends on: phase3 schema migration (archived), phase6 CLI
- Breaking change: NO
- User benefit: developers seed reproducible fixtures with one command and preview exactly
  what a migration will do — and whether it auto-applies — before touching any state.
