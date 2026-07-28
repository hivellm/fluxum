# SPEC-024 — Developer Experience & Tooling

| | |
|---|---|
| **Status** | Shipped. DEV-01x (dev inner loop) with T6/FR-135; DEV-02x (reducer test kit) as [`fluxum-testkit`](../../crates/fluxum-testkit/); DEV-04x — `fluxum seed` (ordered reducer-call fixtures over the admin surface) and `fluxum migrate --plan` (read-only diff + auto-apply verdict via `fluxum_core::migration::plan`, reached through `FLUXUM_MIGRATE_PLAN=1` / `fluxum-server --migrate-plan`); DEV-03x — the built-in console ([`fluxum-server/src/console.rs`](../../crates/fluxum-server/src/console.rs)): `GET /console` serves a self-contained shell (inline CSS/JS, CSP forbids non-self origins) with table browser, read-only query panel, live diff viewer over `GET /console/watch` (NDJSON on the commit broadcast, `?table=` filter), `/metrics` + `/schema` views, and a log tab riding `GET /logs?follow=1` that highlights `slow_reducer` warnings (DEV-032). DEV-031: the SEC-054 network guard covers every console route, and outside the `development` profile the watch stream refuses anonymous callers — a server-peer operator token is required even from loopback; the watch path reads only the commit broadcast and the static table catalog, never a storage lock. |
| **Phase / tasks** | Phase 6 (CLI, console, test kit, seeding) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-04, FR-81, FR-82 (extends); new: FR-135 (dev inner-loop CLI), FR-136 (reducer test kit), FR-137 (admin web console), FR-138 (seeding & migrate dry-run) |
| **Requirement prefix** | `DEV-` |
| **Source** | New (Fluxum-native). The gaps analysis names the `spacetime dev` inner-loop "the single highest-leverage DX asset"; Fluxum's CLI is an empty stub ([fluxum-cli](../../crates/fluxum-cli/)) and there is no admin UI. |

Keywords are RFC 2119. Requirement IDs `DEV-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

The developer inner loop and operability surface: a `fluxum dev` watch/rebuild/reload/log-stream
command with `fluxum init` scaffolding and `fluxum logs -f`; a **reducer test/simulation kit** that
exposes the internal deterministic simulation ([fluxum-dst](../../crates/fluxum-dst/)) to module
authors; a **built-in admin web console** (data browser + live query + metrics) served by the
existing admin endpoints; and **data seeding + migration dry-run**.

## 2. Dev inner loop (`DEV-01x`)

### Requirement: Watch-rebuild-reload-logs
- **DEV-010** [P1] `fluxum dev` SHALL watch the module crate, rebuild on change, restart the server
  (snapshot + log replay), regenerate SDK bindings, and stream merged module+system logs.
- **DEV-011** [P1] `fluxum init [--template]` SHALL scaffold a runnable module (schema + reducers +
  config + a client) that boots with one command.
- **DEV-012** [P1] `fluxum logs -f` SHALL stream structured logs (module + system) over the admin
  transport with level/format filters.

#### Scenario: Edit-save-see
Given `fluxum dev` is running a module
When the developer edits a reducer and saves
Then the server rebuilds and restarts, bindings regenerate, and the next client call hits the new code
without any manual step.

## 3. Reducer test / simulation kit (`DEV-02x`)

### Requirement: Deterministic module testing
- **DEV-020** [P1] A `fluxum-testkit` crate SHALL let module authors drive reducers against an in-process
  shard with a seeded clock/RNG, assert on resulting rows and emitted diffs, and replay a recorded
  transaction sequence deterministically.
- **DEV-021** [P2] The kit SHALL support fault injection (mid-commit crash, torn tail) reusing the DST
  harness so authors can test recovery-affecting logic.

#### Scenario: Unit-test a reducer
Given a test using `fluxum-testkit`
When it calls `complete_task` with a non-owner identity
Then the test asserts the call returns `Err` and the task row is unchanged.

## 4. Admin web console (`DEV-03x`)

### Requirement: Built-in data browser & live viewer
- **DEV-030** [P1] The server SHALL serve a self-contained web console on the HTTP admin port: browse
  tables, run `/query` (read-only), watch a subscription's live diffs, and view `/metrics` and `/schema`.
- **DEV-031** [P1] The console MUST honor auth (no anonymous access outside the `development` profile)
  and take no storage locks that violate the `/health` latency budget.
- **DEV-032** [P2] The console SHALL show reducer invocation logs and slow-reducer warnings.

#### Scenario: Watch a table live
Given the console open on the `OnlineUser` table
When users connect and disconnect
Then the console shows rows appear and vanish in real time via a live subscription.

### Requirement: Integrated admin dashboard (phase8)
- **DEV-033** [P1] The console SHALL grow into the integrated admin dashboard: an operator
  **overview** (health, shard/replication posture, memory-budget occupancy, hardware probe with
  provenance, storage paths, per-table rows), a **data explorer** (SQL console with `EXPLAIN`
  via `POST /query/explain`, keyset pagination per QP-040, a TxUpdate-driven live mode riding
  `/console/watch`, and a read-only row inspector), a **reducer console** (signature-driven typed
  argument forms from `/schema`, `POST /reducer/:name` invoke, rate display, and an OPS-020
  audit panel), a **sessions view** (the SEC-053 directory with per-session subscriptions and
  outbound-queue occupancy, kick, and SEC-033 ban management), an **ops view** (reloadable
  config + `POST /config/reload`, checkpoint, drain, backup create/verify, replication posture,
  per-tenant usage), and **observability** (sampled metric tiles from `/metrics`, `/logs` tail
  with a minimum-severity filter, audit filters). See `docs/CONSOLE.md` for the operator guide.
- **DEV-034** [P1] Console row edits (`POST /rows`, `op: upsert | delete`) MUST commit through the
  shard's own `TxPipeline` with `CommitMeta { caller: admin identity, reducer_name:
  "__console.row_edit" }` — constraints enforced, subscriptions fanned out, commit logged, audit
  trail attributed — and MUST refuse structured column types (enum/struct/blob/crdt) with a
  pointer at reducers, whose invariants live in module code.
- **DEV-035** [P2] Where a capability was CLI-only, the dashboard adds **additive** JSON
  endpoints rather than widening existing ones: `POST /backup` (REP-060, server-side hot backup),
  `POST /backup/verify` (REP-064), and the enriched `GET /sessions` (per-session subscription
  queries + queue occupancy, SUB-042).
- **DEV-036** [P1] The shell MUST stay one self-contained served page — assembled at compile time
  from `console.html` + `console*.js`, CSP `default-src 'none'` with no external origins — and
  every state-changing route the dashboard drives MUST be in the SEC-054 audit inventory. A
  spawned-binary e2e smoke (`tests/console_e2e_smoke.rs`) SHALL sweep the whole console contract
  against the real server.

#### Scenario: Kick a slow consumer
Given the sessions view showing a client whose outbound queue is near capacity
When the operator clicks Kick and confirms
Then the session is terminated over `DELETE /sessions/{id}`, the action lands in the audit
trail, and a well-behaved SDK reconnects with a fresh session.

## 5. Seeding & migration dry-run (`DEV-04x`)

### Requirement: Fixtures and safe migration preview
- **DEV-040** [P1] `fluxum seed <file>` SHALL load fixture rows/reducer calls into a running or fresh
  instance for development and tests.
- **DEV-041** [P1] `fluxum migrate --plan` SHALL print the computed schema diff and the auto-apply
  decision (safe/additive vs. requires-migration) without mutating state.

#### Scenario: Preview a migration
Given a schema change adding a nullable column and dropping an index
When the developer runs `fluxum migrate --plan`
Then it prints the diff, marks the add as auto-applicable and the drop as needing an explicit migration,
and changes nothing.

## 6. Non-goals

- Hot-swapping module code without a restart (deployment is a fast binary restart, per ARCHITECTURE).
- A hosted cloud dashboard (the console is the single-binary, self-hosted surface).
- Runtime `CREATE TABLE` (tables are compiled in via `#[fluxum::table]`; the console's designer
  emits the Rust snippet instead of pretending otherwise) and console writes to structured
  column types — those invariants live in module code, so those edits go through reducers.
  Scalar row edits are in scope (DEV-034, user-directed in phase8) and commit through the same
  `TxPipeline` as any reducer: constraints, subscriptions, commit log, audit.
