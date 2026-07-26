# SPEC-027 — Analytics Interop (read-only Postgres wire)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 7 (post-launch interop) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-44, FR-81 (extends); new: FR-148 (read-only pgwire endpoint) |
| **Requirement prefix** | `PGW-` |
| **Source** | New (Fluxum-native). The gaps analysis lists a Postgres-wire front-end as a roadmap candidate to unlock the BI ecosystem (psql, Grafana, Metabase, Superset) without building bespoke connectors. Read-only by design — Fluxum stays a realtime DB, not an OLAP engine. |

Keywords are RFC 2119. Requirement IDs `PGW-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

A **read-only** Postgres wire-protocol endpoint that lets standard SQL/BI tools connect to Fluxum and
run point-in-time reads over the existing query surface. It reuses the compiled SQL engine and
`/query` semantics; it does not add write paths, transactions, or the full Postgres SQL dialect.

## 2. Requirements (`PGW-0xx`)

### Requirement: Postgres wire endpoint
- **PGW-001** [P2] The server SHALL expose an optional Postgres wire-protocol listener that authenticates,
  answers `SELECT` over exposed tables/views using the existing compiled query + planner (SPEC-018), and
  streams result rows in the Postgres format.
- **PGW-002** [P2] The endpoint is **read-only**: `INSERT`/`UPDATE`/`DELETE`/DDL and multi-statement
  transactions MUST be rejected with a clear error; all mutation goes through reducers.
- **PGW-003** [P2] Schema introspection (`information_schema`/catalog subset) SHALL reflect Fluxum tables,
  views, and column types so BI tools can discover the schema.
- **PGW-004** [P2] Reads MUST honor RLS, column masking (SPEC-017), and per-connection auth identity; the
  endpoint is disabled by default and gated behind auth + config.
- **PGW-005** [P2] `AS OF` (SPEC-022 RV-02x) MAY be surfaced so BI snapshots are point-in-time consistent.

#### Scenario: Connect Metabase
Given the pgwire endpoint enabled with a read-only analytics identity
When Metabase connects and lists tables then runs a filtered `SELECT ... ORDER BY ... LIMIT`
Then it discovers the schema and receives rows served via the index-aware planner, and any attempted
`INSERT` is rejected.

## 3. Non-goals

- Write access, transactions, stored procedures over pgwire (reducers own writes).
- Full Postgres SQL dialect / JOINs / aggregates beyond the Fluxum query surface + materialized views.
- Being an OLAP/warehouse engine (export to dedicated analytics tools for heavy analytics).

## 4. Implementation

Landed in `crates/fluxum-server/src/pgwire/` — a hand-rolled Postgres v3 codec
(`proto.rs`, independent of the FluxRPC `FrameCodec`) plus the listener and per-connection
state machine (`mod.rs`), gated by the `pgwire` config block (`enabled: false` default;
loopback host default because the wire is plaintext — SSLRequest is declined, so a remote
deployment fronts it with a TLS proxy).

- **PGW-001.** Startup + cleartext-token auth: the connection password is a Fluxum token,
  resolved by the [`Authenticator`](SPEC-009-authentication.md) into the per-connection
  identity. Both the **simple** (`Q`) and **extended** (`Parse`/`Bind`/`Describe`/`Execute`/
  `Sync`, zero-parameter) query flows are served. Each `SELECT` runs through the *same*
  `SubscriptionManager::query_json` engine as admin `POST /query` (SPEC-018 index-aware
  planner), and the JSON result is translated to `RowDescription`/`DataRow` in text format
  with a `FluxType`→OID map.
- **PGW-002.** `INSERT`/`UPDATE`/`DELETE`/DDL are rejected with SQLSTATE `25006`; the Fluxum
  SQL grammar only parses `SELECT` anyway. Transaction-control (`BEGIN`/`COMMIT`/`ROLLBACK`)
  and session `SET`/`RESET`/`DISCARD` are accepted as harmless no-ops so tools connect — they
  enable nothing, since no write path exists.
- **PGW-003.** `information_schema.tables`/`.columns` (plus `version()`, `current_schema()`,
  `SHOW`) are reflected from the live `Schema`; views appear as relations (name only). Full
  `pg_catalog` emulation is out of scope (a non-goal).
- **PGW-004.** RLS (SPEC-005) and column masking (SPEC-017) apply because a non-peer token
  yields a `Subscriber::client_with_roles`; a server-peer token bypasses RLS (AUTH-062). The
  listener is disabled by default and its default bind is loopback.
- **PGW-005.** `AS OF` rides through `sql::as_of_point` → `MemStore::snapshot_as_of`, so a BI
  read is point-in-time consistent.

Wire behavior is covered by `crates/fluxum-server/tests/pgwire_e2e.rs` (a real socket
client: handshake, valid/invalid token, streamed `SELECT`, `information_schema` discovery,
read-only rejection, no-op `SET`/`BEGIN`, and the extended-query flow) and the `pgwire::proto`
unit tests.
