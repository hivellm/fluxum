## 0. v1 — data-first console (user-directed scope, 2026-07-27)

The user asked for a deliberately simple first cut before the 8-area vision:
a phpMyAdmin-style data tool — browse tables, run queries, view/edit/delete
rows — localhost-only, modern look modelled on the Vectorizer GUI, with the
minimum possible setup. Constraints honoured: assets stay embedded in the
binary (zero setup — open `/console`; the self-containment test forbids CDN
fetches, and embedded is strictly less setup than a CDN script anyway).
Table *creation* cannot happen at runtime — Fluxum tables are compiled in
via `#[fluxum::table]` (link-time registry, by design) — so the console gets
a table **designer that generates the Rust macro snippet** to paste into the
module, stated honestly in the UI.

- [x] 0.1 Admin row-write endpoint: `POST /rows` (`op: upsert | delete`,
      whole row as JSON) — schema-directed JSON→RowValue conversion
      accepting exactly the shapes `row_value_to_json` renders (hex
      bytes/identity, micros timestamps, stringified 64-bit values),
      committed through the shard's own `TxPipeline` with
      `CommitMeta { caller: admin_identity, reducer_name: "__console.row_edit" }`
      so constraints (unique/check/FK), subscriptions, the commit log and
      the audit trail all see it like any commit; multi-shard routes via
      `shard_of_row`; in `is_mutating_route` for the SEC-054 audit.
      Structured column types (enum/struct/blob/crdt) are refused with a
      pointer at reducers — the console must not guess their invariants.
- [x] 0.2 Console UI rework in the Vectorizer GUI's visual language —
      the exact oklch achromatic palette, sidebar shell (brand / tables
      list / bottom nav), 56px topbar with the health cluster, hairline
      borders with depth by lightness, blue accent, styled scrollbars,
      toast notifications — still one self-contained HTML file; the CSP
      self-containment test stays green (no CDN, by test).
- [x] 0.3 Row editing UX: click a row → modal editor with typed inputs
      (checkbox for Bool, null toggle for Option, JSON array for List,
      PK/auto/opt badges, auto-inc hint on new rows), + New row, two-step
      delete confirm; saves refresh the grid and ride the watch stream.
      Tables with structured columns fall back to read-only with a notice.
- [x] 0.4 Table designer: visual column builder (name/type/pk/auto/Option,
      access, partition_by) emitting the `#[fluxum::table]` struct snippet
      live with a copy button, under an explicit notice that tables are
      compiled into the module — no runtime CREATE TABLE exists to fake.
- [x] 0.5 Tests: `/rows` integration coverage (insert, edit-in-place,
      delete, SQL-visible commits, missing-row delete as 400, unknown
      table 404, type mismatch and missing column naming the field, bad
      op refused); console self-containment test green. Verified against
      the live release binary end to end: curl upsert/query round-trip,
      then a real browser session (Playwright) — table browse, row editor
      save (title + bool committed), and the designer rendering. The
      automated browser e2e harness stays with item 1.8.

## 1. Implementation (full vision — later increments)
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
