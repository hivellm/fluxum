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
- [x] 1.2 Overview + Schema browser (2026-07-27, in the one-file shell — the
      v1 approach continues; 1.1's separate SPA/asset pipeline only becomes
      worthwhile if the file outgrows the 1500-line convention). Overview is
      the landing view: stat tiles (status/uptime/conns/tx/queue/TLS) and
      panels for memory (budget + buffer-pool occupancy bar from `/metrics`,
      memstore estimate, reclaim backlog), shard + replication (role/epoch/
      lag/stale or standalone), hardware probe + every derived value with its
      provenance badge (auto/config/env, HWA-013), storage paths, and
      per-table row counts; rides the existing 5 s health poll. Schema view
      is now a rendered browser: per-table cards (access/partition/visibility
      badges; columns with pk/auto/unique/transform flags; index footer incl.
      spatial/fulltext detail), reducer signatures with callable/rate, views
      — with a Raw JSON toggle. FKs and migration history are not in the
      `/schema` document today; rendered when the document carries them.
      Verified in a real browser against the release binary (Playwright);
      shell view-inventory pinned by a unit test.
- [x] 1.3 Data explorer (2026-07-27). The Query view grew into the SQL
      console: **Explain** panel from `POST /query/explain` (QP-051 —
      access path with index columns/probes/bounds or full_scan, residual
      filter, order served-by-index vs sorted-at-execution, limit, cursor,
      normalized SQL); **keyset pagination** — Next page builds
      `… AFTER (order value, pk value)` from the last row with
      schema-typed SQL literals, enabled on a full page + ORDER BY +
      single-column pk (QP-040/041; a non-indexed order column surfaces
      the server's QP-040 refusal verbatim — the demo module has no
      secondary index, verified); **live mode** — Go live re-executes the
      armed query on every commit touching its table via the
      `/console/watch` stream (TxUpdate-driven, debounced 250 ms, dropped
      markers also re-run; DEV-031 lock discipline — no /rpc SDK bundle
      embedded, the one-file stance holds); **row inspector** — click a
      result row for a read-only typed field view (edits stay in the Data
      view through POST /rows). The shell outgrew the 1500-line
      convention and is now assembled at compile time from console.html
      (markup+styles) + console.js (script) via `concat!`, pinned by a
      unit test; self-containment unchanged. All verified in a real
      browser against the release binary (Playwright): explain render,
      cursor SQL construction + refusal, live grid refresh on a curl'd
      commit, inspector fields.
- [x] 1.4 Reducer console (2026-07-27). New Reducers view: list from the
      `/schema` reducer descriptors with rate badges (`open` / `N per s` /
      `sched`); selecting one renders a **signature-driven typed form**
      (Rust source types SDK-001 — ints validated as safe integers, floats,
      bool checkbox, String, `Vec<T>` as JSON array, `Option<T>` with a
      null toggle, unknown module types accept JSON-or-string) and invokes
      via `POST /reducer/:name` (RPC-051 argument array; F-004
      schedule-only reducers render disabled). Result: committed toast or
      the error envelope verbatim, plus a session-local invocation history
      (latest first, capped 20). **Audit panel**: recent commits for a
      chosen table from `POST /audit` (OPS-020) — tx, time, caller,
      reducer, insert/update/delete — token-aware: without a server-peer
      operator token it states the OPS-021 requirement instead of showing
      empty. Verified end to end in a real browser (Playwright) against
      the release binary with a configured `auth.server_peers` peer:
      send_chat(7, "…") invoked from the form, row confirmed by SQL, and
      the same invocation surfaced in the audit grid via the peer token.
- [x] 1.5 Sessions & subscriptions (2026-07-27). **Additive server surface**:
      `GET /sessions` now attaches to each session its live subscription
      queries (`query_id` + the plan's normalized SQL, via a new
      `SubscriptionManager::subscriptions_by_connection`) and its
      outbound-queue occupancy (`queued`/`capacity` from the ConnHandle
      sink — SUB-042: near-full is a slow consumer about to be dropped);
      gathered across every shard host (the `/metrics` pattern; namespace
      sessions remain out, as the console serves the default database).
      **Sessions view**: connected-client grid (session/identity prefixes,
      connection, age, bound IP, queue occupancy, query count), row click →
      typed detail incl. every subscription SQL, and a two-step **Kick**
      (`DELETE /sessions/{id}`). **Bans panel**: `GET /bans` static +
      runtime with remaining TTL, ban form (`POST /bans`, entry + optional
      ttl), Unban per runtime row (`DELETE /bans/{entry}`, raw CIDR `/`
      rides path-rejoin). Integration test: authenticate + SubscribeSingle
      over real loopback HTTP, then assert the listing carries the
      normalized SQL and queue fields. Browser e2e (Playwright) with the
      served demo app as a live client: session appeared with 3
      subscriptions + 0/1024 queue, detail modal, CIDR ban/unban round-trip,
      and Kick terminated the session — the SDK's auto-reconnect then
      minted a fresh one, proving both sides.
- [x] 1.6 Observability (2026-07-27). **Metrics dashboard**: the Metrics
      view defaults to stat tiles sampled from `/metrics` every 5 s while
      visible (60-sample history): tx rate, connections, subscriptions,
      queue depth, fan-out msgs/s, avg fan-out latency (delta sum/count of
      the stage histogram), buffer-pool hit rate (delta hits/(hits+misses)),
      pool occupancy gauge, memstore — each a single-series 2px accent
      sparkline (no legend/grid — the tile title names it, per the dataviz
      method; SVG born via innerHTML so no namespace URL breaks the
      self-containment test). The raw series grid stays behind a toggle.
      **Logs**: minimum-severity filter (ERROR/WARN+/INFO+/DEBUG+) beside
      the text and reducer-only filters. **Audit viewer**: pk (composite via
      comma-separated values), tx_from/tx_to range filters added to the
      OPS-020 panel. The shell's script crossed 1500 lines and now
      assembles from console.js + console_ops.js. Browser-verified against
      the release binary (Playwright): sparklines carried a live traffic
      burst (tx-rate hump, hit-rate curve, memstore climb), series toggle,
      ERROR-min filter muted INFO traffic then (all) showed it, and
      tx_from=36 trimmed the Task trail to exactly tx 36-40 minus the
      ChatMessage commit.
- [x] 1.7 Ops (2026-07-27). **Additive server surface**: `POST /backup`
      `{out}` hot-backs-up every shard to a server-side directory
      (REP-060 — source dirs from the installed config via a new
      `ShardContext::backup_source`; file copy on the blocking pool; in
      `is_mutating_route` for the SEC-054 audit) and `POST /backup/verify`
      `{dir}` re-hashes a backup against its manifest (REP-064,
      read-only). **Ops view**: Config panel (reloadable values with
      provenance + Reload config → changed keys or the frozen-key refusal,
      OPS-040/041; editing stays in the file — the reload IS the apply);
      Maintenance (Checkpoint now → fresh/covered + tx; two-step Drain →
      state display, OPS-030); Replication posture (role/epoch/replicas/
      lag/stale or standalone + archive-segments-pending — promote rides
      the election/CLI, no HTTP endpoint exists to call); Backup panel
      (create + verify with report cards; restore/PITR stated as
      CLI-offline, never against a live server); Namespaces & quotas from
      the per-tenant metric series (OPS-051/061), with the honest
      empty-state. Deferred within this area: an S3-archive restore-point
      browser (needs a core archive-catalog API that does not exist yet).
      Integration test: create → verify-clean → tamper → verify names the
      file, all over loopback HTTP. Browser e2e (Playwright): reload
      ("nothing changed"), checkpoint at tx 41, backup+verify round-trip
      from the UI (2 files OK), then drain — state shutting_down and /rpc
      refusing 503 while the admin surface kept answering.
- [x] 1.8 Hardening (2026-07-27). **Audit**: a unit test pins the SEC-054
      `is_mutating_route` inventory — every state-changing route the
      dashboard drives (reducer, rows, drain, backup, checkpoint,
      config/reload, plugin toggles, bans, session kills) asserts audited,
      every read asserts not; a new mutating route that forgets the list
      fails the test, not a security review. **CSP**: already pinned at
      build time (no absolute URLs in the shell) and at runtime
      (`default-src 'none'` header + meta), re-asserted by the smoke.
      **E2e smoke** (`tests/console_e2e_smoke.rs`): spawns the REAL
      `fluxum-server` binary (the conformance-runner pattern, off-default
      ports, skip-loudly-if-unbuilt) and sweeps the whole console contract
      over plain HTTP — shell+CSP+complete document, boot state, schema,
      row edit → SQL round-trip, explain, reducer invoke, metrics,
      sessions, ban/unban, checkpoint, backup create+verify, then drain
      asserting /rpc 503 AND /health answering 503 (the load-balancer
      leaves-rotation contract). Runs in ~0.6 s.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Docs (2026-07-27): `docs/CONSOLE.md` — the operator guide (every
      view, auth posture, security model, known limits stated where they
      bite). SPEC-024 updated additively: DEV-033..036 specify the
      dashboard capability areas, the `POST /rows` commit contract, the
      additive ops endpoints, and the self-containment/audit/e2e-smoke
      hardening; the "no row edits" non-goal rewritten to what phase8
      actually shipped (user-directed scalar edits through the TxPipeline;
      runtime CREATE TABLE and structured-column writes stay out).
- [x] 2.2 Tests: unit (route resolution, shell assembly/self-containment/
      view inventory, SEC-054 audit inventory), integration (console
      routes+auth+watch, /rows, sessions listing with subscriptions+queue,
      backup create/verify/tamper over HTTP), and the spawned-binary e2e
      smoke sweeping the full console contract. Browser-level flows
      verified per increment against the release binary (Playwright).
- [x] 2.3 Full fluxum-core + fluxum-server suites green (0 failing
      suites); fmt + clippy --all-features + codespell clean. **Coverage
      floor recovered: 90.01% lines** (2026-07-28, gate command of
      record, PG + STDB drivers live) after dipping to 89.49% under the
      phase8 surface. Recovery was behavior suites, never padding:
      `/rows` converter 41→93.5%, admin dispatch arms
      (`tests/admin_arms.rs` — audit pk coercion across every key type,
      ops refusals, reducer policing, health lifecycle, FTS schema
      render), demo reducers (`tests/demo_reducers.rs` — move_player
      spawn/move/clamp + validations), the full metrics exposition
      (112→3 missed), config validate() refusals (63→5), and the CLI
      dispatch arms (95→~45). Standing recorded in docs/COVERAGE.md.
