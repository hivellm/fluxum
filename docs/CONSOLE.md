# The Fluxum admin console

The console is the single-binary admin dashboard (SPEC-024 DEV-030..036): one
self-contained page served by the server itself at **`http://<host>:15800/console`**.
Nothing to install, no CDN, works air-gapped — the page's CSP is
`default-src 'none'`, so it cannot fetch from any external origin even if asked.

## Opening it

| Profile | What you see |
|---|---|
| `development` | The console opens directly (DEV-031 dev escape). |
| anything else | A login card: paste a **server-peer operator token** (`auth.server_peers` in the config). The token is kept in `sessionStorage` only. |

The data routes are additionally gated by the SEC-054 admin network policy —
loopback always passes; remote IPs must be in `server.admin.trusted`.

## Views

- **Overview** — health status, uptime, connections, last tx, queue depth, TLS
  posture; memory budget and live buffer-pool occupancy; shard + replication
  posture; the hardware probe with every derived value's provenance
  (`auto`/`config`/`env`, HWA-013); resolved storage paths; per-table row
  counts. Refreshes on the 5 s health poll.
- **Data** — the phpMyAdmin-style grid: pick a table, browse rows, click one to
  edit in a typed modal (checkbox for `Bool`, null toggle for `Option`, JSON
  array for `List`), add rows, two-step delete. Edits go through
  `POST /rows` and commit through the shard's own `TxPipeline` — constraints,
  subscriptions, commit log and audit trail all see them (DEV-034). Tables with
  structured columns (enum/struct/blob/crdt) are read-only here: their
  invariants live in module code, so those edits go through reducers.
- **Query** — the SQL console. `Run` executes read-only SQL; `Explain` shows the
  planner's access path (index scan with columns/probes/bounds, or full scan),
  residual filter, whether ORDER BY is served by the index, and the normalized
  SQL (QP-051). `Next page` builds a keyset cursor
  (`… AFTER (order value, pk value)`, QP-040 — needs ORDER BY on an indexed
  column and a single-column pk). `Go live` re-executes the query on every
  commit touching its table, driven by the `/console/watch` stream. Click a
  result row for a read-only typed inspector.
- **Reducers** — every reducer from `/schema` with its declared rate; selecting
  one renders a typed argument form from its signature (SDK-001). Invoke posts
  the RPC-051 argument array; schedule-only reducers are shown disabled
  (F-004). Below: the audit-trail panel (OPS-020) for any table, with pk and
  tx-range filters — it requires a server-peer operator token (OPS-021) and
  says so rather than showing an empty trail.
- **Sessions** — the live HTTP session directory (SEC-053): identity,
  connection, age, bound IP, **outbound-queue occupancy** (a near-full queue is
  a slow consumer about to be dropped, SUB-042) and each session's active
  subscription queries. Kick terminates a session (`DELETE /sessions/{id}`).
  The bans panel manages the SEC-033 runtime blocklist (IP or CIDR, optional
  TTL); static config-file entries are listed but lift only via config+reload.
- **Ops** — day-two actions:
  - *Config* (OPS-040/041): the reloadable values in force with provenance.
    Edit the config **file**, then `Reload config` — the reload is the apply;
    a frozen-key change refuses the whole reload and names the keys.
  - *Maintenance*: `Checkpoint now` (REP-060's `--fresh-checkpoint` path) and
    a two-step `Drain` (OPS-030 — new `/rpc` work gets a retryable 503, and
    `/health` answers **503** so load balancers pull the shard; restart to
    serve again).
  - *Replication*: role/epoch, connected replicas or lag/staleness, archive
    backlog. Promote rides the election/CLI — there is no HTTP promote.
  - *Backup*: `Create` hot-backs-up every shard to a directory on the
    **server's** filesystem (REP-060, no writer stall); `Verify` re-hashes a
    backup against its manifest (REP-064). Restore and PITR are offline CLI
    operations (`fluxum backup restore [--pitr-*]`) — never against a live
    server.
  - *Namespaces & quotas*: per-tenant memory/storage/subscription usage from
    the OPS-051/061 metric series.
- **Live** — the raw committed-diff stream (`/console/watch`), optionally
  filtered to one table. Lock-free by design (DEV-031): it reads the commit
  broadcast only, so an open console can never violate the `/health` budget.
- **Logs** — the `/logs` follow stream with text, minimum-severity
  (ERROR/WARN+/INFO+/DEBUG+) and reducer-only filters; slow-reducer warnings
  highlighted (DEV-032).
- **Metrics** — stat tiles sampled from `/metrics` every 5 s while visible
  (tx rate, connections, subscriptions, queue depth, fan-out rate and latency,
  buffer-pool hit rate and occupancy, memstore), each with a sparkline of the
  last ~5 minutes; the raw Prometheus series sit behind the `Series` toggle.
- **New table** — the designer. Tables are compiled into the module
  (`#[fluxum::table]`, link-time registry — by design there is no runtime
  `CREATE TABLE`), so the designer emits the Rust snippet to paste into your
  module, live, with a copy button.

## Security posture

Every state-changing action the console drives is a SEC-054 `admin_mutation`
audit event, attributed to the operator (peer name, or `loopback`). The audit
inventory is pinned by a unit test; the whole console contract is swept by a
spawned-binary e2e smoke (`crates/fluxum-server/tests/console_e2e_smoke.rs`).
The shell is assembled at compile time from `console.html` + `console*.js`
into one served page; a build-time test forbids any absolute URL in it.

## Known limits (stated in the UI where they bite)

- Foreign keys and migration history are not yet in the `/schema` document, so
  the schema browser cannot render them.
- Keyset paging needs an indexed ORDER BY column; the planner's QP-040
  refusal is surfaced verbatim.
- The watch stream serves the default database; per-namespace watch arrives
  when a UI need appears (the broadcast already exists per namespace).
- No S3-archive restore-point browser yet — it needs an archive-catalog API in
  the core.
