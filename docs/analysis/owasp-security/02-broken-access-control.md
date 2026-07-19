# 02 — A01:2025 Broken Access Control

The single most impactful cluster. The admin surface is unauthenticated, and it
is wired to identities that bypass row-level security by design.

---

## F-001 — Unauthenticated admin API grants arbitrary reads/writes and DoS (CRITICAL)

**Evidence.** `admin::dispatch` routes every admin verb with **no authentication
check** except the one route that self-checks a token:

- `crates/fluxum-server/src/admin.rs:81-103` — `dispatch` matches routes and
  dispatches directly; no credential is consulted for `reducer`, `query`,
  `query/explain`, `schema`, `view`, `plugins`, `plugins/:name/disable|enable`,
  `drain`, `config/reload`, `health`, `metrics`.
- `crates/fluxum-server/src/http.rs:332-333` and `:411-428` — `handle_admin`
  calls `admin::dispatch(ctx, method, path, body)` with no auth gate and **no
  loopback restriction**. It is served on the *same* `http_port` as `/rpc`.
- `crates/fluxum-server/src/admin.rs:894-902` — **only** `POST /audit` requires a
  server-peer token. Every other route is open.

What each open route grants:

- `POST /reducer/:name` (`admin.rs:803-819`) — calls any reducer under
  `ctx.admin_identity` (a server identity): **arbitrary authenticated writes**.
- `POST /query` (`admin.rs:824-852`, esp. `:835`
  `Subscriber::server_peer(ctx.admin_identity) // admin bypasses RLS`) —
  **arbitrary reads of all rows, bypassing RLS and column masking**.
- `POST /drain` and `POST /config/reload` (`admin.rs:99-100`) — **DoS / config
  tampering** by anyone who can reach the port.
- `POST /plugins/:name/disable` (`admin.rs:97,121-130`) — hot-disable plugins,
  including the `visibility`/`column_transform` seams — **can switch off security
  controls**.
- `GET /schema`, `/metrics`, `/view/:name`, `/query/explain` — full schema,
  internal metrics, view results, and query plans, all unauthenticated.

**Impact.** With the project's documented "expose ports directly" model
(`01-scope-threat-model.md`) and `tcp_host` defaulting to `0.0.0.0`
(`crates/fluxum-server/src/boot.rs:150`), any client that can reach `http_port`
has full unauthenticated read/write access to the database *bypassing RLS*, plus
drain/reload/plugin-disable denial of service. This is A01 (Broken Access
Control) and A07 (Authentication Failures) in one surface.

**Confidence: High (verified).** Read `dispatch`, `handle_admin`, and the
per-route handlers directly.

---

## F-002 — Unauthenticated blob upload/download on the HTTP port (HIGH)

**Evidence.** `POST /blob` (256 MB cap) and `GET /blob/:hash` are served by the
HTTP transport with no credential check (`crates/fluxum-server/src/http.rs`
~`:366-409`, per surface map). Uploads are content-addressed but unauthenticated.

**Impact.** Anonymous callers can write up to 256 MB blobs (storage-exhaustion
DoS) and read any blob by hash. Same root cause as F-001: admin/operator surfaces
share the public port with no auth.

**Confidence: Medium** (from the surface map; blob handler not re-read here).

---

## F-003 — RLS visibility rules silently impose no filter for several modes (MEDIUM)

**Evidence.** `crates/fluxum-core/src/sql/mod.rs:743-765` compiles a
`VisibilityRule` into the plan's `rls` closure, but only `owner_only` is
enforced. `shard_local`, `custom` (SUB-032), and part of `member_of` are
documented as **currently imposing no row filter**. Enforcement point is
`row_visible` in `crates/fluxum-core/src/subscription/mod.rs` (~`:436`, `:1078`,
`:1261`, `:1336`, `:1725`); a `viewer = None` (public plans **and** server peers,
`:159-160`, `:1901-1902`) sees every row.

**Impact.** A table declared `#[visibility(shard_local)]` or `custom` receives
**no filtering** — an application relying on those annotations for confidentiality
is silently over-exposed. Broken-access-control by omission.

**Confidence: High.**

---

## F-004 — Admin `reducer_call` skips client-callable / schedule-only gating (LOW–MEDIUM)

**Evidence.** `crates/fluxum-server/src/admin.rs:809` calls `ctx.engine.call(...)`
directly under `admin_identity`, bypassing the `client_callable` / schedule-only
(RED-025) restrictions that a normal client session enforces.

**Impact.** Reducers an application marked *not* client-callable (e.g. internal
or schedule-only) become invocable through the (already unauthenticated) admin
surface. Compounds F-001; even after F-001 is fixed, an operator credential is a
larger blast radius than intended.

**Confidence: Medium.**

---

## Positives (A01)

- RLS/column-grant enforcement is **centralized** in the subscription manager and
  applied on every read/fan-out path (`subscription/mod.rs`), not scattered.
- The server-identity namespace is **non-forgeable**: client canonical tokens
  starting with `SERVER:` are rejected (`crates/fluxum-core/src/auth/mod.rs:300-304`,
  tested `:463-478`), so a client cannot climb into a bypass identity via crafted
  token bytes.
- `owner_only` visibility and per-`(caller, column, row)` masking are correctly
  applied before rows leave the read path.
