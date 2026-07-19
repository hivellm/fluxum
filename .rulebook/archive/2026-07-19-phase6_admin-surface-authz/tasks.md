## 1. Implementation
- [x] 1.1 `admin_guard(ctx, req)` at the top of `dispatch`: the operator credential (a configured `auth.server_peers` token) is read from the `Fluxum-Operator` header or a JSON `token` field (compat with `/audit`), authenticated digest-to-digest via the existing `Authenticator`, never logged. `/audit` keeps its own inline server-peer check (data-sensitivity control, required even from loopback)
- [x] 1.2 Loopback/CIDR gate: `server.admin.trusted` (`IpSet`, default empty = loopback only) + `require_operator` + `open_health_metrics`; a non-loopback client outside `trusted` is `403` before any handler runs; a trusted non-loopback client needs the operator credential. Resolved client IP reuses the SEC-035 `clientip` resolution. `health`/`metrics` stay open by default
- [x] 1.3 Honor `client_callable` in admin `reducer_call` (F-004): a schedule-only reducer is `403` even for an operator, via `registry().declaration(name)`
- [x] 1.4 Blob upload/download (F-002) require an authenticated `Fluxum-Session`; the unauthenticated blob routes are closed (`401`)
- [x] 1.5 Metrics: `fluxum_admin_rejected_total{reason}` with `untrusted_ip`, `unauthenticated`, wired on every guard denial
- [x] 1.6 Spec: SPEC-026 SEC-054/055/056 (admin operator authz + network gate, safe-by-default, client_callable + blob auth) with a config block; SPEC-012 OBS gains `fluxum_admin_rejected_total`
- [x] 1.7 Verification: `admin_authz.rs` — a remote `reducer`/`query` without a credential is `403` (no write, no RLS-bypassing read); a trusted remote needs a valid operator token (401 without, 200 with); outside the trusted range is a hard 403; `/health`/`/metrics` open-vs-gated toggles by config; a schedule-only reducer is refused; blob routes require a session. Default (loopback) behavior unchanged

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
