## 1. Implementation
- [ ] 1.1 `require_operator(ctx, payload)` guard: generalize the `/audit` server-peer check (`admin.rs:894-902`) into a reusable operator-credential guard; call it at the top of `dispatch` for `reducer`, `query`, `query/explain`, `drain`, `config/reload`, `plugins/*/enable|disable`, `schema`, `view`, and blob writes. Constant-time token compare; never log token bytes
- [ ] 1.2 Loopback/CIDR gate: add `server.admin.bind` (default `127.0.0.1`) and/or `server.admin.trusted` `IpSet` (reuse `fluxum-core/src/net.rs::IpSet`); reject admin requests whose resolved client IP (reuse `clientip.rs`) falls outside it, mirroring the `none`-provider loopback guard. Read-only `health`/`metrics` stay open or move behind the guard by config
- [ ] 1.3 Honor `client_callable` / schedule-only in admin `reducer_call` (F-004): route admin reducer invocations through the same gating a client session uses, so admin cannot call reducers a client is forbidden from
- [ ] 1.4 Authenticate blob upload/download (F-002) under the operator guard or the normal session identity; unauthenticated blob routes closed
- [ ] 1.5 Metrics + rejection accounting: `fluxum_admin_rejected_total{reason}` with `unauthenticated`, `untrusted_ip`; wire on every guard denial
- [ ] 1.6 Spec: SPEC-026 §5 posture note; new SEC-04x requirement — admin API operator authz + network gate, safe-by-default on direct exposure
- [ ] 1.7 Verification: an admin `reducer`/`query` from a non-loopback IP without a credential is rejected (401/403) and RLS is never bypassed; with credential + trusted CIDR it succeeds; `/health` open-vs-gated toggles by config; blob routes require identity

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
