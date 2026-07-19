# Proposal: phase6_admin-surface-authz

## Why
OWASP Top 10:2025 analysis finding **F-001 (Critical)**: the HTTP admin surface is
served on the same public `http_port` as `/rpc` with no authentication and no
loopback gate for almost every route (`admin.rs::dispatch`, reached from
`http.rs::handle_admin`). On the project's direct-port-exposure posture with no
TLS, `POST /reducer/:name` is arbitrary writes under the admin identity,
`POST /query` is arbitrary reads bypassing RLS, and `drain`/`config/reload`/
`plugins/*/disable` are DoS and security-control disablement. Only `/audit`
checks a credential. This is the single highest-severity finding; a directly
exposed node must be safe by default, not "safe behind a proxy".

## What Changes
Every mutating/read-sensitive admin route gains an operator-credential guard and a
loopback/CIDR network gate; admin reducer calls honor the same client-callable /
schedule-only gating a client session uses; blob upload/download is authenticated.

## Impact
- Affected specs: SPEC-026 (§5 posture), admin-surface access-control; new SEC
  requirement for operator authz on the admin API.
- Affected code: `fluxum-server/src/admin.rs`, `http.rs` (`handle_admin`),
  `clientip.rs`, `fluxum-core/src/net.rs` (`IpSet`), config `mod.rs`.
- Breaking change: NO in the default posture (guard defaults to loopback-only);
  operators exposing admin remotely must set a credential + trusted CIDR.
- User benefit: a directly exposed Fluxum node no longer offers unauthenticated
  read/write/DoS over its admin API.
