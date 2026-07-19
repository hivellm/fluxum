# Proposal: phase6_auth-rls-hardening

## Why
OWASP Top 10:2025 residual findings across A07/A01/A08: **F-003 (Medium)** — the
`shard_local` / `custom` / `member_of` RLS visibility modes silently impose no
filter (`sql/mod.rs:743-765`), so a declared rule can quietly mean "no filter";
**F-019 (Low)** — JWTs are symmetric HS256, so the DB holds the minting secret and
any DB compromise mints tokens; **F-021 (Medium)** — sidecar responses are
unauthenticated / trust-on-deserialize; **F-020 (Low)** — permissive-auth identity
multiplication. These are the P2 hardening tail after the P0 access-control and
transport work.

## What Changes
Fail-closed enforcement (or explicit schema-load rejection) of the unimplemented
RLS visibility modes; an asymmetric verify-only JWT provider variant; a hard
mTLS/loopback requirement for the sidecar transport with decode failures tripping
the breaker; and a bound on permissive-auth identity minting.

## Impact
- Affected specs: SPEC-009 (auth), SPEC RLS/visibility, SPEC sidecar transport.
- Affected code: `fluxum-core/src/sql/mod.rs` (visibility eval), auth provider,
  sidecar transport/deserialization path.
- Breaking change: POSSIBLE — a schema declaring an unimplemented visibility mode
  will now be rejected instead of silently passing (fail-closed); called out in
  migration notes.
- User benefit: a declared RLS rule can never silently disable itself; DB
  compromise no longer implies token-minting; the sidecar channel is authenticated.

## Notes
Depends on `phase6_admin-surface-authz` and `phase6_transport-tls-secret-hygiene`
(P0). Per-IP failed-auth global ceiling / lockout (F-018) is largely owned by
`phase6_ip-blocklist-global-caps`; this task covers the remaining auth/RLS items,
not that ceiling. Session-token hardening (F-020 adjacency) lives in
`phase6_session-hijack-hardening`.
