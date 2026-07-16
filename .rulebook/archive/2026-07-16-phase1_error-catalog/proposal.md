# Proposal: Stable machine-readable error catalog (SPEC-028 ERR)

## Why

Fluxum clients connect directly to the database — there is no application
server in between to translate failures. The wire `Error` frame therefore IS
the public failure API, and today it is too weak for that role: only 9 generic
HTTP-style `u16` codes (RPC-034), free-form messages the client would have to
string-match, no retry hints, no structured details, and reducer failures
travel as verbatim `Err(String)` with no code at all (RED-060). SDKs cannot
offer ergonomic error matching and the future error documentation has no
stable identifiers to anchor on.

## What Changes

- New **SPEC-028** defining the error catalog: `u16` codes in per-subsystem
  ranges (1xxx protocol, 2xxx auth, 3xxx SQL/txn, 4xxx schema/migration,
  5xxx reducer, 6xxx subscription, 7xxx storage, 8xxx cluster, 9xxx system)
  plus canonical `SCREAMING_SNAKE` names.
- **Amend RPC-034 (SPEC-006)**: `ErrorMessage` gains `name`, `severity`
  (`error|fatal`), `retryable`, `retry_after_ms`, `sqlstate` (SQL range only,
  PostgreSQL-compatible) and a structured `details` map with per-code
  documented keys. Existing HTTP codes are replaced by catalog codes; a
  derived HTTP-status mapping preserves Streamable-HTTP semantics.
- **Amend RED-060 (SPEC-004)**: user `Err(String)` is wrapped as
  `5001 REDUCER_USER_ERROR` (message verbatim); reducers MAY attach an
  optional application-defined `app_code` string for client-side matching.
- Single-source **registry in `fluxum-protocol`**: one table holding code,
  name, default severity/retryability, SQLSTATE and details keys — emission
  sites, docs and future SDK enums all derive from it; uniqueness and
  registry-adherence enforced by tests.
- `FluxumError` (fluxum-core) maps totally onto the catalog;
  `FluxumError::Query { code }` migrates from HTTP codes to catalog codes.

## Impact

- Affected specs: SPEC-028 (new), SPEC-006 RPC-034 (amended), SPEC-004
  RED-060 (amended); SPEC-011 SDK codegen consumes the registry later.
- Affected code: `fluxum-protocol` (`codes.rs` rewritten as registry,
  `messages.rs` ErrorMessage, fluxbin/envelope golden files regenerated),
  `fluxum-core` (`error.rs` mapping, all `FluxumError::query(...)` call
  sites), `fluxum-server` (every Error-frame emission site, HTTP status
  derivation).
- Breaking change: YES (wire format, pre-1.0 — no migration path required)
- User benefit: clients match errors by stable code/name instead of parsing
  strings; SDKs implement safe automatic retry from `retryable`/
  `retry_after_ms`; complete error reference documentation becomes generable
  from a single registry.
