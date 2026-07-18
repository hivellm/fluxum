# Proposal: phase5_connection-abuse-protection

## Why
The only rate limiter Fluxum has is the reducer admission limiter in `crates/fluxum-core/src/reducer/ratelimit.rs`: its token buckets are keyed per `(Identity, reducer)` and, as its own doc says, rejection happens "before any transaction or `TxState` exists" — but still only after the caller has an `Identity`, i.e. POST-auth. It cannot see or throttle an unauthenticated client, the handshake, or a flood of TCP/HTTP connections. Nothing in the transports (`crates/fluxum-server/src/tcp.rs`, `http.rs`, `session.rs`) caps concurrent connections per IP, limits connection accept rate, or throttles repeated failed `Authenticate` attempts, leaving the pre-auth surface open to connection floods, auth brute-force, and slowloris.

## What Changes
The transports enforce a per-IP concurrent-connection cap and a connection-accept rate limit, independent of the post-auth reducer limiter. Repeated failed `Authenticate` attempts from an address are throttled with exponential backoff, tracked in the auth layer (`crates/fluxum-core/src/auth`). The handshake / `Authenticate` exchange gets a bounded time and size budget to blunt slowloris (slow or oversized handshakes are dropped). Abuse events surface as a `fluxum_conn_rejected_total{reason}` counter so operators can see connection caps, accept-rate, failed-auth, and handshake-budget rejections separately. Legitimate clients on other addresses are unaffected.

## Impact
- Governing spec: docs/specs/SPEC-026-security-hardening.md
- Related specs: docs/specs/SPEC-004 (reducer rate limiting, post-auth), transport/handshake + Authenticate specs, docs/specs/SPEC-009 (identity/auth)
- New PRD requirements: FR-147
- Requirements covered: SEC-030, SEC-031, SEC-032
- Affected code: crates/fluxum-server/src/tcp.rs and crates/fluxum-server/src/http.rs (per-IP accept caps + accept-rate limit), crates/fluxum-server/src/session.rs (handshake time+size budget), crates/fluxum-core/src/auth (failed-`Authenticate` exponential backoff per address), metrics registry (`fluxum_conn_rejected_total{reason}`)
- Depends on: phase5 transports (archived)
- Breaking change: NO
- User benefit: The pre-auth connection surface is defended in-process — connection floods, auth brute-force, and slowloris are throttled and counted without impacting well-behaved clients, complementing the post-auth reducer limiter.
