# Proposal: phase6_session-hijack-hardening

## Why
With clients hitting the database port directly, the Streamable HTTP session token (`Fluxum-Session` header) IS the bearer credential for every post-auth request — steal it and you are the victim until it expires. The current minting is weak against this threat:

- **Predictable token** ([http.rs](../../crates/fluxum-server/src/http.rs) `mint_token`): the token is `SHA-256(identity ++ counter)` where `counter` is a process-wide `AtomicU64` starting at 1. The only secret is `identity` (itself `SHA-256(client_token)`); it is not a public value, but it appears in logs, metrics labels, and reducer context, and the sequence input has ~no entropy. If an identity ever leaks, every session token for that identity is brute-forceable by walking a small counter. The doc comment claims "unguessable" but the security rests entirely on identity secrecy, not on token randomness (CSPRNG). This is a genuine hijack vector, not hardening polish.
- **No context binding**: a captured session token is replayable from any IP/connection — nothing ties the session to the peer that authenticated it, so a token leaked through a log, a shared proxy, or a mirrored request grants full access.
- **No rotation / no revocation surface**: tokens are not rotated after issue and there is no admin path to kill a live session suspected of being hijacked.

## What Changes
Harden the session credential against theft/replay on a directly exposed port:
- **CSPRNG tokens**: mint the session token from a cryptographically secure RNG (≥128 bits) — independent of `identity` and unpredictable regardless of what leaks; store only a hash of it server-side so a memory/log disclosure of the session map does not yield usable tokens. Constant-time comparison on lookup.
- **Session binding (configurable)**: optionally bind a session to the resolved client IP (works with `phase6_proxy-aware-client-ip`) and/or a client-nonce, so a token presented from a different context is rejected and counted; default posture documented for both strict and roaming clients.
- **Rotation + idle/absolute lifetime**: rotate the token on re-auth and on a configurable interval (old token grace window for in-flight requests); enforce an absolute session lifetime in addition to the existing RPC-060 idle expiry.
- **Revocation**: admin API to list active sessions (by identity/connection) and terminate one or all sessions for an identity — the operator's answer to a suspected hijack.
- **Anti-fixation**: never accept a client-supplied session token that the server did not mint; a `Fluxum-Session` naming an unknown session is always a fresh handshake, never adopted.

## Impact
- DAG task: new (phase 6 hardening; additive)
- Affected specs: SPEC-009 (authentication — session token security), SPEC-006 (Fluxum-Session semantics), SPEC-026 (§4)
- PRD requirements: FR-147 (extends); AUTH-01x/02x
- Affected code: crates/fluxum-server/src/http.rs (mint_token, session map, evict), crates/fluxum-server/src/tcp.rs (connection binding), crates/fluxum-core/src/config, admin API
- Depends on: none (session IP binding composes with phase6_proxy-aware-client-ip)
- Breaking change: NO for wire protocol (token stays an opaque header value); minting/validation internals change
- User benefit: a stolen or leaked session token is far harder to obtain, useless from another context when binding is on, short-lived, and killable on demand
