## 1. Implementation
- [x] 1.1 CSPRNG session tokens: `session_sec::mint` returns a ≥128-bit CSPRNG token (`fluxum_core::crypto::random_bytes`); the sessions map is keyed by `hex(SHA-256(token))` so only the hash is stored and lookup hashes the presented token first (no plaintext comparison). The identity-derived minting is gone
- [x] 1.2 Anti-fixation: a `Fluxum-Session` with no matching server-minted session is never adopted — it re-auths as a fresh session (new token minted) or gets a 404; regression test `an_unknown_token_is_never_adopted`
- [x] 1.3 Session binding (`server.session.bind_client_ip`, default off): the *resolved* client IP (SEC-035, not the socket peer) is recorded at issue; a mismatch is a 403 counted as `ip_mismatch`, session left intact
- [x] 1.4 Rotation + lifetime: token rotates on re-auth and every `rotate_interval_secs`; the old id lingers in a grace map for `rotate_grace_secs`; `absolute_lifetime_secs` enforced alongside RPC-060 idle expiry
- [x] 1.5 Revocation admin API: `GET /sessions` (no token material), `DELETE /sessions/{id}`, `DELETE /sessions?identity=<hex>`; a terminated session's stream drops (shutdown Notify) and its next request is refused (revoked flag → evict + on_disconnect). Reached from the admin dispatch via a `SessionAdmin` directory installed on `ShardContext` (routes are at the admin root `/sessions`, matching the existing `/health`, `/bans` convention — no `/admin` prefix)
- [x] 1.6 Metrics: `fluxum_session_rejected_total{reason}` with `unknown_token`, `ip_mismatch`, `expired`, `revoked`; recorded on both POST and GET paths
- [x] 1.7 Spec: SPEC-009 §7a (AUTH-090..094: CSPRNG, hashed-at-rest, anti-fixation, binding, rotation, revocation); SPEC-006 notes the opaque-token + anti-fixation contract and lists `/sessions`; SPEC-012 OBS-042 gains the metric; SPEC-026 SEC-050..053 carry the normative block
- [x] 1.8 Verification: `session_hardening.rs` — token unpredictable given a known identity; a bound token replayed from another IP is refused; rotation keeps the old token working across the grace window then kills it; revocation (by id and by identity) refuses the next request; unknown token never adopts a session

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
