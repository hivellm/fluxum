## 1. Implementation
- [ ] 1.1 CSPRNG session tokens: replace `SHA-256(identity ++ counter)` with ≥128-bit CSPRNG output; store only `SHA-256(token)` in the session map; look up + compare in constant time. Remove the identity-derived minting entirely
- [ ] 1.2 Anti-fixation: a `Fluxum-Session` value with no matching server-minted session is treated as a fresh unauthenticated handshake, never adopted; add a regression test
- [ ] 1.3 Session binding (config `server.session.bind_client_ip`, default off): on issue, record the resolved client IP; on later requests, a mismatch is rejected (counted as a hijack-suspected metric). Compose with the proxy-aware resolved IP, not the raw socket peer
- [ ] 1.4 Rotation + lifetime: rotate token on re-auth and every `rotate_interval` (old token honored for a short grace window for in-flight requests); enforce `absolute_lifetime` alongside the existing RPC-060 idle expiry
- [ ] 1.5 Revocation admin API: `GET /admin/sessions` (by identity/connection, no token material exposed), `DELETE /admin/sessions/{id}` and `DELETE /admin/sessions?identity=...` to terminate one or all; terminated sessions run on_disconnect and drop the GET stream
- [ ] 1.6 Metrics: `fluxum_session_rejected_total{reason}` with `unknown_token`, `ip_mismatch`, `expired`, `revoked`; wire both transports
- [ ] 1.7 Spec: SPEC-009 gains a session-token security requirement block (CSPRNG, hashed-at-rest, binding, rotation, revocation); SPEC-006 notes the opaque-token + anti-fixation contract
- [ ] 1.8 Verification: tests — token is unpredictable given a known identity; a leaked token replayed from another IP is refused when binding is on; rotation keeps in-flight requests working across the grace window; revocation kills a live GET stream; unknown token never adopts a session

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
