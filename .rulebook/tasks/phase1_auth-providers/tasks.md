## 1. Implementation
- [ ] 1.1 Define the object-safe `AuthProvider` trait (authenticate to AuthClaims/Identity or error), registrable via `ServerBuilder` (AUTH-030/AUTH-032)
- [ ] 1.2 Implement the `token`, `jwt`, and `none` built-in providers behind config selection
- [ ] 1.3 Implement identity derivation: opaque token provider = SHA-256(token); jwt provider = hash of stable claims (issuer + subject) so token refresh/rotation NEVER changes Identity (FR-70, AUTH-002/AUTH-022); server identity SHA-256("SERVER:" + name) with privileged semantics (FR-72)
- [ ] 1.4 Wire provider selection into the config loader (provider kind + provider-specific options, `server_peers` tokens)
- [ ] 1.5 Enforce the dev-mode loopback guard: `auth.provider: none` with a non-loopback listen address fails startup with the documented error (AUTH-040)
- [ ] 1.6 Verification (DAG exit test): unit tests proving stable identity across reconnects, restarts, and jwt refresh (same iss/sub, different token bytes = same Identity) plus the full provider matrix (token/jwt/none, valid/invalid/expired)
- [ ] 1.7 Gate G1 input: auth suite green (with T1.1 schema and T1.2 codec suites)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
