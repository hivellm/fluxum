## 1. Implementation
- [x] 1.1 Define the object-safe `AuthProvider` trait (authenticate to AuthClaims/Identity or error), registrable via `ServerBuilder` (AUTH-030/AUTH-032) — `auth::AuthProvider` + `Authenticator::with_provider` for custom providers
- [x] 1.2 Implement the `token`, `jwt`, and `none` built-in providers behind config selection — `auth::{TokenProvider, JwtProvider, NoneProvider}` + `provider_from_config`
- [x] 1.3 Implement identity derivation: opaque token provider = SHA-256(token); jwt provider = hash of stable claims (issuer + subject) so token refresh/rotation NEVER changes Identity (FR-70, AUTH-002/AUTH-022); server identity SHA-256("SERVER:" + name) with privileged semantics (FR-72) — `AuthClaims::identity`, `auth::server_identity`, reserved `SERVER:` canonical namespace rejected for clients, `AuthOutcome.bypass_rls`
- [x] 1.4 Wire provider selection into the config loader (provider kind + provider-specific options, `server_peers` tokens) — `Authenticator::from_config(&Config)` consumes `auth.provider`/`auth.secret`/`auth.server_peers` (config schema landed in T0.4)
- [x] 1.5 Enforce the dev-mode loopback guard: `auth.provider: none` with a non-loopback listen address fails startup with the documented error (AUTH-040) — `auth::enforce_loopback_guard`, applied by `Authenticator::from_config`
- [x] 1.6 Verification (DAG exit test): unit tests proving stable identity across reconnects, restarts, and jwt refresh (same iss/sub, different token bytes = same Identity) plus the full provider matrix (token/jwt/none, valid/invalid/expired)
- [x] 1.7 Gate G1 input: auth suite green (with T1.1 schema and T1.2 codec suites) — auth suite green (25 tests); T1.1/T1.2 suites tracked in their own tasks

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — full rustdoc on `fluxum_core::auth` (module, trait, providers, registry, guard) cross-referencing SPEC-009 requirement ids
- [x] 2.2 Write tests covering the new behavior — 25 unit tests: provider matrix, rotation/refresh invariants, loopback guard, server-peer registry + namespace non-collision, custom provider pluggability
- [x] 2.3 Run tests and confirm they pass — `cargo fmt` clean, `cargo clippy --workspace --all-targets -- -D warnings` clean, `cargo test --workspace` 71 passed / 0 failed
