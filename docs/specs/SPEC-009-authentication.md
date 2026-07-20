# SPEC-009 — Authentication & Identity

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 1 · T1.3 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-70, FR-71, FR-72, FR-73 |
| **Requirement prefix** | `AUTH-` |
| **Source** | UzDB spec 10, ported TML → Rust and generalized (game → general-purpose) |

## 1. Overview

Fluxum's authentication model is designed for realtime application backends: simple,
token-based, with stable cross-session identity. OIDC is NOT required as a built-in.
An application can use JWT tokens, API keys, custom signed tokens, or even a dev-mode
"no auth" provider — any scheme is pluggable through the `AuthProvider` trait.

Identity is a 256-bit value derived deterministically from the token, ensuring the
same user always has the same identity across reconnections and sessions.

Cross-references: RLS visibility filtering is specified in
[SPEC-005](SPEC-005-subscriptions.md); the `Authenticate`/`AuthResult`/`Error` message
envelopes in [SPEC-006](SPEC-006-protocol-fluxrpc.md); per-reducer rate limiting in
[SPEC-004](SPEC-004-reducers.md); entity handoff in [SPEC-007](SPEC-007-sharding.md).

## 2. Identity

- **AUTH-001** [P0] An `Identity` SHALL be a 32-byte (256-bit) value derived from the
  authentication token:

  ```
  Identity = SHA-256(canonical_token_bytes)
  ```

  where `canonical_token_bytes` is `AuthClaims.canonical_token` as returned by the
  active `AuthProvider`. The canonical form MUST be a *stable* derivation — stable
  across token rotation, refresh, and expiry — not necessarily the raw token bytes:

  - For the built-in `jwt` provider, `canonical_token` MUST be derived from stable
    claims, `"{issuer}|{subject}"` (the validated `iss` and `sub` claims joined by
    `|`), NOT from the raw token bytes. Identity is therefore
    `SHA-256(issuer || "|" || subject)` and survives token rotation, re-signing,
    refresh, and expiry changes by construction — any number of distinct tokens with
    the same `(iss, sub)` map to the same Identity. Hashing raw JWT bytes would break
    identity on every rotation; SpacetimeDB moved to claims-based identity for
    exactly this reason. `iss` and `sub` MUST be non-empty for the derivation to be
    accepted. *(adopted from SpacetimeDB analysis, file 08)*
  - For the built-in opaque `token` and `none` providers, `canonical_token` is the
    raw token bytes (`Identity = SHA-256(token)`). This is acceptable because opaque
    tokens are long-lived by definition there — the token value itself is the stable
    identifier. Trade-off: if such a token is ever reissued with a different value,
    the identity changes; applications needing rotation-proof identity with opaque
    tokens must use a custom `AuthProvider`. *(adopted from SpacetimeDB analysis, file 08)*
  - A custom `AuthProvider` (AUTH-032) chooses its own stable derivation and is
    responsible for keeping `canonical_token` invariant across refreshes.

  The same canonical form SHALL always produce the same `Identity`. Different
  canonical forms SHALL produce different Identities (SHA-256 collision resistance).
  In Rust, `Identity` is the `Identity([u8; 32])` newtype.

- **AUTH-002** [P0] `Identity` SHALL be stable across:
  - Client reconnections (same token → same identity)
  - Server restarts
  - Shard migrations (entity handoff, [SPEC-007](SPEC-007-sharding.md))
  - Token refresh, rotation, and expiry — **token refresh MUST NOT change `Identity`
    for any provider** (invariant). For the `jwt` provider this holds by construction
    (claims-based derivation, AUTH-001); for the `token`/`none` providers refresh
    returns the same token (AUTH-022); a custom `AuthProvider` MUST keep
    `AuthClaims.canonical_token` byte-identical across refreshes of the same
    principal. *(adopted from SpacetimeDB analysis, file 08)*

- **AUTH-003** [P0] Every `ReducerContext` SHALL carry the caller's `Identity`.
  Reducers MAY use `ctx.identity` to enforce ownership rules, filter data, and
  track per-user state.

- **AUTH-004** [P0] The subscription system ([SPEC-005](SPEC-005-subscriptions.md))
  SHALL use the connection's authenticated identity as the basis for
  `#[visibility(owner_only(owner))]` row-level security filtering.

## 3. ConnectionId

- **AUTH-010** [P0] Each TCP or Streamable HTTP connection SHALL be assigned a random
  128-bit `ConnectionId` (the `ConnectionId(u128)` newtype) at connection
  establishment. `ConnectionId` is ephemeral — it is NOT persisted and changes on
  every reconnect.

  `ConnectionId` is used to:
  - Correlate `#[fluxum::on_connect]` and `#[fluxum::on_disconnect]` lifecycle events
  - Track which connection maps to which `Identity` in the session table
  - Enable multiple connections per identity (multiple devices or tabs, read-only
    dashboard views)

## 4. Authentication flow

- **AUTH-020** [P0] A client connection SHALL send an `Authenticate { id, token }`
  message before any `ReducerCall`, `Subscribe`, or `OneOffQuery` message
  ([SPEC-006](SPEC-006-protocol-fluxrpc.md)).

  Unauthenticated messages SHALL receive:

  ```
  Error { id: <request_id>, code: 401, message: "unauthenticated" }
  ```

- **AUTH-021** [P0] On successful authentication, the server SHALL:
  1. Derive `Identity = SHA-256(canonical_token)` (AUTH-001)
  2. Store `(ConnectionId, Identity)` in the connection context
  3. Fire the `#[fluxum::on_connect]` lifecycle reducer
  4. Return `AuthResult { id, identity: [u8; 32], token: refreshed_token }`

  On authentication failure (invalid token, expired token), the server SHALL return:

  ```
  Error { id, code: 401, message: "authentication failed: <reason>" }
  ```

  The connection SHALL remain open; the client MAY retry with a different token.

- **AUTH-022** [P1] The `AuthResult` MAY return a `token` field with a refreshed
  token value. For JWT providers, this is a new JWT with extended expiry.
  For non-expiring tokens (API keys), the returned token SHALL be identical to the
  input. Token refresh MUST NOT change the caller's `Identity` for any provider —
  built-in or custom (invariant; AUTH-002). For the `jwt` provider this follows from
  claims-based derivation (AUTH-001): the refreshed JWT carries the same
  `(iss, sub)` and thus the same Identity even though the token bytes differ.
  *(adopted from SpacetimeDB analysis, file 08)*

## 5. AuthProvider (pluggable auth)

- **AUTH-030** [P0] Auth logic SHALL be pluggable via an object-safe Rust trait,
  installed as `Arc<dyn AuthProvider>`:

  ```rust
  pub trait AuthProvider: Send + Sync {
      /// Validate a token and return its claims, or an error reason.
      fn authenticate(&self, token: &[u8]) -> Result<AuthClaims, String>;

      /// Return a refreshed token, or the same token for non-expiring schemes.
      fn refresh(&self, token: &[u8]) -> Result<Vec<u8>, String>;
  }

  pub struct AuthClaims {
      /// Used for Identity derivation (stable across refreshes).
      pub canonical_token: Vec<u8>,
      pub display_name: Option<String>,
      pub roles: Vec<String>,
      /// µs since Unix epoch; `None` = no expiry.
      pub expires_at: Option<Timestamp>,
  }
  ```

- **AUTH-031** [P0] The following auth providers SHALL be built in:

  | Provider | Config | Description |
  |----------|--------|-------------|
  | `token` | `secret: <bytes>` | HMAC-SHA256 signed opaque token; `canonical_token` = raw token bytes (long-lived token, see AUTH-001) |
  | `jwt` | `secret: <str>` (hs256) or `jwt_public_key` (asymmetric) | JWT verification (`jsonwebtoken`); `canonical_token` = `"{iss}|{sub}"` — identity is rotation-proof (AUTH-001) *(adopted from SpacetimeDB analysis, file 08)* |
  | `none` | — | Dev mode: any token is accepted; identity = SHA-256(token); distinct identities bounded by `max_permissive_identities` (SEC-062) |

  **JWT algorithm (SEC-061).** `auth.jwt_algorithm` (default `hs256`) selects the signature
  scheme. `hs256` is symmetric — the DB holds the shared secret and can mint tokens (lower
  assurance). `rs256`/`es256`/`ed25519` are asymmetric and **verify-only**: the DB holds only
  `auth.jwt_public_key`, so a DB compromise cannot forge tokens; `refresh` returns the presented
  token unchanged (a fresh token comes from the external issuer).

  Configuration in `config.yml`:

  ```yaml
  auth:
    provider: token      # token | jwt | none
    secret: ${FLUXUM_AUTH_SECRET}
  ```

- **AUTH-032** [P1] An application developer MAY implement a custom `AuthProvider`
  in Rust and register it at startup via `fluxum::ServerBuilder` (as an
  `Arc<dyn AuthProvider>`). This enables application-specific auth schemes
  (third-party identity services, OIDC bridges, custom session tokens, etc.)
  without modifying Fluxum source.

## 6. Dev mode (no-auth)

- **AUTH-040** [P0] When `auth.provider: none` is configured, the server SHALL:
  1. Accept any token bytes
  2. Derive Identity = SHA-256(token)
  3. Return `AuthResult` immediately without validation

  This enables fast local development without an auth infrastructure.
  The `none` provider SHALL be rejected at startup if the server is configured with a
  non-loopback listen address (to prevent accidental exposure on public interfaces):

  ```
  ERROR: auth.provider=none is only permitted when the listen address is 127.0.0.1 or ::1
  ```

## 7. Session tracking table

- **AUTH-050** [P0] The runtime SHALL maintain a built-in private, global
  `__session__` system table (runtime-defined; not declared by application code):

  ```rust
  #[fluxum::table(private, global)]
  pub struct __session__ {
      #[primary_key]
      pub connection_id: ConnectionId,
      pub identity: Identity,
      pub connected_at: Timestamp,
      pub shard_id: u32,
  }
  ```

  This table is populated by the runtime on `#[fluxum::on_connect]` and cleaned up on
  `#[fluxum::on_disconnect]`. Reducers MAY read it via
  `ctx.tx.query_pk::<__session__>(conn_id)` to look up the identity for a given
  connection.

## 7a. Streamable HTTP session-token security (`AUTH-09x`, SPEC-026 SEC-050..053)

On the Streamable HTTP transport the `Fluxum-Session` header is the bearer
credential for every post-auth request. On a directly exposed port (SPEC-026),
stealing it makes the thief the victim until it expires, so the token is
hardened against theft and replay. (The TCP transport keeps its credential on
one long-lived socket and needs none of this.)

- **AUTH-090** [P0] The session token SHALL be **CSPRNG** output of at least
  128 bits, independent of the caller's `Identity` — unpredictable regardless
  of what else (identity, logs, metrics) leaks. Deriving it from the identity
  and a counter is forbidden.
- **AUTH-091** [P0] The server SHALL store only a **hash** of the token
  (`SHA-256`), keyed on it; a disclosure of the session map yields no usable
  token. Lookup hashes the presented token first (no secret-dependent
  comparison in the clear). **Anti-fixation:** a `Fluxum-Session` value the
  server never minted hashes to an absent id and is NEVER adopted — it is a
  fresh unauthenticated handshake (which, if it authenticates, receives a
  freshly minted token), never the client-supplied value.
- **AUTH-092** [P1] Session **binding** MAY be enabled
  (`server.session.bind_client_ip`, default off): the resolved client IP
  (SPEC-026 SEC-035, not the proxy peer) is recorded at issue and a request
  presenting the token from another IP is refused and counted. Default off so
  roaming clients are not logged out on every network change.
- **AUTH-093** [P1] The token SHALL **rotate** on re-authentication and, when
  `server.session.rotate_interval_secs` is set, on that interval; a rotated
  token is honored for a short `rotate_grace_secs` window for in-flight
  requests. An `absolute_lifetime_secs` MAY cap total session age on top of
  the RPC-060 idle expiry.
- **AUTH-094** [P1] The admin API SHALL expose **revocation**: `GET /sessions`
  (identity, connection, age, bound IP — never token material),
  `DELETE /sessions/{id}`, and `DELETE /sessions?identity=<hex>`. A terminated
  session's push stream drops and its next request is refused. Refusals are
  counted as `fluxum_session_rejected_total{reason}` with
  `reason ∈ {unknown_token, ip_mismatch, expired, revoked}`.

```yaml
server:
  session:
    bind_client_ip: false     # AUTH-092 bind to the authenticating client IP
    rotate_interval_secs: 0    # AUTH-093 rotate this often (0 = only on re-auth)
    rotate_grace_secs: 30      # AUTH-093 old-token grace window
    absolute_lifetime_secs: 0  # AUTH-093 hard session-age cap (0 = idle only)
```

## 8. Server-to-server identity

A trusted backend service — for example, an ingestion service or an internal admin
tool — connects to Fluxum as a privileged server peer. It calls reducers (e.g.,
`ingest_readings_batch`, `send_notification_batch`) on behalf of the backend — it is
not an end user.

- **AUTH-060** [P0] A server-level identity SHALL be derived as:

  ```
  ServerIdentity = SHA-256("SERVER:" + server_name_bytes)
  ```

  where `server_name` is a UTF-8 string configured in `config.yml`.
  This ensures the server identity is deterministic, stable, and distinct from any
  user identity (user tokens never start with the `"SERVER:"` prefix — the namespace
  is reserved).

- **AUTH-061** [P0] Server peer tokens SHALL be configured in `config.yml`:

  ```yaml
  auth:
    server_peers:
      - name: "ingestion_service"
        token: ${FLUXUM_INGESTION_TOKEN}    # long-lived shared secret
      - name: "admin_tool"
        token: ${FLUXUM_ADMIN_TOKEN}
  ```

  A server peer authenticates using the standard `Authenticate { token }` message.
  The runtime recognises the token as a server token and assigns the corresponding
  `ServerIdentity`.

- **AUTH-062** [P0] Connections authenticated as a server identity SHALL:
  1. Bypass all `#[visibility]` RLS filters — they can read and write any row
  2. Be exempt from per-reducer rate limits ([SPEC-004](SPEC-004-reducers.md))
  3. NOT fire `#[fluxum::on_connect]` / `#[fluxum::on_disconnect]` lifecycle reducers
     (to avoid polluting presence tables such as `OnlineUser` with service connections)
  4. Have unlimited shard queue priority — server calls are never 503'd due to
     queue full

- **AUTH-063** [P1] Reducers MAY detect whether they are being called by a server
  peer:

  ```rust
  #[fluxum::reducer]
  fn ingest_readings_batch(ctx: &ReducerContext, readings: Vec<Sensor>) -> Result<(), String> {
      if !ctx.is_server_identity() {
          return Err("forbidden: server-only reducer".into());
      }
      for reading in readings {
          ctx.tx.upsert::<Sensor>(reading)?;
      }
      Ok(())
  }
  ```

  `ctx.is_server_identity()` returns `true` if `ctx.identity` is in the server
  identity namespace (`SHA-256("SERVER:*")`). This is a helper method on
  `ReducerContext`, not a runtime-enforced restriction — the reducer itself decides
  what to do.

## 9. Role-based access control (RBAC)

- **AUTH-070** [P2] If the `AuthProvider` returns `claims.roles`, the runtime SHALL
  make the role list available in `ReducerContext`:

  ```rust
  pub struct ReducerContext {
      // ...
      /// From `AuthClaims.roles` (empty if the provider does not supply roles).
      pub roles: Vec<String>,
  }
  ```

  Reducers MAY gate operations on `ctx.roles`:

  ```rust
  #[fluxum::reducer]
  fn admin_ban(ctx: &ReducerContext, target: Identity) -> Result<(), String> {
      if !ctx.roles.iter().any(|r| r == "admin") {
          return Err("forbidden: admin role required".into());
      }
      // ...
      Ok(())
  }
  ```

  **Status:** P2 — not required for launch. Initial launch uses `#[visibility]` and
  identity-based ownership checks only.

## Acceptance criteria

1. **Identity determinism** (AUTH-001): authenticating twice with the same token —
   across a reconnect and across a server restart — yields byte-identical 32-byte
   Identities; two distinct tokens yield distinct Identities (for the `jwt` provider:
   two tokens with distinct `(iss, sub)` yield distinct Identities, while two
   distinct tokens sharing the same `(iss, sub)` — e.g. different expiry or
   signature — yield the SAME Identity). *(adopted from SpacetimeDB analysis, file 08)*
2. **Identity stability across handoff** (AUTH-002): a client whose rows undergo
   entity handoff between shards observes an unchanged `ctx.identity` before and
   after the migration.
3. **Auth gating** (AUTH-020): sending `ReducerCall`, `Subscribe`, or `OneOffQuery`
   before `Authenticate` returns `Error { code: 401, message: "unauthenticated" }`
   and the connection stays open.
4. **Successful auth flow** (AUTH-021, AUTH-050): a valid `Authenticate` returns
   `AuthResult` with a 32-byte identity, fires `#[fluxum::on_connect]`, and inserts a
   `__session__` row keyed by the connection's `ConnectionId`; the row is removed on
   disconnect.
5. **Auth failure is retryable** (AUTH-021): an invalid or expired token returns
   `Error { code: 401, message: "authentication failed: <reason>" }`; the same
   connection then authenticates successfully with a valid token.
6. **Token refresh never changes Identity** (AUTH-002, AUTH-022): with the `jwt`
   provider, `AuthResult.token` is a new JWT with extended expiry (different token
   bytes) and re-authenticating with the refreshed token yields a byte-identical
   Identity to the original; with the `token` provider, the returned token equals the
   input; a custom provider whose `canonical_token` is stable across refresh likewise
   yields an unchanged Identity. This invariant holds for every provider.
   *(adopted from SpacetimeDB analysis, file 08)*
7. **Provider pluggability** (AUTH-030, AUTH-032): a custom `Arc<dyn AuthProvider>`
   registered via `fluxum::ServerBuilder` is invoked for `Authenticate` and its
   `AuthClaims.canonical_token` drives identity derivation.
8. **Dev-mode loopback guard** (AUTH-040): with `auth.provider: none` and a
   non-loopback listen address, the server fails startup with the documented error;
   on `127.0.0.1`/`::1` it accepts an arbitrary token and returns
   `identity = SHA-256(token)`.
9. **Server peer privileges** (AUTH-060–AUTH-062): a peer authenticating with a
   configured `server_peers` token receives `SHA-256("SERVER:" + name)` as identity,
   reads rows hidden by `#[visibility(owner_only(owner))]` belonging to other
   identities, is not rate-limited, and fires no connect/disconnect lifecycle
   reducers.
10. **Server-only reducers** (AUTH-063): `ctx.is_server_identity()` returns `true`
    for a server-peer connection and `false` for a normal client; a guarded reducer
    rejects the latter with the forbidden error.
11. **RBAC roles** (AUTH-070, P2 gate): when the provider supplies
    `AuthClaims.roles`, `ctx.roles` contains them, and a role-gated reducer rejects
    callers lacking the required role.
