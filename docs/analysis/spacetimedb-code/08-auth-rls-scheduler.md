# 08 — Identity/Auth, Row-Level Security, Scheduler, Limits

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` |
| **Crates analyzed** | `crates/auth`, `crates/core` (`src/auth`, `src/host`, `src/energy.rs`, `src/client`, `src/subscription`), `crates/client-api`, `crates/lib` (`identity.rs`, `connection_id.rs`, `scheduler.rs`), `crates/expr`, `crates/engine`, `crates/datastore`, `crates/guard`, `crates/standalone` |
| **Fluxum specs compared** | SPEC-009 (auth), SPEC-004 (reducers/scheduling/rate limits), SPEC-005 (subscriptions/RLS) |
| **Date** | 2026-07-14 |

---

## 1. Identity and authentication

### 1.1 Identity = f(OIDC issuer, subject), not f(token)

SpacetimeDB's `Identity` is a 256-bit value (`u256` newtype, `crates/lib/src/identity.rs`), but since the 1.0 redesign it is **no longer a hash of the token**. It is derived from the *claims* of a validated JWT:

```rust
// crates/lib/src/identity.rs
pub fn from_claims(issuer: &str, subject: &str) -> Self {
    let input = format!("{issuer}|{subject}");
    let first_hash = blake3::hash(input.as_bytes());
    let id_hash = &first_hash.as_bytes()[..26];
    // final layout (big-endian): [0xc2, 0x00] ++ checksum(4 bytes) ++ id_hash(26 bytes)
```

Key properties:

- **Version/checksum prefix**: every claims-derived identity starts with `0xc200`, followed by a 4-byte blake3 checksum of `0xc200 ++ id_hash`. Identities are self-validating and visually recognizable in hex.
- **Token-independent stability**: any number of distinct tokens (refreshed, re-signed, different expiry, different audience) map to the same identity as long as `(iss, sub)` is unchanged. This is the fundamental difference from a token-hash scheme: *identity survives token rotation by construction*.
- **Claims validation** (`crates/auth/src/identity.rs`, `IncomingClaims::try_into`): `iss`/`sub` must be non-empty and ≤ 128 bytes. If the token carries a `hex_identity` claim, it **must match** the computed `Identity::from_claims(iss, sub)` — this is the migration/interop guard against forged or legacy identities.

### 1.2 Token validation pipeline

`crates/core/src/auth/token_validation.rs` implements a chain-of-responsibility:

- `FullTokenValidator<T>` — tries the **local key first** (accepting any issuer, because SpacetimeDB re-signs short-lived tokens under foreign issuers, see `SpacetimeAuth::re_sign_with_expiry` in `crates/client-api/src/auth.rs`); on failure it extracts the raw issuer via `jsonwebtoken::dangerous::insecure_decode` (`get_raw_issuer`, key-discovery only) and delegates to the OIDC validator — unless `iss == local_issuer`, in which case the local-key error is returned.
- `CachingOidcTokenValidator` — an `async_cache::AsyncCache<Arc<JwksValidator>, KeyFetcher>` keyed by issuer; refresh every 300 s, expiry 7200 s. `KeyFetcher::fetch` resolves `{issuer}/.well-known/openid-configuration` → `jwks_uri` → `JwkSet` over `reqwest` (http/https scheme enforced by `validate_url_scheme`).
- `JwksValidator` — matches JWT header `kid` against the keyset; falls back to trying keys without `kid`, then all keys.
- Algorithms accepted: **ES256, RS256, HS256** only (family-matched to the decoding key; Ed rejected). Required claims: `sub`, `iss`. `aud` is *not* validated (TODO in source). Expiration is validated manually (`validate_expiration`, 60 s leeway) to keep accepting legacy `"exp": null` tokens.

### 1.3 Local issuer in standalone

`crates/standalone/src/lib.rs:96`: `auth::default_auth_environment(jwt_keys, LOCALHOST.into())` — the local issuer is the literal `"localhost"` (`crates/client-api/src/auth.rs:34`). Keys are ECDSA P-256 PEM files, auto-generated on first boot (`get_or_create_keys`, `crates/core/src/auth/mod.rs`). Anonymous clients are **allocated an identity server-side**: `SpacetimeAuth::alloc` mints claims `{iss: local_issuer, sub: UUIDv4}`, signs ES256, and returns the token in the `spacetime-identity-token` response header. So SpacetimeDB is *always* JWT-based; "no auth" just means the server is its own OIDC-less issuer.

### 1.4 Transport-level auth and ConnectionId

- Auth happens **at HTTP level, before the WebSocket upgrade** — `Authorization: Bearer` header or `?token=` query param (`SpacetimeCreds::from_request_parts`), enforced by axum extractors (`SpacetimeAuthRequired`) and `anon_auth_middleware`. There is no in-band `Authenticate` message like Fluxum's SPEC-006.
- `ConnectionId` is a `u128` newtype (`crates/lib/src/connection_id.rs`); generated per WebSocket as `ConnectionId::from_le_byte_array(rand::random())` (`crates/client-api/src/routes/subscribe.rs:111`). `ConnectionId::ZERO` is reserved (rejected with 400; used internally to mean "no connection", e.g. scheduled calls).
- The pair `(Identity, ConnectionId)` is persisted in the **`st_client` system table** (`crates/datastore/src/system_tables.rs`, unique index on `(identity, connection_id)`) — this is what makes disconnect delivery crash-safe (§4).

### 1.5 SQL authorization: `AuthCtx` and permissions

`crates/lib/src/identity.rs` defines `SqlPermission { Read(StAccess), Write, ExceedRowLimit, BypassRLS }` and the `SqlAuthorization` trait object (`SqlPermissions`). The default (`owner_permissions`) grants everything iff `caller == database owner`. The `Authorization::authorize_sql` hook (`crates/client-api/src/lib.rs:615`) lets editions attenuate this; standalone just returns `AuthCtx::new(database.owner_identity, subject)`. So "server bypass" in SpacetimeDB is really **database-owner bypass**, evaluated per SQL/subscription request.

---

## 2. Row-level security (`#[client_visibility_filter]`)

### 2.1 Filters are SQL fragments, stored in a system table

A module declares a filter as a `const` of type `Filter`:

```rust
#[client_visibility_filter]
const ACCOUNT_FILTER: Filter = Filter::Sql(
    "SELECT * FROM account WHERE account.identity = :sender"
);
```

Filters travel in `RawModuleDef` and land in the **`st_row_level_security` system table** (`TableId(10)`, columns essentially `(table_id, sql)`, unique on `sql` — `crates/datastore/src/system_tables.rs`). RLS is a property of the *database schema*, not of the connection. The bindings docs still mark the feature "unimplemented/unstable" in places, but the engine path is live.

### 2.2 Compile at publish, expand as views at query time

- **Publish-time validation**: `RowLevelExpr::build_row_level_expr` (`crates/engine/src/sql/rls.rs`) compiles each filter's SQL against the schema when the module is published; malformed SQL or type errors **fail the publish**, not the query.
- **Query-time semantics** (`crates/expr/src/rls.rs`): RLS is implemented as **view expansion**. When a non-owner runs a subscription or one-off `SELECT` on a table with rules, the planner substitutes the table with the union of its rule fragments:
  - multiple rules on one table ⇒ **UNION** (OR semantics, one plan fragment per rule);
  - rules may **join other tables**, whose own RLS rules are recursively expanded — with **cycle detection** (cyclic rule dependencies are an error);
  - a rule's self-reference to its own table is not re-expanded (fixed point);
  - the `:sender` parameter is bound to the **caller identity at execution time**; subscription `QueryHash` incorporates the caller when the query is parameterized, so plans are cached per-identity where needed.
- **Where it applies**: subscription queries and one-off SQL (`crates/core/src/sql`, `crates/core/src/subscription`) — i.e., everything a client can read. It applies **only to public tables** (private tables are invisible to non-owners regardless).
- **Where it does NOT apply**: **reducers bypass RLS entirely** — module code sees all rows. RLS is a read-visibility mechanism, not a write guard.
- **Bypass**: `AuthCtx::bypass_rls()` → `SqlPermission::BypassRLS`; granted to the database owner under the default permission function. Incremental subscription evaluation operates on the expanded plan, so RLS deltas are computed with the same incremental machinery as ordinary queries.

---

## 3. Scheduler: scheduled reducers are rows in scheduled *tables*

### 3.1 Data model

There is no single `__schedule__` table; instead **any table can be a schedule**:

```rust
#[spacetimedb::table(name = send_message_schedule, scheduled(send_message))]
struct SendMessageSchedule {
    #[primary_key] #[auto_inc]
    scheduled_id: u64,
    scheduled_at: ScheduleAt,   // enum: Interval(TimeDuration) | Time(Timestamp)
    // ... arbitrary user columns = the reducer's argument payload
}
```

`ScheduleAt` lives in `crates/lib/src/scheduler.rs`. The scheduled reducer receives **the row itself** as its argument — the schedule *is* the message, transactional with everything else: insert a row → scheduled; delete the row → cancelled; the insert rolls back → never fires.

### 3.2 Timer implementation and correctness

Core scheduler: `crates/core/src/host/scheduler.rs`, a `SchedulerActor` around a **`tokio_util::time::DelayQueue`**.

- **Enqueue hook**: inserts into scheduled tables are intercepted on the datastore insert path (`instance_env` `schedule_row`), sending `(table_id, schedule_id, at)` to the actor.
- **Rollback safety at fire time**: since the hook fires during the transaction, the actor **re-reads the committed row when the timer pops**; if the row is absent (tx rolled back, or deleted since), the firing is a no-op. Cancellation therefore needs no unhook — the row is the source of truth.
- **Restart / catch-up**: on module start the scheduler **rescans all scheduled tables** and re-enqueues every row (draining the channel first; duplicate ids are an error). **Past-due entries fire once, ASAP** — no backfill of missed interval occurrences.
- **Anti-drift**: interval rows are rescheduled based on the **intended tick time**, not on when the handler actually ran — same fixed-timestep, no-accumulation philosophy as Fluxum's RED-020, but per-row.
- **Delivery semantics**: reducers are **at-least-once** — the row is deleted *after* a successful run (a `NoSuchModule` failure leaves the row for restart re-scan); scheduled *procedures* are at-most-once (row deleted *before* the run).
- **Limits**: max delay ≈ 2.17 years (`DelayQueue` limitation).
- **Caller identity**: scheduled calls execute as the **database identity** with `ConnectionId::ZERO`. Caveat carried in their docs: scheduled reducers remain client-callable unless the module explicitly checks `ctx.sender` against the module identity.

---

## 4. Reducer lifecycle hooks

Lifecycle reducers are tagged via a `Lifecycle` enum (`Init`, `OnConnect`, `OnDisconnect`) in the module def; dispatch is in `crates/core/src/host/module_host.rs`.

- **`init`** — runs once on first publish (empty database), as the publisher's identity.
- **`client_connected`** — `ClientConnection::call_client_connected_maybe_reject` (`crates/core/src/client/client_connection.rs:826`): runs **in the same transaction** as the `st_client` row insert; if the reducer returns `Err`, the WebSocket connection is **rejected** (`ClientConnectedError::Rejected(reason)`; also `OutOfEnergy`). Connection admission is thus programmable and atomic with presence bookkeeping.
- **`client_disconnected`** — `call_identity_disconnected_inner` (`module_host.rs:2058`): the reducer is invoked, but the `st_client` row is deleted **even if the reducer fails** (fallback transaction; "the database can't reject a disconnection"). On host restart, `st_client` is **scanned and disconnect reducers are fired for every client that was connected at crash time** — guaranteed eventual disconnect delivery, with possible (accepted, idempotent-by-commitlog) re-invocation if the host crashes between the reducer commit and the cleanup.

---

## 5. Energy/budget and rate limiting

`crates/core/src/energy.rs`:

- `EnergyQuanta` is a `u128` balance; `FunctionBudget(u64)` maps **1:1 onto wasmtime fuel** (`consume_fuel(true)`); default budget ≈ 2×10⁹ fuel/s × 60 s per reducer call.
- **Fuel exhaustion = wasm trap** → `ReducerOutcome::BudgetExceeded` / `ClientConnectedError::OutOfEnergy`; the transaction is not committed. Epoch interruption is used only to *log* long-running reducers.
- The V8/JS host's gas metering is currently a **stubbed timeout** (disabled, "fake logic" in source).
- Standalone installs `NullEnergyMonitor` — no accounting, no billing; energy is effectively a cloud-billing + runaway-loop-kill mechanism, **not** an abuse throttle.
- **There is no per-caller rate limiting anywhere** in the codebase: no per-identity token buckets, no per-reducer call quotas, no 429 path. A hostile client can spam reducer calls limited only by transport backpressure.

## 6. `crates/guard`

Despite the security-sounding name, `spacetimedb-guard` is a **test harness**: it spawns real `spacetimedb standalone` server processes to run restart/persistence/e2e tests. Irrelevant to auth/RLS design; do not mirror it.

---

## What Fluxum will face

**1. SHA-256(token) identity vs OIDC (iss|sub) identity — what we lose and gain (SPEC-009).**
SpacetimeDB's claims-derived identity gives token-rotation-proof identity *by construction*: refresh, re-sign, change expiry — `(iss, sub)` and therefore identity is unchanged. Fluxum's `Identity = SHA-256(canonical_token)` pushes that burden onto the `AuthProvider` (`AuthClaims.canonical_token` must stay stable across refreshes — AUTH-002/AUTH-022). That is workable but fragile: every JWT provider must canonicalize to something claims-like anyway (e.g. `canonical_token = iss|sub`), which converges on SpacetimeDB's design with extra steps. Recommendation: keep the SHA-256 derivation, but make the built-in `jwt` provider canonicalize to `"{iss}|{sub}"` rather than raw token bytes — that yields OIDC-grade stability without importing JWKS/OIDC machinery. What we *gain* over SpacetimeDB: no dependency on outbound HTTPS (`.well-known` fetch), no async key cache, no "insecure_decode for key discovery" surface, and API-key/HMAC schemes that OIDC handles awkwardly. What we *lose*: interop with third-party IdPs out of the box (Fluxum's answer is a custom `AuthProvider` — AUTH-032), and self-validating identities — SpacetimeDB's `0xc200` prefix + checksum lets any component reject a corrupted identity offline; worth stealing as a cheap format upgrade. Also note their `hex_identity`-claim-must-match-computed check: if Fluxum ever migrates derivation schemes, an equivalent embedded-identity cross-check is the migration path.

**2. Auth transport.** SpacetimeDB authenticates at HTTP upgrade time (Bearer/`?token=`); Fluxum authenticates in-band (`Authenticate` first message, AUTH-020), which is the right call for raw TCP but means our WebSocket/HTTP path should also accept header-based auth eventually or SDKs will diverge from ecosystem norms. Their anonymous-identity allocation (server mints a UUID-subject token and returns it) is a nicer dev-mode UX than `none`-provider "any bytes"; consider it for Fluxum dev mode.

**3. RLS-as-SQL vs `#[visibility(owner_only)]` — the expressiveness gap is real but bounded (SPEC-005).**
SpacetimeDB filters are arbitrary SQL with joins, multiple OR'd rules per table, recursive expansion across tables with cycle detection, compiled at publish time and folded into the *same incremental subscription plans* as user queries. Fluxum's `owner_only(column)` covers the 80% case with O(1) per-row cost and trivially correct incremental deltas, but cannot express "visible to members of my group" (join-based visibility) — SpacetimeDB can, in one SQL line. Our escape hatch `#[visibility(custom(fn))]` (SUB-032, P2) closes the gap semantically but as an opaque Rust predicate it defeats index-based pruning and forces per-row evaluation on every fan-out; SpacetimeDB's declarative SQL keeps the filter inside the query optimizer. Two lessons to adopt: (a) validate visibility declarations at startup the way they validate at publish — fail fast, not per-query; (b) their rule: RLS applies only to client reads, **never** to reducer code — Fluxum should state this explicitly in SPEC-005 (currently implicit). Our server-peer bypass (`SERVER:` namespace, SUB-031) is more granular than their owner-only `BypassRLS`; their `SqlPermission` trait-object attenuation is a good shape if Fluxum ever needs per-connection permission policies beyond owner/server.

**4. Scheduled tables vs `__schedule__` + `#[tick]` (SPEC-004).**
The designs converge on the same correctness invariants, and SpacetimeDB validates ours: schedule state lives in a transactional, persisted table (rollback discards the schedule; crash recovery rescans and re-fires — our RED-021 acceptance criteria match their behavior exactly); past-due entries fire **once, ASAP, no backfill**; interval rescheduling is rebased on intended tick time (anti-drift), same as our fixed-timestep RED-020. Differences worth noting: (i) their "schedule row = typed reducer argument" is more ergonomic than our `args: Vec<u8>` MessagePack blob — typed schedule rows also make schedules queryable/cancellable by domain fields, and we lose that; consider allowing user-defined schedule tables in a later phase. (ii) At-least-once: they delete the row *after* execution, so a crash between commit and delete re-fires the reducer — our RED-021 ("row SHALL be removed" as part of the fired transaction) is actually *stronger* (exactly-once given the delete is in the same tx as the execution); keep that and document it as a deliberate improvement. (iii) Their pitfall to avoid: scheduled reducers are client-callable unless the module checks the caller. Fluxum should make `#[tick]`/scheduled functions non-callable via `ReducerCall` by default. (iv) They have no `#[tick]` equivalent — high-frequency loops are interval rows, paying table overhead per tick at 60 Hz; our dedicated in-memory tick scheduler is the better fit for Fluxum's realtime target. (v) Their scheduled calls run as the database identity with `ConnectionId::ZERO` — Fluxum should similarly define a reserved identity + nil ConnectionId for tick/schedule contexts (SPEC-004 currently leaves `ctx.identity` unspecified for scheduled runs).

**5. Energy vs `max_rate` — they solve different problems, and we're missing neither… but they're missing one.**
Energy is per-call *execution cost* metering (wasm fuel → trap → rollback): it kills infinite loops and enables cloud billing, but does nothing against 10,000 cheap calls/s from one user. Fluxum's `max_rate` token bucket (RED-050/051) is per-caller *admission control*: it stops spam before a transaction exists, but does nothing about one call that loops forever. SpacetimeDB has **no per-caller rate limiting at all** — `max_rate` is a genuine differentiator; keep it. What Fluxum lacks is the other half: since we run native Rust (no wasm fuel), a runaway reducer blocks a shard indefinitely. We cannot trap like wasmtime; the realistic options are a watchdog (epoch-style: log at N ms, as they do) plus a documented cooperative budget, and `shard_max_reducers_per_sec` (RED-052) as macro-level overload protection — which SpacetimeDB also lacks. Also adopt their connection-admission trick: our `#[fluxum::on_connect]` returning `Err` should reject the connection (SpacetimeDB semantics); SPEC-004 RED-011 currently only says "runs inside a transaction" and should specify rejection behavior, plus their `st_client`-scan-on-restart pattern so `on_disconnect` is guaranteed to eventually fire for clients connected at crash time — our `__session__` table (AUTH-050) is already the right substrate for exactly that.
