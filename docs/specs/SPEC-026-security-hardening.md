# SPEC-026 — Security Hardening (at-rest encryption, deterministic stdlib, abuse protection)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 2 (encryption at rest) · Phase 3 (deterministic stdlib) · Phase 5 (abuse protection) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-24, FR-70..FR-72, FR-111 (extends); new: FR-145 (encryption at rest), FR-146 (deterministic reducer stdlib), FR-147 (connection abuse protection) |
| **Requirement prefix** | `SEC-` |
| **Source** | New (Fluxum-native). Complements column-level crypto ([SPEC-017](SPEC-017-column-transforms.md)) with whole-store at-rest encryption, gives reducers a determinism-preserving stdlib (RNG/clock), and protects the pre-auth connection surface the per-`(Identity, reducer)` rate limiter cannot reach. |

Keywords are RFC 2119. Requirement IDs `SEC-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

Three hardening tracks: **encryption at rest** for cold pages, checkpoints, and backups under a
managed key; a **deterministic reducer stdlib** (seeded RNG + logical clock helpers) that preserves
replay/DST determinism where today ad-hoc `rand`/wall-clock would break it; and **connection-level
abuse protection** (per-IP connection caps, failed-auth throttling, handshake flood defense).

## 2. Encryption at rest (`SEC-01x`)

### Requirement: Encrypted cold storage & backups
- **SEC-010** [P1] When enabled, cold pages, checkpoints, and backups SHALL be encrypted with an AEAD
  (XChaCha20-Poly1305) under a key from config/KMS, as a stage in the `PageCodec`
  ([pager/codec.rs](../../crates/fluxum-core/src/store/pager/codec.rs)) and checkpoint/backup writers.
- **SEC-011** [P1] Page/segment integrity (existing CRC/hash) MUST be verified before decryption; a key
  mismatch aborts startup rather than serving garbage.
- **SEC-012** [P2] Key rotation SHALL re-encrypt lazily on page rewrite, with `previous` keys accepted
  for read during rotation.

#### Scenario: Stolen disk is opaque
Given at-rest encryption enabled
When the data directory is copied without the key
Then no row data can be recovered from cold pages, checkpoints, or backups.

#### Implementation status (phase 2 — complete)
- **Cipher/keyring** ([crypto.rs](../../crates/fluxum-core/src/crypto.rs)): XChaCha20-Poly1305 with a
  random 192-bit nonce per seal (copy-on-write page rewrites make a derived/counter nonce unsound, so
  random is the safe choice). The sealed envelope is `[24-byte nonce ++ ciphertext ++ 16-byte tag]`; no
  key id is stored — `Keyring::open` tries the active key then each `previous` key, and the Poly1305 tag
  authenticates the match (SEC-012). Key bytes are zeroized on drop and never rendered by `Debug`.
- **Cold pages** ([pager/codec.rs](../../crates/fluxum-core/src/store/pager/codec.rs)): `encode_for_storage`
  runs the AEAD **after** compression and sets a new `FLAG_ENCRYPTED` page-header bit; the CRC32C covers
  the ciphertext, so fault-in verifies integrity **before** `open_image` decrypts (SEC-011). The AEAD
  associated data binds `(shard, table, page id, flags)`, so a sealed page cannot be replayed at another
  position. Lazy rotation (SEC-012) is automatic: any page rewrite re-seals under the active key while
  reads still accept retired keys.
- **Checkpoint/backup artifacts**: `compress_artifact`/`decompress_artifact` seal after zstd compression
  behind a self-describing `FLXENC01` magic; checkpoint objects are content-addressed, so their hash
  verifies the ciphertext before decrypt (SEC-011). A wrong/absent key is an authentication failure, never
  silent garbage.
- **Config** ([config `storage.encryption`](../../crates/fluxum-core/src/config/mod.rs)): `enabled`,
  `active_key_id`, and a `keys` list (`id` + 64-hex `key_hex`); `EncryptionConfig::keyring()` rejects
  enabling with no keys or an `active_key_id` that names none (SEC-010). Config-embedded key material is
  the baseline; a KMS key reference is a future `source` extension. Disabled by default (fully opt-in).
- **Scope note**: the paged cold tier is not yet on the live `MemStore` write path, so encryption is
  exercised at the pager/codec and artifact layers and travels with the pager when it is wired in.

## 3. Deterministic reducer stdlib (`SEC-02x`)

### Requirement: Determinism-preserving helpers
- **SEC-020** [P1] `ReducerContext` SHALL expose `ctx.rand()` seeded deterministically from
  `(tx_id, shard_id)` and logical-time helpers derived from `ctx.timestamp`, so reducers can generate
  ids/rolls and time-bucket without breaking commit-log replay or DST.
- **SEC-021** [P1] Direct wall-clock / OS RNG use inside a reducer MUST be discouraged (lint/doc) as it
  breaks replay; the stdlib is the sanctioned path.

#### Scenario: Replayable random id
Given a reducer that assigns `ctx.rand()` as a token
When the commit log is replayed on recovery
Then every reducer produces the identical token it produced originally.

## 4. Connection abuse protection (`SEC-03x`)

### Requirement: Pre-auth surface defense
- **SEC-030** [P1] The transports SHALL enforce per-IP concurrent-connection caps and a connection
  accept rate limit, independent of the per-`(Identity, reducer)` limiter which only applies post-auth.
- **SEC-031** [P1] Repeated failed `Authenticate` attempts from an address SHALL be throttled with
  exponential backoff; handshake/`Authenticate` MUST have a bounded time and size budget to blunt slowloris.
- **SEC-032** [P2] Abuse events MUST surface as `fluxum_conn_rejected_total{reason}` metrics.

#### Scenario: Auth brute-force throttled
Given an address sending many bad tokens
When it exceeds the failed-auth threshold
Then further connection attempts from it are delayed/refused and counted, without affecting other clients.

#### Configuration & implementation

Both transports share one per-IP guard (`fluxum_server::connguard::ConnGuard`,
held on the `ShardContext`), gating at the accept path before a session
exists; the handshake time/size budget lives in the TCP read loop and the HTTP
pre-auth POST path. Every limit defaults permissively and is opt-out at `0`:

```yaml
server:
  connection_limits:
    max_conns_per_ip: 1024           # SEC-030 concurrent cap (0 = uncapped)
    accept_rate_per_sec: 512         # SEC-030 accept rate/sec + burst (0 = off)
    handshake_timeout_secs: 10       # SEC-031 slowloris budget (0 = off)
    handshake_max_bytes: 65536       # SEC-031 pre-auth frame cap (0 = max_frame_bytes)
    failed_auth_threshold: 10        # SEC-031 backoff after N bad tokens (0 = off)
    failed_auth_backoff_base_ms: 100 # doubles per failure past the threshold, capped
    failed_auth_backoff_max_ms: 30000
```

Rejections increment `fluxum_conn_rejected_total{shard, reason}` with
`reason ∈ {conn_cap, accept_rate, failed_auth, handshake_budget,
proxy_preamble, proxy_header, blocked, global_cap, overload}` (SEC-032).

### Requirement: IP blocklist / allowlist and global ceiling
- **SEC-033** [P1] The guard SHALL refuse (reason `blocked`) any resolved client IP that matches
  `server.connection_limits.blocklist` (IP/CIDR, IPv4+IPv6), or a runtime ban, or — when
  `server.connection_limits.allowlist` is non-empty — fails to match the allowlist (a non-empty
  allowlist is **exclusive**; the blocklist still wins over an allowlist hit). The check runs before
  any per-IP state is touched or allocated, so a flood of banned addresses cannot grow guard memory.
  Runtime bans are managed via the admin API — `POST /bans` (`{"entry", "ttl_secs"?}`), `DELETE
  /bans/{entry}`, `GET /bans` (static + runtime entries with remaining TTL) — are runtime state only
  (a restart clears them; the static list is the durable path), and a TTL ban readmits by itself on
  expiry.
- **SEC-034** [P1] `server.connection_limits.max_total_conns` (`0` = uncapped, the default) SHALL
  bound concurrent connections across **all** peers (reason `global_cap`), checked before per-IP
  state — the backstop a many-IP distributed flood cannot walk past. Lowering it at runtime never
  evicts live connections; it only gates new admissions.
- Both lists and the ceiling are validated at load and hot-reloadable (OPS-040); with everything at
  defaults the behavior is byte-identical to a guard without them.

#### Scenario: A banned address is refused on both transports
Given `10.9.9.9` runtime-banned via `POST /bans`
When it attempts a TCP connection and an HTTP request before authenticating
Then both are refused before any session work, counted as `blocked`, and `DELETE /bans/10.9.9.9`
readmits it immediately.

#### Scenario: A distributed flood hits the global ceiling
Given `max_total_conns: 1000` and a flood from thousands of distinct addresses
When connection 1001 arrives while 1000 are live
Then it is refused with `global_cap`, established connections are untouched, and slots freed by
disconnects readmit newcomers.

```yaml
server:
  connection_limits:
    blocklist: []                    # SEC-033 refused outright (IP/CIDR)
    allowlist: []                    # SEC-033 non-empty = only these connect
    max_total_conns: 0               # SEC-034 global ceiling (0 = uncapped)
    max_tracked_ips: 100000          # SEC-040 guard memory bound (0 = unbounded)
    overload_shed_fraction: 0.90     # SEC-041 shed pre-auth at this load (0 = off)
    overload_shed_all_fraction: 0.98 # SEC-041 shed all new at this load (0 = off)
  accept_backlog: 0                  # SEC-042 listen backlog (0 = built-in 1024)
  tcp_keepalive_secs: 0              # SEC-042 reap dead peers (0 = off)
  tcp_defer_accept_secs: 0           # SEC-042 Linux TCP_DEFER_ACCEPT (0 = off)
```

### Requirement: Overload resilience on a directly exposed port
- **SEC-040** [P1] Guard memory SHALL be bounded: `server.connection_limits.max_tracked_ips`
  (default 100000, `0` = unbounded) caps per-IP entries. At the cap, a pressure sweep reclaims
  entries holding no live connection, no counting failed-auth streak, and no pending backoff —
  what SEC-031 depends on is never reclaimed, so a distinct-IP flood cannot reset a brute-force
  counter. If nothing is reclaimable the newcomer is admitted *untracked* (global caps still
  apply) rather than let the defense become the OOM vector. Exposed as
  `fluxum_connguard_tracked_ips` (gauge) and `fluxum_connguard_evictions_total` (counter).
- **SEC-041** [P1] Admission control SHALL shed load before saturation: the load signal is the
  highest of `total conns / max_total_conns` and `tracked IPs / max_tracked_ips` (only configured
  caps contribute; heap is independently bounded by the SPEC-015 memory budget). At
  `overload_shed_fraction` (default 0.90) new **pre-auth** connections are shed (reason
  `overload`, zero response bytes); established authenticated sessions — including Streamable
  HTTP requests presenting a live `Fluxum-Session` — keep working. At
  `overload_shed_all_fraction` (default 0.98) all new connections are shed. The signal is
  instantaneous, so recovery is immediate when load drains. State is `fluxum_overload_state`
  (0/1/2) and every transition is logged. The admin surface (`/health`, `/metrics`, `/bans`)
  MUST never be gated by admission control.
- **SEC-042** [P2] Listener/socket hardening SHALL be configurable: `server.accept_backlog`
  (`0` = built-in 1024), `server.tcp_keepalive_secs` (`0` = off) applied to accepted sockets so
  dead peers stop holding slots, and `server.tcp_defer_accept_secs` (`0` = off; Linux
  `TCP_DEFER_ACCEPT`, ignored-and-logged elsewhere). The SEC-031 handshake budget and RPC-060
  idle timeout remain the pre-/post-auth read/idle knobs. All default to today's behavior.
- **SEC-043** [P2] Every pre-auth rejection (blocked, caps, rates, backoff, handshake budget,
  overload shed, spoofed preamble) SHALL close with zero response bytes and no per-connection
  allocation beyond what was already read. Documented exceptions, HTTP only: a malformed
  `X-Forwarded-For` from a *trusted* proxy answers `400` (a misconfiguration to surface, not an
  attack), and a pre-auth oversized POST body answers `413` (HTTP requires a status line; the
  response is one short head, no amplification).

#### Scenario: A distinct-IP flood cannot destabilize the process
Given a flood from tens of thousands of distinct addresses
When it exceeds `max_tracked_ips` and pushes load past the shed fractions
Then guard memory stays under the cap (evictions counted), new pre-auth connections are shed with
zero bytes, established clients' reducer calls and TxUpdates keep flowing, and the moment the
flood stops the next legitimate connection is admitted with no cool-down.

### Requirement: Trusted-proxy client-IP resolution
- **SEC-035** [P1] When `server.trusted_proxies` (IP/CIDR list, IPv4+IPv6, default empty = off) names
  the socket peer, the transports SHALL resolve the effective client IP from the peer's forwarding
  metadata and key every per-IP defense (SEC-030/031 caps, backoff, bans) on that resolved IP. On HTTP
  the resolution is `X-Forwarded-For` under the rightmost-untrusted rule (walk right to left, skip
  trusted hops; the first untrusted address is the client; an all-trusted chain falls back to its
  leftmost entry). Forwarding metadata from a peer NOT in the list MUST be ignored (header) — never
  honored. A malformed `X-Forwarded-For` from a trusted proxy MUST reject the request (`400`), counted
  as `proxy_header`.
- **SEC-036** [P1] On TCP the resolution is the PROXY protocol v2 binary preamble, accepted only from
  trusted peers and read within the SEC-031 handshake time budget. A preamble from an untrusted peer,
  or a malformed one from a trusted peer, MUST close the connection with zero response bytes, counted
  as `proxy_preamble`. A trusted peer that opens with ordinary frames (no preamble) is the proxy host
  acting as its own client. `LOCAL` commands and `UNSPEC` families resolve to the peer itself. The v1
  text preamble is unsupported.
- **SEC-037** [P2] `server.trusted_proxies` SHALL be validated at load and hot-reloadable (OPS-040);
  a reload applies to connections admitted after it. With the list empty the transports MUST behave
  byte-identically to a build without proxy awareness.

#### Scenario: Per-IP caps bite the client, not the proxy
Given Fluxum behind a load balancer listed in `server.trusted_proxies`
When one client behind the proxy floods connections while another stays modest
Then the flooding client's resolved IP is throttled and the modest client keeps connecting, and the
proxy's own IP is never capped.

#### Scenario: Spoofed forwarding metadata is refused
Given a client NOT listed in `server.trusted_proxies`
When it sends an `X-Forwarded-For` header or opens with a PROXY v2 preamble
Then the header is ignored (the socket peer stays the client IP) and the preamble closes the
connection with nothing written, counted as `proxy_preamble`.

```yaml
server:
  trusted_proxies: []                # SEC-035 IP/CIDR entries; empty = socket peer is the client
```

### Requirement: Admin-surface access control
The HTTP admin API (`/reducer`, `/query`, `/query/explain`, `/schema`, `/view`, `/drain`,
`/config/reload`, `/plugins/*`, `/bans`, `/sessions`, `/audit`) shares `http_port` with `/rpc`.
On the direct-exposure posture (no mandatory proxy, no TLS) it MUST be safe by default: an
unauthenticated remote peer must not gain read/write/DoS over it.

- **SEC-054** [P0] The admin dispatch SHALL enforce, before any handler runs: (a) a **network
  gate** — loopback always passes; a non-loopback client is refused `403` (`untrusted_ip`) unless
  its SEC-035 resolved IP is in `server.admin.trusted` (IP/CIDR, default empty = loopback only);
  and (b) an **operator credential** — a request from a trusted *non-loopback* IP MUST present a
  valid `auth.server_peers` token (in the `Fluxum-Operator` header or a JSON `token` field) when
  `server.admin.require_operator` (default `true`), else `401` (`unauthenticated`). The token is
  compared digest-to-digest and never logged. `/health` and `/metrics` stay ungated when
  `server.admin.open_health_metrics` (default `true`) so scrapers and load balancers always reach
  them. Refusals increment `fluxum_admin_rejected_total{reason}`.
- **SEC-055** [P1] Admin reducer invocation (`POST /reducer/:name`) SHALL honor the same
  `client_callable` gating a client session does — a schedule-only reducer is refused `403` even
  for an operator (F-004). The `/blob` upload/download routes SHALL require an authenticated
  `Fluxum-Session` (F-002); the unauthenticated blob surface is closed.
- **SEC-056** [P2] `server.admin.*` SHALL be validated at load and hot-reloadable (OPS-040). With
  defaults, the admin surface behaves exactly as before for a loopback operator.

#### Scenario: A directly exposed node is safe by default
Given a Fluxum node on a public IP with default config
When a remote client calls `POST /reducer/:name` or `POST /query` with no credential
Then the request is refused `403` before any handler runs, no write happens and no RLS-bypassing
read is served, while the same call from loopback (the operator's own host) succeeds.

```yaml
server:
  admin:
    trusted: []                 # SEC-054 extra IP/CIDR ranges (beyond loopback); empty = loopback only
    require_operator: true      # SEC-054 remote gated routes need a server-peer token
    open_health_metrics: true   # SEC-054 keep /health and /metrics ungated for scrapers
```

### Requirement: Dependency supply-chain gate
- **SEC-057** [P1] The build SHALL enforce a dependency supply-chain gate (`cargo deny check`,
  policy in repo-root `deny.toml`) as part of the local quality suite (this project runs no
  GitHub Actions): a known RustSec advisory, a license outside the Apache-2.0-compatible
  allow-list, a banned/duplicate/yanked crate, or a dependency from a source other than crates.io
  MUST fail the gate. Wildcard versions are denied except intra-workspace `path` crates. Any
  accepted advisory exception MUST be recorded in `deny.toml`'s `[advisories].ignore` with a
  reason. A CycloneDX SBOM SHALL be generated on release for downstream auditing. The gate is run
  via `scripts/supply-chain-check.sh`, which pins the advisory database to an LF checkout so it is
  deterministic across platforms.

### Requirement: Config-secret hygiene
- **SEC-058** [P1] Every secret configuration field — `auth.secret`, `auth.server_peers[].token`,
  `encryption.keys[].key_hex`, `transforms.keys[].secret` (and `.previous`), and the sidecar
  `token` — SHALL be a `Secret<T>` (`fluxum_core::secret`): its `Debug` and `Serialize` outputs
  redact to `[redacted]` (never the bytes), it zeroizes on drop, and its plaintext is reachable
  only through an explicit `expose_secret()` at the point of use. A serialized or `Debug`-printed
  config therefore never leaks minting material (F-006). `Serialize` is deliberately lossy (the
  config is loaded once and never rebuilt by re-serializing a secret); the hot-reload path re-reads
  the file, and secrets are non-reloadable.

### Requirement: Transport TLS and no-cleartext-on-public-bind
- **SEC-059** [P1] The transports SHALL support optional built-in TLS termination (`rustls`) via
  `server.tls.{cert,key}` (PEM files): when set, a **directly** accepted FluxRPC/TCP or HTTP
  connection completes the TLS handshake before the first byte of the protocol is read. A
  trusted-proxy connection (SEC-035/036) stays plaintext — the proxy terminated TLS and forwards on
  a trusted link. The server SHALL **refuse to start** an *authenticating* listener (`token`/`jwt`
  provider) on a non-loopback bind without TLS unless `server.allow_plaintext` is explicitly set,
  since bearer tokens and row data would otherwise cross the public interface in cleartext (F-011).
  `cert` without `key` (or vice versa) is a load error. The transport-encryption posture is
  surfaced as a boolean in `GET /health` (`"tls": true|false`) and logged once per boot — never any
  key material. TLS on/off is byte-transparent to the protocol.

#### Scenario: A public authenticating bind demands TLS or an explicit opt-out
Given `auth.provider: token` and `server.tcp_host: 0.0.0.0` with no `server.tls`
When the server starts
Then it refuses with an error naming the offending listener, unless `server.tls.cert`/`key` are set
(handshake required before any frame) or `server.allow_plaintext: true` acknowledges a trusted link.

```yaml
server:
  tls:
    cert: /etc/fluxum/tls/cert.pem   # SEC-059 PEM chain; empty = TLS off
    key:  /etc/fluxum/tls/key.pem    # SEC-059 PEM key; required with cert
  allow_plaintext: false             # SEC-059 opt out of the public-bind TLS requirement
```

### Requirement: Fail-closed access-control residuals
- **SEC-060** [P1] Fail-closed row visibility (OWASP A01 F-003). A `#[visibility]` rule that is
  *declared but has no enforcement path* MUST be rejected at schema load, never served with no
  filter — a declared access control can never silently protect nothing. `shard_local` and
  `custom` are unimplemented and so are hard load errors; `owner_only` (per-row closure),
  `member_of` (subscription-manager membership index), and `public_all` (intentionally unfiltered)
  are the enforced set.
- **SEC-061** [P2] Asymmetric JWT (A07 F-019). `auth.jwt_algorithm` MAY select an asymmetric
  algorithm (`rs256`/`es256`/`ed25519`); the provider is then **verify-only** — it holds only the
  public key (`auth.jwt_public_key`), so a database compromise cannot mint tokens. `refresh`
  returns the presented token unchanged (a fresh token comes from the external issuer). Symmetric
  `hs256` (the default) stays available and is documented as lower-assurance (the DB holds the
  minting secret).
- **SEC-062** [P2] Permissive-auth identity bound (A07 F-020). The `none` provider caps the number
  of *distinct* identities it will admit at `auth.max_permissive_identities` (default 10000, `0` =
  unbounded): a never-seen identity past the cap is refused while an already-admitted one keeps
  working, so permissive dev auth cannot multiply identities without limit. (`none` is loopback-only
  regardless, AUTH-040.)
- **SEC-063** [P2] Sidecar channel integrity (A08 F-021). The Plugin RPC sidecar channel is
  plaintext and the sidecar's *responses* are unauthenticated (only Fluxum's Hello carries a
  token), so a non-loopback endpoint is a response-injection surface. Until mTLS lands, a sidecar
  endpoint MUST be loopback — a non-loopback endpoint is a hard manifest-build error. A response
  that fails to decode is treated as a call failure (counted, feeds the PLG-031 breaker), never a
  trusted value.

```yaml
auth:
  jwt_algorithm: es256                 # SEC-061 asymmetric verify-only (hs256 = symmetric default)
  jwt_public_key: /etc/fluxum/jwt.pub  # SEC-061 required for an asymmetric algorithm
  max_permissive_identities: 10000     # SEC-062 distinct-identity cap for provider `none` (0 = off)
```

## 5. Query and reducer execution bounds (`SEC-045..048`, availability)

A shard has a **single writer** and a shared snapshot evaluator, so one caller who can make
either run unboundedly stalls every tenant on the shard (OWASP A06/A10, findings F-014..F-017).
Connection-level admission (SEC-03x/04x above) guards *connections*; these requirements bound
*execution* — what an already-admitted call may cost. Every bound defaults **generously** (a
legitimate workload never notices) and every breach is a typed, counted error, never a silent
truncation.

### Requirement: Query execution bounds
- **SEC-045** [P1] The snapshot evaluator (InitialData, one-off reads, `POST /query`) SHALL
  enforce configurable bounds per query: `query.default_limit` (applied to queries carrying no
  `LIMIT`; `0` = none, the default), `query.max_limit` (default 1,000,000; an effective `LIMIT`
  above it is **clamped** or, with `query.max_limit_action: reject`, refused with `3030
  SQL_LIMIT_REJECTED`), `query.row_scan_budget` (default 10,000,000 rows a single evaluation may
  touch; breach aborts with `3031 SQL_SCAN_BUDGET_EXCEEDED`), and `query.deadline_ms` (default
  5,000 ms wall clock, polled during the scan and after the sort/rank phase; breach aborts with
  `3032 SQL_DEADLINE_EXCEEDED`). The effective (clamped/default) `LIMIT` MUST also drive the
  QP-021 index-ordered early stop. A rejected/aborted `Subscribe` registers nothing. Aborts are
  counted in `fluxum_query_aborted_total{reason ∈ limit, scan_budget, deadline}`. All keys are
  hot-reloadable (OPS-040) through one shared handle covering every hosted database.

### Requirement: Reducer execution bounds
- **SEC-046** [P1] A client reducer call SHALL run under a **cooperative execution deadline**
  (`reducer.max_execution_ms`, default 10,000; `0` = off) polled at every host-call boundary
  (the `ctx.tx` surface all table access goes through — a loop that never calls the host is made
  useless by the SEC-021 determinism rules), and a **per-transaction write ceiling**
  (`reducer.max_tx_bytes`, default 512 MiB; `0` = off) charged on every insert/upsert's stored
  size. A breach aborts with `5007 REDUCER_DEADLINE_EXCEEDED` / `5008
  REDUCER_TX_BUDGET_EXCEEDED` and rolls the transaction back through the existing panic→rollback
  isolation path; the breach is **latched**, so a reducer that catches the typed error and
  returns `Ok` still rolls back. Lifecycle hooks and scheduled executions (server identity) are
  exempt. Aborts are counted in `fluxum_reducer_aborted_total{reason ∈ deadline, alloc}`. The
  RED-052 global shard guard is **mandatory-on**: `reducer.shard_max_reducers_per_sec: 0` is
  rejected at config load and reload — a single-writer shard must always carry an aggregate
  admission bound.

### Requirement: Query admission rates
- **SEC-047** [P1] Subscription registration and one-off queries SHALL pass a token-bucket
  admission check **before any snapshot is touched**: a per-**identity** bucket
  (`query.max_queries_per_sec_per_identity`, default 500) and a secondary per-**source** bucket
  (`query.max_queries_per_sec_per_source`, default 2,000) keyed on the SEC-035 resolved client
  IP (falling back to the connection id where none exists) — so rotating tokens mints fresh
  identities but still drains one source budget. A batch `Subscribe` charges one token per
  query. Server peers (AUTH-062) are exempt. A refusal is the retryable `6003
  SUB_QUERY_RATE_LIMITED` carrying a `retry_after_ms` estimate, counted in
  `fluxum_query_rate_limited_total{bucket ∈ identity, source}`. Tracked buckets are capped
  (100,000 per keyspace) with idle-entry reclamation; at the cap a brand-new key is refused
  (fail closed) rather than growing without bound. Rates are hot-reloadable and a retune never
  hands out a free burst (OPS-040).

### Requirement: Idempotency-key bound
- **SEC-048** [P2] `idempotency_key` SHALL be capped at 256 bytes, enforced at admission before
  the dedup table or any transaction is touched (F-017): an over-length key is refused with
  `5003 REDUCER_BAD_ARGS`. A key exactly at the cap is admitted.

#### Scenario: A hostile query cannot pin the evaluator
Given `query.row_scan_budget: 100000` and `query.deadline_ms: 5000`
When a caller submits an unfiltered query over a hundred-million-row table
Then the evaluation aborts with `3031` (or `3032` if the clock fires first), the abort is
counted, other tenants' queries and the write path proceed unaffected, and a later query inside
the budget serves normally.

#### Scenario: A runaway reducer cannot stall the writer
Given `reducer.max_execution_ms: 10000`
When a reducer spins past the deadline and then touches `ctx.tx`
Then the call aborts with `5007`, the transaction rolls back with no commit-log entry and no
subscription events, `fluxum_reducer_aborted_total{reason="deadline"}` increments, and the shard
keeps serving.

```yaml
query:
  default_limit: 0                     # SEC-045 implicit LIMIT for unlimited queries (0 = none)
  max_limit: 1000000                   # SEC-045 ceiling on any effective LIMIT (0 = unbounded)
  max_limit_action: clamp              # SEC-045 clamp | reject (reject answers 3030)
  row_scan_budget: 10000000            # SEC-045 rows one evaluation may touch (0 = off)
  deadline_ms: 5000                    # SEC-045 per-query wall clock (0 = off)
  max_queries_per_sec_per_identity: 500   # SEC-047 per-caller admission (0 = off)
  max_queries_per_sec_per_source: 2000    # SEC-047 per resolved-IP/connection (0 = off)
reducer:
  shard_max_reducers_per_sec: 200000   # RED-052; SEC-046: mandatory-on, 0 is rejected
  max_execution_ms: 10000              # SEC-046 cooperative deadline (0 = off)
  max_tx_bytes: 512MiB                 # SEC-046 per-transaction write ceiling (0 = off)
```

## 6. Non-goals

- Application-layer secrets management (module config injects keys via `FLUXUM_*`).
- **Volumetric (link-saturation) DDoS absorption.** Fluxum's deployment posture is a *directly
  exposed port* — no proxy or CDN is assumed in front — so everything below link saturation is
  in scope and handled in-process (SEC-030..043: caps, bans, bounded guard memory, admission
  control, cheap rejects, socket hardening; see `docs/DEPLOYMENT-HARDENING.md` for the OS
  baseline). What remains out of scope is traffic that saturates the network link itself: no
  userspace process can absorb a full NIC, and that fight belongs to the hosting provider or an
  upstream scrubbing/anycast layer. The former phrasing ("deploy behind a proxy for DDoS") is
  superseded.
- WAF-style L7 payload inspection (request-content heuristics, bot fingerprinting).
- Replacing column-level crypto (SPEC-017 remains the field-granularity mechanism).
