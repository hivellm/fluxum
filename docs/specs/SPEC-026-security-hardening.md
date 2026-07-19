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
proxy_preamble, proxy_header}` (SEC-032).

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

## 5. Non-goals

- Application-layer secrets management (module config injects keys via `FLUXUM_*`).
- Full WAF / L7 DDoS mitigation (deploy behind a proxy for that; this is basic in-process defense).
- Replacing column-level crypto (SPEC-017 remains the field-granularity mechanism).
