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

## 5. Non-goals

- Application-layer secrets management (module config injects keys via `FLUXUM_*`).
- Full WAF / L7 DDoS mitigation (deploy behind a proxy for that; this is basic in-process defense).
- Replacing column-level crypto (SPEC-017 remains the field-granularity mechanism).
