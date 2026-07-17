# SPEC-017 — Column Transforms, Field-Level Security & Type Normalization

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 1 (type surface) · Phase 3 (crypto) · Phase 4 (column security) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-32, FR-70..FR-73 (extends); new: FR-90 (column transforms), FR-91 (field-level security), FR-92 (native decimal/normalized types) |
| **Requirement prefix** | `CT-` |
| **Source** | New (Fluxum-native). Trust model and mechanics **copied from PostgreSQL**: `pgcrypto`, column-level `GRANT`, dynamic masking (PostgreSQL Anonymizer), and domain/typed columns (`numeric`, `timestamptz`, `citext`). |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`CT-xxx` are stable. Integers are little-endian unless stated otherwise. Priority tags: `[P0]`
MVP · `[P1]` competitive launch · `[P2]` post-launch.

## 1. Scope

Applications routinely wrap a database with a DTO layer that (a) **normalizes** values before
storage — money to fixed-point minor units, timestamps to a canonical UTC instant, strings to a
canonical Unicode/case form — and (b) **protects** sensitive fields — encrypting or signing them,
and exposing raw values only to authorized callers. Today Fluxum forces all of this into hand-written
reducer code: there is no per-column transform, `FluxType` is a closed universe, and row-level
security (`#[visibility]`) is a whole-row boolean keep/drop that never rewrites a column
([SPEC-001 §8](SPEC-001-data-model.md); `crates/fluxum-core/src/subscription/mod.rs:773`).

This spec introduces **column transforms** — declarative, per-column pipelines applied
**automatically on the write path and reversed on the read path** — and **field-level security** —
per-column authorization that decides whether a caller receives the raw value or a masked one. It
covers three families:

- **§4 Normalization / codec transforms** — DTO-in-schema: a first-class `Decimal` type plus
  `datetime` and `string` canonicalizers.
- **§5 Cryptographic transforms** — native elliptic-curve **encryption** (ECIES over X25519) and
  **signatures** (Ed25519) for sensitive fields such as votes.
- **§6 Field-level security** — column privileges + dynamic masking that gate raw exposure.

The design is **server-side** (the `pgcrypto` model): transforms run inside the runtime, which
holds key material; the client never performs the transform. It is **additive and freeze-safe** —
new attributes, new `FluxType` variants, and new `TableSchema` fields only. Anything touching the
FluxBIN wire (`Decimal`, ciphertext envelope) is Phase-1 work and **MUST land before the G5 wire
freeze** ([SPEC-006](SPEC-006-protocol-fluxrpc.md)).

### 1.1 PostgreSQL source mapping (normative intent)

Per the design decision to mirror PostgreSQL, each mechanism has a Postgres analog it is modeled on:

| Fluxum construct | PostgreSQL analog | Notes |
|---|---|---|
| `#[encrypted(ecies, key="…")]` | `pgcrypto` `pgp_pub_encrypt` / `pgp_sym_encrypt`; ciphertext stored as `bytea` | EC modernizes RSA/ElGamal; server holds keys |
| `#[signed(ed25519, by=…)]` | `pgcrypto` digest/sign helpers | integrity/authenticity of a field |
| `#[column_grant(select(col) = role)]` | `GRANT SELECT (col) ON t TO role` (column privileges) | who may read the raw value |
| dynamic masking (raw vs redacted) | PostgreSQL Anonymizer `SECURITY LABEL … MASKED WITH …` | authorized ⇒ raw, else masked |
| `FluxType::Decimal { scale }` | `numeric(precision, scale)` / `money` | exact fixed-point |
| `#[normalize(datetime)]` | `timestamptz` (stored UTC) | canonical instant |
| `#[normalize(string, …)]` | `citext` + `CHECK`/normalization | canonical Unicode/case |
| transparent decrypt for authorized reads | Transparent Data Encryption (TDE) | at-rest confidentiality |

## 2. Attribute surface

- **CT-001** [P0] `#[fluxum::table]` SHALL recognise the following new **per-field** attributes,
  each declaring one stage of a column's transform pipeline. Multiple attributes on one field
  compose top-to-bottom on write and bottom-to-top on read (CT-020):

  | Attribute | Family | Effect on write | Effect on read |
  |---|---|---|---|
  | `#[normalize(kind, …)]` | codec | canonicalize the value before encoding | (identity — value is already canonical) |
  | `#[encrypted(scheme, key = "…")]` | crypto | encrypt the encoded value | decrypt **iff** caller is authorized (§6), else mask |
  | `#[signed(scheme, by = …)]` | crypto | attach a signature computed at write time | verify; expose `verified: bool` to the read side |
  | `#[masked(strategy)]` | security | (identity) | replace with a masked value unless caller is authorized (§6) |

- **CT-002** [P0] Each transform-bearing column SHALL be reflected in `ColumnSchema` (CT-050) so the
  runtime, the FluxBIN codec, the read path, and `/schema` introspection all observe it. A column
  MAY carry at most one `#[encrypted]` and at most one `#[signed]` attribute (compile-time error on
  duplicates).

- **CT-003** [P0] The `#[fluxum::table]` proc-macro (`crates/fluxum-macros/src/table.rs`) SHALL
  parse the new attributes into the existing per-field `Column` model (currently
  `ident, ty, rename_from, default` at `table.rs:99-111`), extending it with an ordered
  `transforms: Vec<TransformSpec>`. Unknown transform kinds/schemes SHALL fail at compile time with
  a `trybuild` golden diagnostic.

```rust
use fluxum::{Identity, Decimal, Timestamp};

#[fluxum::table(public, primary_key(election, voter))]
#[visibility(owner_only(voter))]
pub struct Ballot {
    pub election: u64,
    pub voter: Identity,

    // Encrypted to the "votes" curve keypair; only the election authority role decrypts.
    #[encrypted(ecies, key = "votes")]
    #[column_grant(select = "election_authority")]
    pub choice: String,

    // Signed by the caller so tampering is detectable; stays in clear text.
    #[signed(ed25519, by = voter)]
    pub receipt: Vec<u8>,

    // Normalized: minor units, exact.
    #[normalize(money, scale = 2)]
    pub stake: Decimal,

    // Canonicalized to UTC microseconds at write time.
    #[normalize(datetime)]
    pub cast_at: Timestamp,
}
```

## 3. The transform pipeline

- **CT-010** [P0] A **column transform** SHALL implement the object-safe trait:

  ```rust
  pub trait ColumnTransform: Send + Sync {
      /// Applied on the write path, before the value is FluxBIN-encoded into the stored row.
      fn on_write(&self, value: FluxValue, ctx: &TransformCtx) -> Result<FluxValue, FluxumError>;

      /// Applied on the read path, before the row is sent to a client / returned to a reducer.
      /// `authorized` is the field-level security decision (§6); an unauthorized read MUST NOT
      /// receive the raw value.
      fn on_read(&self, stored: FluxValue, ctx: &TransformCtx, authorized: bool)
          -> Result<FluxValue, FluxumError>;

      /// Stable identifier surfaced in /schema and used by SDK codegen.
      fn descriptor(&self) -> TransformDescriptor;
  }

  pub struct TransformCtx<'a> {
      pub identity: &'a Identity,       // caller identity (server-peer & roles resolved here)
      pub roles: &'a [String],          // AuthClaims.roles (SPEC-009 AUTH-070)
      pub is_server_peer: bool,         // SPEC-009 AUTH-062 — bypasses field-level security
      pub keyring: &'a Keyring,         // named key material (§5.3)
      pub column: &'a ColumnSchema,
  }
  ```

- **CT-011** [P0] On **write**, after a reducer calls `insert`/`upsert` and before the row is
  encoded into `TxState`, the runtime SHALL apply each column's transforms in declared order
  (`normalize` → `encrypted` → `signed`). The **stored** row therefore already contains the
  normalized/ciphertext bytes; the commit log, cold pages, checkpoints, replication stream, and all
  indexes see only the transformed value. Write hooks into the transaction path
  (`crates/fluxum-core/src/store/tx.rs`).

- **CT-012** [P0] On **read**, wherever a row leaves the store toward a caller — subscription
  fan-out (`crates/fluxum-core/src/subscription/mod.rs`), one-off query, HTTP `/query`, and reducer
  `query_pk`/`scan` results returned across the wire — the runtime SHALL apply the inverse
  transforms in reverse order, passing the per-column `authorized` decision from §6. Reducer-internal
  reads (`ctx.tx.scan::<T>()` used for server logic) SHALL see the raw value **iff** the reducer
  runs under a server-peer or an authorized identity; otherwise the masked value.

- **CT-013** [P1] Transforms MUST NOT break existing invariants: a `#[primary_key]`,
  `#[unique]`, `#[index]`, `partition_by`, or `#[spatial]` column SHALL NOT carry an `#[encrypted]`
  transform (a non-deterministic ciphertext cannot be a stable key/index term). `#[normalize]` on a
  key/index column is permitted (it is deterministic) and the **normalized** value is what the index
  stores and matches on — mirroring how Postgres indexes a `citext`/`numeric` column. Violations
  SHALL be rejected at compile time where detectable, else at `ServerBuilder::build()`.

- **CT-014** [P0] Transforms SHALL be deterministic with respect to failure isolation: an
  `on_write` error SHALL fail the enclosing reducer transaction (full rollback, [SPEC-003](SPEC-003-transactions.md));
  an `on_read` error SHALL be surfaced as a masked value plus a `fluxum_transform_read_errors_total`
  metric increment, never as a partial/plaintext leak.

## 4. Normalization / codec transforms (DTO-in-schema)

### 4.1 First-class `Decimal` type

- **CT-020** [P0] `FluxType` SHALL gain a first-class variant `Decimal { scale: u8 }`, and the
  column-type universe SHALL accept a `fluxum::Decimal` newtype:

  ```rust
  // Exact fixed-point: value == unscaled * 10^-scale. Postgres numeric(38, scale) analog.
  pub struct Decimal { unscaled: i128, scale: u8 }
  ```

  - **Wire (FluxBIN):** `i128` LE `unscaled` (16 bytes) followed by `u8` `scale` (1 byte).
    This is a **new wire tag** and MUST be assigned before the G5 freeze ([SPEC-006](SPEC-006-protocol-fluxrpc.md)).
  - Arithmetic, comparison, and index ordering SHALL operate on the exact rational value; two
    `Decimal`s with different `scale` but equal value compare equal.
  - The five SDKs ([SPEC-011](SPEC-011-sdk-codegen.md)) SHALL map `Decimal` to their exact-decimal
    type (TS `bigint`+scale wrapper, Python `decimal.Decimal`, Go `apd`/shopspring, C# `decimal`,
    Rust `Decimal`).

- **CT-021** [P0] `#[normalize(money, scale = N)]` SHALL be shorthand for a `Decimal { scale: N }`
  column whose `on_write` accepts a decimal string, an integer of minor units, or a float and stores
  the exact `unscaled` at fixed `scale`, rejecting values that would lose precision. An optional
  `currency = "ISO4217"` argument SHALL be recorded in `ColumnSchema` for SDK/formatting use (the
  currency is metadata, not stored per row). Postgres analog: `numeric(_, N)` / `money`.

### 4.2 DateTime canonicalization

- **CT-022** [P0] `#[normalize(datetime)]` on a `Timestamp` column SHALL canonicalize any accepted
  input (RFC-3339 string with offset, epoch millis/micros, or `Timestamp`) to **UTC microseconds
  since the Unix epoch** on write, rejecting inputs without a determinable instant. An optional
  `assume_tz = "IANA"` argument SHALL supply the zone for zoneless inputs. Postgres analog:
  `timestamptz` (always stored UTC). The stored representation is unchanged (`Timestamp(i64)`), so
  this is purely a write-side normalizer.

### 4.3 String canonicalization

- **CT-023** [P0] `#[normalize(string, form = "nfc|nfkc", case = "fold|lower|none", trim = true|false)]`
  on a `String` column SHALL apply, on write, Unicode normalization (default `nfc`), optional
  case-folding, and optional trimming, storing the canonical form. When applied to a `#[unique]` or
  `#[index]` column, uniqueness/lookup operate on the canonical form (Postgres `citext` analog).
  Defaults: `form = "nfc"`, `case = "none"`, `trim = false`.

## 5. Cryptographic transforms

### 5.1 Encryption — ECIES over X25519

- **CT-030** [P0] `#[encrypted(ecies, key = "NAME")]` SHALL encrypt the FluxBIN-encoded field value
  on write using **ECIES**: an ephemeral X25519 key agreement against the named recipient public
  key, HKDF-SHA-256 key derivation, and **XChaCha20-Poly1305** AEAD. The stored value SHALL be a
  self-describing ciphertext envelope encoded as `Bytes`:

  ```
  envelope = version:u8 ‖ scheme:u8 ‖ ephemeral_pubkey:[u8;32] ‖ nonce:[u8;24] ‖ ciphertext‖tag
  ```

  The row's declared column type is preserved in `/schema` (the plaintext type), while the **stored**
  FluxType is `Bytes` carrying the envelope. Postgres analog: `pgp_pub_encrypt(...)` into `bytea`.

- **CT-031** [P0] Decryption SHALL occur only on the read path and only when §6 authorizes the
  caller for that column. An unauthorized read SHALL receive the masked value (CT-041), **never** the
  envelope bytes unless the column additionally grants ciphertext visibility
  (`#[masked(ciphertext)]`). Server peers (AUTH-062) are always authorized.

- **CT-032** [P1] AEAD associated data SHALL bind the ciphertext to `(table, column, primary_key)`
  so a ciphertext cannot be replayed into a different row/column. A decryption/AEAD failure SHALL be
  treated as an `on_read` error (CT-014), not a plaintext fallback.

### 5.2 Signatures — Ed25519

- **CT-033** [P0] `#[signed(ed25519, by = SOURCE)]` SHALL, on write, compute an Ed25519 signature
  over `(table, column, primary_key, field_bytes)` and store `field_bytes ‖ signature:[u8;64]`.
  `SOURCE` selects the signing key: `server` (the server key, §5.3), or a field name of type
  `Identity` whose per-identity signing key is used (`by = voter`). The field stays in clear text;
  only integrity/authenticity is added. Postgres analog: application-side signing with `pgcrypto`
  digest helpers.

- **CT-034** [P0] On read, the runtime SHALL verify the signature and strip it, exposing the raw
  field plus a sibling boolean `<field>_verified` in the row projection and in `/schema`. A failed
  verification SHALL set `<field>_verified = false` and increment
  `fluxum_signature_verify_failures_total`; it SHALL NOT drop the row.

### 5.3 Key management

- **CT-035** [P0] Key material SHALL be configured server-side and referenced by name — the
  `pgcrypto` model, never embedded in application code. `config.yml`:

  ```yaml
  transforms:
    keys:
      votes:                          # referenced by #[encrypted(ecies, key = "votes")]
        scheme: x25519
        secret: ${FLUXUM_VOTES_X25519_SK}   # 32-byte private key (base64); pubkey derived
      server_signing:
        scheme: ed25519
        secret: ${FLUXUM_SERVER_ED25519_SK}
  ```

  Keys SHALL be injected via `FLUXUM_*` environment variables (never committed). At
  `ServerBuilder::build()` the runtime SHALL fail startup if any `#[encrypted]`/`#[signed]`
  attribute references a key name absent from `transforms.keys`, or if a key's scheme mismatches the
  attribute (e.g. `ecies` requires `x25519`).

- **CT-036** [P1] The `Keyring` SHALL support **key rotation**: a key MAY declare
  `previous: [<secret>, …]` so existing ciphertext (whose envelope records the encrypting key id)
  still decrypts while new writes use the current key. Re-encryption of stored rows to a new key is
  an application concern (a reducer that reads and re-upserts), not automatic.

- **CT-037** [P2] Per-identity keys (`by = voter`) MAY be sourced from a pluggable
  `KeyProvider` trait (analogous to `AuthProvider`) so signing/decryption keys can live in an
  external KMS/HSM rather than `config.yml`.

### Implementation status (phase 3 — crypto complete; SDK sibling pending)
- **Executors** ([transform/crypto.rs](../../crates/fluxum-core/src/transform/crypto.rs),
  [transform/engine.rs](../../crates/fluxum-core/src/transform/engine.rs)): `#[encrypted(ecies)]`
  (ECIES over X25519 + HKDF-SHA-256 + XChaCha20-Poly1305, CT-030/031/032/036) and
  `#[signed(ed25519, by = server)]` (CT-033/034) execute. The `TransformEngine` compiles the
  link-time registry against the config keyring; `on_write_row` encrypts then signs and
  `on_read_row` verifies+strips then decrypts, both wired into the store write path and the reducer
  `TxHandle` read boundary. AEAD/signature context binds `(table, column, primary_key)`.
- **Config** ([config `transforms.keys`](../../crates/fluxum-core/src/config/mod.rs)): the
  implemented shape is a **list** — `keys: [{ id, scheme: x25519|ed25519, secret: <hex>, previous:
  [<hex>] }]` (rather than the illustrative map above). `#[encrypted(key = "…")]` resolves by id;
  `#[signed(by = server)]` uses the Ed25519 key with id `server`. `TransformEngine::build` aborts
  startup on a missing/scheme-mismatched key (CT-035); `previous` keys give rotation (CT-036).
- **Metrics**: `TransformEngine::verify_failures()` (CT-034) and `read_errors()` (CT-014) counters
  are maintained; exporting them as named Prometheus series rides the server metrics wiring.
- **Deferred**: the reducer-facing `<field>_verified` projection sibling (CT-034) rides the phase-4
  SDK codegen (§7 note); `#[signed(by = <Identity column>)]` per-identity keys are CT-037 [P2] via a
  future `KeyProvider`. `#[masked]`/`#[column_grant]` read-authorization resolution is phase-4 (§6).
  Client-facing reads keep ciphertext until those grants land; server-peer reducers are authorized
  today (CT-031).

## 6. Field-level security

- **CT-040** [P0] `VisibilityRule` (row-level, [SPEC-001 §8](SPEC-001-data-model.md)) is unchanged.
  This spec adds an **orthogonal, per-column** decision. A column MAY declare who is authorized to
  receive its **raw** value via `#[column_grant(select = "ROLE" | owner | server_peer | public)]`,
  modeled on PostgreSQL column privileges (`GRANT SELECT (col) ON t TO role`):

  | Grant | Authorized when |
  |---|---|
  | `public` | always (default for columns with no crypto/mask) |
  | `owner` | the caller's `Identity` equals the row's `#[visibility(owner_only(field))]` owner |
  | `"role"` | `ctx.roles` contains `role` (SPEC-009 AUTH-070) |
  | `server_peer` | `ctx.is_server_identity()` (AUTH-062) — server peers additionally bypass **all** column grants |

- **CT-041** [P0] When a caller is **not** authorized for a column, the runtime SHALL substitute a
  **masked** value on the read path per `#[masked(strategy)]` (default `null`):

  | Strategy | Masked value |
  |---|---|
  | `null` | `Option::None` (column projected as nullable to unauthorized readers) |
  | `redact` | type-appropriate zero/empty (`""`, `0`, empty `Bytes`) |
  | `ciphertext` | the raw stored envelope bytes (for `#[encrypted]` columns only) |
  | `hash` | `SHA-256` of the raw value (stable pseudonym; Anonymizer analog) |

  Masking SHALL be applied uniformly to `InitialData`, `TxUpdate` diffs, one-off queries, and HTTP
  reads — an unauthorized subscriber never receives the raw value in any frame. Postgres analog:
  dynamic masking (PostgreSQL Anonymizer `MASKED WITH`).

- **CT-042** [P1] Field-level authorization SHALL compose with row-level security: a row first
  passes/fails `#[visibility]` (whole-row), and surviving rows then have each column masked per
  CT-040/041. A change in a masked column MUST still produce a `TxUpdate` to authorized subscribers
  and MUST NOT leak (via diff presence or ordering) to unauthorized ones.

## 7. Schema, registry & introspection

- **CT-050** [P0] `ColumnSchema` SHALL be extended (additively) to carry the transform pipeline and
  security metadata:

  ```rust
  pub struct ColumnSchema {
      pub name: &'static str,
      pub ty: FluxType,                       // the PLAINTEXT/logical type (unchanged)
      pub stored_ty: FluxType,                // what is actually stored (e.g. Bytes for #[encrypted])
      pub transforms: &'static [TransformDescriptor],
      pub grant: ColumnGrant,                 // §6 authorization; Public unless declared
      pub mask: MaskStrategy,                  // §6 mask; Null unless declared
  }

  pub enum TransformDescriptor {
      Normalize(NormalizeKind),                // Money{scale,currency} | DateTime{assume_tz} | Str{form,case,trim}
      Encrypted { scheme: CryptoScheme, key: &'static str },
      Signed { scheme: CryptoScheme, by: SignSource },
  }
  ```

- **CT-051** [P0] `ServerBuilder::build()` SHALL validate: every referenced key exists with a
  matching scheme (CT-035); no `#[encrypted]` on key/index/partition/spatial columns (CT-013); at
  most one `#[encrypted]`/`#[signed]` per column (CT-002); `#[normalize(money)]` only on `Decimal`,
  `#[normalize(datetime)]` only on `Timestamp`, `#[normalize(string)]` only on `String`; a
  `#[column_grant(owner)]` requires the table to declare `#[visibility(owner_only(...))]`.

- **CT-052** [P0] `GET /schema` and `fluxum schema export` SHALL emit each column's logical type,
  stored type, transform descriptors, grant, and mask strategy (key **names** only — never key
  material). SDK codegen ([SPEC-011](SPEC-011-sdk-codegen.md)) SHALL render the logical type for
  application code and emit the `<field>_verified` sibling for `#[signed]` columns. The schema hash
  SHALL incorporate transform descriptors so drift is detectable.

## 8. Migration interaction

- **CT-060** [P1] Adding, removing, or changing a column transform SHALL be an auto-diffable schema
  change ([SPEC-010](SPEC-010-schema-migration.md)). Adding `#[encrypted]`/`#[signed]`/`#[normalize]`
  to an existing populated column SHALL require an explicit `#[fluxum::migration]` that backfills
  (reads raw, re-writes transformed) — the runtime SHALL NOT silently reinterpret existing
  unencrypted/unnormalized bytes. Removing `#[encrypted]` likewise requires a decrypt-and-rewrite
  migration. `__schema_meta__` SHALL record the transform descriptor set so a binary started against
  data written under a different transform set aborts with a descriptive error.

## Acceptance criteria

1. **Attribute surface (Phase 1):** `trybuild` golden tests cover every attribute in CT-001; invalid
   combinations — `#[encrypted]` on a `#[primary_key]`/`#[index]`/`partition_by`/`#[spatial]` column,
   two `#[encrypted]` on one column, `#[normalize(money)]` on a non-`Decimal` column, unknown
   scheme/kind — fail to compile (or abort `build()` where compile-time detection is impossible) with
   the specified diagnostics.
2. **Decimal round-trip & wire:** property tests encode/decode `FluxType::Decimal` byte-exactly per
   §4.1; equal values with differing `scale` compare and index equal; the new FluxBIN tag is assigned
   before G5 and consumed unchanged by SDK codegen golden tests.
3. **Normalizers:** `money` stores exact minor units and rejects precision loss; `datetime` stores
   the same UTC microsecond instant for equivalent zoned/epoch inputs; `string` makes a `#[unique]`
   column reject case/Unicode-variant duplicates.
4. **Encryption:** a value written to an `#[encrypted(ecies, key="votes")]` column is stored only as
   the envelope (no plaintext in commit log, cold pages, checkpoints, or indexes — verified by
   scanning the persisted bytes); an authorized reader (role/owner/server-peer) receives the exact
   plaintext; an unauthorized reader receives the masked value in `InitialData` and every `TxUpdate`;
   AEAD associated-data binding rejects a ciphertext relocated to another row/column.
5. **Signatures:** an `#[signed(ed25519, by=voter)]` field round-trips with `<field>_verified = true`;
   tampering with the stored bytes yields `verified = false` without dropping the row.
6. **Field-level security:** two clients subscribing to the same table each receive raw values only
   for columns they are granted, masked values otherwise, in both `InitialData` and diffs; a
   server-peer connection receives all raw values; a mutation to a masked column still delivers a
   `TxUpdate` to authorized subscribers and leaks nothing to unauthorized ones (joint test with
   [SPEC-005](SPEC-005-subscriptions.md)).
7. **Key management:** startup aborts when an attribute references a missing/mismatched key; a rotated
   key still decrypts ciphertext written under its predecessor; no key material appears in `/schema`.
8. **Migration safety:** starting a binary that adds `#[encrypted]` to a populated column without a
   backfill migration aborts via `__schema_meta__` mismatch with a descriptive error; the backfill
   migration transforms all existing rows.
9. **PostgreSQL parity (documentation):** the §1.1 mapping is reflected in the parity harness
   ([SPEC-013](SPEC-013-testing-conformance.md)) — the same encrypted-column + column-privilege
   scenario is expressed in Postgres (`pgcrypto` + column `GRANT`) and in Fluxum, and produces
   equivalent authorized/unauthorized read results.
