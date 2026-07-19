# 05 — A04:2025 Cryptographic Failures

At-rest and field-level cryptography are a genuine strength. The one real gap is
**transport**: nothing on the wire is encrypted.

---

## F-011 — No transport encryption (TLS); secrets and data travel in cleartext (MEDIUM)

**Evidence.** Neither `crates/fluxum-server/src/tcp.rs` nor `http.rs` terminates
TLS, and no `rustls`/`native-tls` dependency exists in the workspace. The
`Authenticate` handshake carries the raw token (JWT / opaque / server-peer
secret) over a plaintext socket; every subsequent row and reducer payload is
plaintext. The sidecar bearer token is likewise sent in the clear
(`plugin/sidecar.rs:38-49`).

**Impact.** Under the project's **direct-port-exposure** model (no mandatory
proxy), a network eavesdropper on any hop captures auth tokens (→ full identity
takeover, since identity = `SHA-256(canonical_token)` and tokens are bearer) and
all row data. TLS termination is only available if the operator *chooses* to put
a proxy in front — which contradicts the direct-exposure stance and is not
enforced.

**Confidence: High** (absence of TLS confirmed across transports and deps).

**Fix direction.** Add optional built-in TLS (`rustls`) on both listeners with a
config section (`server.tls.{cert,key}`), and refuse non-loopback `auth.provider`
material over plaintext when TLS is off (a guard analogous to the `none` loopback
guard). This directly supports the direct-exposure requirement.

---

## F-012 — Non-constant-time hex key parsing at config load (LOW)

**Evidence.** Hex key parsing/comparison in the crypto config path
(`from_hex`-style helpers) is not constant-time. This runs at config-load time on
operator-provided material, not on attacker-influenced input per request.

**Impact.** Negligible — no remote timing oracle. Recorded for completeness.

**Confidence: Medium.**

---

## Positives (A04) — modern, correctly-used primitives

- **At-rest** (`crates/fluxum-core/src/crypto.rs`): XChaCha20-Poly1305 with a
  192-bit **random** nonce per seal (deliberately random, because copy-on-write
  page rewrites make a counter nonce unsound), AAD binding page/artifact position
  so a sealed page can't be relocated (`crypto.rs:1-31`, `:128-145`). Key bytes
  are zeroized on drop (`:93-97`) and never rendered by `Debug` (`:99-104`,
  tested). A wrong/absent key is an authentication failure, never silent garbage
  (`:180-185`); lazy key rotation is supported.
- **Field-level** (`transform/crypto.rs`): ECIES over X25519 + HKDF-SHA-256 +
  XChaCha20-Poly1305, self-describing envelope, AAD binding `(table, column, pk)`
  so a valid ciphertext can't be moved between cells. Ed25519 signing with
  zeroized keys; AEAD key zeroized immediately after cipher construction
  (`transform/crypto.rs:222-223`, `:267-268`).
- **Token auth** uses HMAC-SHA256 with constant-time `verify_slice`
  (`auth/token.rs:63`) — no timing side channel on the hot auth path.
- **Integrity-before-decrypt**: CRC32C / content hash covers the ciphertext, so
  fault-in verifies integrity before any decrypt (SEC-011).

The cryptographic *engineering* here is above the bar for the ecosystem. The
remediation is to extend that same rigor to the transport layer (F-011).
