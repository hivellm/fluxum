# 08 — A07:2025 Authentication Failures

The authentication *core* is solid — stable claims-based identity, non-forgeable
server namespace, constant-time token comparison, HS256 pinning. The gaps are at
the edges: brute-force accounting and the symmetric-secret trust model.

---

## F-018 — Failed-auth backoff is per-IP, not per-identity, with no global ceiling (MEDIUM)

**Evidence.** `crates/fluxum-server/src/connguard.rs:245-267` tracks failed-auth
counts and applies exponential backoff **keyed on peer IP** (threshold 10, base
100 ms, max 30 s; `config/mod.rs:145-147`). There is no per-*account* lockout and
no *global* failed-auth ceiling.

**Impact.** Two-sided:
- **Shared-IP DoS**: many legitimate clients behind one NAT that is *not* a
  declared trusted proxy share a single failure counter — one abuser trips
  backoff for all of them.
- **Distributed credential stuffing**: an attacker spread across many source IPs
  never trips per-IP backoff, and nothing counts failures globally, so a
  low-and-slow distributed guessing campaign is unthrottled.

**Confidence: High.**

**Fix direction.** Add a global failed-auth rate meter (alert + optional
tarpit) and, where an account concept exists, a per-canonical-identity failure
counter independent of source IP.

---

## F-019 — JWT is symmetric HS256; the issuer shares the DB's signing secret (LOW)

**Evidence.** `crates/fluxum-core/src/auth/jwt.rs:46-55` builds both encoding and
decoding keys from the same shared secret; validation is `Algorithm::HS256`
(`:52`). Any component that can *verify* tokens can also *mint* them.

**Impact.** No asymmetric-verification option (RS256/EdDSA) means the database
node holds a secret capable of forging any identity; a read of `auth.secret`
(see F-006, F-011) is total auth compromise. Acceptable for a shared-trust
deployment, limiting for zero-trust ones.

**Confidence: High** (pinning verified). Severity Low given the pinning is
otherwise correct (no `alg:none`, `exp` required, 60 s leeway).

**Fix direction.** Offer an asymmetric JWT provider variant (verify-only with a
public key) so the DB never holds minting capability.

---

## F-020 — Permissive-auth identity multiplication (LOW, compounds F-016)

**Evidence.** Under `auth.provider: none` every token string maps to a distinct
identity; the `token` and `none` providers always return empty `roles`
(`auth/token.rs:68`, `auth/none.rs:21`), so role-based grants are only meaningful
under the JWT provider. Combined with F-016, per-identity throttles are evadable
in dev/permissive modes.

**Impact.** Low in production (JWT/token with real secrets), but the `none`
provider's loopback guard is the *only* thing preventing this from reaching a
public listener.

**Confidence: High.**

---

## Positives (A07) — the parts that are done right

- **Stable, rotation-proof identity**: `Identity = SHA-256(iss|sub)` for JWT, so
  refreshed/re-signed tokens map to the same identity (`auth/jwt.rs:96-119`,
  tested extensively).
- **Non-forgeable server namespace**: client canonical tokens in the reserved
  `SERVER:` space are rejected (`auth/mod.rs:300-304`); server-peer tokens are
  compared digest-to-digest (`auth/mod.rs:138`, `:162-168`), never byte-by-byte,
  and raw tokens are not retained.
- **Constant-time** token verification (`auth/token.rs:63`), **`exp` required**
  and validated for JWT, and a **loopback guard** on the `none` provider
  (`auth/mod.rs:209-222`).
- **Pre-auth abuse defense** (connguard) already caps concurrent conns, accept
  rate, and enforces handshake time/size budgets — the failure-accounting gap
  (F-018) is the refinement, not a missing foundation.
