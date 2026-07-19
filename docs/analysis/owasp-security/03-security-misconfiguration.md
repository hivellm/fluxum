# 03 — A02:2025 Security Misconfiguration

Moved up three places in 2025. The theme: defaults and surfaces that are safe
*only* behind assumptions the code does not enforce.

---

## F-005 — No loopback/network gate on the admin surface; binds `0.0.0.0` by default (HIGH)

**Evidence.** `crates/fluxum-server/src/boot.rs:150` binds to
`config.server.tcp_host`, which the module docs and defaults treat as commonly
`0.0.0.0`. The `none` *auth provider* has a loopback guard
(`crates/fluxum-core/src/auth/mod.rs:209-222`, `LOOPBACK_GUARD_ERROR`), but the
**admin API has no equivalent** — nothing restricts it to loopback or a trusted
CIDR, and it shares the port with `/rpc`.

**Impact.** The mitigation the admin module *documents* ("this surface is for
trusted operators", `admin.rs:19-20`) is purely operational and unenforced. This
is the misconfiguration that turns F-001 from "theoretical" into "default". A01
and A02 reinforce each other here.

**Confidence: High.**

**Fix direction.** Either (a) bind the admin routes to a separate
loopback-only listener, or (b) require a server-peer token on *all* mutating
admin routes (extend the `/audit` check in `admin.rs:894-902` to a shared
`require_operator()` guard in `dispatch`), or both. Prefer both.

---

## F-006 — Config carries plaintext secrets and derives `Serialize` (MEDIUM)

**Evidence.** `auth.secret`, `server_peers[].token`, `encryption.keys[].key_hex`,
`transforms.keys[].secret`, and the sidecar `token` are plain `String`/`Vec<u8>`
in the config structs (`crates/fluxum-core/src/config/mod.rs`, e.g. the auth
section ~`:421-433` and encryption/transform sections). `Config` derives
`Serialize`/`Debug` (~`:526-527`), and these fields are **not** zeroized the way
the parsed crypto keys are (`crypto.rs:93-97`).

Note on the `/health` render: `ctx.effective_config()` and
`reloadable_config()` (`crates/fluxum-server/src/lib.rs:341`, `:359`) serialize
only the *hardware-effective* view (HWA-013) and the *reloadable* values
(OPS-040) — **not** the secret-bearing sections — so `/health` does not currently
leak `auth.secret`. The risk is latent: any future diagnostic dump, error
context, or log line that serializes the whole `Config` would render secrets in
cleartext, because nothing at the type level prevents it.

**Impact.** A latent secret-disclosure footgun. Combined with F-001 (any code
path reachable unauthenticated) the blast radius is large.

**Confidence: Medium** (Serialize derive verified; no live leak found).

**Fix direction.** Wrap secret fields in a `Secret<T>` newtype whose `Debug`/
`Serialize` redact (like `crypto::Key` already does), and zeroize on drop.

---

## F-007 — Connection-limit defaults are permissive against a hostile internet (MEDIUM)

**Evidence.** `crates/fluxum-server/src/config/mod.rs:141-147` defaults:
`max_conns_per_ip = 1024`, `accept_rate_per_sec = 512` (burst 512),
`failed_auth_threshold = 10`. Every limit is opt-out at `0`. The guard is
per-IP only (`connguard.rs`).

**Impact.** For direct exposure, 1024 concurrent conns and 512 accepts/s *per IP*
is generous; a modest botnet, or many clients behind one non-declared NAT, is not
meaningfully bounded, and there is no *global* accept ceiling. See F-018 for the
per-IP-vs-global backoff gap.

**Confidence: High** (defaults read directly).

---

## F-008 — No transport encryption anywhere (cross-listed, see A04/F-011) (MEDIUM)

Both listeners are plaintext; tokens and row data travel in the clear absent an
external TLS proxy. Detailed under `05-cryptographic-failures.md` (F-011). Listed
here too because "ship plaintext with no config knob for TLS" is itself a
misconfiguration-class exposure under the direct-port-exposure model.

---

## Positives (A02)

- Insecure-default guardrails **do** exist where they were thought about:
  `token`/`jwt` providers refuse to boot without a non-empty `auth.secret`
  (`config/mod.rs` ~`:890-895`), unset `${VAR}` secrets become a typed error, and
  `deny_unknown_fields` catches config typos.
- The `none` auth provider is loopback-gated (`auth/mod.rs:209-222`).
- Config precedence (`FLUXUM_*` env > file > profile > default) is explicit and
  documented.
