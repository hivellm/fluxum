# 04 — A03:2025 Software Supply Chain Failures & A08:2025 Data Integrity

Two 2025 categories that share a root theme for Fluxum: **what does the build
trust, and what does the running system trust as input from external code?**

---

## F-009 — No dependency advisory scanning, license policy, or SBOM in CI (HIGH)

**Evidence.** The workspace has **no** `deny.toml`, no `.cargo/audit.toml`, and
no `cargo-audit`/`cargo-deny` invocation in CI. The workflows are limited to
`codespell`, `rust-lint`, `rust-test`, `simd-matrix` — a grep for
`audit|cargo-deny|trivy|semgrep|snyk` across `.github/workflows/` returns
nothing. No SBOM is produced.

The dependency surface *is* security-relevant and current — `jsonwebtoken = "9"`,
`chacha20poly1305 = "0.10"`, `sha2 = "0.10"`, `hmac`/`hkdf = "0.12"`,
`ed25519-dalek`/`x25519-dalek = "2"`, `zeroize = "1"` — which makes the absence of
an advisory gate the gap, not the crate choices.

**Impact.** A newly disclosed RustSec advisory in any transitive dependency
(notably the crypto stack) would not be flagged by CI. No license-compliance or
yanked-crate gate. This is precisely the A03 "supply chain failures" case.

**Confidence: High.**

**Fix direction.** Add `cargo-deny` (advisories + bans + licenses + sources) as a
CI gate with a committed `deny.toml`, and generate a CycloneDX SBOM on release.
This is compatible with the "no GitHub Actions quota" constraint
([memory: no-github-actions-for-now]) because `cargo deny check` runs in the
local gate alongside clippy/coverage.

---

## F-010 — First-party registry dependency provenance / lockfile pinning (LOW)

**Evidence.** `thunder-rpc = "0.2.0"` (HiveLLM's binary RPC wire) is pulled as a
versioned registry crate. Its provenance and pin policy should be part of the
supply-chain posture — a first-party crate published to a registry is still a
supply-chain input.

**Impact.** Low today; matters once `deny.toml` `[sources]` allow-lists are set
(a private/first-party crate needs an explicit allowed source).

**Confidence: Medium.**

---

## F-021 — Sidecar plugin responses are unauthenticated, untrusted MessagePack (MEDIUM)

**Evidence.** `crates/fluxum-core/src/plugin/sidecar.rs` authenticates only
host→sidecar via a **shared bearer token sent in the clear** in the `Hello` frame
(~`:38-49`, `:475-480`); the sidecar's *response* is not authenticated and is
`rmp_serde`-decoded (~`:529`) — untrusted MessagePack deserialization. The module
doc concedes it relies on "loopback or mTLS" as a deployment concern.

**Impact.** On an untrusted network the token is sniffable and responses are
spoofable; a MITM can influence ReadPath **ranking** (A08 data-integrity). Blast
radius is bounded: the proxy is granted no identity, and its outputs still pass
the caller's ordinary RLS/visibility filters (`sidecar.rs:46-49`) — it cannot
mutate stored state or bypass RLS. Mitigated further by a per-call deadline +
circuit breaker.

**Confidence: Medium.**

**Fix direction.** Document mTLS/loopback as a hard requirement (not a
suggestion) for sidecar transport, and treat decode failures as breaker trips
(already partially done).

---

## Positives (A03/A08) — the integrity posture is otherwise strong

- **No dynamic code loading.** In-process plugins are compile-time, feature-gated
  Rust registered via `inventory` (`plugin/mod.rs:355-374`) — no `dlopen`, no
  `.so`/`.dll` loading, so there is no runtime code-injection / load-time
  supply-chain surface. The capability set is *closed* (`plugin/mod.rs:76-98`);
  adding one is a spec change, not a config toggle.
- **Panic isolation with auto-disable.** Every plugin invocation runs under
  `catch_unwind`; a panic auto-disables the plugin and rolls back the enclosing
  transaction (`plugin/mod.rs:412-435`).
- **JWT algorithm pinning** to HS256 with `exp` required (`auth/jwt.rs:51-52`,
  `:26`) — no `alg:none` / algorithm-confusion integrity attack.
- Workspace lints deny `unwrap_used`, `expect_used`, and
  `undocumented_unsafe_blocks`, reducing panic/unsafe integrity risk at the
  source.
