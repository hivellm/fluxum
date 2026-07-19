# 01 — Scope, deployment model, and OWASP 2025 mapping

## Scope

This analysis maps the **OWASP Top 10:2025** (final list, released January 2026)
to the Fluxum codebase and proposes concrete hardening work. It covers the
network-facing server (`fluxum-server`), the core engine (`fluxum-core`: auth,
SQL/query, reducers, subscriptions/RLS, crypto, plugins), configuration, and the
supply chain. It does **not** cover the SDKs, the browser demo, or benchmarks.

Evidence is cited as `path:line`. Findings are numbered globally F-001..F-023
across files 02–09; the phased remediation is in `10-execution-plan.md`.

## Deployment / trust model (as the code assumes it)

Two design decisions shape every finding below:

1. **Ports are exposed directly.** Per the project's own stance
   ([memory: Fluxum direct-port exposure], and SPEC-026 §5 being rewritten),
   there is *no mandatory reverse proxy* in front. In-process DDoS/abuse
   resilience is a stated requirement, not an operator's problem.
2. **No TLS exists in the codebase.** Neither `tcp.rs` nor `http.rs` terminates
   TLS (no `rustls`/`native-tls` dependency in the workspace). The trusted-proxy
   client-IP machinery (`clientip.rs`, `net.rs`) implies an *optional* TLS-
   terminating proxy, but nothing requires or provides transport encryption.

The combination — "expose ports directly" **and** "there is no TLS and the admin
surface is unauthenticated" — is the tension this analysis surfaces. Several
controls that would be acceptable *behind a trusted proxy on a private network*
become high-severity when the documented deployment model is direct exposure.

### Trust boundaries

| Boundary | Where enforced | Notes |
|---|---|---|
| Unauthenticated network → session | `connguard.rs`, handshake budgets | pre-auth per-IP defenses (good) |
| Client identity → data (RLS/columns) | `subscription/mod.rs`, `transform/mask.rs` | centralized; partial gaps (F-003) |
| Client → reducer (writes) | `reducer/engine.rs` + rate limiter | typed args; no time/mem bound (F-015) |
| Operator → admin API | `admin.rs` | **effectively none** (F-001) |
| Host → sidecar plugin | `plugin/sidecar.rs` | bearer token in clear (F-011/F-021) |

## OWASP Top 10:2025 → Fluxum applicability

| # | Category | Applicability to Fluxum | Verdict | File |
|---|---|---|---|---|
| A01 | Broken Access Control (now absorbs SSRF) | **Very high** — unauthenticated admin API is a full RLS bypass | Action required | 02 |
| A02 | Security Misconfiguration (▲ from A05) | **High** — insecure-by-exposure defaults, admin on `0.0.0.0`, config secrets serializable | Action required | 03 |
| A03 | Software Supply Chain Failures (new/expanded) | **High** — no `cargo-audit`/`cargo-deny`/SBOM/CI advisory gate | Action required | 04 |
| A04 | Cryptographic Failures (▼ from A02) | **Medium** — at-rest/field crypto is strong; **no transport TLS** | Partial | 05 |
| A05 | Injection (▼ from A03) | **Low** — closed grammar, typed literals, no string interpolation | Strong already | 06 |
| A06 | Insecure Design (▼ from A04) | **High** — no query/reducer resource ceilings; single-writer DoS | Action required | 07 |
| A07 | Authentication Failures | **Medium** — solid providers; per-IP (not global) backoff, symmetric JWT | Partial | 08 |
| A08 | Software & Data Integrity Failures | **Medium** — sidecar responses unauthenticated/untrusted-deserialized | Partial | 04 |
| A09 | Security Logging & Alerting Failures (renamed) | **Medium** — no durable security-event trail; denials logged at `debug` | Action required | 09 |
| A10 | Mishandling of Exceptional Conditions (new) | **Medium** — panics well-contained; but unbounded work = exceptional-condition DoS | Partial | 07 |

Positive baseline worth recording up front: **injection resistance (A05),
at-rest/field cryptography (A04), reducer panic isolation (A10), and the pre-auth
connection guard (A07)** are already well-engineered. The gaps cluster in access
control (A01/A02), supply-chain tooling (A03), and resource-exhaustion design
(A06/A10).

Sources for the 2025 list: [OWASP Top 10:2025](https://owasp.org/Top10/2025/),
[Introduction](https://owasp.org/Top10/2025/0x00_2025-Introduction/).
