# OWASP Top 10:2025 security analysis — Fluxum

Maps the **OWASP Top 10:2025** (final list, January 2026) onto the Fluxum
codebase and proposes a phased hardening plan. Analysis date: 2026-07-19.
Evidence is cited as `path:line`; findings are numbered globally **F-001..F-023**.

## Executive summary

Fluxum's **cryptography (A04 at-rest/field), injection resistance (A05), reducer
panic isolation (A10), and pre-auth connection guard (A07)** are already
well-engineered — above the ecosystem bar. The risk concentrates in three places,
all sharpened by the project's stated **direct-port-exposure** model (no mandatory
proxy) combined with **no transport TLS anywhere in the codebase**:

1. **Broken Access Control (A01) — CRITICAL.** The HTTP admin surface is served on
   the same public `http_port` as `/rpc` with **no authentication and no loopback
   gate** for almost every route. `POST /reducer/:name` gives arbitrary writes and
   `POST /query` gives arbitrary reads **bypassing RLS**; `drain`/`config reload`/
   `plugin disable` give DoS and security-control disablement. Only `/audit`
   checks a credential. **F-001** is the single highest-severity finding.
2. **Availability / Insecure Design (A06/A10).** No query `LIMIT` ceiling or
   execution timeout, and no reducer time/memory bound — a single caller can stall
   a shard's single writer (**F-014/F-015**).
3. **Supply chain & logging (A03/A09).** No `cargo-audit`/`cargo-deny`/SBOM gate
   (**F-009**); no durable security-event trail — auth failures and RLS denials
   log at `debug`, invisible at the default level (**F-022**).

The remediation (`10-execution-plan.md`) front-loads the two P0 phases —
**authenticate/gate the admin surface** and **add TLS + secret hygiene** — that
make a directly exposed node defensible by default.

### Severity roll-up

| Severity | Findings |
|---|---|
| Critical | F-001 |
| High | F-002, F-005, F-009, F-014, F-015 |
| Medium | F-003, F-006, F-007, F-011, F-016, F-017, F-018, F-021, F-022 |
| Low / Info | F-004, F-008*, F-010, F-012, F-013 (positive), F-019, F-020, F-023 |

\* F-008 is a cross-listing of F-011 (no TLS) under A02.

## OWASP 2025 coverage map

| Category | Verdict | File |
|---|---|---|
| A01 Broken Access Control | **Action required** | [02](02-broken-access-control.md) |
| A02 Security Misconfiguration | **Action required** | [03](03-security-misconfiguration.md) |
| A03 Software Supply Chain Failures | **Action required** | [04](04-supply-chain-and-integrity.md) |
| A04 Cryptographic Failures | Partial (transport gap) | [05](05-cryptographic-failures.md) |
| A05 Injection | Strong already | [06](06-injection.md) |
| A06 Insecure Design | **Action required** | [07](07-insecure-design-and-exceptional-conditions.md) |
| A07 Authentication Failures | Partial | [08](08-authentication-failures.md) |
| A08 Software & Data Integrity | Partial | [04](04-supply-chain-and-integrity.md) |
| A09 Logging & Alerting Failures | **Action required** | [09](09-logging-alerting-failures.md) |
| A10 Mishandling of Exceptional Conditions | Partial | [07](07-insecure-design-and-exceptional-conditions.md) |

## Reading order

1. [01 — Scope, deployment model & OWASP 2025 mapping](01-scope-threat-model.md)
2. [02 — A01 Broken Access Control](02-broken-access-control.md) *(F-001..F-004)*
3. [03 — A02 Security Misconfiguration](03-security-misconfiguration.md) *(F-005..F-008)*
4. [04 — A03 Supply Chain & A08 Data Integrity](04-supply-chain-and-integrity.md) *(F-009, F-010, F-021)*
5. [05 — A04 Cryptographic Failures](05-cryptographic-failures.md) *(F-011, F-012)*
6. [06 — A05 Injection](06-injection.md) *(F-013)*
7. [07 — A06 Insecure Design & A10 Exceptional Conditions](07-insecure-design-and-exceptional-conditions.md) *(F-014..F-017)*
8. [08 — A07 Authentication Failures](08-authentication-failures.md) *(F-018..F-020)*
9. [09 — A09 Logging & Alerting Failures](09-logging-alerting-failures.md) *(F-022, F-023)*
10. [10 — Execution plan](10-execution-plan.md)

## Finding index

| ID | Title | Sev | File |
|---|---|---|---|
| F-001 | Unauthenticated admin API → arbitrary read/write + RLS bypass + DoS | Critical | 02 |
| F-002 | Unauthenticated blob upload/download | High | 02 |
| F-003 | RLS visibility modes silently impose no filter | Medium | 02 |
| F-004 | Admin `reducer_call` skips client-callable/schedule-only gating | Low–Med | 02 |
| F-005 | No loopback/network gate on admin; binds `0.0.0.0` | High | 03 |
| F-006 | Config secrets plaintext + `Serialize` derived | Medium | 03 |
| F-007 | Permissive connection-limit defaults | Medium | 03 |
| F-008 | No transport TLS (cross-list of F-011) | Medium | 03 |
| F-009 | No cargo-audit/deny/SBOM/CI advisory gate | High | 04 |
| F-010 | First-party registry dep provenance/pinning | Low | 04 |
| F-011 | No transport encryption; tokens/data in cleartext | Medium | 05 |
| F-012 | Non-constant-time hex key parse (config-time) | Low | 05 |
| F-013 | Closed, typed query grammar — no injection path (positive) | Info | 06 |
| F-014 | No query LIMIT ceiling / execution timeout | High | 07 |
| F-015 | No reducer execution-time/memory bound | High | 07 |
| F-016 | Subscriptions/queries not per-identity rate-limited; limiter bypassable | Medium | 07 |
| F-017 | `idempotency_key` has no length cap | Medium | 07 |
| F-018 | Failed-auth backoff per-IP only, no global ceiling | Medium | 08 |
| F-019 | Symmetric HS256 JWT; DB holds minting secret | Low | 08 |
| F-020 | Permissive-auth identity multiplication | Low | 08 |
| F-021 | Sidecar responses unauthenticated / untrusted deserialization | Medium | 04 |
| F-022 | No durable security-event trail; denials at `debug` | Medium | 09 |
| F-023 | Abuse metrics ship no alerting rules | Low | 09 |

## Method & confidence

Findings F-001 and F-005 were verified by directly reading `admin.rs::dispatch`,
`http.rs::handle_admin`, and the per-route handlers. The remainder combine a
thorough code-surface sweep with targeted reads; each finding carries its own
confidence note. The OWASP 2025 category list is per
[OWASP Top 10:2025](https://owasp.org/Top10/2025/).
