# 10 — Execution plan (phased hardening)

Phased so each phase lands independently, CI-green, behind opt-in config where it
changes behavior. Aligns with the "expose ports directly" requirement: the goal
is that a **directly exposed Fluxum node is safe by default**, not "safe behind a
proxy". Gate for every phase: local suite + clippy + coverage >90%
([memory: no-github-actions-for-now], [memory: coverage-above-90]).

Severity legend: **P0** ship first / exploitable-by-default · **P1** important ·
**P2** hardening.

---

## Phase A — Close the admin/access-control hole (P0)

Addresses **F-001, F-002, F-004, F-005** (A01/A02). This is the headline risk.

1. **Authenticate every mutating admin route.** Extend the `/audit` server-peer
   check (`admin.rs:894-902`) into a `require_operator(ctx, payload)` guard called
   at the top of `dispatch` for `reducer`, `query`, `query/explain`, `drain`,
   `config/reload`, `plugins/*/disable|enable`, `schema`, `view`, `blob` writes.
   Read-only `health`/`metrics` may stay open or move behind the same guard by
   config.
2. **Loopback/CIDR gate.** Add `server.admin.bind` (default `127.0.0.1`) or an
   `admin_trusted` `IpSet` (reuse `fluxum-core/src/net.rs::IpSet`); reject admin
   requests whose resolved client IP (reuse `clientip.rs`) is outside it. Mirror
   the `none`-provider loopback guard pattern.
3. **Honor `client_callable`/schedule-only in admin `reducer_call`** (F-004):
   route admin reducer calls through the same gating a client session uses.
4. **Authenticate blob upload/download** (F-002) under the same operator guard or
   the normal session identity.

**Task candidate:** `phaseX_admin-surface-authz`.

---

## Phase B — Transport & secret confidentiality (P0/P1)

Addresses **F-011, F-006** (A04/A02).

1. **Optional built-in TLS** (`rustls`) on both listeners: `server.tls.{cert,key}`;
   when enabled, terminate before the handshake read. Add a guard that refuses a
   non-loopback authenticating listener over plaintext (analogous to the `none`
   loopback guard) so tokens are never exposed by default on a public bind.
2. **`Secret<T>` newtype** for `auth.secret`, `server_peers[].token`,
   `encryption.keys[].key_hex`, `transforms.keys[].secret`, sidecar `token`:
   redacting `Debug`/`Serialize`, zeroize-on-drop. Removes the latent
   serialize-leak (F-006).

**Task candidate:** `phaseX_transport-tls-and-secret-hygiene`.

---

## Phase C — Resource-exhaustion / availability design (P1)

Addresses **F-014, F-015, F-016, F-017** (A06/A10).

1. **Query bounds:** configurable default + max `LIMIT`, per-query row-scan
   budget, and a wall-clock deadline that aborts the plan
   (`sql/mod.rs`, `subscription/mod.rs::query_json`).
2. **Reducer bounds:** cooperative execution deadline (checked at stdlib
   boundaries) + per-transaction allocation ceiling; breach → rollback + counter,
   reusing the panic→rollback path (`reducer/engine.rs`).
3. **Per-identity/per-connection rate limits** in front of subscription
   registration and one-off queries; make the global shard guard mandatory-on;
   add an IP/connection-keyed secondary bucket so token rotation can't mint budget
   (`reducer/ratelimit.rs`, `quota.rs`).
4. **Cap `idempotency_key` length** at decode (F-017).

**Task candidate:** `phaseX_resource-limits-availability` (some overlaps the
existing `phase6_ddos-overload-resilience` task already on disk).

---

## Phase D — Supply-chain gate (P1)

Addresses **F-009, F-010** (A03).

1. Add `deny.toml` (advisories + bans + licenses + `[sources]` allow-list
   covering `thunder-rpc`) and wire `cargo deny check` into the **local** gate
   (compatible with the no-Actions constraint).
2. Generate a CycloneDX SBOM on release (`cargo cyclonedx`).

**Task candidate:** `phaseX_supply-chain-cargo-deny`.

---

## Phase E — Security logging & alerting (P1/P2)

Addresses **F-022, F-023** (A09).

1. Structured `security`-target events (survive default `info` filter) for auth
   success/failure (+source IP, reason), connguard rejections, RLS/column-grant
   denials, and admin mutations (operator identity). Never log token bytes.
2. Ship reference Prometheus alert rules (auth-failure rate, rejection spike,
   slow-reducer WARN rate, queue saturation).

**Task candidate:** `phaseX_security-event-audit-and-alerts`.

---

## Phase F — Auth & remaining hardening (P2)

Addresses **F-018, F-019, F-021, F-003, F-020**.

1. Global failed-auth ceiling + optional per-identity lockout (F-018).
2. Asymmetric (verify-only) JWT provider variant (F-019).
3. Enforce (or explicitly reject at schema-load) `shard_local`/`custom`/
   `member_of` visibility so a declared rule never silently means "no filter"
   (F-003).
4. Document mTLS/loopback as a **hard** requirement for the sidecar transport and
   treat decode failures as breaker trips (F-021).

**Task candidate:** `phaseX_auth-and-rls-hardening`.

---

## Suggested sequencing

```
A (P0 access control) ─┬─> C (availability) ──> F (auth/RLS hardening)
B (P0 TLS/secrets) ────┘
D (supply chain) — independent, can run anytime
E (logging/alerting) — after A (so denials have events to emit)
```

Phases A and B are the ones that make a directly-exposed node defensible and
should precede any public-exposure milestone.
