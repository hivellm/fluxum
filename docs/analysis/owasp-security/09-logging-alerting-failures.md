# 09 — A09:2025 Security Logging & Alerting Failures

Renamed in 2025 (Monitoring → **Alerting**) to stress that *detecting* an attack
requires a durable, queryable trail **and** something that fires on it. Fluxum has
good operational logging and a data-mutation audit trail, but **no security-event
trail and no alerting hooks**.

---

## F-022 — No durable security-event trail; auth failures & RLS denials logged at `debug` (MEDIUM)

**Evidence.** Authentication failures, connection-guard rejections, and
access-control denials are emitted as `tracing::debug!`/`warn!` and aggregate
metrics, not as structured security events:

- Failed `Authenticate` handling logs at debug (`crates/fluxum-server/src/tcp.rs`
  ~`:140-141`, `http.rs` ~`:206-207`).
- Connguard rejections surface only as the `fluxum_conn_rejected_total{reason}`
  counter (SEC-032) — an aggregate, not a per-event record with source IP/time.
- The admin surface returns 403/401 for the one route that checks auth
  (`admin.rs:894-902`) but writes **no audit-log entry** on denial.

The default log level is `info`, so **failed-auth and RLS-denial events (both at
`debug`) are not emitted at all** in a default deployment. The existing audit
module (`crates/fluxum-core/src/commitlog/audit.rs`) reconstructs *data mutations*
("who changed this row/table and when", values never returned — good, `:22-27`,
`:95-111`) but has **no notion of authn/authz events**.

**Impact.** No durable trail of authentication attacks or authorization denials.
An operator cannot answer "was this identity brute-forced?" or "who was denied
access to table X and when?" after the fact — only "how many rejections total".
This is the core A09 failure mode.

**Confidence: High.**

**Fix direction.** Emit structured security events (target `security`, level
`warn`/`info`) for: auth success/failure with source IP and reason, connguard
rejections with reason, RLS/column-grant denials, and every admin mutation with
the operator identity. Keep them at a level that survives the default filter, and
ensure identities (already SHA-256 hashes) and never token bytes are logged.

---

## F-023 — Abuse metrics exist but ship no alerting rules/thresholds (LOW)

**Evidence.** `fluxum_conn_rejected_total{reason}` (SEC-032) and the reducer/quota
counters are exposed via `/metrics`, but the repo ships no alert definitions
(Prometheus rules, thresholds) and no push/webhook on abuse spikes.

**Impact.** "Alerting" half of A09 is unmet: the signal exists but nothing fires
on it. Detection depends entirely on an operator having wired external alerting.

**Confidence: High.**

**Fix direction.** Ship a reference alert-rules file (auth-failure rate, rejection
spike, slow-reducer WARN rate, queue-depth saturation) alongside the metrics
docs, so detection is turnkey.

---

## Positives (A09)

- **Structured JSON logging** by default with reloadable level/format and a
  `RUST_LOG` override (`crates/fluxum-server/src/logging.rs`); reducer context
  (shard, reducer, duration) rides each line.
- **Data-mutation audit** returns metadata only — masked/encrypted column
  plaintext cannot leak through the audit path by construction
  (`commitlog/audit.rs:22-27`, `:95-111`).
- **No secret logging** by the subscriber itself; identities are hashes, not raw
  principals.
