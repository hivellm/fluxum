# SPEC-025 — Operations & Multitenancy

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 5 (audit, drain, hot-reload, namespaces, quotas) · Phase 7 (object-storage backup) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-04, FR-90..FR-93, FR-103, FR-110 (extends); new: FR-139 (object-storage backup), FR-140 (audit trail), FR-141 (graceful drain), FR-142 (config hot-reload), FR-143 (database namespaces), FR-144 (per-tenant quotas) |
| **Requirement prefix** | `OPS-` |
| **Source** | New (Fluxum-native). Production/operability gaps beyond the observability + backup specs: remote backup targets, an audit surface over the commit log, zero-downtime restart, live config, and multi-tenant isolation within one binary. |

Keywords are RFC 2119. Requirement IDs `OPS-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

Operational hardening and multi-tenant capability: backup/archive **to object storage**, an
**audit-trail** query surface built on the commit log, **graceful drain** for zero-downtime rolling
restarts, **config hot-reload** without restart, **database namespaces** (multiple logical DBs per
process), and **per-tenant resource quotas**.

## 2. Object-storage backup & archive (`OPS-01x`)

### Requirement: Remote backup targets
- **OPS-010** [P1] `fluxum backup` SHALL support S3-compatible object storage as source/destination for
  checkpoints and archived log segments, using seekable-zstd so PITR can range-read a segment.
- **OPS-011** [P1] Uploaded artifacts MUST be integrity-verified (content hash) and support scheduled,
  incremental archival without stalling writers.

#### Scenario: Nightly offsite backup
Given a configured S3 target
When the nightly backup runs
Then a verified incremental checkpoint + new log segments are uploaded and a later PITR restores from
them by range-reading only the needed segment.

## 3. Audit trail / event-sourcing surface (`OPS-02x`)

### Requirement: Who-changed-what over the commit log
- **OPS-020** [P1] An admin `audit` query SHALL return, for a table/row/time range, the sequence of
  committing reducer calls with `caller`, `reducer_name`, `tx_id`, `timestamp`, reading the commit log /
  archived segments — no separate audit store.
- **OPS-021** [P2] Audit reads MUST honor access control (admin/server-peer only) and never expose
  masked/encrypted column plaintext.

#### Scenario: Trace a row's history
Given an order row changed three times
When an operator runs the audit query for that row
Then it lists the three reducer calls with caller and timestamp in commit order.

## 4. Graceful drain & rolling restart (`OPS-03x`)

### Requirement: Zero-downtime restart
- **OPS-030** [P1] `fluxum drain` (or SIGTERM handling) SHALL stop accepting new connections, finish
  in-flight transactions, checkpoint, and exit cleanly within a bounded deadline.
- **OPS-031** [P2] The SDKs' reconnect/resubscribe (SPEC-021 CS-02x) MUST make a drained restart
  invisible to clients beyond a brief reconnect.

#### Scenario: Deploy without dropped writes
Given a running server draining for a deploy
When a client has an in-flight reducer call
Then that call commits, new calls are refused with a retryable signal, and the process exits after
checkpointing.

## 5. Config hot-reload (`OPS-04x`)

### Requirement: Live config without restart
- **OPS-040** [P1] A defined subset of config (log level/format, slow-reducer threshold, rate limits,
  send-buffer sizes) SHALL be reloadable at runtime via SIGHUP or an admin endpoint; the effective
  values are re-exposed in `/health`.
- **OPS-041** [P1] Non-reloadable keys (ports, storage paths, shard count) MUST be rejected on reload
  with a clear error, never partially applied.

#### Scenario: Raise log verbosity live
Given a running server at `info`
When the operator reloads config with `level: debug`
Then subsequent logs are debug-level with no restart and `/health` reflects the new level.

## 6. Database namespaces (`OPS-05x`)

### Requirement: Multiple logical DBs per process
- **OPS-050** [P2] The server SHALL host multiple named databases, each with independent storage,
  schema, subscriptions, and identity scope, addressable on connect; no cross-namespace transaction or
  subscription is permitted.
- **OPS-051** [P2] Metrics, backups, and quotas MUST be attributable per namespace.

#### Scenario: Two tenants, one binary
Given namespaces `acme` and `globex`
When a client authenticates into `acme`
Then it sees only `acme` tables and cannot subscribe to or mutate `globex`.

## 7. Per-tenant quotas (`OPS-06x`)

### Requirement: Resource isolation
- **OPS-060** [P2] Per-namespace (or per-identity) quotas SHALL bound memory budget share, reducer rate,
  subscription count, and storage bytes; exceeding a quota yields a typed error, never affecting other
  tenants.
- **OPS-061** [P2] Quota usage MUST be exposed as `fluxum_tenant_*` metrics.

#### Scenario: Noisy neighbor contained
Given tenant A hits its reducer-rate quota
When A keeps calling reducers
Then A receives 429s while tenant B's latency is unaffected.

## 8. Non-goals

- Full multi-tenant SaaS billing/control plane (delegated to HiveHub.Cloud, family pattern).
- Cross-namespace queries or transactions (isolation boundary is strict).
- Live repartitioning/shard split-merge (separate post-launch ops tooling).
