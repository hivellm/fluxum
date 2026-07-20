# SPEC-012 — Observability

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 5 · T5.6 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-04, FR-90, FR-91, FR-92, FR-93 |
| **Requirement prefix** | `OBS-` |
| **Source** | UzDB spec 13, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `OBS-xxx`. The metrics and health endpoints are served by the HTTP admin
transport ([SPEC-006](SPEC-006-protocol-fluxrpc.md), default port **15800**). Shard lifecycle
states referenced here are defined in [SPEC-007](SPEC-007-sharding.md); reducer execution,
`#[fluxum::tick]` scheduling, and panic isolation in [SPEC-004](SPEC-004-reducers.md);
subscription fan-out and backpressure tiers in [SPEC-005](SPEC-005-subscriptions.md); MemStore,
CommitLog, and SnapshotRepo in [SPEC-002](SPEC-002-storage-engine.md).

## 1. Overview

A production realtime backend running at 100,000+ tx/s with thousands of connected clients
requires first-class observability. This spec defines the metrics, logs, health endpoint, and
observability-related configuration that Fluxum exposes. Monitoring is not optional — it is part
of the correctness contract.

## 2. Prometheus metrics endpoint

- **OBS-001** [P0] The server SHALL expose Prometheus-compatible metrics at `GET /metrics`
  on the HTTP admin transport (plain text exposition format, no authentication required by
  default).
- **OBS-002** [P0] All metrics SHALL use the prefix `fluxum_`. Label values follow these
  conventions throughout this spec:
  - `shard`: shard ID string (e.g. `"0"`; shards created by the geospatial region strategy MAY
    use composite IDs such as `"0_1"`)
  - `reducer` / `func`: reducer or periodic-function name as declared in the module
  - `table`: table name as declared in the module

## 3. Reducer metrics

- **OBS-010** [P0] Reducer throughput counter:

  ```
  fluxum_reducer_calls_total{shard, reducer, outcome}  Counter
  ```

  - `outcome`: `"ok"` | `"err"` | `"rate_limited"` | `"queue_full"`

  Incremented once per `ReducerCall`, regardless of commit/rollback.

- **OBS-011** [P0] Reducer latency histogram:

  ```
  fluxum_reducer_duration_us{shard, reducer}  Histogram
  ```

  Buckets (µs): `[50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000]`

  Measures time from reducer invocation start to transaction commit (or rollback).

- **OBS-012** [P0] Reducer queue depth:

  ```
  fluxum_reducer_queue_depth{shard}  Gauge
  ```

  Number of pending `ReducerCall` messages in the shard's incoming queue. A sustained high
  value indicates the shard is overloaded.

- **OBS-013** [P0] Transaction commit rate:

  ```
  fluxum_tx_commits_total{shard}    Counter
  fluxum_tx_rollbacks_total{shard}  Counter
  ```

## 4. Subscription metrics

- **OBS-020** [P0] Active subscriptions:

  ```
  fluxum_subscriptions_active{shard}  Gauge
  ```

  Total number of currently registered `CompiledPlan` entries across all clients
  ([SPEC-005](SPEC-005-subscriptions.md)).

- **OBS-021** [P0] Fan-out rate:

  ```
  fluxum_fanout_messages_total{shard}  Counter
  fluxum_fanout_rows_total{shard}      Counter
  ```

  - `fanout_messages`: number of `TxUpdate` messages sent
  - `fanout_rows`: total insert+delete rows delivered across all `TxUpdate` messages

- **OBS-022** [P0] Slow client drops:

  ```
  fluxum_subscriber_drops_total{shard, reason}  Counter
  ```

  - `reason`: `"buffer_full"` | `"idle_timeout"` | `"frame_too_large"`

  Reasons correspond to the backpressure and framing rules in
  [SPEC-005](SPEC-005-subscriptions.md) and [SPEC-006](SPEC-006-protocol-fluxrpc.md).

## 5. Storage metrics

- **OBS-030** [P1] Table row counts:

  ```
  fluxum_table_rows{shard, table}  Gauge
  ```

  Number of rows in each table's `CommittedState`. Updated after each commit.

- **OBS-031** [P1] MemStore size:

  ```
  fluxum_memstore_bytes{shard}  Gauge
  ```

  Estimated in-memory size of `CommittedState` in bytes. Used to monitor RAM usage and trigger
  alerts before OOM.

- **OBS-032** [P0] Commit log metrics:

  ```
  fluxum_commitlog_bytes_total{shard}       Counter    // total bytes written to commit log
  fluxum_commitlog_write_duration_us{shard} Histogram  // async write latency (lag between commit and durable append)
  fluxum_commitlog_segments{shard}          Gauge      // number of active log segments
  ```

- **OBS-033** [P1] Snapshot metrics:

  ```
  fluxum_snapshot_last_tx_id{shard}   Gauge      // tx_id of the most recent snapshot
  fluxum_snapshot_duration_ms{shard}  Histogram  // time to write a snapshot
  fluxum_snapshot_size_bytes{shard}   Gauge
  ```

## 6. Connection metrics

- **OBS-040** [P0] Active connections:

  ```
  fluxum_connections_active  Gauge    // total connected clients (all shards)
  fluxum_connections_total   Counter  // total connections since startup
  fluxum_auth_success_total  Counter
  fluxum_auth_failure_total  Counter
  ```

  Auth outcomes are counted per the `AuthProvider` flow in
  [SPEC-009](SPEC-009-authentication.md).

- **OBS-042** [P1] Pre-auth abuse and overload surface (SPEC-026 SEC-032/040/041):

  ```
  fluxum_conn_rejected_total{shard, reason}    Counter  // reason ∈ {conn_cap, accept_rate,
                                                        // failed_auth, handshake_budget,
                                                        // proxy_preamble, proxy_header,
                                                        // blocked, global_cap, overload}
  fluxum_overload_state{shard}                 Gauge    // 0=normal, 1=shed_preauth, 2=shed_all_new
  fluxum_connguard_tracked_ips{shard}          Gauge    // per-IP guard entries currently tracked
  fluxum_connguard_evictions_total{shard}      Counter  // entries reclaimed under pressure
  fluxum_session_rejected_total{shard, reason} Counter  // reason ∈ {unknown_token, ip_mismatch,
                                                        // expired, revoked} (SPEC-026 SEC-053)
  fluxum_admin_rejected_total{shard, reason}   Counter  // reason ∈ {untrusted_ip, unauthenticated}
                                                        // (SPEC-026 SEC-054)
  ```

  Every `reason` label is emitted even at zero, so rate alerts never go
  stale-for-lack-of-series. The guard gauges refresh at scrape time.

- **OBS-043** [P1] Query/reducer execution-bounds surface (SPEC-026 SEC-045/046/047):

  ```
  fluxum_query_aborted_total{shard, reason}      Counter  // reason ∈ {limit, scan_budget,
                                                          // deadline} (SEC-045)
  fluxum_reducer_aborted_total{shard, reason}    Counter  // reason ∈ {deadline, alloc} —
                                                          // every abort is a rollback (SEC-046)
  fluxum_query_rate_limited_total{shard, bucket} Counter  // bucket ∈ {identity, source}
                                                          // (SEC-047 admission refusals)
  ```

  Like OBS-042, every label is emitted even at zero.

- **OBS-041** [P1] Per-client send buffer:

  ```
  fluxum_client_send_buffer_bytes{shard}  Histogram
  ```

  Distribution of per-client send buffer occupancy at fan-out time. Useful for detecting
  systematic backpressure before clients start being dropped.

## 7. Shard metrics

- **OBS-050** [P0] Shard status:

  ```
  fluxum_shard_state{shard}            Gauge  // 0=starting, 1=recovering, 2=ready, 3=shutting_down
  fluxum_shard_recovered_tx_id{shard}  Gauge  // last tx_id replayed during recovery
  ```

- **OBS-051** [P1] Periodic-reducer (`#[fluxum::tick]`) metrics:

  ```
  fluxum_tick_duration_us{shard, func}   Histogram  // per tick-function execution time
  fluxum_tick_missed_total{shard, func}  Counter    // times a tick exceeded 3× its period (SPEC-004 tick-drift rule)
  ```

## 8. Health endpoint

- **OBS-060** [P0] Health check. `GET /health` SHALL return HTTP 200 with a JSON body when
  the server is operational, or HTTP 503 when in a degraded state.

  ```json
  {
    "status": "ok",
    "shards": [
      { "id": "0", "state": "ready", "tx_id": 84201, "queue_depth": 0 },
      { "id": "1", "state": "ready", "tx_id": 81092, "queue_depth": 2 }
    ],
    "connections": 1024,
    "uptime_s": 3600
  }
  ```

  - `status` is `"ok"` if all shards are in state `"ready"`.
  - `status` is `"degraded"` if any shard is in state `"recovering"`.
  - `status` is `"error"` if any shard is in state `"starting"` or `"shutting_down"`.

- **OBS-061** [P1] Load balancer compatibility. The health endpoint SHALL respond within 50 ms
  under normal operation. It SHALL NOT acquire any storage locks to generate its response — it
  reads pre-computed shard state snapshots published by each `ShardHost`.

## 9. Structured logging

- **OBS-070** [P0] Log levels. The server SHALL emit structured logs through the `tracing`
  facade, rendered by `tracing-subscriber` (env-filter enabled). The default output format is
  JSON (one object per line); a human-readable `pretty` format MUST be selectable via
  configuration (OBS-082). Levels are used as follows:

  | Level | Use |
  |-------|-----|
  | `ERROR` | Reducer panics, crash-recovery failures, data corruption detected |
  | `WARN` | Tick budget exceeded, subscriber dropped, queue full; security denials (on the `security` target, OBS-090) |
  | `INFO` | Shard startup/shutdown, snapshot written, migration applied |
  | `DEBUG` | Per-reducer call (disabled by default — enables high-verbosity tracing) |

- **OBS-071** [P0] Reducer error logging. Every reducer that returns `Err` SHALL be logged at
  `DEBUG` level:

  ```json
  {"level":"debug","shard":"0","reducer":"complete_task","identity":"abc...","err":"task not found","duration_us":42}
  ```

  Unhandled panics (caught by the `std::panic::catch_unwind` isolation boundary in
  [SPEC-004](SPEC-004-reducers.md)) SHALL be logged at `ERROR` level with the panic payload and
  a Rust backtrace when available (`RUST_BACKTRACE`/`std::backtrace`).

- **OBS-072** [P1] Slow reducer alerting. Reducers that exceed
  `observability.slow_reducer_threshold_us` (default: 5,000 µs) SHALL be logged at `WARN`
  level:

  ```json
  {"level":"warn","event":"slow_reducer","shard":"0","reducer":"purge_expired_sessions","duration_us":7200}
  ```

## 9a. Security-event trail & alerting (`OBS-09x`, OWASP A09)

- **OBS-090** [P1] Security-event trail. The security-relevant allow/deny moments — authentication
  success/failure, pre-auth connection-guard rejections (SEC-03x), session-token rejections
  (SEC-05x), and admin access-control decisions and mutations (SEC-054) — SHALL be emitted on a
  dedicated `security` `tracing` target, so they are visible at the default log level without
  turning on global `debug`. Denials are `WARN`, allows are `INFO`; the `security` target is
  pinned to at least `INFO` even when the global level is quieter (an explicit `RUST_LOG` still
  wins). Events share a uniform field schema — `event`, `outcome`, and the applicable subset of
  `identity` / `operator` / `source_ip` / `reason` / `resource`. **No security event ever
  contains token bytes or secret material:** identities are the public `SHA-256`-derived value.

  ```json
  {"level":"warn","target":"security","event":"auth_failure","outcome":"deny","source_ip":"203.0.113.9","reason":"bad_credential"}
  {"level":"info","target":"security","event":"admin_mutation","outcome":"allow","source_ip":"10.0.0.2","operator":"ops-oncall","route":"/config/reload"}
  ```

- **OBS-091** [P1] Row-level visibility (SUB-030) is enforced by *filtering* non-visible rows out
  of a scan, and column grants (CT-040) by *masking* values — neither is a discrete per-request
  deny, so they emit no per-row security event (that would be high-frequency and low-signal); the
  discrete auth/connection/session/admin decisions above are the trail. A reducer or query
  rejected outright by access control surfaces through its error envelope and the reducer error
  log (OBS-071).

- **OBS-092** [P2] Reference Prometheus alerting rules for the abuse metrics
  (`fluxum_conn_rejected_total`, `fluxum_overload_state`, `fluxum_session_rejected_total`,
  `fluxum_admin_rejected_total`, `fluxum_subscriber_drops_total`) SHALL ship in
  `docs/alerts/fluxum-security.rules.yml`, each with a runbook note, as a tunable starting point
  for operators.

## 10. Configuration

- **OBS-080** [P0] Single config file. The server SHALL be configured by one YAML file
  (`config.yml`). Every key SHALL be overridable by an environment variable with prefix
  `FLUXUM_`, formed by upper-casing the key path and joining it with `_` (e.g.
  `observability.slow_reducer_threshold_us` → `FLUXUM_OBSERVABILITY_SLOW_REDUCER_THRESHOLD_US`).
  Precedence: environment variable > config file > built-in default.
- **OBS-081** [P0] Development profile. A `development` profile SHALL be selectable
  (`profile: development` / `FLUXUM_PROFILE=development`) that runs with authentication
  disabled and a single shard, and defaults `observability.log_format` to `pretty`. The default
  profile (`production`) defaults to JSON logs.
- **OBS-082** [P1] Observability configuration block. The following keys SHALL be supported:

  ```yaml
  observability:
    log_level: info                  # error | warn | info | debug | trace; also accepts
                                     # tracing env-filter directives (e.g. "info,fluxum_core=debug")
    log_format: json                 # json | pretty
    slow_reducer_threshold_us: 5000  # WARN threshold for OBS-072 (5 ms default)
  ```

## Acceptance criteria

1. **Metrics catalogue complete.** Run the demo app (chat + presence + per-user tasks) under
   load, then `GET /metrics`: the response is valid Prometheus text exposition format and
   contains every metric defined in OBS-010…OBS-051 with the `fluxum_` prefix and the specified
   types and labels.
2. **Reducer counters.** Calling `send_chat` past its rate limit and calling a reducer that
   returns `Err` increments `fluxum_reducer_calls_total` with outcomes `"rate_limited"` and
   `"err"` respectively; a committed call increments `"ok"` and one of
   `fluxum_tx_commits_total`/`fluxum_tx_rollbacks_total` accordingly.
3. **Histogram buckets.** `fluxum_reducer_duration_us` exposes exactly the bucket boundaries
   `[50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000]`.
4. **Health semantics.** With all shards ready, `/health` returns HTTP 200 with
   `status: "ok"` and per-shard `id`/`state`/`tx_id`/`queue_depth`; forcing a shard into
   recovery yields `status: "degraded"` with HTTP 503.
5. **Health latency.** Under a sustained write workload (`update_reading` at high frequency),
   `/health` p99 latency stays below 50 ms and is unaffected by storage lock contention.
6. **Structured logs.** With `log_format: json`, every emitted log line parses as a JSON
   object with a `level` field; with `log_format: pretty`, output is human-readable.
7. **Slow-reducer warning.** Setting `observability.slow_reducer_threshold_us: 1` causes any
   reducer call to emit a `WARN` line with `"event":"slow_reducer"` including `shard`,
   `reducer`, and `duration_us`; restoring the default (5,000 µs) silences it for fast
   reducers.
8. **Panic logging.** A deliberately panicking reducer produces an `ERROR` log with a
   backtrace, the transaction rolls back, and the shard keeps serving (verified via
   `fluxum_shard_state` remaining `2`).
9. **Configuration override.** `FLUXUM_OBSERVABILITY_LOG_LEVEL=debug` overrides the value in
   `config.yml`; `FLUXUM_PROFILE=development` boots a single shard with auth disabled and
   pretty logs.
