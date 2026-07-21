## 1. Implementation
- [x] 1.1 Build the `fluxum-bench` workload driver: demo workload (chat + presence + tasks) with configurable client population, write rates, subscription counts (NFR-11; crate bootstrapped in phase0_workspace-ci-skeleton) — the driver is written ONCE against a `Side`/`BenchClient` trait pair (`src/workload.rs`), so every side runs byte-identical client behavior (TST-090's "same client behavior" is structural, not a promise). Shipped workloads: **write** (N clients looping the acked small write — `add_task`, uncapped, since `send_chat`'s RED-050 20/s limit would throttle only the Fluxum side and falsify the ratio) and **e2e** (1 writer at a configurable rate under the chat limit + M subscribers; each message carries its send instant from a shared in-process `Instant` epoch, so commit→receipt latency needs no cross-clock reasoning). TST-091 honesty is in the driver, not the caller: warmup precedes every measured window, runs are independent repetitions, and `measure.rs` reduces them to mean ± sample-stddev (throughput, p50/p99) — variance is a first-class output. The Fluxum side (`src/fluxum_side.rs`) drives a REAL `fluxum-server` through the published Rust SDK (distinct token per seed → distinct identity; listener registered before subscribe so InitialData cannot race it). CLI `fluxum-bench <write|e2e> --side fluxum [...] --json out.json` (TST-096's one-command shape); without `--url` it boots the RELEASE server binary beside itself and refuses to fall back to debug — the no-argument path cannot produce dishonest numbers. Hot/cold/mixed workloads land with 1.5. Verified: 5 unit tests (percentile/stddev/summary math) + 2 integration smokes against the real server (write acks flow, e2e delivers messages×subscribers exactly); clippy clean
- [x] 1.2 Build the parity baseline app: functionally identical application on app-server + PostgreSQL (decide OQ-9: axum+sqlx vs Node/Express+pg, possibly both) plus the SQLite variant; same schema, same operations, same client behavior — **OQ-9 decided: axum + sqlx** (one in-repo toolchain, so the whole comparison builds and reruns with cargo alone — the TST-096 reproducibility property; a Node/Express variant stays open, and nothing precludes it since the driver only sees the HTTP/WS protocol). `src/baseline/`: `db.rs` (one `Db` enum over PgPool/SqlitePool; schema DDL with covering indexes lives beside the queries; SQLite in WAL mode), `server.rs` (axum: `POST /tasks`, `POST /chat`, `GET /subscribe` WebSocket; a process-wide broadcast feeds the sockets — on PostgreSQL fed by a dedicated `LISTEN chat` connection with `pg_notify` issued INSIDE the insert statement, so the change signal crosses the database like a production LISTEN/NOTIFY setup; on SQLite fed post-commit by the handler, the honest architecture for an embedded DB), `baseline_side.rs` (the driver's client: ureq keep-alive agent + tungstenite WS, implementing the same `BenchClient` trait — identical behavior by construction). The app server runs as its OWN process (`fluxum-bench baseline-server`, spawned by the postgres/sqlite sides) because the incumbent's app server is one — in-process would share the driver's CPU and undercount the stack. Same client behavior as the Fluxum side: acked writes, subscribe-before-measure, same seeds→same users. Verified: SQLite full-loop smoke in CI (`baseline_sqlite_runs_both_workloads` — HTTP write → SQL → push → WS → client callback, exact messages×subscribers delivery) and both workloads run green against the documented `postgres:17` Docker one-liner locally; clippy clean
- [ ] 1.3 Honest-comparison protocol: equal hardware, tuned Postgres (indexes, prepared statements), durability settings documented both ways (synchronous_commit on/off), LISTEN/NOTIFY fan-out for the e2e latency comparison; publish harness + configs with the repo
- [ ] 1.4 Comparative report generator producing the release artifact (report is regenerated per release: v1 at 0.1.0/G6, v2 at 0.2.0/G7)
- [ ] 1.5 Measure all four NFR-11 ratios: write throughput >= 10x, end-to-end change-to-subscriber p99 >= 10x lower, hot reads >= 50x lower latency, cold (page-in) reads within 2x of PostgreSQL
- [ ] 1.6 Verification (DAG exit test): report v1 meets the write, e2e-latency, and cold-read thresholds
- [ ] 1.7 Gate G6 input: parity report v1 (PRD 12.1)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass

## Progress log

Delivered in complete units (driver → baseline → matrix → report), one commit each:

- **Unit 1 (done, committed)** — 1.1: measurement core + `Side`/`BenchClient` traits + write and
  e2e workloads + the Fluxum side + CLI. See 1.1's checked text.
- **Unit 2 (done, committed)** — 1.2: the axum+sqlx baseline (OQ-9 decided), Postgres
  LISTEN/NOTIFY fan-out + SQLite variant, own-process app server, ureq+tungstenite client.
  See 1.2's checked text.
- **Unit 3 (done, committed)** — TST-092 (c) and (e): **hot read** — `prepare_reads` seeds and
  (on Fluxum) materializes the app-side live view fed by row listeners, then `hot_read` is an
  in-process map lookup on Fluxum vs an indexed single-row `SELECT` over HTTP on the baseline
  (`GET /task`), exactly the "in-process vs SQL round trip" comparison NFR-11 names; both
  return the title read so neither side can dead-code the lookup (`black_box` on the driver
  side). **mixed** — writers + hot-readers + chat sender + subscribers over one measured
  window, reported per class (`mixed/write`, `mixed/read`, `mixed/e2e`) so the report shows
  what contention does to each. CLI grew `hot` and `mixed` (+`--rows`); JSON output is now
  (class → Summary). Smokes: Fluxum hot-read (asserts in-process p99 < 1 ms), Fluxum mixed
  (all three classes nonzero), SQLite baseline all-workloads. Remaining for the matrix:
  **cold read** (TST-092 d) — needs a small-memory-budget Fluxum config so the seed overflows
  to the cold tier, and cache-cleared restarts on both sides; then the report generator.
