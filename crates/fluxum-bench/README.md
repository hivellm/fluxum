# fluxum-bench — the PostgreSQL parity harness (T6.3)

Permanent comparative infrastructure for PRD NFR-11 / SPEC-013 §10 (TST-090..TST-096): the same
application — chat + tasks + live subscriptions — implemented twice, driven by one workload
driver, measured on equal hardware, published as a versioned report with every release.

## The two sides

| | Fluxum | incumbent baseline |
| --- | --- | --- |
| server | `fluxum-server` (release), demo module | axum + sqlx app server, own process (`fluxum-bench baseline-server`) |
| database | — (the server IS the database) | PostgreSQL 17 (tuned: covering indexes, prepared statements, pool) or SQLite (WAL) |
| writes | acked reducer calls over the Rust SDK | `POST` + SQL `INSERT`, HTTP 2xx after commit |
| live queries | subscriptions → `TxUpdate` | WebSocket fed by `LISTEN/NOTIFY` (PG) / post-commit broadcast (SQLite) |
| hot read | in-process lookup of the listener-fed live view | indexed single-row `SELECT` over HTTP |

Both sides implement the same `BenchClient` trait, so identical client behavior is structural
(TST-090), and every workload warms up before measuring and repeats runs with variance reported
(TST-091).

## One command per side (TST-096)

```sh
# The tuned PostgreSQL the comparison runs against:
docker run --rm -d --name fluxum-parity-pg -e POSTGRES_USER=fluxum \
  -e POSTGRES_PASSWORD=fluxum -e POSTGRES_DB=parity -p 15432:5432 postgres:17

cargo build --release -p fluxum-server -p fluxum-bench

# Individual workloads (write | e2e | hot | cold | mixed), either side:
./target/release/fluxum-bench write --side fluxum
./target/release/fluxum-bench write --side postgres \
  --database-url postgres://fluxum:fluxum@127.0.0.1:15432/parity

# Pipelined writes (SDK-032, NFR-01 methodology): N acked calls in flight
# per connection — a separate measurement class, never conflated with the
# acked-serial write row (throughput is its meaningful column):
./target/release/fluxum-bench write --side fluxum --pipeline 32

# The full matrix on both sides → docs/parity/report-v<version>.{json,md} (TST-094):
./target/release/fluxum-bench report \
  --database-url postgres://fluxum:fluxum@127.0.0.1:15432/parity \
  --cold-restart-cmd "docker restart fluxum-parity-pg" \
  --disk-note "NVMe SSD"

# The CI regression guard (TST-095):
./target/release/fluxum-bench regression \
  --current docs/parity/report-vNEW.json --published docs/parity/report-vOLD.json
```

The `report` command floors `--runs` at **5** (F-011: the versioned artifact's verdicts must be
distinguishable from noise — raw rows carry a 95% Student-t confidence half-width on p99);
single-workload invocations keep the CLI default. The regression guard's `--tolerance`
(default 20%) applies to the NFR-11 ratios AND to the TST-097 parity floor, so a
noise-dominated competitive ratio sitting at the boundary (two sub-µs in-process reads) cannot
flap a release. The NFR-11 check is additionally **noise-aware**: a relative drop only counts
as a regression when the two runs' ratio-uncertainty bands (95% Student-t, propagated from
each side's summaries by interval arithmetic) are disjoint — a four-orders-of-magnitude ratio
whose denominator sits at timer resolution (the in-process hot read) swings ±50% run to run,
while a real fall (that read becoming a socket round trip) lands far outside any band.

Without `--url`, the harness boots the **release** `fluxum-server` found beside its own binary
and refuses to fall back to a debug build — the no-argument path cannot produce dishonest
numbers.

## Honesty notes recorded in every report (TST-091)

- Both sides run on the same machine (recorded: CPU, cores, RAM, OS; disk class is
  operator-stated via `--disk-note`).
- Durability is documented on both sides: Fluxum acks after the commit-log append reaches the
  OS (TXN-004; fsync is async group commit, ~50 ms OS-crash window), against PostgreSQL's
  `synchronous_commit` as actually configured (read from the server, not assumed).
- Cold reads restart both servers between runs: database-level caches (buffer pool /
  `shared_buffers`) clear symmetrically; the OS page cache is not dropped on either side, so
  cold numbers measure database page-in, not platter latency.
- The chat rate limit (20/s per identity, RED-050) applies only to the e2e/mixed senders and
  the same offered load is used on both sides; the write-throughput workload uses the uncapped
  task insert, since a Fluxum-side-only admission limit would falsify the ratio.
- The report's own framing is part of the honesty surface (F-008..F-011): the header states
  the NFR-11 verdicts are a **PostgreSQL parity harness** (the SpacetimeDB competitive block
  is separate and never mixed in), the hot-read ratio is footnoted as an in-process-cache vs
  remote-read architecture asymmetry rather than a same-transport comparison, and e2e rows are
  latency-only (their ops/s is the workload's rate cap, not a measurement).
