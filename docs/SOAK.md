# Billion-row soak & small-droplet validation (T7.7)

The two launch-defining claims — **scale** (1B rows, sharded + tiered) and
**frugality** (1 vCPU / 512 MB) — are only proven by sustained soak runs on both
extremes (NFR-12, NFR-13; SPEC-013 TST-110/111/112, SPEC-015). This is the
runbook and the exit criteria; it is the input to gate **G7**.

The `fluxum-bench soak` driver does the mechanical work: it boots a sharded +
tiered server, bulk-loads a dataset, then **sustains writes and live
subscriptions** for a duration while **sampling the server's resident memory**,
and asserts the peak stayed within the configured budget throughout (FR-110 /
TIER-004). It writes a **soak report** (`soak-report.json` + `.md`, JSON is the
source of truth) — the release artifact.

> The driver and report are validated end-to-end at small scale by
> `crates/fluxum-bench/tests/soak_smoke.rs`. The **billion-row** and
> **real-droplet** runs below are operator runs on real hardware — they are not
> run in CI, and they produce the artifacts G7 checks.

## Prerequisites

```
cargo build --release -p fluxum-server -p fluxum-bench
```

`soak` self-hosts the server (it samples the server child's RSS, so it needs the
child PID) — do **not** pass `--url`.

## 1. Billion-row soak (NFR-13, TST-112)

On a box with enough disk for the tiered dataset (the cold-tier page file grows
to the full dataset; RAM stays bounded by the budget):

```
fluxum-bench soak \
  --rows 1000000000 \
  --duration-secs 3600 \
  --shards 8 \
  --memory-budget 8GiB \
  --clients 16 --subscribers 32 \
  --out docs/reports
```

Exit criteria:

- `soak-report.json` `within_budget: true` — peak RSS ≤ `budget + max(tolerance,
  10% of budget)` for the **whole** run (TIER-004).
- `pass: true` — within budget **and** writes flowed **and** the subscriptions
  stayed live throughout (TST-112).

The `--rows`/`--memory-budget` ratio is what forces the cold tier: pick a budget
well under the loaded dataset's in-RAM size so eviction is exercised.

## 2. Small-droplet validation (NFR-12, TST-110/111, HWA-021)

On a real 1 vCPU / 512 MB instance, run the server against the droplet profile:

```
fluxum-server --config config/config.droplet-1vcpu-512mb.yml
```

`config/config.droplet-1vcpu-512mb.yml` pins a single worker thread, a single
shard, a 384 MiB budget (headroom under 512 MB for the OS + page cache), and LZ4
cold-page compression. Then, with a dataset **≥ 10× RAM**, run the full
functional profile plus a soak:

```
fluxum-bench soak --rows 5000000 --duration-secs 1800 --shards 1 \
  --memory-budget 384MiB --out docs/reports
```

Exit criteria:

- The full functional profile passes on the droplet (the conformance corpus +
  demo scenario against this server).
- Idle baseline RSS **< 100 MB** (TST-111) — `soak-report.json` `idle_rss_bytes`
  is sampled before the load phase.
- `within_budget: true` at 10× RAM (TST-110): tiering keeps RSS bounded while the
  dataset far exceeds memory.

## The report

`soak-report.json` (and the rendered `soak-report.md`) carry: `rows_loaded`,
`duration_secs`, `budget_bytes` + `tolerance_bytes`, `idle_rss_bytes`,
`peak_rss_bytes`, `within_budget`, the `rss_samples` time series, the sustained
`write` throughput/latency summary, `subscription_deliveries`, and the overall
`pass`. Commit the report under `docs/reports/` as the G7 evidence.

## What the driver measures

1. **Load** — bulk-inserts `--rows` across `--clients` pipelined writers.
2. **Sustain** — for `--duration-secs`, every writer issues acked writes (which
   also feed the live subscriptions) and `--subscribers` connections hold live
   subscriptions; the server's RSS is sampled on a fixed cadence.
3. **Assert** — peak RSS vs `budget + tolerance`; writes > 0; subscription
   deliveries > 0. All three ⇒ `pass`.

RSS is read cross-platform (Linux + Windows) via `sysinfo`, pointed at the
server process — not the driver.
