# Billion-row soak & small-droplet validation (T7.7)

The two launch-defining claims — **scale** (1B rows, sharded + tiered) and
**frugality** (1 vCPU / 512 MB) — are only proven by sustained soak runs on both
extremes (NFR-12, NFR-13; SPEC-013 TST-110/111/112, SPEC-015). This is the
runbook and the exit criteria; it is the input to gate **G7**.

Two commands do the mechanical work:

- **`fluxum-bench soak`** boots a sharded + tiered server, bulk-loads a dataset,
  then **sustains writes and live subscriptions** for a duration while sampling
  **both** the server's resident memory **and** the engine's own buffer-pool
  gauges (`fluxum_bufferpool_*`, SPEC-015 TIER-080), asserting the peak stayed
  within budget throughout (FR-110 / TIER-004). Writes `soak-report.json` + `.md`.
- **`fluxum-bench droplet`** loads the same dataset into a memory-constrained
  deployment and an unconstrained one, and asserts the constrained run's row sets
  are **identical** to the unconstrained reference (TST-110). Writes
  `droplet-report.json` + `.md`.

Why both: the soak asserts *bounds* (RSS under budget, pool under capacity,
eviction engaging). A bound cannot catch a row that comes back **wrong** after
its page was evicted and faulted back in — that is what the reference-run
comparison is for. JSON is the source of truth in each report; the Markdown is
rendered from it.

> Both drivers are validated end-to-end at small scale by
> `crates/fluxum-bench/tests/soak_smoke.rs` and the `droplet` module's unit
> tests. The **billion-row** run below is an operator run on real hardware. The
> **droplet** runs are the `droplet-profile` CI workflow (weekly +
> `workflow_dispatch`), which is the only place `--cgroup-enforced` is passed.

## Prerequisites

```
cargo build --release -p fluxum-server -p fluxum-bench
```

Both subcommands self-host their servers (the soak samples the server child's
RSS, so it needs the child PID; `droplet` sets each server's budget) — do **not**
pass `--url`.

`--memory-budget` must be at least **128 MiB** (`config::MIN_MEMORY_BUDGET`);
below that the server refuses to start.

> **`--shards` does not shard.** A single-process `fluxum-server` owns exactly
> one shard — `boot.rs` says so outright, and multi-shard hosting is
> `ShardCoord`'s job, which the server binary does not assemble. `--shards` only
> feeds the server's `auto` hardware derivation. The soak records
> `shards_requested` alongside `shards_observed` and warns loudly when they
> disagree, so no report can imply a topology that did not run. **TST-112's
> "sharded + tiered deployment" clause therefore cannot be met by the current
> server binary**; closing it needs a `ShardCoord`-hosted (or process-per-shard)
> deployment, which is outside this task.

## 1. Billion-row soak (NFR-13, TST-112)

On a box with enough disk for the tiered dataset (the cold-tier page file grows
to the full dataset; RAM stays bounded by the budget):

```
fluxum-bench soak \
  --profile billion \
  --rows 1000000000 \
  --duration-secs 3600 \
  --shards 8 \
  --memory-budget 8GiB \
  --clients 16 --subscribers 32 \
  --out docs/reports
```

`--profile billion` makes the eviction witness a pass criterion: a soak whose
data quietly fit in the pool proves nothing about tiering.

Exit criteria (all are `pass` inputs):

- `within_budget: true` — peak RSS ≤ `budget + max(tolerance, 10% of budget)` for
  the **whole** run (TIER-004).
- Every entry of `shard_pools` has `within_capacity: true` — TST-112 requires the
  budget to hold **on every shard**, which one process-wide RSS number cannot
  show, since shards share a process.
- `eviction_engaged: true` — the pool really came under pressure (TST-111).
- Writes flowed and the subscriptions stayed live throughout.

The `--rows`/`--memory-budget` ratio is what forces the cold tier: pick a budget
well under the loaded dataset's in-RAM size so eviction is exercised.

## 2. Small-droplet validation (NFR-12, TST-110/111, HWA-021)

### In CI (the validating run)

`.github/workflows/droplet-profile.yml` runs both halves inside a
`systemd-run` scope capped at `CPUQuota=100%` / `MemoryMax=512M` /
`MemorySwapMax=0`, so the NFR-12 envelope is **cgroup-enforced by the kernel**
rather than merely configured. Trigger it from the Actions tab
(`workflow_dispatch`, with `users`/`rows`/`budget` inputs) or wait for the weekly
schedule.

The job fails early if the runner does not expose cgroup v2 — an artifact that
claims NFR-12 without an enforced envelope would be worse than no artifact.

### On a real droplet, by hand

```
fluxum-server --config config/config.droplet-1vcpu-512mb.yml
```

`config/config.droplet-1vcpu-512mb.yml` pins a single worker thread, a single
shard, a 384 MiB budget (headroom under 512 MB for the OS + page cache), and LZ4
cold-page compression. Then, with a dataset **≥ 10× the budget**:

```
fluxum-bench droplet --users 8 --rows 400000 \
  --memory-budget 384MiB --cgroup-enforced --out docs/reports

fluxum-bench soak --profile droplet --rows 5000000 --duration-secs 1800 \
  --shards 1 --memory-budget 384MiB --out docs/reports
```

Pass `--cgroup-enforced` **only** when the host really enforces the envelope.
Without it the report carries a prominent warning and does not read as an NFR-12
validation — a convenient run on a developer box must never be mistaken for one.

Exit criteria:

- `droplet-report.json` `pass: true` — every user's rows matched the
  unconstrained reference exactly **and** `ten_x_dataset: true` (the dataset
  really was ≥ 10× the budget; the ratio is measured from the cold tier on disk,
  not estimated from row counts).
- `soak-report.json` `idle_rss_ok: true` with `idle_ceiling_enforced: true` —
  idle baseline RSS **< 100 MB** (TST-111).
- `within_budget: true` and `eviction_engaged: true` at ≥ 10× the budget
  (TST-110/111): tiering keeps RSS bounded while the dataset far exceeds memory.
- The full functional profile passes on the droplet (the conformance corpus +
  demo scenario against this server).

## The reports

`soak-report.json` carries: `rows_loaded`, `duration_secs`, `budget_bytes` +
`tolerance_bytes`, `idle_rss_bytes` + `idle_rss_ok` + `idle_ceiling_enforced`,
`peak_rss_bytes`, `within_budget`, `eviction_engaged` + `eviction_required`, the
`rss_samples` and `pool_samples` time series, `shard_pools`, the sustained
`write` throughput/latency summary, `subscription_deliveries`, and `pass`.

`droplet-report.json` carries: `users` × `rows_per_user`, `budget_bytes`,
`dataset_bytes` + `dataset_over_budget` + `ten_x_dataset`, `cgroup_enforced`, the
per-user `users_compared` diffs (counts plus a capped sample of any missing or
unexpected rows), `row_sets_equal`, and `pass`.

Commit both under `docs/reports/` as the G7 evidence.

## What the drivers measure

**`soak`**

1. **Load** — bulk-inserts `--rows` across `--clients` pipelined writers.
2. **Sustain** — for `--duration-secs`, every writer issues acked writes (which
   also feed the live subscriptions) and `--subscribers` connections hold live
   subscriptions. On a fixed cadence the sampler takes the server's RSS *and*
   scrapes `/metrics` for the TIER-080 buffer-pool gauges, so the two witnesses
   are time-aligned. A failed scrape skips one sample rather than aborting an
   hours-long run.
3. **Assert** — peak RSS vs `budget + tolerance`; every shard's pool within its
   own capacity; eviction engaged; idle RSS under the ceiling; writes > 0;
   subscription deliveries > 0. The last two witnesses are only *required* when
   the profile says so, so a small smoke run that never reaches pool pressure is
   not reported as a failure.

**`droplet`**

1. **Constrained run** — a server with a budget far below the dataset. Each user
   writes `--rows` self-identifying rows (`u{user}-r{index}`), then reads them all
   back through a fresh subscription, which forces the server to serve every row
   from storage rather than replay a client cache.
2. **Reference run** — the same dataset with room to spare, so nothing is evicted.
3. **Compare** — per user, as **multisets**: a lost row shows as missing, a
   corrupted one as both missing and unexpected (so a matching count cannot hide
   it), a duplicate as unexpected, and a row leaking across the `owner_only`
   boundary (DM-060) as unexpected in someone else's set.

RSS is read cross-platform (Linux + Windows) via `sysinfo`, pointed at the server
process — not the driver.

## Status

The drivers, reports, witnesses and CI workflow are in place, and the
witnesses have been observed firing on a real pressure run (200k rows under a
128 MiB budget: pool peaked at 101.8 MiB against a 102.4 MiB capacity with
~730k evictions, `eviction_engaged: true`).

Three things stand between here and G7:

1. **The billion-row run has not been performed.** The committed
   `docs/reports/soak-report.*` is a 1M-row / 60 s plumbing proof on a 32-core
   workstation. Measured load throughput is ~4 000 rows/s and does **not**
   improve with more writers or a deeper pipeline (32 clients × pipeline 128
   was *slower* than 8 clients), so 1e9 rows is on the order of **60+ hours of
   load alone** — the `--duration-secs 3600` sustain window is a rounding error
   next to it. Plan the run accordingly, or raise load throughput first.
2. **No cgroup-enforced droplet artifact exists.** Run the `droplet-profile`
   workflow (Actions → Run workflow) or a real droplet.
3. **TST-112's "sharded" clause is not reachable** with today's server binary
   — see the `--shards` note above.
