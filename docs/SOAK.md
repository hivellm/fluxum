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

> **`--shards N` provisions N real shards.** The server assembles
> `sharding.shards` fully-independent hosts behind a `ShardCoord` (SHD-010) —
> each with its own store, buffer-pool split, commit log and checkpoint worker
> under `shard-<k>/` — or **refuses to start** when the memory budget cannot
> host that many pools. Sessions route by identity affinity after
> authentication (SHD-011). The soak records `shards_requested` alongside
> `shards_observed` and **fails** when they disagree, since that can only mean
> the metrics scrape missed shards. Data distribution: `ChatMessage` is
> partitioned by `channel` (SHD-012), so a soak's writes spread across every
> shard instead of parking on shard 0. Validated at small scale (2026-07-28):
> a 2-shard, 200k-row, 384 MiB run PASSED with both shards' pools peaking
> identically (~152.9 MiB against a 161 MiB per-shard capacity), eviction
> engaged, `shards_observed 2/2` — the TST-112 "sharded" clause is
> mechanically satisfiable; what remains is scale.

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

The drivers, reports, witnesses and CI workflow are in place; the witnesses
have been observed firing on real pressure runs, and the sharded clause is
validated (2-shard 200k/384 MiB run: `shards_observed 2/2`, both pools peaking
identically under their per-shard capacity, eviction engaged).

Where the launch-defining runs stand (task owner-archived 2026-07-28):

1. **The scale criterion is owner-redefined: a 10M-row continuous run in
   place of 1e9** (measured load throughput ~4-5k rows/s makes 1e9 a 60+ hour
   run; it does not improve with more writers or a deeper pipeline —
   batching rows per transaction via a bulk reducer is the lever if it is
   ever revisited). The 10M run was performed and **FAILED `within_budget`:
   peak RSS 1067.2 MiB vs 384+38 MiB**, with both shard pools correctly
   capped throughout — the committed `docs/reports/soak-report.*` is that
   FAIL evidence, kept deliberately. The failure is an engine defect, not a
   harness defect: the checkpoint worker holds a store snapshot across the
   whole checkpoint write, freezing the TIER-061 reclamation floor; at scale
   checkpoints run back-to-back, so CoW garbage bookkeeping accumulates
   resident (~150-200 B per single-row commit) and page-file extents are
   never reused (16.8 GiB on disk for a ~1 GiB live dataset), while the
   RV-020 byte ceiling empties the temporal window without freeing anything
   (`AS OF` reach collapses to 0 under sustained writes). Confirmed
   experimentally: `fluxum_reclaim_pending_pages` sawtooths in lockstep with
   checkpoint boundaries (peaks ~730k pages/shard) and
   `fluxum_temporal_window_snapshots` sits at 0 for entire runs. Until that
   is fixed, any long soak legitimately fails — re-run the 10M criterion
   after the fix. When diagnosing, scrape the `fluxum_reclaim_*` and
   `fluxum_temporal_window_*` gauges alongside `fluxum_bufferpool_*`; the
   pool gauges alone cannot distinguish live pages from unreclaimable
   garbage.
2. **No cgroup-enforced droplet artifact exists.** Run the `droplet-profile`
   workflow (Actions → Run workflow) or a real droplet.
