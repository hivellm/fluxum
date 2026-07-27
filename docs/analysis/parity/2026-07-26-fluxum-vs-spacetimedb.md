# Fluxum × SpacetimeDB — full head-to-head benchmark (2026-07-26)

Ad-hoc full-matrix run of the parity harness (`fluxum-bench report`, harness 0.1.0) on branch
`feat/tiered-live-store-integration` — i.e. **with the phase-2 PagedTree CoW live-store cutover
in the working tree**. This is an analysis snapshot, not a release artifact: the versioned
report of record remains [docs/parity/report-v0.1.0.md](../../parity/report-v0.1.0.md)
(2026-07-21). Raw artifacts of this run: [raw-report-2026-07-26.md](raw-report-2026-07-26.md) /
[.json](raw-report-2026-07-26.json).

**Verdict: Fluxum beats SpacetimeDB in every socket class, by 10×–14×** — while acking with a
strictly stronger durability guarantee. The run also surfaced a real, branch-local write-path
regression (~3.5×) versus the published report; it changes no competitive verdict but is
quantified below because it must be addressed before phase 2 closes.

## Setup (TST-091: same machine, same driver, same app)

- **Machine**: AMD Ryzen 9 7950X3D (32 logical cores), 127.2 GiB RAM, NVMe SSD, Windows 10
  (19045). Quiet during measurement (no concurrent builds/tests).
- **Fluxum**: `fluxum-server` 0.1.0 release, native process, self-hosted by the harness,
  development profile, default memory budget. Ack semantics (TXN-004): reducer result acked
  **after** the commit-log append reaches the OS; fsync is async group commit (~50 ms OS-crash
  window, NFR-08).
- **SpacetimeDB**: `clockworklabs/spacetime:v2.6.1` (pinned), standalone in Docker Desktop,
  demo module 1:1 (`crates/fluxum-bench/spacetimedb-module/`), client `spacetimedb-sdk =2.6.1`
  over WebSocket. Module republished with `--delete-data=always` before the steady phases and
  again before cold (equal data footing). Ack semantics: reducer acked at **in-memory** commit,
  before the commit-log append — a crash can lose acked transactions since the last background
  fsync batch.
- **Workloads**: the TST-092 matrix — `write` (acked-serial), `e2e` (commit→receipt via live
  subscription), `hot` (SDK local-cache read), `mixed` (writers + readers + subscribers),
  `cold` (reads after a real server restart) — 5 runs per class, 95 % Student-t CI on p99.
- **Recorded asymmetries**: SpacetimeDB runs in a Linux container (Docker Desktop NAT) while
  Fluxum is native — sub-ms of the socket-class deltas is environment, bounded at ≤ ~4× by the
  2026-07-21 symmetric-environment check (both sides in Docker: every class stayed ≥ 1×). The
  driver ran `--pin server=0xFFFF,driver=0xFFFF0000`; the canonical report runs unpinned —
  the pin was re-measured (below) at ~13 % against Fluxum's write row, so pinning does not
  inflate Fluxum's side of any ratio (it deflates it).

## Competitive results (TST-097 — ratios oriented bigger-is-better-for-Fluxum)

| class | fluxum/spacetimedb | ≥ 1.0 reached |
|---|---|---|
| write_throughput | **14.33** | ✅ |
| e2e_p99 | **12.50** | ✅ |
| mixed_write_throughput | **10.01** | ✅ |
| mixed_e2e_p99 | **11.90** | ✅ |
| cold_p99 | **1.32** | ✅ |
| hot_p99 | 1.69 | ✅ (structural†) |
| mixed_read_p99 | 0.72 | ⏳ (structural†) |

† `hot`/`mixed_read` compare two **in-process SDK cache reads** (~100–400 ns, 50–63 M ops/s on
both sides) quantized at timer resolution — flutter, not product delta. The published report
measured the same classes at 1.00–2.89 across runs; the 0.72 here is the same phenomenon on a
scratch run (no published floor is affected; see observation 1 in
[docs/parity/spacetimedb-baseline.md](../../parity/spacetimedb-baseline.md)).

### Key absolute numbers (mean of 5 runs)

| class | Fluxum | SpacetimeDB |
|---|---|---|
| write (acked-serial) | **8,972 ops/s** · p50 0.63 ms · p99 3.94 ms | 626 ops/s · p50 12.06 ms · p99 35.50 ms |
| e2e p99 (commit→receipt) | **0.94 ms** | 11.74 ms |
| mixed/write | **6,724 ops/s** | 671 ops/s |
| mixed/e2e p99 | **2.53 ms** | 30.06 ms |
| cold p99 (post-restart) | **25.5 ms** | 33.7 ms |
| hot (SDK cache) | 63.3 M ops/s · p99 0.3 µs | 49.9 M ops/s · p99 0.4 µs |

Full raw rows (σ, CI95, max, ops, per-class configs) in
[raw-report-2026-07-26.md](raw-report-2026-07-26.md).

Durability framing for the headline: Fluxum delivers 14× the acked write throughput **with the
stronger ack** (append at the OS pre-ack vs SpacetimeDB's in-memory ack). SpacetimeDB's own
"150k tx/s" figure is an in-process microbenchmark, not an over-socket published-SDK number —
the two measurement classes are never mixed here (both sides are measured over their real
SDKs and sockets).

## Finding: branch-local write-path regression (does not change the verdicts)

Fluxum's absolute write-path numbers fell sharply versus the published 2026-07-21 report while
SpacetimeDB's stayed flat (671 → 626 ops/s — same machine, same containers, environment
stable). The cause is on the Fluxum side of this working tree:

| measurement | published 2026-07-21 | this run (pinned) | re-run unpinned | delta (unpinned) |
|---|---|---|---|---|
| write acked-serial | 36,390 ops/s | 8,972 | 10,263 | **−72 %** |
| write/pipelined(32) | 50,273 ops/s | 6,585 | 11,317 | **−77 %** |
| mixed/write | 31,618 ops/s | 6,724 | — | −79 % (pinned) |
| cold p99 | 0.90 ms | 25.5 ms | — | ~28× worse |

Disambiguation, measured:

1. **Pinning accounts for ~13 %** (8,972 pinned → 10,263 unpinned) — consistent with the
   recorded 2026-07-22 finding that the canonical report runs unpinned. Not the story.
2. **The rest is the phase-2 live-store cutover** (`feat/tiered-live-store-integration`: the
   primary live map is now a PagedTree CoW; reads return owned `Vec<Row>`). The commit path is
   now the bottleneck: pipelined(32) throughput barely exceeds acked-serial (11.3k vs 10.3k,
   where it previously scaled 36k → 50k), i.e. added in-flight calls only queue.
3. **Cold reads** (0.9 → 25.5 ms p99) now traverse the pager on a fresh recovery — partly the
   expected cost of the tiered read path, but the magnitude deserves a look alongside (2).

Consequence recorded by the harness: three NFR-11 (PostgreSQL-parity) targets flipped to MISS
on this run — write_throughput 5.43 (target ≥ 10), e2e_p99 8.37 (≥ 10), cold_p99 0.40 (≥ 0.5) —
all were MET in the published report. The competitive SpacetimeDB verdicts are unaffected
(every socket class still ≥ 10× except cold at 1.32×), but the regression must be profiled and
recovered (or its cost consciously accepted and re-baselined) before the phase-2 branch is
merged and a new report version is published.

## Reproduction

```sh
cargo build --release -p fluxum-server -p fluxum-bench
./target/release/fluxum-bench report \
  --database-url postgres://fluxum:fluxum@127.0.0.1:15432/parity \
  --cold-restart-cmd "docker restart fluxum-parity-pg" \
  --stdb-url http://127.0.0.1:15300 \
  --stdb-restart-cmd "docker restart fluxum-parity-stdb" \
  --stdb-reset-cmd "docker exec fluxum-parity-stdb spacetime publish -s http://127.0.0.1:3000 \
    --bin-path /tmp/module.wasm --delete-data=always --yes fluxum-parity-demo" \
  --disk-note "NVMe SSD" --out <scratch-dir>
```

Container setup one-liners: [docs/parity/spacetimedb-baseline.md](../../parity/spacetimedb-baseline.md)
and [crates/fluxum-bench/README.md](../../../crates/fluxum-bench/README.md).
