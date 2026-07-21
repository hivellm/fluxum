# 03 — Methodology & framing: which "bad numbers" are real vs artifacts

Not every bad-looking number is an engine problem. Three are measurement or
framing issues that should be fixed so the report can't be read dishonestly in
either direction.

### F-008 — There is no SpacetimeDB side; the harness is a PostgreSQL parity harness
- **Evidence:** `crates/fluxum-bench/src/main.rs` only knows sides
  `fluxum | postgres | sqlite` (`:178-215`). NFR-11 itself is defined against
  "app-server + PostgreSQL" (`docs/PRD.md:373`). SpacetimeDB's *own* public
  benchmarks are vs SQLite and vs itself, never PostgreSQL
  (`docs/analysis/spacetimedb-code/09-ops-testing-bench.md:363`, "SpacetimeDB
  has *no* Postgres performance-parity harness").
- **Impact:** The premise "numbers vs SpacetimeDB are bad" is not measurable
  with today's harness — nothing here runs SpacetimeDB. Either (a) reframe the
  narrative as PostgreSQL parity (what NFR-11 actually promises) and lean on
  the existing `sqlite` side to mirror SpacetimeDB's own methodology, or
  (b) add a real SpacetimeDB side (`spacetime` module + SDK client) if a
  head-to-head claim is genuinely wanted. Decide before publishing a "vs
  SpacetimeDB" story.
- **Decision (2026-07-21, user):** option (b) — SpacetimeDB becomes the
  competitive baseline to reach, measured head-to-head. Materialized as
  `phase0_parity-spacetimedb-baseline`; the report gains a separate
  fluxum/spacetimedb ratio block (target ≥ 1× per class) alongside the NFR-11
  PG verdicts.
- **Confidence:** High.

### F-009 — The hot-read ratio (10 201×) is honest but not a meaningful performance claim
- **Evidence:** Fluxum "hot read" is an in-process `HashMap` lookup of the
  SDK's local live view (`crates/fluxum-bench/src/fluxum_side.rs:185-197`),
  clocked at **71 M ops/s, p99 200 ns**; the baseline is an indexed single-row
  `SELECT` over HTTP. The ratio is real per NFR-11's wording ("in-process vs
  SQL round trip") but it is essentially "Rust `HashMap::get` vs a network
  round trip".
- **Impact:** A 10 000× headline invites "you're gaming the benchmark"
  pushback and drowns the numbers that matter (write, e2e). Keep the
  measurement, but label it "in-process cache read vs remote read", cap or
  footnote the ratio, and never lead with it.
- **Confidence:** High.

### F-010 — e2e "throughput" is a rate-limit artifact; only its p99 is meaningful
- **Evidence:** the e2e workload sends at a fixed `rate_per_sec = 10`
  (`crates/fluxum-bench/src/workload.rs:688-693`, `E2eConfig` default `:184`),
  so both sides report ~**496 ops/s** — that number is `messages × subscribers
  / wall`, floored by the sleep, not a throughput measurement. The report
  prints an "ops/s" column for e2e all the same.
- **Impact:** Readers can misread e2e ops/s as "Fluxum and PG are equally fast
  at fan-out". Mark e2e (and mixed/e2e) rows as latency-only, or drop the
  throughput column for them. This does not change any ratio, only how the
  artifact reads.
- **Confidence:** High.

### F-011 — Low run count on a shared dev box inflates tails and variance
- **Evidence:** `runs = 3` (`Opts::default`, `main.rs:751`); the driver and
  both servers share one 32-core workstation; observed instability includes
  cold throughput swinging 170→622 ops/s between generations and large p99
  sigmas (committed write `±545 ops/s`, cold p99 `σ 3.14 ms`). The e2e tail is
  partly driver-thread scheduling (F-005), which more cores-for-server / fewer
  driver threads would move.
- **Impact:** Marginal ratios (e2e at 4.89×) sit inside the noise band; a pass
  or fail can hinge on run-to-run variance. Raising `runs`, pinning driver vs
  server to disjoint cores, and reporting confidence intervals would make the
  e2e verdict trustworthy rather than a coin-flip near the boundary.
- **Confidence:** Medium.

---

## Summary: what is actually "bad"

| finding | real engine problem? | status |
| --- | --- | --- |
| F-003 O(table) commit clone | **Yes** | fixed in flight (F-004), commit it |
| F-004 indexes still cloned | **Yes** | partial; finish `phase6_memstore-structural-sharing` |
| F-005 e2e p99 not 10× | **Yes (fan-out/measurement)** | open — the one live NFR-11 miss |
| F-006 mixed/e2e under contention | **Yes (single-writer)** | open |
| F-007 write round-trip bound | **Yes (SDK, not engine)** | open — blocks NFR-01 claim |
| F-001 stale committed report | No — hygiene | commit the re-run honestly |
| F-008 no SpacetimeDB side | No — framing | decided: add a real side (P0-D) |
| F-009 hot 10 201× | No — presentation | relabel/footnote |
| F-010 e2e throughput artifact | No — presentation | mark latency-only |
| F-011 3 runs / shared box | No — rigor | more runs, core pinning |
