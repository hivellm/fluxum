# Proposal: phase0_parity-report-honesty

## Why

The committed parity artifact is stale and materially wrong, and the report's framing has
four rigor problems. Analysis: `docs/analysis/parity-benchmark-corrections/` findings
**F-001, F-008, F-009, F-010, F-011**:

- **F-001 (urgent):** committed `docs/parity/report-v0.1.0.md` (from `535e1dd`) says
  `write_throughput 0.30× ❌` / 941 ops/s; the working tree says `11.97× ✅` / 36 111 ops/s.
  The engine fix that closes the gap is uncommitted. The published state understates write
  throughput ~40× and shows two failing ratios when reality is one — this is the single most
  misleading artifact in the repo, and it is the input to the TST-095 regression guard.
- **F-008:** the harness compares against **PostgreSQL/SQLite**, not SpacetimeDB — there is
  no SpacetimeDB side in `crates/fluxum-bench`. The report must say so; "vs SpacetimeDB"
  impressions come from the framing, not the data. (The `sqlite` side mirrors SpacetimeDB's
  own published methodology.)
- **F-009:** the hot-read row (71M ops/s, 9 467×) is an in-process cache read vs a remote
  socket read — a true architecture difference, but presented as a headline ratio it reads
  as apples-to-apples when it is not.
- **F-010:** e2e and mixed/e2e rows show ops/s columns that are rate-limit artifacts (the
  workload caps event rate); only their latency columns are meaningful.
- **F-011:** `runs` is too low and driver/server share cores, so the e2e verdict sits inside
  the noise band — no confidence intervals are reported.

## What Changes

Done **last** in phase0, so the committed artifact reflects P0-A, P0-B and the finished
`phase6_memstore-structural-sharing` engine fix:

- Regenerate and commit `report-v0.1.0.{json,md}` from the fixed build; if upstream work
  slips, at minimum commit the current working-tree re-run labeled "engine fix in progress"
  rather than leaving the two-generations-old artifact published.
- Report header states the NFR-11 verdicts are a **PostgreSQL parity harness** and that the
  sqlite side mirrors SpacetimeDB's published methodology. Decision resolved (2026-07-21):
  a real SpacetimeDB side is in scope — `phase0_parity-spacetimedb-baseline` — whose
  competitive-baseline ratios land in this same report as a separate section.
- Relabel hot-read as "in-process cache read vs remote read" with a footnote; stop leading
  with it. Mark e2e / mixed/e2e rows latency-only. Raise `runs` ≥ 5, pin driver vs server to
  disjoint cores, report confidence intervals.

## Impact

- Affected specs: SPEC-013 (TST-093/TST-094/TST-095 report content and regression guard)
- PRD requirements: NFR-11 (honest verdict), G6 gate evidence
- Affected code: crates/fluxum-bench report generator, docs/parity/report-v0.1.0.{json,md}
- Depends on: `phase0_parity-fanout-latency`, `phase0_parity-write-pipelining`,
  `phase0_parity-spacetimedb-baseline`, `phase6_memstore-structural-sharing`
- Breaking change: NO
- User benefit: the published benchmark is trustworthy — every ratio passes on merit and
  every caveat is stated, so the "SpacetimeDB-class" claim survives scrutiny
