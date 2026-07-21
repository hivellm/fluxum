# 01 — Measurements: what the parity numbers actually say

Source of truth: `docs/parity/report-v0.1.0.{json,md}` (harness 0.1.0,
2026-07-21, Ryzen 9 7950X3D / 128 GiB / Win10, `fluxum-bench report` vs tuned
`postgres:17`). NFR-11 targets live in `docs/PRD.md:373` and
`docs/specs/SPEC-013-testing-conformance.md:316` (TST-093).

Two facts frame the whole analysis:

1. The harness compares Fluxum against **PostgreSQL/SQLite**, not SpacetimeDB
   — there is no SpacetimeDB side in `crates/fluxum-bench` (see 03, F-008).
   "Bad vs SpacetimeDB" is really "bad against the NFR-11 PostgreSQL baseline".
2. The committed report and the **working-tree** report disagree, because an
   in-progress storage-engine fix has already been run but not committed
   (F-001). The numbers below give both generations.

## The four NFR-11 ratios (two generations)

| ratio | target | committed `535e1dd` | working tree (current) | verdict now |
| --- | --- | --- | --- | --- |
| write_throughput | ≥ 10× | **0.30× ❌** | **11.97× ✅** | fixed in flight |
| e2e_p99 (lower) | ≥ 10× | 4.87× ❌ | **4.89× ❌** | **still failing** |
| hot_p99 (lower) | ≥ 50× | 9 467× ✅ | 10 201× ✅ | passes (but see F-009) |
| cold_p99 (within 2×) | ≥ 0.5× | 3.56× ✅ | 8.60× ✅ | passes (Fluxum faster) |

## Raw rows that matter (working-tree report)

| side | class | ops/s | p50 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- |
| fluxum | write | 36 111 | 0.165 | 1.696 | 11.49 |
| postgres | write | 3 018 | 2.471 | 5.391 | 14.96 |
| fluxum | e2e | 496 | 0.528 | **0.899** | 2.20 |
| postgres | e2e | 484 | 3.279 | 4.392 | 8.19 |
| fluxum | mixed/write | 31 175 | 0.188 | 1.866 | 16.62 |
| fluxum | mixed/e2e | 495 | 0.504 | **4.802** | 5.66 |
| postgres | mixed/e2e | 475 | 4.465 | 8.060 | 9.27 |
| fluxum | hot | 71 392 813 | 0.0001 | 0.0002 | 0.51 |
| fluxum | cold | 622 | 0.426 | 0.983 | 1.06 |

Committed-report equivalents (for the delta the in-flight fix produced):
fluxum write **941 ops/s / p99 17.9 ms**, mixed/write **257 ops/s / p99 77.3
ms**, mixed/e2e **p99 123 ms**, cold **170 ops/s**.

---

### F-001 — The committed parity artifact is stale and materially wrong
- **Evidence:** `git diff docs/parity/report-v0.1.0.md` — the committed file
  (from `535e1dd`) states `write_throughput 0.30 ❌`; the working tree states
  `11.97 ✅`. The committed raw row is `fluxum | write | 941 ops/s`; the
  working tree is `36 111 ops/s`. The engine change that closes the gap
  (`crates/fluxum-core/src/store/committed.rs`, `BTreeMap`→`imbl::OrdMap`) is
  **uncommitted** in the working tree.
- **Impact:** The only published "honest current state" (the report the repo
  commits as the release artifact, and the input to the TST-095 regression
  guard) understates write throughput by ~40× and shows two failing ratios
  when reality is one. Any reader — or the gate G6 reviewer — sees a false
  picture. This is the single most misleading thing in the repo right now.
- **Confidence:** High (direct git diff).

### F-002 — e2e change→subscriber p99 is the sole remaining NFR-11 miss
- **Evidence:** working-tree `ratios.e2e_p99 = 4.89` vs target `≥ 10`
  (`report-v0.1.0.json:171`). Fluxum e2e p99 `0.899 ms`, postgres `4.392 ms`.
  All other ratios pass in the working tree.
- **Impact:** After the in-flight MVCC fix lands, write/hot/cold pass; the DAG
  exit test for T6.3 (tasks 1.6/1.7, Gate G6) still fails on e2e_p99 alone.
  This is the number the remediation must actually move. Root cause in 02
  (F-005/F-006).
- **Confidence:** High (report), with the caveat that the target itself is
  arguably mis-scoped for a rate-capped workload (see F-010).
