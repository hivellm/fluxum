# Analysis — Fluxum parity benchmark corrections

**Slug:** `parity-benchmark-corrections` · **Date:** 2026-07-21 ·
**Scope:** why the `fluxum-bench` parity numbers look bad, which are real
engine problems vs measurement/framing artifacts, and a phase0 plan to fix
them.

## Executive summary

The premise "numbers vs SpacetimeDB are bad" needs two corrections up front:

1. **The harness compares Fluxum against PostgreSQL/SQLite, not SpacetimeDB**
   (there is no SpacetimeDB side — F-008). It is the NFR-11 PostgreSQL parity
   harness.
2. **Most of the bad numbers are already fixed in the uncommitted working
   tree.** A structural-sharing MVCC change (`BTreeMap` → `imbl::OrdMap` for
   the row map) has been implemented and re-run but not committed. It moved
   write throughput from **0.30× → 11.97×** vs PostgreSQL, mixed/write 257 →
   31 175 ops/s, and mixed/e2e p99 123 ms → 4.8 ms (F-001, F-003, F-004).

After that fix, **three of four NFR-11 ratios pass** (write ✅, hot ✅, cold ✅).
The **only genuine remaining miss is e2e change→subscriber p99: 4.89× vs the
≥ 10× target** (F-002, F-005). It is *not* a subscription-evaluation problem —
the engine already encodes each query's delta once and shares it to a
subscriber bucket (`subscription/mod.rs:12,319,387`); the residual latency is
per-socket fan-out serialization plus driver-side scheduling of 50 subscriber
threads, and it degrades 5× under contention because commits are single-writer
(F-006).

Separately, the write **ratio** passes but the **absolute** number (36k ops/s)
is round-trip-bound — the SDK issues one in-flight reducer call per client with
no pipelining — so NFR-01 (≥ 100 000 tx/s) and any "SpacetimeDB-class" absolute
claim are not yet demonstrable (F-007). And the report has three
presentation/framing issues that make it easy to misread in either direction:
the 10 201× hot ratio is `HashMap::get` vs SQL-over-HTTP (F-009), e2e "ops/s"
is a rate-limit artifact (F-010), and 3 runs on a shared dev box put the e2e
verdict inside the noise band (F-011).

### Bottom line

| what | verdict |
| --- | --- |
| write / mixed throughput | fixed in flight; **commit the engine change + report** |
| e2e_p99 (4.89× vs 10×) | **the real open miss** — fan-out latency + measurement |
| absolute write (NFR-01) | round-trip-bound; needs SDK pipelining |
| hot / cold ratios | pass; hot needs honest relabeling |
| "vs SpacetimeDB" framing | no SpacetimeDB side exists — reframe or add one |

## Numbered files (reading order)

1. [01-measurements.md](01-measurements.md) — the numbers, both generations
   (committed vs working tree); F-001, F-002.
2. [02-root-causes.md](02-root-causes.md) — why: O(table) commit clone and its
   partial fix, e2e fan-out ceiling, contention, round-trip-bound writes;
   F-003–F-007.
3. [03-methodology-and-framing.md](03-methodology-and-framing.md) — artifacts
   vs real problems: no SpacetimeDB side, meaningless hot ratio, rate-capped
   e2e throughput, run-count rigor; F-008–F-011.
4. [04-execution-plan.md](04-execution-plan.md) — the phase0 remediation
   (P0-A fan-out latency, P0-B write pipelining, P0-C report honesty) and
   suggested rulebook task materialization.

## Findings index

| id | title | file | confidence |
| --- | --- | --- | --- |
| F-001 | Committed parity artifact is stale and materially wrong (write 0.30× vs real 11.97×) | 01 | High |
| F-002 | e2e p99 is the sole remaining NFR-11 miss (4.89× vs ≥10×) | 01 | High |
| F-003 | `MemStore::commit` was O(table size) — deep-clones every touched table | 02 | High |
| F-004 | In-flight `imbl::OrdMap` fix is partial — indexes/unique/spatial/FTS still cloned | 02 | High |
| F-005 | e2e p99 ceiling is fan-out socket serialization + driver thread scheduling, not query eval | 02 | Medium |
| F-006 | Fan-out degrades 5× under contention (single-writer commit path) | 02 | Medium |
| F-007 | Write throughput is round-trip-bound; no SDK pipelining → NFR-01 gap | 02 | Med-High |
| F-008 | No SpacetimeDB side exists; it is a PostgreSQL parity harness | 03 | High |
| F-009 | Hot ratio (10 201×) is honest but not a meaningful performance claim | 03 | High |
| F-010 | e2e "throughput" is a rate-limit artifact; only p99 is meaningful | 03 | High |
| F-011 | 3 runs on a shared dev box put the e2e verdict inside the noise band | 03 | Medium |

## Key source references

- `docs/parity/report-v0.1.0.{json,md}` — the artifact (working tree = fixed,
  committed = stale).
- `crates/fluxum-core/src/store/memstore.rs:1504,1517` — the O(table) commit
  clone; `store/committed.rs` — the `BTreeMap`→`imbl::OrdMap` fix.
- `crates/fluxum-core/src/subscription/mod.rs:12,319,387` — encode-once shared
  fan-out (proves F-005 is not an eval problem).
- `crates/fluxum-bench/src/{workload.rs,fluxum_side.rs,main.rs}` — the driver,
  the synchronous client, the sides.
- `phase6_memstore-structural-sharing/tasks.md`,
  `phase6_postgres-parity-harness/tasks.md` — the in-flight fix and the blocked
  T6.3 exit tests.
- `docs/PRD.md:363,365,373` (NFR-01/03/11);
  `docs/specs/SPEC-013-testing-conformance.md:316` (TST-093 targets).
