# 04 — Execution plan (phase0 remediation)

Goal: turn the parity report into an honest artifact where every NFR-11 ratio
passes on merit, and close the absolute-throughput gap that a "SpacetimeDB-
class" claim implies. These are priority-zero corrections — they gate T6.3's
blocked exit tests (`phase6_postgres-parity-harness/tasks.md` 1.6/1.7) and
Gate G6.

## Dependency shape

```
phase6_memstore-structural-sharing   (EXISTS, in progress — F-003/F-004)
        │  finishes the O(1)-clone engine fix (indexes/unique/spatial/FTS)
        ▼
   P0-A fanout-latency ──┐         (F-005, F-006: the live e2e_p99 miss)
   P0-B write-pipelining ┤         (F-007: NFR-01 / absolute-throughput gap)
   P0-D spacetimedb-side ┤         (F-008 decision: real head-to-head baseline;
        │                │          buildable in parallel, report feeds P0-C)
        ▼                ▼
   P0-C report-honesty-and-framing  (F-001, F-008..F-011: commit the honest
                                     re-run + presentation + rigor)
```

The storage-engine fix (F-003/F-004) is **already tracked** as
`phase6_memstore-structural-sharing` and must not be duplicated — it is the
upstream dependency. The three new phase0 tasks are the net-new work.

---

## P0-A — Drive e2e change→subscriber p99 to ≥ 10× (fan-out latency)
Addresses **F-005, F-006**. The one live NFR-11 miss.

1. **Measure before fixing.** Add server-side instrumentation splitting the
   e2e latency into: commit → subscription-eval → per-socket enqueue → socket
   flush, vs the driver-side thread-wake to callback. Confirm whether the
   0.9 ms p99 is server socket serialization or driver 50-thread scheduling
   (F-005 is explicit that this is not yet isolated).
2. If server-side: make per-subscriber fan-out concurrent/batched (write the
   shared, once-encoded bytes to all bucket sockets without serializing behind
   each flush) and take fan-out off the commit critical path so a small chat
   commit is not queued behind write load (F-006).
3. If driver-side: fix the harness — pin the server to a disjoint core set,
   cap subscriber threads, or measure server-emit timestamp instead of
   client-receipt for the fan-out component (documented honestly per TST-091).
4. **Exit:** `fluxum-bench report` shows `e2e_p99 ≥ 10×` **and** mixed/e2e
   materially better than PG (target: keep > 3×, not 1.68×), reproducibly.

## P0-B — Close the absolute write-throughput gap (SDK request pipelining)
Addresses **F-007**. Needed for any NFR-01 / "SpacetimeDB-class" claim; the
10× PG ratio already passes, so this is about the absolute 100k tx/s target.

1. Allow multiple in-flight reducer calls per SDK connection (pipelined
   request IDs, futures/callbacks matched to acks) so a single client is not
   round-trip-limited at ~222 µs/op.
2. Add a `--pipeline N` / batched-write mode to the write workload so the
   report can show both the acked-serial latency number and the pipelined
   throughput number without conflating them.
3. **Exit:** demonstrate the write path scales toward NFR-01 (≥ 100 000 tx/s
   on one shard, `SPEC-013 TST-060`) under pipelining, on the same demo
   reducer — or record precisely what still caps it (network, single-writer
   commit, ack path) so T6.6's load test starts from a known ceiling.

## P0-C — Commit an honest report + fix framing/presentation/rigor
Addresses **F-001, F-008, F-009, F-010, F-011**. Do last, so the committed
artifact reflects P0-A/P0-B and the finished engine fix.

1. **F-001 (urgent hygiene):** regenerate and commit `report-v0.1.0.{json,md}`
   from the fixed build; never leave the stale `write 0.30×` artifact as the
   published state. If P0-A/P0-B slip, at minimum commit the current
   working-tree re-run labeled "engine fix in progress" rather than the
   two-generations-old committed one.
2. **F-008:** state in the report header that this is a **PostgreSQL** parity
   harness (NFR-11), and that the `sqlite` side mirrors SpacetimeDB's own
   published methodology. Decide explicitly whether a real SpacetimeDB side is
   in scope; if yes, file it as a follow-up (out of phase0).
3. **F-009:** relabel the hot-read row "in-process cache read vs remote read",
   footnote the ratio, and stop leading with it.
4. **F-010:** mark e2e / mixed/e2e rows latency-only (drop or annotate their
   ops/s column — it is a rate-limit artifact).
5. **F-011:** raise `runs` (≥ 5), pin driver vs server to disjoint cores,
   report confidence intervals so the e2e verdict is not inside the noise band.
6. **Exit:** `fluxum-bench regression` passes against the prior published
   report; `phase6_postgres-parity-harness` 1.6/1.7 can be checked off with the
   regenerated artifact.

---

## Suggested rulebook task materialization (phase0)

| id | slug | covers | depends on |
| --- | --- | --- | --- |
| P0-A | `phase0_parity-fanout-latency` | F-005, F-006 | memstore-structural-sharing |
| P0-B | `phase0_parity-write-pipelining` | F-007 | memstore-structural-sharing |
| P0-D | `phase0_parity-spacetimedb-baseline` | F-008 (decision: real side) | — (parallel; final numbers after memstore fix) |
| P0-C | `phase0_parity-report-honesty` | F-001, F-008–F-011 | P0-A, P0-B, P0-D |

P0-D (added 2026-07-21, user decision): a real SpacetimeDB side in `fluxum-bench` —
demo app as a SpacetimeDB WASM module + `spacetimedb_side` over the published Rust SDK,
competitive-baseline ratio block (fluxum/spacetimedb, target ≥ 1× per class) separate
from the NFR-11 PG verdicts. SpacetimeDB is the baseline Fluxum must reach.

Each task carries: an Implementation checklist (the numbered steps above), the
exit/verification item (re-run `fluxum-bench report` / `regression`), and the
standard docs+tests tail. F-003/F-004 stay in the existing
`phase6_memstore-structural-sharing` task — reference it as the dependency,
do not recreate it.
