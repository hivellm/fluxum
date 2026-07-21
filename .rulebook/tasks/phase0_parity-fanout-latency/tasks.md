## 1. Implementation
- [ ] 1.1 Instrument before fixing: split e2e latency server-side into commit → subscription-eval → per-socket enqueue → socket flush (histogram per stage, e.g. extend `fluxum_reducer_duration_us`-style metrics), and driver-side into server-emit → thread-wake → callback timestamp; run `fluxum-bench report` and attribute the 0.9 ms standalone p99 and the 4.8 ms mixed p99 to specific stages — record the numbers in this task before touching any fix
- [ ] 1.2 If server-bound: make bucket fan-out concurrent/batched — write the shared once-encoded `SharedDelta` bytes to all bucket sockets without serializing behind each subscriber's flush (vectored/batched writes or parallel enqueue), keeping per-subscriber ordering guarantees (SUB-xxx ordering invariants unchanged)
- [ ] 1.3 If server-bound (contention half, F-006): take fan-out off the commit critical path — publish the delta after commit visibility without holding the single-writer path, and/or prioritize small latency-sensitive commits at admission so mixed/e2e does not degrade 5× vs standalone; property/crash suites must stay green (no delivery-ordering or durability regressions)
- [ ] 1.4 If driver-bound: fix the harness measurement — pin server and driver to disjoint core sets, cap subscriber threads, or timestamp at server emit for the fan-out component; document the methodology change honestly per TST-091 in the report generator
- [ ] 1.5 Verification (exit test): `fluxum-bench report` shows `e2e_p99 ≥ 10×` and mixed/e2e materially better than PG (keep > 3×, not 1.68×), reproducibly across ≥ 3 consecutive runs; record before/after stage-split numbers in this task

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
