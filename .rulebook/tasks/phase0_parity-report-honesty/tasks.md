## 1. Implementation
- [ ] 1.1 (F-001, do immediately, independent of the rest) Regenerate `docs/parity/report-v0.1.0.{json,md}` from the current build and commit it so the published artifact stops showing the stale `write 0.30×` state; if the engine fix has not landed yet, label the run "engine fix in progress" in the report header
- [ ] 1.2 (F-008) Report header/framing: state explicitly that the NFR-11 verdicts are a PostgreSQL parity harness and that the `sqlite` side mirrors SpacetimeDB's own published methodology. Decision resolved (2026-07-21): a real SpacetimeDB side IS in scope — `phase0_parity-spacetimedb-baseline` — and its competitive-baseline ratio block appears in this report as a separate section from the NFR-11 verdicts
- [ ] 1.3 (F-009) Relabel the hot-read row "in-process cache read vs remote read", add the footnote explaining the architectural asymmetry, and restructure the summary so the report does not lead with that ratio
- [ ] 1.4 (F-010) Mark e2e and mixed/e2e rows latency-only: drop or annotate their ops/s columns as rate-limit artifacts of the capped event rate
- [ ] 1.5 (F-011) Rigor: raise `runs` to ≥ 5, pin driver vs server to disjoint core sets, and report confidence intervals so the e2e verdict is distinguishable from noise
- [ ] 1.6 Verification (exit test): `fluxum-bench regression` (TST-095) passes against the prior published report; final regenerated artifact committed with all four NFR-11 ratios passing on merit; `phase6_postgres-parity-harness` items 1.6/1.7 checked off with this artifact

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
