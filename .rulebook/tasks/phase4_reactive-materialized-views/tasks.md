## 1. Implementation
- [ ] 1.1 Parse the `materialized` variant of `#[fluxum::view]` (aggregate fn, optional GROUP BY, optional ORDER BY/LIMIT) and register the view definition (RV-010; crates/fluxum-macros/src/reducer.rs)
- [ ] 1.2 Compile the view definition into an aggregate/top-N plan over one base table, reusing the SQL compiler surface (RV-010, RV-012; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.3 Add a materialized-view state store keyed by group, holding running aggregate accumulators (RV-010; crates/fluxum-core/src/reducer/view.rs)
- [ ] 1.4 Maintain aggregate state incrementally from commit delta rows, touching only affected groups and never full re-scanning (RV-010; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.5 Fan out changed view rows to subscribers as `TxUpdate` with cost O(affected groups) (RV-011; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.6 Maintain a sorted top-N window and emit enter/leave/reorder deltas as underlying rows change (RV-012; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.7 Make view state crash-consistent: rebuild from the base table on recovery or validate persisted state against a bit-identical recompute (RV-013; crates/fluxum-core/src/reducer/view.rs)
- [ ] 1.8 Expose subscription to a materialized view through the existing subscription registry path (RV-011; crates/fluxum-core/src/subscription/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
