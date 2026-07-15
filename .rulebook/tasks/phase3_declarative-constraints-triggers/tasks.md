## 1. Implementation
- [ ] 1.1 Parse `#[check(expr)]`, `#[references(Table(col), on_delete=...)]`, and `#[not_null]` table attributes into schema metadata (RV-030; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 Carry the parsed constraint metadata on TableSchema/ColumnSchema (RV-030; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.3 Validate check/references/not_null constraints in the commit pipeline before merge, aborting the tx with a typed error on violation (RV-030; crates/fluxum-core/src/txn/mod.rs)
- [ ] 1.4 Parse `#[fluxum::on_insert(Table)]` / `on_update` / `on_delete` hook macros and register them per table (RV-031; crates/fluxum-macros/src/reducer.rs)
- [ ] 1.5 Dispatch declarative triggers inside the triggering transaction, reusing reducer isolation (RV-031; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.6 Apply `on_delete` referential actions (restrict default, cascade, set_null) atomically within the triggering transaction (RV-032; crates/fluxum-core/src/txn/mod.rs)
- [ ] 1.7 Ensure constraint aborts and cascade mutations fan out together as one transactional delta (RV-030, RV-032; crates/fluxum-core/src/txn/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
