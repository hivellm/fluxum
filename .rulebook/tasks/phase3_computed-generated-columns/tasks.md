## 1. Implementation
- [ ] 1.1 Parse `#[computed(expr)]` on a column in the table macro, capturing the expression and its sibling-column references (RV-050; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 Add a computed flag + stored expression to ColumnSchema (RV-050; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.3 Evaluate the computed expression from sibling column values on write, before merge (RV-050; crates/fluxum-core/src/txn/mod.rs)
- [ ] 1.4 Make the computed column read-only to reducers so a reducer cannot set its value (RV-050; crates/fluxum-macros/src/table.rs)
- [ ] 1.5 Store, replicate, and fan out the computed value to subscribers like any stored column (RV-050; crates/fluxum-core/src/txn/mod.rs)
- [ ] 1.6 Allow a computed column to be indexed (RV-051; crates/fluxum-core/src/index/mod.rs)
- [ ] 1.7 Allow computed columns in WHERE/ORDER BY like any column (RV-051; crates/fluxum-core/src/sql/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
