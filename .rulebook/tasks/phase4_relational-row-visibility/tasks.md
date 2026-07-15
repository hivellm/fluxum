## 1. Implementation
- [ ] 1.1 Add a `member_of(Table, key)` relational variant to `VisibilityRule` (RV-040; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.2 Parse `#[visibility(member_of(Table, key))]` in the table macro into that variant (RV-040; crates/fluxum-macros/src/table.rs)
- [ ] 1.3 Wire the `VisibilityRule::Custom`/member_of seam in `compile_visibility` to produce a real `RlsFn` instead of `None` (RV-041; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.4 Build/maintain a membership index keyed for identity lookup so per-row visibility is sub-linear (RV-041; crates/fluxum-core/src/index/mod.rs)
- [ ] 1.5 Evaluate the membership rule against the index inside the compiled RlsFn (RV-040, RV-041; crates/fluxum-core/src/sql/mod.rs)
- [ ] 1.6 Apply the relational filter to a subscriber's initial data snapshot (RV-040; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.7 Apply the relational filter to per-commit diffs, so membership changes flip visibility on later commits (RV-040; crates/fluxum-core/src/subscription/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
