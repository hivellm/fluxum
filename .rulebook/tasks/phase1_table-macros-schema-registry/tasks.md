## 1. Implementation
- [ ] 1.1 Implement `#[fluxum::table]` proc-macro parsing struct fields into a `TableSchema` model
- [ ] 1.2 Support `#[primary_key]` (incl. composite PKs), `#[auto_inc]`, and `#[index(btree(...))]` (single + composite) attributes
- [ ] 1.3 Parse `#[spatial]`, `#[visibility]`, and `partition_by` attributes into schema metadata (evaluation lands in later phases)
- [ ] 1.4 Implement the link-time schema registry via inventory and `TableSchema` runtime introspection APIs
- [ ] 1.5 Add the example schema (User/OnlineUser/ChatMessage/Task/Sensor) as a compile test fixture
- [ ] 1.6 Golden-file expansion tests (trybuild) covering every attribute in the DM-020 catalogue, plus compile-fail cases for every invalid combination (`#[primary_key]` + table-level `primary_key(...)`, `#[auto_inc]` on composite/non-u64 PK, quadtree+rtree on one table, duplicate same-type index on one column, `partition_by` with `global`, non-float spatial columns) with the specified diagnostics (SPEC-001 acceptance 1)
- [ ] 1.7 Registry multi-crate + duplicate handling: tables/reducers declared across two or more workspace crates all appear in `ServerBuilder::build()`; a duplicate table name aborts startup with a descriptive error (SPEC-001 acceptance 2)
- [ ] 1.8 Verification (DAG exit test): example schema compiles; schema-registry unit tests green
- [ ] 1.9 Gate G1 input: schema suite green (with T1.2 codec and T1.3 auth suites)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
