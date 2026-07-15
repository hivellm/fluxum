## 1. Implementation

- [ ] 1.1 Scaffold the `fluxum-testkit` crate and its public author-facing API surface (DEV-020; crates/fluxum-testkit)
- [ ] 1.2 Build an in-process shard fixture wrapping the fluxum-core reducer/txn engine (DEV-020; crates/fluxum-testkit, crates/fluxum-core)
- [ ] 1.3 Inject a seeded clock and RNG sourced from the DST harness for deterministic runs (DEV-020; crates/fluxum-testkit, crates/fluxum-dst)
- [ ] 1.4 Provide a reducer-call driver plus assertions over resulting rows and emitted diffs (DEV-020; crates/fluxum-testkit)
- [ ] 1.5 Implement recording and deterministic replay of a transaction sequence (DEV-020; crates/fluxum-testkit)
- [ ] 1.6 Add fault-injection hooks (mid-commit crash, torn tail) reusing the DST harness (DEV-021; crates/fluxum-testkit, crates/fluxum-dst)
- [ ] 1.7 Provide an author-facing example test asserting a non-owner reducer call errors and leaves the row unchanged (DEV-020; crates/fluxum-testkit)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
