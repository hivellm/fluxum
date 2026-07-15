## 1. Implementation

- [ ] 1.1 Define the fixture file format (rows and reducer calls) and its loader/parser (DEV-040; crates/fluxum-cli)
- [ ] 1.2 Implement `fluxum seed <file>` targeting a running or fresh instance (DEV-040; crates/fluxum-cli)
- [ ] 1.3 Apply seeded fixtures through reducers on the server side (DEV-040; crates/fluxum-server)
- [ ] 1.4 Expose the migration diff-planning path read-only from the migration module (DEV-041; crates/fluxum-core/src/migration/diff.rs, runner.rs)
- [ ] 1.5 Implement `fluxum migrate --plan` printing the computed schema diff without mutating state (DEV-041; crates/fluxum-cli, crates/fluxum-core/src/migration/)
- [ ] 1.6 Classify and print the auto-apply decision (safe/additive vs. requires-migration) per diff entry (DEV-041; crates/fluxum-cli, crates/fluxum-core/src/migration/)
- [ ] 1.7 Guarantee `migrate --plan` is side-effect-free (no schema or data mutation) (DEV-041; crates/fluxum-cli, crates/fluxum-core/src/migration/)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
