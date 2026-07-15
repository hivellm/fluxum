## 1. Implementation
- [x] 1.1 Implement `__schema_meta__` tracking + SCHEMA_VERSION comparison at startup (FR-80, MIG-001..)
- [x] 1.2 Implement the `#[fluxum::migration(version = N)]` runner: ascending order, each migration in its own transaction, resume from stored version after a mid-sequence crash, shard marked READY only after completion (SPEC-010 acceptance 5)
- [x] 1.3 Migration context operations: add_column (with default backfill), rename_column / `#[rename(from = ...)]` (SPEC-010 acceptance 1/2)
- [x] 1.4 Automatic schema diff + safe auto-apply for additive changes (new table, new column with `#[default]`) in one startup transaction, logged (MIG-023)
- [x] 1.5 Abort paths: incompatible change without a covering migration refuses startup naming table/column/change type with data unmodified; downgrade (code version < stored) fails fast FATAL (SPEC-010 acceptance 3/4)
- [x] 1.6 Failure rollback: a migration returning Err or panicking leaves CommittedState and schema_version unchanged; server exits non-zero; fixed binary re-runs from stored version (SPEC-010 acceptance 6)
- [x] 1.7 Verification (DAG exit test): add/rename-column migrations pass; incompatible change aborts startup (versioned reducers FR-27 are P2 - excluded)
- [x] 1.8 Gate G3 input: migration suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
