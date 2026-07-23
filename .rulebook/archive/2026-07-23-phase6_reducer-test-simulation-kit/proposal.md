# Proposal: phase6_reducer-test-simulation-kit

## Why
Module authors need to unit-test reducers deterministically, but Fluxum today exposes no
testing surface to app developers. The determinism machinery already exists internally as
the DST harness (`crates/fluxum-dst/src/rng.rs`, `sim.rs`, `model.rs`) with a seeded RNG
and simulated model, and the reducer/transaction engine lives in `crates/fluxum-core`
(reducer + txn modules, plus the migration runner under `crates/fluxum-core/src/migration/`).
Nothing wires these together for an author to drive a reducer against an in-process shard,
assert on rows and emitted diffs, and replay a recorded transaction sequence. Without an
exposed kit, authors resort to spinning a full server, which is slow and non-deterministic.

## What Changes
Add a new `fluxum-testkit` crate that lets module authors drive reducers against an
in-process shard with a seeded clock/RNG, assert on resulting rows and emitted diffs, and
replay a recorded transaction sequence deterministically. The kit reuses the DST harness to
support fault injection (mid-commit crash, torn tail) so authors can exercise
recovery-affecting logic.

## Impact
- Governing spec: docs/specs/SPEC-024-developer-experience-tooling.md
- Related specs: docs/specs/SPEC-011 (schema), Phase 2 DST spec, Phase 3 reducer engine spec
- New PRD requirements: FR-136 (reducer test kit)
- Requirements covered: DEV-020, DEV-021
- Affected code: new crate `fluxum-testkit` wrapping crates/fluxum-core reducer/txn engine
  and crates/fluxum-dst (rng.rs/sim.rs/model.rs) for seeded clock/RNG and fault injection
- Depends on: phase3 reducer engine (archived), phase2 DST
- Breaking change: NO
- User benefit: authors write fast, deterministic, repeatable reducer tests — including
  recovery-path tests — without standing up a live server.
