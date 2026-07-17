# Proposal: phase3_deterministic-reducer-stdlib

## Why
Deterministic commit-log replay and deterministic simulation testing (DST) are a Fluxum pillar. `ReducerContext` in `crates/fluxum-core/src/reducer/mod.rs` today exposes `identity`, `timestamp`, and `shard_id` but no seeded randomness or logical-clock helper. A reducer that reaches for OS RNG (`rand::random`, `getrandom`) or wall-clock (`SystemTime::now`) produces a different value each run, so replaying the commit log on recovery diverges from the original state and DST cannot reproduce a trace. SurrealDB exposes `rand`/`time` functions but they are non-deterministic; Fluxum needs a sanctioned, seeded path so reducers can still generate ids, rolls, and time buckets without breaking replay.

## What Changes
`ReducerContext` gains `ctx.rand()` (and typed helpers over it) seeded deterministically from the transaction's `(tx_id, shard_id)`, plus logical-time helpers derived from the existing `ctx.timestamp` (e.g. time-bucketing) so both are stable across replay and DST. The seed is threaded from the commit pipeline in `crates/fluxum-core/src/txn` where `tx_id`/`shard_id` are known. Direct wall-clock / OS-RNG use inside a reducer is discouraged via a clippy lint and/or doc note steering authors to the stdlib. Same tx replayed yields the identical `ctx.rand()` sequence.

## Impact
- Governing spec: docs/specs/SPEC-026-security-hardening.md
- Related specs: docs/specs/SPEC-004 (reducer engine), docs/specs/SPEC-009 (identity), commit-log replay / DST docs
- New PRD requirements: FR-146
- Requirements covered: SEC-020, SEC-021
- Affected code: crates/fluxum-core/src/reducer/mod.rs (`ctx.rand()` + logical-time helpers on `ReducerContext`), crates/fluxum-core/src/txn (derive and thread the RNG seed from `tx_id`/`shard_id`), a clippy lint or doc note discouraging wall-clock/OS-RNG in reducers
- Depends on: phase3 reducer engine (archived)
- Breaking change: NO
- User benefit: Reducers can generate ids, random values, and time buckets that survive commit-log replay and reproduce under DST, so recovery restores identical state and simulated failures are reproducible.
