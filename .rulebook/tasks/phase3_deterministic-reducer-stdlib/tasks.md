## 1. Implementation
- [ ] 1.1 Derive a deterministic RNG seed from `(tx_id, shard_id)` in the commit pipeline and thread it into the reducer context construction (SEC-020; crates/fluxum-core/src/txn)
- [ ] 1.2 Add a seedable, small-state PRNG (e.g. splitmix/pcg) as the `ctx.rand()` backing generator with no OS entropy (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.3 Expose `ctx.rand()` plus typed helpers (bounded ints, u64, fill bytes) on `ReducerContext` (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.4 Add logical-time helpers derived from `ctx.timestamp` (e.g. time-bucket / floor-to-interval) that do not read the wall clock (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.5 Ensure nested/child reducer calls and scheduled executions inherit or re-derive the seed consistently so replay stays stable (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.6 Add a clippy lint or documented deny-note discouraging direct wall-clock / OS-RNG use inside reducer bodies (SEC-021; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.7 Verify determinism: replaying the same tx reproduces the identical `ctx.rand()` sequence and time-bucket outputs (SEC-020, SEC-021; crates/fluxum-core/src/txn)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
