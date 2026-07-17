## 1. Implementation
- [x] 1.1 The reducer context seeds its RNG from `(tx_id, shard_id)` in `with_context` (the reducer-execution entry, where the Tx's tx_id and the caller's shard_id are both known), before the Tx is moved behind the RefCell (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [x] 1.2 `Rng`: a seedable SplitMix64 (small-state, no OS entropy), interior-mutable (Cell<u64>) so `&self` reducer code advances it; `seed_from` mixes tx/shard through a SplitMix finalizer (SEC-020; crates/fluxum-core/src/reducer/stdlib.rs)
- [x] 1.3 `ctx.rng()` exposes the generator + typed helpers: next_u64/next_u32, below(n) (unbiased Lemire multiply-shift, zero-safe), range(low,high) (empty-safe), bool, f64 in [0,1), fill(&mut [u8]) (SEC-020; crates/fluxum-core/src/reducer/mod.rs, stdlib.rs)
- [x] 1.4 Logical-time helpers `ctx.time_bucket(interval)` (floor timestamp to interval) and `ctx.bucket_index(interval)` (floor(ts/interval)) derived from ctx.timestamp via rem_euclid/div_euclid — negative-safe, never read the wall clock (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [x] 1.5 The whole call tree shares one RNG stream: the RNG lives on TxEnv (one per call tree); nested `ctx.tx.call()` reuses `env.context()`, so a child draws the parent's next value — replay-stable and collision-free (SEC-020; crates/fluxum-core/src/reducer/mod.rs)
- [x] 1.6 SEC-021 guardrail: a "Determinism (SEC-021)" doc section on `#[fluxum::reducer]` and the stdlib module docs steer authors away from rand/OsRng and SystemTime::now to `ctx.rng()`/`ctx.time_bucket` (the sanctioned "doc note" path; a custom clippy lint is deferred) (SEC-021; crates/fluxum-macros/src/lib.rs, crates/fluxum-core/src/reducer/stdlib.rs)
- [x] 1.7 Determinism verified: same (tx_id, shard) reproduces the identical sequence; different shard/tx diverge; successive txs advance and each replays exactly; a nested reducer call draws the middle value of one shared stream; time-buckets are deterministic and pre-epoch-safe (crates/fluxum-core/tests/reducer_stdlib.rs; crates/fluxum-core/src/reducer/stdlib.rs unit tests)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
