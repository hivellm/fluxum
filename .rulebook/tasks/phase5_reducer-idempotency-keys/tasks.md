## 1. Implementation

- [ ] 1.1 Add an optional additive `idempotency_key` field to `ReducerCall` and keep the positional MessagePack encoding stable (CS-030; crates/fluxum-protocol/src/messages.rs)
- [ ] 1.2 Define the durable dedup record (key, `(Identity, reducer)` scope, original `ReducerResult`, timestamp) and its bounded window config (count/age) (CS-030, CS-031; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.3 Add a pre-execute dedup check in the reducer dispatch path: on a hit, return the stored `ReducerResult` without invoking the reducer body (CS-030; crates/fluxum-core/src/reducer/engine.rs)
- [ ] 1.4 Persist the dedup record as part of the reducer commit so it survives crash/restart (CS-031; crates/fluxum-core/src/txn/mod.rs, crates/fluxum-core/src/reducer/engine.rs)
- [ ] 1.5 Prune the dedup window by count/age from the schedule worker (CS-031; crates/fluxum-core/src/scheduler/mod.rs)
- [ ] 1.6 Scope keys strictly per `(Identity, reducer)` so distinct callers/reducers never collide (CS-031; crates/fluxum-core/src/reducer/mod.rs)
- [ ] 1.7 Ensure the SDK offline queue attaches a stable `idempotency_key` to every queued call for safe reconnect replay (CS-032; sdks/rust)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
