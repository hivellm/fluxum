## 1. Implementation
- [x] 1.1 Build the random-mutation generator: inserts/updates/deletes across random tables (including owner_only tables) with a population of clients holding random subscriptions (TST-030..) — `subscription::proptest`: seeded splitmix64 PRNG (no rand dep), a public `Sensor` and an `owner_only` `Task`, 24 clients on random queries (full/equality, some server peers), `mutate()` does insert/update(upsert)/delete keeping live-PK sets in sync
- [x] 1.2 Build the client-cache model: initialized from InitialData, maintained solely by applying TxUpdate diffs — `ModelClient` cache keyed by FluxBIN-encoded PK; seeded from `InitialData`, then `apply(deletes, inserts)` (deletes first, so an in-place update lands as the new value) decoded with the same `crate::store::row` codec the manager encodes with
- [x] 1.3 Full run: 10,000 random mutations - every client cache equals the server-side query result for its subscriptions after every commit; required accuracy 100% (NFR-10, SUB acceptance 8) — after every commit, each client's diff-maintained cache is asserted equal to `SubscriptionManager::snapshot_result` (a new SUB-025 one-off read = ground truth); passes at 100%
- [x] 1.4 Wire into CI as the G4 gate suite (full run per TST-034); scenario/fixture set feeds the shared SDK conformance corpus (phase6_sdk-conformance-corpus) — the suite is an in-crate test, so it runs in the default `cargo test --workspace` set (the CI test job); the mutation/cache model is the fixture the phase-6 SDK conformance corpus will reuse
- [x] 1.5 Verification (DAG exit test): suite green in CI — green locally; CI validation deferred per the no-Actions directive (quota)
- [x] 1.6 Gate G4: subscription correctness + backpressure green — T4.1–T4.5 suites all green locally (compiler, fan-out/dedup/pruning, RLS matrix, backpressure isolation, 10k-mutation correctness)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (module docs on `subscription::proptest` explaining the model-client invariant and the SDK-corpus role)
- [x] 2.2 Write tests covering the new behavior (the property suite itself)
- [x] 2.3 Run tests and confirm they pass (full workspace suite green locally; fmt + clippy clean)
