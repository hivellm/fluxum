## 1. Implementation
- [x] 1.1 Change `QueryDelta.subscribers` to `Vec<(u128, u32)>` and stamp each target in `on_commit` with the query_id that connection holds via a new `query_id_of(connection, hash)`; add `QueryDelta::connections()` for callers that only need the connection ids (crates/fluxum-core/src/subscription/mod.rs). View deltas stamp id 0 (they are name-addressed, RV-011). Fixed in commit `4f6982d`.
- [x] 1.2 In the server fan-out, group each delta's targets by their query_id and stamp `TableUpdate.query_id` on the delivered `TxUpdate`, re-encoding only the envelope per distinct id while the rows stay encoded once per delta (crates/fluxum-server/src/lib.rs). Commit `4f6982d`.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — the per-connection-id semantics and the SUB-024 "encode once, stamp per id" reasoning are documented on `QueryDelta.subscribers` and in the fan-out grouping comment.
- [x] 2.2 Write tests covering the new behavior — Rust: `subscription_fanout` asserts `on_commit` stamps each connection with its own assigned query_id. TS: the conformance `unsubscribe` scenario plus 5 cache unit tests exercise the per-query refcount the stamp enables.
- [x] 2.3 Run tests and confirm they pass — fluxum-core/server/macros suites green; TS suite 81/81; corpus `unsubscribe` green.
