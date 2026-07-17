## 1. Implementation
- [ ] 1.1 Client cache overlay model: authoritative layer + ordered optimistic overlay layer, keyed by row PK, in the Rust SDK (sdks/rust) (CS-010)
- [ ] 1.2 Register optimistic updater per reducer call: `call_optimistic(reducer, args, |store, args| { ... })` applying to the overlay before send (CS-010)
- [ ] 1.3 Reconciliation on ReducerResult::Ok / matching TxUpdate: drop the overlay for that call, apply authoritative rows, no flicker/duplicate (CS-011)
- [ ] 1.4 Rollback on ReducerResult::Err: remove the overlay, restore pre-mutation local state (CS-011)
- [ ] 1.5 Layering guarantees: a later authoritative update never resurrects a rolled-back optimistic row; concurrent optimistic mutations reconcile in submission order (CS-012)
- [ ] 1.6 Offline mutation queue: buffer calls while disconnected, replay in order on reconnect, each with a stable idempotency_key (CS-032, pairs with phase5_reducer-idempotency-keys) — the key discipline already landed with that task as `fluxum_sdk::OfflineQueue` (mints the key once at enqueue, keeps it stable across retries, per-client namespacing, ack removal, unit-tested). What remains here is wiring it to the real transport and to durable storage: buffer while disconnected, replay in order on reconnect, and persist the queue so a call queued before a restart replays under its original key (a fresh key would double-apply).
- [ ] 1.7 Mirror the optimistic + queue API in the TypeScript SDK (sdks/typescript) per SPEC-011 codegen surface
- [ ] 1.8 Verification: property test — random sequences of optimistic apply/confirm/reject leave the local cache bit-identical to server state; offline-then-reconnect replays exactly once

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
