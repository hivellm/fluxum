## 1. Implementation

- [ ] 1.1 Define a pluggable SDK persistence backend trait (put/get/delete keyed by `(server, identity, query)`) with persistence opt-in and off by default (CS-040; sdks/rust)
- [ ] 1.2 Implement a native persistence backend (file/SQLite) for the Rust SDK (CS-040; sdks/rust)
- [ ] 1.3 Persist subscribed table state to the local store as authoritative updates apply (CS-040; sdks/rust)
- [ ] 1.4 Persist the pending-mutation queue (with each call's stable idempotency_key) to the local store (CS-040; sdks/rust)
- [ ] 1.5 On startup, hydrate the cache and queue from the local store before connecting so the UI renders instantly (CS-041; sdks/rust)
- [ ] 1.6 Reconcile after hydrate: resume via `Resume { from_offset }` (CS-02x) or fall back to a fresh `InitialData` when the offset is gone (CS-041; sdks/rust)
- [ ] 1.7 Replay queued mutations through the idempotency key mechanism (CS-03x) so each applies exactly once (CS-041; sdks/rust)
- [ ] 1.8 Scaffold the TypeScript SDK persistence backend over IndexedDB for the browser, sharing the hydrate/reconcile/replay flow (CS-040, CS-041; sdks/typescript)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
