## 1. Implementation

- [ ] 1.1 Add a monotonic per-shard `tx_offset` field to `InitialData` and `TxUpdate` (mirroring `tx_id`) and confirm it stays additive/positional (CS-020, CS-023; crates/fluxum-protocol/src/messages.rs)
- [ ] 1.2 Add the additive `Resume { id, query_id, from_offset }` client message struct and register it in the `ClientMessage` tagged enum (CS-021, CS-023; crates/fluxum-protocol/src/messages.rs)
- [ ] 1.3 Track the highest committed `tx_offset` per subscription and retain a bounded delta window (by count/age) in the subscription manager (CS-020, CS-021; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.4 Implement resume replay: given `from_offset` inside the retained window, emit only committed deltas after it for the compiled query, then resume live `TxUpdate`s (CS-021; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.5 Implement the compacted-window fallback: when `from_offset` predates the retained window, send a full `InitialData` plus a cache-reset signal (CS-022; crates/fluxum-core/src/subscription/mod.rs, crates/fluxum-protocol/src/messages.rs)
- [ ] 1.6 Route the `Resume` message through the transports and session layer, preserving per-subscription offsets across GET reconnects (CS-021; crates/fluxum-server/src/{session.rs,http.rs,tcp.rs})
- [ ] 1.7 Have the Rust SDK retain the highest applied `tx_offset` per subscription and send `Resume` on reconnect, handling the cache-reset fallback (CS-020, CS-022; sdks/rust)
- [ ] 1.8 Confirm all wire additions are additive and sequenced to land before the G5 wire freeze (CS-023; crates/fluxum-protocol/src/messages.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
