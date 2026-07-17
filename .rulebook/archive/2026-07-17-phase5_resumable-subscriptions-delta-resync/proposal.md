# Proposal: phase5_resumable-subscriptions-delta-resync

## Why

On reconnect, SpacetimeDB re-sends the entire `InitialData` snapshot; for a
subscription over 100k rows that is an enormous, avoidable transfer every time a
flaky connection blips. Fluxum already has the substrate to do better: the
commit log is `tx_id`-ordered (`crates/fluxum-core/src/subscription/mod.rs`
`SubscriptionManager::tx_update` copies `diff.tx_id` into every `TxUpdate`, and
`TxUpdate.tx_id` at `crates/fluxum-protocol/src/messages.rs:239` is already
documented as "monotonically increasing per shard"), and the Streamable HTTP
session already survives GET reconnects (`crates/fluxum-server/src/http.rs`,
`crates/fluxum-server/src/session.rs`). The missing pieces are (1) exposing that
offset on `InitialData` too, (2) a `Resume { query_id, from_offset }` client
message, and (3) a bounded retained delta window per subscription so the server
can replay only what changed. Today no `Resume` message exists in the wire
enums (`ClientMessage` at `messages.rs:284`) and `InitialData` carries no
offset, so a reconnecting client has no cheaper option than a full re-download.

## What Changes

Expose a monotonic per-shard `tx_offset` on both `TxUpdate` and `InitialData`
so the SDK can retain the highest offset applied per subscription. Add an
additive `Resume { id, query_id, from_offset }` client message. On receipt the
server replies with only the committed deltas after `from_offset` for that
compiled query (followed by live `TxUpdate`s), never a full snapshot, when the
offset is still inside a bounded retained delta window kept per subscription. If
`from_offset` predates the retained window (compacted away), the server falls
back to a full `InitialData` plus a cache-reset signal so the SDK clears and
replays cleanly. All wire additions are additive and MUST land before the G5
wire freeze.

## Impact

- Governing spec: SPEC-021 (Client Sync & Resilience) — docs/specs/SPEC-021-client-sync-resilience.md §3
- Related specs: SPEC-006 (FluxRPC wire), SPEC-011 (SDK surface)
- New PRD requirements: FR-121 (resumable subscriptions)
- Requirements covered: CS-020, CS-021, CS-022, CS-023
- Affected code: crates/fluxum-protocol/src/messages.rs (new `Resume` msg + `tx_offset` field on `InitialData`/`TxUpdate`, `ClientMessage` enum), crates/fluxum-core/src/subscription/mod.rs (per-subscription offset tracking + bounded delta window + resume replay), crates/fluxum-server/src/{session.rs,http.rs,tcp.rs} (route `Resume`, cache-reset signal), sdks/rust (retain highest applied offset; send `Resume` on reconnect)
- Depends on: phase4 subscription manager (T4.2, archived), phase5 transports (T5.2, archived)
- Breaking change: NO (additive wire fields/messages; must land before G5 wire freeze — CS-023)
- User benefit: a brief disconnect on a 100k-row subscription resyncs only the rows that changed while offline instead of re-downloading the whole result set.
