# Proposal: phase6_sdk-offline-local-persistence

## Why

Local-first apps expect their cache and pending-mutation queue to survive a
page reload or an offline period — SurrealDB ships an in-browser `kv-indxdb`
backend precisely for this. Fluxum's SDK has neither: `sdks/rust/src/lib.rs`
only re-exports `fluxum-protocol` (the client, cache, and codegen land later at
DAG T6.2), and no TypeScript SDK exists at all under `sdks/` (the directory
holds only `rust/`). Without durable client state, a reload drops the subscribed
cache and any queued optimistic mutations, forcing a cold full `InitialData`
re-download and losing offline work. This is a lower-priority (P2) post-launch
feature layered on top of resumable subscriptions and idempotent submission.

## What Changes

The SDK optionally persists subscribed state plus the pending-mutation queue to
a platform local store — IndexedDB in the browser, a file/SQLite backend on
native — keyed by `(server, identity, query)`. On startup it hydrates from that
store so the cache renders instantly, then reconciles with the server: it either
resumes via `Resume { from_offset }` (CS-02x) to pull only the deltas missed
while offline, or falls back to a fresh `InitialData` when the offset is gone.
Queued mutations then replay through the idempotency mechanism (CS-03x) so each
applies exactly once. Persistence is opt-in and off by default.

## Impact

- Governing spec: SPEC-021 (Client Sync & Resilience) — docs/specs/SPEC-021-client-sync-resilience.md §5
- Related specs: SPEC-011 (SDK surface), SPEC-006 (FluxRPC wire)
- New PRD requirements: FR-123 (SDK offline persistence)
- Requirements covered: CS-040, CS-041
- Affected code: sdks/rust (native file/SQLite persistence backend, hydrate + reconcile), sdks/typescript (new — IndexedDB persistence backend for the browser)
- Depends on: phase6_sdk-optimistic-mutations-offline-queue, phase5_resumable-subscriptions-delta-resync
- Breaking change: NO (opt-in, off by default)
- Priority: P2 (post-launch)
- User benefit: after a reload or offline stretch, the app renders instantly from local storage and any queued mutations replay exactly once when it comes back online.
