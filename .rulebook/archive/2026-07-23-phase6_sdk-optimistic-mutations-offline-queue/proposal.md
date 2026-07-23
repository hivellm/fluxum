# Proposal: phase6_sdk-optimistic-mutations-offline-queue

## Why
A realtime SDK is judged by how it feels under latency and disconnection. Convex ships optimistic updates; SpacetimeDB's core SDKs are "dead on disconnect with stale caches". Fluxum's SDK is a stub today — sdks/rust/src/lib.rs only re-exports fluxum-protocol, and there is no TypeScript SDK — so the entire client-resilience layer is unbuilt. Optimistic local mutation with server reconciliation, plus an offline mutation queue that replays on reconnect, is the single feature that makes a realtime app feel instant. Without it every write shows a full round-trip of latency.

## What Changes
Add an optimistic-mutation + offline-queue layer to the client SDK (Rust first as the reference, TypeScript alongside per SPEC-011). A caller registers an optimistic updater (localStore, args) that mutates the local cache immediately; on the authoritative TxUpdate/ReducerResult the overlay is dropped and authoritative rows applied, and on Err the overlay rolls back. Overlays are layered so a rolled-back optimistic row is never resurrected and concurrent optimistic mutations reconcile in submission order. Offline calls are queued and replayed on reconnect, each carrying a stable idempotency_key (pairs with phase5_reducer-idempotency-keys) so replay applies exactly once.

## Impact
- Governing spec: SPEC-021 (Client Sync & Resilience, §2 optimistic mutations, §5 offline queue) — docs/specs/SPEC-021-client-sync-resilience.md
- Related specs: SPEC-011 (SDK codegen / client cache), SPEC-006 (TxUpdate/ReducerResult shapes)
- New PRD requirements: FR-120 (optimistic mutations)
- Requirements covered: CS-010, CS-011, CS-012, CS-032 (idempotency-key attachment on queued calls)
- Affected code: sdks/rust (client cache + optimistic overlay + queue), sdks/typescript (new), crates/fluxum-protocol (reuse ReducerCall/TxUpdate)
- Depends on: phase6_typescript-sdk-browser (T6.2, client + cache), phase5_reducer-idempotency-keys (CS-030 for safe replay)
- Breaking change: NO (client-side layer; wire unchanged except idempotency_key handled in its own task)
- User benefit: writes apply instantly in the UI and survive brief disconnects without duplicates or flicker
