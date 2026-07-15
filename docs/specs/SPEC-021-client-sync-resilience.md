# SPEC-021 — Client Sync & Resilience (optimistic, offline, resumable)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 4 (subscription offsets) · Phase 5 (transport/session, idempotency) · Phase 6 (SDK runtime) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-30, FR-31, FR-42, FR-82 (extends); new: FR-120 (optimistic mutations), FR-121 (resumable subscriptions), FR-122 (reducer idempotency), FR-123 (SDK offline persistence) |
| **Requirement prefix** | `CS-` |
| **Source** | New (Fluxum-native). Closes the client-side gap the analysis flags: Convex ships optimistic updates, SpacetimeDB re-sends full `InitialData` on reconnect and its core SDKs are "dead on disconnect with stale caches". The Fluxum SDK is a stub today ([sdks/rust/src/lib.rs](../../sdks/rust/src/lib.rs)); there is no TypeScript SDK. |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`CS-xxx` are stable. Priority tags: `[P0]` MVP · `[P1]` competitive launch · `[P2]` post-launch.

## 1. Scope & problem statement

A realtime SDK is judged by how it behaves under latency and disconnection. This spec defines the
client-resilience layer on top of the existing subscription + `TxUpdate` machinery: **optimistic
local mutations** with server reconciliation, **resumable subscriptions** that resync a delta from
the last-seen `tx_id` instead of re-downloading the whole result set, **idempotent reducer
submission** so retried calls apply once, and an **offline local cache** that survives reload.
These features share one substrate — the monotonic `tx_id` and the enriched `TxUpdate` — and are
specified together because they must interlock (optimistic + retry without idempotency double-applies).

## 2. Optimistic mutations (`CS-01x`)

### Requirement: Optimistic local application
- **CS-010** [P1] The SDK SHALL let a caller register an optimistic updater for a reducer call
  `(localStore, args) -> void` that mutates the local cache immediately, before the server confirms.
- **CS-011** [P1] On the authoritative `TxUpdate` (or `ReducerResult`) for that call, the SDK MUST
  drop the optimistic overlay and re-apply authoritative rows; on `ReducerResult::Err` it MUST roll
  back the overlay, restoring pre-mutation local state.
- **CS-012** [P1] Overlays MUST be layered so a later authoritative update never resurrects a rolled-back
  optimistic row, and concurrent optimistic mutations reconcile deterministically in submission order.

#### Scenario: Optimistic insert confirmed
Given a client subscribed to `Task` with an optimistic updater for `add_task`
When the client calls `add_task` and the network round-trip takes 200 ms
Then the new task appears in the local cache immediately and, on the resulting `TxUpdate`, is
replaced by the authoritative row with no flicker or duplicate.

#### Scenario: Optimistic mutation rejected
Given an optimistic `add_task` applied locally
When the reducer returns `Err("quota exceeded")`
Then the SDK removes the optimistic row and the local cache matches server state exactly.

## 3. Resumable subscriptions (`CS-02x`)

### Requirement: Delta resync from an offset
- **CS-020** [P1] `TxUpdate` and `InitialData` MUST expose a monotonic per-shard `tx_offset`; the SDK
  MUST retain the highest offset applied per subscription.
- **CS-021** [P1] On reconnect the client MAY send `Resume { query_id, from_offset }`; the server SHALL
  reply with only the committed deltas after `from_offset` for that compiled query, followed by live
  `TxUpdate`s — never a full `InitialData` — when the offset is still within the retained window.
- **CS-022** [P1] If `from_offset` predates the retained delta window (compacted), the server MUST
  fall back to a full `InitialData` and signal a cache reset so the SDK clears and replays.
- **CS-023** [P0] The wire additions (`tx_offset`, `Resume` message) are additive and MUST land before
  the G5 wire freeze.

#### Scenario: Brief disconnect resumes cheaply
Given a client subscribed with 100k rows that briefly loses its connection
When it reconnects and sends `Resume { from_offset }` still inside the retained window
Then it receives only the rows changed while offline, not the full 100k-row snapshot.

## 4. Reducer idempotency (`CS-03x`)

### Requirement: Exactly-once submission
- **CS-030** [P1] `ReducerCall` MAY carry a client-assigned `idempotency_key`; the shard MUST record
  applied keys within a bounded, durable dedup window and return the original `ReducerResult` for a
  replayed key without re-executing the reducer.
- **CS-031** [P1] The dedup window MUST survive reducer commit and be bounded by count/age (configurable),
  pruned by the schedule worker; keys are scoped per `(Identity, reducer)`.
- **CS-032** [P1] The SDK offline queue (CS-04x) MUST attach a stable `idempotency_key` to every queued
  call so reconnect replay is safe.

#### Scenario: Retry after reconnect applies once
Given a client sent `transfer_funds` with `idempotency_key=K` and lost the ack
When it reconnects and resends the same call with `K`
Then the funds move once and the client receives the original result.

## 5. SDK offline local persistence (`CS-04x`)

### Requirement: Durable client cache + queue
- **CS-040** [P2] The SDK SHALL optionally persist subscribed state and the pending-mutation queue to a
  platform local store (IndexedDB in browser, file/SQLite on native) keyed by `(server, identity, query)`.
- **CS-041** [P2] On startup the SDK MUST hydrate from the local store, then reconcile via CS-02x
  (resume) or a fresh `InitialData`; the queued mutations replay via CS-03x.

#### Scenario: Reload preserves optimistic queue
Given an offline client with two queued mutations and a hydrated cache
When the page reloads and the client comes back online
Then the cache renders instantly from local storage and both mutations replay exactly once.

## 6. Non-goals

- CRDT/multi-writer conflict resolution on the client (single authoritative shard resolves order).
- Peer-to-peer sync between clients.
- Optimistic updates for cross-shard mutations (shard boundary = transaction boundary).
