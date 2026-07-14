# 01 — Overview

## What it is

Convex is a "backend-as-a-service" platform where **TypeScript functions run directly inside the
database**. The central premise matches SpacetimeDB's: eliminate the server layer between client
and database.

Instead of:
```
Client → Backend Server (Node/Express) → Database (PostgreSQL)
```

The Convex model is:
```
Client → Convex (TypeScript functions + data + real-time)
```

Business logic is written in TypeScript and uploaded to Convex. The platform executes that logic,
manages state with ACID transactions, and automatically pushes updates to subscribed clients via
WebSocket.

---

## Design philosophy

### 1. Functions as the API surface
All interactions go through typed TypeScript functions — queries, mutations, and actions.
No REST API design needed. No ORM. The function IS the endpoint.

### 2. Reactive by default
Clients subscribe to query functions. When the underlying data changes, the query re-executes
server-side and the new result is pushed to all subscribers automatically. No polling.

### 3. ACID transactions with automatic conflict resolution
Every mutation runs in a snapshot-isolated transaction. Conflicts are detected and mutations
automatically retry (transparent to the application). Developers do not manage optimistic locking.

### 4. Cloud-only, zero infrastructure
Convex is a managed cloud service. No deployment, no servers, no databases to manage.
**Limitation:** cannot be self-hosted, significant vendor lock-in.

---

## Comparison with SpacetimeDB

| Dimension | Convex | SpacetimeDB |
|-----------|--------|-------------|
| Logic execution | TypeScript (V8) in cloud | WASM (Wasmtime/V8) in DB process |
| Execution overhead | 1–5ms per function | <0.1ms per reducer |
| Language | TypeScript only | Rust, C#, TypeScript |
| Data model | Document (JSON) | Relational (typed tables) |
| Wire protocol | JSON over WebSocket | BSATN binary over WebSocket |
| Subscription model | Full document re-send | Incremental delta diffs |
| Transaction model | MVCC + auto-retry | Single-writer per DB |
| Game loop (tick) | Not supported | `schedule_reducer` + scheduled functions |
| Spatial indexes | None (full table scan) | B-tree (limited — no spatial native) |
| Deployment | Cloud-only | Self-hosted or cloud |
| Open source | Backend open-source (partial) | Fully open-source |
| Target | Web apps, collaborative tools | Real-time games, MMORPGs |

---

## Technology stack

### Backend runtime
- **Language:** Rust (entire Convex platform backend)
- **Storage engine:** Proprietary key-value store (WAL-based, not PostgreSQL)
- **Logic runtime:** V8 (Chrome's JavaScript engine)
- **Concurrency:** MVCC with snapshot isolation

### Client libraries
- TypeScript / JavaScript (core)
- React hooks (`useQuery`, `useMutation`, `useAction`)
- Vue 3 composables
- Svelte / SvelteKit integration
- Plain browser JS

### Protocol
- **Queries/mutations:** HTTP POST, JSON body
- **Subscriptions:** WebSocket, JSON messages
- **Auth:** JWT tokens (Convex-issued or custom OAuth)

---

## Known limitations (critical for games)

| Limitation | Severity for games | Notes |
|------------|--------------------|-------|
| No server-side tick loop | **Critical** | Physics, AI, AoI require persistent server state |
| V8 runtime overhead (1–5ms/call) | **Critical** | 60Hz game loop = 60 calls/s per entity |
| JSON encoding (no delta) | **Critical** | 10× bandwidth waste vs binary protocol |
| No spatial indexes | **High** | AoI = full table scan — O(n) |
| MVCC auto-retry (non-deterministic) | **High** | Game simulations require determinism |
| Cloud-only deployment | **High** | Studios need on-premise; latency is non-negotiable |
| Full document on every update | **High** | Even 1-field change sends entire document |
| Mutations cannot call mutations | **Medium** | Limits complex atomic game operations |
| Max function duration (~1s) | **Medium** | Batch operations must be decomposed |
