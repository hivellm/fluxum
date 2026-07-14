# 02 — Architecture

## Layer diagram

```
┌─────────────────────────────────────────────────────┐
│                    CLIENT LAYER                     │
│  React / Vue / Svelte / plain JS                    │
│  useQuery() → subscribes | useMutation() → HTTP     │
└───────────────────────┬─────────────────────────────┘
                        │ HTTP (mutations/actions) + WebSocket (subscriptions)
┌───────────────────────▼─────────────────────────────┐
│                  CONVEX CLOUD LAYER                 │
│  ┌──────────────┐  ┌────────────────┐               │
│  │  HTTP Router │  │ WebSocket Hub  │               │
│  │  /api/v1/    │  │ /subscribe     │               │
│  │  query       │  │ (JSON over WS) │               │
│  │  mutation    │  └───────┬────────┘               │
│  │  action      │          │                        │
│  └──────┬───────┘          │                        │
│         │                  │                        │
│  ┌──────▼──────────────────▼────────────────────┐   │
│  │           FUNCTION RUNTIME                   │   │
│  │  V8 JavaScript engine                        │   │
│  │  Queries     — read-only, subscribable       │   │
│  │  Mutations   — ACID writes, atomic           │   │
│  │  Actions     — side effects, non-transactional│  │
│  │  Scheduled   — cron-like, async              │   │
│  └──────┬────────────────────────────────────────┘  │
│         │                                            │
│  ┌──────▼────────────────────────────────────────┐  │
│  │           STORAGE + SUBSCRIPTION ENGINE       │  │
│  │  Proprietary KV store (WAL-backed)            │  │
│  │  MVCC snapshot isolation                      │  │
│  │  Subscription coordinator (query invalidation)│  │
│  │  Conflict detector → auto-retry for mutations │  │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

---

## Function types

### Queries (read-only, subscribable)
```typescript
// convex/players.ts
export const getPlayer = query({
  args: { playerId: v.id("players") },
  handler: async (ctx, { playerId }) => {
    return await ctx.db.get(playerId);
  },
});
```
- Pure functions: no writes, no side effects
- Results are cached and automatically invalidated when dependencies change
- Clients subscribe via `useQuery(api.players.getPlayer, { playerId })` (React)
- On data change: query re-executes server-side, new result pushed via WebSocket

### Mutations (ACID writes)
```typescript
export const movePlayer = mutation({
  args: { playerId: v.id("players"), x: v.number(), y: v.number() },
  handler: async (ctx, { playerId, x, y }) => {
    await ctx.db.patch(playerId, { position: { x, y } });
  },
});
```
- Atomic: all-or-nothing
- Snapshot isolation: reads see a consistent snapshot
- Conflict detection: if concurrent mutation conflicts, auto-retried (transparent)
- **Cannot call other mutations** (prevents deadlocks; enforced at runtime)

### Actions (side effects)
```typescript
export const sendEmail = action({
  args: { userId: v.id("users"), message: v.string() },
  handler: async (ctx, { userId, message }) => {
    // Can call fetch(), call mutations, but NOT transactional
    await fetch("https://email-api.com/send", { ... });
  },
});
```
- Not transactional with the database
- Can call `ctx.runMutation()` and `ctx.runQuery()`
- For external APIs, file operations, long-running tasks

### Scheduled functions
```typescript
// convex/crons.ts
import { cronJobs } from "convex/server";
const crons = cronJobs();
crons.interval("cleanup-expired-sessions", { minutes: 5 }, api.sessions.cleanup);
export default crons;
```
- Cron-like definitions in `convex/crons.ts`
- Minimum interval: not documented (likely 1 minute)
- **Not suitable for game tick loops** (no 60Hz scheduling)

---

## Transaction model

### MVCC (Multi-Version Concurrency Control)
- Every mutation reads a **snapshot** of the database at transaction start
- Writes accumulate in a private transaction buffer
- On commit: Convex checks for write-write conflicts with concurrent transactions
- **Conflict → auto-retry** (transparent, up to platform limit)

### Guarantees
| Property | Guarantee |
|----------|-----------|
| Atomicity | Yes — all-or-nothing per mutation |
| Consistency | Yes — schema validation |
| Isolation | Snapshot isolation (not serializable) |
| Durability | Yes — WAL-based |
| Linearizability | **No** — no strict global ordering |
| Determinism | **No** — auto-retry means non-deterministic execution order |

### Why MVCC auto-retry is wrong for games
SpacetimeDB uses a single-writer model per database instance:
- Only one reducer executes at a time
- Execution order is deterministic and replayable from the commit log
- Game simulations can be exactly reproduced for debugging/replay

Convex's auto-retry means:
- Two mutations may race and retry in different orders
- Game state is not deterministically replayable
- Multiplayer desync bugs are harder to reproduce

---

## V8 runtime overhead

| Operation | Convex (V8) | SpacetimeDB (WASM) | UzDB (TML native) |
|-----------|-------------|---------------------|-------------------|
| Function call overhead | 1–5ms | <0.1ms | ~0ms |
| Memory per function | ~2MB V8 heap | <100KB WASM | direct stack |
| Startup (cold) | 50–200ms | 10–50ms | 0ms (already loaded) |
| Startup (warm) | 0ms | 0ms | 0ms |

For a 60Hz game tick:
- Convex: 60 × 5ms = **300ms/s** just in function call overhead (impossible)
- SpacetimeDB: 60 × 0.1ms = **6ms/s** overhead (marginal)
- UzDB native: 60 × 0ms = **0ms** overhead (zero)

---

## Storage layer

- **Engine:** Proprietary key-value store (not PostgreSQL, not SQLite)
- **Durability:** Write-ahead log (WAL), append-only
- **Indexes:** B-tree on document fields; no spatial indexes
- **Query model:** Document filter (`ctx.db.query("table").filter(...)`)
- **Schema:** Optional TypeScript `defineSchema()` for validation, not enforced at storage level
- **No joins:** Documents reference other documents by `Id<"table">` (denormalization required)

---

## Protocol details

### HTTP (queries and mutations)
```
POST https://<project>.convex.cloud/api/v1/mutation/<functionName>
Authorization: Bearer <jwt-token>
Content-Type: application/json

{"args": {"x": 10, "y": 20}}
```

### WebSocket (subscriptions)
```json
// Subscribe
{"op": "subscribe", "queryId": "q1", "query": "players:getAll", "args": {}}

// Server push on data change
{"op": "update", "queryId": "q1", "value": [{"_id": "abc", "x": 10, "y": 20}]}

// Unsubscribe
{"op": "unsubscribe", "queryId": "q1"}
```

**Encoding:** JSON (text). No binary mode. No delta encoding — full document on every update.
