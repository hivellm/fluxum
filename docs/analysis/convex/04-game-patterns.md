# 04 — Game Patterns & Suitability

## What Convex handles well (game use cases)

| Use case | Why it works | Example |
|----------|--------------|---------|
| Turn-based games | 1 update per turn, seconds of latency acceptable | Chess, card games, board games |
| Casual multiplayer | 1–5 updates/s, latency forgiving | Drawing games, trivia |
| Persistent world state | Player profiles, inventories, leaderboards | Any RPG backend |
| Matchmaking queues | Low-frequency writes, reactive reads | Lobby systems |
| Chat / social features | Low-frequency, latency acceptable | In-game chat |
| Async multiplayer | Players take turns at their leisure | Clash of Clans style |

---

## What Convex cannot handle (game use cases)

| Use case | Why it fails | Required capability |
|----------|--------------|---------------------|
| First-person shooters | <16ms tick, >60Hz updates, player positions | Tick loop, binary protocol, <1ms latency |
| MOBAs (LoL, Dota) | Deterministic simulation, AoI, 30Hz | Single-writer, spatial index |
| MMORPGs (movement) | Thousands of entities, AoI subscriptions | Spatial queries, delta encoding |
| Real-time physics | Server-side game loop, determinism | `@tick`, single-writer model |
| Action RPGs | Fast combat, 20Hz+ state sync | Binary protocol, spatial AoI |
| Competitive multiplayer | Reproducible game state, anti-cheat | Deterministic execution, audit log |

---

## Anti-patterns for game development with Convex

### Anti-pattern 1: High-frequency position sync
```typescript
// WRONG — called 60x per second per player
export const updatePosition = mutation({
  args: { playerId: v.id("players"), x: v.number(), y: v.number() },
  handler: async (ctx, args) => {
    await ctx.db.patch(args.playerId, { x: args.x, y: args.y });
  },
});
// Result: 60 mutations/s × 100 players = 6000 mutations/s
// Convex limit: ~200 mutations/s → 30x over limit
```

### Anti-pattern 2: Global subscription
```typescript
// WRONG — every client subscribes to all players
export const getAllPlayers = query({
  handler: async (ctx) => {
    return await ctx.db.query("players").collect(); // reads ALL players
  },
});
// Result: any player update triggers re-execution for ALL subscribers
// At 100 players × 20 updates/s: 2000 full re-executions/s
```

### Anti-pattern 3: Server-side game loop
```typescript
// IMPOSSIBLE — Convex has no 60Hz tick
// Workaround: schedule a mutation every X milliseconds
// Minimum practical interval: ~1000ms (cron-like)
// 60Hz game loop: NOT achievable
```

### Anti-pattern 4: Spatial queries
```typescript
// WRONG — simulating AoI with filter
export const getNearbyPlayers = query({
  args: { x: v.number(), y: v.number(), radius: v.number() },
  handler: async (ctx, { x, y, radius }) => {
    const all = await ctx.db.query("players").collect(); // FULL TABLE SCAN
    return all.filter(p =>
      Math.sqrt((p.x - x) ** 2 + (p.y - y) ** 2) <= radius
    );
  },
});
// Result: O(n) per query. At 10,000 players: 10,000 distance calculations
// No spatial index possible in Convex
```

---

## Real-world game performance analysis

### Scenario: 100-player real-time game, 20 updates/s

| Metric | Convex | SpacetimeDB | UzDB target |
|--------|--------|-------------|-------------|
| Updates/s (server) | 2,000 mut/s | 150,000 tx/s | >100,000 tx/s |
| Bandwidth per client | ~1MB/s (JSON, full doc) | ~1KB/s (BSATN, delta) | ~1KB/s (UzBIN, delta) |
| Subscription push latency | 20–150ms | 1–5ms | <1ms |
| End-to-end move latency | 100–400ms | 5–20ms | <10ms |
| Feasibility | Marginal (at limit) | Excellent | Target |

### Scenario: 1000-player MMORPG, 5 updates/s per player

| Metric | Convex | SpacetimeDB | UzDB target |
|--------|--------|-------------|-------------|
| Total updates/s | 5,000 mut/s | Feasible | Feasible |
| Fan-out per update | 1000 clients × full JSON | AoI clients × delta rows | AoI clients × delta rows |
| Bandwidth (server egress) | ~5GB/s | ~5MB/s | ~5MB/s |
| Feasibility | **Impossible** | Feasible | Target |

**Conclusion:** Convex hits a hard wall at MMORPG scale. The JSON + full-document model means
server egress grows as O(players² × update_rate). SpacetimeDB's delta + AoI model keeps it
O(aoi_players × update_rate) — orders of magnitude better.

---

## What UzDB can learn from Convex

Despite the limitations, Convex has excellent UX patterns that UzDB should replicate:

### 1. Typed function declarations
```typescript
// Convex: TypeScript types flow from schema to functions to client
export const getPlayer = query({
  args: { id: v.id("players") },    // type-safe args
  returns: v.object({ name: v.string(), level: v.number() }),
  handler: async (ctx, { id }) => ctx.db.get(id),
});
```
UzDB equivalent: `@reducer` with typed TML signatures, schema-generated client SDKs.

### 2. Automatic client code generation
Convex generates typed TypeScript wrappers from function definitions.
UzDB must do the same: `uzdb generate --lang ts` from the TML schema.

### 3. Reactive hooks (React pattern)
```typescript
// Convex: one line for real-time subscription
const players = useQuery(api.players.getAll);
```
UzDB SDK should provide equivalent hooks for common frameworks.

### 4. Optimistic updates pattern
```typescript
// Convex: client can show optimistic result before server confirms
const sendMessage = useMutation(api.messages.send).withOptimisticUpdate(
  (localStore, args) => {
    localStore.setQuery(api.messages.list, {}, [...current, args]);
  }
);
```
UzDB SDK should support the same pattern for responsive game UI.
