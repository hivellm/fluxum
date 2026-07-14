# 04 — Game Patterns & Suitability

## What SurrealDB handles well

| Use case | Why it works | Example |
|----------|--------------|---------|
| Player entity storage | Flexible schema, ACID writes | Profile, stats, equipment |
| Inventory system | Graph edges RELATE player → item | `RELATE p1 → owns → sword1` |
| Guild / social graph | RELATE player → member_of → guild | Party, friend lists |
| Quest trees | Graph traversal of prerequisite chains | `FETCH prerequisites→quest` |
| Leaderboards | Aggregation queries | `SELECT name, score ORDER BY score DESC` |
| Match history | Document model, append | `INSERT INTO match SET ...` |
| Atomic trades | `BEGIN ... COMMIT` multi-entity | Transfer item + gold atomically |
| World map regions | Document records with geo fields | Zone data, spawn points |

---

## Graph model for MMORPG data

SurrealDB's graph model maps naturally to many MMORPG data structures that are
awkward in a pure relational model.

### Inventory (directed graph)
```sql
-- Setup
DEFINE TABLE item SCHEMAFULL;
DEFINE TABLE owns SCHEMALESS;  -- edge table

-- Player acquires item
RELATE player:p1 -> owns -> item:sword_001
  SET quantity = 1, acquired_at = time::now();

-- Query: all items owned by player p1
SELECT *, ->owns->item.* AS items FROM player:p1;

-- Query: how many of each item type in the whole game
SELECT item.type, math::sum(quantity) AS total
FROM owns GROUP BY item.type;

-- Transfer item: atomic trade
BEGIN TRANSACTION;
DELETE (player:p1)->owns WHERE out = item:sword_001;
RELATE player:p2 -> owns -> item:sword_001 SET quantity = 1;
UPDATE player:p1 SET gold += 50;
UPDATE player:p2 SET gold -= 50;
COMMIT;
```

### Skill tree (graph traversal)
```sql
-- Skills have prerequisites
RELATE skill:fireball -> requires -> skill:fire_bolt;
RELATE skill:fire_bolt -> requires -> skill:mana_control;

-- Can player learn fireball? Check all prerequisites
SELECT * FROM skill:fireball
FETCH requires, requires->requires
WHERE player:p1.learned_skills CONTAINSALL requires.id;
```

### Guild hierarchy
```sql
RELATE player:alice -> leads -> guild:fire_order;
RELATE player:bob -> member_of -> guild:fire_order;
RELATE player:carol -> member_of -> guild:fire_order;

-- Get all guild members and their roles
SELECT * FROM guild:fire_order
  <- member_of <- player AS members,
  <- leads <- player AS leaders;
```

---

## Geospatial queries

SurrealDB has built-in geometry types and functions — the closest any general-purpose
database gets to game AoI queries.

```sql
-- Player position stored as geo Point
DEFINE FIELD player.position TYPE geometry<point>;

-- Find players within 100 units
SELECT * FROM player
WHERE geo::distance(position, <geometry<point>> (20.5, 30.1)) < 100;

-- Find players within a zone polygon
SELECT * FROM player
WHERE geo::within(position, <geometry<polygon>> [[0,0], [100,0], [100,100], [0,100], [0,0]]);

-- Find all items within player's pickup range
SELECT * FROM item
WHERE geo::distance(world_position, player:p1.position) < 2;
```

### Why geo:: functions are insufficient for MMORPGs

```
Problem: geo::distance() is a FILTER — it runs on every row.
At 10,000 players: 10,000 distance calculations per query.

SpacetimeDB / UzDB approach:
- QuadTree spatial index: INSERT updates the tree
- AoI query: tree lookup → O(log n + k) where k = nearby entities
- Result: 10,000 players, query returns 50 nearby → only 50 rows touched

SurrealDB approach:
- No spatial index — B-tree only
- geo::distance() in WHERE → full table scan
- 10,000 players, nearby query → 10,000 distance calculations every time
```

SurrealDB acknowledges this gap: vector indexes exist for ML similarity search,
but not for 2D/3D game-world spatial proximity.

---

## Performance benchmarks (from surrealdb.com)

| Operation | Latency |
|-----------|---------|
| INSERT single row | 0.72ms |
| SELECT single row | 1.94ms |
| UPDATE single row | 2.58ms |
| DELETE single row | 0.73ms |
| RELATE (edge creation) | 2.46ms |
| LIVE query initial | 0.68ms |
| **LIVE notification delivery** | **39.07ms** |
| Graph traversal (2-hop) | 18.02ms |
| Complex JOIN | 78.83ms |
| Full-text search | 148.68ms |

### Game workload analysis: 100 players, 20 position updates/s

| Metric | SurrealDB | SpacetimeDB | UzDB target |
|--------|-----------|-------------|-------------|
| Writes/s | 2,000 tx/s | 150,000 tx/s | >100,000 tx/s |
| Notification latency | ~39ms | 1–5ms | <1ms |
| Payload per update | ~500 bytes (full doc, JSON) | ~50 bytes (delta row, BSATN) | ~50 bytes (delta, UzBIN) |
| Server egress (100 clients subscribed) | ~1MB/s | ~100KB/s | ~100KB/s |
| Feasibility | Marginal | Excellent | Target |

### Game workload analysis: 1000 players, 10 updates/s (AoI = 50 nearby)

| Metric | SurrealDB | SpacetimeDB | UzDB target |
|--------|-----------|-------------|-------------|
| Fan-out per update | 1000 clients (no AoI) | ~50 clients (AoI) | ~50 clients (AoI) |
| Server egress | 10GB/s (impossible) | ~25MB/s | ~25MB/s |
| Feasibility | **Impossible** | Feasible | Target |

---

## Common anti-patterns when using SurrealDB for games

### Anti-pattern 1: Using LIVE SELECT for position sync
```sql
-- WRONG: All clients receive all player updates
LIVE SELECT position FROM player;

-- At 1000 players: every move triggers 1000 full-doc notifications
-- Correct approach: Application server handles position sync via UDP/custom protocol
```

### Anti-pattern 2: Relying on SurrealDB as the game loop
```
WRONG: Game server polls SurrealDB every tick:
while (running) {
  const state = await db.query("SELECT * FROM entities");
  processGameLogic(state);
  await db.query("UPDATE entities SET ...");
  sleep(16ms); // 60Hz
}

Problems:
- SELECT all entities: O(n) read per tick
- Every UPDATE triggers notifications to all subscribers
- Database becomes the bottleneck for game simulation
```

### Anti-pattern 3: Trusting notification ordering
```
WRONG:
player.on("update", (data) => {
  player.position = data.position; // directly trusting server data
});

PROBLEM: Notifications may arrive out of order.
Player moves: (0,0) → (10,0) → (20,0)
Client may receive: (20,0) then (10,0) — visual glitch
```

---

## Recommended architecture: SurrealDB + Game Server

The correct pattern when using SurrealDB for a real-time game:

```
┌─────────────────────────────────────────────────────┐
│                  GAME CLIENTS                       │
│  UDP/WebSocket → Game Server (positions, combat)    │
│  HTTP/WebSocket → SurrealDB (inventory, chat, auth) │
└──────────┬──────────────────────┬───────────────────┘
           │ UDP/WebSocket        │ SurrealDB WS
┌──────────▼──────────┐  ┌───────▼────────────────────┐
│   GAME SERVER       │  │      SURREALDB              │
│   (Rust/Go/C++)     │  │                             │
│   - Tick loop       │  │   - Player profiles         │
│   - Physics         │  │   - Inventories (graph)     │
│   - AoI management  │  │   - Guilds, quests          │
│   - Fast positions  │  │   - Chat history            │
│   - Combat          │  │   - Leaderboards            │
│                     │  │   - Auth                    │
└─────────────────────┘  └─────────────────────────────┘
```

**This is the architecture SpacetimeDB and UzDB eliminate by design.**
UzDB's goal: the left box (Game Server) does not exist — all logic runs inside the database.
