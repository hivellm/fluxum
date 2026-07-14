# 03 — Real-time Model (LIVE SELECT)

## LIVE SELECT mechanics

```sql
-- Subscribe to all changes in the player table
LIVE SELECT * FROM player;

-- Returns: live_id (UUID) + initial result set
-- Then: streams CREATE / UPDATE / DELETE events

-- Subscribe with filter
LIVE SELECT * FROM player WHERE team = team:guild_alpha;

-- Kill subscription
KILL "uuid-of-live-query";
```

---

## Notification format

```json
{
  "id": "live-query-uuid",
  "result": {
    "action": "UPDATE",
    "id": "player:p1",
    "data": {
      "id": "player:p1",
      "name": "Alice",
      "position": [20.5, 30.1, 0.0],
      "health": 95,
      "inventory": ["item:sword1", "item:potion"],
      "team": "team:guild_alpha",
      "last_update": "2026-04-14T10:30:00Z"
    }
  }
}
```

**Critical observation:** The `data` field contains the **full document**, every field,
even if only `position` changed. There is no delta encoding — the entire row is serialized
and sent for every update.

For a player with 20 fields, a position update sends all 20 fields over the wire.
At 60Hz with 100 players visible: `100 players × 20 updates/s × ~500 bytes/doc = 1MB/s` per client.

---

## Filtering: WHERE in LIVE SELECT

```sql
-- SurrealDB applies WHERE to initial fetch:
LIVE SELECT * FROM player WHERE health > 0;

-- BUT: change notifications are NOT filtered by WHERE server-side.
-- All changes to `player` trigger evaluation, and clients receive
-- notifications even if the changed row does NOT match the WHERE.

-- Client must filter notifications:
surreal.live("player", (action, data) => {
  if (data.health > 0) { ... }  // client-side filtering
});
```

This means:
- A client subscribed to `WHERE health > 0` still receives notifications for dead players
- Server sends all notifications; filtering is the client's responsibility
- For game AoI: ALL player updates are sent to ALL subscribers regardless of distance

**This is a fundamental scalability problem for MMORPGs.** Without server-side filtering,
fan-out grows as O(players × subscribers) instead of O(aoi_players × subscribers).

---

## Notification latency

| Benchmark | Latency |
|-----------|---------|
| LIVE query initial result | 0.68ms |
| LIVE query update notification | **39.07ms** |
| INSERT single row | 0.72ms |
| UPDATE single row | 2.58ms |
| End-to-end (mutation → client sees update) | ~42ms |

**Analysis for game workloads:**
- 60Hz tick = 16.7ms between ticks
- 39ms notification latency = client is **2.3 ticks behind** by the time they receive an update
- This is **not a network issue** — it is the database notification pipeline latency
- For comparison, SpacetimeDB delivers notifications in 1–5ms (in-process push after commit)
- UzDB target: <1ms (same-process, zero network hop between commit and push)

---

## Ordering and causality

LIVE notifications in SurrealDB have **no guaranteed ordering**:
- Two mutations may commit in order A then B
- Clients may receive notification B before A (async notification loop)
- No sequence numbers in the notification format

For a game, this means:
- Player moves from (0,0) → (1,0) → (2,0)
- Client may receive: position=(2,0), then position=(1,0)
- Client sees player move backwards — desync

SpacetimeDB's single-writer model eliminates this: reducers execute in commit-log order,
subscriptions are pushed in commit order, clients see a linearizable stream.

UzDB must adopt the same guarantee: notifications are pushed in commit-log order,
with a monotonic sequence number per subscription.

---

## Subscription scalability

### What works
- Thousands of concurrent LIVE queries (server maintains registry efficiently)
- Low-frequency updates (chat, inventory changes, leaderboards)
- Different subscriptions on different tables (independent notification channels)

### What breaks at game scale

**Problem 1: Full document fan-out**
```
1000 players, each subscribed to `LIVE SELECT * FROM player`
1 player moves → 1 UPDATE notification generated
→ Sent to all 1000 subscribers as full document (~500 bytes × 1000 = 500KB)
→ At 20 moves/s: 10MB/s server egress per movement event
→ At 1000 players all moving: 10GB/s server egress
→ IMPOSSIBLE
```

**Problem 2: No AoI (Area of Interest) management**
```
In an MMORPG, each player only cares about entities within ~100 units.
SurrealDB has no mechanism to:
- Subscribe "only to players within 100 units of ME"
- Automatically receive/stop notifications as entities move in/out of range
- Compute which clients care about a given row change
```

**Problem 3: O(n) geo queries**
```sql
-- "Find players near me" — requires full table scan:
LIVE SELECT * FROM player WHERE geo::distance(position, $myPos) < 100;

-- As player moves, subscription is NOT updated — it uses $myPos at subscription time.
-- Must re-subscribe every time the player moves.
-- Even the initial scan is O(all players), not O(log n + k).
```

---

## DIFF and PATCH primitives

SurrealDB provides `value::diff()` and `value::patch()` as query functions:

```sql
-- Compute diff between two values
SELECT value::diff(old_state, new_state) FROM history;
-- Returns JSON Patch format diff

-- Apply patch
SELECT value::patch(document, $patch) AS result FROM tmp;
```

However, **these are query functions, not subscription delta encoding**.
The LIVE notification still sends the full document — `value::diff()` is not
applied automatically to subscription pushes.

UzDB must implement delta encoding at the subscription layer, not as a query function:
commit → compute delta rows (from TxState diff) → serialize only changed rows → push.
