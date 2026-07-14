# 08 — MMORPG Patterns (BitCraft Online)

## BitCraft Online — the proof of concept

BitCraft Online is a crafting/survival MMORPG built by Clockwork Labs (the same team that built
SpacetimeDB) specifically to validate the SpacetimeDB architecture. It is the primary evidence
that the DB-as-server model works at MMORPG scale.

**Key facts:**
- Entire backend runs as a single SpacetimeDB module
- Handles: chat, inventory, terrain, player positions, crafting, economy — all in one module
- Synchronizes to thousands of concurrent players in real-time
- **150,000 tx/s** measured throughput (vs ~1,500 tx/s for Node.js + PostgreSQL equivalent)

---

## Core MMORPG patterns extracted

### 1. Entity-Component via tables
Instead of a classical ECS with component arrays, SpacetimeDB represents entities as rows
across multiple tables joined by a shared entity ID:

```rust
// Entity identity table
#[spacetimedb::table(name = entities, public)]
struct Entity {
    #[primary_key] #[auto_inc]
    id: u64,
}

// Position component — separate table, joined by entity_id
#[spacetimedb::table(name = positions, public)]
struct Position {
    #[primary_key]
    entity_id: u64,
    x: f32,
    y: f32,
    z: f32,
}

// Health component
#[spacetimedb::table(name = health, public)]
struct Health {
    #[primary_key]
    entity_id: u64,
    current: u32,
    max: u32,
}
```

**Observation:** This is a relational ECS. Queries like "find all entities with position AND health
near (x,y)" require joining across tables. SpacetimeDB does not optimize this join in subscriptions
(limitation). UzDB should consider native ECS primitives.

### 2. Spatial interest management (area of interest)
Players only need to receive updates for entities near their character.
SpacetimeDB implements this via RLS filters:

```rust
#[spacetimedb::client_visibility_filter]
fn position_filter(ctx: &ReducerContext, pos: &Position) -> bool {
    // Only send positions within 100 units of the subscribing player
    if let Some(my_pos) = Position::filter_by_entity_id(&ctx.sender_entity_id()) {
        let dx = pos.x - my_pos.x;
        let dy = pos.y - my_pos.y;
        (dx * dx + dy * dy).sqrt() < 100.0
    } else {
        false
    }
}
```

**Limitation:** this filter runs for every row on every update, which is O(n×m) — expensive as
entity count grows. UzDB should implement a proper spatial index (quadtree/R-tree) to compute
area-of-interest in O(log n).

### 3. Chunk-based terrain
World terrain is chunked and stored as rows:

```rust
#[spacetimedb::table(name = terrain_chunks, public)]
struct TerrainChunk {
    #[primary_key]
    chunk_x: i32,
    #[primary_key]
    chunk_y: i32,
    tile_data: Vec<u8>,  // compressed tile array for this 32×32 chunk
    version: u64,
}
```

Players subscribe to chunks in their current area:
```sql
SELECT * FROM terrain_chunks WHERE chunk_x BETWEEN ? AND ? AND chunk_y BETWEEN ? AND ?
```

When terrain changes (mining, building), only the affected chunk row is updated.
Subscribers to that chunk receive the updated `tile_data`.

### 4. Player session management
```rust
// Online player tracking
#[spacetimedb::table(name = online_players, public)]
struct OnlinePlayer {
    #[primary_key]
    identity: Identity,
    connection_id: ConnectionId,
    last_seen: Timestamp,
}

#[spacetimedb::reducer]
fn __connect__(ctx: &ReducerContext) {
    OnlinePlayer::insert(OnlinePlayer {
        identity: ctx.sender,
        connection_id: ctx.connection_id,
        last_seen: ctx.timestamp,
    });
}

#[spacetimedb::reducer]
fn __disconnect__(ctx: &ReducerContext) {
    OnlinePlayer::delete_by_identity(&ctx.sender);
}
```

### 5. Game loop via scheduled reducers
```rust
#[spacetimedb::reducer]
fn game_tick(ctx: &ReducerContext) {
    // Process all entities that need updating this tick
    for entity in ActiveEntities::iter() {
        process_entity(ctx, &entity);
    }
    // Reschedule for next tick (60 Hz = ~16ms)
    ctx.schedule_reducer_after(Duration::from_millis(16), "game_tick", &());
}

#[spacetimedb::reducer]
fn __init__(ctx: &ReducerContext) {
    // Start the game loop on first deploy
    ctx.schedule_reducer_after(Duration::from_millis(16), "game_tick", &());
}
```

**Observation:** All game logic runs server-side inside the database. The tick rate is limited by
transaction throughput. At 150,000 tx/s, a 16ms tick has budget for ~2,400 entity updates per tick.

### 6. Inventory and item system
```rust
#[spacetimedb::table(name = items)]  // private — only owner sees their items
struct Item {
    #[primary_key] #[auto_inc]
    id: u64,
    owner_identity: Identity,
    item_type: u32,
    quantity: u32,
    slot: u32,
}

// RLS: each client only sees their own items
#[spacetimedb::client_visibility_filter]
fn item_filter(ctx: &ReducerContext, item: &Item) -> bool {
    item.owner_identity == ctx.sender
}

#[spacetimedb::reducer]
fn pick_up_item(ctx: &ReducerContext, world_item_id: u64) {
    // Atomic: remove from world, add to inventory
    let world_item = WorldItem::filter_by_id(&world_item_id)
        .expect("item not found");
    WorldItem::delete_by_id(&world_item_id);
    Item::insert(Item {
        id: 0,  // auto_inc
        owner_identity: ctx.sender,
        item_type: world_item.item_type,
        quantity: 1,
        slot: find_free_slot(&ctx.sender),
    });
}
```

### 7. Chat system
```rust
#[spacetimedb::table(name = chat_messages, public)]
struct ChatMessage {
    #[primary_key] #[auto_inc]
    id: u64,
    sender: Identity,
    channel: u32,
    content: String,
    timestamp: Timestamp,
}

#[spacetimedb::reducer]
fn send_message(ctx: &ReducerContext, channel: u32, content: String) {
    assert!(content.len() <= 500, "message too long");
    ChatMessage::insert(ChatMessage {
        id: 0,
        sender: ctx.sender,
        channel,
        content,
        timestamp: ctx.timestamp,
    });
}
```

Clients subscribe to a channel: `SELECT * FROM chat_messages WHERE channel = 1 ORDER BY timestamp DESC LIMIT 100`

---

## Performance analysis

### Throughput
- 150,000 tx/s vs 1,500 tx/s (100× advantage over Node.js+PostgreSQL)
- In-memory removes disk I/O from the critical path
- No serialization/deserialization overhead between application and DB layers
- No network hop between "server" and "database" (they are the same process)

### Latency
- Sub-microsecond reads (CommittedState is in RAM)
- Write latency: transaction commit + subscription broadcast (microseconds to low milliseconds)
- Client-perceived latency: one WebSocket round-trip for reducer calls

### Scalability limits
- Single-node: bounded by RAM (all data in memory)
- Single-writer: one reducer at a time per database instance
- Subscription fan-out: O(clients × delta_rows) per commit

### What breaks at MMO scale (10k+ concurrent players)
1. **RAM:** a world with 10k players × 100 entities each × 1KB per entity = ~1GB just for entity data. Manageable.
2. **Subscription fan-out:** 10k clients × 100 delta rows per tick × 16ms tick = fan-out becomes the bottleneck.
3. **Single-writer throughput:** 150k tx/s / 10k players = 15 reducers per player per second — barely enough for 60Hz movement updates.
4. **Spatial queries:** area-of-interest without a spatial index is O(n) per client per tick.

---

## Key design decisions for UzDB derived from this analysis

1. **Native spatial indexing** — quadtree or R-tree as a first-class index type, not a filter workaround
2. **Multi-shard architecture** — design shard boundaries at the world-region level from day one
3. **Interest management built-in** — AoI (Area of Interest) as a DB primitive, not application logic
4. **ECS-aware schema** — native entity-component tables with efficient cross-component queries
5. **Tick scheduler** — built-in game loop scheduler, not just general `schedule_reducer`
6. **Higher write concurrency** — explore multi-writer sharding within a single region
