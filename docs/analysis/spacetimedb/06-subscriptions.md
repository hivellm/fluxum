# 06 — Subscriptions & Client Sync

## Overview

The subscription system is SpacetimeDB's most distinctive feature for real-time applications.
Instead of clients polling for changes, they declare interest in data via SQL queries,
and the server pushes incremental diffs automatically whenever the relevant data changes.

This eliminates:
- Polling loops
- Manual cache invalidation
- Custom WebSocket message handling
- Client-side state reconciliation logic

---

## Subscription lifecycle

```
1. Client connects via WebSocket to /database/:name/subscribe
2. Client sends: Subscribe { query_strings: ["SELECT * FROM players WHERE zone_id = 5"] }
3. SubscriptionManager compiles each SQL string into an execution plan (once)
4. SubscriptionManager registers: { client_id → [compiled_plan_1, compiled_plan_2, ...] }
5. Server sends initial state: all rows matching the query at subscription time

On every reducer commit:
6. ModuleSubscriptions.commit_and_broadcast_event() is called
7. Delta rows (inserted + deleted rows) are computed from TxState
8. Each registered plan is evaluated against the delta rows
9. For each client: matching delta rows are serialized (BsatnRowListBuilderPool)
10. WebSocket push: TransactionUpdate { inserts: [...], deletes: [...] }

On unsubscribe or disconnect:
11. Client registration is removed from SubscriptionManager
```

---

## Query compilation

Subscriptions use SQL as the query language. Queries are compiled once at subscription time:

```sql
-- Example subscription queries a client might send:
SELECT * FROM players WHERE zone_id = ?zone
SELECT * FROM items WHERE owner_id = ?identity
SELECT * FROM chat_messages WHERE channel_id = 1 ORDER BY timestamp DESC LIMIT 100
```

The compiled plan is a physical query plan (similar to a prepared statement) that is evaluated
against delta rows on every commit. This is much cheaper than re-parsing SQL on every update.

### Query limitations observed
- `JOIN` support is limited — SpacetimeDB recommends denormalization over joins in subscriptions
- Aggregation (`COUNT`, `SUM`) is not supported in subscriptions (only in views/SQL endpoint)
- `ORDER BY` + `LIMIT` semantics in subscriptions can be surprising (applies to the initial set, not to diffs)

---

## Row-Level Security (RLS)

SpacetimeDB supports **row-level security filters** that restrict which rows a client can subscribe to.
This is how private data (player inventory, private messages) is protected.

### How it works
```rust
// RLS filter declared in the module
#[spacetimedb::client_visibility_filter]
fn player_filter(ctx: &ReducerContext, player: &Player) -> bool {
    // A client can only see their own player row
    player.identity == ctx.sender
}
```

The filter is:
1. Declared in the module and compiled into WASM
2. Called by the host during subscription evaluation
3. Applied before rows are sent to the client — the client never receives rows that fail the filter
4. Applied to both the initial state and incremental updates

### RLS implications for game design
- Public tables (terrain, world events) need no filter
- Player-private tables (inventory, stats) use `identity == ctx.sender`
- Scoped tables (party members, guild) use group membership checks
- Admin tables have no client-visible filter (never exposed via subscriptions)

---

## Client-side state (SDK cache)

SpacetimeDB client SDKs maintain a **local cache** that mirrors the subscribed server state:

```
SDK Local Cache
├── Table: players    → [row1, row2, ...]   (all rows matching subscription)
├── Table: items      → [row1, row2, ...]
└── Table: zones      → [row1, row2, ...]

On TransactionUpdate received:
  for each insert: add row to local table
  for each delete: remove row from local table
  fire onChange callbacks / reactive signals
```

This means:
- Client reads are always local (zero network latency for reads)
- Client writes go through reducers (one network round-trip)
- The client sees a eventually-consistent view of the server state
- Consistency: the client always sees the state at a specific committed transaction

---

## Subscription protocol messages (WebSocket)

### Client → Server
```
Subscribe {
  query_strings: Vec<String>,   // SQL subscription queries
  request_id: u32
}

Unsubscribe {
  query_id: u32,
  request_id: u32
}

CallReducer {
  reducer: String,              // reducer name
  args: Bytes,                  // BSATN-encoded arguments
  request_id: u32,
  flags: CallReducerFlags
}
```

### Server → Client
```
InitialSubscription {
  database_update: DatabaseUpdate,  // initial rows for all subscribed queries
  request_id: u32,
  total_host_execution_duration_micros: u64
}

TransactionUpdate {
  status: UpdateStatus,         // Committed | Failed | OutOfEnergy
  timestamp: Timestamp,
  caller_identity: Identity,
  caller_connection_id: ConnectionId,
  reducer_call: ReducerCallInfo,
  energy_quanta_used: EnergyQuanta,
  host_execution_duration_micros: u64,
  database_update: DatabaseUpdate
}

DatabaseUpdate {
  tables: Vec<TableUpdate>
}

TableUpdate {
  table_id: u32,
  table_name: String,
  inserts: Vec<Row>,            // BSATN-encoded rows
  deletes: Vec<Row>
}
```

---

## Performance characteristics

- **Fan-out cost:** O(subscriptions × delta_rows) per commit — can be expensive at scale
- **Mitigation:** compiled query plans avoid re-parsing; `BsatnRowListBuilderPool` pools allocations
- **Bottleneck at scale:** when thousands of clients subscribe to overlapping data, the broadcast
  loop becomes the limiting factor. SpacetimeDB does not yet have a documented sharding strategy for this.

---

## Implications for UzDB

| SpacetimeDB pattern | UzDB consideration |
|---------------------|--------------------|
| SQL subscription queries | Adopt — but extend with spatial queries (`WITHIN RADIUS`, `IN REGION`) |
| Compiled query plans | Adopt — compile once, evaluate against deltas |
| RLS filters in module code | Adopt — but make the filter DSL more declarative |
| SDK local cache | Adopt — zero-latency reads on client are essential for games |
| TransactionUpdate with inserts+deletes | Adopt — clean diff model |
| Fan-out bottleneck | Design interest management system (spatial partitioning) to limit fan-out |
| No subscription to computed views | Improve — UzDB should support subscribable derived tables |
