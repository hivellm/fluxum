# 03 — Real-time Model

## Subscription pipeline

```
1. Client calls useQuery(api.players.getAll, {})
2. Convex HTTP: executes query, returns initial result as JSON
3. Client opens WebSocket to /subscribe
4. SubscriptionCoordinator: records (queryId → function + args)
   and tracks which documents/tables the query read

5. [Any mutation commits]
6. Convex: checks which subscriptions depend on modified documents
7. For each affected subscription:
   a. Re-executes the query function with original args
   b. Compares new result with previous result (full diff)
   c. Sends full new result via WebSocket (NOT a delta)
8. Client library replaces its local cache entry
9. React: component re-renders automatically
```

---

## Reactivity model: dependency tracking

Convex tracks which documents each query reads during execution.
When a mutation writes to any of those documents, all subscriptions
that read them are invalidated and re-executed.

```
Query execution:    reads Player#abc, Player#def, Players-index
Mutation writes:    Player#abc (position update)
Result:             subscription invalidated → re-execute → push new result
```

### Granularity
- **Document-level tracking:** if a query reads 1000 players, any update to
  any of those 1000 players triggers a full re-execution
- **No field-level tracking:** updating `position.x` of Player#abc is the same
  cost as updating the entire document
- **No spatial optimization:** "players within 100 units" = reads ALL players =
  any player update anywhere triggers re-execution

---

## WebSocket protocol

### Message types

```json
// Client → Server: subscribe to a query
{
  "op": "subscribe",
  "queryId": "q1",
  "query": "players:getAll",
  "args": {}
}

// Server → Client: initial state (after subscribe)
{
  "op": "initialResult",
  "queryId": "q1",
  "value": [{"_id": "abc", "x": 0, "y": 0}, ...]
}

// Server → Client: data changed, full new result
{
  "op": "update",
  "queryId": "q1",
  "value": [{"_id": "abc", "x": 10, "y": 0}, ...]  // full array, not delta
}

// Client → Server: unsubscribe
{
  "op": "unsubscribe",
  "queryId": "q1"
}
```

### Critical gap: no delta encoding
Every subscription update sends the **full result set**, not just changed rows.

For a game with 100 visible players:
- Player "abc" moves 1 step
- Convex sends: all 100 player objects (JSON, ~500 bytes each)
- **Bandwidth per update: ~50KB** for a single player movement
- At 20 updates/s: **1MB/s per subscribing client**

SpacetimeDB sends only the changed row(s):
- Same player moves 1 step
- SpacetimeDB sends: 1 row delta (~50 bytes BSATN)
- **Bandwidth per update: ~50 bytes**
- At 20 updates/s: **1KB/s per subscribing client**

**Ratio: Convex uses ~1000× more bandwidth than SpacetimeDB for game workloads.**

---

## Latency profile

| Operation | p50 | p99 | Notes |
|-----------|-----|-----|-------|
| Query (HTTP) | 50–100ms | 200–500ms | Cloud round-trip |
| Mutation (HTTP) | 50–200ms | 200–500ms | Includes V8 + DB write |
| Subscription push | 20–150ms | 150–300ms | After mutation commits |
| End-to-end (client sees own mutation) | 100–400ms | 400–1000ms | Two round-trips |

For comparison:
- SpacetimeDB (co-located): 1–5ms for reducer + subscription push
- UzDB target: <1ms for reducer + subscription push (in-process)

**Convex latency is fundamentally cloud-bound** — irreducible regardless of code optimization.
The physical speed-of-light round-trip to a US-East datacenter from Europe is ~80ms alone.

---

## Guarantees

| Property | Convex | SpacetimeDB | Notes |
|----------|--------|-------------|-------|
| No missed updates | Yes | Yes | |
| No duplicate updates | Yes | Yes | |
| Causal consistency | Yes | Yes | A happens before B → all clients see A before B |
| Read-your-writes | Yes | Yes | |
| Linearizability | **No** | Yes | No strict global ordering across clients |
| Ordering of concurrent mutations | Non-deterministic | Deterministic | MVCC vs single-writer |

---

## Subscription scalability

### What Convex handles well
- Thousands of concurrent subscribers
- Low-frequency updates (1–5/s per subscription)
- Subscriptions to different data sets (fan-out is bounded)

### What breaks at game scale

**Scenario:** 1000-player game room, all subscribed to `getPlayers()`
- Every player movement → re-execute `getPlayers()` for ALL 1000 subscribers
- Re-execute sends 1000 player JSON objects × 1000 clients = **1M objects serialized per event**
- At 20 events/second: **20M serializations/s** — this saturates the platform

SpacetimeDB handles this via:
1. Per-client subscriptions with `WHERE` filters (only see nearby players)
2. Delta encoding (only changed rows)
3. Compiled SQL plans evaluated against delta rows only (O(delta) not O(all))

Convex has no equivalent mechanism.
