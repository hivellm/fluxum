# 05 — Mapping to UzDB / TML

## Mapping strategy

- **ADOPT** — take the concept as-is, implement the same design in TML
- **ADAPT** — take the concept but modify it to fit TML or improve on SurrealDB's gaps
- **DISCARD** — SurrealDB's approach does not apply or is superseded by TML/UzDB
- **IMPROVE** — SurrealDB has the right idea but UzDB can do it better

---

## Architecture mapping

| SurrealDB concept | UzDB / TML mapping | Category |
|------------------|--------------------|----------|
| Single binary deployment | Same — `uzdb` single binary | ADOPT |
| Self-hosted (no cloud lock-in) | Same — UzDB must be fully self-hostable | ADOPT |
| Pluggable storage backends | `MemStore` (hot) + `CommitLog` (durable) + cold storage plugin | ADAPT |
| In-memory backend (kv-mem) | `MemStore` — all game state in RAM | ADOPT |
| RocksDB backend (kv-rocksdb) | `CommitLog` append-only log (lighter than RocksDB for games) | ADAPT |
| TiKV distributed backend | Multi-shard with `ShardCoordinator` | ADAPT |
| Axum HTTP + WebSocket server | Same pattern: HTTP reducers + WS subscriptions | ADOPT |
| WASM runtime (Surrealism) | TML native compilation — no WASM FFI | DISCARD |

---

## Data model mapping

| SurrealDB concept | UzDB / TML mapping | Category |
|------------------|--------------------|----------|
| Typed tables (SCHEMAFULL) | `@table` type declarations in TML | ADOPT |
| Schemaless tables (SCHEMALESS) | Not supported — UzDB requires typed schemas | DISCARD |
| Graph edges (RELATE) | `@relation` annotation — typed graph edges | ADOPT |
| Record IDs (`player:john`) | `@pk` with typed ID: `PlayerId`, `ItemId` | ADAPT |
| Graph traversal (FETCH) | `JOIN` + graph-aware query extension | ADAPT |
| Geometry types | `Vec2`, `Vec3`, `Rect`, `Circle` — first-class game types | IMPROVE |
| LIVE SELECT subscriptions | SQL subscriptions with compiled query plans | ADOPT |
| `geo::distance()` function | Spatial index lookup (O(log n)) — not O(n) scan | IMPROVE |
| `geo::within()` function | `WITHIN RADIUS N OF POINT` — index-accelerated | IMPROVE |
| Built-in math/crypto functions | TML stdlib: math, crypto, rand modules | ADOPT |
| `value::diff()` / `value::patch()` | Row-level delta at subscription layer (automatic) | IMPROVE |

### Graph edges: ADOPT recommendation

SurrealDB's RELATE pattern maps to a real need in MMORPG data:
```tml
// UzDB equivalent of SurrealDB RELATE player -> owns -> item
@relation
type Owns {
    @pk id: OwnsId,
    @autoInc
    player: PlayerId,   // from
    item: ItemId,       // to
    quantity: U32,
    acquiredAt: Timestamp,
}
```

The `@relation` annotation marks this as a graph edge, enabling:
- Graph traversal queries: `SELECT * FROM Owns JOIN Item WHERE Owns.player = $playerId`
- Bidirectional queries: find all items a player owns, find all players who own an item
- Atomic edge operations within reducer transactions

---

## Real-time / subscription mapping

| SurrealDB concept | UzDB / TML mapping | Category |
|------------------|--------------------|----------|
| `LIVE SELECT` subscription | SQL subscription with compiled query plan | ADOPT |
| WebSocket delivery of change events | WebSocket push (same) | ADOPT |
| Full document per notification | Row-level delta diffs: INSERT/DELETE/UPDATE columns | IMPROVE |
| No server-side WHERE filtering | Server-side RLS + spatial AoI filter | IMPROVE |
| ~39ms notification latency | <1ms (in-process push after commit, zero network hop) | IMPROVE |
| No ordering guarantee | Commit-log-ordered push, monotonic sequence numbers | IMPROVE |
| Client-side filtering | Server computes per-client diff, sends only relevant rows | IMPROVE |
| No AoI subscription | `WITHIN RADIUS N OF SELF` spatial subscriptions | IMPROVE |
| Re-subscribe for moving AoI | AoI subscription auto-tracks as entity moves | IMPROVE |

---

## Transaction mapping

| SurrealDB concept | UzDB / TML mapping | Category |
|------------------|--------------------|----------|
| `BEGIN / COMMIT / CANCEL` | Implicit per-reducer transaction (no explicit BEGIN) | ADAPT |
| Optimistic MVCC | MVCC snapshot isolation per shard | ADOPT |
| Pessimistic locking | Not supported — single-writer per shard eliminates need | DISCARD |
| Auto-retry on conflict | **No auto-retry** — single-writer eliminates conflicts | DISCARD |
| Multi-table atomic transaction | Same — reducer can touch multiple tables atomically | ADOPT |
| No distributed transactions | No cross-shard transactions | ADOPT |

**Why DISCARD pessimistic locking and auto-retry:**
Single-writer model (one reducer at a time, per shard) means:
- No concurrent writers → no write-write conflicts → no retry needed
- Execution is deterministic and replayable from commit log
- Simpler implementation, stronger guarantees for game correctness

---

## Function / logic mapping

| SurrealDB concept | UzDB / TML mapping | Category |
|------------------|--------------------|----------|
| Surrealism WASM functions | TML `@reducer` functions (compiled natively) | ADAPT |
| On-demand function execution | Atomic reducer execution (client-triggered) | ADAPT |
| Built-in `geo::*` functions | Spatial indexes + TML query predicates | ADAPT |
| Built-in `http::*` functions | `@procedure` with TML stdlib HTTP client | ADOPT |
| Built-in `crypto::*` functions | TML stdlib crypto module | ADOPT |
| Built-in `rand::*` functions | TML stdlib rand module | ADOPT |
| No server-side tick loop | `@tick(rate: 60hz)` declarative game loop | IMPROVE |
| No scheduled execution | `@schedule(cron: "*/5 * * * *")` | IMPROVE |

---

## Summary: what UzDB takes from SurrealDB

### ADOPT (implement the same)
1. **Self-hosted single binary** — no cloud lock-in, studio-deployable
2. **LIVE-SELECT style push subscriptions** — declare intent, receive change events
3. **Graph relations (`@relation`)** — MMORPG data (inventory, guilds, quests) maps naturally
4. **MVCC snapshot isolation** — correct isolation level for concurrent game clients
5. **Multi-table atomic transactions** — atomic reducer spans all tables in a shard
6. **WebSocket + HTTP dual channel** — WS for subscriptions, HTTP for reducer calls
7. **Pluggable storage concept** — hot path (MemStore), durable path (CommitLog)
8. **Rich built-in function library** — crypto, rand, math as first-class stdlib

### IMPROVE (SurrealDB has the right idea, UzDB does it better)
1. **Notification latency:** in-process push <1ms vs ~39ms pipeline
2. **Delta encoding:** row-level diffs vs full document per update
3. **Spatial queries:** QuadTree/R-tree index (O(log n)) vs geo:: scan (O(n))
4. **AoI subscriptions:** server-managed spatial subscriptions, auto-tracking
5. **Subscription filtering:** server-side per-client filters vs client-side
6. **Ordering guarantees:** commit-log-ordered push with sequence numbers
7. **Game loop:** `@tick(60hz)` in-process vs no tick support
8. **Execution determinism:** single-writer per shard vs MVCC auto-retry

### DISCARD
1. **Schemaless mode** — UzDB requires typed schemas for type-safe client SDK generation
2. **WASM function runtime** — TML native compilation eliminates FFI overhead
3. **Pessimistic locking** — single-writer model makes it unnecessary
4. **Auto-retry on conflict** — single-writer eliminates conflicts entirely
5. **No ordering guarantee** — linearizable commit-log ordering is required for games
6. **Client-side notification filtering** — all filtering must be server-side at game scale

---

## Final verdict for UzDB

SurrealDB is the most architecturally sophisticated competitor to UzDB among general-purpose
databases. It is written in Rust, is self-hosted, has WASM modules, graph queries, and LIVE
subscriptions — all features UzDB must have.

The critical gaps are:
1. **~39ms notification latency** (UzDB eliminates the network hop — <1ms)
2. **No server-side tick loop** (UzDB's `@tick` is the fundamental game primitive)
3. **Full document per notification** (UzDB uses row-level delta diffs)
4. **O(n) geo queries** (UzDB has spatial indexes as first-class primitives)
5. **Non-deterministic execution** (UzDB's single-writer is deterministic + replayable)

SurrealDB is an excellent **persistence + social layer** for a game. It is NOT a game server.
UzDB is both — and that is what makes it different.
