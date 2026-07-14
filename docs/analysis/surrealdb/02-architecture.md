# 02 — Architecture

## Layer diagram

```
┌─────────────────────────────────────────────────────┐
│                    CLIENT LAYER                     │
│  JS/TS, Rust, Python, Go, C# SDKs                  │
│  HTTP (queries) + WebSocket (LIVE, RPC)             │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                  NETWORK LAYER                      │
│  surrealdb/server/src/ntw/                          │
│  Axum HTTP server (REST + WebSocket upgrade)        │
│  WebSocket RPC: JSON-RPC 2.0 / binary               │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                    RPC LAYER                        │
│  surrealdb/core/src/rpc/                            │
│  Method enum: Live, Select, Insert, Update, Relate, │
│   Query, Begin, Commit, Cancel, Kill, Run, ...      │
│  RpcState:                                          │
│   live_queries: HashMap<UUID, (WS_ID, Session_ID)>  │
│   notifications() → fan-out loop                   │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                  QUERY ENGINE                       │
│  surrealdb/parser/ → AST → surrealdb/core/exec/     │
│  SurrealQL parser → execution plan → KVS ops        │
│  Built-in functions: geo, crypto, http, math, ...   │
│  WASM functions: Surrealism (Wasmtime)              │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                TRANSACTION LAYER                    │
│  surrealdb/core/src/kvs/tr.rs                       │
│  TransactionType: Read | Write                      │
│  LockType: Optimistic | Pessimistic                 │
│  MVCC: version-based conflict detection + retry     │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                  STORAGE LAYER                      │
│  surrealdb/core/src/kvs/  (pluggable)               │
│  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐ │
│  │kv-mem   │ │kv-rocksdb│ │ kv-tikv  │ │kv-indxdb│ │
│  │(default)│ │(RocksDB) │ │(distrib.)│ │(browser)│ │
│  └─────────┘ └──────────┘ └──────────┘ └─────────┘ │
└─────────────────────────────────────────────────────┘
```

---

## Data flow: LIVE SELECT lifecycle

```
1. Client  →  WebSocket connect to SurrealDB server
2. Client  →  RPC: { "method": "live", "params": {"sql": "LIVE SELECT * FROM player"} }
3. RpcState  →  Execute LIVE query: return initial result + assign live_id (UUID)
4. RpcState.live_queries  →  register (live_id → websocket_id + session_id)
5. Server  →  Client: { "result": { "live_id": "uuid", "initial": [...rows...] } }

[Any mutation on `player` table]
6. KVS transaction commit  →  publish change event to notification channel
7. Notification loop  →  route event to live_queries matching `player`
8. For each matching live_id:
   a. Serialize full row as JSON
   b. Wrap: { "id": live_id, "action": "CREATE|UPDATE|DELETE", "data": { ...full row... } }
   c. Send over WebSocket to registered client

9. Client  →  RPC: { "method": "kill", "params": {"uuid": live_id} }
10. RpcState  →  remove from live_queries registry
```

---

## Transaction model

### MVCC with optimistic / pessimistic locking

```rust
// surrealdb/core/src/kvs/tr.rs
pub enum TransactionType { Read, Write }
pub enum LockType { Optimistic, Pessimistic }

// TransactionBuilderFactory trait — all KVS backends implement this
trait TransactionBuilderFactory {
    async fn transaction(&self, tx_type: TransactionType, lock: LockType) -> Transaction;
}
```

**Optimistic:** reads without locking, detects conflicts at commit, retries automatically.
**Pessimistic:** acquires locks on read, prevents conflicts, serializes execution.

SurrealDB's default mode for user transactions is **optimistic** — correct for reads,
but problematic for deterministic game simulations (retry means non-deterministic order).

### SurrealQL transaction control
```sql
BEGIN TRANSACTION;
UPDATE player:p1 SET position = [20, 30];
UPDATE player:p2 SET gold -= 100;
COMMIT;  -- atomic: both or neither

-- On conflict: automatic retry (transparent to user)
```

---

## WASM runtime (Surrealism)

```
surrealism/
├── runtime/         -- Wasmtime executor + host interface
│   └── host.rs      -- Host callbacks: sql(), run(), kv(), stdout()
├── macros/          -- Procedural macros for WASM function binding
└── types/           -- WASM type marshaling
```

### Host interface (WASM → SurrealDB)
```rust
// surrealism/runtime/src/host.rs
register_host_function!(linker, "sql", |controller, query: String| {
    // WASM module can execute SurrealQL queries back into the DB
    controller.sql(query).await
});

register_host_function!(linker, "kv", |controller, key: String| {
    // Direct KV access bypassing the query engine
    controller.kv_get(key).await
});
```

Functions register as named entries, called from SurrealQL:
```sql
-- Call a WASM function from a query
SELECT my_wasm_function(health, level) AS damage FROM player;

-- Or during INSERT/UPDATE
CREATE player SET damage = calc_damage(strength, weapon_type);
```

### Comparison with SpacetimeDB modules

| Aspect | SurrealDB Surrealism | SpacetimeDB module |
|--------|---------------------|-------------------|
| Execution trigger | Per-query call | Client reducer call |
| State | Stateless (request-scoped) | Stateful (DB is the state) |
| Transaction | Participates via host `sql()` | IS the transaction |
| Game loop | Not possible | `schedule_reducer` recursive |
| Isolation | WASM sandbox | WASM sandbox |
| Languages | Any → WASM | Rust, C#, TypeScript → WASM |

---

## Storage backends in detail

### kv-mem (in-memory)
- All data in RAM, no persistence
- Fastest: sub-millisecond for all operations
- Data lost on restart
- **Use case for UzDB:** Same pattern as SpacetimeDB's MemStore — hot game state in RAM

### kv-rocksdb (persistent single-node)
- Facebook's LSM-tree KV store
- Persistent, WAL-based durability
- Single node (no replication built-in)
- **Use case for UzDB:** Similar to UzDB's CommitLog — durability without in-memory sacrifice

### kv-tikv (distributed)
- CNCF project, Raft consensus, horizontally scalable
- Network latency cost for every transaction (~1–5ms LAN)
- **Use case for UzDB:** Reference architecture for multi-shard coordination

---

## Built-in function library (game-relevant)

| Category | Functions | Game use case |
|----------|-----------|---------------|
| `geo::*` | `distance()`, `within()`, `intersects()`, `contains()` | AoI, collision detection, zone queries |
| `crypto::*` | `bcrypt`, `argon2`, `pbkdf2`, `md5`, `sha256`, `ed25519` | Auth, item verification, anti-cheat hash |
| `http::*` | `get()`, `post()`, `put()`, `patch()`, `delete()`, `head()` | External service calls from WASM functions |
| `math::*` | `abs`, `ceil`, `floor`, `sqrt`, `pow`, `clamp`, `lerp` | Game math helpers |
| `rand::*` | `float()`, `int()`, `enum()`, `uuid()`, `guid()` | Loot rolls, random events, session IDs |
| `array::*` | `map`, `filter`, `reduce`, `sort`, `unique`, `flatten` | Inventory manipulation in queries |
| `value::*` | `diff()`, `patch()` | JSON delta computation (not delta push) |
| `string::*` | `len`, `split`, `join`, `contains`, `starts_with` | Chat filtering, name validation |
| `time::*` | `now()`, `day()`, `hour()`, `format()` | Timestamps, schedules |

**Notable:** `geo::distance()` and `geo::within()` confirm that spatial queries are a required
primitive for games. However, these are **query functions** executed per-row — not indexed spatial
lookups. `SELECT * FROM player WHERE geo::distance(pos, origin) < 100` is O(n).
