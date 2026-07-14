# 09 — Mapping to UzDB / TML

## Mapping strategy

Three categories for each SpacetimeDB concept:

- **ADOPT** — take the concept as-is, implement the same design in TML
- **ADAPT** — take the concept but modify it to fit TML or improve on SpacetimeDB's gaps
- **DISCARD** — SpacetimeDB's approach does not apply or is superseded by TML capabilities

---

## Architecture mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| DB-as-server (no intermediate server) | Same — UzDB runtime is the server | ADOPT |
| Single binary deployment | TML compiles to single executable | ADOPT |
| `HostController` managing DB lifecycle | `UzDB.Runtime` managing world shards | ADAPT |
| `ModuleHost` per database | `ShardHost` per world region | ADAPT |
| `StandaloneEnv` for single-node | `UzDB.StandaloneEnv` for local dev | ADOPT |
| WASM sandbox for module isolation | TML native compilation — no sandbox needed | DISCARD |
| Multi-language modules (Rust, C#, TS) | Single language: TML | ADAPT |

**Key TML advantage:** eliminating WASM removes the FFI boundary, the JIT compile step, and the
cross-boundary marshaling overhead. All game logic runs in the same memory space as the DB engine.

---

## Data model mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| `#[table]` struct declaration | `@table` annotation in TML | ADOPT |
| `#[primary_key]` | `@pk` annotation | ADOPT |
| `#[auto_inc]` | `@autoInc` annotation | ADOPT |
| `public` / private tables | `@public` / `@private` modifiers | ADOPT |
| SATS type system (algebraic types) | TML type system (already algebraic) | ADOPT |
| BSATN wire encoding | `UzBIN` encoding (same principles, TML types) | ADAPT |
| B-tree single-column index | B-tree index + spatial index (quadtree/R-tree) | ADAPT |
| No multi-column index | Multi-column composite index | IMPROVE |
| Manual schema migration | `@migration` annotated functions, auto-diffed | IMPROVE |
| In-memory storage + commit log | `MemStore` + `CommitLog` | ADOPT |
| Snapshot + log recovery | `SnapshotWorker` + `CommitLogWorker` | ADOPT |

### TML type system — confirmed from stdlib docs
TML primitive types: `Bool`, `I8`, `I16`, `I32`, `I64`, `U8`, `U16`, `U32`, `U64`, `F32`, `F64`, `Str`, `Buffer`.
Collections: `List[T]`, `HashMap[K, V]`, `BTreeMap[K, V]`, `BTreeSet[T]`.
Error handling: `Outcome[T, E]` (equivalent to `Result` in Rust).
Abstractions use `behavior` (equivalent to Rust traits / interfaces).
Structs declared with `type`. Functions declared with `func`.

**No spatial index exists in TML stdlib** — must be built as `uzdb::index::QuadTree` and `uzdb::index::RTree`
using `BTreeMap` as the underlying sorted structure.

---

## Reducer & module mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| `#[reducer]` function | `@reducer` function in TML | ADOPT |
| `ReducerContext` (sender, timestamp, db) | `ReducerContext` in TML | ADOPT |
| Atomic transaction per reducer call | Same — each `@reducer` is one transaction | ADOPT |
| Full rollback on error/panic | Same — TML errors trigger rollback | ADOPT |
| `__init__` lifecycle reducer | `@onInit` reducer | ADOPT |
| `__connect__` / `__disconnect__` | `@onConnect` / `@onDisconnect` | ADOPT |
| Scheduled reducers via system table | `@tick(rate: 60hz)` + `@schedule` | ADAPT |
| `#[view]` read-only functions | `@view` in TML | ADOPT |
| `#[procedure]` HTTP-callable functions | `@procedure` in TML | ADOPT |
| Reducer calling reducer (shared tx) | Explicit: `call(reducer, args)` within same tx | ADOPT |
| No reducer versioning | `@reducer(version: 2)` with migration path | IMPROVE |

### TML language notes (confirmed from stdlib docs)
- Functions: `func name(args) -> ReturnType { ... }`
- Structs: `type MyStruct { field: Type }`
- Behaviors (traits): `behavior MyBehavior { ... }`
- Error handling: `Outcome[T, E]` with `is_err()`, `From[T]` for `Ok` conversion
- Concurrency: `Channel` (bounded), `mpsc::channel` (unbounded), `AsyncMutex[T]`
- Threading: `thread::spawn[T: Send](f: func() -> T) -> JoinHandle[T]`
- WebSocket: `WsMessage`, `WsFrame`, `WsOpcode` — native RFC 6455 support in stdlib

UzDB reducers will be TML `func` declarations registered with the UzDB runtime.
The annotation syntax (`@reducer`, `@tick`) is UzDB-specific — to be designed.

---

## Transaction mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| MVCC (CommittedState + TxState) | Same MVCC model | ADOPT |
| Single-writer per DB instance | Single-writer per shard | ADOPT |
| Async commit log (no fsync per tx) | Same — game tolerance for small durability gap | ADOPT |
| Snapshot + log recovery | Same | ADOPT |
| No distributed transactions | No cross-shard transactions (by design) | ADOPT |
| Shard boundaries undefined | World region = shard boundary (explicit) | IMPROVE |

### Multi-shard architecture (UzDB addition)
```
World
├── Shard[0,0] — region (0,0)-(1000,1000)   → own CommitLog, own MemStore
├── Shard[1,0] — region (1000,0)-(2000,1000)
├── Shard[0,1] — region (0,1000)-(1000,2000)
└── Shard[1,1] — region (1000,1000)-(2000,2000)

Cross-shard rules:
- Players moving between shards: handoff protocol (player state migrated atomically)
- No cross-shard reducer calls (by design — eliminates distributed transaction complexity)
- Global tables (server config, global events) replicated read-only to all shards
```

---

## Subscription mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| SQL subscription queries | SQL + spatial extensions | ADAPT |
| Compiled query plans | Same | ADOPT |
| Push incremental diffs (inserts+deletes) | Same `TransactionUpdate` model | ADOPT |
| RLS filters in module code | Declarative `@visibility` annotation | ADAPT |
| SDK local cache | Same — SDK maintains local mirror | ADOPT |
| Fan-out O(clients × delta_rows) | Spatial partitioning reduces fan-out to O(aoi_clients × delta_rows) | IMPROVE |
| No subscribable views | Subscribable `@materializedView` | IMPROVE |

### UzDB spatial subscription (improvement)
```tml
// Instead of RLS filter that checks distance for every row,
// UzDB uses the spatial index to compute interested clients automatically

@table @spatial(QuadTree)
struct Position { ... }

// Client subscribes with spatial query:
// SELECT * FROM Position WITHIN RADIUS 100 OF SELF
// UzDB resolves "SELF" to the subscribing player's position
// The spatial index returns only nearby entities — O(log n + k) not O(n)
// As the player moves, subscriptions update automatically
```

### Declarative RLS (improvement over SpacetimeDB's imperative filters)
```tml
// SpacetimeDB: imperative Rust function
// UzDB: declarative annotation

@table
@visibility(rule: "owner_only", ownerField: "identityId")
struct Inventory {
    @pk id: ItemId,
    identityId: Identity,
    itemType: u32,
    quantity: u32,
}

// Automatically: clients only see Inventory rows where identityId == ctx.sender
```

---

## Protocol mapping

| SpacetimeDB concept | UzDB / TML mapping | Category |
|--------------------|--------------------|----------|
| HTTP + WebSocket dual channel | Same | ADOPT |
| BSATN binary encoding | `UzBIN` (same design, TML types) | ADAPT |
| SDK code generation from schema | `uzdb generate --lang <lang>` | ADOPT |
| 256-bit stable Identity | Same — `Identity` type | ADOPT |
| Ephemeral `ConnectionId` | Same | ADOPT |
| OIDC-only auth | Token auth + optional OIDC | ADAPT |
| `v1.bin.spacetimedb` WS subprotocol | `v1.bin.uzdb` WS subprotocol | ADAPT |

---

## Summary: what UzDB adds that SpacetimeDB lacks

| Feature | Priority | Rationale |
|---------|----------|-----------|
| Native spatial indexes (QuadTree/R-tree) | Critical | AoI without spatial index is O(n) — unscalable |
| Multi-shard world regions | Critical | RAM-bounded single-node cannot fit a full MMORPG world |
| Shard handoff protocol for player movement | Critical | Players must cross region boundaries seamlessly |
| `@tick` declarative game loop | High | Cleaner than manual `schedule_reducer` recursion |
| Subscribable materialized views | High | Avoids forced denormalization for computed data |
| Declarative `@visibility` RLS | High | Imperative filters are hard to audit and compose |
| Multi-column composite indexes | Medium | SQL queries often filter on 2+ columns |
| Versioned reducers with migration path | Medium | Schema evolution is painful in SpacetimeDB |
| First-class ECS query API | Medium | MMORPGs are component-based; SQL joins are awkward |
| Built-in auth (not OIDC-only) | Medium | Game clients need simple token auth |
| Automated schema migration diffing | Medium | Huge DX improvement over manual migration reducers |

---

## What UzDB must NOT do differently

These SpacetimeDB decisions are correct and should be adopted without modification:

1. **DB-as-server** — the central insight; do not compromise it
2. **In-memory store + append-only commit log** — proven, correct for game workloads
3. **Atomic reducer transactions** — no partial updates; all-or-nothing
4. **Push subscriptions with local SDK cache** — the right UX for game clients
5. **Stable Identity across sessions** — game accounts must be durable
6. **Single binary deployment** — zero-ops is a feature, not a nice-to-have
7. **SDK code generation from schema** — eliminates an entire class of client bugs
