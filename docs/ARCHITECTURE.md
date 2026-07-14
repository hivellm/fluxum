# Fluxum — System Architecture

> **Version:** 0.1 (initial design)
> **Date:** 2026-07-14
> **Language:** Rust (edition 2024)
> **Status:** Design phase
> **Related:** [PRD](PRD.md) · [DAG](DAG.md) · [Spec index](specs/README.md) · [SpacetimeDB analysis](analysis/spacetimedb/00-README.md)

---

## Core premise

Fluxum is a **database that is also a server**, designed for MMORPG backends.
There is no intermediate application server. Game logic (reducers) runs inside the database.
Clients connect directly to Fluxum and receive real-time state updates via push subscriptions.

### Responsibility split: three-layer engine stack

```
┌─────────────────────────────────────────────────────────────────┐
│  C++ Game Server (engine layer)                                 │
│  - Combat resolution & damage calculation                       │
│  - Loot table generation, crafting validation                   │
│  - AI / pathfinding / spawn logic                                │
│  - netudp: position, rotation, animation (UDP, ~20-60 Hz)       │
│  - FluxRPC client: calls Fluxum for persistent state mutations  │
└──────────────────┬──────────────────────────────────────────────┘
                   │ FluxRPC (TCP, same machine ~0.1-0.3 ms RTT)
                   │ multiplexed: bulk calls are parallel, not serial
┌──────────────────▼──────────────────────────────────────────────┐
│  Fluxum (persistence layer)                                     │
│  - Inventory & items, player stats, skills, quests              │
│  - Chat, economy, trades, guilds                                │
│  - World events & zone state, session management                │
│  - Subscriptions: push state changes to clients                 │
└─────────────────────────────────────────────────────────────────┘
```

**Rule:** changes every frame, loss-tolerant → netudp.
Must survive a crash, transactional → Fluxum.
Game rules, validation, loot logic → C++ game server.

### RPC latency model

On the same host (loopback TCP), a FluxRPC round trip costs ~0.1–0.3 ms. For item drops and
crafting this is imperceptible. Two patterns keep it off the critical path:

- **Fire-and-forget for drops:** the C++ server sends `ReducerCall("create_item_batch", …)`
  without awaiting; only the authoritative pick-up call is awaited.
- **Batch reducers:** one call, one atomic transaction —
  `ReducerCall("create_item_batch", [player_id, [(sword,1),(gold,50),(potion,3)]])`.

FluxRPC multiplexing (per-message `id`) lets the game server pipeline many in-flight calls on a
single connection.

### Memory model vs. the SQLite-in-memory pattern

```
SQLite in-memory + 5 min snapshot    Fluxum MemStore + CommitLog
─────────────────────────────────    ───────────────────────────────
All data in RAM                      All data in RAM (CommittedState)
Write to memory first                CommittedState merge (microseconds)
Async flush queue to disk            CommitLog async append (no fsync)
Snapshot every 5 minutes             Every tx logged continuously
Crash: lose up to 5 minutes          Crash: lose < ~50 ms (log buffer)
Manual sync complexity               MVCC guarantees consistency
```

Spatial indexes apply only to persistent world geometry (terrain chunks, spawn points) —
never to real-time entity positions.

---

## Workspace layout (HiveLLM family pattern)

Cargo workspace, layered Foundation → Core → Features → Presentation (the same crate topology as
Vectorizer, Synap, and Nexus). Higher layers depend on lower ones, never the reverse.

```
fluxum/
├── Cargo.toml                  # workspace: [workspace.package], centralized deps, lints, profiles
├── rust-toolchain.toml         # nightly, rustfmt + clippy
├── crates/
│   ├── fluxum-core/            # Core: storage, transactions, indexes, runtime, subscriptions,
│   │   │                       #   sharding, migration — no network dependencies
│   │   └── src/
│   │       ├── error.rs        # FluxumError (thiserror) + Result<T>
│   │       ├── types.rs        # Identity, ConnectionId, EntityId, Timestamp newtypes
│   │       ├── schema/         # TableSchema registry (populated by fluxum-macros at link time)
│   │       ├── store/          # MemStore — CommittedState + TxState (MVCC)
│   │       ├── commitlog/      # Append-only log (u32 LE + MsgPack + CRC32), async writer
│   │       ├── snapshot/       # SnapshotRepo — periodic dumps + recovery
│   │       ├── index/          # btree.rs, quadtree.rs, rtree.rs
│   │       ├── tx/             # Transaction pipeline, constraints, TxHandle
│   │       ├── runtime/        # ReducerExecutor, tick scheduler, schedule worker, rate limiter
│   │       ├── subscriptions/  # SQL compiler → CompiledPlan, SubscriptionManager, fan-out,
│   │       │                   #   backpressure, visibility (RLS)
│   │       ├── shard/          # ShardHost, ShardCoord, handoff protocol, global replication
│   │       └── migration/      # Schema diff + #[migration] runner, __schema_meta__
│   ├── fluxum-macros/          # Proc macros: #[table], #[reducer], #[tick], #[schedule],
│   │                           #   lifecycle hooks, #[view], #[procedure], #[migration]
│   ├── fluxum-protocol/        # Pure wire layer (no storage deps): FluxValue, FluxBIN codec,
│   │                           #   FluxRPC framing + message types — shared with SDKs
│   ├── fluxum-server/          # Presentation: FluxRPC TCP, WebSocket, HTTP admin (axum),
│   │                           #   auth providers, metrics, ServerBuilder + reference binary
│   └── fluxum-cli/             # `fluxum` binary: generate, schema export, admin commands
├── sdks/
│   ├── rust/                   # fluxum-sdk (workspace member; reuses fluxum-protocol)
│   ├── typescript/             # generated runtime + hand-written transport
│   └── cpp/                    # generated headers + minimal transport
├── config/
│   ├── config.yml              # base config
│   ├── config.example.yml
│   └── config.development.yml  # no auth, single shard
└── docs/                       # this documentation set
```

### The native module model (key decision)

SpacetimeDB isolates game logic in WASM modules and pays FFI marshaling on every reducer call.
The UzDB design eliminated that with native TML compilation; Fluxum preserves the property in Rust:

- A **game module is a plain Rust crate** that depends on `fluxum` and declares tables and
  reducers with proc-macros.
- `#[fluxum::table]` / `#[fluxum::reducer]` register schema and dispatch entries in a link-time
  registry (`inventory`-style collection — OQ-1 tracks the exact mechanism).
- The developer's `main.rs` is three lines:

```rust
fn main() -> fluxum::Result<()> {
    fluxum::ServerBuilder::from_config("config.yml")?.run()
}
```

- The output is **one binary** containing the storage engine, the transports, and the game logic —
  zero sandbox, zero FFI, zero startup JIT. Reducer calls are plain function calls through a
  dispatch table.
- Isolation is provided by the transaction layer, not a sandbox: reducers receive `&ReducerContext`
  and can only mutate state through `TxHandle`; panics are caught (`catch_unwind`), roll the
  transaction back, and never take the shard down.

Trade-off vs SpacetimeDB accepted deliberately: modules cannot be hot-swapped at runtime —
deployment is a binary restart (fast: snapshot + log replay). Schema evolution is handled by
`#[fluxum::migration]` (SPEC-010).

---

## System layers

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                              │
│  C++ game server (FluxRPC/TCP, loopback)                         │
│  Browser clients (WebSocket, TypeScript SDK)                     │
│  Rust services (fluxum-sdk) · Admin tools (HTTP + JSON)          │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                     TRANSPORT LAYER          (fluxum-server)     │
│  TCP :15801 (FluxRPC)   WS :15802 (FluxRPC over WS,              │
│                          subprotocol v1.bin.fluxum)              │
│  HTTP :15800 (JSON admin: health, metrics, schema, query)        │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                     PROTOCOL LAYER          (fluxum-protocol)    │
│  Frame: [ u32 LE length ][ MessagePack body ]                    │
│  Client → Server: Authenticate · ReducerCall · Subscribe ·       │
│                   SubscribeSingle · Unsubscribe · OneOffQuery    │
│  Server → Client: AuthResult · ReducerResult · InitialData ·     │
│                   TxUpdate · Error                               │
│  Row data inside TableUpdate: FluxBIN (schema-driven)            │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                     RUNTIME LAYER              (fluxum-core)     │
│  ┌─────────────────┐    ┌──────────────────────────────────┐     │
│  │  ShardCoord     │    │  ShardHost (one per world region)│     │
│  │  - world map    │    │  - ReducerExecutor               │     │
│  │  - route req    │    │  - TickScheduler + ScheduleWorker│     │
│  │  - handoff      │    │  - SubscriptionManager           │     │
│  │  - global repl. │    │  - commit_and_broadcast()        │     │
│  └─────────────────┘    └──────────────┬───────────────────┘     │
└────────────────────────────────────────┼─────────────────────────┘
                                         │
┌────────────────────────────────────────▼─────────────────────────┐
│                      STORAGE LAYER             (fluxum-core)     │
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────────┐    │
│  │  MemStore     │  │  CommitLog   │  │  SnapshotRepo       │    │
│  │  - BTreeMap   │  │  - append    │  │  - periodic dumps   │    │
│  │    per table  │  │    only      │  │  - recovery base    │    │
│  │  - B-tree idx │  │  - u32 LE +  │  │                     │    │
│  │  - QuadTree / │  │    MsgPack + │  │                     │    │
│  │    R-tree     │  │    CRC32     │  │                     │    │
│  └───────────────┘  └──────────────┘  └─────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Protocol: FluxRPC

FluxRPC is Fluxum's native binary protocol. It uses the **HiveLLM wire-framing standard** —
`u32 LE length prefix + MessagePack body` — shared with SynapRPC (Synap), VectorizerRPC
(Vectorizer), and the Nexus binary RPC, all proven in production. Full normative definition:
[SPEC-006](specs/SPEC-006-protocol-fluxrpc.md).

### Two-layer encoding model

| Layer | Encoding | Rationale |
|-------|----------|-----------|
| **Message envelope** | MessagePack (`rmp-serde`) | Flexible, debuggable, mature tooling, handles tagged variants |
| **Row data** (`TableUpdate.inserts/deletes`) | **FluxBIN** | Schema-driven, no field names/tags → ~40% smaller than MessagePack for typed rows |

FluxBIN is a BSATN-equivalent encoding. At 100k tx/s × 1,000 subscribers, row-encoding overhead
compounds — FluxBIN is mandatory for the hot path.

### Wire framing

```
┌───────────────────┬──────────────────────────┐
│  length: u32 (LE) │  body: MessagePack bytes │
└───────────────────┴──────────────────────────┘
     4 bytes              length bytes
```

- Both directions use the same framing.
- Connections are **multiplexed**: each message carries an `id: u32` chosen by the sender;
  responses echo it. Out-of-order delivery is supported.

### FluxBIN row encoding

```
Bool         → 1 byte: 0x00 | 0x01
u8/i8        → 1 byte          u16/i16 → 2 bytes LE
u32/i32      → 4 bytes LE      u64/i64 → 8 bytes LE
f32 / f64    → IEEE 754 LE (4 / 8 bytes)
String       → u32 LE length + UTF-8 bytes
Vec<u8>      → u32 LE length + raw bytes
Vec<T>       → u32 LE count + N × encode(T)
Option<T>    → 0x00 (None) | 0x01 + encode(T)
Identity     → 32 bytes raw          ConnectionId → 16 bytes raw
EntityId     → 8 bytes LE            Timestamp    → 8 bytes LE (i64 µs)
struct       → sequential field encodings, no separators, no names
enum         → u8 tag + encode(variant payload)
```

No field names, no per-value type tags — the schema (known to both sides) provides all context.
A row is just sequential field values in column declaration order.

### Value type: FluxValue

Used only in reducer arguments/results (the non-hot-path envelope):

```rust
pub enum FluxValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    Str(String),
    Array(Vec<FluxValue>),
    Map(Vec<(FluxValue, FluxValue)>),
    Identity([u8; 32]),   // stable 256-bit player identity
    EntityId(u64),        // entity primary key
    Timestamp(i64),       // microseconds since Unix epoch
}
```

Real-time positions are **not** a `FluxValue` concern — they live in the UDP layer of the game
server. Fluxum stores only persistent geometry (as typed table columns).

### Messages

| Client → Server | Fields |
|---------|--------|
| `Authenticate` | `id, token: Bytes` |
| `ReducerCall` | `id, reducer: String, version?: u32, args: Vec<FluxValue>` |
| `Subscribe` | `id, queries: Vec<String>` |
| `SubscribeSingle` | `id, query: String` |
| `Unsubscribe` | `id, query_ids: Vec<u32>` |
| `OneOffQuery` | `id, sql: String` |

| Server → Client | Fields |
|---------|--------|
| `AuthResult` | `id, identity: [u8; 32], token: Bytes` |
| `ReducerResult` | `id, outcome: Ok \| Err(String)` |
| `InitialData` | `id, schema_version: u32, tables: Vec<TableUpdate>` |
| `TxUpdate` | `tx_id: u64, timestamp: i64, reducer_name: String, caller: [u8; 32], duration_us: u32, tables: Vec<TableUpdate>` |
| `Error` | `id, code: u16, message: String` |

`TxUpdate` enrichment: `caller` drives "Player X attacked you" UI; `reducer_name` drives
client-side event routing; `timestamp` orders events; `duration_us` enables client-side profiling.

```
TableUpdate {
  table_id:   u32,
  table_name: String,
  query_id:   u32,                     // server-assigned (Unsubscribe correlation)
  inserts:    Vec<FluxBIN row>,
  deletes:    Vec<FluxBIN primary key>,  // pk fields only
}
```

### HTTP admin (JSON envelope)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/health` | Server and shard status (< 50 ms, no storage locks) |
| `GET` | `/v1/metrics` | Prometheus text format |
| `GET` | `/v1/schema` | Full schema JSON (tables, reducers, types) |
| `POST` | `/v1/reducer/:name` | Call a reducer (JSON body) |
| `POST` | `/v1/query` | One-off read-only SQL query |
| `GET` | `/v1/view/:name` | Call a `#[fluxum::view]` function |
| `POST` | `/v1/procedure/:name` | Call a `#[fluxum::procedure]` function (admin, P2) |

---

## Runtime: ShardCoord + ShardHost

### ShardCoord

Owns the world map; routes incoming connections and calls to the correct shard.

```rust
pub struct ShardCoord {
    world_bounds: Rect,
    shards: BTreeMap<ShardId, ShardHandle>,
    routing_table: HashMap<RegionKey, ShardId>,
}
```

Responsibilities: accept all TCP/WS connections; route reducer calls by world position or entity
ownership; coordinate player handoff across shard boundaries; replicate `#[table(global)]` tables
read-only to every shard; aggregate cross-shard subscriptions.

### ShardHost

One instance per world region — fully independent: owns its `MemStore`, `CommitLog`,
`SnapshotRepo`, and `SubscriptionManager`. Runs as a tokio task with a single-writer command
queue (OQ-2 tracks process-per-shard as an alternative deployment).

Reducer execution flow per shard:

```
1. ReducerCall dequeued (single-writer per shard)
2. Rate-limit check — token bucket per (Identity, reducer); reject 429 before any work
3. ReducerExecutor runs the reducer fn against TxState (catch_unwind)
4. On success: validate constraints → merge TxState into CommittedState
5. commit_and_broadcast():
   a. CommitLog.append(tx_record)           — async, no fsync per tx
   b. SubscriptionManager.evaluate_delta()  — fan-out TxUpdate to subscribers
6. ReducerResult sent to caller
   On error/panic: TxState discarded, nothing visible, shard alive
```

---

## Storage: MemStore

In-memory transactional store with MVCC isolation ([SPEC-002](specs/SPEC-002-storage-engine.md)).

```rust
pub struct MemStore {
    committed: CommittedState,      // stable snapshot, lock-free reads
    tx: Option<TxState>,            // in-flight mutations (one at a time per shard)
}

pub struct CommittedState {
    tables: HashMap<TableId, Table>,
}

pub struct Table {
    rows: BTreeMap<PrimaryKey, Row>,          // O(log n) pk lookup
    indexes: HashMap<IndexId, BTreeIndex>,    // secondary indexes
    spatial: Option<SpatialIndex>,            // QuadTree / R-tree (persistent geometry only)
}

pub struct TxState {
    inserts: HashMap<TableId, Vec<Row>>,
    deletes: HashMap<TableId, Vec<PrimaryKey>>,
}
```

### Commit log format (same framing as the wire)

```
┌───────────────────┬──────────────────────────────────────┐
│  length: u32 (LE) │  TxRecord: MessagePack bytes + CRC32 │
└───────────────────┴──────────────────────────────────────┘

TxRecord { tx_id: u64, timestamp: i64, shard_id: u32, mutations: Vec<TableMutation> }
TableMutation { table_id: u32, inserts: Vec<Row>, deletes: Vec<PrimaryKey> }
```

Using the same `u32 LE + MessagePack` framing as FluxRPC keeps the stack homogeneous — the commit
log is effectively "recorded wire messages"; one codec, one decoder, one debugging tool.

---

## Reducers

Game logic is written as Rust functions registered by `#[fluxum::reducer]`
([SPEC-004](specs/SPEC-004-reducers.md)):

```rust
pub struct ReducerContext {
    pub identity: Identity,               // 256-bit caller identity
    pub connection_id: ConnectionId,
    pub entity_id: Option<EntityId>,      // caller's entity, if bound
    pub timestamp: Timestamp,             // call timestamp (µs)
    pub shard_id: u32,
    pub tx: TxHandle,                     // the only mutation path
}
```

```rust
#[fluxum::reducer]
fn move_player(ctx: &ReducerContext, dx: f32, dy: f32) -> Result<(), String> {
    let pos = ctx.tx.query_pk::<Position>(ctx.entity_id.ok_or("no entity")?)
        .ok_or("position not found")?;
    ctx.tx.upsert::<Position>(Position { entity_id: pos.entity_id, x: pos.x + dx, y: pos.y + dy })?;
    Ok(())
}

#[fluxum::tick(rate = 60)]
fn physics_tick(ctx: &ReducerContext) {
    for body in ctx.tx.scan_where::<PhysicsBody>(|b| b.dirty) {
        integrate_and_update(ctx, body);
    }
}
```

| Hook | Trigger |
|------|---------|
| `#[fluxum::on_init]` | First start with empty state |
| `#[fluxum::on_connect]` | Client establishes a connection |
| `#[fluxum::on_disconnect]` | Client disconnects |
| `#[fluxum::schedule]` | One-shot / recurring, persisted in `__schedule__`, run by the schedule worker |

`#[fluxum::tick(rate = N)]` implements fixed-timestep semantics: absolute-clock targets
(`start + N × period`), immediate reschedule + log on missed ticks, and a drift reset with warning
when more than 3 periods behind.

---

## Subscriptions

Clients subscribe with a bounded SQL subset; each query compiles once to a `CompiledPlan`
evaluated against the delta rows of every commit ([SPEC-005](specs/SPEC-005-subscriptions.md)).

```rust
pub struct CompiledPlan {
    query_id: u32,
    table_id: TableId,
    filter: Option<FilterExpr>,        // compiled predicate
    spatial: Option<SpatialConstraint>, // IN REGION / WITHIN RADIUS
    rls: Option<VisibilityRule>,       // row-level security
}
```

After each commit: for every plan touching the mutated tables, filter the delta through
predicate + spatial constraint + RLS, and emit a per-client `TxUpdate` if non-empty. The fan-out
loop never blocks on any individual client — the 3-tier backpressure policy (Normal / Pressured /
Full) protects the broadcast path from slow consumers, the same lesson Synap's pub/sub encodes
with bounded per-subscriber channels.

Spatial subscriptions apply only to persistent world geometry:

```sql
SELECT * FROM TerrainChunk IN REGION (0, 0, 4000, 4000)
SELECT * FROM SpawnPoint WITHIN RADIUS 500 OF (1200, 800)
```

---

## Authentication & identity

```
Identity = SHA-256(token bytes)          — 256-bit, stable across reconnects
ServerIdentity = SHA-256("SERVER:" + name) — privileged peers, bypass RLS
ConnectionId = random u128               — ephemeral, per connection
```

Tokens are opaque bytes; the `AuthProvider` trait (`token` / `jwt` / `none` built-ins) validates
them ([SPEC-009](specs/SPEC-009-authentication.md)). No mandatory OIDC.

---

## Configuration

YAML with `FLUXUM_*` environment overrides (family pattern):

```yaml
# config.yml
server:
  tcp_host: "127.0.0.1"
  http_port: 15800        # HTTP/JSON admin
  tcp_port: 15801         # FluxRPC binary
  ws_port: 15802          # FluxRPC over WebSocket

world:
  bounds: { x: 0, y: 0, width: 16000, height: 16000 }
  shard_size: 4000        # 4000×4000 units per shard → 4×4 = 16 shards
  shards: auto

storage:
  commit_log_dir: ./data/log
  snapshot_dir: ./data/snapshots
  snapshot_interval_tx: 10000

auth:
  provider: token          # token | jwt | none (dev only)
  secret: ${FLUXUM_AUTH_SECRET}
  server_peers:
    - name: "game_server"
      token: ${FLUXUM_GAME_SERVER_TOKEN}

subscriptions:
  send_buffer_bytes: 2097152        # 2 MB per client

observability:
  slow_reducer_threshold_us: 5000

logging:
  level: info
  format: json               # json | pretty
```

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| FluxRPC envelope = HiveLLM standard (`u32 LE + MessagePack`) | Proven in Synap/Vectorizer/Nexus; debuggable; one framing across wire, commit log, and snapshots |
| **Row data = FluxBIN** (BSATN-equivalent, not MsgPack) | Schema-driven encoding is ~40% smaller; at 100k tx/s × 1,000 subscribers the difference compounds |
| Native static Rust modules (no WASM) | Eliminates FFI marshaling, startup JIT, and sandbox complexity — reducers are function calls |
| Transaction layer = isolation boundary (`catch_unwind`) | Panic ⇒ rollback; `CommittedState` untouched mid-transaction; shard never dies |
| Single-writer per shard, multi-shard world | Maximizes per-shard throughput without lock contention; scales horizontally by region |
| Composite primary keys | Terrain chunks need `(cx, cy)`; SpacetimeDB lacks this |
| `TxUpdate` carries caller + reducer_name + timestamp + duration | Clients need context for UI, sound, VFX, event routing, and profiling |
| Fan-out backpressure (3-tier per-client buffer) | One slow client must not stall the broadcast loop (DoS vector otherwise) |
| Declarative `max_rate` limiting | Token bucket per (identity, reducer) beats SpacetimeDB's energy accounting |
| Server-to-server identity namespace | The C++ game server is a privileged peer that bypasses RLS |
| Spatial index as first-class primitive | SpacetimeDB's O(n) AoI filter is the main scalability ceiling; solved at the DB level |
| HTTP = JSON admin only; TCP/WS = binary game traffic | Each transport optimized for its consumer; prevents protocol bloat |
| Ports 15800–15802 | HiveLLM 15xxx port family (Nexus 15474–6, Synap 15500–2, Vectorizer 15002/15503) |
