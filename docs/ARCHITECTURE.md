# Fluxum — System Architecture

> **Version:** 0.1 (initial design)
> **Date:** 2026-07-14
> **Language:** Rust (edition 2024)
> **Status:** Design phase
> **Related:** [PRD](PRD.md) · [DAG](DAG.md) · [Spec index](specs/README.md) · [SpacetimeDB analysis](analysis/spacetimedb/00-README.md)

---

## Core premise

Fluxum is a **database that is also a server**, designed for realtime applications.
There is no intermediate application server. Application logic (reducers) runs inside the
database. Clients connect directly to Fluxum and receive real-time state updates via push
subscriptions.

### Where Fluxum sits

```
┌─────────────────────────────────────────────────────────────────┐
│  Clients                                                        │
│  - Web / mobile apps (Streamable HTTP + JS/TS SDK)              │
│  - Trusted backend services (FluxRPC/TCP, often loopback):      │
│    ingestion pipelines, pricing engines, schedulers, bots       │
│  - Admin tooling (HTTP/JSON)                                    │
└──────────────────┬──────────────────────────────────────────────┘
                   │ FluxRPC (TCP / Streamable HTTP)
                   │ multiplexed: bulk calls are parallel, not serial
┌──────────────────▼──────────────────────────────────────────────┐
│  Fluxum (the entire backend)                                    │
│  - State: users, sessions, documents, orders, telemetry, …      │
│  - Reducers: all mutations, validated and atomic                │
│  - Subscriptions: push every committed change to interested     │
│    clients as incremental diffs                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Rule:** state that must survive a crash and be observed live by many clients → Fluxum.
Heavy computation that only *produces* state changes (ML inference, media processing,
third-party API calls) → a trusted service that feeds Fluxum through reducers.

### RPC latency model

On the same host (loopback TCP), a FluxRPC round trip costs ~0.1–0.3 ms. Two patterns keep it
off the critical path for high-rate writers:

- **Fire-and-forget:** an ingestion service sends `ReducerCall("update_reading", …)` without
  awaiting; only calls whose result gates further work are awaited.
- **Batch reducers:** one call, one atomic transaction —
  `ReducerCall("ingest_batch", [[r1, r2, r3, …]])`.

FluxRPC multiplexing (per-message `id`) lets a client pipeline many in-flight calls on a single
connection.

### Memory model: tiered, budgeted, PostgreSQL-like envelope

SpacetimeDB requires the whole dataset in RAM. Fluxum does not — it works like PostgreSQL's
buffer-pool model, with a realtime-first hot path:

```
PostgreSQL                            Fluxum
─────────────────────────────────     ───────────────────────────────────
shared_buffers page cache             Buffer pool under memory.budget
Heap pages on disk                    Paged cold tier (own format, LZ4)
WAL, fsync per commit (default)       CommitLog async append (no fsync)
LISTEN/NOTIFY + app fan-out           Native subscriptions on every commit
Reads: SQL round trip (~0.1–1 ms)     Hot reads in-process (< 1 µs)
```

- **Hot working set** lives in the buffer pool → microsecond reads, zero disk I/O (NFR-07).
- **Cold data** lives in a paged, compressed on-disk store; pages fault in on demand and are
  evicted under the configured memory budget — datasets are bounded by disk, not RAM
  ([SPEC-015](specs/SPEC-015-tiered-storage.md)).
- **Durability** is the commit log's job in both worlds; crash loses < ~50 ms (log buffer),
  never integrity.

The permanent performance baseline is a functionally identical app on app-server + PostgreSQL,
run by the `fluxum-bench` parity harness on equal hardware (PRD NFR-11).

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
│   │       ├── store/          # MemStore — CommittedState + TxState (MVCC), buffer pool
│   │       ├── pager/          # Paged cold tier: page format, eviction, compression (LZ4/zstd)
│   │       ├── commitlog/      # Append-only log (u32 LE + MsgPack + CRC32), async writer
│   │       ├── snapshot/       # Checkpoints — periodic dumps + recovery + log truncation
│   │       ├── index/          # btree.rs, quadtree.rs, rtree.rs
│   │       ├── tx/             # Transaction pipeline, constraints, TxHandle
│   │       ├── runtime/        # ReducerExecutor, tick scheduler, schedule worker, rate limiter
│   │       ├── subscriptions/  # SQL compiler → CompiledPlan, SubscriptionManager, fan-out,
│   │       │                   #   backpressure, visibility (RLS)
│   │       ├── shard/          # ShardHost, ShardCoord, handoff protocol, global replication
│   │       ├── replication/    # Replica sets: log streaming, election, failover, backup/PITR
│   │       ├── simd/           # SIMD kernels + runtime dispatch (x86/aarch64/scalar)
│   │       ├── hw/             # Hardware probe (cores, RAM, cgroups) + adaptive tuning
│   │       └── migration/      # Schema diff + #[migration] runner, __schema_meta__
│   ├── fluxum-macros/          # Proc macros: #[table], #[reducer], #[tick], #[schedule],
│   │                           #   lifecycle hooks, #[view], #[procedure], #[migration]
│   ├── fluxum-protocol/        # Pure wire layer (no storage deps): FluxValue, FluxBIN codec,
│   │                           #   FluxRPC framing + message types — shared with SDKs
│   ├── fluxum-server/          # Presentation: FluxRPC TCP, Streamable HTTP /rpc + admin (axum),
│   │                           #   auth providers, metrics, ServerBuilder + reference binary
│   ├── fluxum-cli/             # `fluxum` binary: generate, schema export, backup, admin
│   └── fluxum-bench/           # Parity harness: identical app on Fluxum vs app-server+PostgreSQL
│                               #   (and SQLite); comparative report per release (NFR-11)
├── sdks/
│   ├── rust/                   # fluxum-sdk (workspace member; reuses fluxum-protocol)
│   ├── typescript/             # browser-native JS/TS: binary FluxRPC over WS (ArrayBuffer),
│   │                           #   plain-JS consumable (ESM/CJS + .d.ts, zero deps) + Node TCP
│   ├── python/                 # asyncio-first client (0.2.0)
│   ├── go/                     # context-aware client (0.2.0)
│   └── csharp/                 # async/await client, NuGet (0.2.0)
├── config/
│   ├── config.yml              # base config
│   ├── config.example.yml
│   └── config.development.yml  # no auth, single shard
└── docs/                       # this documentation set
```

### The native module model (key decision)

SpacetimeDB isolates application logic in WASM modules and pays FFI marshaling on every reducer
call. The UzDB design eliminated that with native TML compilation; Fluxum preserves the property
in Rust:

- An **application module is a plain Rust crate** that depends on `fluxum` and declares tables
  and reducers with proc-macros.
- `#[fluxum::table]` / `#[fluxum::reducer]` register schema and dispatch entries in a link-time
  registry (`inventory`-style collection — OQ-1 tracks the exact mechanism).
- The developer's `main.rs` is three lines:

```rust
fn main() -> fluxum::Result<()> {
    fluxum::ServerBuilder::from_config("config.yml")?.run()
}
```

- The output is **one binary** containing the storage engine, the transports, and the application
  logic — zero sandbox, zero FFI, zero startup JIT. Reducer calls are plain function calls through
  a dispatch table.
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
│  Backend services (FluxRPC/TCP, fluxum-sdk)                      │
│  Web & mobile clients (Streamable HTTP, JS/TS SDK)               │
│  Admin tools (HTTP + JSON)                                       │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                     TRANSPORT LAYER          (fluxum-server)     │
│  TCP :15801 (FluxRPC binary)                                     │
│  HTTP :15800 — /rpc (FluxRPC over Streamable HTTP: POST frames   │
│           + GET binary push stream)  ·  JSON admin endpoints     │
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
│  │  ShardCoord     │    │  ShardHost (one per partition)   │     │
│  │  - partition map│    │  - ReducerExecutor               │     │
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
    Identity([u8; 32]),   // stable 256-bit client identity
    EntityId(u64),        // row/entity primary key
    Timestamp(i64),       // microseconds since Unix epoch
}
```

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

`TxUpdate` enrichment: `caller` lets clients attribute changes ("Alice edited this document");
`reducer_name` drives client-side event routing; `timestamp` orders events; `duration_us` enables
client-side profiling.

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
| `POST`/`GET` | `/rpc` | **FluxRPC over Streamable HTTP** (binary — see below) |
| `GET` | `/health` | Server and shard status (< 50 ms, no storage locks) |
| `GET` | `/metrics` | Prometheus text format |
| `GET` | `/schema` | Full schema JSON (tables, reducers, types) |
| `POST` | `/reducer/:name` | Call a reducer (JSON body) |
| `POST` | `/query` | One-off read-only SQL query |
| `GET` | `/view/:name` | Call a `#[fluxum::view]` function |
| `POST` | `/procedure/:name` | Call a `#[fluxum::procedure]` function (admin, P2) |

Paths are unversioned by design — no `/v1` prefix; compatibility is governed by the format
freezes (G5/T6.1) and additive evolution, not by path versioning.

### Browser transport: FluxRPC over Streamable HTTP

Browsers speak the same binary protocol through standard HTTP (the modern pattern MCP adopted;
the family already ships Streamable HTTP in Synap and Vectorizer) — **no WebSocket, no SSE, no
JSON on the hot path**:

```
POST /rpc   Content-Type: application/x-fluxum
            body: one or more FluxRPC frames (u32 LE + MessagePack)
            response: the matching response frames, streamed as they complete

GET /rpc    Fluxum-Session: <id>
            response: long-lived binary stream of server-initiated frames
            (InitialData, TxUpdate) — consumed via fetch ReadableStream
```

The first `Authenticate` returns an opaque session id (`Fluxum-Session` header) binding identity
and subscriptions; on connection loss the SDK re-authenticates and resubscribes automatically.
Same framing, FluxBIN rows, multiplexing, and limits as TCP — only the carrier differs. Works
through ordinary proxies/load balancers, multiplexes over HTTP/2, and upgrades naturally to
WebTransport/HTTP-3 later (FR-88, P2).

---

## Runtime: ShardCoord + ShardHost

### ShardCoord

Owns the partition map; routes incoming connections and calls to the correct shard.

```rust
pub struct ShardCoord {
    partitioning: PartitionScheme,     // hash | range | region, per partitioned table
    shards: BTreeMap<ShardId, ShardHandle>,
    routing_table: HashMap<PartitionKey, ShardId>,
}
```

Responsibilities: accept all TCP/WS connections; route reducer calls by partition key; coordinate
entity handoff when a row set changes partition; replicate `#[table(global)]` tables read-only to
every shard; aggregate cross-shard subscriptions.

### ShardHost

One instance per partition — fully independent: owns its `MemStore`, `CommitLog`, `SnapshotRepo`,
and `SubscriptionManager`. Runs as a tokio task with a single-writer command queue (OQ-2 tracks
process-per-shard as an alternative deployment).

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

## Storage: MemStore + tiered pager

Transactional store with MVCC isolation ([SPEC-002](specs/SPEC-002-storage-engine.md)). The
committed state is **tiered** ([SPEC-015](specs/SPEC-015-tiered-storage.md)): hot rows live
uncompressed in the buffer pool; cold rows live in a paged, compressed on-disk store and fault in
on demand. The transaction and subscription layers see one logical `CommittedState` — tiering is
invisible above the storage API.

```rust
pub struct MemStore {
    committed: CommittedState,      // logical committed state (buffer pool + cold pager)
    tx: Option<TxState>,            // in-flight mutations (one at a time per shard)
}

pub struct CommittedState {
    tables: HashMap<TableId, Table>,
}

pub struct Table {
    rows: BTreeMap<PrimaryKey, Row>,          // O(log n) pk lookup
    indexes: HashMap<IndexId, BTreeIndex>,    // secondary indexes
    spatial: Option<SpatialIndex>,            // QuadTree / R-tree
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
log is effectively "recorded wire messages"; one codec, one decoder, one debugging tool. It is
also the **replication stream** (see Replication below) and, archived, the basis for point-in-time
recovery.

Durability engineering (adopted from the SpacetimeDB source analysis): entries carry **CRC32C**
(hardware-accelerated) and an **epoch** number; a **group-commit flush actor** batches many
transactions per write to bound p99 without per-tx fsync; recovery performs **non-destructive
torn-tail repair** (the damaged tail is quarantined, never silently truncated); checkpoints are
**incremental and content-addressed** — unchanged pages are shared between checkpoints, so
checkpoint cost tracks the write rate, not the dataset size.

### Storage tiers & memory budget

```
                      memory.budget (auto = f(RAM, cgroup limits))
        ┌─────────────────────────────────────────────┐
        │                BUFFER POOL                  │   hot: < 1 µs reads
        │   uncompressed pages, clock-LRU eviction    │
        └───────────────▲───────────────┬─────────────┘
                 fault in │              │ evict (compress)
        ┌───────────────┴───────────────▼─────────────┐
        │              PAGED COLD TIER                │   cold: one page-in
        │   fixed-size pages, FluxBIN rows, CRC32,    │
        │   LZ4 per page (zstd for checkpoints)       │
        └─────────────────────────────────────────────┘
```

- **One knob:** `memory.budget: auto | <bytes>`. `auto` derives from detected RAM and container
  limits. The process never grows unbounded with dataset size (PRD FR-110, NFR-12).
- Pages are the unit of I/O, eviction, compression, and checksums; indexes are paged too.
  Datasets are bounded by disk — billions of rows per deployment when combined with sharding
  (NFR-13).
- Writes always land in the hot tier + commit log; eviction is asynchronous and never blocks the
  writer. Full design: [SPEC-015](specs/SPEC-015-tiered-storage.md).

---

## Reducers

Application logic is written as Rust functions registered by `#[fluxum::reducer]`
([SPEC-004](specs/SPEC-004-reducers.md)):

```rust
pub struct ReducerContext {
    pub identity: Identity,               // 256-bit caller identity
    pub connection_id: ConnectionId,
    pub timestamp: Timestamp,             // call timestamp (µs)
    pub shard_id: u32,
    pub tx: TxHandle,                     // the only mutation path
}
```

```rust
#[fluxum::reducer]
fn complete_task(ctx: &ReducerContext, task_id: u64) -> Result<(), String> {
    let task = ctx.tx.query_pk::<Task>(task_id).ok_or("task not found")?;
    if task.owner != ctx.identity {
        return Err("not your task".into());
    }
    ctx.tx.upsert::<Task>(Task { done: true, ..task })?;
    Ok(())
}

#[fluxum::tick(rate = 1)]
fn purge_expired_sessions(ctx: &ReducerContext) {
    let cutoff = ctx.timestamp - Duration::from_mins(30);
    for s in ctx.tx.scan_where::<OnlineUser>(|s| s.connected_at < cutoff) {
        ctx.tx.delete::<OnlineUser>(s.identity).ok();
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
    filter: Option<FilterExpr>,         // compiled predicate
    spatial: Option<SpatialConstraint>, // IN REGION / WITHIN RADIUS
    rls: Option<VisibilityRule>,        // row-level security
}
```

Fan-out is designed to scale with *matching plans*, never with client count (adopted from the
SpacetimeDB source analysis):

- **Query dedup** — identical queries share one `CompiledPlan`: evaluated once per commit and
  encoded once (FluxBIN), then distributed to every subscriber of that query.
- **Value-level pruning** — plans are indexed by their equality-filter values, so a commit's
  delta rows select only the plans whose values match; there is no linear scan over all plans.
- **Per-client work** is limited to queueing (+ optional compression). The broadcast loop never
  blocks on any individual client — the 3-tier backpressure policy (Normal / Pressured / Full)
  with bounded queues and kick-on-overflow protects it from slow consumers.
- **Admission control** caps subscriptions per connection and total compiled plans.

Geospatial subscriptions:

```sql
SELECT * FROM Sensor IN REGION (0, 0, 4000, 4000)
SELECT * FROM Vehicle WITHIN RADIUS 500 OF (1200, 800)
```

---

## Authentication & identity

```
Identity (jwt provider)   = SHA-256(issuer ‖ subject)  — stable across token rotation/refresh
Identity (token provider) = SHA-256(token bytes)       — opaque long-lived tokens
ServerIdentity            = SHA-256("SERVER:" + name)  — privileged service peers, bypass RLS
ConnectionId              = random u128                — ephemeral, per connection
```

Identity never changes on token refresh (adopted from the SpacetimeDB analysis: claims-based
derivation is what keeps identity stable when tokens rotate).

Tokens are opaque bytes; the `AuthProvider` trait (`token` / `jwt` / `none` built-ins) validates
them ([SPEC-009](specs/SPEC-009-authentication.md)). No mandatory OIDC.

---

## Replication & backup (replica sets)

Per shard, a **replica set**: one primary + N replicas ([SPEC-014](specs/SPEC-014-replication.md)).

```
            writes                    commit-log stream
Clients ──────────────▶ PRIMARY ═══════════════════════▶ REPLICA 1
            reads /                   (async | semi-sync)      │
            subscriptions ─────────────────────────────▶ REPLICA 2
                                                          serve reads +
                                                          subscription fan-out
```

- **The commit log is the replication protocol.** Full sync = checkpoint transfer; partial sync =
  stream from a log offset. No second wire format to maintain.
- **Modes:** async by default (lowest latency); semi-sync quorum acknowledgment for
  zero-committed-loss guarantees.
- **Failover:** consensus-based primary election (OQ-8: `openraft` vs custom Raft — both have
  family precedent); epoch numbers in the stream fence stale primaries. SDKs reconnect and
  resubscribe transparently.
- **Read offload:** replicas serve one-off reads and **subscription fan-out**, taking broadcast
  work off the primary's write path (staleness bounded and observable via
  `fluxum_replication_lag`).
- **Backup:** `fluxum backup create / restore / verify` — hot backup (checkpoint + archived log
  segments, zstd), no writer stall; **PITR** replays archived segments to a target timestamp or
  `tx_id`.

---

## SIMD & hardware adaptivity

Fluxum adapts to the machine instead of assuming one ([SPEC-016](specs/SPEC-016-hardware-adaptivity.md)).

**Boot-time probe** — cores, total/available RAM, cgroup/container limits → derives buffer-pool
size, tokio worker counts, fan-out concurrency, WAL buffer, checkpoint cadence. Effective values
are logged at boot and exposed in `/health`. The same binary is correct on a 1 vCPU / 512 MB
droplet and fast on a 64-core server; config can override any derived value.

**SIMD with runtime dispatch** (family precedent: Nexus `simd/`, Vectorizer SIMD matrix CI) —
selected once at startup per kernel: AVX-512 / AVX2 / SSE4.2 on x86-64, NEON on aarch64, scalar
fallback everywhere. Hot kernels:

| Kernel | Used by |
|---|---|
| CRC32 (hw / PCLMUL) | Commit log, page checksums |
| xxHash-class hashing | Partition routing, index hashing |
| FluxBIN batch encode/decode | Fan-out serialization, page materialization |
| Batched predicate evaluation | Subscription filters over row batches, scans |
| LZ4 / zstd block paths | Page compression, checkpoints, backups |

Correctness rule (PRD FR-112): every SIMD kernel is **bit-identical to its scalar reference**,
enforced by property tests on an ISA matrix in CI. Performance claims come from the parity
harness, not microbenchmarks alone.

---

## Configuration

YAML with `FLUXUM_*` environment overrides (family pattern):

```yaml
# config.yml
server:
  tcp_host: "127.0.0.1"
  http_port: 15800        # HTTP: admin API + /rpc (FluxRPC over Streamable HTTP)
  tcp_port: 15801         # FluxRPC binary TCP

sharding:
  shards: 1               # or N, or "auto"
  strategy: hash          # hash | range | region (per partitioned table override)

memory:
  budget: auto            # auto = f(detected RAM, cgroup limits) | absolute bytes ("2GiB")

storage:
  data_dir: ./data
  commit_log_dir: ./data/log
  checkpoint_dir: ./data/checkpoints
  checkpoint_interval_tx: 10000
  page_compression: lz4    # lz4 | zstd | none

replication:
  role: primary            # primary | replica (auto after election)
  mode: async              # async | semi_sync
  peers: []                # replica set members

simd: auto                 # auto | avx512 | avx2 | neon | scalar (force for debugging)

auth:
  provider: token          # token | jwt | none (dev only)
  secret: ${FLUXUM_AUTH_SECRET}
  server_peers:
    - name: "ingest_service"
      token: ${FLUXUM_INGEST_TOKEN}

subscriptions:
  send_buffer_bytes: 2097152        # 2 MiB per client

reducer:
  shard_max_reducers_per_sec: 200000  # RED-052 global shard guard; mandatory-on (SEC-046)
  max_execution_ms: 10000             # SEC-046 cooperative deadline per client call (0 = off)
  max_tx_bytes: 512MiB                # SEC-046 per-transaction write ceiling (0 = off)

query:                                # SPEC-026 SEC-045/047 execution bounds + admission
  default_limit: 0                    # implicit LIMIT for unlimited queries (0 = none)
  max_limit: 1000000                  # ceiling on any effective LIMIT (0 = unbounded)
  max_limit_action: clamp             # clamp | reject (reject answers 3030)
  row_scan_budget: 10000000           # rows one evaluation may touch (0 = off)
  deadline_ms: 5000                   # per-query wall clock (0 = off)
  max_queries_per_sec_per_identity: 500  # SEC-047 per-caller admission (0 = off)
  max_queries_per_sec_per_source: 2000   # SEC-047 per resolved-IP/connection (0 = off)

observability:
  slow_reducer_threshold_us: 5000

logging:
  level: info
  format: json               # json | pretty
```

### Hot reload (SPEC-025 OPS-040/041)

A defined subset of keys can change on a **running** process — no restart,
no dropped connections:

| Reloadable key | Reaches |
|----------------|---------|
| `logging.level` | the live `tracing` filter |
| `logging.format` | the live `tracing` layer |
| `observability.slow_reducer_threshold_us` | the shard's metrics registry |
| `reducer.shard_max_reducers_per_sec` | the RED-052 admission guard (0 is rejected — SEC-046) |
| `reducer.max_execution_ms`, `reducer.max_tx_bytes` | the SEC-046 reducer execution bounds |
| `query.*` (limits, scan budget, deadline, admission rates) | the SEC-045/047 query bounds and limiter |
| `subscriptions.send_buffer_bytes` | each connection admitted after the reload |

Everything else — ports, storage paths, shard count, auth — is **frozen**
until restart. The allowlist is opt-*in*: a key added to `Config` is frozen
until someone classifies it, so the failure mode of forgetting is a rejected
reload (loud, harmless) rather than a silently hot-swapped storage path.

Trigger a reload with `POST /config/reload`. It re-reads the file and the
environment through the same layered loader as boot, so precedence is
unchanged — notably, a `RUST_LOG` or `FLUXUM_*` override still outranks the
file, which is why `/health` reports each value's **source** alongside it.

Reload is **all-or-nothing** (OPS-041). If any frozen key changed, the
response is `400` naming *every* offender at once and nothing is applied —
not even the reloadable keys in the same file. A rejection is not a latch:
fix the file and reload again.

```console
$ curl -XPOST localhost:15800/config/reload
{"success":true,"payload":{"reloaded":true,"changed":["logging.level"], ...}}

$ curl -XPOST localhost:15800/config/reload   # after editing http_port
{"success":false,"error":"reload rejected: these keys cannot change at
 runtime: server.http_port. Restart to apply them. Reloadable keys: ..."}
```

`GET /health` reports the values in force under `reloadable`, each with its
provenance:

```json
{ "reloadable": {
    "logging.level": { "value": "debug", "source": "file" },
    "reducer.shard_max_reducers_per_sec": { "value": 200000, "source": "default" }
} }
```

Boot and reload run the *same* publish path, deliberately: a key that
applied on reload but was only read at assembly time would silently revert
on the next restart.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| FluxRPC envelope = HiveLLM standard (`u32 LE + MessagePack`) | Proven in Synap/Vectorizer/Nexus; debuggable; one framing across wire, commit log, and snapshots |
| **Row data = FluxBIN** (BSATN-equivalent, not MsgPack) | Schema-driven encoding is ~40% smaller; at 100k tx/s × 1,000 subscribers the difference compounds |
| Native static Rust modules (no WASM) | Eliminates FFI marshaling, startup JIT, and sandbox complexity — reducers are function calls |
| Transaction layer = isolation boundary (`catch_unwind`) | Panic ⇒ rollback; `CommittedState` untouched mid-transaction; shard never dies |
| Single-writer per shard, partitioned keyspace | Maximizes per-shard throughput without lock contention; scales horizontally by partition |
| Composite primary keys | Natural keys like `(tenant, key)` or `(grid_x, grid_y)`; SpacetimeDB lacks this |
| `TxUpdate` carries caller + reducer_name + timestamp + duration | Clients need context for attribution, event routing, ordering, and profiling |
| Fan-out backpressure (3-tier per-client buffer) | One slow client must not stall the broadcast loop (DoS vector otherwise) |
| Declarative `max_rate` limiting | Token bucket per (identity, reducer) beats SpacetimeDB's energy accounting |
| Server-to-server identity namespace | Trusted services are privileged peers that bypass RLS |
| Geospatial index as first-class primitive | SpacetimeDB's O(n) location filter is a scalability ceiling; solved at the DB level |
| Browser transport = **Streamable HTTP** (`/rpc`), not WebSocket | Modern standard (MCP pattern, family precedent in Synap/Vectorizer); binary end-to-end via fetch streams; proxy/LB-friendly; natural HTTP/3-WebTransport path |
| Unversioned HTTP paths (no `/v1`) | Compatibility via format freezes + additive evolution, not path versioning |
| **Tiered storage under one memory budget** (vs SpacetimeDB's all-in-RAM) | PostgreSQL-like memory envelope; datasets bounded by disk; small-droplet viable (NFR-12); billions of rows with sharding (NFR-13) |
| Own paged store — no RocksDB/LMDB/SQLite dependency | The performance envelope *is* the product; one codec/framing across wire, log, and pages |
| Commit log doubles as replication stream and PITR source | One format to test; full sync = checkpoint, partial sync = offset; backups replay the same segments |
| Replica sets: single primary per shard, consensus election | MongoDB-style operational model; semi-sync mode gives zero-committed-loss failover |
| SIMD via runtime dispatch, scalar-parity enforced | Portable binary, maximum per-machine performance, correctness provable (family precedent) |
| Parity benchmark vs app-server + PostgreSQL in-repo | The product's reason to exist is measured, not asserted (NFR-11); published every release |
| Fan-out = query dedup + value-level pruning (never O(clients)) | Adopted from SpacetimeDB source analysis — the proven way subscriptions scale (SPEC-005) |
| Incremental content-addressed checkpoints + group-commit + torn-tail quarantine | Adopted — avoids their full-dump scaling cliff; bounded p99; recovery never destroys evidence (SPEC-002) |
| Deterministic simulation testing (DST) in CI for storage/replication | Adopted and extended — seeded runtime, fault injection, model oracle (SPEC-013) |
| Ports 15800 (HTTP) + 15801 (TCP) | HiveLLM 15xxx port family (Nexus 15474–6, Synap 15500–2, Vectorizer 15002/15503) |
