# ⚡ Fluxum

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-nightly%20(edition%202024)-orange.svg)](rust-toolchain.toml)
[![Status](https://img.shields.io/badge/status-in%20development%20(phase%205)-green.svg)](#-project-status)
[![Specs](https://img.shields.io/badge/specs-28%20documents-blue.svg)](docs/specs/README.md)
[![Version](https://img.shields.io/badge/version-0.1.0--alpha-blue.svg)](CHANGELOG.md)

> **Database as a Server for Realtime Applications**

Fluxum is a general-purpose realtime database written in Rust. It eliminates the intermediate
application server — application logic (reducers) runs natively inside the database process,
clients subscribe directly to live data updates, and the entire backend ships as a single binary.

Inspired by [SpacetimeDB](docs/analysis/spacetimedb/00-README.md), built to exceed it.
Part of the [HiveLLM](https://github.com/hivellm) family (Nexus · Vectorizer · Synap).

---

## 🎯 Overview

```
Without Fluxum                  With Fluxum
─────────────────               ──────────────────────────────────
Client                          Client
  │                               │  FluxRPC (TCP / Streamable HTTP)
  ▼                               │
App Server   ──SQL──▶  DB         ▼
(Node.js / Go / …)              Fluxum  ← reducers + state + push sync
  + Redis cache
  + WebSocket fan-out service
```

Fluxum replaces the "application server + database + cache + push service" stack with a single
process. Application logic runs as atomic **reducer** functions — plain Rust, statically compiled
into the server binary (no WASM sandbox, no FFI). Clients receive automatic incremental diffs via
push **subscriptions** — no polling, no manual cache invalidation.

**The permanent baseline is that stack:** the in-repo `fluxum-bench` parity harness runs the same
application on app-server + PostgreSQL, on equal hardware, and the comparative report ships with
every release. Targets: ≥ 10× write throughput, ≥ 10× lower end-to-end change→client latency,
cold reads within 2× of PostgreSQL — inside a PostgreSQL-like memory envelope (datasets are
bounded by disk, not RAM).

Typical workloads: chat and messaging, presence, collaborative apps, live dashboards, IoT fleets,
marketplaces and order books, any system where many clients must observe shared mutable state in
real time.

---

## ✨ Features

### Storage & Transactions
- **Tiered storage** — hot working set in a buffer pool (< 1 µs reads); cold data in a paged,
  LZ4-compressed on-disk tier under a single `memory.budget` knob — datasets bounded by disk, not RAM
- **Commit log** — durability via append-only log (CRC32C + epoch per entry, group-commit flush
  actor: batched fsync, bounded p99, no per-tx fsync)
- **ACID transactions** — every reducer call is one atomic transaction; full rollback (with index
  revert and undelete) on error or panic
- **MVCC snapshot isolation** — single writer per shard, lock-free reads from committed state
- **Crash recovery** — incremental content-addressed checkpoints + log replay; recovers a 10 GB
  log in < 30 s; torn tails are quarantined, never silently truncated

### Reducers (Application Logic — native Rust)
- **`#[fluxum::reducer]`** — atomic mutation functions callable over FluxRPC
- **`#[fluxum::tick(rate = N)]`** — high-frequency periodic reducers with fixed-timestep drift control
- **`#[fluxum::schedule]`** — one-shot and recurring deferred reducers stored in a system table
- **`#[fluxum::on_init]` / `on_connect` / `on_disconnect`** — lifecycle hooks (presence for free)
- **`#[fluxum::reducer(max_rate = "10/s")]`** — declarative per-identity rate limiting
- **`#[fluxum::view]` / `#[fluxum::procedure]`** — read-only and admin HTTP endpoints

### Subscriptions
- **SQL push subscriptions** — `SELECT * FROM ChatMessage WHERE channel = 5` → auto-diff on every commit
- **Geospatial SQL** — `SELECT * FROM Sensor IN REGION (0, 0, 4000, 4000)` at O(log n + k)
- **`#[visibility(owner_only(owner))]`** — declarative row-level security, no imperative filters
- **Scalable fan-out** — query-hash dedup (shared query = one evaluation + one encoding for all
  subscribers) + value-level plan pruning: cost scales with matching plans, never with client count
- **Backpressure** — 3-tier per-client send buffer, bounded queues with kick-on-overflow; one
  slow client never stalls the fan-out

### Sharding & Replication
- **Horizontal partitions** — tables declare a partition key (hash/range/region); each shard owns
  its storage and subscriptions; billions of rows per deployment
- **`ShardCoord`** — routes requests, aggregates subscriptions across shards
- **Entity handoff** — atomic row-set migration when a partition key changes
- **`#[fluxum::table(global)]`** — tables replicated read-only to all shards
- **Replica sets** — per-shard primary + N replicas; the commit log is the replication stream;
  async or semi-sync; automatic failover with consensus election
- **Read offload** — replicas serve reads and subscription fan-out
- **Backup + PITR** — `fluxum backup create/restore/verify`; point-in-time recovery from archived log segments

### Performance Engineering
- **SIMD everywhere it pays** — runtime dispatch (AVX-512/AVX2/SSE4.2/NEON, scalar fallback) on
  CRC32, hashing, FluxBIN batch codec, and predicate evaluation; every kernel bit-identical to its
  scalar reference (CI-enforced)
- **Hardware adaptivity** — boot-time probe (cores, RAM, cgroup limits) derives buffer pool,
  workers, and queue sizes; the same binary is correct on a 512 MB droplet and fast on a 64-core server
- **Compression** — LZ4 cold pages, zstd checkpoints/backups (≥ 3× target ratio)

### Geospatial Indexes
- **`#[spatial(quadtree(x, y))]`** — O(log n + k) point and radius queries
- **`#[spatial(rtree(…))]`** — bounding-box range queries
- Spatial indexes are **first-class** — not a filter workaround (SpacetimeDB's is O(n))

### Protocol: FluxRPC
- **`u32 LE length + MessagePack`** envelope — the HiveLLM wire-framing standard (SynapRPC lineage)
- **FluxBIN row encoding** — schema-driven, ~40% smaller than MessagePack for typed rows
- **Multiplexed TCP + Streamable HTTP** — one connection, many in-flight requests; browsers get
  binary push streams via fetch `ReadableStream` (the modern MCP-style transport, no WebSocket)
- **Enriched `TxUpdate`** — `caller`, `reducer_name`, `timestamp`, `duration_us` on every diff
- **HTTP/JSON admin** — unversioned paths: `/schema`, `/metrics`, `/health` (no `/v1`)

### Data Model
- **`#[fluxum::table]` + `#[primary_key]` / `#[auto_inc]`** — schema declared as Rust structs
- **Composite primary keys** — `#[fluxum::table(primary_key(a, b))]` (SpacetimeDB can't)
- **`#[index(btree(...))]`** — single and multi-column secondary indexes
- **`#[fluxum::migration(version = N)]`** — schema migration with automatic diff and safe auto-apply
- **`#[fulltext]` columns** — native inverted index; `WHERE col MATCH 'query'` with BM25 ranking,
  no external search engine
- **Column transforms** — `#[transform(...)]` field-level encryption / hashing pipelines with a
  pluggable key provider (SPEC-017)

### Query & Full-Text Search
- **Index-aware query planner** — chooses PK, secondary, spatial, or full-text access paths and
  prunes on selectivity (SPEC-018)
- **Full-text `MATCH`** — BM25-ranked lexical search over `#[fulltext]` columns, `ORDER BY SCORE`,
  with optional re-rank and hybrid (lexical + dense) fusion via the plugin system (SPEC-019)
- **Reactive materialized views** — declared aggregate/derived views maintained incrementally on
  commit and subscribable like any table (SPEC-022)

### Extensibility (Plugin System, SPEC-020)
- **Closed capability set** — every extension point is a reviewed trait with a fixed placement
  class (WritePath / ReadPath / OffPath); no arbitrary "run any code" hook
- **In-process plugins** — feature-gated native Rust registered at link time, run under
  `catch_unwind` isolation (a panic disables the plugin, never crashes the shard)
- **Out-of-process sidecars** — heavy/optional plugins (model re-rankers, a Vectorizer client) run
  as a separate process over Plugin RPC; per-call timeout, graceful degradation to the base result,
  and a circuit breaker keep a slow or dead sidecar from ever breaking a query. The core binary
  stays lean (no model runtime, no weights in-image)

### SDKs (5 languages)
- **JavaScript/TypeScript** · **Python** · **Go** · **Rust** · **C#** — generated from
  `GET /schema` (`fluxum generate`), all passing the same conformance corpus; C++ post-launch
- **Browser-native JS** — the browser talks the binary FluxRPC directly to the database over
  **Streamable HTTP** (`/rpc`: POST frames + GET push stream via fetch `ReadableStream`, FluxBIN
  on `ArrayBuffer`, no JSON hot path, no gateway); plain-JS consumable via npm or
  `<script type="module">`, zero dependencies, ≤ 50 KB min+gzip

### Performance Targets

| Metric | Target |
|--------|--------|
| **vs app-server + PostgreSQL (parity harness)** | **write ≥ 10× · e2e change→client ≥ 10× · cold reads ≤ 2×** |
| Reducer throughput | ≥ 100,000 tx/s per shard |
| Committed read latency (hot) | < 1 µs (buffer pool) |
| Reducer commit latency | < 1 ms p99 (async log, no fsync) |
| FluxRPC round-trip (loopback) | < 0.5 ms p99 |
| Fan-out latency (1,000 subscribers) | < 5 ms p99 |
| Crash recovery (10 GB log) | < 30 s |
| Memory envelope | functional on 1 vCPU / 512 MB with dataset ≥ 10× RAM |
| Capacity | ≥ 1 billion rows (sharded + tiered) |

Reference: SpacetimeDB has sustained 150,000 tx/s in production from a single binary — the
architecture is proven; Fluxum removes its ceilings (RAM-bound datasets, single node, no replicas).

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                             │
│  Backend services (FluxRPC/TCP, fluxum-sdk)                      │
│  Web & mobile clients (Streamable HTTP, JS/TS SDK)               │
│  Admin tools (HTTP/JSON)                                         │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                      TRANSPORT LAYER                             │
│  TCP :15801 (FluxRPC)   HTTP :15800 (/rpc streamable + admin)    │
└─────────────────────┬────────────────────────────────────────────┘
                      │
┌─────────────────────▼────────────────────────────────────────────┐
│                       RUNTIME LAYER                              │
│  ┌──────────────┐   ┌──────────────────────────────────────┐     │
│  │  ShardCoord  │   │  ShardHost (one per partition)       │     │
│  │  routing     │   │  ReducerExecutor + TickScheduler     │     │
│  │  handoff     │   │  SubscriptionManager                 │     │
│  └──────────────┘   └──────────────┬───────────────────────┘     │
└────────────────────────────────────┼─────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────┐
│                        STORAGE LAYER                             │
│   MemStore (BTreeMap + spatial idx)  ·  CommitLog (append-only)  │
│   SnapshotRepo (periodic dumps, recovery base)                   │
└──────────────────────────────────────────────────────────────────┘
```

Workspace: `crates/fluxum-core` · `fluxum-macros` · `fluxum-protocol` · `fluxum-server` ·
`fluxum-cli` + `sdks/` — the HiveLLM Foundation → Core → Features → Presentation layering.
Full design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## 🚀 Quick Start

### Define a schema

```rust
use fluxum::prelude::*;

#[fluxum::table(public)]
pub struct ChatMessage {
    #[primary_key] #[auto_inc]
    pub id: u64,
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub sent_at: Timestamp,
}

// Per-user data (owner-only visibility — server filters rows automatically)
#[fluxum::table(public)]
#[visibility(owner_only(owner))]
pub struct Task {
    #[primary_key] #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
}

// Geospatial + composite PK
#[fluxum::table(public, primary_key(grid_x, grid_y))]
#[spatial(quadtree(x, y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    pub x: f32,
    pub y: f32,
    pub reading: f64,
    pub updated_at: Timestamp,
}
```

### Write reducers

```rust
#[fluxum::on_connect]
fn on_connect(ctx: &ReducerContext) {
    ctx.tx.upsert::<OnlineUser>(OnlineUser {
        identity: ctx.identity,
        connection_id: ctx.connection_id,
        connected_at: ctx.timestamp,
    }).ok();
}

#[fluxum::reducer(max_rate = "5/s")]
fn send_chat(ctx: &ReducerContext, channel: u32, text: String) -> Result<(), String> {
    ctx.tx.insert::<ChatMessage>(ChatMessage {
        id: 0, // auto_inc
        sender: ctx.identity,
        channel,
        content: text,
        sent_at: ctx.timestamp,
    })?;
    Ok(())
}

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

### Build the server binary

```rust
fn main() -> fluxum::Result<()> {
    fluxum::ServerBuilder::from_config("config.yml")?.run()
}
```

```bash
cargo build --release
./target/release/my-backend --config config.yml

# Dev mode (no auth, single shard)
./target/release/my-backend --config config.development.yml
```

### Subscribe from a client (browser, JavaScript/TypeScript SDK)

```typescript
// Browser: binary FluxRPC over Streamable HTTP — POST /rpc + GET push stream
// (fetch ReadableStream). No gateway, no WebSocket, no JSON hot path.
// (Backend services use fluxum://host:15801 over TCP with the same API.)
const db = await FluxumClient.connect("http://localhost:15800");
await db.authenticate(token);

// Subscribe to a channel
await db.subscribe(`SELECT * FROM ChatMessage WHERE channel = 5`);

// Subscribe to own tasks (server applies owner_only filter automatically)
await db.subscribe(`SELECT * FROM Task`);

// Geospatial subscription (quadtree index, O(log n + k))
await db.subscribe(`SELECT * FROM Sensor IN REGION (0, 0, 4000, 4000)`);

// Listen for changes
db.on("ChatMessage:insert", (row) => render(row));
db.on("Task:delete", (row) => remove(row.id));

// Call a reducer
await db.call("send_chat", [5, "hello world"]);
```

---

## 🧬 Transports

| Port | Protocol | Consumers |
|------|----------|-----------|
| 15800 | HTTP — `/rpc` Streamable HTTP (binary FluxRPC) + JSON admin (`/health`, `/metrics`, `/schema`, …) | Web/mobile clients, admin tools |
| 15801 | FluxRPC / TCP | Backend services, native SDKs (binary traffic) |

HiveLLM 15xxx port family — Nexus 15474–6 · Synap 15500–2 · Vectorizer 15002/15503.

---

## ⚖️ vs SpacetimeDB

Fluxum adopts SpacetimeDB's best ideas and deliberately improves on its ceilings:

| Capability | SpacetimeDB | Fluxum |
|------------|------------|--------|
| Module isolation | WASM sandbox (FFI overhead) | Native Rust, statically compiled (zero overhead) |
| Dataset size | Bounded by RAM | Bounded by disk (tiered storage + compression, one `memory.budget` knob) |
| Scale | Single node | Multi-shard partitions + replica sets with failover |
| Geospatial queries | O(n) filter function | O(log n + k) QuadTree / R-tree |
| Row-level security | Imperative Rust filter | Declarative `#[visibility]` |
| Periodic logic | Manual `schedule_reducer` recursion | `#[fluxum::tick(rate = N)]` |
| `TxUpdate` context | Basic | `caller` + `reducer_name` + `timestamp` + `duration_us` |
| Row encoding | BSATN | FluxBIN (same design, Rust types) |
| Composite PKs | Not supported | `#[fluxum::table(primary_key(a, b))]` |
| Rate limiting | Energy system (complex) | `#[fluxum::reducer(max_rate = "10/s")]` |
| Schema migration | Manual migration reducers | `#[fluxum::migration(version)]` + auto-diff |

Adopted as-is: DB-as-server · in-memory + commit log · atomic reducer transactions ·
push subscriptions with local SDK cache · stable 256-bit Identity · single binary · SDK codegen.

Full rationale: [gaps analysis](docs/analysis/README.md).

---

## 📊 Project Status

**Phase: In development** — the storage engine, execution/reducer runtime, subscriptions, and the
transport/scale layer are implemented and under test (line coverage held above 90%). Work is now in
[Phase 5](docs/DAG.md) with the SDK/hardening and replication phases ahead.

| Track | Status |
|-------|--------|
| SpacetimeDB / Convex / SurrealDB reference analysis | ✅ Done |
| Architecture · PRD · implementation DAG · 28 specs | ✅ Done |
| Phase 0 — bootstrap (workspace + hardware probe) | ✅ Done |
| Phase 1 — foundation (macros, FluxBIN codec, auth, column transforms) | ✅ Done |
| Phase 2 — storage core (tiered + compression + SIMD + full-text index) | ✅ Done |
| Phase 3 — execution core (transactions, reducer runtime, plugin framework) | ✅ Done |
| Phase 4 — subscriptions + fan-out, query planner, full-text MATCH, reactive views | ✅ Done |
| Phase 5 — transport & scale (FluxRPC, sharding, plugin sidecar, ops/hot-reload) | 🔄 In progress |
| Phase 6 — developer experience & hardening (TS/Rust SDKs + PostgreSQL parity) → 0.1.0 | 📋 Planned |
| Phase 7 — replica sets, backup/PITR, Python/Go/C# SDKs, 1B-row soak → 0.2.0 | 📋 Planned |

Remaining in Phase 5: audit-trail/event-sourcing, connection-abuse protection, database
namespaces/multitenancy, and per-tenant resource quotas.

---

## 📚 Documentation

- [Architecture](docs/ARCHITECTURE.md) — full system design, data flows, key decisions
- [PRD](docs/PRD.md) — product requirements (FR/NFR), success metrics, risks, MVP criteria
- [Implementation DAG](docs/DAG.md) — dependency graph, 8 phases, gates, critical path
- [Roadmap](docs/ROADMAP.md) — milestones to 0.1.0 and 0.2.0, parallel tracks, post-launch backlog
- [Spec index](docs/specs/README.md) — 28 normative implementation specs (SPEC-001…SPEC-028)
- [SpacetimeDB source dossier](docs/analysis/spacetimedb-code/README.md) — deep analysis of the real v2.7.0 codebase (~237k LOC); hard problems + adoptions in [10-hard-problems](docs/analysis/spacetimedb-code/10-hard-problems.md)
- [Reference analysis](docs/analysis/README.md) — SpacetimeDB, Convex, SurrealDB design studies
- [Contributing](CONTRIBUTING.md) — setup, conventions, spec-driven development
- [Security](SECURITY.md) — security model, vulnerability reporting

## 📄 License

Licensed under the [Apache License 2.0](LICENSE).

## 🤝 Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). This project follows the
HiveLLM family conventions: spec-driven development (PRD → DAG → SPEC → tests), Conventional
Commits, Keep a Changelog, and zero-warning quality gates.
