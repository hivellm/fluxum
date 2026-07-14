# 01 — Overview

## What it is

SurrealDB is a multi-model database — document, graph, and relational in one engine — written in
Rust. It ships as a single binary and supports real-time subscriptions via `LIVE SELECT` over
WebSocket. Server-side logic runs as WASM modules via the Surrealism runtime.

Unlike SpacetimeDB (which eliminates the application server entirely), SurrealDB is a **database
first** — it provides the data layer, real-time sync, and server-side computation, but game logic
such as physics and tick-based simulation is expected to run in a separate process.

---

## Design philosophy

### 1. One database, all models
Documents, relational tables, and graph edges coexist in the same engine and query language
(SurrealQL). A single query can traverse tables, follow graph edges, and aggregate results.

### 2. Schema flexibility — schemafull or schemaless per table
Tables can be defined with strict schemas (`SCHEMAFULL`) or accept arbitrary fields
(`SCHEMALESS`). Mixed within the same database.

### 3. Real-time via LIVE SELECT
Clients issue `LIVE SELECT * FROM player;` and receive a stream of change events
(CREATE / UPDATE / DELETE) over WebSocket. No polling. Change events are pushed within ~39ms.

### 4. Pluggable storage backends
The KVS (key-value store) layer is abstracted. Backends: in-memory (default), RocksDB
(persistent single-node), TiKV (distributed), SurrealKV (proprietary), IndexedDB (browser WASM).

### 5. Self-hosted single binary
Unlike Convex, SurrealDB can be self-hosted. No external dependencies for the default config.
A single `surreal start` command launches the full server.

---

## Comparison with SpacetimeDB

| Dimension | SurrealDB | SpacetimeDB |
|-----------|-----------|-------------|
| Core language | Rust | Rust |
| Data model | Document + Graph + Relational | Relational (typed tables) |
| Logic execution | WASM (Surrealism) + built-in functions | WASM (Wasmtime) + V8 |
| Logic position | Runs on-demand per query | Runs as atomic reducers (game loop) |
| Real-time mechanism | `LIVE SELECT` over WebSocket | Subscription + push diffs |
| Notification latency | ~39ms | 1–5ms |
| Update payload | Full document (no delta) | Incremental delta rows |
| Wire encoding | JSON / CBOR / MessagePack | BSATN binary |
| Transaction model | MVCC + optimistic/pessimistic | Single-writer per DB (deterministic) |
| Game loop (tick) | Not supported | `schedule_reducer` (recursive scheduled calls) |
| Spatial queries | Function-level (`geo::distance`) | No native spatial (B-tree only) |
| Storage backends | RocksDB, TiKV, in-memory, browser | Custom in-memory + commit log |
| Deployment | Self-hosted single binary | Self-hosted or cloud |
| Target | General real-time apps + games | Real-time multiplayer games (MMORPG) |

---

## Technology stack

### Backend
- **Language:** Rust (2024 edition)
- **Async runtime:** Tokio
- **HTTP/WebSocket server:** Axum
- **Query parser:** Custom SurrealQL parser (hand-written Rust)
- **WASM runtime:** Wasmtime (via Surrealism crate)
- **JS runtime:** QuickJS (via `rquickjs` crate) for JavaScript functions
- **Serialization:** serde + JSON, CBOR, MessagePack, FlatBuffers

### Storage backends (feature flags)
```toml
kv-mem       # In-memory (default, ephemeral)
kv-rocksdb   # RocksDB (persistent, single-node)
kv-tikv      # TiKV (distributed, Raft-based)
kv-surrealkv # SurrealKV (proprietary)
kv-indxdb    # IndexedDB (browser WASM)
```

### Client SDKs
- JavaScript / TypeScript
- Rust
- Python
- Go
- .NET (C#)
- Java
- PHP

### Query language
**SurrealQL** — SQL-like with graph extensions:
- Standard `SELECT / INSERT / UPDATE / DELETE`
- `RELATE` for graph edge creation
- `FETCH` for graph traversal
- `LIVE SELECT` for real-time subscriptions
- `BEGIN / COMMIT / CANCEL` for transactions
- Built-in function library (geo, crypto, string, math, http, etc.)

---

## Known limitations (critical for games)

| Limitation | Severity | Notes |
|------------|----------|-------|
| ~39ms notification latency | **Critical** | Game tick = 16.7ms; DB is always ≥2 ticks behind |
| No server-side tick loop | **Critical** | Physics, AI must live in separate application server |
| Full document per LIVE notification | **Critical** | 10–100× bandwidth waste for position updates |
| Client-side filtering of notifications | **High** | WHERE clause not applied to change events |
| No AoI subscription management | **High** | No "subscribe within radius" primitive |
| MVCC auto-retry (non-deterministic) | **High** | Game state is not deterministically replayable |
| No commit-log ordering of notifications | **High** | Events may arrive out of order under load |
| Geo functions — no spatial index | **Medium** | `geo::within()` is O(n) — full table scan |
| WASM function latency | **Medium** | On-demand execution; not suitable for tight loops |
