# 01 — Overview

## What it is

SpacetimeDB is a relational database that is also a server. The central premise:
**eliminate the server layer between the client and the database**.

Instead of:
```
Client → Web Server → Database
```

The SpacetimeDB model is:
```
Client → SpacetimeDB (logic + data + real-time)
```

Application logic is uploaded directly into the database as a **module** (compiled to WASM).
The database executes that logic, manages state, and automatically synchronizes connected clients.

---

## Design philosophy

### 1. State in memory, durability via log
All game state lives in RAM for sub-microsecond access.
Durability is guaranteed by an append-only commit log on disk.
On recovery, the log is replayed to reconstruct state.

### 2. Reducers as API
All state mutations go through **reducers** — atomic, transactional functions.
A reducer is analogous to a REST API endpoint, but:
- Runs inside the database (no network round-trip)
- Is atomically transactional (full commit or full rollback)
- Can be called directly by the client via binary protocol

### 3. Push subscriptions (not polling)
Clients declare interest in data via SQL queries.
When data changes, the database sends incremental diffs automatically over WebSocket.
No polling. No manual cache. No cache invalidation.

### 4. Zero infrastructure
Deploy as a single binary. No Docker, Kubernetes, Redis, message queues.
Operational complexity is eliminated by design.

---

## Comparison with alternatives

| System | Difference vs SpacetimeDB |
|--------|--------------------------|
| PostgreSQL + Node.js | SpacetimeDB removes the Node layer, puts logic in the DB, has automatic sync |
| Firebase/Firestore | SpacetimeDB supports complex logic in real languages, no serverless cold starts |
| Redis | SpacetimeDB has a relational model, ACID transactions, embedded logic |
| Nakama / Photon | SpacetimeDB is integrated DB+server; Nakama is a framework on top of a separate DB |
| ENet / KCP | Transport only; SpacetimeDB adds persistence, logic, and automatic sync |

---

## Proven use cases

### BitCraft Online (MMORPG)
- Entire backend as a single SpacetimeDB module
- Chat, inventory, terrain, player positions — all in the same module
- Real-time synchronization for thousands of simultaneous players
- **150,000 tx/s** measured in benchmark (vs 1,500 tx/s Node.js+PostgreSQL)

### Other domains
- Chat apps and collaborative tools
- Real-time dashboards
- IoT with low latency
- Any system where "low-latency state synchronization" is a core requirement

---

## Technology stack

### Server (core)
- **Rust** (51.9% of code) — runtime, datastore, subscription engine
- **C++** (21.9%) — performance-critical parts
- **TypeScript** (14.0%) — TypeScript module SDK
- **C#** (7.5%) — C# module SDK

### Module runtimes
- **Wasmtime** — executes Rust/C# modules compiled to WASM
- **V8** — executes TypeScript/JavaScript modules

### Client SDKs
- TypeScript (React, Next.js, Vue, Svelte, Angular)
- Rust
- C#
- C++ (Unreal Engine)

---

## Known limitations

| Limitation | Impact | Relevance for UzDB |
|------------|--------|--------------------|
| Data limited by RAM | Large worlds need sharding | High — UzDB must design for sharding |
| Primarily single-node | Vertical scaling only | High — opportunity for differentiation |
| Manual schema migrations | Poor DX for schema evolution | Medium — UzDB can improve this |
| No native spatial queries | MMORPGs need area queries | High — include as a first-class primitive |
| WASM FFI overhead | Added latency at the boundary | Low for UzDB (TML compiles natively) |
| Auth requires OIDC | No simple built-in auth | Medium — UzDB can have embedded auth |
