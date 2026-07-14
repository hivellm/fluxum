# 02 — System Architecture

## Layer diagram

```
┌─────────────────────────────────────────────────────┐
│                    CLIENT LAYER                     │
│  Game clients (C++/Unreal, C#/Unity, TypeScript)    │
│  HTTP POST (reducer calls) + WebSocket (subscriptions)│
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                    API LAYER                        │
│  crates/client-api  (Axum HTTP framework)           │
│  Routes: /database/:name/call/:reducer              │
│          /database/:name/subscribe  (WS)            │
│          /database/:name/sql                        │
│  HostController + ClientActorIndex                  │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                    CORE LAYER                       │
│  crates/core                                        │
│  ┌─────────────────┐  ┌──────────────────────────┐  │
│  │  HostController │  │  ModuleSubscriptions     │  │
│  │  - DB lifecycle │  │  - SubscriptionManager   │  │
│  │  - HostCell reg │  │  - compiled query plans  │  │
│  │  - PagePool     │  │  - commit_and_broadcast  │  │
│  └────────┬────────┘  └──────────────────────────┘  │
│           │                                          │
│  ┌────────▼────────┐                                 │
│  │   ModuleHost    │  per-database actor             │
│  │  - call reducer │                                 │
│  │  - call view    │                                 │
│  └────────┬────────┘                                 │
│           │                                          │
│  ┌────────▼────────┐                                 │
│  │  RelationalDB   │  central DB abstraction         │
│  │  - Locking wrap │                                 │
│  │  - DurabilityWk │                                 │
│  │  - SnapshotWk   │                                 │
│  └────────┬────────┘                                 │
└───────────┼─────────────────────────────────────────┘
            │
┌───────────▼─────────────────────────────────────────┐
│               MODULE RUNTIME LAYER                  │
│  Sandboxed execution: Wasmtime (Rust/C#) + V8 (TS)  │
│  InstanceEnv — translates WASM calls to DB ops      │
│  TxSlot — thread-local active transaction ID        │
└───────────┬─────────────────────────────────────────┘
            │
┌───────────▼─────────────────────────────────────────┐
│                  STORAGE LAYER                      │
│  ┌──────────────┐ ┌───────────┐ ┌────────────────┐  │
│  │   Locking    │ │ CommitLog │ │SnapshotRepo    │  │
│  │  Datastore   │ │(append-   │ │(point-in-time  │  │
│  │  (in-memory) │ │only disk) │ │ page dumps)    │  │
│  │  CommittedSt │ │Durability │ │SnapshotWorker  │  │
│  │  + TxState   │ │  Worker   │ │                │  │
│  └──────────────┘ └───────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────┘
```

---

## Components in detail

### HostController (`crates/core`)
- Manages database lifecycle (create, launch, stop)
- Maintains a registry mapping replica IDs → `HostCell` entries
- Launches `ModuleHost` instances on demand
- Holds shared `PagePool` and serialization pools

### ModuleHost
- Per-database actor (one per deployed database)
- Orchestrates user module execution
- Exposes: `call_reducer()`, `call_view()`, `call_procedure()`
- Coordinates with the subscription system on every commit

### ModuleSubscriptions + SubscriptionManager
- `SubscriptionManager`: registry of active client subscriptions + compiled SQL query plans
- `ModuleSubscriptions`: exposes `commit_and_broadcast_event()` — commits a transaction and fans out incremental updates to all subscribers
- Query plans are compiled once on subscription, then evaluated against delta rows on each commit

### RelationalDB
- Central abstraction wrapping the `Locking` datastore
- Manages `DurabilityWorker` (async commit log writes)
- Manages `SnapshotWorker` (periodic point-in-time page dumps)

### InstanceEnv (Module Runtime boundary)
- Bridge between WASM module code and the database
- Translates WASM ABI calls (`datastore_insert`, `datastore_delete`, `iter_start`, etc.) into actual database operations
- Holds a thread-local `TxSlot` with the active `MutTxId` so module code can access the current transaction without explicit parameter passing across the FFI boundary

### Locking Datastore (`crates/datastore`)
- In-memory transactional store
- Two logical regions: `CommittedState` (stable snapshot) and `TxState` (in-flight mutations)
- MVCC isolation: reads see `CommittedState`; writes accumulate in `TxState` until commit

---

## Data flow: reducer call lifecycle

```
1. Client  →  HTTP POST /database/:name/call/:reducer  (BSATN-encoded args)
2. API Layer  →  HostController.get_or_launch_module_host()
3. ModuleHost  →  deserializes arguments, invokes reducer via WasmInstance
4. InstanceEnv  →  sets active transaction in thread-local TxSlot
5. User code  →  executes inside WASM sandbox, reads/writes via FFI
6. Transaction commit  →  commit_and_broadcast_event() notifies subscribers
7. DurabilityWorker  →  appends transaction record to commit log (async)
8. ModuleSubscriptions  →  evaluates affected subscriptions, pushes WebSocket diffs
```

---

## Data flow: subscription update

```
1. Client  →  WebSocket /database/:name/subscribe  (SQL query string)
2. SubscriptionManager  →  compiles SQL to execution plan, registers client
3. [On any reducer commit]
4. ModuleSubscriptions  →  evaluates delta rows against all registered plans
5. BsatnRowListBuilderPool  →  serializes matching rows efficiently
6. WebSocket push  →  incremental diff sent to subscribed client
```

---

## Standalone deployment (`crates/standalone`)

`StandaloneEnv` wires all layers for single-node deployment:
- Instantiates `ControlDb` (local metadata)
- Creates `HostController` with `LocalPersistenceProvider`
- Implements `NodeDelegate` trait for API routing
- Wires subscription, durability, and module execution systems into one binary

---

## Crate map

| Crate | Path | Role |
|-------|------|------|
| `spacetimedb-core` | `crates/core` | HostController, RelationalDB, subscriptions |
| `spacetimedb-client-api` | `crates/client-api` | Axum HTTP/WebSocket routes |
| `spacetimedb-standalone` | `crates/standalone` | Single-node wiring, StandaloneEnv |
| `spacetimedb-datastore` | `crates/datastore` | Locking MVCC in-memory store |
| `spacetimedb-table` | `crates/table` | Table, page, B-tree index structures |
| `spacetimedb-commitlog` | `crates/commitlog` | Append-only commit log |
| `spacetimedb-snapshot` | `crates/snapshot` | Snapshot management |
| `spacetimedb-lib` | `crates/lib` | Shared types, BSATN encoding |
