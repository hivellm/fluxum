# 04 — Modules & Reducers

## What is a module

A SpacetimeDB module is a WebAssembly binary (or JavaScript bundle for TypeScript modules)
that imports a specific low-level WASM ABI and exports a small number of special functions.

The module contains:
- **Table declarations** — the schema
- **Reducers** — mutable logic (write operations)
- **Views** — read-only query functions
- **Procedures** — functions callable via external HTTP (not by clients directly)
- **Lifecycle hooks** — `__init__`, `__connect__`, `__disconnect__`

Modules are deployed to SpacetimeDB via the CLI (`spacetime publish`). The host calls
`__describe_module__` to extract schema and then stores the WASM binary. All subsequent
reducer calls execute the stored binary inside a sandboxed runtime.

---

## Module ABI (low-level interface)

### Exports (called by the host)
| Function | Description |
|----------|-------------|
| `__describe_module__` | Returns schema: tables, reducers, indexes, RLS filters |
| `__call_reducer__(id, sender_identity, connection_id, timestamp, args_ptr, args_len)` | Invokes a reducer by its numeric ID |
| `__init__` | Called once when the module is first published |

### Imports (host functions available to the module)
**Logging:**
```
_console_log(level: u8, target_ptr, target_len, filename_ptr, filename_len, line: u32, msg_ptr, msg_len)
```

**Buffer management:**
```
_buffer_alloc(data_ptr, data_len) → BufId
_buffer_consume(buf_id, dst_ptr, dst_len)
_buffer_len(buf_id) → u32
```

**Table operations:**
```
_get_table_id(name_ptr, name_len) → TableId
_insert(table_id, row_ptr, row_len) → status: u16
_delete_by_col_eq(table_id, col_id, value_ptr, value_len) → status: u16
_iter_by_col_eq(table_id, col_id, value_ptr, value_len) → BufId (rows)
_iter_start(table_id) → IteratorId
_iter_start_filtered(table_id, filter_ptr, filter_len) → IteratorId
_iter_next(iter_id, buf_ptr, buf_len) → status: u16
_iter_drop(iter_id)
```

**Index creation:**
```
_create_index(table_id, col_id, index_type: u8, index_name_ptr, index_name_len)
```

**Reducer scheduling:**
```
_schedule_reducer(name_ptr, name_len, args_ptr, args_len, time: u64) → ScheduleToken
_cancel_reducer(token: ScheduleToken)
```

All status codes: `0` = success, non-zero = error.
All data is BSATN-encoded. Buffers are host-side memory; the module reads them via `_buffer_consume`.

---

## Reducers

### Definition
A reducer is a function exported by a database module that connected clients can call to
interact with the database. It is the primary mutation API.

```rust
// Rust reducer example
#[spacetimedb::reducer]
pub fn move_player(ctx: &ReducerContext, dx: f32, dy: f32) {
    let identity = ctx.sender;
    if let Some(mut player) = Player::filter_by_identity(&identity) {
        player.x += dx;
        player.y += dy;
        player.update();
    }
}
```

### Reducer properties
- Runs inside a **database transaction** — all changes commit atomically or not at all
- Errors (panics, explicit `Err` returns) trigger **full rollback** of the transaction
- Each reducer call is a **separate transaction** — no shared transaction state between calls
- When a reducer calls another reducer **directly** (not via schedule), the inner call does NOT run in its own child transaction — it shares the outer transaction

### ReducerContext
Provided by the host at invocation time:
```
ctx.sender        → Identity  (caller's 256-bit identity)
ctx.connection_id → ConnectionId (ephemeral connection ID)
ctx.timestamp     → Timestamp (when the reducer was called)
ctx.db            → database handle (access to all tables)
```

### Lifecycle reducers (special)
| Reducer | Trigger | Use case |
|---------|---------|----------|
| `__init__` | First publish | Initialize default data, seed world state |
| `__connect__` | Client connects | Track online players, initialize session |
| `__disconnect__` | Client disconnects | Clean up session state, log-off logic |

### Scheduled reducers
Reducers can be scheduled for future execution:
```rust
#[spacetimedb::reducer]
pub fn spawn_enemies(ctx: &ReducerContext) {
    // schedule self to run again in 5 seconds
    ctx.schedule_reducer_after(Duration::from_secs(5), "spawn_enemies", &());
}
```
This enables game loops, timers, and AI tick systems without external schedulers.

---

## Views

Read-only functions that can be called via HTTP. Not callable by clients directly (no WebSocket path).
Used for server-side queries returning computed results.

```rust
#[spacetimedb::view]
pub fn get_leaderboard(ctx: &ViewContext) -> Vec<PlayerScore> {
    Player::iter()
        .map(|p| PlayerScore { name: p.name, score: p.score })
        .collect()
}
```

---

## Procedures

Functions callable via external HTTP requests (not by game clients via the normal protocol).
Used for admin operations, webhooks, or server-to-server communication.

---

## Module deployment lifecycle

```
1. Developer writes module in Rust/C#/TypeScript
2. Module compiles to WASM via standard toolchain (cargo build --target wasm32-unknown-unknown)
3. spacetime publish → uploads WASM binary to SpacetimeDB host
4. Host calls __describe_module__ → extracts schema (tables, reducers, indexes)
5. Host stores WASM binary + schema in ControlDb
6. __init__ reducer is called once
7. ModuleHost is launched; Wasmtime instance is warmed up
8. Clients can now call reducers and subscribe to tables
```

---

## WASM runtime internals

### Wasmtime instance lifecycle
- One `WasmInstance` per concurrent reducer execution (instances may be pooled)
- Instance is pre-compiled for faster startup (Wasmtime AOT compilation)
- Memory is reset between invocations (no persistent WASM memory across calls)
- All persistent state lives in the host-side database, not in WASM memory

### Known performance issue
WASM startup time and FFI overhead per ABI call add measurable latency.
An open GitHub issue (#2300) tracks reducing this via memoization and more direct call paths.
For UzDB in TML: since TML compiles to native code, this entire overhead category is eliminated.

---

## Anti-patterns observed

1. **Reducer calling reducer directly creates implicit coupling** — shared transaction makes it hard
   to reason about rollback boundaries. UzDB should make transaction boundaries explicit.

2. **No reducer versioning** — if a reducer's signature changes, all clients break immediately.
   UzDB should design versioned reducer signatures from day one.

3. **Views are not subscribable** — you cannot subscribe to a view, only to tables.
   This forces denormalization into tables for anything that needs real-time updates.
   UzDB could support subscribable computed views.
