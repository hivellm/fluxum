# 10 — TML Standard Library (UzDB-relevant subset)

Extracted from TML MCP docs (5120 indexed items). Only items relevant to UzDB design are listed.

---

## Type system

### Primitive types
| TML type | Description |
|----------|-------------|
| `Bool` | Boolean |
| `I8`, `I16`, `I32`, `I64` | Signed integers |
| `U8`, `U16`, `U32`, `U64` | Unsigned integers |
| `F32`, `F64` | IEEE 754 floats |
| `Str` | UTF-8 string |
| `Buffer` | Raw byte buffer |

### Generic types
| TML type | Description |
|----------|-------------|
| `List[T]` | Homogeneous list |
| `HashMap[K, V]` | Hash-based key-value map |
| `BTreeMap[K, V]` | Sorted key-value map (parallel sorted arrays) |
| `BTreeSet[T]` | Sorted set backed by `BTreeMap[T, I64]` |
| `Outcome[T, E]` | Error-handling type (equivalent to `Result` in Rust) |

### Key behaviors (traits)
| Behavior | Description |
|----------|-------------|
| `behavior Transaction` | Commit/rollback semantics — exactly one of commit/rollback before drop |
| `behavior Connection` | DB connection — execute queries + manage transactions |
| `behavior ToJson` | Serialize to JSON |
| `behavior AsRef[T]` | Cheap reference-to-reference conversion |
| `behavior IntoIterator` | Conversion into an iterator |
| `behavior AsyncDrop` | Async cleanup when value goes out of scope |
| `behavior AsyncFn[Args]` | Async callable by shared reference |

---

## Database layer (`std::db`)

### Driver abstraction
```
lib::std::src::db::driver::connection::Connection   (behavior)
lib::std::src::db::driver::transaction::Transaction (behavior)
lib::std::src::db::driver::pool::ConnectionPool     (struct)
```

- `Connection` behavior: every driver implements this to participate in the db abstraction layer.
  Methods: `execute` (no rows), `prepare` (result set via concrete connection type).
- `Transaction` behavior: commit/rollback semantics. Implementations must call exactly one before drop.
  Drivers supporting savepoints may expose additional methods on their concrete type.
- `ConnectionPool`: manages reusable connections.

### Concrete drivers
```
lib::std::src::db::sqlite::connection::SqliteConnection  (struct)  — SQLite backend
lib::std::src::sqlite::database::Database                (struct)  — direct SQLite connection
lib::postgresql::src::connection::PgConnection           (struct)  — PostgreSQL via libpq
```

### Query builders
```
lib::std::src::db::query::create_table::CreateTableQuery  (struct)
lib::std::src::db::query::alter_table::AlterTableQuery    (struct)
lib::std::src::db::query::drop_table::DropTableQuery      (struct)
```

### Key insight for UzDB
TML's `std::db` layer is a **driver abstraction over existing databases** (SQLite, PostgreSQL).
UzDB is not a wrapper — it is its own storage engine. TML's `Connection`/`Transaction` behaviors
are the right interface pattern to adopt for UzDB's internal storage API, but UzDB will
implement them from scratch rather than delegating to SQLite/PostgreSQL.

---

## Network layer (`std::http`, `std::http::websocket`)

### HTTP server
```
lib::std::src::http::server::server::HttpServer         (struct)
lib::std::src::http::server::server_response::ServerResponse (struct)
lib::std::src::http::server::connection::Connection     (struct)
lib::std::src::http::server::connection::ConnectionInfo (struct)  — TLS/connection metadata
```

### WebSocket (RFC 6455)
```
lib::std::src::http::websocket::websocket::WsMessage    (struct)  — complete message (1+ frames)
lib::std::src::http::websocket::websocket::WsFrame      (struct)  — parsed WS frame
lib::std::src::http::websocket::websocket::WsOpcode     (struct)  — RFC 6455 Section 5.2 opcodes
lib::std::src::http::websocket::websocket::WsError      (struct)  — WS operation errors
lib::std::src::http::websocket::websocket::WS_GUID      (const)   — magic string for Sec-WebSocket-Accept
```

Functions:
```tml
func ws_upgrade_response(client_key: Str) -> Str
  // Generates the HTTP/1.1 101 Switching Protocols response

func ws_validate_upgrade(headers: ref Headers) -> Outcome[Str, WsError]
  // Validates WebSocket upgrade request headers

func ws_mask(buf: ref Buffer, mk0: I32, mk1: I32, mk2: I32, mk3: I32)
  // XOR masking/unmasking (same operation per RFC 6455)
```

### Key insight for UzDB
TML has **native WebSocket support** at the frame/message level. UzDB's client protocol
(the equivalent of SpacetimeDB's `v1.bin.spacetimedb`) can be built directly on `WsMessage`
and `WsFrame` without external dependencies.

---

## Async & concurrency (`core::async`, `std::runtime`, `std::sync`)

### Task / executor
```
lib::std::src::runtime::channel::Channel         (struct)  — bounded async inter-task channel
lib::std::src::runtime::multi_executor::Task     (struct)  — unit of work for executor
lib::std::src::sync::mpsc::channel               (func)    — unbounded MPSC channel
lib::std::src::sync::async_mutex::AsyncMutex[T]  (struct)  — async mutual exclusion
```

Channel details:
- `Channel`: **bounded**, circular buffer, fixed capacity. Send to full / receive from empty = returns 0 (non-blocking).
- `sync::mpsc::channel[T]() -> (Sender[T], Receiver[T])`: **unbounded** MPSC channel.

### Threading
```tml
func spawn[T: Send](f: func() -> T) -> JoinHandle[T]
func spawn_fn(f: func()) -> UnitJoinHandle
func spawn_i64(f: func() -> I64) -> I64JoinHandle
```

### Key insight for UzDB
TML has both **bounded channels** (for backpressure, e.g. commit log writer) and
**unbounded MPSC** (for subscription fan-out). The `AsyncMutex` is sufficient for
protecting the `CommittedState` during MVCC merge.

---

## Collections (`std::collections`)

| Type | Behavior | Use in UzDB |
|------|----------|-------------|
| `HashMap[K, V]` | Hash lookup O(1) | Table row index by primary key |
| `BTreeMap[K, V]` | Sorted, range queries O(log n) | B-tree column index |
| `BTreeSet[T]` | Sorted unique set | Index leaf nodes |

---

## What is NOT in TML stdlib (must be built for UzDB)

| Missing primitive | UzDB must implement |
|-------------------|---------------------|
| Spatial index (QuadTree, R-tree) | `uzdb::index::QuadTree`, `uzdb::index::RTree` |
| Append-only commit log | `uzdb::commitlog::CommitLog` |
| MVCC datastore | `uzdb::store::MemStore` |
| BSATN / UzBIN encoder | `uzdb::encoding::UzBin` |
| Subscription manager | `uzdb::subscriptions::SubscriptionManager` |
| Shard coordinator | `uzdb::shard::ShardCoordinator` |

All of these must be written in TML on top of the available primitives
(`Buffer`, `BTreeMap`, `Channel`, `AsyncMutex`, `WsMessage`, etc.).
