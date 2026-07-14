# 03 — Data Model

## Tables

Tables are the primary data structure. They are declared in the module language (Rust, C#, TypeScript)
and compiled into the WASM binary. The database host reads the schema from the module's
`__describe_module__` export at deployment time.

### Table declaration (Rust example)
```rust
#[spacetimedb::table(name = players, public)]
pub struct Player {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub name: String,
    pub x: f32,
    pub y: f32,
    pub health: u32,
}
```

### Table properties
- **Primary key:** single column, used for O(1) lookup and identity tracking in subscriptions
- **Auto-increment:** server-side counter, re-encoded on insert at the ABI layer (`_insert()` re-encodes auto-inc fields)
- **Public / private:** `public` tables are visible to client subscriptions; private tables are server-internal only
- **Unique constraints:** supported via index declarations
- **Multi-column indexes:** currently B-tree on single columns only (multi-column is a known limitation)

---

## Type system (SATS — SpacetimeDB Algebraic Type System)

SpacetimeDB defines its own algebraic type system used for schema definition, encoding, and SDK generation.

### Primitive types
| SATS Type | Size | Description |
|-----------|------|-------------|
| `bool` | 1 byte | Boolean |
| `u8`, `i8` | 1 byte | Unsigned/signed byte |
| `u16`, `i16` | 2 bytes | 16-bit integers |
| `u32`, `i32` | 4 bytes | 32-bit integers |
| `u64`, `i64` | 8 bytes | 64-bit integers |
| `u128`, `i128` | 16 bytes | 128-bit integers |
| `f32` | 4 bytes | IEEE 754 single |
| `f64` | 8 bytes | IEEE 754 double |
| `String` | variable | UTF-8 string |
| `Bytes` | variable | Raw byte array |

### Composite types
| SATS Type | Description |
|-----------|-------------|
| `ProductType` | Struct / row — ordered named fields |
| `SumType` | Enum / tagged union — one of N variants |
| `ArrayType` | Homogeneous list |
| `MapType` | Key-value map (limited support) |

### Identity types
| Type | Description |
|------|-------------|
| `Identity` | 256-bit unique client identity (derived from credentials) |
| `ConnectionId` | Per-connection ephemeral identifier |
| `Timestamp` | Microsecond-precision Unix timestamp |

---

## Encoding: BSATN

**Binary SpacetimeDB Algebraic Type Notation** is the wire and storage encoding for all values.

- Fixed-size types are encoded little-endian with no padding
- Variable-length types are prefixed with a `u32` length
- `SumType` (enum) is encoded as a `u8` tag followed by the variant payload
- `ProductType` (struct) is encoded as sequential field encodings with no separators
- Invalid UTF-8 in strings is handled lossily (replacement characters)
- All ABI buffer operations use BSATN; error codes are returned as `u16` status

**Why BSATN matters for UzDB:** it is more compact than JSON, faster to encode/decode than protobuf
for this use case (no reflection at runtime), and is strongly typed. UzDB should adopt a similar
binary algebraic encoding as its wire format.

---

## Storage layout

### In-memory structure
```
RelationalDB
└── Locking Datastore
    ├── CommittedState      ← stable snapshot, readable by all transactions
    │   └── [TableId → PagedTable]
    │       └── [Pages of fixed-size row slots]
    │           └── B-tree index per indexed column
    └── TxState             ← in-flight writes for the current transaction
        └── [TableId → delta rows (inserts + deletes)]
```

### Page structure (`crates/table`)
- Data is stored in fixed-size pages (similar to traditional RDBMS page management)
- Each page holds a variable number of rows depending on row size
- B-tree indexes are maintained per indexed column for O(log n) lookups
- Full table scans use the iterator ABI (`_iter_start`, `_iter_next`, `_iter_drop`)

### Commit log (`crates/commitlog`)
- Append-only file on disk
- Each entry encodes: transaction ID, timestamp, table mutations (insert/delete sets in BSATN)
- Written asynchronously by `DurabilityWorker` after in-memory commit — no synchronous disk wait on reducer calls
- On crash recovery: load latest snapshot (if any), then replay log entries from that point forward

### Snapshots (`crates/snapshot`)
- Periodic point-in-time page dumps managed by `SnapshotWorker`
- Accelerate recovery: instead of replaying the entire log, load the snapshot and replay only subsequent entries
- Snapshots are immutable; old snapshots are retained for a configurable window

---

## Indexing

| Index type | Status | Notes |
|------------|--------|-------|
| B-tree (single column) | Supported | Created via `_create_index()` ABI call |
| B-tree (multi-column) | Not yet supported | Known limitation |
| Hash index | Not documented | B-tree used for equality too |
| Spatial (R-tree, quadtree) | Not supported | Major gap for MMORPGs |
| Full-text | Not supported | Out of scope |

---

## Schema evolution

SpacetimeDB has limited automated migration support:
- Adding a column with a default value is supported
- Removing a column is **not** automatically supported — requires manual migration reducers
- Renaming a column requires a migration
- Type changes require manual data conversion

**Anti-pattern identified:** schema evolution in SpacetimeDB is a manual, error-prone process.
UzDB should design a first-class migration system from day one.
