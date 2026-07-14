# SPEC-001 — Data Model

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 1 · T1.1 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-03, FR-15, FR-16, FR-81 |
| **Requirement prefix** | `DM-` |
| **Source** | UzDB spec 02, ported TML → Rust and generalized (game → general-purpose) |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`DM-xxx` are stable. Integers are little-endian unless stated otherwise.

## 1. Scope

Fluxum's data model is table-centric: every persistent application entity is a row in a table.
Tables are declared as plain Rust structs annotated with `#[fluxum::table]` in the application
module — a native Rust crate compiled statically into the server binary (FR-03). The schema is
assembled at link time and used to build the in-memory storage structures (SPEC-002), validate
reducer calls (SPEC-004), filter subscriptions (SPEC-005), and drive SDK code generation
(SPEC-011).

This spec defines the table attribute surface, the column type system, index declarations, the
link-time schema registry with its `TableSchema` introspection structures, and row-level
visibility declarations. The `#[fluxum::*]` attribute surface and the `/schema` JSON defined
here freeze at the module API freeze (T6.1); post-freeze changes must be additive.

## 2. Table declarations

- **DM-001** [P0] The system SHALL recognise `#[fluxum::table(...)]` as the attribute that
  declares a Rust struct as a database table. Every named field of the struct SHALL become a
  column, in declaration order. The struct MUST use named fields (no tuple or unit structs).

```rust
use fluxum::Identity;

#[fluxum::table(public)]
pub struct User {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub identity: Identity,
    pub name: String,
}
```

- **DM-002** [P0] Every table SHALL have exactly one primary key: either a single column carrying
  the `#[primary_key]` field attribute, or a composite primary key (DM-003). The primary key
  SHALL be used for O(1) row lookup, identity tracking in subscriptions (SPEC-005), and as the
  key of the in-memory `BTreeMap<PrimaryKey, Row>` primary index (SPEC-002).

```rust
use fluxum::{ConnectionId, Identity, Timestamp};

#[fluxum::table(public)]
pub struct OnlineUser {
    #[primary_key]
    pub identity: Identity,
    pub connection_id: ConnectionId,
    pub connected_at: Timestamp,
}
```

- **DM-003** [P0] A table MAY declare a composite primary key using the table-level argument
  `primary_key(col1, col2, ...)`. In this case, no field carries `#[primary_key]`. The composite
  PK is treated as a tuple key in the in-memory store: the primary index SHALL be a
  `BTreeMap<(Pk1, Pk2, ...), Row>`, and the composite PK MUST be unique across all rows.
  In `TxUpdate` delete entries, all PK field values SHALL be included in the FluxBIN-encoded
  delete row, in column declaration order (SPEC-006). A table SHALL NOT have both a
  `#[primary_key]` field attribute AND a table-level `primary_key(...)` argument; the proc macro
  SHALL reject that combination at compile time.

```rust
use fluxum::Timestamp;

#[fluxum::table(public, primary_key(grid_x, grid_y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    pub x: f32,
    pub y: f32,
    pub reading: f64,
    pub updated_at: Timestamp,
}
```

- **DM-004** [P0] A primary key column MAY be annotated with `#[auto_inc]`. The runtime SHALL
  assign a monotonically increasing value on insert; the caller SHALL pass `0` for this field.
  `#[auto_inc]` is only valid on a single-column `#[primary_key]` of type `u64` — composite PKs
  do not support auto-increment (compile-time error).

```rust
use fluxum::{Identity, Timestamp};

#[fluxum::table(public)]
pub struct ChatMessage {
    #[primary_key]
    #[auto_inc]
    pub id: u64, // caller passes 0; runtime assigns
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub sent_at: Timestamp,
}
```

- **DM-005** [P0] Tables are private by default — only server-side reducer code can read them.
  A table declared `#[fluxum::table(public)]` SHALL be visible to client subscriptions. A table
  declared `#[fluxum::table(private)]` (or with no access argument) SHALL NOT appear in any
  `InitialData` or `TxUpdate` message sent to clients (SPEC-005, SPEC-006).

```rust
#[fluxum::table(public)]
pub struct OnlineUser { /* ... clients can subscribe */ }

#[fluxum::table] // private by default
pub struct SessionSecret { /* ... server-internal only */ }
```

- **DM-006** [P1] A table MAY declare a multi-column unique constraint with the table-level
  attribute `#[unique(col1, col2, ...)]`. The runtime SHALL reject any insert that would violate
  the uniqueness constraint and roll back the containing transaction (SPEC-003).

- **DM-007** [P0] Each table instance SHALL belong to exactly one shard. A table declared
  `#[fluxum::table(global)]` SHALL be replicated read-only to all shards by the `ShardCoord`
  after every committed write to the owning shard (SPEC-007).

- **DM-008** [P0] A table MAY declare a partition key with the table-level argument
  `partition_by(field)`. The named field MUST be an existing column of the table. Rows SHALL map
  to shards by hash or range of the partition key, and a geospatial region strategy SHALL remain
  available for spatial workloads (strategy selection and routing are SPEC-007). When a committed
  write changes a row's partition key to a value owned by another shard, the runtime SHALL
  perform an entity handoff — the atomic migration of that row set between shards (SPEC-007).
  Tables without `partition_by` SHALL live on shard 0 unless declared `global`; `partition_by`
  MUST NOT be combined with `global` (compile-time error).

```rust
use fluxum::Identity;

// Hash-partitioned by owner: all of a user's tasks live on one shard.
#[fluxum::table(public, partition_by(owner))]
#[visibility(owner_only(owner))]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
}
```

## 3. Type system

Column types are a closed subset of Rust types with a defined FluxBIN wire encoding (SPEC-006).

- **DM-010** [P0] The following primitive Rust types SHALL be valid column types:

| Rust type | Wire encoding | Notes |
|-----------|---------------|-------|
| `bool` | 1 byte | `0x00` false, `0x01` true |
| `i8`, `i16`, `i32`, `i64` | 1/2/4/8 bytes LE | Signed integer |
| `u8`, `u16`, `u32`, `u64` | 1/2/4/8 bytes LE | Unsigned integer |
| `f32`, `f64` | 4/8 bytes LE | IEEE 754 |
| `String` | `u32 LE length` + UTF-8 bytes | Variable length |
| `Vec<u8>` | `u32 LE length` + raw bytes | Variable length |

- **DM-011** [P0] The following newtypes SHALL be first-class column types with dedicated
  MessagePack encoding tags in the FluxRPC protocol (SPEC-006):

| Type | Size | Description |
|------|------|-------------|
| `Identity([u8; 32])` | 32 bytes | SHA-256 of auth token; stable across sessions (SPEC-009) |
| `ConnectionId(u128)` | 16 bytes | Random `u128`; ephemeral per connection |
| `EntityId(u64)` | 8 bytes | `u64` newtype; generic row/entity identifier |
| `Timestamp(i64)` | 8 bytes | `i64` microseconds since Unix epoch |

- **DM-012** [P1] The following composite types SHALL be valid column types:

| Rust type | Description | Encoding |
|-----------|-------------|----------|
| `Vec<T>` | Homogeneous list | Length-prefixed array (`u32 LE` count + elements) |
| `Option<T>` | Nullable column | `0x00` (None) or `0x01` + encoded `T` |

  `HashMap`/`BTreeMap` and nested `#[fluxum::table]` struct types SHALL NOT be valid column
  types (compile-time error). Relationships between tables are expressed via foreign-key-style
  `EntityId`/`u64` columns (see §7).

## 4. Attributes (full list)

- **DM-020** [P0] The complete attribute catalogue SHALL be:

| Attribute | Target | Description |
|-----------|--------|-------------|
| `#[fluxum::table(...)]` | struct | Declares a database table |
| `#[primary_key]` | field | Single-column primary key |
| `primary_key(cols...)` | table argument | Composite primary key across multiple columns |
| `#[auto_inc]` | field (`u64`) | Server-assigned monotonic ID (single-column `#[primary_key]` only) |
| `public` | table argument | Visible to client subscriptions |
| `private` | table argument | Server-internal only (default) |
| `global` | table argument | Replicated read-only to all shards |
| `partition_by(field)` | table argument | Partition key for horizontal sharding (SPEC-007) |
| `#[unique(cols...)]` | table | Multi-column unique constraint |
| `#[index(btree(col))]` | table | B-tree secondary index on `col` |
| `#[index(btree(a, b))]` | table | Composite B-tree index on multiple columns |
| `#[spatial(quadtree(x, y))]` | table | QuadTree spatial index (2D point geometry, SPEC-008) |
| `#[spatial(rtree(min_x, min_y, max_x, max_y))]` | table | R-tree spatial index (bounding-box range queries, SPEC-008) |
| `#[visibility(rule)]` | table | Declarative row-level security filter (§8, SPEC-005) |
| `#[fluxum::reducer]` | fn | Declares a mutation function callable by clients (SPEC-004) |
| `#[fluxum::view]` | fn | Read-only query callable via HTTP (SPEC-004) |
| `#[fluxum::procedure]` | fn | Admin-callable function, not exposed to client applications (SPEC-004) |
| `#[fluxum::tick(rate = N)]` | fn | High-frequency periodic reducer (SPEC-004) |
| `#[fluxum::schedule]` | fn | Deferred reducer, one-shot or recurring via `ctx.schedule_after(...)` (SPEC-004) |
| `#[fluxum::on_init]` | fn | Lifecycle hook: first shard startup (SPEC-004) |
| `#[fluxum::on_connect]` | fn | Lifecycle hook: client connects (SPEC-004) |
| `#[fluxum::on_disconnect]` | fn | Lifecycle hook: client disconnects (SPEC-004) |
| `#[fluxum::migration(version = N)]` | fn | Schema migration reducer (SPEC-010) |

## 5. Indexes

- **DM-030** [P0] A table annotated with `#[index(btree(column_name))]` SHALL have a B-tree
  secondary index on that column. The index SHALL support:
  - Equality lookup: O(log n)
  - Range scan: O(log n + k) where k = matching rows

```rust
use fluxum::Identity;

#[fluxum::table(public)]
#[index(btree(owner))]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
}
```

- **DM-031** [P1] A table annotated with `#[index(btree(col1, col2))]` SHALL have a B-tree index
  on the concatenated key `(col1, col2)`. Prefix scans (equality on `col1`, range on `col2`)
  SHALL be supported.

```rust
// Equality on channel + range on sent_at resolves via the composite index.
#[fluxum::table(public)]
#[index(btree(channel, sent_at))]
pub struct ChatMessage { /* ... */ }
```

- **DM-032** [P0] A table annotated with `#[spatial(quadtree(x, y))]` SHALL maintain a QuadTree
  index over the two designated coordinate columns. The designated columns MUST be a pair of
  `f32` or `f64` columns of the same table, explicitly named in the attribute. Spatial indexes
  are intended for geospatial row sets queried by location — zones, sensors, vehicles, POIs
  (see SPEC-008 for query semantics and update cost). Every committed write to an indexed
  coordinate column updates the index, so very high-churn columns bear that maintenance cost.

```rust
use fluxum::Timestamp;

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

- **DM-033** [P0] A table SHALL NOT have both `#[spatial(quadtree(...))]` and
  `#[spatial(rtree(...))]`. A column SHALL NOT be indexed twice with the same index type.
  Violations SHALL be rejected at compile time by the proc macro.

## 6. Schema declaration, registry, and introspection

- **DM-040** [P0] `#[fluxum::table]` SHALL generate a `static` `TableSchema` (DM-042) and
  register it in a **link-time registry** (distributed-slice collection via `inventory` or
  `linkme` — PRD OQ-1). Reducers, views, procedures, ticks, schedules, lifecycle hooks, and
  migrations SHALL be registered the same way by their respective macros. At startup,
  `fluxum::ServerBuilder::build()` SHALL collect every registered declaration into the
  **schema** before opening any transport. There is no source-file scanning, no WASM sandbox,
  no FFI, and no dynamic loading (FR-03). `build()` SHALL validate the collected schema and
  abort startup with a descriptive error on: duplicate table names, a table with zero or two
  primary key declarations, `#[auto_inc]` on a non-`u64` or composite PK, spatial columns that
  are missing or not `f32`/`f64`, a `partition_by`/`#[unique]`/`#[visibility]` reference to a
  nonexistent column, `partition_by` combined with `global`, or an `owner_only` owner field
  that is not of type `Identity`. Constraints checkable from a single struct SHOULD instead be
  rejected at compile time by the proc macro (golden-file `trybuild` tests).

- **DM-041** [P0] The schema SHALL NOT change while the server is running. The schema is fixed
  at compile time; schema changes ship as a new server binary and take effect only on restart,
  after applying any pending `#[fluxum::migration]` reducers (SPEC-010).

- **DM-042** [P0] The schema SHALL be introspectable in-process through the following structures
  (generated by the macros, `'static`, allocation-free):

```rust
/// One registered table. Generated by #[fluxum::table]; collected at link time.
pub struct TableSchema {
    pub name: &'static str,                // struct name; unique across the module
    pub columns: &'static [ColumnSchema],  // declaration order == FluxBIN column order
    pub primary_key: &'static [u16],       // column ordinals; len == 1 unless composite
    pub auto_inc: Option<u16>,             // ordinal of the #[auto_inc] column, if any
    pub access: TableAccess,               // Private (default) | Public | Global
    pub partition_by: Option<u16>,         // partition-key column ordinal (SPEC-007)
    pub unique: &'static [&'static [u16]], // multi-column unique constraints
    pub indexes: &'static [IndexSchema],
    pub visibility: VisibilityRule,        // §8; PublicAll unless declared
}

pub struct ColumnSchema {
    pub name: &'static str,
    pub ty: FluxType,
}

/// Closed column type universe (§3).
pub enum FluxType {
    Bool,
    I8, I16, I32, I64,
    U8, U16, U32, U64,
    F32, F64,
    Str,
    Bytes,
    Identity, ConnectionId, EntityId, Timestamp,
    Option(&'static FluxType),
    List(&'static FluxType),
}

pub enum IndexSchema {
    BTree { columns: &'static [u16] },
    Spatial { kind: SpatialKind, columns: &'static [u16] },
}

pub enum SpatialKind { QuadTree, RTree }

pub enum TableAccess { Private, Public, Global }

pub enum VisibilityRule {
    PublicAll,
    OwnerOnly { owner: u16 },       // ordinal of the Identity owner column
    ShardLocal,
    Custom(&'static str),           // registered filter function (DM-061)
}
```

- **DM-043** [P0] For each table struct `T`, `#[fluxum::table]` SHALL also generate an
  implementation of the `Table` trait, which the storage engine (SPEC-002), the `TxHandle` typed
  accessors (`insert::<T>`, `query_pk::<T>`, `scan::<T>`, ... — SPEC-004), and the FluxBIN codec
  (SPEC-006) consume:

```rust
pub trait Table: Sized + 'static {
    /// Primary key type; a tuple for composite PKs, e.g. (i32, i32) for Sensor.
    type Pk: Ord + Clone + Send;

    const SCHEMA: &'static TableSchema;

    fn primary_key(&self) -> Self::Pk;

    /// FluxBIN row encoding, schema-driven, columns in declaration order (SPEC-006).
    fn encode_row(&self, buf: &mut Vec<u8>);
    fn decode_row(bytes: &[u8]) -> Result<Self, FluxumError>;
}
```

- **DM-044** [P0] The system SHALL expose the full schema — tables with columns, types, and
  attributes (PK, auto-inc, access, partition key, unique constraints, indexes, visibility);
  reducers with parameter types; views; procedures — as a JSON document at `GET /schema` on
  the HTTP admin API (:15800), and via `fluxum schema export` from the CLI. This document is
  rendered from the `TableSchema` registry and is the single source of truth for SDK code
  generation (FR-81, SPEC-011). It SHOULD include a stable schema hash/version so generated SDKs
  can detect drift (SPEC-011). *(Uplifted from P1 in the source spec to match FR-81 [P0].)*

## 7. Entity-component pattern (relational composition)

- **DM-050** [P1] The system SHALL support the relational entity-component pattern, where
  multiple tables share a common entity ID as a foreign-key-style column — e.g. a fleet-tracking
  module composing a tracked asset from separate location and battery-telemetry tables:

```rust
use fluxum::EntityId;

#[fluxum::table(public)]
pub struct Entity {
    #[primary_key]
    #[auto_inc]
    pub id: EntityId,
}

#[fluxum::table(public)]
#[index(btree(entity_id))]
pub struct Location {
    #[primary_key]
    pub entity_id: EntityId,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[fluxum::table(public)]
#[index(btree(entity_id))]
pub struct Battery {
    #[primary_key]
    pub entity_id: EntityId,
    pub level_mv: u32,
    pub capacity_mv: u32,
}
```

  The system SHALL NOT enforce referential integrity between these tables (no foreign key
  constraints). Integrity is the reducer's responsibility. This keeps the storage layer simple
  and avoids cascading-delete overhead.

## 8. Visibility (row-level security)

- **DM-060** [P0] A `public` table MAY declare `#[visibility(owner_only(owner_column))]`. The
  runtime SHALL automatically filter subscription results so each client only receives rows
  where `owner_column == ctx.identity` — applied to both `InitialData` and `TxUpdate` diffs
  (SPEC-005). The owner column MUST be of type `Identity`. *(Uplifted from P1 in the source spec
  to match FR-32 [P0].)*

```rust
use fluxum::Identity;

#[fluxum::table(public)]
#[visibility(owner_only(owner))]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
}
```

- **DM-061** [P1] The visibility rules catalogue SHALL be:

| Rule | Behaviour |
|------|-----------|
| `owner_only(field)` | Client sees only rows where `field ==` the client's `Identity` |
| `public_all` | All clients see all rows (default for `public` tables) |
| `shard_local` | Client sees only rows belonging to its current shard |

  Custom rules MAY be expressed as Rust functions registered with the runtime and referenced via
  `#[visibility(custom(filter_fn))]`, where `filter_fn` has the signature
  `fn(row: &T, viewer: &Identity) -> bool`. Trusted server peers (`SHA-256("SERVER:" + name)`
  identities, SPEC-009) bypass row-level security.

## 9. Source requirement mapping

| UzDB (spec 02) | Fluxum | Notes |
|---|---|---|
| REQ-02-1 | DM-001 | `@table` → `#[fluxum::table]` |
| REQ-02-2 | DM-002 | `@pk` → `#[primary_key]` |
| REQ-02-2a | DM-003 | `@compositePk(cols)` → `primary_key(a, b)` table argument |
| REQ-02-3 | DM-004 | `@autoInc` → `#[auto_inc]` |
| REQ-02-4 | DM-005 | `@public`/`@private` → table arguments |
| REQ-02-5 | DM-006 | `@unique(cols)` → `#[unique(a, b)]` |
| REQ-02-6 | DM-007 | `@global` → `#[fluxum::table(global)]` |
| — (new) | DM-008 | `partition_by` — keyspace partitioning replaces world-region sharding (FR-50) |
| REQ-02-7 | DM-010 | TML primitives → Rust primitives |
| REQ-02-8 | DM-011 | Identity/ConnectionId/EntityId/Timestamp newtypes |
| REQ-02-9 | DM-012 | `List[T]`/`Option[T]` → `Vec<T>`/`Option<T>` |
| REQ-02-10 | DM-020 | Annotation catalogue → attribute catalogue |
| REQ-02-11 | DM-030 | `@index(col)` → `#[index(btree(col))]` |
| REQ-02-12 | DM-031 | `@compositeIndex(cols)` → `#[index(btree(a, b))]` |
| REQ-02-13 | DM-032 | Spatial columns now always explicit; game-geometry restriction generalized |
| REQ-02-14 | DM-033 | Duplicate-index rejection moved to compile time |
| REQ-02-15 | DM-040 | Startup TML source scan → link-time registry + `ServerBuilder` validation |
| REQ-02-16 | DM-041 | Unchanged |
| — (new) | DM-042, DM-043 | `TableSchema` introspection structs + generated `Table` trait (Rust-specific) |
| REQ-02-17 | DM-044 | P1 → P0 (FR-81) |
| REQ-02-18 | DM-050 | ECS example de-gamed (asset/location/battery) |
| REQ-02-19 | DM-060 | P1 → P0 (FR-32); example ported to `Task` |
| REQ-02-20 | DM-061 | Custom rules as registered Rust functions |

## Acceptance criteria

1. **Macro surface (T1.1):** golden-file expansion tests (`trybuild`) cover every attribute in
   the DM-020 catalogue; each invalid combination — `#[primary_key]` plus table-level
   `primary_key(...)`, `#[auto_inc]` on a composite or non-`u64` PK, `quadtree` plus `rtree` on
   one table, duplicate same-type index on one column, `partition_by` with `global`, non-`f32/f64`
   spatial columns — fails to compile (or aborts `ServerBuilder::build()` where compile-time
   detection is impossible) with the specified diagnostics.
2. **Registry:** tables and reducers declared across at least two workspace crates all appear in
   the schema assembled by `ServerBuilder::build()`; a duplicate table name aborts startup with a
   descriptive error; the process performs no schema file I/O and no dynamic loading.
3. **Type round-trip:** property tests encode/decode every `FluxType` (including nested
   `Option`/`Vec`) through the generated `encode_row`/`decode_row` byte-exactly per the §3
   encoding tables; `HashMap` and nested table-struct columns are rejected at compile time.
4. **Composite PK:** `Sensor` keyed by `(grid_x, grid_y)` supports insert, `query_pk`, and
   delete; uniqueness over the tuple is enforced; `TxUpdate` delete entries carry all PK fields
   in declaration order.
5. **Auto-increment:** concurrent inserts with `id = 0` receive strictly increasing unique IDs;
   IDs survive restart (no reuse after log replay, SPEC-002).
6. **Constraints:** an insert violating `#[unique(a, b)]` rolls back its whole transaction and
   leaves committed state and all indexes untouched.
7. **Indexes:** equality and range scans on `#[index(btree(owner))]` and prefix scans on
   `#[index(btree(channel, sent_at))]` return exactly the rows a full scan would, verified by
   property tests.
8. **Introspection:** `GET /schema` and `fluxum schema export` emit identical JSON covering
   all tables, columns, types, PKs, indexes, partition keys, visibility rules, reducers, views,
   and procedures; SPEC-011 codegen golden tests consume this document unchanged.
9. **Visibility:** with `#[visibility(owner_only(owner))]` on `Task`, two clients subscribing to
   `SELECT * FROM Task` each receive only their own rows in both `InitialData` and subsequent
   diffs (joint test with SPEC-005); a private `SessionSecret` table never appears in any client
   message.
