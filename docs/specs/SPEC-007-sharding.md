# SPEC-007 — Sharding

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 5 · T5.4–T5.5 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-50, FR-51, FR-52, FR-53, FR-54 |
| **Requirement prefix** | `SHD-` |
| **Source** | UzDB spec 08, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `SHD-xxx`. Storage recovery is [SPEC-002](SPEC-002-storage-engine.md); transaction
semantics are [SPEC-003](SPEC-003-transactions.md); `TxUpdate` fan-out is
[SPEC-005](SPEC-005-subscriptions.md); wire messages are [SPEC-006](SPEC-006-protocol-fluxrpc.md).

## 1. Overview

A **shard** is an independent storage and execution unit that owns a horizontal **partition of the
keyspace**. Each shard has its own `MemStore`, `CommitLog`, `SnapshotRepo`, and
`SubscriptionManager`. Shards never share write locks: the single-writer discipline of
[SPEC-003](SPEC-003-transactions.md) applies *per shard*, so total write throughput scales with
the shard count.

Tables opt into partitioning by declaring a partition key
(`#[fluxum::table(partition_by(field))]`). Rows map to shards by **hash** or **range** of that
key; a **geospatial region** strategy is available for spatial workloads.

```
Keyspace (hash strategy, 4 shards)
├── Shard 0   stable_hash64(key) % 4 == 0    own MemStore + CommitLog + SubscriptionManager
├── Shard 1   stable_hash64(key) % 4 == 1
├── Shard 2   stable_hash64(key) % 4 == 2
└── Shard 3   stable_hash64(key) % 4 == 3

Keyspace (region strategy, 16000 × 16000 coordinate space, region_size 4000)
├── Shard 0   cell (0,0)–(4000,4000)         own MemStore + CommitLog + SubscriptionManager
├── Shard 1   cell (4000,0)–(8000,4000)
├── Shard 2   cell (0,4000)–(4000,8000)
└── Shard 3   cell (4000,4000)–(8000,8000)   … 4×4 = 16 cells mapped onto the shard set
```

`ShardCoord` is the global router that accepts all incoming connections and routes requests to
the correct shard. In the reference deployment every `ShardHost` runs as a tokio task inside one
process; PRD open question **OQ-2** tracks process-per-shard as an alternative deployment.

## 2. Partitioning model

- **SHD-001** [P0] A table **MAY** declare a partition key with
  `#[fluxum::table(partition_by(field))]`. The partition key **MUST** be either a single column
  of a hashable/orderable scalar type (`u8..u64`, `i8..i64`, `String`, `Identity`, `EntityId`)
  or exactly two numeric columns (region strategy). All rows of a partitioned table **SHALL**
  be stored on exactly one shard, determined by the table's partition strategy applied to the
  row's partition key value.

- **SHD-002** [P0] Three partition strategies **SHALL** be supported:

  | Strategy | Key form | Mapping |
  |---|---|---|
  | `hash` (default) | single column | `shard_id = stable_hash64(key) % shard_count` |
  | `range` | single orderable column | greatest configured boundary ≤ key → shard |
  | `region` | two numeric columns `(x, y)` | rectangular grid cell → shard (§3, SHD-012) |

- **SHD-003** [P0] Shard resolution **MUST** be deterministic and stable: for a fixed
  configuration, the same partition key value **SHALL** always resolve to the same shard across
  process restarts, platforms, and versions. The hash strategy **MUST** therefore use a stable,
  platform-independent 64-bit hash over the FluxBIN encoding of the key — never a per-process
  seeded hasher (e.g., randomized SipHash).

- **SHD-004** [P0] Tables without `partition_by` **SHALL** live on shard 0 (the **default
  shard**) unless declared `#[fluxum::table(global)]` (§5). PRD open question **OQ-6** tracks
  whether the default for non-partitioned tables becomes hash-of-primary-key instead of shard 0;
  until resolved, shard 0 is normative.

Canonical examples:

```rust
// Hash partitioning: chat traffic spreads across shards by channel.
#[fluxum::table(public, partition_by(channel))]
pub struct ChatMessage {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub sent_at: Timestamp,
}

// Hash partitioning by owner: each user's tasks co-locate on one shard.
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

// Region partitioning over two numeric columns: geospatial workloads.
#[fluxum::table(public, primary_key(grid_x, grid_y), partition_by(x, y))]
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

## 3. ShardCoord

- **SHD-010** [P0] The system **SHALL** implement a `ShardCoord` that owns the partition map and
  routes all incoming connections:

  ```rust
  pub type ShardId = u32;

  pub enum PartitionStrategy {
      /// shard_id = stable_hash64(fluxbin(key)) % shard_count
      Hash { shard_count: u32 },
      /// Ordered key boundaries; greatest boundary ≤ key wins.
      Range { boundaries: BTreeMap<RangeKey, ShardId> },
      /// Rectangular grid over two numeric columns.
      Region { bounds: Rect, region_size: f32, grid: BTreeMap<(i32, i32), ShardId> },
  }

  pub struct ShardCoord {
      partitioning: HashMap<TableId, PartitionStrategy>, // per partitioned table
      shards: BTreeMap<ShardId, ShardHandle>,            // live shard instances
      routing_table: HashMap<PartitionKey, ShardId>,     // resolved key → shard
      default_shard: ShardId,                            // shard 0 (SHD-004)
  }
  ```

- **SHD-011** [P0] `ShardCoord` **SHALL** accept all FluxRPC TCP and Streamable HTTP connections.
  After authentication, the client's `ReducerCall` messages **SHALL** be forwarded to the shard
  that owns the relevant data. A connection acquires **shard affinity** when the caller's rows
  live on a determinable shard (e.g., the shard owning the caller's session row or the entity
  row bound to the connection — for identity-partitioned domains the shard of
  `stable_hash64(identity)`; for region domains the shard containing the bound entity's current
  coordinates). Calls from a connection with affinity **SHALL** route to the affinity shard.
  Connections without affinity (unauthenticated clients, admin and observer sessions, callers
  with no partitioned rows) **SHALL** route to the default shard.

- **SHD-012** [P0] Given a partition key value, the owning shard **SHALL** be resolved as:

  ```
  hash:    shard_id = stable_hash64(fluxbin(key)) % shard_count

  range:   shard_id = boundaries[greatest boundary ≤ key]

  region:  grid_x   = floor(x / region_size)
           grid_y   = floor(y / region_size)
           shard_id = grid[(grid_x, grid_y)]
  ```

- **SHD-013** [P0] The number of shards and the partition strategies **SHALL** be configured via
  `config.yml` (env-var overrides use the `FLUXUM_` prefix):

  ```yaml
  sharding:
    shards: 16               # shard count; "auto" derives it for the region strategy
    strategy: hash           # default strategy: hash | range | region
    tables:                  # per-table overrides
      Sensor:
        strategy: region
        bounds: { x: 0, y: 0, width: 16000, height: 16000 }
        region_size: 4000    # 4000×4000 cells → 4×4 = 16 grid cells
      ChatMessage:
        strategy: range
        boundaries: [0, 1024, 2048, 3072]   # channel ranges → shards 0..3
  ```

- **SHD-014** [P2] Shard count changes **SHALL** require a restart. Online shard splitting or
  merging is out of scope for the initial version.

## 4. ShardHost

- **SHD-020** [P0] Each `ShardHost` **SHALL** be fully independent:

  ```rust
  pub struct ShardHost {
      shard_id: ShardId,
      partition: PartitionAssignment,       // the slice of the keyspace this shard owns
      store: MemStore,
      subscriptions: SubscriptionManager,
      commit_log: CommitLog,
      snapshot: SnapshotRepo,
      queue: mpsc::Receiver<ReducerCall>,   // bounded incoming work queue
      schedule: ScheduleWorker,             // fires #[fluxum::tick] and #[fluxum::schedule]
  }
  ```

- **SHD-021** [P0] Within a shard, the execution flow **SHALL** be:

  ```
  1. ReducerCall arrives in shard.queue
  2. ShardHost dequeues (single-writer — blocks until the previous transaction completes)
  3. ReducerExecutor runs the reducer fn against TxState
  4. On success:
     a. CommittedState merge (write lock, brief)
     b. CommitLog.append(tx_record) — async, no fsync per transaction
     c. SubscriptionManager.on_commit(delta) — fan-out to subscribers
     d. Release write lock
  5. ReducerResult sent to the caller
  ```

- **SHD-022** [P0] Each shard **SHALL** run its own `ScheduleWorker` that fires
  `#[fluxum::tick]` and `#[fluxum::schedule]` reducers for that shard. Tick reducers for shard A
  **SHALL NOT** affect shard B.

## 5. Global tables

- **SHD-030** [P1] Tables annotated `#[fluxum::table(global)]` **SHALL** be:
  - written to one **authoritative shard** (shard 0 by default, configurable);
  - replicated read-only to all other shards by `ShardCoord`.

  Replication **SHALL** happen synchronously after each commit to the authoritative shard:
  `ShardCoord` sends the mutation to all other shards, which apply it to their local
  `CommittedState` without generating a separate `CommitLog` entry.

  ```rust
  #[fluxum::table(global, public)]
  pub struct ServerConfig {
      #[primary_key]
      pub key: String,
      pub value: Vec<u8>, // MessagePack-encoded configuration value
  }
  ```

- **SHD-031** [P0] Reducers running on non-authoritative shards **SHALL NOT** insert, update,
  or delete rows in global tables. Attempting to do so **SHALL** return
  `Err("global table writes must execute on the authoritative shard")`.

## 6. Entity handoff protocol

An **entity** is the set of rows, across all tables of the same partition domain (tables
declaring `partition_by` over the same key type and strategy), that share one partition key
value. When a committed transaction changes an entity's partition key so that it maps to a
different shard — under the region strategy, updated coordinates crossing a grid-cell boundary;
under hash/range, the key column being rewritten (e.g., a `Task.owner` reassignment) — the
entity's row set must be migrated atomically. This is the **handoff protocol**.

```rust
// A moving entity under the region strategy: crossing a cell boundary triggers handoff.
#[fluxum::table(public, partition_by(x, y))]
#[spatial(quadtree(x, y))]
pub struct Vehicle {
    #[primary_key]
    pub id: EntityId,
    pub x: f32,
    pub y: f32,
    pub heading: f32,
    pub updated_at: Timestamp,
}
```

- **SHD-040** [P0] `ShardCoord` **SHALL** detect when a committed transaction moves a partition
  key across a shard boundary. The trigger is a committed row update in a partitioned table
  where the new partition key value resolves (SHD-012) to a different shard than the one storing
  the row.

  Detection: `ShardCoord` subscribes to updates of the partition-key columns of every
  partitioned table via an internal subscription (not the client-facing subscription system).
  On each `TxUpdate`, it re-resolves the partition key of every touched row and checks whether
  any row now maps to a different shard.

- **SHD-041** [P0] The handoff procedure for an entity moving from shard A to shard B **SHALL**
  be the following 11-step protocol:

  ```
   1. ShardCoord sends HandoffBegin(entity_key) to shard A (the current owner).
   2. Shard A executes the __handoff_export__ reducer for the entity: it reads every row of
      the entity's row set across all participating tables.
   3. Shard A serializes the row set (FluxBIN) into an opaque Vec<u8> buffer, returned as the
      reducer result.
   4. Shard A marks the entity "handoff pending" in the __handoff__ system table.
   5. Shard A commits. The entity still lives on shard A, but ShardCoord queues any new
      ReducerCall targeting it (SHD-044).
   6. ShardCoord sends HandoffImport(entity_key, buffer) to shard B (the new owner).
   7. Shard B executes the __handoff_import__ reducer: it deserializes the buffer and inserts
      every row of the row set into shard B's MemStore.
   8. Shard B commits.
   9. ShardCoord sends HandoffComplete(entity_key) to shard A.
  10. Shard A executes the __handoff_cleanup__ reducer: it deletes the entity's rows and the
      __handoff__ marker, then commits.
  11. ShardCoord updates the routing table and connection affinity: the calls queued at step 5
      and all subsequent ReducerCalls for the entity are delivered to shard B. The entity now
      lives entirely on shard B.
  ```

- **SHD-042** [P0] The handoff **SHALL** be atomic from the client's perspective:
  - the client **SHALL NOT** observe a state where the entity's rows exist on neither shard;
  - the client **SHALL NOT** observe a state where the entity's rows exist on both shards.

  If shard B's `__handoff_import__` fails, the entity remains on shard A (its state is still
  intact there) and the handoff **SHALL** be retried. If retries exhaust a configurable budget,
  `ShardCoord` **SHALL** abort the handoff: clear the `__handoff__` marker on shard A and
  deliver the queued calls to shard A.

- **SHD-043** [P1] The set of tables constituting an entity's row set is application-defined.
  Fluxum provides the infrastructure (trigger detection, inter-shard transport, the protocol of
  SHD-041) and **SHALL** autogenerate the `__handoff_export__`, `__handoff_import__`, and
  `__handoff_cleanup__` reducers from `partition_by` declarations: every table in the same
  partition domain participates, and the row set for key value `k` is every row whose partition
  key equals `k`. Applications **MAY** override the generated pair when entity state spans
  tables that are not co-partitioned (e.g., rows linked by foreign key rather than by the
  partition column).

- **SHD-044** [P1] `ReducerCall` messages arriving for an entity during an in-progress handoff
  **SHALL** be queued by `ShardCoord` and delivered to the destination shard after the handoff
  completes (or to the origin shard if the handoff is aborted per SHD-042). They **SHALL NOT**
  be dropped or failed.

## 7. Cross-shard communication

- **SHD-050** [P0] Reducer code **SHALL NOT** call reducers on other shards, nor read or write
  rows owned by another shard, within a transaction: the shard boundary **is** the transaction
  boundary, and there are **no cross-shard transactions**
  (see [SPEC-003](SPEC-003-transactions.md)). Cross-shard communication is exclusively handled
  by `ShardCoord` (for entity handoff and global-table replication), never by reducer code.

- **SHD-051** [P1] A client **MAY** be subscribed to tables on multiple shards simultaneously
  (e.g., an operations dashboard aggregating several partitions, or a spatial viewport spanning
  multiple region cells). `ShardCoord` **SHALL** aggregate `TxUpdate` messages from multiple
  shards for the same client and deliver them as separate `TxUpdate` messages tagged with the
  originating `shard_id` ([SPEC-005](SPEC-005-subscriptions.md),
  [SPEC-006](SPEC-006-protocol-fluxrpc.md)).

## 8. Shard lifecycle

- **SHD-060** [P0] Each shard **SHALL** go through the following startup phases:

  ```
  1. Recover MemStore from snapshot + commit-log replay (SPEC-002)
  2. Register #[fluxum::tick] and #[fluxum::schedule] workers
  3. Call the #[fluxum::on_shard_start] reducer (if defined)
  4. Mark the shard READY in the ShardCoord routing table
  5. Begin accepting ReducerCall messages from the queue
  ```

- **SHD-061** [P1] On shutdown signal, each shard **SHALL**:
  1. Stop accepting new `ReducerCall` messages (retryable unavailable error on FluxRPC
     transports; HTTP 503 on the admin API)
  2. Drain and complete all in-flight reducers
  3. Write a final snapshot
  4. Flush and close the CommitLog
  5. Signal `ShardCoord` that the shard is stopped

## Acceptance criteria

1. **Routing resolution** (T5.4): unit tests for all three strategies against golden vectors —
   the same key resolves to the same shard across process restarts and platforms (SHD-003);
   region resolution matches the `floor(x / region_size)` grid formula at and around cell
   boundaries.
2. **Shard independence**: with 2 shards, a panicking reducer and a saturated work queue on
   shard 0 leave shard 1 throughput and latency unaffected; `#[fluxum::tick]` reducers on each
   shard fire independently (SHD-020..022).
3. **Two-shard handoff with zero data loss** (T5.5 exit test): an entity with rows in at least
   two co-partitioned tables migrates from shard A to shard B while a client issues a continuous
   stream of `ReducerCall`s against it. Every call executes exactly once — none dropped, none
   duplicated (SHD-044); the post-handoff row set on B is byte-identical (FluxBIN) to the
   pre-handoff export; shard A retains no entity rows and no `__handoff__` marker; a subscribed
   client observes no interval in which the entity is absent and never sees duplicate rows
   (SHD-042).
4. **Handoff fault injection**: inject failure or crash at each of the 11 steps of SHD-041 —
   after retry (or abort back to shard A), the entity is readable and consistent on exactly one
   shard with no lost or duplicated rows.
5. **Global tables**: a write attempted on a non-authoritative shard returns the SHD-031 error;
   a committed write on the authoritative shard is readable on every shard before the
   originating `ReducerResult` returns; replica shards produce no `CommitLog` entries for
   replicated mutations (SHD-030).
6. **Cross-shard subscriptions**: a client subscribed on 2 shards receives updates from both,
   each `TxUpdate` tagged with the correct `shard_id`, with per-shard ordering preserved
   (SHD-051).
7. **Lifecycle**: kill -9 a shard and restart — recovery replays the commit log per SPEC-002
   with all committed transactions present; graceful shutdown (SHD-061) drains in-flight calls,
   writes a final snapshot, and closes the log so the next startup needs no replay.
