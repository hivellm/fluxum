# SPEC-002 — Storage Engine

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 2 · T2.1–T2.4 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-10, FR-12, FR-13, FR-14, FR-18 (interface only); NFR-02, NFR-06, NFR-07, NFR-08 |
| **Requirement prefix** | `STG-` |
| **Related** | [SPEC-014](SPEC-014-replication.md) (commit log = replication stream) · [SPEC-015](SPEC-015-tiered-storage.md) (buffer pool, pager, compression) |
| **Source** | UzDB spec 03, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `STG-xxx`. Integers little-endian unless stated otherwise. The commit-log entry
format shares the FluxRPC framing (SPEC-006), doubles as the replication stream format
([SPEC-014](SPEC-014-replication.md), STG-016), and is part of the **wire freeze at gate G5**
together with the on-disk page format ([SPEC-015](SPEC-015-tiered-storage.md)).
The transaction pipeline built on top of this engine is SPEC-003; reducer execution is SPEC-004.

## 1. Overview

Fluxum's storage engine has three components that work together, instantiated **once per shard**
(SPEC-007):

```
MemStore         — transactional store (CommittedState + TxState)
CommitLog        — append-only on-disk log for durability; also the replication stream (SPEC-014)
SnapshotRepo     — periodic checkpoints that accelerate crash recovery and bound log growth
```

`CommittedState` is a **logical** view over tiered physical storage
([SPEC-015](SPEC-015-tiered-storage.md), FR-18): the hot working set resides uncompressed in
the buffer pool — a buffer-pool hit performs **zero disk I/O** and completes in < 1 µs
(NFR-02, NFR-07) — while cold rows live in the paged, compressed on-disk tier and fault into
the buffer pool on demand under the configured memory budget. The buffer pool, pager, eviction,
and compression are specified in SPEC-015 (`TIER-` requirements); this spec owns MemStore
semantics (MVCC, commit, rollback), the CommitLog, checkpoints, and recovery — all of which
operate on the logical `CommittedState` and are unaffected by row residency. Disk writes on
the commit path are limited to the CommitLog append; checkpointing and cold-tier eviction are
background work. These types live in `fluxum-core` and have no network dependencies.

## 2. MemStore

- **STG-001** [P0] The system SHALL implement a `MemStore` with two logical regions:

  ```rust
  pub struct MemStore {
      /// Stable snapshot — readable by all transactions.
      committed: CommittedState,
      /// In-flight mutations (at most one at a time per shard).
      tx: Option<TxState>,
  }
  ```

- **STG-002** [P0] `CommittedState` SHALL store one entry per table. Each table entry SHALL
  contain the primary row map, secondary B-tree indexes, and an optional spatial index:

  ```rust
  pub struct CommittedState {
      tables: HashMap<TableId, Table>,
  }

  pub struct Table {
      /// O(log n) pk lookup; ordered for range scans.
      rows: BTreeMap<PrimaryKey, Row>,
      /// Secondary B-tree indexes (SPEC-001).
      indexes: HashMap<IndexId, BTreeIndex>,
      /// QuadTree or R-tree, if the table carries #[spatial(...)] (SPEC-008).
      spatial: Option<SpatialIndex>,
  }
  ```

  Row storage uses `BTreeMap` to enable range scans and ordered iteration in addition to
  O(log n) point lookup.

  `Table.rows` (and the index structures) are **logical** maps: hot entries are served from the
  buffer pool with zero disk I/O, while entries whose pages have been evicted to the cold tier
  are transparently faulted in on access (SPEC-015). Residency SHALL be invisible to callers —
  lookups, scans, and range iteration return identical results whether a row is hot or cold;
  only latency differs. The MVCC, commit, and rollback semantics below (STG-003–STG-006) are
  defined over this logical view and are unchanged by tiering.

- **STG-003** [P0] `TxState` SHALL be a write buffer for a single in-flight transaction:

  ```rust
  pub struct TxState {
      tx_id: u64,
      inserts: HashMap<TableId, Vec<Row>>,
      deletes: HashMap<TableId, Vec<PrimaryKey>>,
  }
  ```

  There SHALL be at most one active `TxState` per shard at any time (single-writer guarantee,
  FR-12). Attempting to begin a second concurrent transaction on the same shard SHALL block
  until the first commits or rolls back.

- **STG-004** [P0] **MVCC read isolation.** Default reads during a transaction
  (`query_pk::<T>`, `scan::<T>`, `scan_where::<T>` on `TxHandle`) SHALL see only
  `CommittedState`. They SHALL NOT see uncommitted inserts from the same transaction's
  `TxState`. This gives every reducer consistent snapshot semantics over committed data.
  In-flight writes are observable only through the explicit intra-transaction read methods
  `scan_pending::<T>`, `scan_all::<T>`, and `count_pending::<T>` (specified in SPEC-004,
  FR-17) — never through the default read path.

  **Scenario: read isolation**
  ```
  Given  a transaction has inserted row R into TxState
  When   the same transaction reads the same table via scan::<T> or query_pk::<T>
  Then   row R is NOT visible — only CommittedState rows are returned
         (R is visible only via scan_pending::<T> / scan_all::<T>, SPEC-004)
  ```

- **STG-005** [P0] **Transaction commit (merge).** On successful reducer completion, the
  system SHALL:

  1. Acquire the commit mutex on `CommittedState`
  2. Apply all inserts from `TxState` into `CommittedState.tables[id].rows`
  3. Remove all deletes from `CommittedState.tables[id].rows`
  4. Update all affected secondary indexes
  5. Update any affected spatial indexes
  6. Set `MemStore.tx = None`
  7. Release the commit mutex

  The merge SHALL be atomic from the perspective of other readers — no intermediate state
  SHALL be observable.

- **STG-006** [P0] **Transaction rollback.** On reducer error — any `Err` return, or a panic
  caught by the executor's `std::panic::catch_unwind` boundary (SPEC-004) — the system SHALL:

  1. Discard `TxState` entirely (no writes applied to `CommittedState`)
  2. Set `MemStore.tx = None`
  3. Return the error to the caller via the `ReducerResult` (`Err(message)`)

  No partial state SHALL be visible in `CommittedState` after a rollback.

## 3. CommitLog

- **STG-010** [P0] **Append-only log.** The system SHALL maintain an append-only commit log on
  disk per shard. Every committed transaction SHALL be appended to the log BEFORE the
  `ReducerResult` is sent to the caller. The log provides durability: after a crash, the
  system SHALL be able to reconstruct all committed state by replaying the log from the last
  checkpoint (§4).

- **STG-011** [P0] **Log entry format.** Each log entry SHALL use the same framing as FluxRPC
  (SPEC-006): a `u32` little-endian length prefix, a MessagePack body (`rmp-serde`), and a
  trailing CRC32 checksum (`crc32fast`) over the body bytes:

  ```
  ┌──────────────────┬───────────────────────────────┬──────────────────┐
  │ length: u32 (LE) │ TxRecord: MessagePack bytes   │ crc32: u32 (LE)  │
  └──────────────────┴───────────────────────────────┴──────────────────┘
  ```

  ```rust
  #[derive(Serialize, Deserialize)]
  pub struct TxRecord {
      /// Monotonically increasing, per-shard (STG-015).
      tx_id: u64,
      /// Microseconds since Unix epoch.
      timestamp: i64,
      shard_id: u32,
      mutations: Vec<TableMutation>,
  }

  #[derive(Serialize, Deserialize)]
  pub struct TableMutation {
      table_id: u32,
      /// Full row values (not diffs).
      inserts: Vec<Row>,
      deletes: Vec<PrimaryKey>,
  }
  ```

  Using the same `u32 LE + MessagePack` framing as the wire protocol means a single codec
  covers wire encoding, commit log, checkpoint, and replication-stream formats (STG-016).
  This entry format freezes with the wire format at gate G5.

- **STG-012** [P0] **Async log write (no fsync per transaction).** The CommitLog writer SHALL
  run in a dedicated background thread. Log entries SHALL be appended asynchronously — the
  runtime SHALL NOT call `fsync` after every transaction. The durability gap SHALL be bounded
  by the OS write-behind buffer (typically < 50 ms; NFR-08). This is acceptable for realtime
  application workloads (chat, presence, telemetry), which can tolerate losing the last few
  milliseconds of state on a process crash.

  **Rationale:** synchronous fsync per transaction would limit throughput to disk IOPS
  (~1,000/s on HDD, ~10,000/s on SSD). Async append enables 100,000+ tx/s.

- **STG-013** [P1] **Log rotation and compaction.** The CommitLog SHALL rotate to a new file
  segment when the current segment reaches a configurable size threshold (`segment_max_bytes`,
  default: 128 MB). Old segments SHALL be retained until a **completed checkpoint** covers
  their `tx_id` range, after which they MAY be deleted (log compaction, FR-14) — subject to
  any replication retention hold: segments still needed by a connected replica's replication
  offset are retained per SPEC-014.

- **STG-014** [P1] **Log file naming.** Log segment files SHALL be named
  `shard-<shard_id>-<first_tx_id>.log` in the configured `commit_log_dir`. This enables the
  recovery process to identify the correct segment to begin replay from without reading
  segment contents.

- **STG-015** [P0] **`tx_id` monotonicity.** `tx_id` SHALL be a per-shard `u64` counter that
  increases monotonically across the shard's lifetime, including restarts: after recovery the
  counter SHALL resume from the last recovered `tx_id` (next transaction =
  `recovered_tx_id + 1`). Within a shard's log, entries SHALL appear in strictly increasing
  `tx_id` order; replay SHALL treat a decrease or repeat as corruption (STG-031).

- **STG-016** [P0] **Log doubles as the replication stream.** The commit-log entry format
  (STG-011) SHALL also be the replication stream format: replicas consume `TxRecord` entries
  verbatim from the log, addressed by log offset ([SPEC-014](SPEC-014-replication.md),
  FR-100) — the log IS the replication protocol, with no separate encoding. Consequently the
  entry format SHALL be frozen at gate G5 together with the FluxRPC wire format (SPEC-006)
  and the on-disk page format ([SPEC-015](SPEC-015-tiered-storage.md)); post-G5 changes
  require a versioned format migration.

## 4. Checkpoints (SnapshotRepo)

- **STG-020** [P0] **Periodic checkpoints.** A `SnapshotWorker` SHALL periodically write a
  full point-in-time dump of `CommittedState` — a **checkpoint** — to the `snapshot_dir`. The
  default interval is every 10,000 committed transactions (`snapshot_interval_tx: 10000` in
  `config.yml`; FR-14). A snapshot file IS the checkpoint mechanism referenced by FR-13/FR-14;
  the `Snapshot*` type names and `snapshot_*` config keys are retained for continuity.
  Checkpoints serve two purposes: (a) crash-recovery acceleration — instead of replaying the
  entire commit log from the beginning, the system loads the latest checkpoint and replays
  only log entries after its `last_tx_id`; and (b) **log compaction** — a completed checkpoint
  covers all transactions ≤ its `last_tx_id`, allowing older commit-log segments to be
  truncated (STG-013, FR-14). The dump SHALL cover the full **logical** `CommittedState`,
  including rows resident only in the cold tier (SPEC-015).

- **STG-021** [P0] **Checkpoint format.** Each checkpoint SHALL be a MessagePack-encoded file
  containing the structure below. Checkpoint files are zstd-compressed per FR-19; the
  compression envelope is specified in SPEC-015 (`TIER-` requirements).

  ```rust
  #[derive(Serialize, Deserialize)]
  pub struct Snapshot {
      shard_id: u32,
      /// Last tx included in this checkpoint.
      last_tx_id: u64,
      timestamp: i64,
      tables: Vec<TableSnapshot>,
  }

  #[derive(Serialize, Deserialize)]
  pub struct TableSnapshot {
      table_id: u32,
      table_name: String,
      /// All rows in the logical CommittedState (hot + cold tiers) at checkpoint time.
      rows: Vec<Row>,
  }
  ```

- **STG-022** [P0] **Non-blocking checkpoint.** The checkpoint write SHALL NOT block reducer
  execution. The `SnapshotWorker` SHALL operate on a consistent copy of `CommittedState`
  taken under a brief read lock. Reducer processing SHALL resume before the disk write
  completes. Reading cold-tier pages to include them in the checkpoint (STG-020) SHALL happen
  outside that lock and SHALL NOT block the writer.

- **STG-023** [P1] **Checkpoint retention.** The system SHALL retain at least 2 checkpoints at
  all times to allow rollback to a known-good state. Checkpoints older than the configured
  retention window (`snapshot_retention`, default: 3 checkpoints) MAY be deleted. Checkpoints
  also serve as the full-sync seed for new replicas (SPEC-014, FR-100); a checkpoint being
  transferred to a replica SHALL NOT be deleted until the transfer completes.

## 5. Crash recovery

- **STG-030** [P0] **Recovery procedure.** On startup, the system SHALL execute the following
  recovery sequence for each shard:

  ```
  1. Scan snapshot_dir for the latest valid checkpoint (Snapshot) with shard_id == this shard
  2. If checkpoint found:
     a. Load all rows from the checkpoint into CommittedState
     b. Set recovered_tx_id = snapshot.last_tx_id
  3. If no checkpoint found:
     a. Start with empty CommittedState
     b. Set recovered_tx_id = 0
  4. Scan commit_log_dir for log segments with first_tx_id > recovered_tx_id
  5. Replay each TxRecord in order, applying mutations to CommittedState
  6. After replay, CommittedState reflects the last committed transaction
  7. Mark shard as ready to accept new reducer calls
  ```

  Recovery restores the **logical** `CommittedState`. Physical placement after recovery
  (buffer pool vs cold tier) is governed by SPEC-015 and SHALL NOT affect recovered contents:
  the recovered state SHALL be identical regardless of which rows were hot or cold at crash
  time, and loading a large checkpoint MAY spill directly to the cold tier to stay within the
  memory budget.

- **STG-031** [P1] **Corruption detection.** The system SHALL detect log entry corruption
  using the CRC32 checksum carried by each entry (STG-011). A corrupted entry SHALL cause
  recovery to stop at that point: the log is truncated at the first corrupt entry (the entry
  and everything after it are discarded as a torn tail; FR-13), all entries before it are
  kept, and the last successfully recovered `tx_id` SHALL be reported. The operator SHALL be
  notified via structured log output (`tracing`, SPEC-012).

- **STG-032** [P1] **Recovery time target.** Recovery of a shard with a 10 GB commit log and a
  recent checkpoint SHALL complete in under 30 seconds (NFR-06).

## 6. Auto-increment counters

- **STG-040** [P0] **Per-table auto-inc counter.** Each table with an `#[auto_inc]` column
  (SPEC-001) SHALL maintain a monotonically increasing `u64` counter in `CommittedState`.
  The counter SHALL persist across restarts via the commit log (the last inserted ID is
  derivable from log replay). On insert, the runtime SHALL:

  1. Read the current counter value
  2. Replace the `0` placeholder in the incoming row with the counter value
  3. Increment the counter
  4. Insert the row with the assigned ID into `TxState`

  The assigned ID SHALL be returned to the caller as part of the inserted row in the
  `ReducerResult` (or via a return value from the reducer). Rolled-back transactions MAY
  leave gaps in the ID sequence.

  ```rust
  #[fluxum::reducer(max_rate = "5/s")]
  fn send_chat(ctx: &ReducerContext, channel: u32, text: String) -> Result<(), String> {
      ctx.tx.insert::<ChatMessage>(ChatMessage {
          id: 0, // auto_inc — replaced with the assigned ID at insert time
          sender: ctx.identity,
          channel,
          content: text,
          sent_at: ctx.timestamp,
      })?;
      Ok(())
  }
  ```

## 7. TableId and IndexId assignment

- **STG-050** [P0] **Stable table IDs.** Each table SHALL be assigned a stable `u32` `TableId`
  at startup, derived from a CRC32 hash of the table name. The same table name SHALL always
  produce the same `TableId`, enabling commit log entries to be replayed without a live
  schema lookup.

- **STG-051** [P1] **Stable index IDs.** Each index (B-tree or spatial) SHALL be assigned a
  stable `u32` `IndexId` derived from a hash of `(table_name, index_name_or_columns)`.

## 8. Configuration

Storage keys in `config.yml` (overridable via `FLUXUM_`-prefixed environment variables);
defaults are normative per the requirements above:

```yaml
storage:
  commit_log_dir: ./data/commitlog     # STG-014
  snapshot_dir: ./data/snapshots       # checkpoints — STG-020
  segment_max_bytes: 134217728         # 128 MB — STG-013
  snapshot_interval_tx: 10000          # checkpoint interval — STG-020
  snapshot_retention: 3                # checkpoint retention — STG-023
```

Cold-tier keys (`memory.budget`, page size, compression codec) are specified in SPEC-015.

## Acceptance criteria

1. **Crash suite** (T2.7, SPEC-013): kill -9 harness terminating the process at every commit
   boundary (before append, mid-append, after append/before ack, after ack) — after recovery,
   zero acknowledged transactions are lost beyond the bounded async-write window (STG-012),
   and `CommittedState` never contains a partial transaction (STG-005/STG-006).
2. **Replay tests** (T2.2): write/replay round-trips over arbitrary insert/delete
   interleavings on the canonical demo schema (`User`, `ChatMessage`, `Task`, `Sensor`) —
   replayed state is identical to pre-crash committed state; `tx_id` sequence is strictly
   increasing across restart (STG-015); auto-inc counters resume without reuse (STG-040).
3. **Corruption-truncation drills** (T2.7): CRC bit-flip and truncation injected at every
   byte offset of the final log entry — recovery stops at the first corrupt entry, keeps all
   prior entries, reports the last recovered `tx_id`, and emits the operator notification
   (STG-031).
4. **Checkpoint equivalence** (T2.3): checkpoint + replay recovery produces state identical
   to full-log replay; checkpoint writes do not block reducer execution (STG-022 verified
   under sustained write load); segments covered by a completed checkpoint are compacted and
   recovery still succeeds afterwards (STG-013, FR-14).
5. **Recovery benchmark** (T2.7, gate G2): timed restart with a 10 GB commit log and a recent
   checkpoint completes in < 30 s (STG-032, NFR-06).
6. **Tiered recovery equivalence** (T2.7, with SPEC-015): with a dataset larger than the
   memory budget (cold tier populated), crash + recovery produces logical state identical to
   the all-hot case — same rows, indexes, `tx_id`, and auto-inc counters regardless of
   pre-crash residency (STG-030); a crash during cold-tier eviction never loses acknowledged
   transactions (cold pages are redundant with checkpoint + log).
