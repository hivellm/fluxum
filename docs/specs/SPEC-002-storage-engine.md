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

- **STG-007** [P0] **Rollback undoes eagerly applied effects atomically.** "Discard `TxState`"
  (STG-006) is sufficient only for effects buffered in `TxState`. For any effect applied
  eagerly to `CommittedState` structures during the transaction, rollback SHALL restore the
  prior state atomically with the rollback, before the shard accepts the next transaction:

  1. **Undelete:** rows deleted by the transaction SHALL be restored (delete marks reverted;
     a delete-then-reinsert of the same committed row within one transaction cancels to a
     no-op — the row's committed identity is preserved, not deleted and recreated).
  2. **Index revert:** every index entry inserted or removed by the transaction — including
     partially completed multi-index maintenance after a mid-flight constraint violation —
     SHALL be reverted, restoring all secondary and spatial indexes to their pre-transaction
     contents.
  3. **Undo log for eager structural changes:** any structural change applied eagerly (e.g.
     index creation/removal, future transactional DDL per SPEC-010) SHALL push an undo record
     when applied; rollback SHALL replay undo records in reverse order.

  Unique/PK constraint checks during a transaction SHALL evaluate against the overlay of
  `CommittedState` and `TxState`: a committed row marked deleted by this transaction does not
  count as a conflict, and uncommitted inserts in `TxState` do. *(adopted from SpacetimeDB
  analysis, file 02)*

## 3. CommitLog

- **STG-010** [P0] **Append-only log.** The system SHALL maintain an append-only commit log on
  disk per shard. Every committed transaction SHALL be appended to the log BEFORE the
  `ReducerResult` is sent to the caller. The log provides durability: after a crash, the
  system SHALL be able to reconstruct all committed state by replaying the log from the last
  checkpoint (§4).

- **STG-011** [P0] **Log entry format.** Each log entry SHALL use the FluxRPC body encoding
  (SPEC-006, MessagePack via `rmp-serde`) inside a fixed, checksummed frame: a `u32`
  little-endian length prefix, a `u64` little-endian **epoch** number (the fencing term of the
  shard's current leader lineage, SPEC-014 — recorded durably in every entry so that
  divergence detection, PITR lineage, and checkpoint invalidation after failover need no
  separate epoch→offset map), the MessagePack body, and a trailing **CRC32C** checksum
  (hardware-accelerated; e.g. the `crc32c` crate using SSE4.2/ARMv8 CRC instructions). The
  CRC32C SHALL cover the length prefix, the epoch, AND the body bytes — a corrupted length or
  epoch field is detected as a checksum failure rather than mis-framing the rest of the
  segment. Segment file headers SHALL likewise record the log format version, checksum
  algorithm, and the epoch at segment creation. An append with an epoch lower than the last
  durably written epoch SHALL be rejected. *(adopted from SpacetimeDB analysis, file 03)*

  ```
  ┌──────────────────┬────────────────┬───────────────────────────────┬──────────────────┐
  │ length: u32 (LE) │ epoch: u64 (LE)│ TxRecord: MessagePack bytes   │ crc32c: u32 (LE) │
  └──────────────────┴────────────────┴───────────────────────────────┴──────────────────┘
                       └── CRC32C covers length + epoch + body ──┘
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

  Using the same MessagePack body encoding as the wire protocol means a single codec covers
  wire encoding, commit log, checkpoint, and replication-stream formats (STG-016). This entry
  format freezes with the wire format at gate G5.

- **STG-012** [P0] **Group commit (async durability with a dedicated flush actor).** The
  CommitLog writer SHALL be a dedicated background **fsync/flush actor** fed by a bounded
  queue of committed-transaction entries. The actor SHALL drain all queued entries in one
  batch, append them to the log, and perform **one `fsync` per batch** — amortizing fsync
  cost to near zero per transaction under load while degrading to fsync-per-transaction when
  idle. The runtime SHALL NOT call `fsync` inline on the commit path. After each successful
  fsync, the actor SHALL publish the new **durable offset** (highest fsynced `tx_id`) via a
  watch/observable channel: replication quorum acks (SPEC-014, REP-021) and optional
  confirmed reads gate on it. The `ReducerResult` ack remains decoupled from durability
  (sent at in-memory commit); the durability gap is bounded and deterministic — queue depth ×
  fsync latency, within the < 50 ms NFR-08 bound — rather than dependent on OS write-behind
  timing. An fsync failure SHALL be treated as fatal for the writer (no retry; the log state
  on disk is undefined after a failed fsync). *(adopted from SpacetimeDB analysis, file 03)*

  **Rationale:** synchronous fsync per transaction would limit throughput to disk IOPS
  (~1,000/s on HDD, ~10,000/s on SSD). Group commit bounds p99 commit latency without
  per-transaction fsync and enables 100,000+ tx/s.

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
  point-in-time image of `CommittedState` — a **checkpoint** (incremental/content-addressed
  per STG-021) — to the `snapshot_dir`. The
  default interval is every 10,000 committed transactions (`snapshot_interval_tx: 10000` in
  `config.yml`; FR-14). A snapshot file IS the checkpoint mechanism referenced by FR-13/FR-14;
  the `Snapshot*` type names and `snapshot_*` config keys are retained for continuity.
  Checkpoints serve two purposes: (a) crash-recovery acceleration — instead of replaying the
  entire commit log from the beginning, the system loads the latest checkpoint and replays
  only log entries after its `last_tx_id`; and (b) **log compaction** — a completed checkpoint
  covers all transactions ≤ its `last_tx_id`, allowing older commit-log segments to be
  truncated (STG-013, FR-14). The dump SHALL cover the full **logical** `CommittedState`,
  including rows resident only in the cold tier (SPEC-015).

- **STG-021** [P0] **Checkpoint format — incremental and content-addressed.** Checkpoints
  MUST be **incremental and content-addressed**: a checkpoint SHALL be a manifest plus a set
  of objects (pages, per SPEC-015 TIER-060/TIER-063; large values, per STG-041) stored in an
  object repository keyed by content hash (BLAKE3-class), so that objects unchanged since the
  previous checkpoint are **shared** (referenced/hardlinked, not rewritten) between
  checkpoints. A checkpoint SHALL cost only the changed objects — never a full dump whose
  write cost scales with database size. The manifest SHALL record `shard_id`, `last_tx_id`,
  the epoch (STG-011), the timestamp, and the content hashes of every referenced object, and
  SHALL itself carry an **integrity hash** over its serialized bytes; restore SHALL verify
  the manifest hash and every object hash before adopting the checkpoint, falling back to an
  older retained checkpoint (STG-023) on permanent mismatch. Checkpoint creation SHALL be
  two-phase crash-safe: objects written first, then the fsynced manifest written last as the
  commit record — a checkpoint whose manifest is absent or fails verification does not exist.
  *(adopted from SpacetimeDB analysis, files 02/03)*

  The MessagePack-encoded structure below is retained as the portable **export** format
  (backups, replica seeding; SPEC-015 §8); it is not the recovery fast path. Export files are
  zstd-compressed per FR-19; the compression envelope is specified in SPEC-015 (`TIER-`
  requirements).

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

- **STG-031** [P1] **Corruption detection and non-destructive torn-tail repair.** The system
  SHALL detect log entry corruption using the CRC32C checksum carried by each entry (STG-011).
  A corrupted entry SHALL cause replay to stop at that point: all entries before it are kept
  and applied, and the last successfully recovered `tx_id` SHALL be reported. Repair SHALL be
  **non-destructive**: the torn tail (the corrupt entry and everything after it) SHALL NOT be
  truncated in place; it SHALL be preserved by quarantining the affected bytes into a sidecar
  file (`<segment>.torn`, alongside the segment) before the writer resumes appending at the
  last valid entry boundary. Preserving the tail keeps the evidence needed to distinguish a
  torn local write from a diverged suffix under an old epoch once replication (SPEC-014)
  lands. Destructive physical truncation SHALL exist only as an explicit `reset_to(tx_id)`
  operation invoked by the replication layer for confirmed divergence (REP-013), never as an
  implicit side effect of opening the log. The operator SHALL be notified of any quarantine
  via structured log output (`tracing`, SPEC-012), including the quarantined byte range and
  sidecar path. *(adopted from SpacetimeDB analysis, file 03)*

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
  `ReducerResult` (or via a return value from the reducer).

  **Batched allocation.** Counter values SHALL be handed out from a pre-allocated batch: the
  counter's durable high-water mark advances by an allocation step (`auto_inc_allocation_step`,
  default 4096) at a time, persisted through the commit log as an ordinary logged write, so a
  durable write is needed only once per batch — never per insert. On recovery, generation
  resumes from the persisted high-water mark. Consequently **gaps in the ID sequence are
  normal and documented**: rolled-back transactions leave gaps (handed-out values are not
  returned), and a crash MAY skip up to one unconsumed allocation batch. IDs SHALL remain
  unique and monotonically increasing; they SHALL NOT be assumed dense. *(adopted from
  SpacetimeDB analysis, file 02)*

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

- **STG-041** [P1] **Refcounted blob store for large values.** Values whose encoded size
  exceeds a configurable threshold (`blob_threshold_bytes`, default 4096 — at most a page
  payload, SPEC-015 TIER-026) SHALL be stored out-of-row in a per-shard **content-addressed,
  reference-counted blob store**: the row holds the value's content hash (BLAKE3-class); the
  blob store maps hash → bytes with a refcount incremented on insert and decremented on
  delete. Identical values share one stored copy, and row equality/comparison over large
  values compares hashes, never blob bytes. In-flight transactions SHALL use a **tx-local
  blob overlay** merged into the shard store on commit, so rollback drops the overlay without
  walking rows to adjust refcounts (STG-006/STG-007). Physical reclamation (GC) of
  zero-refcount blobs SHALL be **tied to checkpoint retention** (STG-023): a blob's bytes MAY
  be deleted only when no retained checkpoint and no retained commit-log segment references
  its hash. Blobs are checkpoint objects under STG-021's content-addressed scheme (their hash
  IS their object key). *(adopted from SpacetimeDB analysis, file 02)*

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
  auto_inc_allocation_step: 4096       # STG-040 batched allocation
  blob_threshold_bytes: 4096           # STG-041 out-of-row threshold
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
3. **Corruption / torn-tail drills** (T2.7): CRC32C bit-flip and truncation injected at every
   byte offset of the final log entry — replay stops at the first corrupt entry, keeps all
   prior entries, reports the last recovered `tx_id`, and emits the operator notification;
   the torn tail is quarantined to the sidecar file (byte-identical to the pre-repair tail),
   never destructively truncated, and subsequent appends resume at the last valid boundary
   and recover cleanly (STG-031). *(adopted from SpacetimeDB analysis, file 03)*
4. **Checkpoint equivalence and incrementality** (T2.3): checkpoint + replay recovery
   produces state identical to full-log replay; checkpoint writes do not block reducer
   execution (STG-022 verified under sustained write load); segments covered by a completed
   checkpoint are compacted and recovery still succeeds afterwards (STG-013, FR-14). A
   checkpoint taken after modifying a small fraction of a large dataset writes only the
   changed objects (unchanged objects are shared with the previous checkpoint); a corrupted
   manifest or object hash is detected on restore and recovery falls back to an older
   retained checkpoint (STG-021). *(adopted from SpacetimeDB analysis, files 02/03)*
5. **Recovery benchmark** (T2.7, gate G2): timed restart with a 10 GB commit log and a recent
   checkpoint completes in < 30 s (STG-032, NFR-06).
6. **Tiered recovery equivalence** (T2.7, with SPEC-015): with a dataset larger than the
   memory budget (cold tier populated), crash + recovery produces logical state identical to
   the all-hot case — same rows, indexes, `tx_id`, and auto-inc counters regardless of
   pre-crash residency (STG-030); a crash during cold-tier eviction never loses acknowledged
   transactions (cold pages are redundant with checkpoint + log).
7. **Rollback correctness** (T2.2): property tests over interleaved insert/delete/rollback
   sequences — after any rollback, deleted rows are restored, delete-then-reinsert cancels to
   a no-op, and every secondary/spatial index is bit-identical to a freshly rebuilt index
   over `CommittedState` (STG-007); unique-constraint checks correctly ignore committed rows
   tx-deleted in the same transaction. *(adopted from SpacetimeDB analysis, file 02)*
8. **Group-commit durability window** (T2.7): under sustained load, fsync count is far below
   transaction count while the published durable offset advances monotonically; a confirmed
   read gated on the durable offset never observes a transaction lost by a subsequent kill -9
   (STG-012). Blob-store drills: identical large values are stored once; blob bytes are never
   reclaimed while any retained checkpoint references their hash (STG-041). *(adopted from
   SpacetimeDB analysis, files 02/03)*
