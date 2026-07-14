# 05 — Transactions

## ACID guarantees

SpacetimeDB provides full ACID guarantees:

| Property | Implementation |
|----------|----------------|
| **Atomicity** | Reducer transactions are all-or-nothing; panic/error = full rollback |
| **Consistency** | Schema constraints enforced at insert time; primary key uniqueness guaranteed |
| **Isolation** | MVCC via `CommittedState` + `TxState` separation; readers see stable snapshot |
| **Durability** | Commit log written to disk before broadcast; crash recovery via log replay |

---

## Transaction lifecycle

```
┌─────────────────────────────────────────────────────┐
│  Reducer invoked via __call_reducer__(id, args)     │
│                                                     │
│  1. InstanceEnv creates new TxState                 │
│  2. TxSlot (thread-local) set to active MutTxId     │
│  3. User code runs:                                 │
│     - reads hit CommittedState (stable snapshot)    │
│     - writes accumulate in TxState (inserts/deletes)│
│  4a. Success → commit:                              │
│      - TxState merged into CommittedState           │
│      - commit_and_broadcast_event() called          │
│      - DurabilityWorker appends to commit log       │
│      - SubscriptionManager evaluates delta rows     │
│  4b. Error/panic → rollback:                        │
│      - TxState discarded                            │
│      - CommittedState unchanged                     │
│      - No log entry written                         │
└─────────────────────────────────────────────────────┘
```

---

## MVCC (Multi-Version Concurrency Control)

SpacetimeDB uses a simplified MVCC model:

### CommittedState
- Represents the last committed snapshot of the database
- Immutable during any in-flight transaction
- Reads always go to `CommittedState` — readers never block writers

### TxState
- Accumulates in-flight mutations (inserts and deletes) for the current transaction
- Not visible to other transactions
- On commit: merged atomically into `CommittedState`
- On rollback: discarded entirely

### Isolation level
- Equivalent to **Snapshot Isolation** — each transaction sees a consistent point-in-time view
- Write-write conflicts: SpacetimeDB uses a `Locking` datastore — concurrent writes to the same rows are serialized by an internal lock, not optimistically detected
- This means: **only one reducer can write at a time** per database instance

### Concurrency model
The current design is effectively **single-writer** per database:
- One active write transaction at a time
- Multiple concurrent read transactions possible (reads are lock-free on `CommittedState`)
- This is acceptable because the in-memory design makes each transaction extremely fast (microseconds), so the throughput is still very high despite serialized writes

---

## Commit log

### Structure (`crates/commitlog`)
```
commit_log/
├── segment_0000.stdb.log    ← sealed, immutable
├── segment_0001.stdb.log    ← sealed, immutable
└── segment_0002.stdb.log    ← active, being appended
```

Each log entry contains:
```
[tx_id: u64][timestamp: u64][mutations: BSATN-encoded]
  mutations = Vec<TableMutation>
  TableMutation = { table_id, inserts: Vec<Row>, deletes: Vec<PrimaryKey> }
```

### Write path
1. Transaction commits in memory
2. `DurabilityWorker` receives the committed event
3. Worker serializes mutations to BSATN
4. Appends to the active log segment (buffered write, not fsync per tx by default)
5. Log segment is sealed when it reaches a size threshold; a new segment is opened

### Read path (recovery)
1. On startup, load the latest snapshot (if any) into `CommittedState`
2. Identify the log segment corresponding to the snapshot's last tx_id
3. Replay all log entries from that point forward
4. After replay, the database is in the same state as before the crash

---

## Snapshots

### Purpose
- Avoid replaying the entire commit log on every startup
- Reduce recovery time proportional to snapshot frequency

### Structure (`crates/snapshot`)
```
snapshots/
├── snapshot_0001/
│   ├── manifest.json     ← { last_tx_id, timestamp, tables: [...] }
│   └── table_0001.pages  ← raw page dumps in BSATN
│   └── table_0002.pages
└── snapshot_0002/        ← newer, replaces previous for recovery
```

### Snapshot policy
- Triggered by `SnapshotWorker` on a configurable interval (e.g., every N transactions or every T seconds)
- Snapshots are immutable once written
- Old snapshots are retained for a configurable window (for debugging/point-in-time recovery)

---

## Scheduled reducers and time

Scheduled reducers interact with the transaction system:
- A scheduled reducer is stored as a row in a special system table (`__scheduler__`)
- On each server tick, SpacetimeDB checks for due scheduled reducers and invokes them
- Each scheduled reducer call is its own transaction (same as a client-triggered reducer)
- Cancellation removes the row from `__scheduler__`

---

## Implications for UzDB

| SpacetimeDB decision | UzDB consideration |
|----------------------|--------------------|
| Single-writer per DB instance | Acceptable for a shard; UzDB needs multi-shard architecture for world scale |
| Async commit log (no fsync per tx) | Adopt — game servers tolerate small durability window (last 100ms of data loss on crash) |
| MVCC snapshot isolation | Adopt — correct semantics for game state mutations |
| Snapshot + log recovery | Adopt — standard approach, well-proven |
| Scheduled reducers via system table | Adopt — game loops need a built-in tick scheduler |
| No distributed transactions | Acceptable initially; design shard boundaries to minimize cross-shard mutations |
