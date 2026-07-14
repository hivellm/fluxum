# SpacetimeDB Deep-Dive 02 — In-Memory Table Engine & Datastore

| | |
|---|---|
| **Source** | SpacetimeDB (clockworklabs/SpacetimeDB) |
| **Version** | v2.7.0 |
| **Commit** | `1a8df2a` |
| **Crates analyzed** | `crates/table` (~21k LOC), `crates/datastore` (~15.6k LOC), supporting: `crates/sats/src/layout.rs`, `crates/memory-usage`, `crates/snapshot` |
| **Fluxum specs compared** | SPEC-002 (storage engine), SPEC-015 (tiered storage) |

---

## 1. Big picture

SpacetimeDB's storage stack is **two crates**: `crates/table` implements a pure in-memory,
page-based row store (`Table`, `Page`, `Pages`, indexes, blob store), and `crates/datastore`
wraps it in a transactional shell (`CommittedState` + `TxState` under a single big lock,
constraint checking, sequences, system tables). There is **no buffer pool and no cold tier**:
the entire committed state lives in RAM; durability comes from the commitlog + snapshot crates.
The design center is *not* disk I/O but (a) making the RAM representation compact and
cache-friendly, (b) making rows cheap to serialize to the wire (BSATN), and (c) making commit
(merge of tx state into committed state) and rollback cheap.

Key architectural facts up front, because they contradict SPEC-002's mental model:

1. **There is no `BTreeMap<PrimaryKey, Row>`.** A `Table` is a heap of 64 KiB pages plus
   indexes. Row identity is a *physical* 8-byte `RowPointer` (page index + offset). Tables are
   **sets** (duplicate-row prevention), not keyed maps; a primary key is just another unique
   index.
2. **MVCC is a single-writer overlay, not multi-version.** One writer (holding a `RwLock`
   write guard on the whole `CommittedState`) mutates a per-tx scratchpad; readers hold read
   guards. The writer *sees its own writes* (unlike Fluxum STG-004's opposite choice).
3. **Rows never move while committed.** Everything — indexes, delete-tracking, subscription
   machinery — leans on `RowPointer` stability. This single invariant generates most of the
   subtle machinery described below.

---

## 2. `crates/table` — the page-based row store

### 2.1 Page layout (`crates/table/src/page.rs`, `indexes.rs`)

- `PAGE_SIZE = 64 KiB` (`u16::MAX + 1`), `PAGE_HEADER_SIZE = 64` bytes, so
  `PAGE_DATA_SIZE = 65_472` bytes (`indexes.rs`). `Page` is `#[repr(C, align(64))]`,
  `static_assert_size!(Page, PAGE_SIZE)` — header and data share one boxed allocation,
  allocated with `alloc_zeroed` so no byte is ever uninit (required so pages can be
  serialized for snapshots; see the `Byte = u8` comment in `indexes.rs`, which notes they
  *migrated away from* `MaybeUninit<u8>` precisely to make pages dumpable).
- Intra-page layout (documented on `struct Page`):
  - **Fixed-length row parts grow left→right** from offset 0 up to a fixed high-water mark
    (`FixedHeader.last`).
  - **Var-length granules grow right→left** from the end down to a var HWM
    (`VarHeader.first`). The page is full when the HWMs meet.
  - Both sections have intrusive **freelists** (`FreeCellRef`, a 2-byte next-offset stored
    inside the freed cell itself; `PageOffset::PAGE_END` is the nil sentinel). Freed slots are
    reused before consuming the gap. There is an acknowledged TODO: the HWM is never lowered
    when the topmost row is freed (`TODO(perf,future-work)` in `FixedHeader`).
- `PageHeader` (64 bytes, `#[repr(C, align(64))]`) = `FixedHeader` (16 B: freelist head, HWM,
  `num_rows: u16`, `present_rows: FixedBitSet`) + `VarHeader` (8 B: freelist head + memoized
  `freelist_len`, HWM, `num_granules`) + `unmodified_hash: Option<blake3::Hash>`.
  - `present_rows` is a bitset with **one bit per fixed-row slot** (`offset / fixed_row_size`)
    — this is how "is this row live?" is answered and how full-table scans skip holes
    (`FixedLenRowsIter` walks set bits). Note it is a heap allocation hanging off the header
    (`fixed_bit_set.rs`), so a `Page` is not literally a flat 64 KiB blob — snapshot
    serialization handles it as a BSATN product.
  - `unmodified_hash` memoizes the page's **BLAKE3 content hash**; every mutating operation
    (`alloc_fixed_len`, `delete_row`, `get_fixed_row_data_mut`, `clear`, …) resets it to
    `None`. `Page::save_or_get_content_hash()` recomputes lazily. This powers **incremental
    snapshots**: unmodified pages are recognized by hash and deduplicated in the
    content-addressed snapshot object store (`crates/snapshot`, `ObjectType::Page(blake3::Hash)`).

### 2.2 `RowPointer` — 64-bit packed physical row identity (`indexes.rs`)

```
bit 0        : reserved bit (borrowed by data structures, see below)
bits 1..40   : PageIndex   (39 bits → max ~5.5 × 10^11 pages)
bits 40..56  : PageOffset  (16 bits, intra-page byte offset)
bits 56..64  : SquashedOffset (u8): TX_STATE(0) | COMMITTED_STATE(1)
```

`SquashedOffset` tags *which state* a pointer refers to — the tx scratchpad or committed
state — and the comment says it is designed to be extended to savepoints / committed-unsquashed
transactions later. The **reserved bit** is reused twice: `PointerMap` packs
"is this a pointer or a collider-slot index" into it, and `UniqueDirectIndex` uses it as its
presence sentinel (`NONE_PTR`). Cheap tricks, but they make `RowPointer` load-bearing across
four modules.

### 2.3 BFLATN vs BSATN, alignment (`sats/layout.rs`, `bflatn_to.rs`, `bflatn_from.rs`, `static_layout.rs`)

- **BFLATN** is the in-page format: C-struct-like layout computed by `RowTypeLayout` — fields
  get natural alignment (`align_to`), products get max-child alignment, sums store a tag plus
  max-sized variant payload. `MIN_ROW_SIZE = 2` and rows must be aligned enough to store a
  `FreeCellRef` (compile-time asserts in `page.rs`), because *freed row slots become freelist
  nodes in place*.
- **BSATN** is the compact, padding-free wire/durability format. Conversion is normally a
  layout-tree walk (`serialize_row_from_page` / `write_row_to_pages`), but there is a fast
  path: `StaticLayout` (`static_layout.rs`) precompiles the conversion into a short list of
  `MemcpyField`s ("copy bytes a..b, skip padding") whenever the row type has a fixed BSATN
  length. Insert-from-BSATN then becomes: validate with a precompiled `StaticBsatnValidator`,
  `alloc_fixed_len`, one-or-few `memcpy`s (`Table::insert_physically_bsatn`). This is their
  single biggest hot-path trick for plain rows.
- Var-len fields (strings, arrays) are stored in the fixed part as a 4-byte
  `VarLenRef { length_in_bytes: u16, first_granule: PageOffset }` pointing at an intra-page
  linked list of **64-byte granules** (`var_len.rs`): 2-byte packed header (6 bits length,
  10 bits next-granule offset — the low 6 bits of the offset are always 0 due to 64-byte
  alignment) + 62 data bytes. Objects > 16 granules (**992 bytes**) overflow to the **blob
  store**, with the single granule then holding a 32-byte BLAKE3 `BlobHash`
  (`VarLenGranule::OBJECT_SIZE_BLOB_THRESHOLD`). `length_in_bytes == u16::MAX` is the
  large-blob sentinel.
- Because fixed parts contain intra-page offsets, **copying/moving a row between pages
  requires pointer fixups**. This is driven by a `VarLenMembers` visitor; per row type the
  engine compiles a `VarLenVisitorProgram` (`row_type_visitor.rs`) — a tiny bytecode
  (`VisitOffset` / `SwitchOnTag` / `Goto`) interpreted to find every `VarLenRef` in a row,
  including inside sum variants. Empty program for fixed-only rows.

### 2.4 Blob store (`blob_store.rs`)

`trait BlobStore` — content-addressed by BLAKE3, **refcounted** (`clone_blob` / `free_blob`),
with `iter_blobs` for snapshotting and two accounting views (`bytes_used_by_blobs` counts
refs × size so per-table stats stay additive; `physical_bytes_used_by_blobs` counts unique
bytes for memory tracking). The production implementation is literally `HashMapBlobStore`
(hash map + refcount) — GC is *just* refcounting, freed at delete/merge time; there is no
background GC. Content addressing also buys them cheap row equality: comparing two rows never
needs blob bytes, comparing the 32-byte hashes suffices (noted in
`Table::find_same_row_via_pointer_map` docs).

### 2.5 Row hashing & set-semantics dedup (`row_hash.rs`, `pointer_map.rs`)

- `RowHash(u64)` is computed by walking the BFLATN row with a typed visitor
  (`hash_row_in_page`) using `ahash` with a fixed seed (`RandomState::with_seed(0x42)`).
  Explicitly documented as **process-lifetime only** — never persisted, may differ across
  restarts/machines (hardware-dependent ahash).
- `PointerMap` is an `IntMap<RowHash, PtrOrCollider>` (identity hashing — "no need to hash a
  hash") mapping row hash → row pointer(s). Collisions (rare) spill to an append-only
  `Vec<Vec<RowPointer>>` collider table indexed through the reserved bit, with a free-slot
  stack (`emptied_collider_slots`).
- **Duality invariant**: a table has a `PointerMap` **iff it has no unique index**
  (`Table.pointer_map: Option<PointerMap>`). If any unique index exists, duplicates are
  impossible anyway, so the map is dropped (`Table::add_index` returns the taken map;
  `make_index_non_unique` rebuilds it via `rebuild_pointer_map`). Set-semantics enforcement is
  therefore: unique-index probe if one exists, else hash-probe + `eq_row_in_page` byte/typed
  comparison (`find_same_row`).

### 2.6 Pages manager & page pool (`pages.rs`, `page_pool.rs`)

- `Pages` = `Vec<Box<Page>>` + `non_full_pages: BTreeSet<(available_granules, PageIndex)>`.
  Insertion looks up the first non-full page with enough var-len granule capacity
  (`range((num_granules, PageIndex(0))..)`) — a best-fit-ish search keyed by free granules.
  The set is deliberately ordered `(granules, page_index)` so that **replaying the same ops
  yields the same physical layout on every replica** ("deterministic sort order … regardless
  of when those datastores were (re)started" — comment on `non_full_pages`). Physical
  determinism is a real design requirement because snapshots hash pages.
- `PagePool` (`page_pool.rs`) is a **shared, size-capped pool of recycled 64 KiB pages**
  ("Pages are shared between all modules running on a particular host, not allocated
  per-module" — `CommittedState.page_pool` docs). `take_with_fixed_row_size` reuses an old
  page after `reset_for(max_rows)` (which reuses the `present_rows` bitset allocation when the
  block count matches — see `page_pool_bitset_reuse` test). Deserializing snapshot pages also
  goes through the pool (`take_deserialize_from`). On commit-merge, tx-state pages are
  returned via `put_many`.

### 2.7 Table, insert/delete protocol (`table.rs`)

`Table` = `TableInner { row_layout, static_layout + validator, visitor_prog, pages }` +
`pointer_map` + `indexes: BTreeMap<IndexId, TableIndex>` + `schema: Arc<TableSchema>` +
`squashed_offset` + statistics (`row_count: u64`, `blob_store_bytes`). `TableInner` exists
purely to let `RowRef` borrow row storage while `indexes` are mutably borrowed — index
insert/delete take a `RowRef` and read key columns straight out of the page
(`ReadColumn::read_column`, `read_column.rs`, which reads primitives directly from BFLATN
bytes without materializing a `ProductValue`).

The insert protocol is **optimistic physical insert, then confirm**:

1. `insert_physically_pv/bsatn` writes the row into some page (no constraint checks).
2. `confirm_insertion::<CHECK_SAME_ROW>` → `insert_into_pointer_map` (set-semantics check,
   deletes the physical row and returns `DuplicateError` on conflict) →
   `insert_into_indices` (per unique index `check_and_insert`; on violation, rolls back by
   deleting the row from all indexes inserted *so far* — `delete_from_indices_until(index_id)`
   walks `indexes.range_mut(..index_id)` — then deletes the physical row).
3. `confirm_update` does delete-old-from-indexes → insert-new-into-indexes → *re-insert old on
   failure* → physically delete old row. Careful ordering so a unique violation leaves the old
   row fully intact.

Deletion frees index entries first (needs the still-live `RowRef` to project keys), then the
pointer-map entry, then the physical row (fixed slot → freelist; each granule → var freelist;
large blob → `free_blob` refcount decrement). `delete_equal_row` / `delete_by_row_value` use a
notable idiom: **temporarily insert the probe row physically**, use `find_same_row`, then
delete the temp row while skipping the pointer map — the temp row is a scratch value that must
not disturb logical state.

### 2.8 Indexes (`table_index/`)

Not one structure but a **monomorphized matrix** (`TypedIndex` enum in `table_index/mod.rs`,
~3.3k LOC of macro-generated arms):

- Axes: {non-unique, unique} × {BTree, Hash, Direct} × key type
  {bool, u8/i8 … u256/i256, F32/F64, `SumTag`, `Box<str>`, `AlgebraicValue` (fallback for
  multi-column/product keys), and fixed-size `BytesKey<N>`/`RangeCompatBytesKey<N>` for N ∈
  {8,16,32,64,128} that pack short multi-column keys into inline byte arrays}.
- The module doc states the rationale: hoisting the type enum out of the keys makes `u64::cmp`
  instead of `AlgebraicValue::cmp`, avoiding wasted memory and branches. This is the second
  big perf trick and it costs them enormous code-size ugliness (they acknowledge it).
- `BTreeIndex<K>` (`btree_index.rs`) = `std::collections::BTreeMap<K, SameKeyEntry>` where
  `SameKeyEntry` is a `SmallVec`-style set with one pointer inline (common 1-row case).
  `UniqueBTreeIndex<K>` maps `K → RowPointer` directly. Hash variants exist for point-only
  workloads.
- `UniqueDirectIndex` (`unique_direct_index.rs`): for dense unsigned integer keys — a two-level
  `Vec<Option<Box<[RowPointer; 512]>>>` (inner block = 4 KiB), i.e. **key = array subscript**.
  Presence encoded in the row pointer's reserved bit. `UniqueDirectFixedCapIndex` is the
  fixed-capacity u8/SumTag variant. Used heavily for system tables keyed by small ids.
- Every index maintains `num_rows` and `num_key_bytes` (`key_size.rs::KeySize`) for metrics
  and query planning; `Table::bytes_used_by_index_keys` aggregates them.
- Uniqueness is **convertible in place**: `make_unique()` (fails with the offending pointer if
  duplicates exist) / `make_non_unique()` — needed because constraints can be added/dropped
  transactionally (see §3.5).

### 2.9 Memory accounting (`crates/memory-usage`, throughout)

`trait MemoryUsage { fn heap_usage(&self) -> usize }` is implemented *manually on every single
type in the stack* (page headers, freelists, indexes, pointer map, tx state, committed state),
always by exhaustive struct destructuring so a new field breaks the build rather than being
silently unaccounted. On top of that, cheap-to-read counters are maintained incrementally:
`Table::row_count`, `Table::blob_store_bytes`, per-page `num_granules` (memoized so
`bytes_used_by_rows` doesn't walk rows), `CommittedState::datastore_page_bytes` (bumped on
merge by diffing `Table::page_bytes()` before/after; rebuilt from scratch only on
bootstrap/restore). Several `reconstruct_*` functions (e.g. `Table::reconstruct_num_rows`,
`Page::reconstruct_num_var_len_granules`) exist to recompute the memoized statistics from raw
pages after snapshot restore, and as debug-assert oracles.

---

## 3. `crates/datastore` — transactional shell

### 3.1 Locking model (`locking_tx_datastore/datastore.rs`)

```rust
pub struct Locking {
    pub committed_state: Arc<RwLock<CommittedState>>,   // parking_lot
    pub(super) sequence_state: Arc<Mutex<SequencesState>>,
    ...
}
```

- `begin_mut_tx` takes the **write lock on the entire committed state** plus the sequence
  mutex → exactly **one mutable transaction at a time**, blocking behind all readers.
  `begin_tx` takes a read lock → many concurrent read-only transactions. Isolation level is
  ignored: "this implementation guarantees the highest isolation level, Serializable" —
  trivially, by mutual exclusion.
- Locks are `ArcRwLockWriteGuard`s held *inside* `MutTxId`/`TxId`, so a transaction is a
  first-class value that owns its lock. Commit can **downgrade** the write guard to a read
  guard atomically (`commit_downgrade_and_then` → `SharedWriteGuard::downgrade`), which is how
  subscription evaluation reads the post-commit state without letting another writer in
  between.
- This is exactly Fluxum's single-writer-per-shard model (STG-003), except SpacetimeDB has one
  "shard" per database and readers get real concurrency via the read lock.

### 3.2 State organization (`committed_state.rs`, `tx_state.rs`)

- `CommittedState`: `next_tx_offset: u64`, `tables: IntMap<TableId, Table>`,
  `blob_store: HashMapBlobStore`, `index_id_map: IntMap<IndexId, TableId>`, the shared
  `page_pool`, `datastore_page_bytes`, plus view/read-set machinery (2.7-era incremental
  views) and `ephemeral_tables` (tables excluded from durability).
- `TxState` (96 bytes, static-asserted):
  - `insert_tables: BTreeMap<TableId, Table>` — a **full `Table` clone-structure**
    (same layout, same index *structure*, `SquashedOffset::TX_STATE`) holding only rows
    inserted this tx. Indexes in the tx table mirror the committed table's indexes, so
    tx-local rows are index-scannable.
  - `delete_tables: BTreeMap<TableId, DeleteTable>` — deletions of committed rows are **not**
    applied; they are *marked*. `DeleteTable` (`delete_table.rs`) is a
    `Vec<Option<FixedBitSet>>` — **one bit per committed row slot per page** (indexed
    `page_offset / fixed_row_size`), i.e. an O(1) membership structure exploiting
    `RowPointer` physical addressing. Far denser than a hash set of pointers.
  - `blob_store: HashMapBlobStore` — a **separate tx-local blob store**, explicitly so that
    rollback is "drop the whole thing" instead of walking rows to decrement refcounts (see
    struct comment).
  - `pending_schema_changes: ThinVec<PendingSchemaChange>` — see §3.5.
- Cumulative-effect invariants (struct docs): a row is in `insert_tables` only if the *net*
  effect of the tx is insertion; delete-then-reinsert of a committed row cancels out to
  nothing ("undelete"); no row is ever in both.

### 3.3 Read path — overlay semantics (`state_view.rs`, `mut_tx.rs`)

`MutTxId` implements `StateView` by merging: `iter` chains committed rows (skipping those in
the `DeleteTable`) with tx insert-table rows; `table_row_count` computes
`committed − deletes + inserts`. Index scans consult both the committed table's index (with a
delete-table filter) and the tx table's index. So the writer reads its own uncommitted writes
through the normal API — **the opposite of Fluxum STG-004**, which hides tx writes from
default reads and exposes them only via `scan_pending`/`scan_all`.

### 3.4 Write path — the delicate parts (`mut_tx.rs::insert`, `update`, `delete`)

The free function `insert::<GENERATE>` (~line 3255) is the heart:

1. Physically insert BSATN row into the tx table (`insert_physically_bsatn`).
2. If `GENERATE`: find sequence-triggered columns (`Table::sequence_triggers_for` — columns
   whose value **is numeric zero**), generate values (§3.6), and patch them into the row
   in-place (`write_gen_val_to_col` writes little-endian bytes at the column's BFLATN offset —
   no re-serialization).
3. `confirm_insertion::<CHECK_SAME_ROW=true>` — tx-local set-semantics + unique checks.
4. **Cross-state set-semantics**: `Table::find_same_row(commit_table, tx_table, …)` — if the
   identical row exists in committed state, the tx insert is *elided* and, if the row was
   marked deleted this tx, it is **undeleted** (`delete_table.remove(commit_ptr)`). A long
   comment ("NOTE for future MVCC implementors") warns this elision is *invalid* under real
   MVCC — a documented landmine for anyone (like Fluxum) building multi-version semantics.
5. **Cross-state unique constraints**: `commit_table.check_unique_constraints(row, …,
   is_deleted)` probes every committed unique index but ignores hits that this tx has marked
   deleted — the `is_deleted` closure is the delete-table membership test. Failure rolls back
   the physical insert.

`update(table_id, index_id, row)` resolves the old row via the given unique index in *either*
state, with mirror-image handling: same-row-as-committed short-circuits (delete temp, maybe
undelete), old-row-in-committed → mark deleted + `confirm_insertion::<false>` (the
`CHECK_SAME_ROW=false` justification is a small proof in comments), old-row-in-tx →
`confirm_update` in place.

`delete(table_id, ptr)` dispatches on `ptr.squashed_offset()`: TX_STATE rows are physically
removed from the insert table; COMMITTED_STATE rows are bit-marked in the `DeleteTable`.
`delete_by_row_value` uses the temp-physical-insert trick from §2.7 to find the victim.

### 3.5 Commit merge & rollback (`committed_state.rs::merge`, `rollback`)

`merge(tx_state, …) -> TxData`:

1. **Deletes first** (`merge_apply_deletes`): for each marked pointer, `Table::delete` the
   committed row (this materializes each deleted row to a `ProductValue` for `TxData` /
   subscriptions — an acknowledged TODO cost), freeing slots.
2. **Inserts second** (`merge_apply_inserts`): each tx-table row is *serialized to
   `ProductValue` and re-inserted* into the committed table through the full `Table::insert`
   path — deletes-then-inserts ordering deliberately lets inserts re-fill just-freed holes.
   A big `TODO(perf)` comment discusses moving whole pages instead of copying rows (rejected
   for now due to index fixups and fragmentation).
3. Tx pages are returned to the shared `PagePool`; the tx blob store is merged
   (`HashMapBlobStore::merge_from` adds refcounts).
4. `next_tx_offset` is consumed only if the tx actually changed durable tables.

**Rollback is almost free** for data: drop `TxState` (pages → pool, tx blob store dropped).
The only work is **reverting schema changes**, because SpacetimeDB applies DDL *immediately to
committed state* during the tx (so the rest of the engine never sees half-applied schemas).
Every DDL action pushes a `PendingSchemaChange` (`tx_state.rs`) — `IndexAdded/Removed` (the
removed `TableIndex` is kept alive inside the enum for cheap re-add!), `TableAdded/Removed`,
`TableAlterRowType` (old `ColumnSchema`s kept), `ConstraintAdded/Removed` (with the
made-(non)unique index ids and the taken/rebuilt `PointerMap`), `SequenceAdded/Removed`.
`CommittedState::rollback` replays them **in reverse**, restoring indexes, pointer maps,
sequences, and schemas (`rollback_pending_schema_change`, ~130 lines of careful inverse ops —
e.g. re-establishing the "pointer map iff no unique index" invariant on constraint rollback).
This is a chunk of correctness machinery with **no counterpart in SPEC-002**, which has no
transactional DDL at all.

### 3.6 Sequences / auto-inc (`sequence.rs`, `mut_tx.rs::get_next_sequence_value`)

- `Sequence { schema, value, allocated }`: `value` is the in-memory next value; `allocated`
  is the **persisted high-water mark**. Values are handed out from a pre-allocated batch;
  when exhausted, `get_next_sequence_value` allocates `SEQUENCE_ALLOCATION_STEP` more by
  **rewriting the `st_sequence` system-table row inside the current transaction**
  (delete old row + `insert::<GENERATE=false>` new row with bumped `allocated` — the `false`
  avoids infinite recursion since `st_sequence` itself has a sequence with id 0).
- On restart, generation resumes from `allocated` (start-1 special-casing for pre-1.4
  databases) — so **gaps of up to one batch appear after a crash**, and gaps appear on
  rollback (batch allocation is a schema-change-tracked, but handed-out values are not
  returned). Same behavior Fluxum STG-040 permits ("rolled-back transactions MAY leave gaps"),
  but SpacetimeDB adds the batch-allocation trick so a durable write is needed only every
  N values, not per insert — which matters because *sequence allocation is itself a row write*
  that flows through the commitlog.
- `SequencesState` lives outside `CommittedState` under its own `Mutex`, locked for the tx
  duration alongside the write lock.

### 3.7 System tables (`system_tables.rs`)

All metadata is dogfooded as ordinary tables with reserved ids: `st_table(1)`, `st_column(2)`,
`st_sequence(3)`, `st_index(4)`, `st_constraint(5)`, `st_module(6)`, `st_client(7)`,
`st_var(8)`, `st_scheduled(9)`, `st_row_level_security(10)`, `st_connection_credentials(11)`,
`st_view*(12–16)`, `st_event_table(17)`, `st_*_accessor(18–20)`. Typed row structs
(`StTableRow`, `StSequenceRow`, …) convert to/from `ProductValue`. Bootstrap
(`CommittedState::bootstrap_system_tables`) is flagged "extremely delicate": it hand-inserts
the meta-rows describing the system tables into the system tables themselves and then asserts
the round-trip (`assert_system_table_schemas_match`). Schema lookups during normal operation
come from the in-memory `Arc<TableSchema>` on each `Table`; the system tables are the durable
source of truth replayed at startup. DDL = writes to system tables + immediate in-memory
application + `PendingSchemaChange` for rollback.

---

## 4. Answers to the key questions

### 4.1 How close are their pages to a spillable/disk format?

Closer than "an in-RAM format" suggests, but not disk-ready:

**Working in favor:**
- Pages are fully self-relative: all intra-row references (`VarLenRef.first_granule`, granule
  `next`, freelists) are 16-bit *intra-page* offsets. A page can be relocated in memory — or
  written out and faulted back in — without rewriting contents. This is a deliberate
  consequence of the copy-fixup discipline (`VarLenMembers`).
- No uninit bytes ever (`alloc_zeroed` + the `Byte = u8` migration), `#[repr(C)]` stable-ABI
  annotations, and BSATN `Serialize/Deserialize` on `Page`/`PageHeader` — pages already
  round-trip to disk today for snapshots, verified by content hash, restored through the page
  pool (`PagePool::take_deserialize_from`).
- Per-page BLAKE3 `unmodified_hash` gives free dirty-tracking and dedup — directly analogous
  to what a checkpoint/eviction tier needs (Fluxum TIER-013/TIER-060 could reuse the idea:
  content hash instead of/in addition to a dirty bit buys incremental checkpoints).
- Deterministic physical layout across replicas (`non_full_pages` ordering) means page images
  are comparable/shippable between nodes.

**Working against:**
- **64 KiB pages**, vs Fluxum's 8 KB (TIER-022). Their size is chosen for RAM locality and
  amortizing per-page overhead, not for I/O granularity; nothing in the code depends on 64 KiB
  except `u16` offsets (16-bit offsets *cap* the page at 64 KiB — an 8 KB Fluxum page could
  use the same scheme with room to spare).
- The page holds **BFLATN with native alignment/padding** — fine as an on-disk image on the
  same architecture (it's LE-integer based), but it is a *row-slotted heap*, not a B-tree
  node. Fluxum SPEC-015 TIER-050 wants clustered B-tree leaves; SpacetimeDB's model has **no
  clustered order at all** — order comes only from side indexes.
- **Everything around the pages is RAM-resident and pointer-based**: all indexes
  (`std BTreeMap`/`Vec` nodes), the `PointerMap`, and the blob store are conventional heap
  structures with no paged representation. Evicting a data page without evicting its index
  entries is fine (indexes store `RowPointer`s, not row bytes), but the indexes themselves
  cannot spill. SpacetimeDB's own comment (`datastore_memory_bytes` TODO: "once indexes are
  managed by the page cache") shows they know this is future work *for them too*.
- `RowHash`/`PointerMap` are explicitly **not persistence-safe** (ahash, random per-process
  behavior documented in `indexes.rs`) — the map is rebuilt on restore
  (`rebuild_pointer_map`). Any spill design must treat them as derived state.

**Net**: the *page payload* format would survive being an on-disk format almost unchanged;
the *engine above it* assumes O(1) `Vec<Box<Page>>` access to any page at any moment
(`page_and_offset` is an unconditional index) — no fault-in seam exists. Adding tiering to
this engine would mean auditing every `unsafe` path that assumes the page is resident.

### 4.2 Subtle correctness machinery SPEC-002 does not mention

1. **Row pointer stability.** Indexes, `DeleteTable` bitsets, and subscription diffs all hold
   physical `RowPointer`s; correctness demands committed rows never move and freed slots not
   be confused with live ones. SpacetimeDB explicitly documents the ABA hazard
   (`TableInner::is_row_present` comment: pointer to deleted row A can "validly" hit new row B
   in the recycled slot) and mitigates it by discipline (pointers only obtained from live
   scans/seeks within the same lock hold), not by versioning. Fluxum SPEC-002's
   `BTreeMap<PrimaryKey, Row>` sidesteps this entirely — but the moment SPEC-015's paged
   representation gives out physical locations (page ids + slots) to secondary indexes,
   Fluxum inherits the identical problem, plus eviction (a faulted-out-and-back page must
   reappear at the same logical coordinates).
2. **Index maintenance on partial failure and rollback.** The
   insert-into-indexes-then-unwind-prefix protocol (`delete_from_indices_until`), the
   re-insert-old-row-on-update-failure path, and the whole `PendingSchemaChange` reverse
   replay (including keeping removed `TableIndex` values alive for O(1) restoration and the
   pointer-map/unique-index invariant repair). SPEC-002 STG-006 says "discard TxState" — true
   only if *nothing* was applied eagerly; any eager application (index builds, DDL) needs an
   undo log like this.
3. **Delete/insert cancellation ("undelete") and set semantics across states**, including the
   documented MVCC incompatibility of insert-elision. If Fluxum ever moves from
   single-writer overlay to real MVCC (SPEC-003 evolution), this comment is the checklist.
4. **Unique-constraint checks must see the overlay**: committed-index probes filtered by the
   tx delete set (`is_deleted` closure), tx-index probes on the clone-structure tx table.
   SPEC-002 never specifies where constraint checking happens relative to TxState; this is
   the answer sheet.
5. **Blob store GC**: refcounted, content-addressed, with a *separate tx-local blob store* so
   rollback never walks rows. Fluxum's specs (SPEC-002/015) have no large-value story beyond
   overflow pages (TIER-026); refcounting interacts with replication/snapshots
   (`insert_with_uses` restores counts) and with statistics (double-count vs physical count).
6. **Deterministic physical layout for replica-identical snapshots** — if Fluxum's SPEC-014
   replication ever compares state hashes or ships page-level checkpoints, insertion-point
   selection must be deterministic, as `Pages::non_full_pages` is.
7. **Sequence allocation as in-tx system-table writes** with batch high-water marks — the
   mechanism that makes STG-040's "counter persists via log replay" actually cheap.
8. **Memory-safety proof discipline**: the entire crate is `unsafe`-heavy with rigorous
   SAFETY comments and `#![forbid(unsafe_op_in_unsafe_fn)]`; validity of BSATN input is
   enforced by a precompiled `StaticBsatnValidator` *before* bytes touch a page. Any Fluxum
   implementation that stores rows as raw bytes needs an equivalent validation seam
   (FluxBIN decode-validate before write).

### 4.3 Perf tricks worth stealing

- **StaticLayout memcpy programs** for fixed-BSATN row types (skip the type-tree walk for the
  90% case) + thread-local scratch buffers for serialization (`insert_via_serialize_bsatn`).
- **Monomorphized index key types** (u64 keys compared as u64, short multi-column keys packed
  into `BytesKey<N>` inline arrays) and **direct (array-subscript) unique indexes** for dense
  integer keys.
- **Page pooling** with allocation reuse (incl. the bitset), shared across databases/shards.
- **Per-page bitsets for presence and for tx delete-marks** (`FixedBitSet`,
  `DeleteTable`) — O(1) membership keyed by physical slot, minimal memory.
- **Identity-hash pointer map with inline-single-pointer collision layout**; fixed-seed ahash.
- **Memoized statistics everywhere** (num_granules, freelist_len, num_key_bytes,
  datastore_page_bytes) with reconstruct-and-assert paths; the `MemoryUsage`
  destructuring-based accounting trait.
- **BLAKE3 page content hashes** for incremental snapshot dedup; blob content-addressing to
  avoid comparing large values.
- **64-byte granule size = cache line = header alignment**, and the 6/10-bit packed granule
  header exploiting alignment-forced zero bits.

---

## What Fluxum will face

**1. SPEC-002's data structures are a different (and heavier) engine, not a simplification of
SpacetimeDB's.** `BTreeMap<PrimaryKey, Row>` with `Row` as a materialized value costs per-row
heap allocations, pointer-chasing scans, and 3–10× the memory of BFLATN pages. That may be an
acceptable *Phase-2 milestone*, but SPEC-002 presents it as the design. Decision needed: either
(a) declare STG-002's types a logical contract (SPEC-015 already hints at this in TIER-050) and
implement page-based clustered leaves from the start, or (b) accept the naive engine and plan a
rewrite. Path (a) is what SpacetimeDB's 21k-LOC, unsafe-dense `crates/table` costs — expect the
table crate to be Fluxum's single largest and riskiest component (multiple person-months, plus
a proptest/fuzz investment comparable to the code itself; SpacetimeDB tests pages with
proptest and content-hash oracles). Path (b) ships sooner but makes NFR-02 (<1 µs) and TIER-043
(3× compression, which presumes a compact row format) harder.

**2. Fluxum's tiering (SPEC-015) has no precedent in this codebase — but the page design
transfers.** SpacetimeDB validates that a self-relative, no-uninit, checksummable page with
slot bitmaps and intra-page var-len storage works and snapshots cleanly. Fluxum can adopt:
16-bit intra-page offsets, granule-style var-len storage with a blob threshold, per-page
content hash for dirty tracking/checkpoint dedup, and pooled page frames. What Fluxum must
add that SpacetimeDB never solved: a fault-in seam (every row access behind a
pin/translate step), paged *indexes* (SpacetimeDB's are RAM-only `std` structures — TIER-050
is genuinely novel work), and eviction-safe row addressing (either logical keys in indexes,
paying a lookup per index hit, or physical addresses plus pinned residency — SpacetimeDB's
experience says physical addresses are a huge perf win but create the ABA/stability
obligations in §4.2.1).

**3. SPEC-002 is silent on machinery that is provably necessary.** Concretely missing vs this
codebase: (i) where unique/PK constraint checks happen relative to `TxState` and how they see
tx-deleted committed rows; (ii) delete-then-reinsert cancellation semantics; (iii) transactional
DDL / schema-change rollback (SPEC-010 migrations will need a `PendingSchemaChange`-style undo
log the moment any DDL applies eagerly); (iv) large-value handling beyond overflow pages —
recommend a refcounted content-addressed blob store with a tx-local overlay, which also
de-duplicates across snapshots; (v) auto-inc batch allocation (STG-040 as written implies a
per-insert durable counter update; adopt allocation steps persisted in a system table);
(vi) system-table catalog — Fluxum has no st_* equivalent specced anywhere, yet recovery
(STG-030) and migrations need a durable schema source of truth.

**4. Concurrency: Fluxum's single-writer choice matches, but read isolation differs.**
SpacetimeDB: writer sees own writes; readers concurrent under a read lock; commit can downgrade
write→read for subscription evaluation. Fluxum STG-004 hides tx writes from default reads —
simpler to implement (no overlay on the read path) but means reducers can't read-back inserts
without the `scan_pending` split API, and SPEC-005 subscription evaluation after commit will
want SpacetimeDB's lock-downgrade trick to avoid a writer sneaking in between commit and diff
evaluation. Worth adding to SPEC-003.

**5. Effort implications, summarized.** Adopting the SpacetimeDB-grade engine ≈ rewriting
`crates/table` + the overlay half of `crates/datastore` (~30k LOC of dense Rust, much of it
unsafe with proof comments) *plus* the tiering layer SpacetimeDB doesn't have. Staying with
SPEC-002's literal structures ≈ weeks not months, at the cost of memory footprint and the <1 µs
target, and with a guaranteed later migration. A pragmatic middle path: keep SPEC-002's
`BTreeMap` semantics but store rows as boxed FluxBIN bytes (compact, serialization-free
commitlog append), design `RowId`/page coordinates and the constraint/undelete/undo-log
machinery from day one per §4.2, and land SPEC-015's pager under the same logical API in
Phase 2 as planned. Either way, SPEC-002 should be amended with the six omissions in point 3 —
they are correctness requirements, not optimizations.
