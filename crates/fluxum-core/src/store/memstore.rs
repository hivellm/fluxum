//! [`MemStore`] — CommittedState + single-writer transactions (STG-001,
//! STG-003..STG-007, STG-040). See [`super`] for the design decisions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arc_swap::ArcSwap;

use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};
use crate::index::{
    BTreeIndex, FullTextIndexState, IndexId, Rect, SpatialIndexState, SpatialPredicate,
};
use crate::schema::{IndexSchema, Schema, SpatialKind, TableSchema};
use crate::store::TableId;
use crate::store::committed::{CommittedState, Snapshot, TableState};
use crate::store::pager::{PagedTree, Pager, PagerOptions, Reclaimer, VersionGuard};
use crate::store::row::{
    PkBytes, Row, RowValue, check_row, display_pk_of_row, encode_pk_of_row, encode_pk_values,
};
use crate::store::tx::{PendingOp, TableDiff, TriggerEvent, TriggerKind, TxDiff, TxState};
use crate::store::unique::{self, UniqueIndex};

/// Tuning knobs for a [`MemStore`] (SPEC-002 §8; wired into `config.yml`
/// with the server assembly).
#[derive(Debug, Clone, Copy)]
pub struct StoreOptions {
    /// STG-040 batched allocation step: how far the auto-inc high-water mark
    /// advances per durable allocation (`auto_inc_allocation_step`,
    /// default 4096). Must be ≥ 1.
    pub auto_inc_allocation_step: u64,
    /// SPX-003 spatial node capacity: the QuadTree leaf bucket size (max
    /// entries per leaf before it splits) and the R-tree node fan-out,
    /// default 8. Must be ≥ 1 (≥ 2 effective for R-trees). Any value
    /// produces identical query results — this only tunes tree shape.
    pub spatial_bucket_size: usize,
    /// SPX-004 root bounds for this shard's spatial indexes: the shard's
    /// assigned region under geospatial partitioning (SPEC-007), or the
    /// table's configured coordinate range otherwise. Width and height must
    /// be finite and > 0. Rows outside the bounds are still indexed
    /// correctly (overflow bucket) — the bounds only size the tree.
    pub spatial_bounds: Rect,
    /// SPEC-022 RV-020: how many recent commits' snapshots stay reachable
    /// for `AS OF` reads (default 64; `0` disables temporal reads). This is
    /// the **feature** bound — how far back `AS OF` can see. It is not the
    /// binding one under load; see [`Self::temporal_window_max_bytes`].
    pub temporal_window: usize,
    /// SPEC-022 RV-020's *"bounded by budget"* half: the ceiling, in bytes,
    /// on the superseded pages the temporal window is allowed to pin.
    /// `None` derives it from the buffer pool's capacity.
    ///
    /// The count bound alone is not enough on a paged store. Retaining a
    /// snapshot pins every page that snapshot's commit superseded (TIER-061
    /// cannot free them while a live version reaches them), so the window's
    /// real cost is `superseded pages per commit x window` — and
    /// transaction size is unbounded. Measured before this bound existed: a
    /// 100 000-row table with ~5 MB of live data held **241 MiB** of pinned
    /// version garbage under 800-row transactions, which pushed the pool
    /// past its eviction watermark and cut write throughput by 24x (37 900
    /// to 1 563 rows/s). The count bound was doing its job; it was simply
    /// measuring the wrong thing.
    pub temporal_window_max_bytes: Option<u64>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            auto_inc_allocation_step: 4096,
            spatial_bucket_size: 8,
            // ±2^20 covers typical projected-coordinate workloads; out-of-
            // bounds rows stay correct via the overflow bucket (SPX-004).
            spatial_bounds: Rect::new(-1_048_576.0, -1_048_576.0, 2_097_152.0, 2_097_152.0),
            temporal_window: 64,
            temporal_window_max_bytes: None,
        }
    }
}

/// Share of the buffer pool the temporal window may pin when
/// [`StoreOptions::temporal_window_max_bytes`] is left to derive.
///
/// A quarter leaves the working set three quarters of the pool. The exact
/// figure matters far less than having *some* ceiling: without one the
/// window's cost is unbounded in transaction size, and with one the pool
/// stops being able to fill with garbage at all.
const TEMPORAL_WINDOW_POOL_SHARE: f64 = 0.25;

/// Per-table auto-inc counter (STG-040).
///
/// `next` is the next value to hand out; `high_water` the highest value
/// covered by a durable batch allocation. Both live on the writer side and
/// deliberately survive rollback: handed-out values are never returned, so
/// gaps after rollback are normal and documented.
#[derive(Debug, Clone, Copy)]
struct AutoIncCounter {
    next: u64,
    high_water: u64,
}

impl AutoIncCounter {
    const fn new() -> Self {
        Self {
            next: 1,
            high_water: 0,
        }
    }

    /// Hand out the next value, advancing the high-water mark by `step`
    /// when the current batch is exhausted. Returns `(value, advanced)`.
    fn allocate(&mut self, step: u64) -> (u64, bool) {
        let mut advanced = false;
        if self.next > self.high_water {
            self.high_water = self.next.saturating_add(step - 1);
            advanced = true;
        }
        let value = self.next;
        self.next = self.next.saturating_add(1);
        (value, advanced)
    }

    /// Record an explicitly supplied (non-placeholder) id so the sequence
    /// stays unique and monotonic. Returns whether the high-water mark
    /// advanced.
    fn observe_explicit(&mut self, value: u64) -> bool {
        if value >= self.next {
            self.next = value.saturating_add(1);
        }
        if value > self.high_water {
            self.high_water = value;
            true
        } else {
            false
        }
    }
}

/// Writer-side state guarded by the single-writer mutex.
#[derive(Debug)]
struct WriterState {
    /// The id the next *committed* transaction receives (TXN-030: rollbacks
    /// do not consume ids).
    next_tx_id: u64,
    /// The version the next published [`CommittedState`] receives (SPEC-015
    /// TIER-061): monotonic, assigned under the writer lock, and the key the
    /// page reclaimer scopes superseded-page frees to.
    next_version: u64,
    /// Live auto-inc counters (STG-040). Persist across rollback — gaps.
    counters: HashMap<TableId, AutoIncCounter>,
    /// Tables whose high-water mark advanced since the last commit; the
    /// next commit's [`TxDiff`] publishes them (even advances made by a
    /// transaction that later rolled back).
    high_water_dirty: BTreeSet<TableId>,
}

/// The per-shard transactional store (STG-001): a lock-free committed
/// snapshot plus at most one in-flight transaction.
#[derive(Debug)]
pub struct MemStore {
    /// `TableId` → link-time schema, fixed at construction.
    catalog: HashMap<TableId, &'static TableSchema>,
    /// The per-shard paged cold tier: every table's primary row tree (and, as
    /// TIER-050/051 lands, its index trees) faults in and evicts through this
    /// pager's buffer pool under `memory.budget` (SPEC-015). Shared (`Arc`)
    /// with every [`TableState`]'s [`PagedTree`].
    pager: Arc<crate::store::pager::Pager>,
    /// Version-scoped page reclaimer (TIER-061): frees the pages a commit
    /// superseded once no live snapshot pins the version that still reaches
    /// them. Pinned by each [`CommittedState`]'s guard.
    reclaimer: Arc<crate::store::pager::Reclaimer>,
    /// Keeps the auto-created default page directory alive for the store's
    /// lifetime (`None` when the caller supplied its own pager via
    /// [`MemStore::with_pager`]).
    _page_dir: Option<tempfile::TempDir>,
    /// The stable snapshot, swapped atomically on commit (STG-005).
    committed: ArcSwap<CommittedState>,
    /// Single-writer guarantee (STG-003): `begin` holds this for the whole
    /// transaction.
    writer: Mutex<WriterState>,
    options: StoreOptions,
    /// The shard's blob store, once attached (SPEC-023 DMX-040): `Blob`
    /// columns are validated against it at write time and reference-counted
    /// in the commit merge; without one, `Blob` writes are rejected.
    blobs: std::sync::OnceLock<Arc<crate::commitlog::BlobStore>>,
    /// The column-transform executor, once attached (SPEC-017 §5): encrypts
    /// `#[encrypted]` columns on write so stored rows carry ciphertext.
    transforms: std::sync::OnceLock<Arc<crate::transform::engine::TransformEngine>>,
    /// `#[computed]` columns per table (SPEC-022 RV-050): `(ordinal, compute)`
    /// derivations applied on write, resolved from the link-time registry at
    /// construction (pure fns; no runtime attach).
    computed: HashMap<TableId, Vec<(u16, crate::schema::ComputeFn)>>,
    /// `#[check(expr)]` constraints per table (SPEC-022 RV-030), validated on
    /// every write before merge.
    checks: HashMap<TableId, Vec<&'static crate::schema::CheckDef>>,
    /// `#[not_null]` columns per table (RV-030): `Option`-typed columns that
    /// reject `None` on write.
    not_null: HashMap<TableId, Vec<&'static crate::schema::NotNullDef>>,
    /// Outgoing foreign keys per child table (RV-030): parent existence is
    /// validated on every child write.
    fks_out: HashMap<TableId, Vec<ResolvedFk>>,
    /// Incoming foreign keys per parent table (RV-032): the referential
    /// action each parent delete applies to its child rows.
    fks_in: HashMap<TableId, Vec<ResolvedFk>>,
    /// SPEC-022 RV-020: the bounded temporal window — the last
    /// `options.temporal_window` commits' snapshots, ascending by `tx_id`,
    /// each tagged `(tx_id, commit timestamp µs)`. `AS OF` reads resolve
    /// here ([`MemStore::snapshot_as_of`]).
    history: Mutex<std::collections::VecDeque<(u64, i64, Arc<CommittedState>)>>,
    /// The resolved RV-020 byte ceiling for `history` (see
    /// [`StoreOptions::temporal_window_max_bytes`]).
    temporal_window_max_bytes: u64,
    /// How many snapshots the byte ceiling has dropped that the count bound
    /// would have kept — the operator-visible signal that `AS OF` reach is
    /// being traded for memory, which is otherwise silent.
    temporal_window_budget_evictions: std::sync::atomic::AtomicU64,
    /// SPEC-007 SHD-031: `false` on non-authoritative shards — a reducer
    /// writing a `#[fluxum::table(global)]` row there errors instead of
    /// diverging from the authoritative copy. Default `true` (single-shard
    /// deployments and the authoritative shard).
    global_writes_allowed: std::sync::atomic::AtomicBool,
}

/// The point an `AS OF` read resolves to (SPEC-022 RV-021).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsOfPoint {
    /// The committed state right after transaction `tx_id`.
    Tx(u64),
    /// The committed state at `µs` since the Unix epoch (the newest commit
    /// at or before it).
    Timestamp(i64),
}

/// A `#[references]` declaration resolved against the assembled catalog
/// (SPEC-022 RV-030/032): both ends validated at store construction, so the
/// write/delete hot paths never re-resolve names.
#[derive(Debug, Clone, Copy)]
struct ResolvedFk {
    /// The child (referencing) table.
    child: TableId,
    /// The child table's schema.
    child_schema: &'static TableSchema,
    /// The child's referencing column ordinal.
    child_ordinal: u16,
    /// The child's referencing column name (error messages).
    child_column: &'static str,
    /// The parent (referenced) table; its single-column PK is the target.
    parent: TableId,
    /// The parent table's schema.
    parent_schema: &'static TableSchema,
    /// What a parent delete does to referencing child rows (RV-032).
    on_delete: crate::schema::RefAction,
}

impl MemStore {
    /// Build a store over an assembled [`Schema`] with default options.
    pub fn new(schema: &Schema) -> Result<Self> {
        Self::with_options(schema, StoreOptions::default())
    }

    /// Build a store over an assembled [`Schema`] with a paged cold tier
    /// rooted at a private temporary directory and a generous default buffer
    /// pool — the convenience constructor for tests and tooling. A server
    /// deployment threads its configured page directory and budget-sized pool
    /// in through [`MemStore::with_pager`].
    ///
    /// Fails on invalid options or a `TableId` collision (two table names
    /// hashing to the same CRC32, STG-050 — must be renamed).
    pub fn with_options(schema: &Schema, options: StoreOptions) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix("fluxum-memstore-")
            .tempdir()
            .map_err(|e| FluxumError::Storage(format!("open default page directory: {e}")))?;
        // A default pool large enough that tests stay resident (nothing spills
        // until the working set exceeds it); real deployments size it from
        // `memory.budget` via `with_pager`.
        let pager = Pager::open(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 256 << 20,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: crate::config::PageCompression::None,
                compression_min_bytes: 1024,
            },
        )?;
        Self::build(schema, options, pager, Some(dir))
    }

    /// Build a store over `schema` serving through `pager` — the server-assembly
    /// constructor: the pager is opened over the configured `storage.page_dir`
    /// with a pool sized from the effective `memory.budget` (SPEC-015), so the
    /// live store's steady-state RSS is bounded by the budget (TIER-004).
    pub fn with_pager(schema: &Schema, options: StoreOptions, pager: Arc<Pager>) -> Result<Self> {
        Self::build(schema, options, pager, None)
    }

    /// Shared construction over an already-open `pager`.
    fn build(
        schema: &Schema,
        options: StoreOptions,
        pager: Arc<Pager>,
        page_dir: Option<tempfile::TempDir>,
    ) -> Result<Self> {
        if options.auto_inc_allocation_step == 0 {
            return Err(FluxumError::Storage(
                "auto_inc_allocation_step must be >= 1 (STG-040)".into(),
            ));
        }
        if options.spatial_bucket_size == 0 {
            return Err(FluxumError::Storage(
                "spatial_bucket_size must be >= 1 (SPX-003)".into(),
            ));
        }
        let b = options.spatial_bounds;
        if !(b.x.is_finite() && b.y.is_finite() && b.w.is_finite() && b.h.is_finite())
            || b.w <= 0.0
            || b.h <= 0.0
        {
            return Err(FluxumError::Storage(format!(
                "spatial_bounds must be finite with positive extents, got \
                 ({}, {}, {}, {}) (SPX-004)",
                b.x, b.y, b.w, b.h
            )));
        }
        let reclaimer = Reclaimer::new(Arc::clone(&pager));
        let mut catalog: HashMap<TableId, &'static TableSchema> = HashMap::new();
        // RV-020 "bounded by budget": the garbage the temporal window pins
        // lives in the buffer pool, so the pool's capacity is what the
        // ceiling should be a share of. Resolved here, before `pager` moves
        // into the store.
        let temporal_window_max_bytes = options.temporal_window_max_bytes.unwrap_or_else(|| {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_sign_loss,
                clippy::cast_possible_truncation
            )]
            {
                (pager.metrics().snapshot().bufferpool_capacity_bytes as f64
                    * TEMPORAL_WINDOW_POOL_SHARE) as u64
            }
        });
        let mut tables: HashMap<TableId, Arc<TableState>> = HashMap::new();
        let mut counters: HashMap<TableId, AutoIncCounter> = HashMap::new();
        for table in schema.tables() {
            let id = TableId::of(table.name);
            if let Some(existing) = catalog.insert(id, table) {
                return Err(FluxumError::Schema(format!(
                    "TableId collision: `{}` and `{}` both hash to {id} (STG-050); \
                     rename one of the tables",
                    existing.name, table.name
                )));
            }
            let mut unique = Vec::with_capacity(table.unique.len());
            for columns in table.unique {
                unique.push(UniqueIndex::new(columns, &pager, id)?);
            }
            tables.insert(
                id,
                Arc::new(TableState {
                    schema: table,
                    rows: PagedTree::create(&pager, id, false)?,
                    row_count: 0,
                    indexes: build_btree_indexes(table, &pager, id)?,
                    spatial: build_spatial_index(table, &options, &pager, id)?,
                    fulltext: build_fulltext_indexes(table, &pager, id)?,
                    unique,
                    auto_inc_high_water: 0,
                }),
            );
            if table.auto_inc.is_some() {
                counters.insert(id, AutoIncCounter::new());
            }
        }
        let computed = build_computed(&catalog);
        let (checks, not_null) = build_checks(&catalog);
        let (fks_out, fks_in) = build_foreign_keys(&catalog)?;
        // Version 0 is the initial empty state; its guard pins it until the
        // first commit's state supersedes it.
        let guard0 = Arc::new(VersionGuard::new(&reclaimer, 0));
        Ok(Self {
            catalog,
            pager,
            reclaimer,
            _page_dir: page_dir,
            blobs: std::sync::OnceLock::new(),
            transforms: std::sync::OnceLock::new(),
            computed,
            checks,
            not_null,
            fks_out,
            fks_in,
            history: Mutex::new(std::collections::VecDeque::new()),
            temporal_window_max_bytes,
            temporal_window_budget_evictions: std::sync::atomic::AtomicU64::new(0),
            global_writes_allowed: std::sync::atomic::AtomicBool::new(true),
            committed: ArcSwap::from_pointee(CommittedState::new(tables, 0, guard0)),
            writer: Mutex::new(WriterState {
                next_tx_id: 1,
                next_version: 1,
                counters,
                high_water_dirty: BTreeSet::new(),
            }),
            options,
        })
    }

    /// The per-shard pager backing this store's paged cold tier (SPEC-015) —
    /// the checkpoint/recovery seam and buffer-pool diagnostics reach it here.
    pub fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    /// Retain `published` in the SPEC-022 RV-020 temporal window, trimmed
    /// back to **both** its bounds: the commit count (how far `AS OF` may
    /// see) and the byte ceiling (what that reach is allowed to cost).
    ///
    /// The byte bound is the one that binds under load. Retaining a
    /// snapshot of a paged store pins every page its commit superseded —
    /// TIER-061 cannot free a page while a live version reaches it — so the
    /// window's cost scales with transaction size, which nothing bounds.
    ///
    /// Popped entries are dropped **after** the history lock is released.
    /// Dropping one releases its `VersionGuard`, which unpins the version
    /// and runs reclamation (page-file frees); doing that inline would hold
    /// this lock across I/O and would take the reclaimer lock beneath it.
    fn retain_in_temporal_window(
        &self,
        tx_id: u64,
        at_micros: i64,
        published: Arc<CommittedState>,
    ) {
        if self.options.temporal_window == 0 {
            return;
        }
        let mut evicted_for_budget = 0u64;
        let dropped: Vec<_> = {
            let mut history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
            history.push_back((tx_id, at_micros, published));
            let mut dropped = Vec::new();
            while history.len() > self.options.temporal_window {
                dropped.extend(history.pop_front());
            }
            // Then the cost bound. `pending()` counts pages pinned by
            // *anything*, including snapshots this store does not own, so
            // the loop is bounded by the history rather than by reaching a
            // target it might never reach.
            while !history.is_empty()
                && self.retained_window_bytes() > self.temporal_window_max_bytes
            {
                dropped.extend(history.pop_front());
                evicted_for_budget += 1;
            }
            dropped
        };
        if evicted_for_budget > 0 {
            self.temporal_window_budget_evictions
                .fetch_add(evicted_for_budget, std::sync::atomic::Ordering::Relaxed);
        }
        drop(dropped);
    }

    /// Bytes of superseded pages currently pinned awaiting reclamation.
    fn retained_window_bytes(&self) -> u64 {
        let pages = self.reclaimer.pending().pages as u64;
        pages.saturating_mul(self.pager.page_size() as u64)
    }

    /// Snapshots the temporal window currently holds — how far `AS OF`
    /// actually reaches, which the configured `temporal_window` is only an
    /// upper bound on once the byte ceiling starts binding.
    pub fn temporal_window_len(&self) -> usize {
        self.history
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// How many snapshots the RV-020 byte ceiling has dropped that the
    /// commit-count bound would have kept. A non-zero and rising count means
    /// `AS OF` reach is being traded for memory — expected under large
    /// transactions, but it should be visible rather than inferred.
    pub fn temporal_window_budget_evictions(&self) -> u64 {
        self.temporal_window_budget_evictions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// What the TIER-061 reclaimer is still holding: superseded pages that
    /// some live version can still reach, so the pool cannot drop them.
    ///
    /// Read alongside `pager().metrics()`: buffer-pool occupancy alone
    /// cannot distinguish a pool full of live pages from one full of
    /// version garbage waiting on a pinned snapshot, and the two call for
    /// opposite responses.
    pub fn reclaim_pending(&self) -> crate::store::pager::ReclaimPending {
        self.reclaimer.pending()
    }

    /// Assign the next version under the writer lock, pin it, and swap it in as
    /// the new committed state (STG-005). Infallible — used by publishes that
    /// supersede no pages (spatial/full-text rebuilds share the row trees).
    fn publish_versioned(
        &self,
        writer: &mut WriterState,
        tables: HashMap<TableId, Arc<TableState>>,
    ) -> Arc<CommittedState> {
        let version = writer.next_version;
        writer.next_version = version.saturating_add(1);
        let guard = Arc::new(VersionGuard::new(&self.reclaimer, version));
        let published = Arc::new(CommittedState::new(tables, version, guard));
        self.committed.store(Arc::clone(&published));
        published
    }

    /// Publish `tables`, then retire the pages this commit `superseded` against
    /// the new version (TIER-061 — freed once no older snapshot pins them).
    fn publish(
        &self,
        writer: &mut WriterState,
        tables: HashMap<TableId, Arc<TableState>>,
        superseded: Vec<(TableId, u64)>,
    ) -> Result<Arc<CommittedState>> {
        let published = self.publish_versioned(writer, tables);
        self.reclaimer.retire(published.version(), superseded)?;
        Ok(published)
    }

    /// The [`TableId`] of a registered table name.
    pub fn table_id(&self, name: &str) -> Option<TableId> {
        let id = TableId::of(name);
        self.catalog.contains_key(&id).then_some(id)
    }

    /// The schema of a registered table (by id).
    pub fn table_schema(&self, table: TableId) -> Option<&'static TableSchema> {
        self.catalog.get(&table).copied()
    }

    /// Every registered table's schema (order unspecified) — the SPEC-012
    /// `/metrics` exporter walks this for per-table row gauges (OBS-030).
    pub fn table_schemas(&self) -> impl Iterator<Item = &'static TableSchema> + '_ {
        self.catalog.values().copied()
    }

    /// Attach the shard's blob store (SPEC-023 DMX-040) and rebuild its
    /// refcounts from the **current** committed snapshot — call after
    /// recovery, before serving writes. `Blob` column writes are rejected
    /// until a store is attached. Idempotent-safe: a second attach is
    /// ignored (the first store stays authoritative).
    pub fn attach_blob_store(&self, blobs: Arc<crate::commitlog::BlobStore>) {
        use crate::commitlog::BlobHash;
        let snapshot = self.snapshot();
        let mut references: Vec<BlobHash> = Vec::new();
        for (table_id, schema) in &self.catalog {
            let ordinals = blob_ordinals(schema);
            if ordinals.is_empty() {
                continue;
            }
            if let Ok(rows) = snapshot.scan(*table_id) {
                for row in rows {
                    for &ordinal in &ordinals {
                        if let Some(RowValue::Blob(blob)) = row.value(ordinal) {
                            references.push(BlobHash::from_bytes(*blob.as_bytes()));
                        }
                    }
                }
            }
        }
        blobs.rebuild_refcounts(references);
        let _ = self.blobs.set(blobs);
    }

    /// Attach the shard's column-transform executor (SPEC-017 §5). `#[encrypted]`
    /// columns are sealed on write once this is set; idempotent (the first
    /// attach wins).
    pub fn attach_transform_engine(&self, engine: Arc<crate::transform::engine::TransformEngine>) {
        let _ = self.transforms.set(engine);
    }

    /// The attached transform executor, if any.
    pub fn transform_engine(&self) -> Option<&Arc<crate::transform::engine::TransformEngine>> {
        self.transforms.get()
    }

    /// The attached blob store, if any.
    pub fn blob_store(&self) -> Option<&Arc<crate::commitlog::BlobStore>> {
        self.blobs.get()
    }

    /// The [`IndexId`] of the B-tree index declared on table `name` over
    /// exactly `columns` (names in declared key order), if one exists.
    pub fn index_id(&self, name: &str, columns: &[&str]) -> Option<IndexId> {
        let schema = self.catalog.get(&TableId::of(name))?;
        let declared = schema.indexes.iter().any(|index| {
            let IndexSchema::BTree { columns: ordinals } = index else {
                return false;
            };
            ordinals.len() == columns.len()
                && ordinals
                    .iter()
                    .zip(columns)
                    .all(|(&ordinal, &want)| schema.column(ordinal).is_some_and(|c| c.name == want))
        });
        declared.then(|| IndexId::of(name, columns))
    }

    /// A lock-free, consistent point-in-time view of the committed state
    /// (STG-004, FR-10). Never blocks, regardless of writer activity.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: self.committed.load_full(),
        }
    }

    /// SPX-031, step 1: put every spatial index into the **rebuilding**
    /// state — emptied, not ready. From this point spatial queries answer
    /// `503 spatial index not ready` (SPX-023) until
    /// [`MemStore::rebuild_spatial_indexes`] completes; the server assembly
    /// gates `ReducerCall` admission on [`MemStore::spatial_ready`] so the
    /// rebuild always finishes before the shard serves writes. Called by the
    /// recovery path when spatial state must be reconstructed asynchronously
    /// (spatial indexes are not persisted).
    pub fn mark_spatial_rebuilding(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            if let Some(spatial) = &slot.spatial {
                // A fresh paged index (its own root); the superseded root of
                // the index being replaced is one empty page in this flow —
                // the store has just booted — and is reclaimed with the file.
                let rebuilding = spatial.rebuilding_like()?;
                Arc::make_mut(slot).spatial = Some(rebuilding);
            }
        }
        self.publish_versioned(&mut writer, tables);
        Ok(())
    }

    /// SPX-031, step 2: rebuild every spatial index from the committed rows
    /// and publish the ready state atomically. After this returns, spatial
    /// query results are identical to a never-crashed store's.
    pub fn rebuild_spatial_indexes(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        for (id, slot) in &mut tables {
            let Some(spatial) = &slot.spatial else {
                continue;
            };
            let mut rebuilt = spatial.fresh_like()?;
            let mut sup = Vec::new();
            slot.for_each_row(|pk, row| rebuilt.insert_row(&row, pk, &mut sup))?;
            Arc::make_mut(slot).spatial = Some(rebuilt);
            superseded.extend(sup.into_iter().map(|p| (*id, p)));
        }
        self.publish(&mut writer, tables, superseded)?;
        Ok(())
    }

    /// FTS-022, step 1: put every full-text index into the **rebuilding**
    /// state — emptied, not ready — mirroring [`mark_spatial_rebuilding`].
    ///
    /// [`mark_spatial_rebuilding`]: Self::mark_spatial_rebuilding
    pub fn mark_fulltext_rebuilding(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            if slot.fulltext.is_empty() {
                continue;
            }
            // Fresh paged indexes (their own roots); the superseded roots of
            // the indexes being replaced are empty pages in this flow — the
            // store has just booted — and are reclaimed with the file.
            let rebuilding = slot
                .fulltext
                .iter()
                .map(FullTextIndexState::rebuilding_like)
                .collect::<Result<Vec<_>>>()?;
            Arc::make_mut(slot).fulltext = rebuilding;
        }
        self.publish_versioned(&mut writer, tables);
        Ok(())
    }

    /// FTS-022, step 2: rebuild every full-text index from the committed rows
    /// and publish the ready state atomically. After this returns, full-text
    /// query results are identical to a never-crashed store's.
    pub fn rebuild_fulltext_indexes(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        for (id, slot) in &mut tables {
            if slot.fulltext.is_empty() {
                continue;
            }
            let mut rebuilt = slot
                .fulltext
                .iter()
                .map(FullTextIndexState::fresh_like)
                .collect::<Result<Vec<_>>>()?;
            let mut sup = Vec::new();
            slot.for_each_row(|pk, row| {
                for index in &mut rebuilt {
                    index.insert_row(&row, pk.clone(), &mut sup)?;
                }
                Ok(())
            })?;
            Arc::make_mut(slot).fulltext = rebuilt;
            superseded.extend(sup.into_iter().map(|p| (*id, p)));
        }
        self.publish(&mut writer, tables, superseded)?;
        Ok(())
    }

    /// Whether every full-text index of this store is ready (FTS-022).
    pub fn fulltext_ready(&self) -> bool {
        self.committed
            .load()
            .tables
            .values()
            .all(|t| t.fulltext.iter().all(FullTextIndexState::is_ready))
    }

    /// Whether every spatial index of this store is ready to serve queries
    /// (SPX-031). The server assembly defers `ReducerCall` admission until
    /// this reports `true` after recovery.
    pub fn spatial_ready(&self) -> bool {
        self.committed
            .load()
            .tables
            .values()
            .all(|t| t.spatial.as_ref().is_none_or(SpatialIndexState::is_ready))
    }

    /// Begin a transaction. Blocks until any in-flight transaction on this
    /// shard commits or rolls back (single-writer guarantee, STG-003).
    pub fn begin(&self) -> Tx<'_> {
        // Poison recovery is sound here: a panicking transaction's `TxState`
        // died with its `Tx` handle (nothing was applied to the committed
        // snapshot), and counter advances it made are exactly the documented
        // STG-040 rollback gaps.
        let writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let base = self.committed.load_full();
        let tx_id = writer.next_tx_id;
        Tx {
            store: self,
            writer,
            base,
            state: TxState::new(tx_id),
        }
    }

    /// T2.3 recovery seam (STG-030 steps 2/5/7): atomically install a
    /// recovered `CommittedState`, resume tx-id assignment at `next_tx_id`
    /// (STG-015), and resume every auto-inc counter from its recovered
    /// high-water mark (STG-040 — generation continues at `high_water + 1`,
    /// never reusing an id).
    ///
    /// Recovery-only: must run before the first transaction on this store
    /// (the checkpoint module's `recover` is the sole caller).
    pub(crate) fn install_recovered(&self, state: CommittedState, next_tx_id: u64) -> Result<()> {
        if next_tx_id == 0 {
            return Err(FluxumError::Storage(
                "recovered next_tx_id must be >= 1 (there is no tx 0)".into(),
            ));
        }
        if state.tables.len() != self.catalog.len()
            || !self.catalog.keys().all(|id| state.tables.contains_key(id))
        {
            return Err(FluxumError::Storage(
                "recovered state does not cover exactly the assembled schema (STG-030)".into(),
            ));
        }
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if writer.next_tx_id != 1 {
            return Err(FluxumError::Storage(
                "recovery must run before the first transaction (STG-030)".into(),
            ));
        }
        for (id, table) in &state.tables {
            if table.schema.auto_inc.is_some() {
                let high_water = table.auto_inc_high_water;
                writer.counters.insert(
                    *id,
                    AutoIncCounter {
                        next: high_water.saturating_add(1),
                        high_water,
                    },
                );
            }
        }
        writer.next_tx_id = next_tx_id;
        self.publish_versioned(&mut writer, state.tables);
        Ok(())
    }

    fn schema_of(&self, table: TableId) -> Result<&'static TableSchema> {
        self.catalog.get(&table).copied().ok_or_else(|| {
            FluxumError::Storage(format!(
                "unknown table id {table}: not in the assembled schema"
            ))
        })
    }

    /// SPEC-007 SHD-031: mark this store a non-authoritative replica for
    /// `#[fluxum::table(global)]` tables — reducer writes to them error,
    /// and mutations arrive only through
    /// [`MemStore::apply_replicated_diff`]. Called by `ShardCoord` at
    /// assembly for every shard except the authoritative one.
    pub fn set_global_replica(&self) {
        self.global_writes_allowed
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// SPEC-007 SHD-030: apply a replicated global-table mutation to this
    /// replica's `CommittedState` — indexes maintained like a commit merge,
    /// but NO commit-log entry and NO tx-id consumption (the authoritative
    /// shard's log is the durable record). Only global tables in `diff` are
    /// applied; everything else is ignored.
    pub fn apply_replicated_diff(&self, diff: &TxDiff) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        let mut touched = false;
        for table_diff in &diff.tables {
            let Some(schema) = self.catalog.get(&table_diff.table_id) else {
                continue;
            };
            if schema.access != crate::schema::TableAccess::Global {
                continue;
            }
            let Some(slot) = tables.get_mut(&table_diff.table_id) else {
                continue;
            };
            let table = Arc::make_mut(slot);
            let mut sup: Vec<u64> = Vec::new();
            for (pk, _old) in &table_diff.deletes {
                if let Some(existing) = table.take_row(pk, &mut sup)? {
                    for constraint in &mut table.unique {
                        constraint.remove(&existing, pk, &mut sup)?;
                    }
                    for index in table.indexes.values_mut() {
                        index.remove(&existing, pk, &mut sup)?;
                    }
                    if let Some(spatial) = &mut table.spatial {
                        spatial.remove_row(&existing, pk, &mut sup)?;
                    }
                    for fulltext in &mut table.fulltext {
                        fulltext.remove_row(&existing, pk, &mut sup)?;
                    }
                }
            }
            for row in &table_diff.inserts {
                let pk = encode_pk_of_row(schema, row.values())?;
                // An in-place replacement upserts (CoW replace); the retired
                // row's index entries come out before the new row's go in.
                if let Some(existing) = table.put_row(&pk, row, &mut sup)? {
                    for constraint in &mut table.unique {
                        constraint.remove(&existing, &pk, &mut sup)?;
                    }
                    for index in table.indexes.values_mut() {
                        index.remove(&existing, &pk, &mut sup)?;
                    }
                    if let Some(spatial) = &mut table.spatial {
                        spatial.remove_row(&existing, &pk, &mut sup)?;
                    }
                    for fulltext in &mut table.fulltext {
                        fulltext.remove_row(&existing, &pk, &mut sup)?;
                    }
                }
                for index in table.indexes.values_mut() {
                    index.insert(row, pk.clone(), &mut sup)?;
                }
                for constraint in &mut table.unique {
                    constraint.insert(row, pk.clone(), &mut sup)?;
                }
                if let Some(spatial) = &mut table.spatial {
                    spatial.insert_row(row, pk.clone(), &mut sup)?;
                }
                for fulltext in &mut table.fulltext {
                    fulltext.insert_row(row, pk.clone(), &mut sup)?;
                }
            }
            superseded.extend(sup.into_iter().map(|p| (table_diff.table_id, p)));
            touched = true;
        }
        if touched {
            self.publish(&mut writer, tables, superseded)?;
        }
        Ok(())
    }

    /// SPEC-014 REP-014 step 4: apply one replicated commit-log record to
    /// this replica's `CommittedState` — every table, atomically, with the
    /// same index/unique/spatial/fulltext maintenance as a commit merge —
    /// consuming the record's own `tx_id` (strict monotonicity, STG-015) and
    /// resuming auto-inc high-water marks (STG-040). Returns the applied
    /// [`TxDiff`] so the replica evaluates its local subscriptions and fans
    /// out `TxUpdate`s identical to the primary's (REP-014 step 5, REP-043).
    ///
    /// The caller is the replica's single apply loop; the writer lock is
    /// held for the merge, exactly like a local commit.
    ///
    /// # Errors
    /// A `tx_id` that is not exactly `next_tx_id` (gap or repeat — the
    /// session must abort and resync, REP-014 step 2), an unknown table, or
    /// an index-maintenance invariant failure.
    pub fn apply_replica_record(
        &self,
        record: &crate::commitlog::record::TxRecord,
    ) -> Result<TxDiff> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if record.tx_id != writer.next_tx_id {
            return Err(FluxumError::Storage(format!(
                "replicated tx_id {} does not continue the replica's log at {} \
                 (STG-015 — abort the session and resync, REP-014)",
                record.tx_id, writer.next_tx_id
            )));
        }
        let mut tables = self.committed.load_full().tables.clone();
        let mut diffs: Vec<TableDiff> = Vec::with_capacity(record.mutations.len());
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        for mutation in &record.mutations {
            let table_id = mutation.table();
            let schema = self.schema_of(table_id)?;
            let slot = tables.get_mut(&table_id).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "replicated record references table {table_id} missing from CommittedState"
                ))
            })?;
            let table = Arc::make_mut(slot);
            let mut sup: Vec<u64> = Vec::new();
            let mut diff = TableDiff {
                table_id,
                inserts: Vec::new(),
                deletes: Vec::new(),
            };
            for pk in mutation.delete_pks() {
                let pk = PkBytes::from_bytes(pk);
                if let Some(existing) = table.take_row(&pk, &mut sup)? {
                    for constraint in &mut table.unique {
                        constraint.remove(&existing, &pk, &mut sup)?;
                    }
                    for index in table.indexes.values_mut() {
                        index.remove(&existing, &pk, &mut sup)?;
                    }
                    if let Some(spatial) = &mut table.spatial {
                        spatial.remove_row(&existing, &pk, &mut sup)?;
                    }
                    for fulltext in &mut table.fulltext {
                        fulltext.remove_row(&existing, &pk, &mut sup)?;
                    }
                    diff.deletes.push((pk, existing));
                }
            }
            for row in mutation.insert_rows()? {
                let pk = encode_pk_of_row(schema, row.values())?;
                // An in-place replacement upserts (CoW replace); a bare insert
                // over an existing key (convergent replays) retires the old
                // row's index entries first, mirroring recovery semantics.
                if let Some(existing) = table.put_row(&pk, &row, &mut sup)? {
                    for constraint in &mut table.unique {
                        constraint.remove(&existing, &pk, &mut sup)?;
                    }
                    for index in table.indexes.values_mut() {
                        index.remove(&existing, &pk, &mut sup)?;
                    }
                    if let Some(spatial) = &mut table.spatial {
                        spatial.remove_row(&existing, &pk, &mut sup)?;
                    }
                    for fulltext in &mut table.fulltext {
                        fulltext.remove_row(&existing, &pk, &mut sup)?;
                    }
                }
                for index in table.indexes.values_mut() {
                    index.insert(&row, pk.clone(), &mut sup)?;
                }
                for constraint in &mut table.unique {
                    constraint.insert(&row, pk.clone(), &mut sup)?;
                }
                if let Some(spatial) = &mut table.spatial {
                    spatial.insert_row(&row, pk.clone(), &mut sup)?;
                }
                for fulltext in &mut table.fulltext {
                    fulltext.insert_row(&row, pk.clone(), &mut sup)?;
                }
                diff.inserts.push(row);
            }
            superseded.extend(sup.into_iter().map(|p| (table_id, p)));
            diffs.push(diff);
        }

        // STG-040: resume auto-inc generation from the primary's published
        // high-water marks — a later promotion continues without id reuse.
        let mut auto_inc = Vec::with_capacity(record.auto_inc.len());
        for (raw_id, high_water) in &record.auto_inc {
            let table_id = TableId::from_raw(*raw_id);
            writer.counters.insert(
                table_id,
                AutoIncCounter {
                    next: high_water.saturating_add(1),
                    high_water: *high_water,
                },
            );
            if let Some(slot) = tables.get_mut(&table_id) {
                Arc::make_mut(slot).auto_inc_high_water = *high_water;
            }
            auto_inc.push((table_id, *high_water));
        }

        // Consume the record's tx id and swap — atomic for readers, exactly
        // like a local commit (REP-014 step 4).
        writer.next_tx_id = record.tx_id.saturating_add(1);
        let published = self.publish(&mut writer, tables, superseded)?;

        // SPEC-022 RV-020: replicas retain the same temporal window, so
        // `AS OF` reads answer identically on primary and replica.
        self.retain_in_temporal_window(record.tx_id, record.timestamp, published);

        let diff = TxDiff {
            tx_id: record.tx_id,
            tables: diffs,
            auto_inc,
        };
        // DMX-040: replicated rows hold blob references too.
        if let Some(blobs) = self.blobs.get() {
            apply_blob_refcounts(&self.catalog, blobs, &diff.tables);
        }
        Ok(diff)
    }

    /// SPEC-022 RV-021: the committed state at an earlier point — the
    /// newest retained snapshot at or before `point`. `AS OF` a point newer
    /// than every retained commit answers with the LIVE snapshot (the state
    /// there is the current one); a point older than the retained window is
    /// a typed 3020 (RV-020's bound is real, never silently approximated).
    pub fn snapshot_as_of(&self, point: AsOfPoint) -> Result<Snapshot> {
        let history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
        let (Some(oldest), Some(newest)) = (history.front(), history.back()) else {
            return Err(FluxumError::query(
                codes::SQL_AS_OF_OUT_OF_WINDOW,
                "AS OF: no commits are retained yet (RV-020/021)",
            ));
        };
        let newer_than_all = match point {
            AsOfPoint::Tx(tx) => tx >= newest.0,
            AsOfPoint::Timestamp(us) => us >= newest.1,
        };
        if newer_than_all {
            return Ok(self.snapshot());
        }
        let older_than_window = match point {
            AsOfPoint::Tx(tx) => tx < oldest.0,
            AsOfPoint::Timestamp(us) => us < oldest.1,
        };
        if older_than_window {
            return Err(FluxumError::query(
                codes::SQL_AS_OF_OUT_OF_WINDOW,
                format!(
                    "AS OF {point:?} is older than the retained temporal window \
                     (oldest: tx {} at {} µs) — raise StoreOptions::temporal_window or \
                     use PITR for long horizons (RV-020/021)",
                    oldest.0, oldest.1
                ),
            ));
        }
        // Newest retained snapshot at or before the point.
        let state = history
            .iter()
            .rev()
            .find(|(tx, us, _)| match point {
                AsOfPoint::Tx(want) => *tx <= want,
                AsOfPoint::Timestamp(want) => *us <= want,
            })
            .map(|(_, _, state)| Arc::clone(state))
            .ok_or_else(|| {
                FluxumError::query(
                    codes::SQL_AS_OF_OUT_OF_WINDOW,
                    "AS OF point precedes every retained commit (RV-021)",
                )
            })?;
        Ok(Snapshot { state })
    }

    /// Whether `table` is an ephemeral (memory-only) table (SPEC-023 DMX-010):
    /// its mutations bypass the commit log, checkpoints, and replication.
    /// Unknown table ids are treated as non-ephemeral (durable) — the safe
    /// default for the WAL path.
    pub fn is_ephemeral(&self, table: TableId) -> bool {
        self.catalog
            .get(&table)
            .is_some_and(|schema| schema.is_ephemeral())
    }
}

mod build;
mod tx;
#[allow(clippy::wildcard_imports)]
use build::*;
pub use tx::Tx;

#[cfg(test)]
mod tests;
