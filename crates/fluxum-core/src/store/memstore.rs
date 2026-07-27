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
    /// for `AS OF` reads (default 64; `0` disables temporal reads). Each
    /// retained snapshot structurally shares untouched tables with its
    /// neighbors, so the cost is bounded by the touched-table copies the
    /// commits already made.
    pub temporal_window: usize,
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
        }
    }
}

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
                    fulltext: build_fulltext_indexes(table),
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
    pub fn mark_fulltext_rebuilding(&self) {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            if slot.fulltext.is_empty() {
                continue;
            }
            let rebuilding = slot.fulltext.iter().map(|f| f.rebuilding_like()).collect();
            Arc::make_mut(slot).fulltext = rebuilding;
        }
        self.publish_versioned(&mut writer, tables);
    }

    /// FTS-022, step 2: rebuild every full-text index from the committed rows
    /// and publish the ready state atomically. After this returns, full-text
    /// query results are identical to a never-crashed store's.
    pub fn rebuild_fulltext_indexes(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            if slot.fulltext.is_empty() {
                continue;
            }
            let mut rebuilt: Vec<_> = slot.fulltext.iter().map(|f| f.fresh_like()).collect();
            slot.for_each_row(|pk, row| {
                for index in &mut rebuilt {
                    index.insert_row(&row, pk.clone())?;
                }
                Ok(())
            })?;
            Arc::make_mut(slot).fulltext = rebuilt;
        }
        self.publish_versioned(&mut writer, tables);
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
                        fulltext.remove_row(&existing, pk)?;
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
                        fulltext.remove_row(&existing, &pk)?;
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
                    fulltext.insert_row(row, pk.clone())?;
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
                        fulltext.remove_row(&existing, &pk)?;
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
                        fulltext.remove_row(&existing, &pk)?;
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
                    fulltext.insert_row(&row, pk.clone())?;
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
        if self.options.temporal_window > 0 {
            let mut history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
            history.push_back((record.tx_id, record.timestamp, published));
            while history.len() > self.options.temporal_window {
                history.pop_front();
            }
        }

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

/// A single in-flight transaction (holds the shard's writer lock).
///
/// Reads ([`Tx::query_pk`], [`Tx::scan`]) see only the committed snapshot
/// captured at [`MemStore::begin`] — never this transaction's own pending
/// writes (STG-004 / TXN-050; the explicit `scan_pending`/`scan_all` family
/// is SPEC-004 / T3.x surface). Dropping the handle without calling
/// [`Tx::commit`] rolls back (STG-006).
#[derive(Debug)]
pub struct Tx<'a> {
    store: &'a MemStore,
    writer: MutexGuard<'a, WriterState>,
    base: Arc<CommittedState>,
    state: TxState,
}

impl Tx<'_> {
    /// The id this transaction receives if it commits (TXN-030).
    pub fn tx_id(&self) -> u64 {
        self.state.tx_id
    }

    /// The store's column-transform executor, if attached (SPEC-017 §5) —
    /// the read boundary decrypts `#[encrypted]` columns through it.
    pub(crate) fn transform_engine(
        &self,
    ) -> Option<Arc<crate::transform::engine::TransformEngine>> {
        self.store.transforms.get().cloned()
    }

    /// Insert a row (column values in declaration order).
    ///
    /// Enforces PK uniqueness (TXN-040) and every `#[unique]` constraint
    /// (TXN-041) eagerly against the STG-007 overlay: committed rows
    /// tx-deleted in this transaction do not conflict; pending inserts do.
    /// For `#[auto_inc]` tables, a `0` placeholder in the auto-inc column is
    /// replaced with the next counter value (TXN-042: assigned at insert
    /// time); an explicit non-zero id is kept and the counter jumps past it.
    /// Returns the row as stored, with the assigned id.
    pub fn insert(&mut self, table: TableId, values: Vec<RowValue>) -> Result<Row> {
        self.write(table, values, false)
    }

    /// Insert a row, replacing any existing row with the same primary key
    /// (the TXN-040 exception: an occupied PK replaces instead of erroring).
    /// `#[unique]` constraints against *other* rows still apply (TXN-041),
    /// and auto-inc placeholders are assigned exactly as in [`Tx::insert`].
    /// Returns the row as stored.
    pub fn upsert(&mut self, table: TableId, values: Vec<RowValue>) -> Result<Row> {
        self.write(table, values, true)
    }

    /// Shared insert/upsert path; `replace` selects the TXN-040 semantics
    /// for an occupied primary key.
    fn write(&mut self, table: TableId, mut values: Vec<RowValue>, replace: bool) -> Result<Row> {
        let schema = self.store.schema_of(table)?;
        // SPEC-007 SHD-031: global tables are writable only on the
        // authoritative shard; replicas receive mutations via replication.
        if schema.access == crate::schema::TableAccess::Global
            && !self
                .store
                .global_writes_allowed
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(FluxumError::Reducer(
                "global table writes must execute on the authoritative shard".into(),
            ));
        }
        // RV-050: derive every `#[computed]` column from the other columns,
        // overwriting whatever the reducer set (the column is read-only). Runs
        // before validation so the derived value is what gets type-checked,
        // stored, indexed, and fanned out — and before encryption, so a
        // derivation over a plaintext sibling sees the plaintext.
        if let Some(cols) = self.store.computed.get(&table) {
            for (ordinal, compute) in cols {
                let derived = compute(&values)?;
                if let Some(slot) = values.get_mut(usize::from(*ordinal)) {
                    *slot = derived;
                }
            }
        }
        // DMX-040: a `Blob` value must reference a stored object — validated
        // here (write time) so the commit merge's incref can never miss.
        for &ordinal in &blob_ordinals(schema) {
            if let Some(RowValue::Blob(blob)) = values.get(usize::from(ordinal)) {
                let Some(blobs) = self.store.blobs.get() else {
                    return Err(FluxumError::Storage(format!(
                        "table `{}`: Blob column write with no blob store attached (DMX-040)",
                        schema.name
                    )));
                };
                let hash = crate::commitlog::BlobHash::from_bytes(*blob.as_bytes());
                if !blobs.contains(&hash) {
                    return Err(FluxumError::Storage(format!(
                        "table `{}`: Blob reference {hash} names no stored object — upload                          it first (DMX-040)",
                        schema.name
                    )));
                }
            }
        }
        check_row(schema, &values)?;
        // SPX-010 eager constraint (like PK uniqueness below): an R-tree row
        // must satisfy min <= max per axis, so the commit merge stays
        // infallible.
        if let Some(spatial) = &self.base.table(table)?.spatial {
            spatial.check_insert(schema.name, &values)?;
        }
        self.assign_auto_inc(table, schema, &mut values)?;
        // RV-030: declarative `#[not_null]` / `#[check]` / `#[references]`
        // constraints, validated eagerly at write time (like TXN-040/041) so
        // the commit merge stays infallible — on the plaintext row, after
        // computed derivation and auto-inc assignment, before `#[encrypted]`
        // sealing.
        self.check_constraints(table, schema, &values)?;
        let pk = encode_pk_of_row(schema, &values)?;

        // SPEC-017 CT-011/030: seal `#[encrypted]` columns after validation and
        // pk derivation (key columns are never encrypted, CT-013), before the
        // row is stored — so the commit log, cold pages, checkpoints, and
        // replication stream only ever see ciphertext. Non-encrypted tables
        // pay nothing (the engine fast-skips).
        if let Some(engine) = self.store.transforms.get()
            && engine.touches(table)
        {
            engine.on_write_row(table, &mut values, pk.as_bytes())?;
        }

        let committed_row = self.base.table(table)?.row_at(&pk)?;
        // Overlay occupancy (STG-007): a pending insert/reinsert holds the
        // key; a committed row holds it unless tx-deleted. `PendingOp` rows
        // are `Arc`-shared, so this clone is a pointer bump.
        let pending = self
            .state
            .tables
            .get(&table)
            .and_then(|ops| ops.get(&pk))
            .cloned();
        let occupied = matches!(pending, Some(PendingOp::Insert(_) | PendingOp::Update(_)))
            || (pending.is_none() && committed_row.is_some());
        if occupied && !replace {
            return Err(FluxumError::Storage(format!(
                "primary key conflict: table={} pk={}",
                schema.name,
                display_pk_of_row(schema, &values)
            )));
        }

        // TXN-041: every `#[unique]` constraint, eagerly, over the same
        // overlay — so the commit merge stays validated by construction.
        self.check_unique(table, schema, &values, &pk)?;

        // RV-031: the visible row this write replaces, captured before the
        // buffer mutation so a trigger's `old` argument is the pre-write
        // view. Only tables with registered triggers pay for the clone.
        let wants_events = crate::reducer::has_triggers(table);
        let old_visible: Option<Row> = if wants_events {
            match (&pending, &committed_row) {
                (Some(PendingOp::Insert(row) | PendingOp::Update(row)), _) => Some(row.clone()),
                (Some(PendingOp::Delete), _) => None,
                (None, committed) => committed.clone(),
            }
        } else {
            None
        };

        let ops = self.state.tables.entry(table).or_default();
        let stored = match pending {
            // Tx-deleted committed row: reinsert is allowed (STG-007).
            Some(PendingOp::Delete) => {
                let committed = committed_row.ok_or_else(|| {
                    FluxumError::Storage(format!(
                        "internal invariant violated: Delete op for pk absent from \
                         CommittedState (table={})",
                        schema.name
                    ))
                })?;
                if committed.values() == values.as_slice() {
                    // Delete-then-reinsert of identical content cancels to a
                    // no-op; the committed row identity is preserved
                    // (STG-007 rule 1).
                    ops.remove(&pk);
                    committed
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    row
                }
            }
            // Upsert over this transaction's own pending write: the pending
            // row is replaced in place, keeping its Insert/Update flavor
            // (the flavor records whether a *committed* row underlies the
            // key, which has not changed).
            Some(PendingOp::Insert(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Insert(row.clone()));
                row
            }
            Some(PendingOp::Update(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Update(row.clone()));
                row
            }
            None => {
                if let Some(committed) = committed_row {
                    // Upsert over a committed row (TXN-040 exception).
                    if committed.values() == values.as_slice() {
                        // Identical content: structural no-op, committed row
                        // identity preserved (STG-007 rule 1) — no trigger
                        // event either, nothing changed.
                        return Ok(committed);
                    }
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    row
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Insert(row.clone()));
                    row
                }
            }
        };
        if wants_events {
            let kind = if old_visible.is_some() {
                TriggerKind::Update
            } else {
                TriggerKind::Insert
            };
            self.state.trigger_events.push(TriggerEvent {
                table,
                kind,
                old: old_visible,
                new: Some(stored.clone()),
            });
        }
        Ok(stored)
    }

    /// SPEC-022 RV-030: declarative constraints, validated eagerly at write
    /// time. `#[not_null]` rejects `None`, `#[check]` predicates must hold,
    /// and every `#[references]` value must name an overlay-visible parent
    /// row. Violations abort the transaction with a typed error.
    fn check_constraints(
        &self,
        table: TableId,
        schema: &'static TableSchema,
        values: &[RowValue],
    ) -> Result<()> {
        for def in self.store.not_null.get(&table).into_iter().flatten() {
            if let Some(RowValue::Optional(None)) = values.get(usize::from(def.ordinal)) {
                return Err(FluxumError::query(
                    codes::TXN_NOT_NULL_VIOLATION,
                    format!(
                        "table `{}` column `{}`: None violates #[not_null] (RV-030)",
                        def.table, def.column
                    ),
                ));
            }
        }
        for def in self.store.checks.get(&table).into_iter().flatten() {
            if !(def.check)(values)? {
                return Err(FluxumError::query(
                    codes::TXN_CHECK_VIOLATION,
                    format!(
                        "table `{}` column `{}`: #[check({})] violated (RV-030)",
                        def.table, def.column, def.expr
                    ),
                ));
            }
        }
        for fk in self.store.fks_out.get(&table).into_iter().flatten() {
            let Some(value) = values.get(usize::from(fk.child_ordinal)) else {
                continue; // arity already rejected by check_row
            };
            let target = match value {
                // A None reference is explicitly unlinked — valid.
                RowValue::Optional(None) => continue,
                RowValue::Optional(Some(inner)) => inner.as_ref(),
                other => other,
            };
            if !self.parent_exists(fk, target)? {
                return Err(FluxumError::query(
                    codes::TXN_FK_VIOLATION,
                    format!(
                        "table `{}` column `{}`: referenced `{}` row {target:?} does \
                         not exist (#[references], RV-030)",
                        schema.name, fk.child_column, fk.parent_schema.name
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Overlay-aware parent existence for an RV-030 foreign-key write: a
    /// parent row this transaction inserted satisfies the reference; one it
    /// deleted does not.
    fn parent_exists(&self, fk: &ResolvedFk, value: &RowValue) -> Result<bool> {
        let pk = encode_pk_values(fk.parent_schema, std::slice::from_ref(value))?;
        match self
            .state
            .tables
            .get(&fk.parent)
            .and_then(|ops| ops.get(&pk))
        {
            Some(PendingOp::Insert(_) | PendingOp::Update(_)) => Ok(true),
            Some(PendingOp::Delete) => Ok(false),
            None => self.base.table(fk.parent)?.contains_pk(&pk),
        }
    }

    /// TXN-041: reject `values` if any `#[unique]` constraint value is held
    /// by a *visible* row other than the one at `pk` — visible per the
    /// STG-007 overlay: pending inserts/replacements count, committed rows
    /// count unless tx-deleted or tx-replaced, and the row being written
    /// never conflicts with itself.
    fn check_unique(
        &self,
        table: TableId,
        schema: &'static TableSchema,
        values: &[RowValue],
        pk: &PkBytes,
    ) -> Result<()> {
        let base = self.base.table(table)?;
        if base.unique.is_empty() {
            return Ok(());
        }
        let ops = self.state.tables.get(&table);
        for constraint in &base.unique {
            let key = constraint.key_of_values(values)?;
            // Pending overlay: another pending row carrying this value.
            if let Some(ops) = ops {
                for (other_pk, op) in ops {
                    if other_pk == pk {
                        continue;
                    }
                    let row = match op {
                        PendingOp::Insert(row) | PendingOp::Update(row) => row,
                        PendingOp::Delete => continue,
                    };
                    if constraint.key_of_values(row.values())? == key {
                        return Err(unique::violation_error(
                            schema,
                            constraint.columns(),
                            values,
                        ));
                    }
                }
            }
            // Committed owner — unless it is the row being written, or this
            // transaction tx-deleted/tx-replaced it (an Update whose new row
            // still carries the value was already caught just above).
            if let Some(owner) = constraint.owner(&key)? {
                let shadowed = owner == *pk
                    || ops
                        .and_then(|m| m.get(&owner))
                        .is_some_and(|op| matches!(op, PendingOp::Delete | PendingOp::Update(_)));
                if !shadowed {
                    return Err(unique::violation_error(
                        schema,
                        constraint.columns(),
                        values,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Delete by primary key values (in `primary_key` declaration order).
    ///
    /// Returns whether a (committed or pending) row was deleted. Deleting a
    /// row inserted by this same transaction cancels the insert entirely.
    ///
    /// SPEC-022 RV-032: deleting a row referenced by `#[references]` child
    /// rows applies the declared referential action atomically within this
    /// same transaction — `restrict` (the default) rejects the delete with a
    /// typed 3102 while any child row still references it, `cascade` deletes
    /// the referencing rows (transitively, via an explicit worklist — cycles
    /// terminate because re-deleting an already-deleted row is a no-op), and
    /// `set_null` clears the referencing column.
    pub fn delete(&mut self, table: TableId, pk_values: &[RowValue]) -> Result<bool> {
        let Some(old) = self.delete_one(table, pk_values)? else {
            return Ok(false);
        };
        let store = self.store;
        // RV-032 worklist: every deleted row whose table has incoming
        // references fans out its action; cascade pushes further entries.
        let mut deleted: Vec<(TableId, Row)> = vec![(table, old)];
        while let Some((parent, old_row)) = deleted.pop() {
            let Some(fks) = store.fks_in.get(&parent) else {
                continue;
            };
            for fk in fks {
                // Single-column PK validated at store assembly.
                let &[pk_ord] = fk.parent_schema.primary_key else {
                    continue;
                };
                let parent_value = old_row.values()[usize::from(pk_ord)].clone();
                self.apply_ref_action(fk, &parent_value, &mut deleted)?;
            }
        }
        Ok(true)
    }

    /// Apply one RV-032 referential action: find every overlay-visible child
    /// row referencing `parent_value` and restrict / cascade / set-null it.
    fn apply_ref_action(
        &mut self,
        fk: &ResolvedFk,
        parent_value: &RowValue,
        deleted: &mut Vec<(TableId, Row)>,
    ) -> Result<()> {
        let children: Vec<Row> = self
            .scan_all(fk.child)?
            .into_iter()
            .filter(|row| {
                fk_value_matches(
                    row.values().get(usize::from(fk.child_ordinal)),
                    parent_value,
                )
            })
            .collect();
        for child in children {
            match fk.on_delete {
                crate::schema::RefAction::Restrict => {
                    return Err(FluxumError::query(
                        codes::TXN_FK_VIOLATION,
                        format!(
                            "delete from `{}` restricted: `{}`.`{}` rows still reference \
                             {parent_value:?} (on_delete = restrict, RV-032)",
                            fk.parent_schema.name, fk.child_schema.name, fk.child_column
                        ),
                    ));
                }
                crate::schema::RefAction::Cascade => {
                    let pk_vals: Vec<RowValue> = fk
                        .child_schema
                        .primary_key
                        .iter()
                        .map(|&ord| child.values()[usize::from(ord)].clone())
                        .collect();
                    if let Some(old) = self.delete_one(fk.child, &pk_vals)? {
                        deleted.push((fk.child, old));
                    }
                }
                crate::schema::RefAction::SetNull => {
                    let mut vals = child.values().to_vec();
                    // Stored rows carry `#[encrypted]` columns sealed —
                    // unseal before the rewrite so `write` doesn't seal
                    // ciphertext twice (SPEC-017 CT-011).
                    if let Some(engine) = self.store.transforms.get()
                        && engine.touches(fk.child)
                    {
                        let cpk = encode_pk_of_row(fk.child_schema, &vals)?;
                        engine.on_read_row(fk.child, &mut vals, cpk.as_bytes(), true)?;
                    }
                    vals[usize::from(fk.child_ordinal)] = RowValue::Optional(None);
                    self.write(fk.child, vals, true)?;
                }
            }
        }
        Ok(())
    }

    /// Apply the buffer mutation for one delete, returning the previously
    /// visible row (`None` = nothing to delete). Records the RV-031 trigger
    /// event; RV-032 referential actions are [`Tx::delete`]'s worklist.
    fn delete_one(&mut self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let schema = self.store.schema_of(table)?;
        // SPEC-007 SHD-031: deletes are writes too.
        if schema.access == crate::schema::TableAccess::Global
            && !self
                .store
                .global_writes_allowed
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(FluxumError::Reducer(
                "global table writes must execute on the authoritative shard".into(),
            ));
        }
        let pk = encode_pk_values(schema, pk_values)?;
        let committed_row = self.base.table(table)?.row_at(&pk)?;
        let ops = self.state.tables.entry(table).or_default();
        let old = match ops.get(&pk).cloned() {
            // Insert-then-delete of a pending row cancels to a no-op.
            Some(PendingOp::Insert(row)) => {
                ops.remove(&pk);
                Some(row)
            }
            // The committed row stays deleted; the pending replacement dies.
            Some(PendingOp::Update(row)) => {
                ops.insert(pk, PendingOp::Delete);
                Some(row)
            }
            Some(PendingOp::Delete) => None,
            None => {
                if committed_row.is_some() {
                    ops.insert(pk, PendingOp::Delete);
                }
                committed_row
            }
        };
        if let Some(old_row) = &old
            && crate::reducer::has_triggers(table)
        {
            self.state.trigger_events.push(TriggerEvent {
                table,
                kind: TriggerKind::Delete,
                old: Some(old_row.clone()),
                new: None,
            });
        }
        Ok(old)
    }

    /// SHD-041 steps 2–3: read every row of the entity's row set — each
    /// `(table, key ordinals)` domain member's committed rows whose
    /// partition key equals `key` — and serialize it as the opaque handoff
    /// buffer (`rmp` of `(table id, FluxBIN rows)` pairs, deterministic
    /// table order).
    pub fn handoff_export(
        &self,
        tables: &[(TableId, Vec<u16>)],
        key: &[RowValue],
    ) -> Result<Vec<u8>> {
        let mut export: Vec<(u32, Vec<Vec<u8>>)> = Vec::new();
        let mut sorted: Vec<&(TableId, Vec<u16>)> = tables.iter().collect();
        sorted.sort_by_key(|(table, _)| table.as_u32());
        for (table, key_ordinals) in sorted {
            let mut rows = Vec::new();
            for row in self.scan(*table)? {
                let matches = key_ordinals
                    .iter()
                    .zip(key)
                    .all(|(&ordinal, want)| row.value(ordinal) == Some(want));
                if matches {
                    rows.push(crate::store::row::encode_row(row.values())?);
                }
            }
            export.push((table.as_u32(), rows));
        }
        rmp_serde::to_vec(&export)
            .map_err(|e| FluxumError::Storage(format!("handoff export encode failed: {e}")))
    }

    /// SHD-041 step 7: deserialize a handoff buffer and insert every row of
    /// the row set into THIS shard's buffered transaction. Rows re-validate
    /// through the ordinary write path (constraints, indexes on commit).
    pub fn handoff_import(&mut self, buffer: &[u8]) -> Result<u64> {
        let export: Vec<(u32, Vec<Vec<u8>>)> = rmp_serde::from_slice(buffer)
            .map_err(|e| FluxumError::Storage(format!("handoff import decode failed: {e}")))?;
        let mut imported = 0u64;
        for (table_raw, rows) in export {
            let table = TableId::from_raw(table_raw);
            let schema = self.store.schema_of(table)?;
            for bytes in rows {
                let row = crate::store::row::decode_row(schema, &bytes)?;
                self.upsert(table, row.values().to_vec())?;
                imported += 1;
            }
        }
        Ok(imported)
    }

    /// SHD-041 step 10: delete the entity's rows from this shard (the
    /// cleanup half). Returns how many rows were deleted.
    pub fn handoff_delete(
        &mut self,
        tables: &[(TableId, Vec<u16>)],
        key: &[RowValue],
    ) -> Result<u64> {
        let mut deleted = 0u64;
        for (table, key_ordinals) in tables {
            let schema = self.store.schema_of(*table)?;
            let victims: Vec<Vec<RowValue>> = self
                .scan(*table)?
                .into_iter()
                .filter(|row| {
                    key_ordinals
                        .iter()
                        .zip(key)
                        .all(|(&ordinal, want)| row.value(ordinal) == Some(want))
                })
                .map(|row| {
                    schema
                        .primary_key
                        .iter()
                        .map(|&ordinal| row.values()[usize::from(ordinal)].clone())
                        .collect()
                })
                .collect();
            for pk in victims {
                if self.delete(*table, &pk)? {
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }

    /// Drain the RV-031 trigger events recorded so far, in mutation order.
    /// The reducer layer's `TxHandle` calls this after every mutation and
    /// dispatches registered `#[fluxum::on_*]` hooks inside this same
    /// transaction.
    pub fn take_trigger_events(&mut self) -> Vec<TriggerEvent> {
        std::mem::take(&mut self.state.trigger_events)
    }

    /// Decrypt a stored row's `#[encrypted]` columns (SPEC-017 CT-031) for
    /// the RV-031 trigger dispatch path; a no-op without a transform engine
    /// or when the table has no encrypted column.
    pub(crate) fn decrypt_stored(&self, table: TableId, row: &Row) -> Result<Row> {
        let Some(engine) = self.store.transforms.get() else {
            return Ok(row.clone());
        };
        if !engine.touches(table) {
            return Ok(row.clone());
        }
        let schema = self.store.schema_of(table)?;
        let mut values = row.values().to_vec();
        let pk = encode_pk_of_row(schema, &values)?;
        engine.on_read_row(table, &mut values, pk.as_bytes(), true)?;
        Ok(Row::new(values))
    }

    /// Point lookup against the committed snapshot captured at `begin`
    /// (STG-004: pending writes of this transaction are never visible).
    pub fn query_pk(&self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let t = self.base.table(table)?;
        let pk = encode_pk_values(t.schema, pk_values)?;
        t.row_at(&pk)
    }

    /// Scan the committed snapshot captured at `begin`, in encoded-PK byte
    /// order (STG-004: pending writes are never visible). Materialized from the
    /// paged tree.
    pub fn scan(&self, table: TableId) -> Result<Vec<Row>> {
        self.base.table(table)?.all_rows()
    }

    /// The rows a SPEC-010 (T3.6) migration rewrite operates on: every
    /// committed row keyed by its exact encoded PK bytes, seen **through**
    /// this transaction's pending replacements — a row already rewritten by
    /// an earlier [`Tx::migrate_replace`] of this same step yields its
    /// pending (new-layout) values, so consecutive DDL operations compose.
    /// Rows the transaction deleted or freshly inserted are excluded
    /// (inserts already carry the compiled layout).
    ///
    /// Keying by stored PK bytes — never re-deriving them from the compiled
    /// schema's ordinals — is what keeps rows in an old column layout
    /// addressable.
    pub(crate) fn migrate_rows(&self, table: TableId) -> Result<Vec<(PkBytes, Row)>> {
        let base = self.base.table(table)?;
        let ops = self.state.tables.get(&table);
        let mut out = Vec::with_capacity(base.row_count());
        base.for_each_row(|pk, row| {
            let effective = match ops.and_then(|pending| pending.get(&pk)) {
                Some(PendingOp::Update(replacement)) => Some(replacement.clone()),
                Some(PendingOp::Delete | PendingOp::Insert(_)) => None,
                None => Some(row),
            };
            if let Some(effective) = effective {
                out.push((pk, effective));
            }
            Ok(())
        })?;
        Ok(out)
    }

    /// SPEC-010 (T3.6) migration seam: buffer an in-place replacement of the
    /// committed row at `pk` **without** validating `values` against the
    /// compiled schema.
    ///
    /// Mid-migration rows live in intermediate layouts (the stored catalog's
    /// layout plus the DDL steps applied so far), which only match the
    /// compiled schema after the last migration step — `check_row` would
    /// reject them. Safety rests on the migration runner's contract instead:
    /// `values` are derived from the committed row itself by appending or
    /// renaming columns, so PK bytes and every existing column ordinal
    /// (indexes, `#[unique]`, spatial coordinates) are preserved by
    /// construction, and the commit merge's remove(old)/insert(new) pairs
    /// stay symmetric.
    pub(crate) fn migrate_replace(
        &mut self,
        table: TableId,
        pk: PkBytes,
        values: Vec<RowValue>,
    ) -> Result<()> {
        let schema = self.store.schema_of(table)?;
        if !self.base.table(table)?.contains_pk(&pk)? {
            return Err(FluxumError::Storage(format!(
                "migrate_replace: table `{}` has no committed row at pk {pk} (SPEC-010 \
                 rewrites address committed rows only)",
                schema.name
            )));
        }
        let ops = self.state.tables.entry(table).or_default();
        ops.insert(pk, PendingOp::Update(Row::new(values)));
        Ok(())
    }

    /// Scan a B-tree index of the committed snapshot captured at `begin`
    /// (STG-004: pending writes are never visible). Same shapes and
    /// ordering as [`super::Snapshot::index_scan`].
    pub fn index_scan(
        &self,
        table: TableId,
        index: IndexId,
        prefix: &[RowValue],
        lower: Bound<&RowValue>,
        upper: Bound<&RowValue>,
    ) -> Result<Vec<Row>> {
        self.base
            .table(table)?
            .index_scan(index, prefix, lower, upper)
    }

    /// Equality lookup on a B-tree index of the committed snapshot captured
    /// at `begin` (see [`super::Snapshot::index_eq`]).
    pub fn index_eq(&self, table: TableId, index: IndexId, key: &[RowValue]) -> Result<Vec<Row>> {
        self.index_scan(table, index, key, Bound::Unbounded, Bound::Unbounded)
    }

    /// Spatial region query over the committed snapshot captured at `begin`
    /// (STG-004: pending writes are never visible). See
    /// [`super::Snapshot::spatial_region`].
    pub fn spatial_region(&self, table: TableId, region: Rect) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_region(region)
    }

    /// Spatial radius query over the committed snapshot captured at `begin`.
    /// See [`super::Snapshot::spatial_radius`].
    pub fn spatial_radius(&self, table: TableId, x: f64, y: f64, r: f64) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_radius(x, y, r)
    }

    /// Spatial point query over the committed snapshot captured at `begin`.
    /// See [`super::Snapshot::spatial_point`].
    pub fn spatial_point(&self, table: TableId, x: f64, y: f64) -> Result<Vec<Row>> {
        self.base.table(table)?.spatial_point(x, y)
    }

    /// Evaluate an `IN REGION` / `WITHIN RADIUS` predicate over the
    /// committed snapshot captured at `begin` (SPX-020/021). See
    /// [`super::Snapshot::eval_spatial`] for the 400/503 error contract.
    pub fn eval_spatial(&self, table: TableId, predicate: &SpatialPredicate) -> Result<Vec<Row>> {
        self.base.table(table)?.eval_spatial(predicate)
    }

    /// Rows written by THIS transaction — pending inserts and the new
    /// content of upsert replacements — in encoded-PK byte order. Pending
    /// deletes contribute nothing. This is the explicit TXN-051
    /// read-your-own-writes seam that SPEC-004 surfaces to reducers as
    /// `scan_pending` (FR-17); the default reads above never see these rows
    /// (TXN-050).
    pub fn scan_pending(&self, table: TableId) -> Result<impl Iterator<Item = &Row>> {
        // Unknown-table error parity with `scan`.
        self.base.table(table)?;
        Ok(self
            .state
            .tables
            .get(&table)
            .into_iter()
            .flat_map(|ops| ops.values())
            .filter_map(|op| match op {
                PendingOp::Insert(row) | PendingOp::Update(row) => Some(row),
                PendingOp::Delete => None,
            }))
    }

    /// Combined view (TXN-051): the committed snapshot overlaid with this
    /// transaction's pending effects, deduplicated by primary key — a
    /// pending insert or upsert replacement wins over the committed row
    /// with the same key, and a pending delete removes it. Order: committed
    /// keys in encoded-PK byte order (replacements in place), followed by
    /// the rows newly inserted by this transaction in encoded-PK byte order.
    pub fn scan_all(&self, table: TableId) -> Result<Vec<Row>> {
        let base = self.base.table(table)?;
        let pending = self.state.tables.get(&table);
        let mut out = Vec::with_capacity(base.row_count());
        base.for_each_row(|pk, row| {
            match pending.and_then(|ops| ops.get(&pk)) {
                None => out.push(row),
                Some(PendingOp::Update(replacement)) => out.push(replacement.clone()),
                // Delete: shadowed. Insert over a committed key cannot happen
                // (the write path turns it into Update/Delete), but skipping it
                // here keeps the dedup-by-PK contract even then — the pending
                // pass below yields it once.
                Some(PendingOp::Delete | PendingOp::Insert(_)) => {}
            }
            Ok(())
        })?;
        if let Some(ops) = pending {
            for op in ops.values() {
                if let PendingOp::Insert(row) = op {
                    out.push(row.clone());
                }
            }
        }
        Ok(out)
    }

    /// Commit: merge `TxState` into a new `CommittedState` and swap it in
    /// atomically (STG-005). Constraints were enforced eagerly at write
    /// time, so the merge itself is infallible under the single-writer
    /// guarantee. Returns the [`TxDiff`] for the commit log (T2.2) and
    /// subscription evaluation (SPEC-005).
    pub fn commit(mut self) -> Result<TxDiff> {
        // 1. Build the new state off to the side (readers stay on the old
        //    snapshot; nothing is observable until the swap).
        let mut tables = self.base.tables.clone(); // Arc bumps only
        let mut diffs: Vec<TableDiff> = Vec::with_capacity(self.state.tables.len());
        // Pages the copy-on-write merge superseded, per table — retired
        // against the new version once it is published (TIER-061).
        let mut superseded: Vec<(TableId, u64)> = Vec::new();
        let tx_tables = std::mem::take(&mut self.state.tables);
        for (table_id, ops) in tx_tables {
            if ops.is_empty() {
                continue;
            }
            let slot = tables.get_mut(&table_id).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: touched table {table_id} missing from \
                     CommittedState"
                ))
            })?;
            let table = Arc::make_mut(slot); // deep-clones only touched tables
            let mut sup: Vec<u64> = Vec::new();
            let mut diff = TableDiff {
                table_id,
                inserts: Vec::new(),
                deletes: Vec::new(),
            };
            // Unique-map maintenance is two-pass across the table's ops:
            // every vacated value is released before any new value is
            // claimed, so a transaction that moves a `#[unique]` value
            // between rows (validated eagerly at write time, TXN-041)
            // merges regardless of pk iteration order.
            for (pk, op) in &ops {
                if matches!(op, PendingOp::Delete | PendingOp::Update(_)) {
                    let old = table.row_at(pk)?.ok_or_else(invariant_missing_row)?;
                    for constraint in &mut table.unique {
                        constraint.remove(&old, pk, &mut sup)?;
                    }
                }
            }
            // Rows (paged, CoW) and secondary indexes are updated together on
            // this private pre-swap copy (STG-005 steps 2–4), so the published
            // snapshot's rows and indexes are mutually consistent and rollback
            // never has index state to revert (STG-007 rule 2).
            for (pk, op) in ops {
                match op {
                    PendingOp::Insert(row) => {
                        for index in table.indexes.values_mut() {
                            index.insert(&row, pk.clone(), &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            fulltext.insert_row(&row, pk.clone())?;
                        }
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone(), &mut sup)?;
                        }
                        table.put_row(&pk, &row, &mut sup)?;
                        diff.inserts.push(row);
                    }
                    PendingOp::Delete => {
                        let old = table
                            .take_row(&pk, &mut sup)?
                            .ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk, &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.remove_row(&old, &pk, &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            fulltext.remove_row(&old, &pk)?;
                        }
                        diff.deletes.push((pk, old));
                    }
                    PendingOp::Update(row) => {
                        let old = table
                            .put_row(&pk, &row, &mut sup)?
                            .ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk, &mut sup)?;
                            index.insert(&row, pk.clone(), &mut sup)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            // SPX-032: old coordinates out, new coordinates
                            // in — atomic with the row swap on this private
                            // pre-swap copy, so no stale entry can publish.
                            spatial.remove_row(&old, &pk, &mut sup)?;
                            spatial.insert_row(&row, pk.clone(), &mut sup)?;
                        }
                        for fulltext in &mut table.fulltext {
                            // FTS-021: re-analyze the old text out, the new
                            // text in — atomic with the row swap.
                            fulltext.remove_row(&old, &pk)?;
                            fulltext.insert_row(&row, pk.clone())?;
                        }
                        // The old row's unique values were released in the
                        // two-pass removal above; claim the new row's.
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone(), &mut sup)?;
                        }
                        diff.deletes.push((pk, old));
                        diff.inserts.push(row);
                    }
                }
            }
            superseded.extend(sup.into_iter().map(|p| (table_id, p)));
            diffs.push(diff);
        }

        // 2. Publish auto-inc high-water advances (including ones made by
        //    earlier rolled-back transactions) so T2.2 can log them.
        let dirty = std::mem::take(&mut self.writer.high_water_dirty);
        let mut auto_inc = Vec::with_capacity(dirty.len());
        for table_id in dirty {
            let high_water = self
                .writer
                .counters
                .get(&table_id)
                .map_or(0, |c| c.high_water);
            let slot = tables.get_mut(&table_id).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: auto-inc table {table_id} missing from \
                     CommittedState"
                ))
            })?;
            Arc::make_mut(slot).auto_inc_high_water = high_water;
            auto_inc.push((table_id, high_water));
        }

        // 3. Consume the tx id and publish the new version — atomic for
        //    readers — retiring the pages the CoW merge superseded (TIER-061).
        let tx_id = self.state.tx_id;
        self.writer.next_tx_id = tx_id.saturating_add(1);
        let published = self.store.publish(&mut self.writer, tables, superseded)?;

        // SPEC-022 RV-020: retain this commit's snapshot in the bounded
        // temporal window. Untouched tables are `Arc`-shared with the
        // neighbors, so retention costs only the copies the commit already
        // made. Metadata only — never reducer-visible (SEC-020 unaffected).
        if self.store.options.temporal_window > 0 {
            let mut history = self
                .store
                .history
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            history.push_back((tx_id, crate::types::Timestamp::now().as_micros(), published));
            while history.len() > self.store.options.temporal_window {
                history.pop_front();
            }
        }

        // 4. Blob refcounts (DMX-040): row references drive GC. Applied
        //    under the writer lock, increments before decrements so an
        //    intra-commit move never dips a count to a reclaimable zero.
        if let Some(blobs) = self.store.blobs.get() {
            apply_blob_refcounts(&self.store.catalog, blobs, &diffs);
        }

        Ok(TxDiff {
            tx_id,
            tables: diffs,
            auto_inc,
        })
    }

    /// Roll back: revert eagerly applied effects (STG-007; none exist in
    /// T2.1) and discard `TxState` (STG-006). Committed state is untouched
    /// by construction; the tx id is not consumed (TXN-030). Dropping the
    /// handle without committing is equivalent (see the [`Drop`] impl).
    pub fn rollback(self) {
        // Drop performs the STG-007 revert; being explicit here documents
        // intent at call sites.
    }

    /// Assign / observe the auto-inc column (STG-040, TXN-042).
    fn assign_auto_inc(
        &mut self,
        table: TableId,
        schema: &'static TableSchema,
        values: &mut [RowValue],
    ) -> Result<()> {
        let Some(ordinal) = schema.auto_inc else {
            return Ok(());
        };
        let idx = usize::from(ordinal);
        // DM-004 (registry-validated): the auto-inc column is u64.
        let RowValue::U64(current) = values[idx] else {
            return Err(FluxumError::Storage(format!(
                "table `{}`: #[auto_inc] column must be u64 (FluxType::{:?} found)",
                schema.name, schema.columns[idx].ty
            )));
        };
        let counter = self
            .writer
            .counters
            .entry(table)
            .or_insert_with(AutoIncCounter::new);
        let advanced = if current == 0 {
            let (assigned, advanced) =
                counter.allocate(self.store.options.auto_inc_allocation_step);
            values[idx] = RowValue::U64(assigned);
            advanced
        } else {
            counter.observe_explicit(current)
        };
        if advanced {
            self.writer.high_water_dirty.insert(table);
        }
        Ok(())
    }
}

impl Drop for Tx<'_> {
    /// A dropped (committed or not) transaction always replays its undo log
    /// (STG-007 rule 3) — a reducer panic unwinding through the handle gets
    /// the same revert as an explicit [`Tx::rollback`]. After a successful
    /// [`Tx::commit`] the log is empty, so this is a no-op.
    fn drop(&mut self) {
        self.state.revert_eager_effects();
    }
}

fn invariant_missing_row() -> FluxumError {
    FluxumError::Storage(
        "internal invariant violated: Delete/Update op for pk absent from CommittedState".into(),
    )
}

/// The empty spatial index of `table`, if it declares one (SPEC-008).
///
/// At most one `#[spatial(...)]` declaration per table: SPEC-008 models "the
/// table's spatial index" (SPX-020/021 route by table alone), so a second
/// declaration is rejected here.
fn build_spatial_index(
    table: &'static TableSchema,
    options: &StoreOptions,
    pager: &Arc<Pager>,
    table_id: TableId,
) -> Result<Option<SpatialIndexState>> {
    let mut spatial = None;
    for index in table.indexes {
        let IndexSchema::Spatial { kind, columns } = index else {
            continue;
        };
        if spatial.is_some() {
            return Err(FluxumError::Schema(format!(
                "table `{}`: multiple #[spatial(...)] declarations; a table has at most one \
                 spatial index (SPEC-008)",
                table.name
            )));
        }
        spatial = Some(match kind {
            SpatialKind::QuadTree => SpatialIndexState::quadtree(
                columns,
                options.spatial_bounds,
                options.spatial_bucket_size,
                pager,
                table_id,
            )?,
            SpatialKind::RTree => {
                SpatialIndexState::rtree(columns, options.spatial_bucket_size, pager, table_id)?
            }
        });
    }
    Ok(spatial)
}

/// Resolve the `#[computed]` derivations for every table in `catalog` from the
/// link-time registry (SPEC-022 RV-050), keyed by [`TableId`].
fn build_computed(
    catalog: &HashMap<TableId, &'static TableSchema>,
) -> HashMap<TableId, Vec<(u16, crate::schema::ComputeFn)>> {
    let mut out: HashMap<TableId, Vec<(u16, crate::schema::ComputeFn)>> = HashMap::new();
    for def in crate::schema::registered_computed() {
        let id = TableId::of(def.table);
        if catalog.contains_key(&id) {
            out.entry(id).or_default().push((def.ordinal, def.compute));
        }
    }
    // Apply in ordinal order so a computed column may reference an earlier one.
    for cols in out.values_mut() {
        cols.sort_by_key(|(ord, _)| *ord);
    }
    out
}

/// Resolve the `#[check]` and `#[not_null]` constraints for every table in
/// `catalog` from the link-time registry (SPEC-022 RV-030).
type BuiltChecks = (
    HashMap<TableId, Vec<&'static crate::schema::CheckDef>>,
    HashMap<TableId, Vec<&'static crate::schema::NotNullDef>>,
);

fn build_checks(catalog: &HashMap<TableId, &'static TableSchema>) -> BuiltChecks {
    let mut checks: HashMap<TableId, Vec<&'static crate::schema::CheckDef>> = HashMap::new();
    for def in crate::schema::registered_checks() {
        let id = TableId::of(def.table);
        if catalog.contains_key(&id) {
            checks.entry(id).or_default().push(def);
        }
    }
    let mut not_null: HashMap<TableId, Vec<&'static crate::schema::NotNullDef>> = HashMap::new();
    for def in crate::schema::registered_not_nulls() {
        let id = TableId::of(def.table);
        if catalog.contains_key(&id) {
            not_null.entry(id).or_default().push(def);
        }
    }
    (checks, not_null)
}

/// Resolve every `#[references]` declaration whose child table is assembled
/// (SPEC-022 RV-030/032), validating both ends: the parent table must be in
/// the same shard's schema, the referenced column must be the parent's
/// single-column primary key, and the child column's type must match it
/// (directly, or as `Option<parent type>`). Returns `(by child, by parent)`.
type BuiltFks = (
    HashMap<TableId, Vec<ResolvedFk>>,
    HashMap<TableId, Vec<ResolvedFk>>,
);

/// Whether a child row's referencing value matches the deleted parent key
/// (RV-032): direct equality, or `Some(parent)` for an `Option`-typed column.
fn fk_value_matches(value: Option<&RowValue>, parent: &RowValue) -> bool {
    match value {
        Some(RowValue::Optional(Some(inner))) => inner.as_ref() == parent,
        Some(other) => other == parent,
        None => false,
    }
}

fn build_foreign_keys(catalog: &HashMap<TableId, &'static TableSchema>) -> Result<BuiltFks> {
    let mut fks_out: HashMap<TableId, Vec<ResolvedFk>> = HashMap::new();
    let mut fks_in: HashMap<TableId, Vec<ResolvedFk>> = HashMap::new();
    for def in crate::schema::registered_foreign_keys() {
        let child = TableId::of(def.table);
        let Some(child_schema) = catalog.get(&child).copied() else {
            continue;
        };
        let parent = TableId::of(def.parent_table);
        let invalid = |detail: String| {
            FluxumError::query(
                codes::SCHEMA_INVALID,
                format!(
                    "table `{}` column `{}`: invalid `#[references({}({}))]`: {detail} (RV-030)",
                    def.table, def.column, def.parent_table, def.parent_column
                ),
            )
        };
        let parent_schema = catalog.get(&parent).copied().ok_or_else(|| {
            invalid(format!(
                "referenced table `{}` is not in the assembled schema",
                def.parent_table
            ))
        })?;
        let &[parent_pk_ord] = parent_schema.primary_key else {
            return Err(invalid(format!(
                "referenced table `{}` has a composite primary key — foreign keys \
                 target single-column primary keys only",
                def.parent_table
            )));
        };
        let parent_col = &parent_schema.columns[usize::from(parent_pk_ord)];
        if parent_col.name != def.parent_column {
            return Err(invalid(format!(
                "referenced column `{}` is not `{}`'s primary key (`{}`) — foreign \
                 keys target the parent's primary key",
                def.parent_column, def.parent_table, parent_col.name
            )));
        }
        let child_col = child_schema
            .columns
            .get(usize::from(def.ordinal))
            .ok_or_else(|| invalid(format!("column ordinal {} out of range", def.ordinal)))?;
        let child_ty = match &child_col.ty {
            crate::schema::FluxType::Option(inner) => *inner,
            other => other,
        };
        if *child_ty != parent_col.ty {
            return Err(invalid(format!(
                "type mismatch: `{}` is {:?} but `{}`.`{}` is {:?}",
                def.column, child_col.ty, def.parent_table, def.parent_column, parent_col.ty
            )));
        }
        if def.on_delete == crate::schema::RefAction::SetNull
            && !matches!(child_col.ty, crate::schema::FluxType::Option(_))
        {
            return Err(invalid(format!(
                "`on_delete = set_null` requires `{}` to be Option-typed",
                def.column
            )));
        }
        let resolved = ResolvedFk {
            child,
            child_schema,
            child_ordinal: def.ordinal,
            child_column: def.column,
            parent,
            parent_schema,
            on_delete: def.on_delete,
        };
        fks_out.entry(child).or_default().push(resolved);
        fks_in.entry(parent).or_default().push(resolved);
    }
    Ok((fks_out, fks_in))
}

/// The empty full-text indexes of `table`, one per `#[fulltext(...)]`
/// declaration, in declaration order (SPEC-019 FTS-001/010).
fn build_fulltext_indexes(table: &'static TableSchema) -> Vec<FullTextIndexState> {
    use crate::index::{Analyzer, Language};
    table
        .indexes
        .iter()
        .filter_map(|index| match index {
            IndexSchema::FullText {
                column,
                language,
                stop_words,
                stemming,
            } => {
                let analyzer = Analyzer {
                    language: match language {
                        crate::schema::FullTextLanguage::Simple => Language::Simple,
                        crate::schema::FullTextLanguage::English => Language::English,
                    },
                    stop_words: *stop_words,
                    stemming: *stemming,
                };
                Some(FullTextIndexState::new(*column, analyzer))
            }
            _ => None,
        })
        .collect()
}

/// Empty secondary B-tree indexes for `table`, keyed by stable [`IndexId`]
/// (STG-051), one per `#[index(btree(...))]` declaration. Spatial
/// declarations are handled by [`build_spatial_index`].
/// Apply one commit's blob reference deltas (DMX-040): incref every `Blob`
/// value in inserted rows, then unref every one in deleted rows (an update
/// contributes both sides). Write-time validation guarantees every incref
/// target exists; count bookkeeping errors are logged, never a commit
/// failure — the snapshot already swapped.
fn apply_blob_refcounts(
    catalog: &HashMap<TableId, &'static TableSchema>,
    blobs: &crate::commitlog::BlobStore,
    diffs: &[TableDiff],
) {
    use crate::commitlog::BlobHash;
    let hash_of = |value: Option<&RowValue>| match value {
        Some(RowValue::Blob(blob)) => Some(BlobHash::from_bytes(*blob.as_bytes())),
        _ => None,
    };
    for diff in diffs {
        let Some(schema) = catalog.get(&diff.table_id) else {
            continue;
        };
        let ordinals = blob_ordinals(schema);
        if ordinals.is_empty() {
            continue;
        }
        for row in &diff.inserts {
            for &ordinal in &ordinals {
                if let Some(hash) = hash_of(row.value(ordinal))
                    && let Err(e) = blobs.incref(&hash)
                {
                    tracing::error!(target: "fluxum::blob", error = %e, "blob incref failed");
                }
            }
        }
        for (_, row) in &diff.deletes {
            for &ordinal in &ordinals {
                if let Some(hash) = hash_of(row.value(ordinal))
                    && let Err(e) = blobs.unref(&hash)
                {
                    tracing::error!(target: "fluxum::blob", error = %e, "blob unref failed");
                }
            }
        }
    }
}

/// The ordinals of a schema's `Blob` columns (SPEC-023 DMX-040).
fn blob_ordinals(schema: &TableSchema) -> Vec<u16> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.ty, crate::schema::FluxType::Blob))
        .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
        .collect()
}

fn build_btree_indexes(
    table: &'static TableSchema,
    pager: &Arc<Pager>,
    table_id: TableId,
) -> Result<BTreeMap<IndexId, BTreeIndex>> {
    let mut indexes = BTreeMap::new();
    for index in table.indexes {
        let IndexSchema::BTree { columns } = index else {
            continue;
        };
        let mut names = Vec::with_capacity(columns.len());
        for &ordinal in *columns {
            let column = table.column(ordinal).ok_or_else(|| {
                FluxumError::Schema(format!(
                    "table `{}`: #[index(btree)] ordinal {ordinal} out of range (the \
                     registry should have rejected this schema)",
                    table.name
                ))
            })?;
            names.push(column.name);
        }
        let id = IndexId::of(table.name, &names);
        if indexes
            .insert(id, BTreeIndex::new(columns, pager, table_id)?)
            .is_some()
        {
            return Err(FluxumError::Schema(format!(
                "IndexId collision: two #[index(btree(...))] declarations on table `{}` \
                 hash to {id} (STG-051)",
                table.name
            )));
        }
    }
    Ok(indexes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, VisibilityRule};

    static ITEM_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "note",
            ty: FluxType::Str,
        },
    ];

    /// A minimal table schema literal (const so statics can be built from it).
    const fn table(
        name: &'static str,
        columns: &'static [ColumnSchema],
        auto_inc: Option<u16>,
        indexes: &'static [IndexSchema],
    ) -> TableSchema {
        TableSchema {
            name,
            columns,
            primary_key: &[0],
            auto_inc,
            access: TableAccess::Private,
            partition_by: None,
            unique: &[],
            indexes,
            visibility: VisibilityRule::PublicAll,
        }
    }

    static ITEM: TableSchema = table("CovItem", ITEM_COLS, None, &[]);

    fn item_store() -> (MemStore, TableId) {
        let schema = Schema::from_tables([&ITEM]).unwrap_or_else(|e| panic!("{e}"));
        let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
        let table = TableId::of("CovItem");
        (store, table)
    }

    fn item_values(id: u64, note: &str) -> Vec<RowValue> {
        vec![RowValue::U64(id), RowValue::Str(note.into())]
    }

    fn item_pk(id: u64) -> PkBytes {
        encode_pk_values(&ITEM, &[RowValue::U64(id)]).unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn a_zero_allocation_step_is_rejected() {
        let schema = Schema::from_tables([&ITEM]).unwrap_or_else(|e| panic!("{e}"));
        let err = match MemStore::with_options(
            &schema,
            StoreOptions {
                auto_inc_allocation_step: 0,
                ..StoreOptions::default()
            },
        ) {
            Ok(_) => panic!("step 0 accepted"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("must be >= 1 (STG-040)"), "{err}");
    }

    #[test]
    fn colliding_table_ids_are_rejected_with_both_names() {
        // "plumless" and "buckeroo" are the classic IEEE CRC32 collision.
        static COLS: &[ColumnSchema] = &[ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        }];
        static PLUMLESS: TableSchema = TableSchema {
            name: "plumless",
            columns: COLS,
            primary_key: &[0],
            auto_inc: None,
            access: TableAccess::Private,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        };
        static BUCKEROO: TableSchema = TableSchema {
            name: "buckeroo",
            ..PLUMLESS
        };
        assert_eq!(
            TableId::of("plumless"),
            TableId::of("buckeroo"),
            "collision precondition"
        );
        let schema = Schema::from_tables([&PLUMLESS, &BUCKEROO]).unwrap_or_else(|e| panic!("{e}"));
        let err = match MemStore::new(&schema) {
            Ok(_) => panic!("colliding table ids accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("TableId collision"), "{err}");
        assert!(
            err.contains("plumless") && err.contains("buckeroo"),
            "{err}"
        );
        assert!(err.contains("STG-050"), "{err}");
    }

    #[test]
    fn install_recovered_rejects_tx_id_zero_and_wrong_coverage() {
        let (store, _) = item_store();
        let good = (*store.snapshot().state).clone();
        let err = match store.install_recovered(good, 0) {
            Ok(()) => panic!("tx id 0 accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("next_tx_id must be >= 1"), "{err}");

        let empty = CommittedState::detached(HashMap::new());
        let err = match store.install_recovered(empty, 5) {
            Ok(()) => panic!("empty recovered state accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("does not cover exactly"), "{err}");
    }

    #[test]
    fn a_delete_op_without_a_committed_row_is_an_invariant_breach() {
        let (store, table) = item_store();
        let mut tx = store.begin();
        // Corrupt TxState directly: a Delete op for a pk that was never
        // committed (the public delete() path cannot produce this).
        tx.state
            .tables
            .entry(table)
            .or_default()
            .insert(item_pk(1), PendingOp::Delete);
        let err = match tx.insert(table, item_values(1, "x")) {
            Ok(_) => panic!("insert over a phantom Delete op succeeded"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Delete op for pk absent from CommittedState"),
            "{err}"
        );
    }

    #[test]
    fn commit_reports_ops_for_a_table_missing_from_committed_state() {
        let (store, _) = item_store();
        let mut tx = store.begin();
        tx.state
            .tables
            .entry(TableId::from_raw(0xDEAD_BEEF))
            .or_default()
            .insert(item_pk(1), PendingOp::Delete);
        let err = match tx.commit() {
            Ok(_) => panic!("commit over an unknown table succeeded"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("touched table") && err.contains("missing from"),
            "{err}"
        );
    }

    #[test]
    fn commit_reports_a_delete_op_for_a_missing_committed_row() {
        let (store, table) = item_store();
        let mut tx = store.begin();
        tx.state
            .tables
            .entry(table)
            .or_default()
            .insert(item_pk(7), PendingOp::Delete);
        let err = match tx.commit() {
            Ok(_) => panic!("commit of a phantom Delete succeeded"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Delete/Update op for pk absent from CommittedState"),
            "{err}"
        );
    }

    #[test]
    fn commit_reports_a_dirty_auto_inc_table_missing_from_committed_state() {
        let (store, _) = item_store();
        let mut tx = store.begin();
        tx.writer.high_water_dirty.insert(TableId::from_raw(0xBEEF));
        let err = match tx.commit() {
            Ok(_) => panic!("commit with a bogus dirty auto-inc table succeeded"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("auto-inc table") && err.contains("missing from"),
            "{err}"
        );
    }

    #[test]
    fn assign_auto_inc_rejects_a_non_u64_auto_inc_column() {
        // A schema whose #[auto_inc] ordinal points at a Str column — the
        // registry rejects this (DM-004); the writer still guards it.
        static BAD: TableSchema = TableSchema {
            name: "CovBadAutoInc",
            columns: ITEM_COLS,
            primary_key: &[0],
            auto_inc: Some(1),
            access: TableAccess::Private,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        };
        let (store, table) = item_store();
        let mut tx = store.begin();
        let mut values = item_values(1, "not-a-counter");
        let err = match tx.assign_auto_inc(table, &BAD, &mut values) {
            Ok(()) => panic!("Str auto-inc column accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("#[auto_inc] column must be u64"), "{err}");
        assert!(err.contains("Str"), "{err}");
    }

    #[test]
    fn migrate_rows_sees_replacements_and_skips_deletes_and_inserts() {
        let (store, table) = item_store();
        let mut tx = store.begin();
        for id in 1..=3u64 {
            tx.insert(table, item_values(id, "old"))
                .unwrap_or_else(|e| panic!("{e}"));
        }
        tx.commit().unwrap_or_else(|e| panic!("{e}"));

        let mut tx = store.begin();
        tx.migrate_replace(table, item_pk(2), item_values(2, "rewritten"))
            .unwrap_or_else(|e| panic!("{e}"));
        tx.delete(table, &[RowValue::U64(3)])
            .unwrap_or_else(|e| panic!("{e}"));
        tx.insert(table, item_values(4, "fresh"))
            .unwrap_or_else(|e| panic!("{e}"));

        let rows = tx.migrate_rows(table).unwrap_or_else(|e| panic!("{e}"));
        let ids: Vec<(PkBytes, RowValue)> = rows
            .iter()
            .map(|(pk, row)| (pk.clone(), row.values()[1].clone()))
            .collect();
        assert_eq!(
            ids,
            vec![
                (item_pk(1), RowValue::Str("old".into())),
                (item_pk(2), RowValue::Str("rewritten".into())),
            ],
            "replacements compose; deletes and fresh inserts are excluded"
        );
    }

    #[test]
    fn migrate_replace_requires_a_committed_row() {
        let (store, table) = item_store();
        let mut tx = store.begin();
        let err = match tx.migrate_replace(table, item_pk(99), item_values(99, "x")) {
            Ok(()) => panic!("migrate_replace of an absent pk succeeded"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("has no committed row at pk"), "{err}");
        assert!(err.contains("SPEC-010"), "{err}");
    }

    // --- spatial surface -------------------------------------------------

    static SPOT_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "x",
            ty: FluxType::F64,
        },
        ColumnSchema {
            name: "y",
            ty: FluxType::F64,
        },
    ];

    static SPOT: TableSchema = TableSchema {
        name: "CovSpot",
        columns: SPOT_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Private,
        partition_by: None,
        unique: &[],
        indexes: &[IndexSchema::Spatial {
            kind: SpatialKind::QuadTree,
            columns: &[1, 2],
        }],
        visibility: VisibilityRule::PublicAll,
    };

    #[test]
    fn index_id_ignores_spatial_declarations() {
        let schema = Schema::from_tables([&SPOT]).unwrap_or_else(|e| panic!("{e}"));
        let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
        // The spatial declaration over (x, y) is not a B-tree index.
        assert_eq!(store.index_id("CovSpot", &["x", "y"]), None);
        assert_eq!(store.index_id("CovSpot", &["x"]), None);
    }

    #[test]
    fn tx_spatial_queries_read_the_committed_snapshot() {
        let schema = Schema::from_tables([&SPOT]).unwrap_or_else(|e| panic!("{e}"));
        let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
        let table = TableId::of("CovSpot");
        let mut tx = store.begin();
        tx.insert(
            table,
            vec![RowValue::U64(1), RowValue::F64(1.0), RowValue::F64(2.0)],
        )
        .unwrap_or_else(|e| panic!("{e}"));
        tx.insert(
            table,
            vec![RowValue::U64(2), RowValue::F64(50.0), RowValue::F64(50.0)],
        )
        .unwrap_or_else(|e| panic!("{e}"));
        tx.commit().unwrap_or_else(|e| panic!("{e}"));

        let tx = store.begin();
        let near = tx
            .spatial_region(table, Rect::new(0.0, 0.0, 10.0, 10.0))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(near.len(), 1);
        assert_eq!(near[0].values()[0], RowValue::U64(1));
        let far = tx
            .spatial_radius(table, 50.0, 50.0, 1.0)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(far.len(), 1);
        assert_eq!(far[0].values()[0], RowValue::U64(2));
    }

    // --- schema-shape guards over the index builders ----------------------

    #[test]
    fn a_second_spatial_declaration_is_rejected() {
        static DOUBLE: TableSchema = TableSchema {
            indexes: &[
                IndexSchema::Spatial {
                    kind: SpatialKind::QuadTree,
                    columns: &[1, 2],
                },
                IndexSchema::Spatial {
                    kind: SpatialKind::RTree,
                    columns: &[1, 2, 1, 2],
                },
            ],
            ..SPOT
        };
        let dir = tempfile::Builder::new()
            .prefix("fluxum-spx-build-")
            .tempdir()
            .unwrap_or_else(|e| panic!("{e}"));
        let pager = Pager::open(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 64 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: crate::config::PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let err = match build_spatial_index(
            &DOUBLE,
            &StoreOptions::default(),
            &pager,
            TableId::of("Spot"),
        ) {
            Ok(_) => panic!("two spatial declarations accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("multiple #[spatial(...)]"), "{err}");
        assert!(err.contains("SPEC-008"), "{err}");
    }

    #[test]
    fn btree_index_builders_guard_ordinals_and_id_collisions() {
        let dir = tempfile::Builder::new()
            .prefix("fluxum-idx-build-")
            .tempdir()
            .unwrap_or_else(|e| panic!("{e}"));
        let pager = Pager::open(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 64 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: crate::config::PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let table_id = TableId::of("Item");

        static OUT_OF_RANGE: TableSchema = TableSchema {
            indexes: &[IndexSchema::BTree { columns: &[9] }],
            ..ITEM
        };
        let err = match build_btree_indexes(&OUT_OF_RANGE, &pager, table_id) {
            Ok(_) => panic!("out-of-range index ordinal accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("ordinal 9 out of range"), "{err}");

        static DUPLICATED: TableSchema = TableSchema {
            indexes: &[
                IndexSchema::BTree { columns: &[1] },
                IndexSchema::BTree { columns: &[1] },
            ],
            ..ITEM
        };
        let err = match build_btree_indexes(&DUPLICATED, &pager, table_id) {
            Ok(_) => panic!("duplicate index declarations accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("IndexId collision"), "{err}");
        assert!(err.contains("STG-051"), "{err}");
    }
}
