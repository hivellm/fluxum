//! [`MemStore`] — CommittedState + single-writer transactions (STG-001,
//! STG-003..STG-007, STG-040). See [`super`] for the design decisions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arc_swap::ArcSwap;

use crate::error::{FluxumError, Result};
use crate::index::{BTreeIndex, IndexId, Rect, SpatialIndexState, SpatialPredicate};
use crate::schema::{IndexSchema, Schema, SpatialKind, TableSchema};
use crate::store::TableId;
use crate::store::committed::{CommittedState, Snapshot, TableState};
use crate::store::row::{
    PkBytes, Row, RowValue, check_row, display_pk_of_row, encode_pk_of_row, encode_pk_values,
};
use crate::store::tx::{PendingOp, TableDiff, TxDiff, TxState};
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
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            auto_inc_allocation_step: 4096,
            spatial_bucket_size: 8,
            // ±2^20 covers typical projected-coordinate workloads; out-of-
            // bounds rows stay correct via the overflow bucket (SPX-004).
            spatial_bounds: Rect::new(-1_048_576.0, -1_048_576.0, 2_097_152.0, 2_097_152.0),
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
    /// The stable snapshot, swapped atomically on commit (STG-005).
    committed: ArcSwap<CommittedState>,
    /// Single-writer guarantee (STG-003): `begin` holds this for the whole
    /// transaction.
    writer: Mutex<WriterState>,
    options: StoreOptions,
}

impl MemStore {
    /// Build a store over an assembled [`Schema`] with default options.
    pub fn new(schema: &Schema) -> Result<Self> {
        Self::with_options(schema, StoreOptions::default())
    }

    /// Build a store over an assembled [`Schema`].
    ///
    /// Fails on invalid options or a `TableId` collision (two table names
    /// hashing to the same CRC32, STG-050 — must be renamed).
    pub fn with_options(schema: &Schema, options: StoreOptions) -> Result<Self> {
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
            tables.insert(
                id,
                Arc::new(TableState {
                    schema: table,
                    rows: BTreeMap::new(),
                    indexes: build_btree_indexes(table)?,
                    spatial: build_spatial_index(table, &options)?,
                    unique: table.unique.iter().map(|c| UniqueIndex::new(c)).collect(),
                    auto_inc_high_water: 0,
                }),
            );
            if table.auto_inc.is_some() {
                counters.insert(id, AutoIncCounter::new());
            }
        }
        Ok(Self {
            catalog,
            committed: ArcSwap::from_pointee(CommittedState { tables }),
            writer: Mutex::new(WriterState {
                next_tx_id: 1,
                counters,
                high_water_dirty: BTreeSet::new(),
            }),
            options,
        })
    }

    /// The [`TableId`] of a registered table name.
    pub fn table_id(&self, name: &str) -> Option<TableId> {
        let id = TableId::of(name);
        self.catalog.contains_key(&id).then_some(id)
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
    pub fn mark_spatial_rebuilding(&self) {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            if let Some(spatial) = &slot.spatial {
                let rebuilding = spatial.rebuilding_like();
                Arc::make_mut(slot).spatial = Some(rebuilding);
            }
        }
        self.committed.store(Arc::new(CommittedState { tables }));
    }

    /// SPX-031, step 2: rebuild every spatial index from the committed rows
    /// and publish the ready state atomically. After this returns, spatial
    /// query results are identical to a never-crashed store's.
    pub fn rebuild_spatial_indexes(&self) -> Result<()> {
        let _writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tables = self.committed.load_full().tables.clone();
        for slot in tables.values_mut() {
            let Some(spatial) = &slot.spatial else {
                continue;
            };
            let mut rebuilt = spatial.fresh_like();
            for (pk, row) in &slot.rows {
                rebuilt.insert_row(row, pk.clone())?;
            }
            Arc::make_mut(slot).spatial = Some(rebuilt);
        }
        self.committed.store(Arc::new(CommittedState { tables }));
        Ok(())
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
        self.committed.store(Arc::new(state));
        Ok(())
    }

    fn schema_of(&self, table: TableId) -> Result<&'static TableSchema> {
        self.catalog.get(&table).copied().ok_or_else(|| {
            FluxumError::Storage(format!(
                "unknown table id {table}: not in the assembled schema"
            ))
        })
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
        check_row(schema, &values)?;
        // SPX-010 eager constraint (like PK uniqueness below): an R-tree row
        // must satisfy min <= max per axis, so the commit merge stays
        // infallible.
        if let Some(spatial) = &self.base.table(table)?.spatial {
            spatial.check_insert(schema.name, &values)?;
        }
        self.assign_auto_inc(table, schema, &mut values)?;
        let pk = encode_pk_of_row(schema, &values)?;

        let committed_row = self.base.table(table)?.rows.get(&pk).cloned();
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

        let ops = self.state.tables.entry(table).or_default();
        match pending {
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
                    Ok(committed)
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    Ok(row)
                }
            }
            // Upsert over this transaction's own pending write: the pending
            // row is replaced in place, keeping its Insert/Update flavor
            // (the flavor records whether a *committed* row underlies the
            // key, which has not changed).
            Some(PendingOp::Insert(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Insert(row.clone()));
                Ok(row)
            }
            Some(PendingOp::Update(_)) => {
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Update(row.clone()));
                Ok(row)
            }
            None => {
                if let Some(committed) = committed_row {
                    // Upsert over a committed row (TXN-040 exception).
                    if committed.values() == values.as_slice() {
                        // Identical content: structural no-op, committed row
                        // identity preserved (STG-007 rule 1).
                        return Ok(committed);
                    }
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Update(row.clone()));
                    Ok(row)
                } else {
                    let row = Row::new(values);
                    ops.insert(pk, PendingOp::Insert(row.clone()));
                    Ok(row)
                }
            }
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
            if let Some(owner) = constraint.owner(&key) {
                let shadowed = owner == pk
                    || ops
                        .and_then(|m| m.get(owner))
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
    pub fn delete(&mut self, table: TableId, pk_values: &[RowValue]) -> Result<bool> {
        let schema = self.store.schema_of(table)?;
        let pk = encode_pk_values(schema, pk_values)?;
        let committed_has = self.base.table(table)?.rows.contains_key(&pk);
        let ops = self.state.tables.entry(table).or_default();
        match ops.get(&pk) {
            // Insert-then-delete of a pending row cancels to a no-op.
            Some(PendingOp::Insert(_)) => {
                ops.remove(&pk);
                Ok(true)
            }
            // The committed row stays deleted; the pending replacement dies.
            Some(PendingOp::Update(_)) => {
                ops.insert(pk, PendingOp::Delete);
                Ok(true)
            }
            Some(PendingOp::Delete) => Ok(false),
            None => {
                if committed_has {
                    ops.insert(pk, PendingOp::Delete);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Point lookup against the committed snapshot captured at `begin`
    /// (STG-004: pending writes of this transaction are never visible).
    pub fn query_pk(&self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let t = self.base.table(table)?;
        let pk = encode_pk_values(t.schema, pk_values)?;
        Ok(t.rows.get(&pk).cloned())
    }

    /// Scan the committed snapshot captured at `begin`, in encoded-PK byte
    /// order (STG-004: pending writes are never visible).
    pub fn scan(&self, table: TableId) -> Result<impl Iterator<Item = &Row>> {
        Ok(self.base.table(table)?.rows.values())
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
        Ok(base
            .rows
            .iter()
            .filter_map(|(pk, row)| {
                let effective = match ops.and_then(|pending| pending.get(pk)) {
                    Some(PendingOp::Update(replacement)) => replacement.clone(),
                    Some(PendingOp::Delete | PendingOp::Insert(_)) => return None,
                    None => row.clone(),
                };
                Some((pk.clone(), effective))
            })
            .collect())
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
        if !self.base.table(table)?.rows.contains_key(&pk) {
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
    ) -> Result<impl Iterator<Item = &Row>> {
        self.base
            .table(table)?
            .index_scan(index, prefix, lower, upper)
    }

    /// Equality lookup on a B-tree index of the committed snapshot captured
    /// at `begin` (see [`super::Snapshot::index_eq`]).
    pub fn index_eq(
        &self,
        table: TableId,
        index: IndexId,
        key: &[RowValue],
    ) -> Result<impl Iterator<Item = &Row>> {
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
    pub fn scan_all(&self, table: TableId) -> Result<impl Iterator<Item = &Row>> {
        let committed = &self.base.table(table)?.rows;
        let pending = self.state.tables.get(&table);
        let overlaid = committed.iter().filter_map(move |(pk, row)| {
            match pending.and_then(|ops| ops.get(pk)) {
                None => Some(row),
                Some(PendingOp::Update(replacement)) => Some(replacement),
                // Delete: shadowed. Insert over a committed key cannot
                // happen (the write path turns it into Update/Delete), but
                // yielding it from the pending pass below keeps the
                // dedup-by-PK contract even then.
                Some(PendingOp::Delete | PendingOp::Insert(_)) => None,
            }
        });
        let inserted = pending.into_iter().flat_map(|ops| {
            ops.values().filter_map(|op| match op {
                PendingOp::Insert(row) => Some(row),
                PendingOp::Update(_) | PendingOp::Delete => None,
            })
        });
        Ok(overlaid.chain(inserted))
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
                    let old = table.rows.get(pk).ok_or_else(invariant_missing_row)?;
                    for constraint in &mut table.unique {
                        constraint.remove(old, pk)?;
                    }
                }
            }
            // Rows and secondary indexes are updated together on this
            // private pre-swap copy (STG-005 steps 2–4), so the published
            // snapshot's rows and indexes are mutually consistent and
            // rollback never has index state to revert (STG-007 rule 2).
            for (pk, op) in ops {
                match op {
                    PendingOp::Insert(row) => {
                        for index in table.indexes.values_mut() {
                            index.insert(&row, pk.clone())?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.insert_row(&row, pk.clone())?;
                        }
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone())?;
                        }
                        table.rows.insert(pk, row.clone());
                        diff.inserts.push(row);
                    }
                    PendingOp::Delete => {
                        let old = table.rows.remove(&pk).ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk)?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            spatial.remove_row(&old, &pk)?;
                        }
                        diff.deletes.push((pk, old));
                    }
                    PendingOp::Update(row) => {
                        let old = table
                            .rows
                            .insert(pk.clone(), row.clone())
                            .ok_or_else(invariant_missing_row)?;
                        for index in table.indexes.values_mut() {
                            index.remove(&old, &pk)?;
                            index.insert(&row, pk.clone())?;
                        }
                        if let Some(spatial) = &mut table.spatial {
                            // SPX-032: old coordinates out, new coordinates
                            // in — atomic with the row swap on this private
                            // pre-swap copy, so no stale entry can publish.
                            spatial.remove_row(&old, &pk)?;
                            spatial.insert_row(&row, pk.clone())?;
                        }
                        // The old row's unique values were released in the
                        // two-pass removal above; claim the new row's.
                        for constraint in &mut table.unique {
                            constraint.insert(&row, pk.clone())?;
                        }
                        diff.deletes.push((pk, old));
                        diff.inserts.push(row);
                    }
                }
            }
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

        // 3. Consume the tx id and swap the snapshot — atomic for readers.
        let tx_id = self.state.tx_id;
        self.writer.next_tx_id = tx_id.saturating_add(1);
        self.store
            .committed
            .store(Arc::new(CommittedState { tables }));

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
            ),
            SpatialKind::RTree => SpatialIndexState::rtree(columns, options.spatial_bucket_size),
        });
    }
    Ok(spatial)
}

/// Empty secondary B-tree indexes for `table`, keyed by stable [`IndexId`]
/// (STG-051), one per `#[index(btree(...))]` declaration. Spatial
/// declarations are handled by [`build_spatial_index`].
fn build_btree_indexes(table: &'static TableSchema) -> Result<BTreeMap<IndexId, BTreeIndex>> {
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
        if indexes.insert(id, BTreeIndex::new(columns)).is_some() {
            return Err(FluxumError::Schema(format!(
                "IndexId collision: two #[index(btree(...))] declarations on table `{}` \
                 hash to {id} (STG-051)",
                table.name
            )));
        }
    }
    Ok(indexes)
}
