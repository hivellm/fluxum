//! [`MemStore`] — CommittedState + single-writer transactions (STG-001,
//! STG-003..STG-007, STG-040). See [`super`] for the design decisions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arc_swap::ArcSwap;

use crate::error::{FluxumError, Result};
use crate::schema::{Schema, TableSchema};
use crate::store::TableId;
use crate::store::committed::{CommittedState, Snapshot, TableState};
use crate::store::row::{
    Row, RowValue, check_row, display_pk_of_row, encode_pk_of_row, encode_pk_values,
};
use crate::store::tx::{PendingOp, TableDiff, TxDiff, TxState};

/// Tuning knobs for a [`MemStore`] (SPEC-002 §8; wired into `config.yml`
/// with the server assembly).
#[derive(Debug, Clone, Copy)]
pub struct StoreOptions {
    /// STG-040 batched allocation step: how far the auto-inc high-water mark
    /// advances per durable allocation (`auto_inc_allocation_step`,
    /// default 4096). Must be ≥ 1.
    pub auto_inc_allocation_step: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            auto_inc_allocation_step: 4096,
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

    /// A lock-free, consistent point-in-time view of the committed state
    /// (STG-004, FR-10). Never blocks, regardless of writer activity.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: self.committed.load_full(),
        }
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
    /// Enforces PK uniqueness eagerly against the STG-007 overlay: committed
    /// rows tx-deleted in this transaction do not conflict; pending inserts
    /// do. For `#[auto_inc]` tables, a `0` placeholder in the auto-inc
    /// column is replaced with the next counter value (TXN-042: assigned at
    /// insert time); an explicit non-zero id is kept and the counter jumps
    /// past it. Returns the row as stored, with the assigned id.
    pub fn insert(&mut self, table: TableId, mut values: Vec<RowValue>) -> Result<Row> {
        let schema = self.store.schema_of(table)?;
        check_row(schema, &values)?;
        self.assign_auto_inc(table, schema, &mut values)?;
        let pk = encode_pk_of_row(schema, &values)?;

        let committed_row = self.base.table(table)?.rows.get(&pk).cloned();
        let ops = self.state.tables.entry(table).or_default();
        let conflict = || {
            Err(FluxumError::Storage(format!(
                "primary key conflict: table={} pk={}",
                schema.name,
                display_pk_of_row(schema, &values)
            )))
        };
        match ops.get(&pk) {
            // Overlay rule: a pending insert (or reinsert) occupies the key.
            Some(PendingOp::Insert(_) | PendingOp::Update(_)) => conflict(),
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
            None => {
                if committed_row.is_some() {
                    return conflict();
                }
                let row = Row::new(values);
                ops.insert(pk, PendingOp::Insert(row.clone()));
                Ok(row)
            }
        }
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
            for (pk, op) in ops {
                match op {
                    PendingOp::Insert(row) => {
                        table.rows.insert(pk, row.clone());
                        diff.inserts.push(row);
                    }
                    PendingOp::Delete => {
                        let old = table.rows.remove(&pk).ok_or_else(invariant_missing_row)?;
                        diff.deletes.push((pk, old));
                    }
                    PendingOp::Update(row) => {
                        let old = table
                            .rows
                            .insert(pk.clone(), row.clone())
                            .ok_or_else(invariant_missing_row)?;
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
