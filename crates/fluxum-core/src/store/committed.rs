//! [`CommittedState`] — the stable, atomically swapped snapshot (STG-002),
//! and [`Snapshot`], the lock-free reader handle over it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::schema::TableSchema;
use crate::store::TableId;
use crate::store::row::{PkBytes, Row, RowValue, encode_pk_values};

/// One table's committed contents (STG-002).
///
/// `rows` is the logical primary map (BTreeMap for O(log n) point lookup and
/// deterministic iteration). Secondary/spatial index structures join this
/// struct in T2.4; the rollback hook for their eager maintenance already
/// exists ([`super::UndoRecord`]).
#[derive(Debug, Clone)]
pub struct TableState {
    /// The table's link-time schema.
    pub(crate) schema: &'static TableSchema,
    /// Primary row map, keyed by FluxBIN-encoded PK.
    pub(crate) rows: BTreeMap<PkBytes, Row>,
    /// Durable auto-inc high-water mark (STG-040): every value ≤ this has
    /// been covered by a batch allocation that a committed [`super::TxDiff`]
    /// carried (T2.2 persists it through the commit log).
    pub(crate) auto_inc_high_water: u64,
}

impl TableState {
    /// The table's schema.
    pub fn schema(&self) -> &'static TableSchema {
        self.schema
    }

    /// Number of committed rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// The stable committed snapshot: one entry per registered table (STG-002).
///
/// Immutable once published — commits build a new `CommittedState` (sharing
/// untouched tables via `Arc`) and atomically swap the root pointer, so no
/// reader ever observes a partial transaction (STG-005).
#[derive(Debug, Clone)]
pub struct CommittedState {
    pub(crate) tables: HashMap<TableId, Arc<TableState>>,
}

impl CommittedState {
    /// The state of table `id`, or an unknown-table error.
    pub(crate) fn table(&self, id: TableId) -> Result<&Arc<TableState>> {
        self.tables.get(&id).ok_or_else(|| {
            FluxumError::Storage(format!(
                "unknown table id {id}: not in the assembled schema"
            ))
        })
    }
}

/// A consistent, lock-free point-in-time view of [`CommittedState`]
/// (STG-004, FR-10).
///
/// Obtained wait-free from [`super::MemStore::snapshot`]; holding it pins
/// this exact state — commits that land afterwards are invisible, which is
/// exactly the TXN-061 view-isolation contract.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub(crate) state: Arc<CommittedState>,
}

impl Snapshot {
    /// Point lookup by primary key values (in `primary_key` declaration
    /// order; one value for simple PKs, N for composite).
    pub fn query_pk(&self, table: TableId, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let t = self.state.table(table)?;
        let pk = encode_pk_values(t.schema, pk_values)?;
        Ok(t.rows.get(&pk).cloned())
    }

    /// Iterate all committed rows of `table` in encoded-PK byte order.
    pub fn scan(&self, table: TableId) -> Result<impl Iterator<Item = &Row>> {
        Ok(self.state.table(table)?.rows.values())
    }

    /// Number of committed rows in `table`.
    pub fn row_count(&self, table: TableId) -> Result<usize> {
        Ok(self.state.table(table)?.rows.len())
    }

    /// The durable auto-inc high-water mark of `table` (STG-040).
    pub fn auto_inc_high_water(&self, table: TableId) -> Result<u64> {
        Ok(self.state.table(table)?.auto_inc_high_water)
    }

    /// Whether this snapshot and `other` are the same published state
    /// (pointer identity — used by tests to prove rollback restored the
    /// prior state *exactly*, STG-007).
    pub fn same_state(&self, other: &Snapshot) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}
