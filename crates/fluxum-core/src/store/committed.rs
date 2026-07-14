//! [`CommittedState`] — the stable, atomically swapped snapshot (STG-002),
//! and [`Snapshot`], the lock-free reader handle over it.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::index::btree::{self, BTreeIndex};
use crate::index::{IndexId, Rect, SpatialIndexState};
use crate::schema::TableSchema;
use crate::store::TableId;
use crate::store::row::{PkBytes, Row, RowValue, encode_pk_values};

/// One table's committed contents (STG-002).
///
/// `rows` is the logical primary map (BTreeMap for O(log n) point lookup and
/// deterministic iteration); `indexes` the secondary B-tree indexes (T2.4)
/// and `spatial` the SPEC-008 spatial index (T2.5), all maintained together
/// with `rows` inside the commit merge so a published snapshot's rows and
/// indexes are always mutually consistent.
#[derive(Debug, Clone)]
pub struct TableState {
    /// The table's link-time schema.
    pub(crate) schema: &'static TableSchema,
    /// Primary row map, keyed by FluxBIN-encoded PK.
    pub(crate) rows: BTreeMap<PkBytes, Row>,
    /// Secondary B-tree indexes by stable id (STG-051), one per
    /// `#[index(btree(...))]` declaration (DM-030/DM-031).
    pub(crate) indexes: BTreeMap<IndexId, BTreeIndex>,
    /// The `#[spatial(...)]` index, if declared (SPEC-008, SPX-030).
    pub(crate) spatial: Option<SpatialIndexState>,
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

    /// Scan a B-tree index of this table: equality on `prefix` (0..=all
    /// index columns, in declared key order), then `lower`/`upper` bounds
    /// over the next index column. See [`Snapshot::index_scan`].
    pub(crate) fn index_scan(
        &self,
        index_id: IndexId,
        prefix: &[RowValue],
        lower: Bound<&RowValue>,
        upper: Bound<&RowValue>,
    ) -> Result<impl Iterator<Item = &Row>> {
        let index = self.indexes.get(&index_id).ok_or_else(|| {
            FluxumError::Storage(format!(
                "unknown index {index_id} on table `{}`",
                self.schema.name
            ))
        })?;
        let columns = index.columns();
        if prefix.len() > columns.len() {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index {index_id} scan prefix has {} value(s) but the index \
                 has {} column(s)",
                self.schema.name,
                prefix.len(),
                columns.len()
            )));
        }
        let mut prefix_bytes = Vec::new();
        for (value, &ordinal) in prefix.iter().zip(columns) {
            btree::encode_value(
                self.check_index_value(index_id, ordinal, value)?,
                &mut prefix_bytes,
            );
        }
        let ranged = !matches!((&lower, &upper), (Bound::Unbounded, Bound::Unbounded));
        if ranged && prefix.len() == columns.len() {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index {index_id} scan has range bounds but the equality prefix \
                 already covers all {} index column(s)",
                self.schema.name,
                columns.len()
            )));
        }
        let range_ordinal = columns.get(prefix.len()).copied();
        let lower = self.encode_bound(index_id, range_ordinal, lower)?;
        let upper = self.encode_bound(index_id, range_ordinal, upper)?;
        let (start, end) = btree::plan_scan(prefix_bytes, lower, upper);
        Ok(index.scan_pks(start, end).filter_map(move |pk| {
            let row = self.rows.get(pk);
            debug_assert!(
                row.is_some(),
                "index {index_id} points at a pk absent from the row map"
            );
            row
        }))
    }

    /// Type-check `value` against the index column at `ordinal`.
    fn check_index_value<'v>(
        &self,
        index_id: IndexId,
        ordinal: u16,
        value: &'v RowValue,
    ) -> Result<&'v RowValue> {
        let column = self.schema.column(ordinal).ok_or_else(|| {
            FluxumError::Storage(format!(
                "internal invariant violated: index {index_id} ordinal {ordinal} out of \
                 range for table `{}`",
                self.schema.name
            ))
        })?;
        if !value.matches_type(&column.ty) {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index column `{}` expects {:?}, got {value}",
                self.schema.name, column.name, column.ty
            )));
        }
        Ok(value)
    }

    /// Memcomparable-encode one scan bound over the index column at
    /// `range_ordinal`.
    fn encode_bound(
        &self,
        index_id: IndexId,
        range_ordinal: Option<u16>,
        bound: Bound<&RowValue>,
    ) -> Result<Bound<Vec<u8>>> {
        let encode = |value: &RowValue| -> Result<Vec<u8>> {
            let ordinal = range_ordinal.ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: range bound without a range column \
                     (index {index_id}, table `{}`)",
                    self.schema.name
                ))
            })?;
            let mut bytes = Vec::new();
            btree::encode_value(
                self.check_index_value(index_id, ordinal, value)?,
                &mut bytes,
            );
            Ok(bytes)
        };
        Ok(match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(encode(value)?),
            Bound::Excluded(value) => Bound::Excluded(encode(value)?),
        })
    }

    /// The table's spatial index, or the SPX-022 error when none is
    /// declared.
    pub(crate) fn spatial(&self) -> Result<&SpatialIndexState> {
        self.spatial.as_ref().ok_or_else(|| {
            FluxumError::Storage(format!("table '{}' has no spatial index", self.schema.name))
        })
    }

    /// Resolve index-returned PKs to rows (spatial indexes and the row map
    /// of one snapshot are mutually consistent, so every PK resolves).
    fn rows_of(&self, pks: Vec<PkBytes>) -> Vec<Row> {
        pks.iter()
            .filter_map(|pk| {
                let row = self.rows.get(pk);
                debug_assert!(
                    row.is_some(),
                    "spatial index points at a pk absent from the row map"
                );
                row.cloned()
            })
            .collect()
    }

    /// Rows inside `region` via the spatial index (SPX-020). Never a scan.
    pub(crate) fn spatial_region(&self, region: Rect) -> Result<Vec<Row>> {
        Ok(self.rows_of(self.spatial()?.query_region(region)))
    }

    /// Rows within Euclidean distance `r` of `(x, y)` via the spatial index
    /// (SPX-021): bbox prefilter + exact circle filter, distance exactly `r`
    /// included.
    pub(crate) fn spatial_radius(&self, x: f64, y: f64, r: f64) -> Result<Vec<Row>> {
        Ok(self.rows_of(self.spatial()?.query_radius(x, y, r)))
    }

    /// Rows at exactly `(x, y)` (IEEE `==`) via the spatial index.
    pub(crate) fn spatial_point(&self, x: f64, y: f64) -> Result<Vec<Row>> {
        Ok(self.rows_of(self.spatial()?.query_point(x, y)))
    }

    /// STG-007 rule 2 check: every secondary index equals (bit-identically)
    /// a freshly built index over this table's committed rows.
    pub(crate) fn verify_index_integrity(&self, table_id: TableId) -> Result<()> {
        for (index_id, index) in &self.indexes {
            let mut rebuilt = BTreeIndex::new(index.columns());
            for (pk, row) in &self.rows {
                rebuilt.insert(row, pk.clone())?;
            }
            if rebuilt != *index {
                return Err(FluxumError::Storage(format!(
                    "index {index_id} on table `{}` ({table_id}) diverged from a fresh \
                     rebuild over CommittedState (STG-007)",
                    self.schema.name
                )));
            }
        }
        if let Some(spatial) = &self.spatial {
            let mut rebuilt = spatial.fresh_like();
            for (pk, row) in &self.rows {
                rebuilt.insert_row(row, pk.clone())?;
            }
            if rebuilt != *spatial {
                return Err(FluxumError::Storage(format!(
                    "spatial index on table `{}` ({table_id}) diverged from a fresh rebuild \
                     over CommittedState (STG-007, SPX-030)",
                    self.schema.name
                )));
            }
        }
        Ok(())
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

    /// Scan a B-tree index (DM-030/DM-031), lock-free over this snapshot.
    ///
    /// `prefix` gives equality values for the leading index columns (0..=all,
    /// in declared key order); `lower`/`upper` bound the *next* index column.
    /// Rows come back in index order — ascending by the indexed values
    /// (memcomparable order == natural value order), then by encoded PK
    /// within one index key. Scan shapes:
    ///
    /// - point lookup: full `prefix`, both bounds `Unbounded`;
    /// - range scan: empty `prefix`, bounds over the first column;
    /// - composite prefix scan (DM-031): equality prefix + bounds over the
    ///   following column (e.g. `channel` equality, `sent_at` range).
    pub fn index_scan(
        &self,
        table: TableId,
        index: IndexId,
        prefix: &[RowValue],
        lower: Bound<&RowValue>,
        upper: Bound<&RowValue>,
    ) -> Result<impl Iterator<Item = &Row>> {
        self.state
            .table(table)?
            .index_scan(index, prefix, lower, upper)
    }

    /// Equality lookup on a B-tree index: all rows whose leading index
    /// columns equal `key` (a full key for point lookups, a shorter prefix
    /// for DM-031 prefix equality).
    pub fn index_eq(
        &self,
        table: TableId,
        index: IndexId,
        key: &[RowValue],
    ) -> Result<impl Iterator<Item = &Row>> {
        self.index_scan(table, index, key, Bound::Unbounded, Bound::Unbounded)
    }

    /// Rows of `table` inside `region` (bounds inclusive, SPX-020),
    /// resolved via the spatial index — never a table scan (SPX-023).
    /// Errors when the table declares no `#[spatial(...)]` index (SPX-022).
    pub fn spatial_region(&self, table: TableId, region: Rect) -> Result<Vec<Row>> {
        self.state.table(table)?.spatial_region(region)
    }

    /// Rows of `table` within Euclidean distance `r` of `(x, y)` — bbox
    /// prefilter + exact circle filter, distance exactly `r` included
    /// (SPX-021). Errors when the table has no spatial index (SPX-022).
    pub fn spatial_radius(&self, table: TableId, x: f64, y: f64, r: f64) -> Result<Vec<Row>> {
        self.state.table(table)?.spatial_radius(x, y, r)
    }

    /// Rows of `table` at exactly `(x, y)` (IEEE `==`) via the spatial
    /// index. Errors when the table has no spatial index (SPX-022).
    pub fn spatial_point(&self, table: TableId, x: f64, y: f64) -> Result<Vec<Row>> {
        self.state.table(table)?.spatial_point(x, y)
    }

    /// Verify STG-007 rule 2 for `table`: every secondary index is
    /// bit-identical to a freshly rebuilt index over the committed rows.
    /// Diagnostic surface for tests and DST (SPEC-013).
    pub fn verify_index_integrity(&self, table: TableId) -> Result<()> {
        self.state.table(table)?.verify_index_integrity(table)
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
