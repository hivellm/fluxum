//! Secondary indexes (SPEC-001 §5, SPEC-002 §2/§7, SPEC-008, T2.4/T2.5).
//!
//! [`BTreeIndex`] implements `#[index(btree(...))]` declarations — single
//! and composite column — over the store's committed rows. [`QuadTree`]
//! implements `#[spatial(quadtree(x, y))]` (SPEC-008), registered per table
//! through [`SpatialIndexState`] and maintained by the same commit-merge
//! pipeline.
//!
//! # Design decisions (T2.4)
//!
//! - **Indexes live inside `TableState`** (the committed snapshot), exactly
//!   as STG-002 sketches. That is what keeps index reads lock-free (FR-10)
//!   and MVCC-consistent: a [`crate::store::Snapshot`] pins rows *and*
//!   indexes of the same published state, so an index scan can never observe
//!   a row set different from what a full scan of the same snapshot returns.
//! - **Maintenance rides the commit merge** (STG-005 steps 2–4): the commit
//!   builds the next `CommittedState` off to the side — row map and index
//!   updates applied together on the private copy — and publishes both with
//!   one atomic swap. Nothing is applied eagerly to shared structures during
//!   the transaction, so rollback remains pure `TxState` discard and the
//!   STG-007 rule-2 property ("after rollback every index is bit-identical
//!   to a freshly rebuilt index over `CommittedState`") holds by
//!   construction; `verify_index_integrity` and the T2.4 property suite
//!   prove it. The [`crate::store::UndoRecord`] hook therefore stays
//!   uninhabited — an eager design would leak uncommitted index entries to
//!   concurrent snapshot readers (violating STG-004) or force index reads
//!   through the writer lock (violating FR-10).
//! - **Memcomparable keys** (deferred from T2.1): index keys are an
//!   order-preserving byte transform of the indexed column values —
//!   `memcmp` on encoded keys equals the natural ordering of the values —
//!   so range scans and composite prefix scans are plain byte-range
//!   iteration. See [`btree`] for the per-type transform. This is deliberate
//!   groundwork for T2.8 (SPEC-015 TIER-050): pages of a byte-ordered map
//!   can be evicted and compared without decoding, and the index map is
//!   reached only through the [`BTreeIndex`] API, so the paged
//!   implementation replaces the in-memory `BTreeMap` behind the same
//!   surface.
//! - **Stable [`IndexId`]s** (STG-051): CRC32 over
//!   `table_name \0 col_1 \0 … col_n`, so an index id survives restarts and
//!   is derivable from the schema alone.

pub mod btree;
pub mod quadtree;

pub use btree::BTreeIndex;
pub use quadtree::{QuadTree, Rect};

use crate::error::{FluxumError, Result};
use crate::store::row::{PkBytes, Row, RowValue};

/// Stable `u32` index identifier (STG-051): CRC32 (IEEE) of
/// `table_name \0 column_1 \0 … \0 column_n`.
///
/// Deterministic from the schema, so commit-log entries and paged-index
/// metadata (T2.8) can reference an index without a live schema lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(u32);

impl IndexId {
    /// The stable id of a B-tree index on `table_name` over `column_names`
    /// (in declared key order).
    pub fn of(table_name: &str, column_names: &[&str]) -> Self {
        let mut bytes = Vec::with_capacity(
            table_name.len() + column_names.iter().map(|c| c.len() + 1).sum::<usize>(),
        );
        bytes.extend_from_slice(table_name.as_bytes());
        for column in column_names {
            bytes.push(0);
            bytes.extend_from_slice(column.as_bytes());
        }
        Self(crate::store::crc32(&bytes))
    }

    /// Wrap a raw index id (e.g. decoded from paged-index metadata).
    pub const fn from_raw(id: u32) -> Self {
        Self(id)
    }

    /// The raw `u32` value.
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#010x}", self.0)
    }
}

/// One table's spatial index (SPEC-008): the declared coordinate column
/// ordinals plus the backing structure.
///
/// Lives inside the committed `TableState` next to the B-tree indexes and is
/// maintained by the same commit-merge pipeline (SPX-030): inserts/deletes/
/// updates are applied to the private pre-swap copy and published with the
/// row map in one atomic swap, so uncommitted `TxState` changes are never
/// visible to the index and rollback leaves it untouched by construction
/// (SPX-032 update coherence follows from the delete-old + insert-new merge
/// of `PendingOp::Update`).
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialIndexState {
    /// Coordinate column ordinals: `(x, y)` for a QuadTree (SPX-001).
    columns: &'static [u16],
    index: SpatialIndex,
}

/// The backing structure family (SPX-002 / SPX-010). The R-tree variant
/// lands with T2.6.
#[derive(Debug, Clone, PartialEq)]
enum SpatialIndex {
    QuadTree(QuadTree),
}

impl SpatialIndexState {
    /// An empty QuadTree spatial index over the point columns `(x, y)`
    /// (`columns`, ordinals into the table schema), rooted at `bounds`
    /// (SPX-004) with the given leaf `bucket_size` (SPX-003).
    pub(crate) fn quadtree(columns: &'static [u16], bounds: Rect, bucket_size: usize) -> Self {
        Self {
            columns,
            index: SpatialIndex::QuadTree(QuadTree::new(bounds, bucket_size)),
        }
    }

    /// An empty index with this index's exact configuration — the rebuild
    /// seed for the STG-007 rule-2 integrity check.
    pub(crate) fn fresh_like(&self) -> Self {
        match &self.index {
            SpatialIndex::QuadTree(qt) => {
                Self::quadtree(self.columns, qt.bounds(), qt.bucket_size())
            }
        }
    }

    /// Read one coordinate column of `row`, widening `f32` to `f64`.
    fn coord(&self, row: &Row, ordinal: u16) -> Result<f64> {
        match row.value(ordinal) {
            Some(RowValue::F32(v)) => Ok(f64::from(*v)),
            Some(RowValue::F64(v)) => Ok(*v),
            other => Err(FluxumError::Storage(format!(
                "internal invariant violated: spatial coordinate ordinal {ordinal} is not a \
                 float column (got {other:?}); the registry validates DM-032"
            ))),
        }
    }

    /// The point coordinates of `row` per the declared columns.
    fn point_of(&self, row: &Row) -> Result<(f64, f64)> {
        match self.columns {
            [x, y] => Ok((self.coord(row, *x)?, self.coord(row, *y)?)),
            other => Err(FluxumError::Storage(format!(
                "internal invariant violated: quadtree spatial index declares {} coordinate \
                 column(s), expected 2 (DM-032)",
                other.len()
            ))),
        }
    }

    /// Add `row`'s spatial entry (commit merge, insert side — SPX-030).
    pub(crate) fn insert_row(&mut self, row: &Row, pk: PkBytes) -> Result<()> {
        let (x, y) = self.point_of(row)?;
        match &mut self.index {
            SpatialIndex::QuadTree(qt) => {
                qt.insert(x, y, pk);
            }
        }
        Ok(())
    }

    /// Remove `row`'s spatial entry (commit merge, delete side — SPX-030).
    pub(crate) fn remove_row(&mut self, row: &Row, pk: &PkBytes) -> Result<()> {
        let (x, y) = self.point_of(row)?;
        match &mut self.index {
            SpatialIndex::QuadTree(qt) => {
                qt.remove(x, y, pk);
            }
        }
        Ok(())
    }

    /// PKs of the rows inside `region` (bounds inclusive, SPX-020).
    pub(crate) fn query_region(&self, region: Rect) -> Vec<PkBytes> {
        match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_region(region),
        }
    }

    /// PKs of the rows within Euclidean distance `r` of `(x, y)` (SPX-021).
    pub(crate) fn query_radius(&self, x: f64, y: f64, r: f64) -> Vec<PkBytes> {
        match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_radius(x, y, r),
        }
    }

    /// PKs of the rows at exactly `(x, y)` under IEEE `==`.
    pub(crate) fn query_point(&self, x: f64, y: f64) -> Vec<PkBytes> {
        match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_point(x, y),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_id_is_stable_and_order_sensitive() {
        assert_eq!(
            IndexId::of("Msg", &["channel", "sent_at"]),
            IndexId::of("Msg", &["channel", "sent_at"])
        );
        // Column order is part of the identity (a btree(a, b) is not a
        // btree(b, a)).
        assert_ne!(
            IndexId::of("Msg", &["channel", "sent_at"]),
            IndexId::of("Msg", &["sent_at", "channel"])
        );
        // The table name is part of the identity.
        assert_ne!(
            IndexId::of("Msg", &["channel"]),
            IndexId::of("Log", &["channel"])
        );
        // The separator prevents concatenation ambiguity.
        assert_ne!(
            IndexId::of("Msg", &["ab", "c"]),
            IndexId::of("Msg", &["a", "bc"])
        );
        assert_eq!(IndexId::from_raw(0xAB).as_u32(), 0xAB);
        assert_eq!(IndexId::from_raw(0xAB).to_string(), "0x000000ab");
    }
}
