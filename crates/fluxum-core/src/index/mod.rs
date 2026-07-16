//! Secondary indexes (SPEC-001 §5, SPEC-002 §2/§7, SPEC-008, T2.4/T2.5).
//!
//! [`BTreeIndex`] implements `#[index(btree(...))]` declarations — single
//! and composite column — over the store's committed rows. [`QuadTree`]
//! implements `#[spatial(quadtree(x, y))]` and [`RTree`]
//! `#[spatial(rtree(...))]` (SPEC-008), registered per table through
//! [`SpatialIndexState`] and maintained by the same commit-merge pipeline;
//! [`SpatialPredicate`] is the typed `IN REGION` / `WITHIN RADIUS` surface
//! (SPX-020/021) the T4.1 SQL compiler lowers to.
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
pub mod rtree;

pub use btree::BTreeIndex;
pub use quadtree::{QuadTree, Rect};
pub use rtree::{Aabb, RTree};

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
///
/// # Readiness (SPX-031)
///
/// Spatial indexes are not persisted; after crash recovery they are rebuilt
/// from the recovered rows. A slot in the **rebuilding** state (`ready ==
/// false`, `MemStore::mark_spatial_rebuilding`) answers every query with the
/// SPX-023 error `503 spatial index not ready` until
/// `MemStore::rebuild_spatial_indexes` publishes the rebuilt state; the
/// server assembly gates `ReducerCall` admission on
/// `MemStore::spatial_ready` so rebuilding always completes before the shard
/// serves writes.
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialIndexState {
    /// Coordinate column ordinals: `(x, y)` for a QuadTree (SPX-001),
    /// `(min_x, min_y, max_x, max_y)` for an R-tree (SPX-010).
    columns: &'static [u16],
    index: SpatialIndex,
    /// SPX-031 gate: `false` while the index awaits its post-recovery
    /// rebuild — queries return 503, commit-merge maintenance is skipped
    /// (the rebuild recreates the index from the rows wholesale).
    ready: bool,
}

/// The backing structure family (SPX-002 / SPX-010).
#[derive(Debug, Clone, PartialEq)]
enum SpatialIndex {
    QuadTree(QuadTree),
    RTree(RTree),
}

impl SpatialIndexState {
    /// An empty QuadTree spatial index over the point columns `(x, y)`
    /// (`columns`, ordinals into the table schema), rooted at `bounds`
    /// (SPX-004) with the given leaf `bucket_size` (SPX-003).
    pub(crate) fn quadtree(columns: &'static [u16], bounds: Rect, bucket_size: usize) -> Self {
        Self {
            columns,
            index: SpatialIndex::QuadTree(QuadTree::new(bounds, bucket_size)),
            ready: true,
        }
    }

    /// An empty R-tree spatial index over the box columns
    /// `(min_x, min_y, max_x, max_y)` (SPX-010) with node capacity
    /// `max_entries`.
    pub(crate) fn rtree(columns: &'static [u16], max_entries: usize) -> Self {
        Self {
            columns,
            index: SpatialIndex::RTree(RTree::new(max_entries)),
            ready: true,
        }
    }

    /// An empty index with this index's exact configuration — the rebuild
    /// seed for the STG-007 rule-2 integrity check and for SPX-031 rebuilds.
    pub(crate) fn fresh_like(&self) -> Self {
        match &self.index {
            SpatialIndex::QuadTree(qt) => {
                Self::quadtree(self.columns, qt.bounds(), qt.bucket_size())
            }
            SpatialIndex::RTree(rt) => Self::rtree(self.columns, rt.max_entries()),
        }
    }

    /// This configuration in the SPX-031 rebuilding state: empty, not
    /// ready — every query answers `503 spatial index not ready`.
    pub(crate) fn rebuilding_like(&self) -> Self {
        Self {
            ready: false,
            ..self.fresh_like()
        }
    }

    /// Whether the index serves queries (SPX-031).
    pub(crate) fn is_ready(&self) -> bool {
        self.ready
    }

    /// The SPX-023 not-ready guard.
    fn check_ready(&self) -> Result<()> {
        if self.ready {
            Ok(())
        } else {
            Err(FluxumError::query(
                fluxum_protocol::codes::STORAGE_SPATIAL_REBUILDING,
                "spatial index not ready",
            ))
        }
    }

    /// Read one coordinate column, widening `f32` to `f64`.
    fn coord(values: &[RowValue], ordinal: u16) -> Result<f64> {
        match values.get(usize::from(ordinal)) {
            Some(RowValue::F32(v)) => Ok(f64::from(*v)),
            Some(RowValue::F64(v)) => Ok(*v),
            other => Err(FluxumError::Storage(format!(
                "internal invariant violated: spatial coordinate ordinal {ordinal} is not a \
                 float column (got {other:?}); the registry validates DM-032"
            ))),
        }
    }

    /// The bounding box of a row per the declared columns (R-tree).
    fn box_of(&self, values: &[RowValue]) -> Result<Aabb> {
        match self.columns {
            [min_x, min_y, max_x, max_y] => Ok(Aabb::new(
                Self::coord(values, *min_x)?,
                Self::coord(values, *min_y)?,
                Self::coord(values, *max_x)?,
                Self::coord(values, *max_y)?,
            )),
            other => Err(arity_invariant("rtree", 4, other.len())),
        }
    }

    /// SPX-010 insert constraint, enforced eagerly at `Tx::insert` time:
    /// R-tree rows must satisfy `min_x <= max_x` and `min_y <= max_y`
    /// (NaN coordinates fail the comparisons and are rejected too).
    pub(crate) fn check_insert(&self, table_name: &str, values: &[RowValue]) -> Result<()> {
        match &self.index {
            SpatialIndex::QuadTree(_) => Ok(()),
            SpatialIndex::RTree(_) => {
                let b = self.box_of(values)?;
                if b.min_x <= b.max_x && b.min_y <= b.max_y {
                    Ok(())
                } else {
                    Err(FluxumError::Storage(format!(
                        "table `{table_name}`: rtree constraint violated — min_x <= max_x and \
                         min_y <= max_y must hold, got ({}, {}, {}, {}) (SPX-010)",
                        b.min_x, b.min_y, b.max_x, b.max_y
                    )))
                }
            }
        }
    }

    /// Add `row`'s spatial entry (commit merge, insert side — SPX-030).
    /// Skipped while rebuilding: the SPX-031 rebuild recreates the index
    /// from the committed rows wholesale.
    pub(crate) fn insert_row(&mut self, row: &Row, pk: PkBytes) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        match &mut self.index {
            SpatialIndex::QuadTree(qt) => {
                let (x, y) = match self.columns {
                    [x, y] => (
                        Self::coord(row.values(), *x)?,
                        Self::coord(row.values(), *y)?,
                    ),
                    _ => return Err(arity_invariant("quadtree", 2, self.columns.len())),
                };
                qt.insert(x, y, pk);
            }
            SpatialIndex::RTree(rt) => {
                let aabb = match self.columns {
                    [a, b, c, d] => Aabb::new(
                        Self::coord(row.values(), *a)?,
                        Self::coord(row.values(), *b)?,
                        Self::coord(row.values(), *c)?,
                        Self::coord(row.values(), *d)?,
                    ),
                    _ => return Err(arity_invariant("rtree", 4, self.columns.len())),
                };
                rt.insert(aabb, pk);
            }
        }
        Ok(())
    }

    /// Remove `row`'s spatial entry (commit merge, delete side — SPX-030).
    pub(crate) fn remove_row(&mut self, row: &Row, pk: &PkBytes) -> Result<()> {
        if !self.ready {
            return Ok(());
        }
        match &mut self.index {
            SpatialIndex::QuadTree(qt) => {
                let (x, y) = match self.columns {
                    [x, y] => (
                        Self::coord(row.values(), *x)?,
                        Self::coord(row.values(), *y)?,
                    ),
                    _ => return Err(arity_invariant("quadtree", 2, self.columns.len())),
                };
                qt.remove(x, y, pk);
            }
            SpatialIndex::RTree(rt) => {
                let aabb = match self.columns {
                    [a, b, c, d] => Aabb::new(
                        Self::coord(row.values(), *a)?,
                        Self::coord(row.values(), *b)?,
                        Self::coord(row.values(), *c)?,
                        Self::coord(row.values(), *d)?,
                    ),
                    _ => return Err(arity_invariant("rtree", 4, self.columns.len())),
                };
                rt.remove(&aabb, pk);
            }
        }
        Ok(())
    }

    /// PKs matching an `IN REGION` box (SPX-020): QuadTree — points inside
    /// the closed box; R-tree — stored boxes **intersecting** it.
    pub(crate) fn query_region(&self, region: Rect) -> Result<Vec<PkBytes>> {
        self.check_ready()?;
        Ok(match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_region(region),
            SpatialIndex::RTree(rt) => rt.query_region(&Aabb::new(
                region.x,
                region.y,
                region.x + region.w,
                region.y + region.h,
            )),
        })
    }

    /// PKs matching `WITHIN RADIUS r OF (x, y)` (SPX-021): QuadTree —
    /// Euclidean point distance ≤ `r`; R-tree — minimum box distance ≤ `r`.
    pub(crate) fn query_radius(&self, x: f64, y: f64, r: f64) -> Result<Vec<PkBytes>> {
        self.check_ready()?;
        Ok(match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_radius(x, y, r),
            SpatialIndex::RTree(rt) => rt.query_radius(x, y, r),
        })
    }

    /// PKs at the point `(x, y)`: QuadTree — coordinates equal under IEEE
    /// `==`; R-tree — stored boxes containing the point.
    pub(crate) fn query_point(&self, x: f64, y: f64) -> Result<Vec<PkBytes>> {
        self.check_ready()?;
        Ok(match &self.index {
            SpatialIndex::QuadTree(qt) => qt.query_point(x, y),
            SpatialIndex::RTree(rt) => rt.query_point(x, y),
        })
    }
}

/// Coordinate-arity invariant breach (the registry validates DM-032, so
/// reaching this is a bug, surfaced as an error rather than a panic).
fn arity_invariant(kind: &str, expected: usize, got: usize) -> FluxumError {
    FluxumError::Storage(format!(
        "internal invariant violated: {kind} spatial index declares {got} coordinate \
         column(s), expected {expected} (DM-032)"
    ))
}

/// A typed spatial predicate — the SPX-020/021 SQL surface
/// (`IN REGION (x, y, w, h)` / `WITHIN RADIUS r OF (x, y)`) after parsing.
///
/// Evaluated through `Snapshot::eval_spatial` / `Tx::eval_spatial`, always
/// via the table's spatial index — a full-table-scan fallback does not exist
/// (SPX-023). Validation failures and missing spatial indexes surface as
/// [`FluxumError::Query`] with the SPEC-008 wire codes (400/503).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpatialPredicate {
    /// `IN REGION (x, y, w, h)`: the closed box `[x, x+w] × [y, y+h]`
    /// (SPX-020). `w`/`h` must be non-negative.
    InRegion {
        /// Bottom-left corner X.
        x: f64,
        /// Bottom-left corner Y.
        y: f64,
        /// Box width (non-negative).
        w: f64,
        /// Box height (non-negative).
        h: f64,
    },
    /// `WITHIN RADIUS r OF (x, y)` (SPX-021). `r` must be non-negative;
    /// rows at distance exactly `r` match.
    WithinRadius {
        /// Centre X.
        x: f64,
        /// Centre Y.
        y: f64,
        /// Radius (non-negative).
        r: f64,
    },
}

impl SpatialPredicate {
    /// SPX-020/021 compile-time validation: negative (or NaN) `w`/`h`/`r`
    /// are rejected with wire code 400.
    pub fn validate(&self) -> Result<()> {
        let reject = |what: &str, value: f64| {
            Err(FluxumError::query(
                fluxum_protocol::codes::SQL_MALFORMED,
                format!("spatial predicate {what} must be non-negative, got {value}"),
            ))
        };
        match *self {
            Self::InRegion { w, h, .. } => {
                if w.is_nan() || w < 0.0 {
                    return reject("width", w);
                }
                if h.is_nan() || h < 0.0 {
                    return reject("height", h);
                }
                Ok(())
            }
            Self::WithinRadius { r, .. } => {
                if r.is_nan() || r < 0.0 {
                    return reject("radius", r);
                }
                Ok(())
            }
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

    #[allow(clippy::unwrap_used)]
    mod spatial_state {
        use super::super::*;
        use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
        use crate::store::row::{Row, encode_pk_values};

        /// A distinct `PkBytes` per `n` (FluxBIN-encoded u64, like the store).
        fn pk(n: u64) -> PkBytes {
            static COLS: &[ColumnSchema] = &[ColumnSchema {
                name: "id",
                ty: FluxType::U64,
            }];
            static T: TableSchema = TableSchema {
                name: "P",
                columns: COLS,
                primary_key: &[0],
                auto_inc: None,
                access: TableAccess::Private,
                partition_by: None,
                unique: &[],
                indexes: &[],
                visibility: VisibilityRule::PublicAll,
            };
            encode_pk_values(&T, &[RowValue::U64(n)]).unwrap()
        }

        fn point_row(x: f64, y: f64) -> Row {
            Row::new(vec![RowValue::F64(x), RowValue::F64(y)])
        }

        #[test]
        fn quadtree_state_round_trips_points_and_rebuilding_gates_queries() {
            let mut state =
                SpatialIndexState::quadtree(&[0, 1], Rect::new(0.0, 0.0, 100.0, 100.0), 4);
            assert!(state.is_ready());
            state.insert_row(&point_row(5.0, 5.0), pk(1)).unwrap();
            assert_eq!(
                state.query_region(Rect::new(0.0, 0.0, 10.0, 10.0)).unwrap(),
                vec![pk(1)]
            );
            assert_eq!(state.query_point(5.0, 5.0).unwrap(), vec![pk(1)]);
            state.remove_row(&point_row(5.0, 5.0), &pk(1)).unwrap();
            assert!(state.query_radius(5.0, 5.0, 1.0).unwrap().is_empty());

            // SPX-031: the rebuilding clone answers 503 and skips maintenance.
            let mut rebuilding = state.rebuilding_like();
            assert!(!rebuilding.is_ready());
            rebuilding.insert_row(&point_row(1.0, 1.0), pk(2)).unwrap(); // no-op
            rebuilding.remove_row(&point_row(1.0, 1.0), &pk(2)).unwrap(); // no-op
            let err = rebuilding.query_point(1.0, 1.0).unwrap_err();
            assert_eq!(
                err.query_code(),
                Some(fluxum_protocol::codes::STORAGE_SPATIAL_REBUILDING),
                "{err}"
            );

            // fresh_like restores a ready, empty index of the same shape.
            assert!(state.fresh_like().is_ready());
        }

        #[test]
        fn rtree_state_supports_point_queries_and_insert_constraints() {
            let mut state = SpatialIndexState::rtree(&[0, 1, 2, 3], 8);
            let row = Row::new(vec![
                RowValue::F64(0.0),
                RowValue::F64(0.0),
                RowValue::F64(10.0),
                RowValue::F64(10.0),
            ]);
            state.check_insert("Zone", row.values()).unwrap();
            state.insert_row(&row, pk(1)).unwrap();
            assert_eq!(state.query_point(5.0, 5.0).unwrap(), vec![pk(1)]);
            assert!(state.query_point(50.0, 50.0).unwrap().is_empty());
            state.remove_row(&row, &pk(1)).unwrap();
            assert!(state.query_point(5.0, 5.0).unwrap().is_empty());

            // SPX-010: inverted boxes are rejected eagerly at insert time.
            let inverted = Row::new(vec![
                RowValue::F64(10.0),
                RowValue::F64(0.0),
                RowValue::F64(0.0),
                RowValue::F64(10.0),
            ]);
            let err = state.check_insert("Zone", inverted.values()).unwrap_err();
            assert!(err.to_string().contains("SPX-010"), "{err}");
        }

        #[test]
        fn coordinate_and_arity_invariants_surface_as_errors_not_panics() {
            // A non-float coordinate column (the registry validates DM-032;
            // this is the runtime backstop).
            let mut qt = SpatialIndexState::quadtree(&[0, 1], Rect::new(0.0, 0.0, 1.0, 1.0), 4);
            let bad = Row::new(vec![RowValue::Str("x".into()), RowValue::Str("y".into())]);
            let err = qt.insert_row(&bad, pk(1)).unwrap_err();
            assert!(err.to_string().contains("not a float column"), "{err}");

            // Wrong coordinate arity for each family, on every maintenance op.
            let mut qt_bad = SpatialIndexState::quadtree(&[0], Rect::new(0.0, 0.0, 1.0, 1.0), 4);
            let row = point_row(0.5, 0.5);
            let err = qt_bad.insert_row(&row, pk(1)).unwrap_err();
            assert!(err.to_string().contains("quadtree"), "{err}");
            assert!(err.to_string().contains("expected 2"), "{err}");
            let err = qt_bad.remove_row(&row, &pk(1)).unwrap_err();
            assert!(err.to_string().contains("DM-032"), "{err}");

            let mut rt_bad = SpatialIndexState::rtree(&[0, 1], 8);
            let err = rt_bad.insert_row(&row, pk(1)).unwrap_err();
            assert!(err.to_string().contains("rtree"), "{err}");
            assert!(err.to_string().contains("expected 4"), "{err}");
            let err = rt_bad.remove_row(&row, &pk(1)).unwrap_err();
            assert!(err.to_string().contains("DM-032"), "{err}");
            let err = rt_bad.check_insert("Zone", row.values()).unwrap_err();
            assert!(err.to_string().contains("expected 4"), "{err}");
        }

        #[test]
        fn spatial_predicate_validation_rejects_negative_and_nan_parameters() {
            SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: 1.0,
                h: 1.0,
            }
            .validate()
            .unwrap();
            let err = SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: -1.0,
                h: 1.0,
            }
            .validate()
            .unwrap_err();
            assert_eq!(
                err.query_code(),
                Some(fluxum_protocol::codes::SQL_MALFORMED),
                "{err}"
            );
            let err = SpatialPredicate::InRegion {
                x: 0.0,
                y: 0.0,
                w: 1.0,
                h: f64::NAN,
            }
            .validate()
            .unwrap_err();
            assert!(err.to_string().contains("height"), "{err}");
            let err = SpatialPredicate::WithinRadius {
                x: 0.0,
                y: 0.0,
                r: -2.0,
            }
            .validate()
            .unwrap_err();
            assert!(err.to_string().contains("radius"), "{err}");
        }
    }
}
