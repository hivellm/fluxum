//! [`UniqueIndex`] — the committed-side lookup structure behind `#[unique]`
//! constraints (DM-006, TXN-041) — the secondary-constraint work T2.1
//! deferred to T3.1 (see [`super`]'s "Constraint overlay" decision).
//!
//! One `UniqueIndex` exists per declared constraint (single or multi-column),
//! mapping the memcomparable encoding of the constraint columns to the PK of
//! the row owning that value. It lives inside the committed
//! [`super::TableState`] and follows exactly the T2.4 B-tree index
//! discipline: checked eagerly at write time against the STG-007 overlay
//! (`CommittedState` ⊕ `TxState`), maintained inside the commit merge on the
//! private pre-swap copy — never eagerly — so rollback leaves it bit-identical
//! to a fresh rebuild over `CommittedState` (STG-007 rule 2).
//!
//! Keys reuse the [`crate::index::btree`] memcomparable transform: equality
//! is all a constraint needs, and the transform already gives every value —
//! including `NaN` and `None` — one deterministic, prefix-free encoding.

use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::index::btree;
use crate::schema::TableSchema;
use crate::store::TableId;
use crate::store::pager::{PagedTree, Pager};
use crate::store::row::{PkBytes, Row, RowValue};

/// One `#[unique]` constraint's committed value map, served through the
/// paged cold tier (SPEC-015 TIER-050): a [`PagedTree`] mapping the
/// memcomparable key of the constraint columns to the encoded PK of the row
/// owning that value, so constraint memory counts against `memory.budget`
/// like every other index page (TIER-070). Copy-on-write under the commit
/// merge exactly like [`crate::index::btree::BTreeIndex`] (TIER-061).
#[derive(Debug, Clone)]
pub(crate) struct UniqueIndex {
    /// Constraint column ordinals in declared order (DM-006).
    columns: &'static [u16],
    /// Paged map: memcomparable constraint key → encoded PK.
    tree: PagedTree,
}

impl UniqueIndex {
    /// An empty paged constraint map over `columns` (ordinals into the
    /// table's schema, registry-validated), allocating its pages from
    /// `table_id`'s page file.
    pub(crate) fn new(
        columns: &'static [u16],
        pager: &Arc<Pager>,
        table_id: TableId,
    ) -> Result<Self> {
        Ok(Self {
            columns,
            tree: PagedTree::create(pager, table_id, true)?,
        })
    }

    /// The constraint's column ordinals in declared order.
    pub(crate) fn columns(&self) -> &'static [u16] {
        self.columns
    }

    /// The memcomparable constraint key of a full row's `values`.
    pub(crate) fn key_of_values(&self, values: &[RowValue]) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        for &ordinal in self.columns {
            let value = values.get(usize::from(ordinal)).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: #[unique] ordinal {ordinal} out of range \
                     for a row of {} columns",
                    values.len()
                ))
            })?;
            btree::encode_value(value, &mut key);
        }
        Ok(key)
    }

    /// The PK owning `key` in the committed state, if any (faults pages on
    /// demand).
    pub(crate) fn owner(&self, key: &[u8]) -> Result<Option<PkBytes>> {
        Ok(self.tree.get(key)?.map(PkBytes::from_bytes))
    }

    /// Claim `row`'s constraint value for `pk` (commit merge, insert side).
    /// Copy-on-write: superseded pages go to `superseded` for the version
    /// reclaimer (TIER-061).
    ///
    /// Violations are rejected eagerly at write time (TXN-041), so an
    /// occupied key here is an internal invariant failure, never a user
    /// error.
    pub(crate) fn insert(
        &mut self,
        row: &Row,
        pk: PkBytes,
        superseded: &mut Vec<u64>,
    ) -> Result<()> {
        let key = self.key_of_values(row.values())?;
        if let Some(existing) = self.owner(&key)?
            && existing != pk
        {
            return Err(FluxumError::Storage(format!(
                "internal invariant violated: unique key claimed by pk {existing} while \
                 merging pk {pk} — eager TXN-041 validation missed a conflict"
            )));
        }
        self.tree.insert_cow(&key, pk.as_bytes(), superseded)
    }

    /// Release `row`'s constraint value if `pk` owns it (commit merge,
    /// delete side). Releasing a key owned by another PK is a no-op: the
    /// two-pass merge removes every vacated key before any claim, so a
    /// same-transaction value move never drops the new owner's entry.
    pub(crate) fn remove(
        &mut self,
        row: &Row,
        pk: &PkBytes,
        superseded: &mut Vec<u64>,
    ) -> Result<()> {
        let key = self.key_of_values(row.values())?;
        if self.owner(&key)?.is_some_and(|owner| owner == *pk) {
            self.tree.delete_cow(&key, superseded)?;
        }
        Ok(())
    }

    /// Every `(constraint key, pk)` entry in key order — the STG-007 rule-2
    /// integrity-check surface.
    pub(crate) fn entries(&self) -> Result<Vec<(Vec<u8>, PkBytes)>> {
        let mut out = Vec::new();
        self.tree.scan(&[], None, &mut |key, pk| {
            out.push((key.to_vec(), PkBytes::from_bytes(pk.to_vec())));
            Ok(true)
        })?;
        Ok(out)
    }

    /// Bulk-load sorted `(constraint key, pk)` entries into this **empty**
    /// map (the recovery fold — pages enter the pool scan-resistant).
    pub(crate) fn bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        self.tree.bulk_load(entries)
    }
}

/// The TXN-041 violation error: names the table, the constraint columns,
/// and the conflicting values.
pub(crate) fn violation_error(
    schema: &TableSchema,
    columns: &[u16],
    values: &[RowValue],
) -> FluxumError {
    let names: Vec<&str> = columns
        .iter()
        .filter_map(|&ordinal| schema.column(ordinal).map(|c| c.name))
        .collect();
    let shown: Vec<String> = columns
        .iter()
        .filter_map(|&ordinal| values.get(usize::from(ordinal)))
        .map(ToString::to_string)
        .collect();
    FluxumError::Storage(format!(
        "unique constraint violation: table={} columns=({}) value=({})",
        schema.name,
        names.join(", "),
        shown.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, VisibilityRule};
    use crate::store::row::encode_pk_values;

    static COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "email",
            ty: FluxType::Str,
        },
    ];

    static T: TableSchema = TableSchema {
        name: "CovUnique",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Private,
        partition_by: None,
        unique: &[&[1]],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    fn pk(id: u64) -> PkBytes {
        encode_pk_values(&T, &[RowValue::U64(id)]).unwrap_or_else(|e| panic!("{e}"))
    }

    fn row(id: u64, email: &str) -> Row {
        Row::new(vec![RowValue::U64(id), RowValue::Str(email.into())])
    }

    /// A throwaway pager for constraint-unit tests — one fresh directory per
    /// call, so concurrently running tests never share page files.
    fn test_pager() -> Arc<Pager> {
        use crate::config::PageCompression;
        use crate::store::pager::PagerOptions;
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "fluxum-unique-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        Pager::open(
            dir,
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 64 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    fn unique(columns: &'static [u16]) -> UniqueIndex {
        UniqueIndex::new(columns, &test_pager(), TableId::of("CovUnique"))
            .unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn out_of_range_constraint_ordinals_are_an_invariant_breach() {
        let index = unique(&[9]);
        let err = match index.key_of_values(&[RowValue::U64(1)]) {
            Ok(_) => panic!("out-of-range ordinal keyed"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("#[unique] ordinal 9 out of range"), "{err}");
    }

    #[test]
    fn merging_a_key_claimed_by_another_pk_is_an_invariant_breach() {
        let mut index = unique(&[1]);
        let mut sup = Vec::new();
        index
            .insert(&row(1, "a@example.com"), pk(1), &mut sup)
            .unwrap_or_else(|e| panic!("{e}"));
        // Reclaiming a key for the SAME pk is idempotent (update merges).
        index
            .insert(&row(1, "a@example.com"), pk(1), &mut sup)
            .unwrap_or_else(|e| panic!("{e}"));
        // A different pk claiming the same value means the eager TXN-041
        // check missed a conflict — an invariant error, never silent.
        let err = match index.insert(&row(2, "a@example.com"), pk(2), &mut sup) {
            Ok(()) => panic!("conflicting unique claim merged"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("eager TXN-041 validation missed a conflict"),
            "{err}"
        );
    }

    #[test]
    fn cow_release_and_reclaim_keep_snapshot_versions_intact() {
        let mut index = unique(&[1]);
        let mut sup = Vec::new();
        index
            .insert(&row(1, "a@example.com"), pk(1), &mut sup)
            .unwrap_or_else(|e| panic!("{e}"));
        let key = index
            .key_of_values(row(1, "a@example.com").values())
            .unwrap_or_else(|e| panic!("{e}"));
        let snap = index.clone();

        // Releasing under a foreign pk is a no-op; under the owner it
        // vacates the key — but only on the new version.
        index
            .remove(&row(1, "a@example.com"), &pk(2), &mut sup)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            index.owner(&key).unwrap_or_else(|e| panic!("{e}")),
            Some(pk(1))
        );
        index
            .remove(&row(1, "a@example.com"), &pk(1), &mut sup)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(index.owner(&key).unwrap_or_else(|e| panic!("{e}")), None);
        assert_eq!(
            snap.owner(&key).unwrap_or_else(|e| panic!("{e}")),
            Some(pk(1)),
            "the pinned snapshot still reads the old version"
        );
    }
}
