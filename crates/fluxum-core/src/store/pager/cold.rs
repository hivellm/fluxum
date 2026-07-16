//! [`ColdTable`] — one table's committed contents in the paged cold tier:
//! a primary [`PagedTree`] of FluxBIN rows keyed by encoded PK, plus one
//! index tree per declared secondary/spatial index (TIER-050/051).
//!
//! The logical semantics of SPEC-002 are unchanged: point lookups, ordered
//! range scans, and index queries answer exactly what the in-memory
//! [`crate::store::Snapshot`] answers — [`ColdTable::spill_snapshot`] takes
//! a published snapshot and materializes it as pages, which is the eviction
//! target of the T2.3 checkpoint flush. Index entries reference logical
//! primary keys (`memcomparable index key ++ encoded PK → encoded PK`),
//! never heap pointers, so pages fault back in at the same logical
//! coordinates (eviction-safe addressing, TIER-050).
//!
//! Spatial index declarations (SPEC-008) map onto the same paged B-tree:
//! their linear (quadtree/rtree) keys are memcomparable column encodings,
//! exactly like SPEC-008's `BTreeMap`-backed linear-quadtree design
//! (TIER-051).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::index::IndexId;
use crate::index::btree::{encode_value, plan_scan};
use crate::schema::{IndexSchema, TableSchema};
use crate::store::TableId;
use crate::store::committed::Snapshot;
use crate::store::row::{
    Row, RowValue, check_row, decode_row, encode_pk_of_row, encode_pk_values, encode_row,
};

use super::{PagedTree, Pager};

/// One paged secondary/spatial index.
#[derive(Debug)]
struct ColdIndex {
    id: IndexId,
    columns: &'static [u16],
    tree: PagedTree,
}

/// One table in the paged cold tier: primary row tree + index trees.
#[derive(Debug)]
pub struct ColdTable {
    schema: &'static TableSchema,
    table_id: TableId,
    primary: PagedTree,
    indexes: Vec<ColdIndex>,
}

impl ColdTable {
    /// Materialize `table_id`'s committed contents from a published
    /// snapshot into paged trees (bulk-loaded left to right; pages beyond
    /// the pool capacity spill to the page file as the load runs, so a
    /// table 10× the budget loads without ever exceeding it).
    pub fn spill_snapshot(
        pager: &Arc<Pager>,
        snapshot: &Snapshot,
        table_id: TableId,
    ) -> Result<Self> {
        let state = Arc::clone(snapshot.state.table(table_id)?);
        let schema = state.schema();

        let mut primary = PagedTree::create(pager, table_id, false)?;
        let mut rows = Vec::with_capacity(state.rows.len());
        for (pk, row) in &state.rows {
            rows.push((pk.as_bytes().to_vec(), encode_row(row.values())?));
        }
        primary.bulk_load(rows)?;

        let mut indexes = Vec::with_capacity(schema.indexes.len());
        for declared in schema.indexes {
            let columns = match declared {
                IndexSchema::BTree { columns } => *columns,
                IndexSchema::Spatial { columns, .. } => *columns,
                // Full-text indexes are rebuilt from rows on recovery, not
                // spilled as paged B-trees (FTS-022).
                IndexSchema::FullText { .. } => continue,
            };
            let id = index_id_of(schema, columns)?;
            let mut entries: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for (pk, row) in &state.rows {
                entries.insert(
                    index_key(schema, columns, row.values(), pk.as_bytes())?,
                    pk.as_bytes().to_vec(),
                );
            }
            let mut tree = PagedTree::create(pager, table_id, true)?;
            tree.bulk_load(entries)?;
            indexes.push(ColdIndex { id, columns, tree });
        }

        Ok(Self {
            schema,
            table_id,
            primary,
            indexes,
        })
    }

    /// The table's schema.
    pub fn schema(&self) -> &'static TableSchema {
        self.schema
    }

    /// The table's stable id.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// The primary row tree (diagnostics: root page id, content hashing).
    pub fn primary_tree(&self) -> &PagedTree {
        &self.primary
    }

    /// The index tree of `index`, if declared.
    pub fn index_tree(&self, index: IndexId) -> Option<&PagedTree> {
        self.indexes.iter().find(|i| i.id == index).map(|i| &i.tree)
    }

    /// Point lookup by primary key values (mirrors
    /// [`crate::store::Snapshot::query_pk`]).
    pub fn get(&self, pk_values: &[RowValue]) -> Result<Option<Row>> {
        let pk = encode_pk_values(self.schema, pk_values)?;
        match self.primary.get(pk.as_bytes())? {
            Some(bytes) => Ok(Some(decode_row(self.schema, &bytes)?)),
            None => Ok(None),
        }
    }

    /// All rows in encoded-PK byte order (mirrors
    /// [`crate::store::Snapshot::scan`]).
    pub fn scan_all(&self) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        self.primary.scan(&[], None, &mut |_, value| {
            rows.push(decode_row(self.schema, value)?);
            Ok(true)
        })?;
        Ok(rows)
    }

    /// Index scan (mirrors [`crate::store::Snapshot::index_scan`]):
    /// equality on `prefix` over the leading index columns, then
    /// `lower`/`upper` bounds over the next column. Rows come back in
    /// index order.
    pub fn index_scan(
        &self,
        index: IndexId,
        prefix: &[RowValue],
        lower: Bound<&RowValue>,
        upper: Bound<&RowValue>,
    ) -> Result<Vec<Row>> {
        let cold = self.indexes.iter().find(|i| i.id == index).ok_or_else(|| {
            FluxumError::Storage(format!(
                "unknown index {index} on table `{}`",
                self.schema.name
            ))
        })?;
        if prefix.len() > cold.columns.len() {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index {index} scan prefix has {} value(s) but the index \
                 has {} column(s)",
                self.schema.name,
                prefix.len(),
                cold.columns.len()
            )));
        }
        let ranged = !matches!((&lower, &upper), (Bound::Unbounded, Bound::Unbounded));
        if ranged && prefix.len() == cold.columns.len() {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index {index} scan has range bounds but the equality \
                 prefix already covers all {} index column(s)",
                self.schema.name,
                cold.columns.len()
            )));
        }
        let mut prefix_bytes = Vec::new();
        for (value, &ordinal) in prefix.iter().zip(cold.columns) {
            self.check_value(ordinal, value)?;
            encode_value(value, &mut prefix_bytes);
        }
        let range_ordinal = cold.columns.get(prefix.len()).copied();
        let encode_bound = |bound: Bound<&RowValue>| -> Result<Bound<Vec<u8>>> {
            Ok(match bound {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(v) | Bound::Excluded(v) => {
                    let ordinal = range_ordinal.ok_or_else(|| {
                        FluxumError::Storage(format!(
                            "internal invariant violated: range bound without a range \
                             column (index {index}, table `{}`)",
                            self.schema.name
                        ))
                    })?;
                    self.check_value(ordinal, v)?;
                    let mut bytes = Vec::new();
                    encode_value(v, &mut bytes);
                    match bound {
                        Bound::Included(_) => Bound::Included(bytes),
                        Bound::Excluded(_) => Bound::Excluded(bytes),
                        Bound::Unbounded => Bound::Unbounded,
                    }
                }
            })
        };
        let lower = encode_bound(lower)?;
        let upper = encode_bound(upper)?;
        let (start, end) = plan_scan(prefix_bytes, lower, upper);

        // Index leaves hold `index key ++ pk → pk`; resolve each hit
        // through the primary tree (both fault through the same pool).
        let mut pks: Vec<Vec<u8>> = Vec::new();
        cold.tree.scan(&start, end.as_deref(), &mut |_, pk| {
            pks.push(pk.to_vec());
            Ok(true)
        })?;
        let mut rows = Vec::with_capacity(pks.len());
        for pk in pks {
            let bytes = self.primary.get(&pk)?.ok_or_else(|| {
                FluxumError::Storage(format!(
                    "index {index} on table `{}` points at a pk absent from the \
                     primary tree",
                    self.schema.name
                ))
            })?;
            rows.push(decode_row(self.schema, &bytes)?);
        }
        Ok(rows)
    }

    /// Equality lookup on an index: all rows whose leading index columns
    /// equal `key`.
    pub fn index_eq(&self, index: IndexId, key: &[RowValue]) -> Result<Vec<Row>> {
        self.index_scan(index, key, Bound::Unbounded, Bound::Unbounded)
    }

    /// Insert or replace a full row (upsert): primary and every index tree
    /// stay mutually consistent, including superseded index entries.
    pub fn insert(&mut self, values: Vec<RowValue>) -> Result<()> {
        check_row(self.schema, &values)?;
        let pk = encode_pk_of_row(self.schema, &values)?;
        // Replacement: retire the old row's index entries first.
        if let Some(old_bytes) = self.primary.get(pk.as_bytes())? {
            let old = decode_row(self.schema, &old_bytes)?;
            for index in &mut self.indexes {
                let key = index_key(self.schema, index.columns, old.values(), pk.as_bytes())?;
                index.tree.delete(&key)?;
            }
        }
        let row_bytes = encode_row(&values)?;
        self.primary.insert(pk.as_bytes(), &row_bytes)?;
        for index in &mut self.indexes {
            let key = index_key(self.schema, index.columns, &values, pk.as_bytes())?;
            index.tree.insert(&key, pk.as_bytes())?;
        }
        Ok(())
    }

    /// Delete by primary key. Returns whether the row existed.
    pub fn delete(&mut self, pk_values: &[RowValue]) -> Result<bool> {
        let pk = encode_pk_values(self.schema, pk_values)?;
        let Some(old_bytes) = self.primary.get(pk.as_bytes())? else {
            return Ok(false);
        };
        let old = decode_row(self.schema, &old_bytes)?;
        for index in &mut self.indexes {
            let key = index_key(self.schema, index.columns, old.values(), pk.as_bytes())?;
            index.tree.delete(&key)?;
        }
        self.primary.delete(pk.as_bytes())?;
        Ok(true)
    }

    /// Type-check `value` against the column at `ordinal`.
    fn check_value(&self, ordinal: u16, value: &RowValue) -> Result<()> {
        let column = self.schema.column(ordinal).ok_or_else(|| {
            FluxumError::Storage(format!(
                "internal invariant violated: index ordinal {ordinal} out of range \
                 for table `{}`",
                self.schema.name
            ))
        })?;
        if !value.matches_type(&column.ty) {
            return Err(FluxumError::Storage(format!(
                "table `{}`: index column `{}` expects {:?}, got {value}",
                self.schema.name, column.name, column.ty
            )));
        }
        Ok(())
    }
}

/// The STG-051 stable id of an index over `columns`.
fn index_id_of(schema: &'static TableSchema, columns: &[u16]) -> Result<IndexId> {
    let mut names = Vec::with_capacity(columns.len());
    for &ordinal in columns {
        let column = schema.column(ordinal).ok_or_else(|| {
            FluxumError::Storage(format!(
                "table `{}`: index ordinal {ordinal} out of range",
                schema.name
            ))
        })?;
        names.push(column.name);
    }
    Ok(IndexId::of(schema.name, &names))
}

/// The cold index-tree key of one row: memcomparable index columns, then
/// the encoded PK (which makes the key unique per row).
fn index_key(
    schema: &'static TableSchema,
    columns: &[u16],
    values: &[RowValue],
    pk: &[u8],
) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    for &ordinal in columns {
        let value = values.get(usize::from(ordinal)).ok_or_else(|| {
            FluxumError::Storage(format!(
                "table `{}`: index ordinal {ordinal} out of range for a row of {} \
                 columns",
                schema.name,
                values.len()
            ))
        })?;
        encode_value(value, &mut key);
    }
    key.extend_from_slice(pk);
    Ok(key)
}

#[cfg(test)]
mod tests {
    //! Accessor surface, query-shape validation errors, and the
    //! internal-invariant guards (which the public API can only reach
    //! through corrupted pages) of the cold tier.
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::super::PagerOptions;
    use super::*;
    use crate::config::PageCompression;
    use crate::schema::{ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, VisibilityRule};
    use crate::store::MemStore;

    static ITEM_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "label",
            ty: FluxType::Str,
        },
        ColumnSchema {
            name: "qty",
            ty: FluxType::I64,
        },
    ];

    static ITEM: TableSchema = TableSchema {
        name: "Item",
        columns: ITEM_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[IndexSchema::BTree { columns: &[1] }],
        visibility: VisibilityRule::PublicAll,
    };

    fn spilled(dir: &std::path::Path) -> ColdTable {
        let schema = Schema::from_tables([&ITEM]).unwrap();
        let store = MemStore::new(&schema).unwrap();
        let table = store.table_id("Item").unwrap();
        let mut tx = store.begin();
        for id in 0..4u64 {
            tx.insert(
                table,
                vec![
                    RowValue::U64(id),
                    RowValue::Str(format!("label-{id}")),
                    RowValue::I64(id as i64 * 3),
                ],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let pager = Pager::open(
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
        .unwrap();
        ColdTable::spill_snapshot(&pager, &store.snapshot(), table).unwrap()
    }

    #[test]
    fn accessors_expose_schema_ids_and_trees() {
        let dir = tempfile::tempdir().unwrap();
        let cold = spilled(dir.path());
        assert_eq!(cold.schema().name, "Item");
        assert_eq!(cold.table_id(), TableId::of("Item"));
        assert!(cold.primary_tree().root_page_id() > 0);
        let label_index = IndexId::of("Item", &["label"]);
        assert!(cold.index_tree(label_index).is_some());
        assert!(cold.index_tree(IndexId::of("Item", &["nope"])).is_none());
    }

    #[test]
    fn malformed_index_scans_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cold = spilled(dir.path());
        let label_index = IndexId::of("Item", &["label"]);

        // Unknown index id.
        let err = cold
            .index_eq(IndexId::of("Item", &["ghost"]), &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown index"), "{err}");

        // Equality prefix longer than the index.
        let err = cold
            .index_eq(
                label_index,
                &[RowValue::Str("a".into()), RowValue::Str("b".into())],
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("prefix has 2 value(s)"), "{err}");

        // Range bounds after the prefix already covers every column.
        let err = cold
            .index_scan(
                label_index,
                &[RowValue::Str("label-1".into())],
                Bound::Included(&RowValue::Str("x".into())),
                Bound::Unbounded,
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("already covers"), "{err}");

        // Mistyped prefix value.
        let err = cold
            .index_eq(label_index, &[RowValue::U64(1)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("expects"), "{err}");
    }

    #[test]
    fn internal_ordinal_guards_report_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let cold = spilled(dir.path());

        let err = index_id_of(&ITEM, &[9]).unwrap_err().to_string();
        assert!(err.contains("ordinal 9 out of range"), "{err}");

        let row = vec![RowValue::U64(0)];
        let err = index_key(&ITEM, &[9], &row, &[]).unwrap_err().to_string();
        assert!(err.contains("ordinal 9 out of range"), "{err}");

        let err = cold
            .check_value(9, &RowValue::U64(0))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ordinal 9 out of range"), "{err}");
    }

    #[test]
    fn dangling_index_entries_are_reported_not_served() {
        let dir = tempfile::tempdir().unwrap();
        let mut cold = spilled(dir.path());
        // Corrupt the invariant deliberately: remove a row from the primary
        // tree while its index entry survives.
        let pk = encode_pk_values(&ITEM, &[RowValue::U64(2)]).unwrap();
        assert!(cold.primary.delete(pk.as_bytes()).unwrap());
        let err = cold
            .index_eq(
                IndexId::of("Item", &["label"]),
                &[RowValue::Str("label-2".into())],
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("absent from the primary tree"), "{err}");
    }
}
