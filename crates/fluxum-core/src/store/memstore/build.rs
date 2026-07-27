//! Store-assembly builders (indexes, constraints, foreign keys) and the
//! commit-side blob/refcount helpers — split from the parent module to
//! honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn invariant_missing_row() -> FluxumError {
    FluxumError::Storage(
        "internal invariant violated: Delete/Update op for pk absent from CommittedState".into(),
    )
}

/// The empty spatial index of `table`, if it declares one (SPEC-008).
///
/// At most one `#[spatial(...)]` declaration per table: SPEC-008 models "the
/// table's spatial index" (SPX-020/021 route by table alone), so a second
/// declaration is rejected here.
pub(super) fn build_spatial_index(
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
pub(super) fn build_computed(
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
pub(super) type BuiltChecks = (
    HashMap<TableId, Vec<&'static crate::schema::CheckDef>>,
    HashMap<TableId, Vec<&'static crate::schema::NotNullDef>>,
);

pub(super) fn build_checks(catalog: &HashMap<TableId, &'static TableSchema>) -> BuiltChecks {
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
pub(super) type BuiltFks = (
    HashMap<TableId, Vec<ResolvedFk>>,
    HashMap<TableId, Vec<ResolvedFk>>,
);

/// Whether a child row's referencing value matches the deleted parent key
/// (RV-032): direct equality, or `Some(parent)` for an `Option`-typed column.
pub(super) fn fk_value_matches(value: Option<&RowValue>, parent: &RowValue) -> bool {
    match value {
        Some(RowValue::Optional(Some(inner))) => inner.as_ref() == parent,
        Some(other) => other == parent,
        None => false,
    }
}

pub(super) fn build_foreign_keys(
    catalog: &HashMap<TableId, &'static TableSchema>,
) -> Result<BuiltFks> {
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
pub(super) fn build_fulltext_indexes(
    table: &'static TableSchema,
    pager: &Arc<Pager>,
    table_id: TableId,
) -> Result<Vec<FullTextIndexState>> {
    use crate::index::{Analyzer, Language};
    let mut out = Vec::new();
    for index in table.indexes {
        let IndexSchema::FullText {
            column,
            language,
            stop_words,
            stemming,
        } = index
        else {
            continue;
        };
        let analyzer = Analyzer {
            language: match language {
                crate::schema::FullTextLanguage::Simple => Language::Simple,
                crate::schema::FullTextLanguage::English => Language::English,
            },
            stop_words: *stop_words,
            stemming: *stemming,
        };
        out.push(FullTextIndexState::new(*column, analyzer, pager, table_id)?);
    }
    Ok(out)
}

/// Empty secondary B-tree indexes for `table`, keyed by stable [`IndexId`]
/// (STG-051), one per `#[index(btree(...))]` declaration. Spatial
/// declarations are handled by [`build_spatial_index`].
/// Apply one commit's blob reference deltas (DMX-040): incref every `Blob`
/// value in inserted rows, then unref every one in deleted rows (an update
/// contributes both sides). Write-time validation guarantees every incref
/// target exists; count bookkeeping errors are logged, never a commit
/// failure — the snapshot already swapped.
pub(super) fn apply_blob_refcounts(
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
pub(super) fn blob_ordinals(schema: &TableSchema) -> Vec<u16> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.ty, crate::schema::FluxType::Blob))
        .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
        .collect()
}

pub(super) fn build_btree_indexes(
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
