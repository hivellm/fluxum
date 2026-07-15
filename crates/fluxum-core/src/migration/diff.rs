//! Automatic schema diff (MIG-020): stored catalog vs compiled schema,
//! classified into safe auto-applied changes (MIG-021) and incompatible
//! changes that abort startup (MIG-022).

use std::collections::BTreeSet;

use crate::migration::catalog::{StoredCatalog, StoredType};
use crate::migration::{TableColumnMeta, registered_column_meta};
use crate::store::RowValue;

/// One detected difference between the stored catalog and the compiled
/// schema (the MIG-020 change table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// Safe (MIG-021): table in the compiled schema, not in the stored
    /// catalog — created automatically (the store starts it empty).
    AddTable {
        /// New table name.
        table: String,
    },
    /// Safe (MIG-021): new column carrying `#[default(value)]`, appended
    /// after every stored column — existing rows are backfilled.
    AddColumnWithDefault {
        /// Table name.
        table: String,
        /// New column name.
        column: String,
    },
    /// Safe (MIG-021): compiled column carries `#[rename(from = "old")]`
    /// and the stored layout has `old` in its place — renamed in place,
    /// values untouched.
    RenameColumn {
        /// Table name.
        table: String,
        /// Stored (old) column name.
        from: String,
        /// Compiled (new) column name.
        to: String,
    },
    /// Incompatible (MIG-022): new column without `#[default]` — existing
    /// rows have no value for it.
    AddColumnNoDefault {
        /// Table name.
        table: String,
        /// New column name.
        column: String,
    },
    /// Incompatible (MIG-022): stored column absent from the compiled
    /// schema (and not renamed away).
    RemoveColumn {
        /// Table name.
        table: String,
        /// Removed column name.
        column: String,
    },
    /// Incompatible (MIG-022): stored table absent from the compiled
    /// schema.
    RemoveTable {
        /// Removed table name.
        table: String,
    },
    /// Incompatible (MIG-022): a column's type differs between the stored
    /// and compiled layouts.
    ChangeColumnType {
        /// Table name.
        table: String,
        /// Column name (compiled name if renamed).
        column: String,
        /// Type in the stored catalog.
        stored: StoredType,
        /// Type in the compiled schema.
        compiled: StoredType,
    },
    /// Incompatible (MIG-022): the primary-key ordinals differ.
    ChangePrimaryKey {
        /// Table name.
        table: String,
    },
    /// Incompatible: a new column was inserted before existing columns, or
    /// existing columns were reordered — rows are positional, so recovery
    /// would misread every pre-migration row.
    ReorderColumns {
        /// Table name.
        table: String,
        /// First offending column.
        column: String,
    },
}

impl SchemaChange {
    /// Whether this change is auto-applied at startup (MIG-021) rather than
    /// aborting it (MIG-022).
    pub fn is_safe(&self) -> bool {
        matches!(
            self,
            Self::AddTable { .. } | Self::AddColumnWithDefault { .. } | Self::RenameColumn { .. }
        )
    }

    /// The table this change belongs to.
    pub fn table(&self) -> &str {
        match self {
            Self::AddTable { table }
            | Self::AddColumnWithDefault { table, .. }
            | Self::RenameColumn { table, .. }
            | Self::AddColumnNoDefault { table, .. }
            | Self::RemoveColumn { table, .. }
            | Self::RemoveTable { table }
            | Self::ChangeColumnType { table, .. }
            | Self::ChangePrimaryKey { table }
            | Self::ReorderColumns { table, .. } => table,
        }
    }

    /// The MIG-020 "required action" column, for startup diagnostics.
    pub fn required_action(&self) -> &'static str {
        match self {
            Self::AddTable { .. } => "created automatically (MIG-021)",
            Self::AddColumnWithDefault { .. } => {
                "existing rows backfilled from #[default] automatically (MIG-021)"
            }
            Self::RenameColumn { .. } => "renamed in place via #[rename(from)] (MIG-021)",
            Self::AddColumnNoDefault { .. } => {
                "add #[default(value)] to the field, or write a #[fluxum::migration] calling \
                 ctx.add_column (MIG-022)"
            }
            Self::RemoveColumn { .. } => {
                "destructive change: requires an explicit migration; not auto-applied (MIG-022)"
            }
            Self::RemoveTable { .. } => {
                "destructive change: requires an explicit migration; not auto-applied (MIG-022)"
            }
            Self::ChangeColumnType { .. } => {
                "type changes require an explicit row-transform migration; not auto-applied \
                 (MIG-022)"
            }
            Self::ChangePrimaryKey { .. } => {
                "primary-key changes require an explicit migration; not auto-applied (MIG-022)"
            }
            Self::ReorderColumns { .. } => {
                "rows are positional: declare new columns after every existing column (MIG-022)"
            }
        }
    }
}

impl std::fmt::Display for SchemaChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddTable { table } => write!(f, "table `{table}`: new table"),
            Self::AddColumnWithDefault { table, column } => {
                write!(
                    f,
                    "table `{table}`, column `{column}`: new column with #[default]"
                )
            }
            Self::RenameColumn { table, from, to } => {
                write!(f, "table `{table}`: column `{from}` renamed to `{to}`")
            }
            Self::AddColumnNoDefault { table, column } => {
                write!(
                    f,
                    "table `{table}`, column `{column}`: new column without #[default]"
                )
            }
            Self::RemoveColumn { table, column } => {
                write!(f, "table `{table}`, column `{column}`: column removed")
            }
            Self::RemoveTable { table } => write!(f, "table `{table}`: table removed"),
            Self::ChangeColumnType {
                table,
                column,
                stored,
                compiled,
            } => write!(
                f,
                "table `{table}`, column `{column}`: column type changed ({stored} -> {compiled})"
            ),
            Self::ChangePrimaryKey { table } => {
                write!(f, "table `{table}`: primary key changed")
            }
            Self::ReorderColumns { table, column } => write!(
                f,
                "table `{table}`, column `{column}`: column inserted before existing columns \
                 or layout reordered"
            ),
        }
    }
}

/// Lookup view over the `#[default]` / `#[rename]` link-time metadata.
pub(crate) struct MetaIndex<'a> {
    metas: Vec<&'a TableColumnMeta>,
}

impl<'a> MetaIndex<'a> {
    /// Build from an explicit metadata list (test seam; [`MetaIndex::collect`]
    /// is the production path).
    pub(crate) fn new(metas: &[&'a TableColumnMeta]) -> Self {
        Self {
            metas: metas.to_vec(),
        }
    }

    /// Build from the link-time registry.
    pub(crate) fn collect() -> MetaIndex<'static> {
        MetaIndex {
            metas: registered_column_meta().collect(),
        }
    }

    /// The `#[default(value)]` constructor of `table.column`, if declared.
    pub(crate) fn default_of(&self, table: &str, column: &str) -> Option<fn() -> RowValue> {
        self.metas
            .iter()
            .filter(|meta| meta.table == table)
            .flat_map(|meta| meta.defaults)
            .find(|default| default.column == column)
            .map(|default| default.value)
    }

    /// The `#[rename(from = "…")]` source of `table.column`, if declared.
    pub(crate) fn rename_from(&self, table: &str, column: &str) -> Option<&'static str> {
        self.metas
            .iter()
            .filter(|meta| meta.table == table)
            .flat_map(|meta| meta.renames)
            .find(|rename| rename.column == column)
            .map(|rename| rename.from)
    }
}

/// Diff the stored catalog against the compiled catalog (MIG-020), using
/// the link-time `#[default]` / `#[rename]` metadata registered in this
/// binary to classify additive changes.
pub fn diff_catalogs(stored: &StoredCatalog, compiled: &StoredCatalog) -> Vec<SchemaChange> {
    diff_catalogs_with(stored, compiled, &MetaIndex::collect())
}

/// [`diff_catalogs`] with explicit metadata (test seam and runner path).
pub(crate) fn diff_catalogs_with(
    stored: &StoredCatalog,
    compiled: &StoredCatalog,
    meta: &MetaIndex<'_>,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();
    let names: BTreeSet<&String> = stored.tables.keys().chain(compiled.tables.keys()).collect();
    for name in names {
        match (stored.tables.get(name), compiled.tables.get(name)) {
            (None, Some(_)) => changes.push(SchemaChange::AddTable {
                table: name.clone(),
            }),
            (Some(_), None) => changes.push(SchemaChange::RemoveTable {
                table: name.clone(),
            }),
            (Some(stored_table), Some(compiled_table)) => {
                diff_table(name, stored_table, compiled_table, meta, &mut changes);
            }
            (None, None) => unreachable!("name came from one of the two maps"),
        }
    }
    changes
}

/// Diff one table present in both catalogs.
fn diff_table(
    table: &str,
    stored: &super::StoredTable,
    compiled: &super::StoredTable,
    meta: &MetaIndex<'_>,
    changes: &mut Vec<SchemaChange>,
) {
    // 1. Resolve `#[rename(from = "old")]`: a compiled column absent from
    //    the stored layout whose declared source exists there (and is gone
    //    from the compiled layout) maps the stored column to its new name.
    let mut mapped: Vec<super::StoredColumn> = stored.columns.clone();
    for compiled_column in &compiled.columns {
        if stored.has_column(&compiled_column.name) {
            continue;
        }
        let Some(from) = meta.rename_from(table, &compiled_column.name) else {
            continue;
        };
        if !stored.has_column(from) || compiled.columns.iter().any(|c| c.name == from) {
            continue;
        }
        for column in &mut mapped {
            if column.name == from {
                column.name = compiled_column.name.clone();
                changes.push(SchemaChange::RenameColumn {
                    table: table.to_owned(),
                    from: from.to_owned(),
                    to: compiled_column.name.clone(),
                });
            }
        }
    }

    let mapped_names: BTreeSet<&str> = mapped.iter().map(|c| c.name.as_str()).collect();
    let compiled_names: BTreeSet<&str> = compiled.columns.iter().map(|c| c.name.as_str()).collect();

    // 2. Removals and type changes over the (rename-resolved) stored layout.
    for column in &mapped {
        match compiled.columns.iter().find(|c| c.name == column.name) {
            None => changes.push(SchemaChange::RemoveColumn {
                table: table.to_owned(),
                column: column.name.clone(),
            }),
            Some(compiled_column) if compiled_column.ty != column.ty => {
                changes.push(SchemaChange::ChangeColumnType {
                    table: table.to_owned(),
                    column: column.name.clone(),
                    stored: column.ty.clone(),
                    compiled: compiled_column.ty.clone(),
                });
            }
            Some(_) => {}
        }
    }

    // 3. Additions: a compiled column absent from the stored layout is a
    //    safe append only when no stored column follows it (rows are
    //    positional — mid-layout insertions would shift every later
    //    ordinal under pre-migration rows).
    for (position, column) in compiled.columns.iter().enumerate() {
        if mapped_names.contains(column.name.as_str()) {
            continue;
        }
        let appended = compiled.columns[position + 1..]
            .iter()
            .all(|later| !mapped_names.contains(later.name.as_str()));
        if !appended {
            changes.push(SchemaChange::ReorderColumns {
                table: table.to_owned(),
                column: column.name.clone(),
            });
        } else if meta.default_of(table, &column.name).is_some() {
            changes.push(SchemaChange::AddColumnWithDefault {
                table: table.to_owned(),
                column: column.name.clone(),
            });
        } else {
            changes.push(SchemaChange::AddColumnNoDefault {
                table: table.to_owned(),
                column: column.name.clone(),
            });
        }
    }

    // 4. Common columns must keep their relative order.
    let order_stored: Vec<&str> = mapped
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| compiled_names.contains(name))
        .collect();
    let order_compiled: Vec<&str> = compiled
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| mapped_names.contains(name))
        .collect();
    if let Some((stored_name, _)) = order_stored
        .iter()
        .zip(&order_compiled)
        .find(|(s, c)| s != c)
    {
        changes.push(SchemaChange::ReorderColumns {
            table: table.to_owned(),
            column: (*stored_name).to_owned(),
        });
    }

    // 5. Primary key must be identical (ordinals and order).
    if stored.primary_key != compiled.primary_key {
        changes.push(SchemaChange::ChangePrimaryKey {
            table: table.to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::{ColumnDefault, ColumnRename, StoredColumn, StoredTable};

    fn table(columns: &[(&str, StoredType)], pk: &[u16]) -> StoredTable {
        StoredTable {
            columns: columns
                .iter()
                .map(|(name, ty)| StoredColumn {
                    name: (*name).to_owned(),
                    ty: ty.clone(),
                })
                .collect(),
            primary_key: pk.to_vec(),
        }
    }

    fn catalog(tables: &[(&str, StoredTable)]) -> StoredCatalog {
        StoredCatalog {
            tables: tables
                .iter()
                .map(|(name, t)| ((*name).to_owned(), t.clone()))
                .collect(),
        }
    }

    fn no_meta() -> MetaIndex<'static> {
        MetaIndex::new(&[])
    }

    #[test]
    fn identical_catalogs_have_no_diff() {
        let c = catalog(&[(
            "Task",
            table(&[("id", StoredType::U64), ("title", StoredType::Str)], &[0]),
        )]);
        assert!(diff_catalogs_with(&c, &c, &no_meta()).is_empty());
    }

    #[test]
    fn added_and_removed_tables_are_detected() {
        let stored = catalog(&[("Old", table(&[("id", StoredType::U64)], &[0]))]);
        let compiled = catalog(&[("New", table(&[("id", StoredType::U64)], &[0]))]);
        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert_eq!(
            changes,
            vec![
                SchemaChange::AddTable {
                    table: "New".into()
                },
                SchemaChange::RemoveTable {
                    table: "Old".into()
                },
            ]
        );
        assert!(changes[0].is_safe());
        assert!(!changes[1].is_safe());
    }

    #[test]
    fn appended_column_classified_by_default_presence() {
        let stored = catalog(&[("Task", table(&[("id", StoredType::U64)], &[0]))]);
        let compiled = catalog(&[(
            "Task",
            table(
                &[("id", StoredType::U64), ("priority", StoredType::U8)],
                &[0],
            ),
        )]);

        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert_eq!(
            changes,
            vec![SchemaChange::AddColumnNoDefault {
                table: "Task".into(),
                column: "priority".into()
            }]
        );
        assert!(!changes[0].is_safe());

        static DEFAULTS: &[ColumnDefault] = &[ColumnDefault {
            column: "priority",
            value: || RowValue::U8(0),
        }];
        static META: TableColumnMeta = TableColumnMeta {
            table: "Task",
            defaults: DEFAULTS,
            renames: &[],
        };
        let changes = diff_catalogs_with(&stored, &compiled, &MetaIndex::new(&[&META]));
        assert_eq!(
            changes,
            vec![SchemaChange::AddColumnWithDefault {
                table: "Task".into(),
                column: "priority".into()
            }]
        );
        assert!(changes[0].is_safe());
    }

    #[test]
    fn rename_attribute_maps_the_stored_column() {
        let stored = catalog(&[(
            "Sensor",
            table(
                &[("id", StoredType::U64), ("reading", StoredType::F64)],
                &[0],
            ),
        )]);
        let compiled = catalog(&[(
            "Sensor",
            table(&[("id", StoredType::U64), ("value", StoredType::F64)], &[0]),
        )]);

        // Without #[rename]: a remove + an add — both incompatible sides.
        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::RemoveColumn { .. }))
        );
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::AddColumnNoDefault { .. }))
        );

        static RENAMES: &[ColumnRename] = &[ColumnRename {
            column: "value",
            from: "reading",
        }];
        static META: TableColumnMeta = TableColumnMeta {
            table: "Sensor",
            defaults: &[],
            renames: RENAMES,
        };
        let changes = diff_catalogs_with(&stored, &compiled, &MetaIndex::new(&[&META]));
        assert_eq!(
            changes,
            vec![SchemaChange::RenameColumn {
                table: "Sensor".into(),
                from: "reading".into(),
                to: "value".into()
            }]
        );
        assert!(changes[0].is_safe());
    }

    #[test]
    fn type_change_and_pk_change_are_incompatible() {
        let stored = catalog(&[(
            "Sensor",
            table(
                &[("id", StoredType::U64), ("reading", StoredType::F32)],
                &[0],
            ),
        )]);
        let compiled = catalog(&[(
            "Sensor",
            table(
                &[("id", StoredType::U64), ("reading", StoredType::F64)],
                &[1],
            ),
        )]);
        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert_eq!(
            changes,
            vec![
                SchemaChange::ChangeColumnType {
                    table: "Sensor".into(),
                    column: "reading".into(),
                    stored: StoredType::F32,
                    compiled: StoredType::F64,
                },
                SchemaChange::ChangePrimaryKey {
                    table: "Sensor".into()
                },
            ]
        );
        assert!(changes.iter().all(|c| !c.is_safe()));
        let rendered = changes[0].to_string();
        assert!(rendered.contains("Sensor"), "{rendered}");
        assert!(rendered.contains("reading"), "{rendered}");
        assert!(rendered.contains("F32 -> F64"), "{rendered}");
    }

    #[test]
    fn mid_layout_insertion_is_a_reorder() {
        let stored = catalog(&[(
            "Task",
            table(&[("id", StoredType::U64), ("title", StoredType::Str)], &[0]),
        )]);
        let compiled = catalog(&[(
            "Task",
            table(
                &[
                    ("id", StoredType::U64),
                    ("priority", StoredType::U8),
                    ("title", StoredType::Str),
                ],
                &[0],
            ),
        )]);
        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert_eq!(
            changes,
            vec![SchemaChange::ReorderColumns {
                table: "Task".into(),
                column: "priority".into()
            }]
        );
        assert!(!changes[0].is_safe());
    }

    #[test]
    fn swapped_columns_are_a_reorder() {
        let stored = catalog(&[(
            "Task",
            table(
                &[
                    ("id", StoredType::U64),
                    ("a", StoredType::U8),
                    ("b", StoredType::U8),
                ],
                &[0],
            ),
        )]);
        let compiled = catalog(&[(
            "Task",
            table(
                &[
                    ("id", StoredType::U64),
                    ("b", StoredType::U8),
                    ("a", StoredType::U8),
                ],
                &[0],
            ),
        )]);
        let changes = diff_catalogs_with(&stored, &compiled, &no_meta());
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ReorderColumns { .. })),
            "{changes:?}"
        );
    }
}
