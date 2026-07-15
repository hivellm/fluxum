//! [`MigrationContext`] — what a `#[fluxum::migration(version = N)]`
//! function receives (MIG-011): versioned DDL operations over the stored
//! layout plus full read/write access to the shard's data through the
//! migration step's own transaction.

use crate::error::{FluxumError, Result};
use crate::migration::catalog::{StoredCatalog, StoredColumn, StoredType};
use crate::schema::Schema;
use crate::store::{RowValue, TableId, Tx};

/// The context of one migration step (MIG-011).
///
/// Every operation is buffered in the step's single transaction (MIG-040):
/// if the migration function returns `Err` or panics, the whole step —
/// row rewrites, catalog updates, the version bump — rolls back and the
/// server refuses to start. Row-level visibility rules and reducer rate
/// limits do not apply here.
///
/// T3.6 exposes the additive/rename surface; see the module docs of
/// [`crate::migration`] for the scope of the remaining MIG-011 operations.
pub struct MigrationContext<'a, 'b> {
    /// `schema_version` stored when this startup's migration run began.
    pub from_version: u32,
    /// The version this migration step targets.
    pub to_version: u32,
    pub(crate) tx: &'a mut Tx<'b>,
    /// The stored layout as of this step (previous steps already applied).
    pub(crate) working: &'a mut StoredCatalog,
    pub(crate) compiled: &'a Schema,
}

impl<'b> MigrationContext<'_, 'b> {
    /// Full read/write access to the shard's data through this step's
    /// transaction (MIG-011): `scan`, `query_pk`, `insert`, `upsert`,
    /// `delete`, index and spatial reads. Writes are validated against the
    /// **compiled** schema — do data fixups on tables whose layout is
    /// already final (i.e. after this step's DDL calls).
    pub fn tx(&mut self) -> &mut Tx<'b> {
        self.tx
    }

    /// Add column `column` to `table`, backfilling every existing row with
    /// `default` (MIG-011; SPEC-010 acceptance 1).
    ///
    /// The column must exist in the compiled schema — declared **after**
    /// every column of the stored layout (rows are positional; the module
    /// docs of [`crate::migration`] explain the append-only rule) — and
    /// `default` must inhabit its type. Existing rows are rewritten inside
    /// this step's transaction; the stored catalog gains the column so the
    /// startup schema diff sees the layouts converge.
    pub fn add_column(&mut self, table: &str, column: &str, default: RowValue) -> Result<()> {
        let err = |msg: String| {
            FluxumError::Schema(format!("add_column(\"{table}\", \"{column}\"): {msg}"))
        };
        let compiled = self
            .compiled
            .table(table)
            .ok_or_else(|| err("table is not in the compiled schema".into()))?;
        let compiled_column = compiled
            .columns
            .iter()
            .find(|c| c.name == column)
            .ok_or_else(|| {
                err(
                    "column is not declared on the compiled table (add the field to the \
                     struct first)"
                        .into(),
                )
            })?;
        if !default.matches_type(&compiled_column.ty) {
            return Err(err(format!(
                "default value {default} does not inhabit the column type {:?}",
                compiled_column.ty
            )));
        }
        let working = self.working.tables.get_mut(table).ok_or_else(|| {
            err(
                "table is not in the stored catalog (new tables are created automatically \
                 by the startup schema diff, MIG-021)"
                    .into(),
            )
        })?;
        if working.has_column(column) {
            return Err(err("column already exists in the stored layout".into()));
        }

        append_column(self.tx, table, working.columns.len(), &default)?;
        working.columns.push(StoredColumn {
            name: column.to_owned(),
            ty: StoredType::from(&compiled_column.ty),
        });
        Ok(())
    }

    /// Rename column `from` to `to` on `table` (MIG-011; SPEC-010
    /// acceptance 2).
    ///
    /// Metadata-only: rows are positional value vectors, so every value is
    /// preserved in place — only the stored catalog changes. The new name
    /// must be declared on the compiled table with the **same** type
    /// (a rename never changes a column's type; that is a row-transform
    /// migration).
    pub fn rename_column(&mut self, table: &str, from: &str, to: &str) -> Result<()> {
        let err = |msg: String| {
            FluxumError::Schema(format!(
                "rename_column(\"{table}\", \"{from}\" -> \"{to}\"): {msg}"
            ))
        };
        if from == to {
            return Err(err("old and new names are identical".into()));
        }
        let compiled = self
            .compiled
            .table(table)
            .ok_or_else(|| err("table is not in the compiled schema".into()))?;
        let compiled_column = compiled
            .columns
            .iter()
            .find(|c| c.name == to)
            .ok_or_else(|| {
                err(
                    "new name is not declared on the compiled table (rename the struct \
                     field first)"
                        .into(),
                )
            })?;
        let working = self
            .working
            .tables
            .get_mut(table)
            .ok_or_else(|| err("table is not in the stored catalog".into()))?;
        if working.has_column(to) {
            return Err(err(
                "a column with the new name already exists in the stored \
                            layout"
                    .into(),
            ));
        }
        let column = working
            .columns
            .iter_mut()
            .find(|c| c.name == from)
            .ok_or_else(|| err("no such column in the stored layout".into()))?;
        let compiled_ty = StoredType::from(&compiled_column.ty);
        if column.ty != compiled_ty {
            return Err(err(format!(
                "rename cannot change the column type ({} -> {compiled_ty}); write a \
                 row-transform migration instead",
                column.ty
            )));
        }
        column.name = to.to_owned();
        Ok(())
    }
}

/// Rewrite every row of `table` appending `default` as a new trailing
/// column value, inside `tx` (shared by [`MigrationContext::add_column`]
/// and the runner's MIG-021 auto-apply).
///
/// Reads through the transaction's own pending rewrites
/// ([`Tx::migrate_rows`]), so consecutive appends on one table compose.
/// `old_arity` is the stored layout's column count before this append —
/// any row with a different value count means the stored catalog and the
/// data diverged, which aborts the migration.
pub(crate) fn append_column(
    tx: &mut Tx<'_>,
    table: &str,
    old_arity: usize,
    default: &RowValue,
) -> Result<()> {
    let id = TableId::of(table);
    let mut rewrites = Vec::new();
    for (pk, row) in tx.migrate_rows(id)? {
        if row.values().len() != old_arity {
            return Err(FluxumError::Schema(format!(
                "table `{table}`: committed row has {} values but the stored layout \
                 declares {old_arity} columns — the stored catalog and the data diverged",
                row.values().len()
            )));
        }
        let mut values = row.values().to_vec();
        values.push(default.clone());
        rewrites.push((pk, values));
    }
    for (pk, values) in rewrites {
        tx.migrate_replace(id, pk, values)?;
    }
    Ok(())
}
