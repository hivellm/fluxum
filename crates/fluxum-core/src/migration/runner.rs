//! The startup migration runner (SPEC-010): version comparison, ordered
//! `#[fluxum::migration]` execution, automatic schema diff, safe
//! auto-apply, and every abort path — MIG-001..MIG-003, MIG-010..MIG-012,
//! MIG-020..MIG-023, MIG-040.

use std::panic::AssertUnwindSafe;

use crate::commitlog::CommitLog;
use crate::error::{FluxumError, Result};
use crate::migration::catalog::{StoredCatalog, decode_version, encode_version};
use crate::migration::context::{MigrationContext, append_column};
use crate::migration::diff::{MetaIndex, SchemaChange, diff_catalogs_with};
use crate::migration::{
    META_KEY_CATALOG, META_KEY_VERSION, META_TABLE, MigrationDef, TableColumnMeta,
    declared_schema_version, registered_migrations,
};
use crate::schema::Schema;
use crate::store::{MemStore, RowValue, TableId, Tx};
use crate::txn::panic_message;
use crate::types::Timestamp;

/// One executed migration step, for the [`MigrationReport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedMigration {
    /// The version the step migrated to.
    pub version: u32,
    /// The migration function's name.
    pub name: &'static str,
}

/// What a startup migration run did.
#[derive(Debug)]
pub struct MigrationReport {
    /// Whether this was the first startup (no `__schema_meta__` yet):
    /// the compiled schema was adopted verbatim (MIG-002).
    pub first_boot: bool,
    /// `schema_version` stored when the run began.
    pub from_version: u32,
    /// `schema_version` stored when the run finished (the code's
    /// `SCHEMA_VERSION`).
    pub to_version: u32,
    /// `#[fluxum::migration]` steps executed, in ascending version order.
    pub applied: Vec<AppliedMigration>,
    /// Safe schema changes auto-applied by the startup diff (MIG-021).
    pub auto_applied: Vec<SchemaChange>,
}

/// The startup migration runner (SPEC-010).
///
/// Runs after recovery ([`crate::checkpoint::recover`]) and **before** the
/// shard is marked READY: the server assembly must not open any transport
/// or accept reducer calls until [`MigrationRunner::run`] returns `Ok` —
/// an `Err` aborts startup with the stored data untouched by the failing
/// step (MIG-022/MIG-040).
pub struct MigrationRunner<'a> {
    store: &'a MemStore,
    log: &'a CommitLog,
    schema: &'a Schema,
    meta_id: TableId,
}

impl<'a> MigrationRunner<'a> {
    /// Build a runner over a recovered store and its shard's commit log.
    ///
    /// `schema` must be the same assembled schema the store was built over
    /// and must include [`super::SCHEMA_META`] (the runtime-owned
    /// `__schema_meta__` table, MIG-002).
    pub fn new(store: &'a MemStore, log: &'a CommitLog, schema: &'a Schema) -> Result<Self> {
        if schema.table(META_TABLE).is_none() {
            return Err(FluxumError::Schema(format!(
                "the assembled schema does not include `{META_TABLE}`: add \
                 migration::SCHEMA_META when building the Schema (MIG-002)"
            )));
        }
        let meta_id = store.table_id(META_TABLE).ok_or_else(|| {
            FluxumError::Schema(format!(
                "the store was assembled without `{META_TABLE}` (MIG-002)"
            ))
        })?;
        Ok(Self {
            store,
            log,
            schema,
            meta_id,
        })
    }

    /// Run the full startup migration sequence with the link-time
    /// registrations of this binary: `fluxum::schema_version!`,
    /// every `#[fluxum::migration]`, and every `#[default]`/`#[rename]`
    /// column annotation.
    pub async fn run(&self) -> Result<MigrationReport> {
        let code_version = declared_schema_version()?;
        let migrations: Vec<&MigrationDef> = registered_migrations().collect();
        self.run_inner(code_version, &migrations, &MetaIndex::collect())
            .await
    }

    /// [`MigrationRunner::run`] with explicit inputs instead of the
    /// link-time registries — the seam tests and embedders use to simulate
    /// binaries at different schema versions.
    pub async fn run_with(
        &self,
        code_version: u32,
        migrations: &[&MigrationDef],
        column_meta: &[&TableColumnMeta],
    ) -> Result<MigrationReport> {
        self.run_inner(code_version, migrations, &MetaIndex::new(column_meta))
            .await
    }

    async fn run_inner(
        &self,
        code_version: u32,
        migrations: &[&MigrationDef],
        meta: &MetaIndex<'_>,
    ) -> Result<MigrationReport> {
        if code_version == 0 {
            return Err(FluxumError::Schema(
                "SCHEMA_VERSION 0 is invalid: versions start at 1 (MIG-001)".into(),
            ));
        }
        let sorted = validate_migrations(migrations, code_version)?;
        let compiled = StoredCatalog::from_schema(self.schema);

        // MIG-002: first startup writes schema_version + schema_catalog.
        let Some(version_bytes) = self.read_meta(META_KEY_VERSION)? else {
            let mut tx = self.store.begin();
            self.write_meta(&mut tx, META_KEY_VERSION, encode_version(code_version)?)?;
            self.write_meta(&mut tx, META_KEY_CATALOG, compiled.encode()?)?;
            let last_tx = self.commit_and_log(tx).await?;
            self.log.wait_durable(last_tx).await?;
            tracing::info!(
                target: "fluxum::migration",
                version = code_version,
                "first startup: adopted the compiled schema (MIG-002)"
            );
            return Ok(MigrationReport {
                first_boot: true,
                from_version: code_version,
                to_version: code_version,
                applied: Vec::new(),
                auto_applied: Vec::new(),
            });
        };
        let stored_version = decode_version(&version_bytes)?;

        // MIG-003: downgrade rejection, before anything else.
        if code_version < stored_version {
            return Err(FluxumError::Schema(format!(
                "FATAL: schema downgrade detected (code={code_version} < \
                 stored={stored_version}). Aborting."
            )));
        }

        let catalog_bytes = self.read_meta(META_KEY_CATALOG)?.ok_or_else(|| {
            FluxumError::Storage(format!(
                "__schema_meta__.{META_KEY_VERSION} is present but \
                 __schema_meta__.{META_KEY_CATALOG} is missing — the schema metadata is \
                 corrupt"
            ))
        })?;
        let mut working = StoredCatalog::decode(&catalog_bytes)?;

        // MIG-010/MIG-012: pending steps in ascending version order, one
        // transaction each, resuming past the stored version.
        let mut applied = Vec::new();
        let mut last_tx = None;
        for def in sorted
            .iter()
            .filter(|def| def.version > stored_version && def.version <= code_version)
        {
            let tx_id = self.run_step(def, stored_version, &mut working).await?;
            last_tx = Some(tx_id);
            applied.push(AppliedMigration {
                version: def.version,
                name: def.name,
            });
            tracing::info!(
                target: "fluxum::migration",
                version = def.version,
                name = def.name,
                "migration step committed (MIG-012)"
            );
        }

        // MIG-020: diff what is now stored against the compiled schema.
        let changes = diff_catalogs_with(&working, &compiled, meta);

        // MIG-023: a schema change without a version bump never boots.
        if !changes.is_empty() && code_version == stored_version {
            return Err(FluxumError::Schema(format!(
                "schema changed but SCHEMA_VERSION is still {code_version}: bump \
                 fluxum::schema_version! so the change deploys through the migration path \
                 (MIG-023). Detected changes:\n{}",
                render_changes(&changes)
            )));
        }

        // MIG-022: destructive or ambiguous changes abort startup with the
        // stored data untouched.
        let incompatible: Vec<&SchemaChange> =
            changes.iter().filter(|change| !change.is_safe()).collect();
        if !incompatible.is_empty() {
            let mut lines = String::new();
            for change in &incompatible {
                lines.push_str(&format!("  - {change} — {}\n", change.required_action()));
            }
            return Err(FluxumError::Schema(format!(
                "incompatible schema changes require an explicit #[fluxum::migration]; \
                 refusing to start, stored data untouched (MIG-022):\n{lines}"
            )));
        }

        // MIG-021 + MIG-020 tail: auto-apply the safe changes and persist
        // the compiled catalog / final version in one startup transaction.
        // Steps already stamped their own version (MIG-012), so only a
        // remaining gap to `code_version` — or a non-empty diff — commits.
        let current_version = applied.last().map_or(stored_version, |step| step.version);
        if !changes.is_empty() || current_version != code_version {
            let mut tx = self.store.begin();
            auto_apply(
                &mut tx,
                self.schema,
                &changes,
                &mut working,
                &compiled,
                meta,
            )?;
            if working != compiled {
                return Err(FluxumError::Schema(
                    "internal invariant violated: the stored catalog did not converge to \
                     the compiled schema after auto-apply (MIG-021)"
                        .into(),
                ));
            }
            self.write_meta(&mut tx, META_KEY_VERSION, encode_version(code_version)?)?;
            self.write_meta(&mut tx, META_KEY_CATALOG, compiled.encode()?)?;
            last_tx = Some(self.commit_and_log(tx).await?);
            for change in &changes {
                tracing::info!(
                    target: "fluxum::migration",
                    version = code_version,
                    "auto-applied schema change: {change} (MIG-021)"
                );
            }
        }

        // Migrations are startup-critical: gate READY on durability rather
        // than the reducer path's asynchronous handoff (TXN-004).
        if let Some(tx_id) = last_tx {
            self.log.wait_durable(tx_id).await?;
        }

        Ok(MigrationReport {
            first_boot: false,
            from_version: stored_version,
            to_version: code_version,
            applied,
            auto_applied: changes,
        })
    }

    /// Execute one migration step in its own transaction (MIG-012/MIG-040):
    /// run the function under a panic boundary, persist the step's version
    /// and catalog inside the same transaction, commit, and append to the
    /// commit log. `Err` or panic rolls the whole step back.
    async fn run_step(
        &self,
        def: &MigrationDef,
        from_version: u32,
        working: &mut StoredCatalog,
    ) -> Result<u64> {
        let mut tx = self.store.begin();
        // A failed step aborts startup, so a partially mutated working
        // catalog is never observed — it is dropped with the error.
        let outcome = {
            let mut ctx = MigrationContext {
                from_version,
                to_version: def.version,
                tx: &mut tx,
                working,
                compiled: self.schema,
            };
            // AssertUnwindSafe: on unwind `tx` survives (only borrowed) and
            // is rolled back below — same boundary as the reducer pipeline
            // (TXN-022); `working` is discarded with the startup error.
            std::panic::catch_unwind(AssertUnwindSafe(|| (def.run)(&mut ctx)))
        };
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tx.rollback();
                return Err(FluxumError::Schema(format!(
                    "migration `{}` (version {}) failed: {e}; transaction rolled back, \
                     refusing to start (MIG-040)",
                    def.name, def.version
                )));
            }
            Err(payload) => {
                tx.rollback();
                return Err(FluxumError::Schema(format!(
                    "migration `{}` (version {}) panicked: {}; transaction rolled back, \
                     refusing to start (MIG-040)",
                    def.name,
                    def.version,
                    panic_message(payload.as_ref())
                )));
            }
        }
        self.write_meta(&mut tx, META_KEY_VERSION, encode_version(def.version)?)?;
        self.write_meta(&mut tx, META_KEY_CATALOG, working.encode()?)?;
        let tx_id = self.commit_and_log(tx).await?;
        // A committed step is durable before the next one runs: if a later
        // step aborts startup (or the process dies), the resume path reads
        // this step's version stamp and never re-runs it (MIG-012).
        self.log.wait_durable(tx_id).await?;
        Ok(tx_id)
    }

    /// Read a `__schema_meta__` value from the committed state.
    fn read_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let snapshot = self.store.snapshot();
        let Some(row) = snapshot.query_pk(self.meta_id, &[RowValue::Str(key.to_owned())])? else {
            return Ok(None);
        };
        match row.value(1) {
            Some(RowValue::Bytes(bytes)) => Ok(Some(bytes.clone())),
            other => Err(FluxumError::Storage(format!(
                "__schema_meta__.{key} holds {other:?} instead of Bytes — the schema \
                 metadata is corrupt"
            ))),
        }
    }

    /// Upsert a `__schema_meta__` key inside `tx`.
    fn write_meta(&self, tx: &mut Tx<'_>, key: &str, value: Vec<u8>) -> Result<()> {
        tx.upsert(
            self.meta_id,
            vec![RowValue::Str(key.to_owned()), RowValue::Bytes(value)],
        )?;
        Ok(())
    }

    /// Commit `tx` and append its diff to the commit log (every commit is
    /// logged, TXN-030). Returns the committed tx id.
    async fn commit_and_log(&self, tx: Tx<'_>) -> Result<u64> {
        let diff = tx.commit()?;
        let tx_id = diff.tx_id;
        self.log.append_diff(&diff, Timestamp::now()).await?;
        Ok(tx_id)
    }
}

/// Sort migrations ascending and reject invalid sets: duplicate versions
/// (MIG-010), versions below 2, and versions above the code's
/// `SCHEMA_VERSION`.
fn validate_migrations<'d>(
    migrations: &[&'d MigrationDef],
    code_version: u32,
) -> Result<Vec<&'d MigrationDef>> {
    let mut sorted: Vec<&MigrationDef> = migrations.to_vec();
    sorted.sort_by_key(|def| def.version);
    for pair in sorted.windows(2) {
        if pair[0].version == pair[1].version {
            return Err(FluxumError::Schema(format!(
                "migrations `{}` and `{}` both declare version {}: each version has \
                 exactly one migration (MIG-010)",
                pair[0].name, pair[1].name, pair[0].version
            )));
        }
    }
    for def in &sorted {
        if def.version < 2 {
            return Err(FluxumError::Schema(format!(
                "migration `{}` declares version {}: version 1 is the initial schema, \
                 migrations start at 2 (MIG-010)",
                def.name, def.version
            )));
        }
        if def.version > code_version {
            return Err(FluxumError::Schema(format!(
                "migration `{}` targets version {} but SCHEMA_VERSION is {code_version}: \
                 bump fluxum::schema_version! (MIG-001/MIG-023)",
                def.name, def.version
            )));
        }
    }
    Ok(sorted)
}

/// Apply the (all-safe) diff inside the single MIG-021 startup transaction,
/// mutating `working` toward the compiled catalog.
fn auto_apply(
    tx: &mut Tx<'_>,
    schema: &Schema,
    changes: &[SchemaChange],
    working: &mut StoredCatalog,
    compiled: &StoredCatalog,
    meta: &MetaIndex<'_>,
) -> Result<()> {
    for change in changes {
        match change {
            SchemaChange::AddTable { table } => {
                let layout = compiled.tables.get(table).ok_or_else(|| {
                    FluxumError::Schema(format!(
                        "internal invariant violated: added table `{table}` missing from \
                         the compiled catalog"
                    ))
                })?;
                working.tables.insert(table.clone(), layout.clone());
            }
            SchemaChange::RenameColumn { table, from, to } => {
                let layout = working.tables.get_mut(table).ok_or_else(|| {
                    FluxumError::Schema(format!(
                        "internal invariant violated: renamed table `{table}` missing from \
                         the stored catalog"
                    ))
                })?;
                for column in &mut layout.columns {
                    if column.name == *from {
                        column.name = to.clone();
                    }
                }
            }
            SchemaChange::AddColumnWithDefault { table, column } => {
                let default_fn = meta.default_of(table, column).ok_or_else(|| {
                    FluxumError::Schema(format!(
                        "internal invariant violated: #[default] for `{table}.{column}` \
                         vanished between diff and apply"
                    ))
                })?;
                let default = default_fn();
                let flux_column = schema
                    .table(table)
                    .and_then(|t| t.columns.iter().find(|c| c.name == column))
                    .ok_or_else(|| {
                        FluxumError::Schema(format!(
                            "internal invariant violated: added column `{table}.{column}` \
                             missing from the compiled schema"
                        ))
                    })?;
                if !default.matches_type(&flux_column.ty) {
                    return Err(FluxumError::Schema(format!(
                        "table `{table}`, column `{column}`: the #[default] value \
                         {default} does not inhabit the column type {:?} (MIG-021)",
                        flux_column.ty
                    )));
                }
                let layout = working.tables.get_mut(table).ok_or_else(|| {
                    FluxumError::Schema(format!(
                        "internal invariant violated: table `{table}` missing from the \
                         stored catalog"
                    ))
                })?;
                append_column(tx, table, layout.columns.len(), &default)?;
                layout.columns.push(crate::migration::StoredColumn {
                    name: column.clone(),
                    ty: crate::migration::StoredType::from(&flux_column.ty),
                });
            }
            other => {
                return Err(FluxumError::Schema(format!(
                    "internal invariant violated: incompatible change `{other}` reached \
                     auto-apply (MIG-022 must reject it first)"
                )));
            }
        }
    }
    Ok(())
}

/// Render a change list for diagnostics, one indented line per change.
fn render_changes(changes: &[SchemaChange]) -> String {
    let mut lines = String::new();
    for change in changes {
        lines.push_str(&format!("  - {change} — {}\n", change.required_action()));
    }
    lines
}

#[cfg(test)]
mod tests {
    //! `auto_apply` defends MIG-021 invariants that the public runner path
    //! can only reach through corrupted inputs; each guard is probed here
    //! directly.
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::BTreeMap;

    use super::*;
    use crate::migration::catalog::{StoredColumn, StoredTable, StoredType};
    use crate::migration::{ColumnDefault, TableColumnMeta};
    use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};

    const fn task_table(name: &'static str, columns: &'static [ColumnSchema]) -> TableSchema {
        TableSchema {
            name,
            columns,
            primary_key: &[0],
            auto_inc: None,
            access: TableAccess::Public,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        }
    }

    static TWO_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "title",
            ty: FluxType::Str,
        },
    ];
    static TASK: TableSchema = task_table("Task", TWO_COLS);

    static PRIORITY_STR_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "title",
            ty: FluxType::Str,
        },
        ColumnSchema {
            name: "priority",
            ty: FluxType::Str,
        },
    ];
    static TASK_PRIORITY_STR: TableSchema = task_table("Task", PRIORITY_STR_COLS);

    static PRIORITY_DEFAULTS: &[ColumnDefault] = &[ColumnDefault {
        column: "priority",
        value: || RowValue::U8(0),
    }];
    static TASK_META: TableColumnMeta = TableColumnMeta {
        table: "Task",
        defaults: PRIORITY_DEFAULTS,
        renames: &[],
    };

    fn stored_task() -> StoredTable {
        StoredTable {
            columns: vec![
                StoredColumn {
                    name: "id".into(),
                    ty: StoredType::U64,
                },
                StoredColumn {
                    name: "title".into(),
                    ty: StoredType::Str,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn task_catalog() -> StoredCatalog {
        let mut tables = BTreeMap::new();
        tables.insert("Task".to_owned(), stored_task());
        StoredCatalog { tables }
    }

    fn apply(
        table: &'static TableSchema,
        changes: &[SchemaChange],
        working: &mut StoredCatalog,
        compiled: &StoredCatalog,
        meta: &MetaIndex<'_>,
    ) -> String {
        let schema = Schema::from_tables([table]).unwrap();
        let store = MemStore::new(&schema).unwrap();
        let mut tx = store.begin();
        let err = auto_apply(&mut tx, &schema, changes, working, compiled, meta)
            .expect_err("the corrupted input must be rejected")
            .to_string();
        tx.rollback();
        err
    }

    #[test]
    fn added_table_missing_from_the_compiled_catalog_is_rejected() {
        let changes = [SchemaChange::AddTable {
            table: "Ghost".into(),
        }];
        let err = apply(
            &TASK,
            &changes,
            &mut task_catalog(),
            &StoredCatalog::default(),
            &MetaIndex::new(&[]),
        );
        assert!(err.contains("missing from the compiled catalog"), "{err}");
    }

    #[test]
    fn renamed_table_missing_from_the_stored_catalog_is_rejected() {
        let changes = [SchemaChange::RenameColumn {
            table: "Ghost".into(),
            from: "a".into(),
            to: "b".into(),
        }];
        let err = apply(
            &TASK,
            &changes,
            &mut StoredCatalog::default(),
            &task_catalog(),
            &MetaIndex::new(&[]),
        );
        assert!(err.contains("missing from the stored catalog"), "{err}");
    }

    #[test]
    fn vanished_default_is_rejected() {
        let changes = [SchemaChange::AddColumnWithDefault {
            table: "Task".into(),
            column: "priority".into(),
        }];
        let err = apply(
            &TASK,
            &changes,
            &mut task_catalog(),
            &task_catalog(),
            &MetaIndex::new(&[]), // no #[default] metadata at all
        );
        assert!(err.contains("vanished"), "{err}");
    }

    #[test]
    fn added_column_missing_from_the_compiled_schema_is_rejected() {
        let changes = [SchemaChange::AddColumnWithDefault {
            table: "Task".into(),
            column: "priority".into(),
        }];
        // Metadata declares the default, but the compiled TASK has no
        // `priority` column.
        let err = apply(
            &TASK,
            &changes,
            &mut task_catalog(),
            &task_catalog(),
            &MetaIndex::new(&[&TASK_META]),
        );
        assert!(err.contains("missing from the compiled schema"), "{err}");
    }

    #[test]
    fn default_value_that_does_not_inhabit_the_column_type_is_rejected() {
        let changes = [SchemaChange::AddColumnWithDefault {
            table: "Task".into(),
            column: "priority".into(),
        }];
        // #[default] yields U8(0) but the compiled column type is Str.
        let err = apply(
            &TASK_PRIORITY_STR,
            &changes,
            &mut task_catalog(),
            &task_catalog(),
            &MetaIndex::new(&[&TASK_META]),
        );
        assert!(err.contains("does not inhabit"), "{err}");
    }

    #[test]
    fn defaulted_column_on_a_table_missing_from_the_stored_catalog_is_rejected() {
        static PRIORITY_U8_COLS: &[ColumnSchema] = &[
            ColumnSchema {
                name: "id",
                ty: FluxType::U64,
            },
            ColumnSchema {
                name: "title",
                ty: FluxType::Str,
            },
            ColumnSchema {
                name: "priority",
                ty: FluxType::U8,
            },
        ];
        static TASK_PRIORITY_U8: TableSchema = task_table("Task", PRIORITY_U8_COLS);
        let changes = [SchemaChange::AddColumnWithDefault {
            table: "Task".into(),
            column: "priority".into(),
        }];
        let err = apply(
            &TASK_PRIORITY_U8,
            &changes,
            &mut StoredCatalog::default(), // no Task layout stored
            &task_catalog(),
            &MetaIndex::new(&[&TASK_META]),
        );
        assert!(err.contains("missing from the stored catalog"), "{err}");
    }

    #[test]
    fn incompatible_changes_never_reach_auto_apply() {
        let changes = [SchemaChange::RemoveTable {
            table: "Task".into(),
        }];
        let err = apply(
            &TASK,
            &changes,
            &mut task_catalog(),
            &task_catalog(),
            &MetaIndex::new(&[]),
        );
        assert!(err.contains("reached auto-apply"), "{err}");
        assert!(err.contains("MIG-022"), "{err}");
    }
}
