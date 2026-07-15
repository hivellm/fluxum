//! T3.6 verification suite (SPEC-010 acceptance 1-6, 8; DAG exit test):
//! add-column and rename-column migrations pass end-to-end across simulated
//! binary upgrades (fresh store + recovery per "boot"), incompatible
//! changes abort startup with the stored data untouched, downgrades fail
//! fast, multi-step runs execute in ascending order — one transaction per
//! step — and resume correctly after a mid-sequence crash, failed or
//! panicking migrations roll back completely, and safe additive changes
//! auto-apply in a single logged startup transaction (with the MIG-023
//! version-bump enforcement).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, replay};
use fluxum_core::migration::{
    ColumnDefault, ColumnRename, MigrationContext, MigrationDef, MigrationRunner, SCHEMA_META,
    SchemaChange, StoredCatalog, TableColumnMeta,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId, Tx};
use fluxum_core::types::Timestamp;

const SHARD: u32 = 9;
const EPOCH: u64 = 1;

// --- Hand-built static schemas standing in for compiled binaries ----------
// (same macro-output stand-in pattern as store_acid / txn_pipeline)

const fn table(
    name: &'static str,
    columns: &'static [ColumnSchema],
    primary_key: &'static [u16],
) -> TableSchema {
    TableSchema {
        name,
        columns,
        primary_key,
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    }
}

// Task v1: id, title, done.
static TASK_V1_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "done",
        ty: FluxType::Bool,
    },
];
static TASK_V1: TableSchema = table("Task", TASK_V1_COLS, &[0]);

// Task v2: + priority (appended).
static TASK_V2_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "done",
        ty: FluxType::Bool,
    },
    ColumnSchema {
        name: "priority",
        ty: FluxType::U8,
    },
];
static TASK_V2: TableSchema = table("Task", TASK_V2_COLS, &[0]);

// Task with `done` removed — an incompatible change.
static TASK_DROPPED_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
];
static TASK_DROPPED: TableSchema = table("Task", TASK_DROPPED_COLS, &[0]);

// Sensor v1: composite PK, reading f64.
static SENSOR_V1_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F64,
    },
];
static SENSOR_V1: TableSchema = table("Sensor", SENSOR_V1_COLS, &[0, 1]);

// Sensor with `reading` renamed to `value` (same type).
static SENSOR_RENAMED_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::F64,
    },
];
static SENSOR_RENAMED: TableSchema = table("Sensor", SENSOR_RENAMED_COLS, &[0, 1]);

// Sensor with `reading` narrowed to f32 — an incompatible type change.
static SENSOR_TYPECHANGE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F32,
    },
];
static SENSOR_TYPECHANGE: TableSchema = table("Sensor", SENSOR_TYPECHANGE_COLS, &[0, 1]);

// A brand-new table for the MIG-021 auto-apply path.
static AUDIT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "action",
        ty: FluxType::Str,
    },
];
static AUDIT: TableSchema = table("AuditEvent", AUDIT_COLS, &[0]);

// --- One simulated shard boot ----------------------------------------------

struct Shard {
    store: Arc<MemStore>,
    log: Arc<CommitLog>,
    schema: Schema,
}

/// Boot a "binary" whose compiled schema is `tables` (+ the runtime's
/// `__schema_meta__`) against the data directory `dir`: fresh store, log
/// open (torn-tail recovery), checkpoint+replay recovery.
fn boot(dir: &Path, tables: &[&'static TableSchema]) -> Shard {
    let mut all: Vec<&'static TableSchema> = tables.to_vec();
    all.push(&SCHEMA_META);
    let schema = Schema::from_tables(all).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log_dir = dir.join("log");
    let log =
        Arc::new(CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap());
    let repo = CheckpointRepo::open(&dir.join("snapshots")).unwrap();
    recover(&store, &repo, &log_dir, SHARD).unwrap();
    Shard { store, log, schema }
}

impl Shard {
    /// Run the startup migration sequence of this "binary".
    async fn migrate(
        &self,
        code_version: u32,
        migrations: &[&MigrationDef],
        meta: &[&TableColumnMeta],
    ) -> fluxum_core::Result<fluxum_core::migration::MigrationReport> {
        MigrationRunner::new(&self.store, &self.log, &self.schema)
            .unwrap()
            .run_with(code_version, migrations, meta)
            .await
    }

    /// Commit one write transaction and append it to the log, durably.
    async fn commit(&self, write: impl FnOnce(&mut Tx<'_>)) {
        let mut tx = self.store.begin();
        write(&mut tx);
        let diff = tx.commit().unwrap();
        let tx_id = diff.tx_id;
        self.log.append_diff(&diff, Timestamp::now()).await.unwrap();
        self.log.wait_durable(tx_id).await.unwrap();
    }

    fn meta_bytes(&self, key: &str) -> Vec<u8> {
        let id = self.store.table_id("__schema_meta__").unwrap();
        let row = self
            .store
            .snapshot()
            .query_pk(id, &[RowValue::Str(key.into())])
            .unwrap()
            .unwrap_or_else(|| panic!("__schema_meta__.{key} missing"));
        match row.value(1) {
            Some(RowValue::Bytes(bytes)) => bytes.clone(),
            other => panic!("__schema_meta__.{key} holds {other:?}"),
        }
    }

    fn meta_version(&self) -> u32 {
        rmp_serde::from_slice(&self.meta_bytes("schema_version")).unwrap()
    }

    fn meta_catalog(&self) -> StoredCatalog {
        StoredCatalog::decode(&self.meta_bytes("schema_catalog")).unwrap()
    }

    fn rows(&self, table: &str) -> Vec<Vec<RowValue>> {
        let id = self.store.table_id(table).unwrap();
        let snapshot = self.store.snapshot();
        let rows: Vec<Vec<RowValue>> = snapshot
            .scan(id)
            .unwrap()
            .map(|row| row.values().to_vec())
            .collect();
        rows
    }
}

fn task(id: u64, title: &str, done: bool) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Str(title.into()),
        RowValue::Bool(done),
    ]
}

fn sensor(x: i32, y: i32, reading: f64) -> Vec<RowValue> {
    vec![RowValue::I32(x), RowValue::I32(y), RowValue::F64(reading)]
}

/// Number of records in the shard's log (every migration transaction must
/// be durably logged).
fn logged_records(dir: &Path) -> usize {
    let mut count = 0usize;
    let report = replay(&dir.join("log"), SHARD, |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert!(report.corruption.is_none());
    count
}

// --- Migration functions (macro-output stand-ins) ---------------------------

fn migrate_task_priority(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    ctx.add_column("Task", "priority", RowValue::U8(0))
}
static ADD_PRIORITY_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_task_priority",
    run: migrate_task_priority,
};

fn migrate_sensor_rename(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    ctx.rename_column("Sensor", "reading", "value")
}
static RENAME_READING_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_sensor_rename",
    run: migrate_sensor_rename,
};
static RENAME_READING_V3: MigrationDef = MigrationDef {
    version: 3,
    name: "migrate_sensor_rename",
    run: migrate_sensor_rename,
};

// --- Acceptance 1: add-column migration ------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn add_column_migration_backfills_existing_rows() {
    let dir = tempfile::tempdir().unwrap();

    // v1 binary: first boot, then some data.
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        let report = shard.migrate(1, &[], &[]).await.unwrap();
        assert!(report.first_boot);
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "write spec", false)).unwrap();
                tx.insert(task_id, task(2, "review spec", true)).unwrap();
            })
            .await;
    }

    // v2 binary: Task gains `priority`; the migration backfills it.
    {
        let shard = boot(dir.path(), &[&TASK_V2]);
        let report = shard.migrate(2, &[&ADD_PRIORITY_V2], &[]).await.unwrap();
        assert!(!report.first_boot);
        assert_eq!(report.from_version, 1);
        assert_eq!(report.to_version, 2);
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].version, 2);

        // Every pre-existing row carries priority = 0 (acceptance 1).
        let rows = shard.rows("Task");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.len(), 4);
            assert_eq!(row[3], RowValue::U8(0));
        }
        assert_eq!(shard.meta_version(), 2);
        // The rewritten rows satisfy the compiled schema: normal writes work.
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(
                    task_id,
                    vec![
                        RowValue::U64(3),
                        RowValue::Str("ship".into()),
                        RowValue::Bool(false),
                        RowValue::U8(2),
                    ],
                )
                .unwrap();
            })
            .await;
    }

    // Third boot: the migration is durable — nothing pending, rows intact.
    {
        let shard = boot(dir.path(), &[&TASK_V2]);
        let report = shard.migrate(2, &[&ADD_PRIORITY_V2], &[]).await.unwrap();
        assert!(report.applied.is_empty());
        assert!(report.auto_applied.is_empty());
        assert_eq!(shard.rows("Task").len(), 3);
        assert_eq!(shard.meta_version(), 2);
    }
}

// --- Acceptance 2: rename-column migration ----------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn rename_column_migration_preserves_row_data() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let sensor_id = shard.store.table_id("Sensor").unwrap();
        shard
            .commit(|tx| {
                tx.insert(sensor_id, sensor(-2, 9, 101.25)).unwrap();
                tx.insert(sensor_id, sensor(4, 4, 7.5)).unwrap();
            })
            .await;
    }
    {
        let shard = boot(dir.path(), &[&SENSOR_RENAMED]);
        let report = shard.migrate(2, &[&RENAME_READING_V2], &[]).await.unwrap();
        assert_eq!(report.applied.len(), 1);

        // All row data preserved under the new column name (acceptance 2).
        let rows = shard.rows("Sensor");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r[2] == RowValue::F64(101.25)));
        assert!(rows.iter().any(|r| r[2] == RowValue::F64(7.5)));

        // The stored catalog speaks the new name; the old one is gone.
        let catalog = shard.meta_catalog();
        let sensor_layout = &catalog.tables["Sensor"];
        assert!(sensor_layout.has_column("value"));
        assert!(!sensor_layout.has_column("reading"));
        assert_eq!(shard.meta_version(), 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_via_attribute_auto_applies_without_a_migration() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let sensor_id = shard.store.table_id("Sensor").unwrap();
        shard
            .commit(|tx| {
                tx.insert(sensor_id, sensor(1, 1, 3.25)).unwrap();
            })
            .await;
    }
    {
        static RENAMES: &[ColumnRename] = &[ColumnRename {
            column: "value",
            from: "reading",
        }];
        static META: TableColumnMeta = TableColumnMeta {
            table: "Sensor",
            defaults: &[],
            renames: RENAMES,
        };
        let shard = boot(dir.path(), &[&SENSOR_RENAMED]);
        let report = shard.migrate(2, &[], &[&META]).await.unwrap();
        assert_eq!(
            report.auto_applied,
            vec![SchemaChange::RenameColumn {
                table: "Sensor".into(),
                from: "reading".into(),
                to: "value".into()
            }]
        );
        assert_eq!(shard.rows("Sensor")[0][2], RowValue::F64(3.25));
        assert!(shard.meta_catalog().tables["Sensor"].has_column("value"));
        assert_eq!(shard.meta_version(), 2);
    }
}

// --- Acceptance 3: incompatible change aborts startup ------------------------

#[tokio::test(flavor = "multi_thread")]
async fn incompatible_type_change_aborts_startup_with_data_untouched() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let sensor_id = shard.store.table_id("Sensor").unwrap();
        shard
            .commit(|tx| {
                tx.insert(sensor_id, sensor(0, 0, 1.0)).unwrap();
            })
            .await;
    }
    {
        let shard = boot(dir.path(), &[&SENSOR_TYPECHANGE]);
        let before = shard.store.snapshot();
        let err = shard.migrate(2, &[], &[]).await.unwrap_err().to_string();
        // The error names the table, the column, and the change type.
        assert!(err.contains("MIG-022"), "{err}");
        assert!(err.contains("Sensor"), "{err}");
        assert!(err.contains("reading"), "{err}");
        assert!(err.contains("type changed"), "{err}");
        assert!(err.contains("F64 -> F32"), "{err}");
        // Stored data unmodified: the published state is pointer-identical.
        assert!(before.same_state(&shard.store.snapshot()));
        assert_eq!(shard.meta_version(), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_offending_change_is_listed_in_the_abort() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1, &SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
    }
    {
        // Two incompatible changes at once: Task loses `done`, Sensor
        // changes `reading`'s type.
        let shard = boot(dir.path(), &[&TASK_DROPPED, &SENSOR_TYPECHANGE]);
        let err = shard.migrate(2, &[], &[]).await.unwrap_err().to_string();
        assert!(err.contains("`Task`"), "{err}");
        assert!(err.contains("done"), "{err}");
        assert!(err.contains("column removed"), "{err}");
        assert!(err.contains("`Sensor`"), "{err}");
        assert!(err.contains("type changed"), "{err}");
    }
}

// --- Acceptance 4: downgrade rejection ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn downgrade_fails_fast_with_the_mig003_fatal_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(3, &[], &[]).await.unwrap(); // first boot at version 3
    }
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        let err = shard.migrate(2, &[], &[]).await.unwrap_err().to_string();
        assert!(
            err.contains("FATAL: schema downgrade detected (code=2 < stored=3)"),
            "{err}"
        );
        assert_eq!(shard.meta_version(), 3);
    }
}

// --- Acceptance 5: ordering and crash-resume ---------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn multi_step_runs_ascending_one_transaction_per_step() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1, &SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        let sensor_id = shard.store.table_id("Sensor").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", false)).unwrap();
                tx.insert(sensor_id, sensor(1, 2, 9.0)).unwrap();
            })
            .await;
    }
    let records_before = logged_records(dir.path());
    {
        // Stored version 1, code version 3: v2 then v3 run in order.
        let shard = boot(dir.path(), &[&TASK_V2, &SENSOR_RENAMED]);
        let report = shard
            .migrate(3, &[&RENAME_READING_V3, &ADD_PRIORITY_V2], &[])
            .await
            .unwrap();
        let versions: Vec<u32> = report.applied.iter().map(|a| a.version).collect();
        assert_eq!(versions, [2, 3], "ascending order (MIG-012)");
        // One logged transaction per step (MIG-040) — and nothing else.
        assert_eq!(logged_records(dir.path()), records_before + 2);
        assert_eq!(shard.meta_version(), 3);
        assert_eq!(shard.rows("Task")[0][3], RowValue::U8(0));
        assert_eq!(shard.rows("Sensor")[0][2], RowValue::F64(9.0));
    }
}

static RESUME_V3_FAILS: AtomicBool = AtomicBool::new(true);

fn migrate_resume_v3(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    if RESUME_V3_FAILS.load(Ordering::SeqCst) {
        return Err(fluxum_core::FluxumError::Storage(
            "simulated crash before v3 (test)".into(),
        ));
    }
    ctx.rename_column("Sensor", "reading", "value")
}
static RESUME_V3: MigrationDef = MigrationDef {
    version: 3,
    name: "migrate_resume_v3",
    run: migrate_resume_v3,
};

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_sequence_resumes_at_the_correct_step() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1, &SENSOR_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        let sensor_id = shard.store.table_id("Sensor").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", true)).unwrap();
                tx.insert(sensor_id, sensor(5, 5, 2.5)).unwrap();
            })
            .await;
    }
    {
        // First attempt: v2 commits, v3 "crashes" — startup aborts.
        let shard = boot(dir.path(), &[&TASK_V2, &SENSOR_RENAMED]);
        let err = shard
            .migrate(3, &[&ADD_PRIORITY_V2, &RESUME_V3], &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("simulated crash"), "{err}");
        // v2's step committed with its own version stamp (MIG-012).
        assert_eq!(shard.meta_version(), 2);
    }
    {
        // Restart with the fixed binary: only v3 runs.
        RESUME_V3_FAILS.store(false, Ordering::SeqCst);
        let shard = boot(dir.path(), &[&TASK_V2, &SENSOR_RENAMED]);
        let report = shard
            .migrate(3, &[&ADD_PRIORITY_V2, &RESUME_V3], &[])
            .await
            .unwrap();
        let versions: Vec<u32> = report.applied.iter().map(|a| a.version).collect();
        assert_eq!(versions, [3], "resume skips the committed v2 (MIG-012)");
        assert_eq!(shard.meta_version(), 3);
        assert_eq!(shard.rows("Task")[0][3], RowValue::U8(0), "v2 not re-run");
        assert_eq!(shard.rows("Sensor")[0][2], RowValue::F64(2.5));
    }
}

// --- Acceptance 6: failure rollback ------------------------------------------

fn migrate_failing_after_ddl(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    ctx.add_column("Task", "priority", RowValue::U8(0))?;
    Err(fluxum_core::FluxumError::Storage(
        "business rule violated midway (test)".into(),
    ))
}
static FAILING_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_failing_after_ddl",
    run: migrate_failing_after_ddl,
};

fn migrate_panicking(_ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    panic!("migration bug (test)");
}
static PANICKING_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_panicking",
    run: migrate_panicking,
};

#[tokio::test(flavor = "multi_thread")]
async fn failing_or_panicking_migration_rolls_back_completely() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", false)).unwrap();
            })
            .await;
    }
    let records_before = logged_records(dir.path());
    {
        let shard = boot(dir.path(), &[&TASK_V2]);
        let before = shard.store.snapshot();

        let err = shard.migrate(2, &[&FAILING_V2], &[]).await.unwrap_err();
        assert!(err.to_string().contains("MIG-040"), "{err}");
        assert!(err.to_string().contains("business rule"), "{err}");
        // CommittedState and schema_version unchanged (acceptance 6):
        // rollback is a pure discard, so the published state is the *same*
        // pointer — the half-done column rewrite never surfaced.
        assert!(before.same_state(&shard.store.snapshot()));
        assert_eq!(shard.meta_version(), 1);

        let err = shard.migrate(2, &[&PANICKING_V2], &[]).await.unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
        assert!(err.to_string().contains("migration bug"), "{err}");
        assert!(before.same_state(&shard.store.snapshot()));
        assert_eq!(shard.meta_version(), 1);
    }
    // No log entry was written by either failed attempt.
    assert_eq!(logged_records(dir.path()), records_before);
    {
        // Restarting with a fixed binary re-runs the migration from the
        // stored version.
        let shard = boot(dir.path(), &[&TASK_V2]);
        let report = shard.migrate(2, &[&ADD_PRIORITY_V2], &[]).await.unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(shard.rows("Task")[0][3], RowValue::U8(0));
        assert_eq!(shard.meta_version(), 2);
    }
}

// --- Acceptance 8: safe auto-apply + MIG-023 ---------------------------------

static PRIORITY_DEFAULTS: &[ColumnDefault] = &[ColumnDefault {
    column: "priority",
    value: || RowValue::U8(7),
}];
static TASK_META: TableColumnMeta = TableColumnMeta {
    table: "Task",
    defaults: PRIORITY_DEFAULTS,
    renames: &[],
};

#[tokio::test(flavor = "multi_thread")]
async fn safe_additive_changes_auto_apply_in_one_startup_transaction() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", false)).unwrap();
                tx.insert(task_id, task(2, "b", true)).unwrap();
            })
            .await;
    }
    let records_before = logged_records(dir.path());
    {
        // v2 adds a table AND a defaulted column — no migration function.
        let shard = boot(dir.path(), &[&TASK_V2, &AUDIT]);
        let report = shard.migrate(2, &[], &[&TASK_META]).await.unwrap();
        assert!(report.applied.is_empty());
        assert_eq!(report.auto_applied.len(), 2);
        assert!(report.auto_applied.iter().all(SchemaChange::is_safe));

        // Both changes landed in ONE startup transaction (MIG-021).
        assert_eq!(logged_records(dir.path()), records_before + 1);

        for row in shard.rows("Task") {
            assert_eq!(row[3], RowValue::U8(7), "backfilled from #[default]");
        }
        // The new table is registered, tracked, and usable.
        assert!(shard.meta_catalog().tables.contains_key("AuditEvent"));
        let audit_id = shard.store.table_id("AuditEvent").unwrap();
        shard
            .commit(|tx| {
                tx.insert(
                    audit_id,
                    vec![RowValue::U64(1), RowValue::Str("boot".into())],
                )
                .unwrap();
            })
            .await;
        assert_eq!(shard.meta_version(), 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_without_version_bump_aborts() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
    }
    {
        // Same binary layout change, but SCHEMA_VERSION still 1 (MIG-023).
        let shard = boot(dir.path(), &[&TASK_V2, &AUDIT]);
        let err = shard
            .migrate(1, &[], &[&TASK_META])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("MIG-023"), "{err}");
        assert!(err.contains("bump"), "{err}");
        assert_eq!(shard.meta_version(), 1);
    }
}

// --- Registration validation (MIG-010) ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_migration_versions_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V2, &SENSOR_RENAMED]);
    let err = shard
        .migrate(2, &[&ADD_PRIORITY_V2, &RENAME_READING_V2], &[])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("both declare version 2"), "{err}");
    assert!(err.contains("migrate_task_priority"), "{err}");
    assert!(err.contains("migrate_sensor_rename"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn migration_above_schema_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V1, &SENSOR_RENAMED]);
    let err = shard
        .migrate(2, &[&RENAME_READING_V3], &[])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("targets version 3"), "{err}");
    assert!(err.contains("bump fluxum::schema_version!"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn runner_requires_the_schema_meta_table() {
    let dir = tempfile::tempdir().unwrap();
    // A store assembled WITHOUT migration::SCHEMA_META cannot run
    // migrations — construction fails with a descriptive error.
    let schema = Schema::from_tables([&TASK_V1]).unwrap();
    let store = MemStore::new(&schema).unwrap();
    let log = CommitLog::open(
        &dir.path().join("log"),
        SHARD,
        EPOCH,
        CommitLogOptions::default(),
    )
    .unwrap();
    let err = match MigrationRunner::new(&store, &log, &schema) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("runner must reject a schema without __schema_meta__"),
    };
    assert!(err.contains("__schema_meta__"), "{err}");
    assert!(err.contains("MIG-002"), "{err}");
}

// --- First boot ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn first_boot_adopts_the_compiled_schema() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V1, &SENSOR_V1]);
    let report = shard.migrate(1, &[], &[]).await.unwrap();
    assert!(report.first_boot);
    assert_eq!(shard.meta_version(), 1);
    let catalog = shard.meta_catalog();
    assert_eq!(catalog.tables.len(), 2, "system tables are not tracked");
    assert!(catalog.tables.contains_key("Task"));
    assert!(catalog.tables.contains_key("Sensor"));

    // An immediate second run is a no-op: no new log records.
    let records = logged_records(dir.path());
    let report = shard.migrate(1, &[], &[]).await.unwrap();
    assert!(!report.first_boot);
    assert!(report.applied.is_empty());
    assert!(report.auto_applied.is_empty());
    assert_eq!(logged_records(dir.path()), records);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_uses_the_link_time_registries() {
    // This test binary registers no schema_version! and no migrations, so
    // the production `run()` path resolves to version 1 with empty inputs.
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V1]);
    let report = MigrationRunner::new(&shard.store, &shard.log, &shard.schema)
        .unwrap()
        .run()
        .await
        .unwrap();
    assert!(report.first_boot);
    assert_eq!(report.to_version, 1);
    assert!(report.applied.is_empty());
}

// --- MigrationContext error surface (MIG-011) ---------------------------------

/// Probes every `MigrationContext` validation failure, then performs a real
/// data fixup through `ctx.tx()` so the accessor path is exercised too.
fn migrate_probe_context_errors(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    assert_eq!(ctx.from_version, 1);
    assert_eq!(ctx.to_version, 2);

    // add_column: table not in the compiled schema.
    let err = ctx
        .add_column("Ghost", "col", RowValue::U8(0))
        .unwrap_err()
        .to_string();
    assert!(err.contains("not in the compiled schema"), "{err}");
    // add_column: column not declared on the compiled table.
    let err = ctx
        .add_column("Task", "ghost_col", RowValue::U8(0))
        .unwrap_err()
        .to_string();
    assert!(err.contains("not declared on the compiled table"), "{err}");
    // add_column: default value does not inhabit the column type.
    let err = ctx
        .add_column("Task", "priority", RowValue::Str("high".into()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not inhabit"), "{err}");
    // add_column: table missing from the stored catalog (new table — the
    // startup diff creates it, not the migration).
    let err = ctx
        .add_column("AuditEvent", "action", RowValue::Str(String::new()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("not in the stored catalog"), "{err}");
    // add_column: column already exists in the stored layout.
    let err = ctx
        .add_column("Task", "title", RowValue::Str(String::new()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "{err}");

    // rename_column: identical names.
    let err = ctx
        .rename_column("Task", "title", "title")
        .unwrap_err()
        .to_string();
    assert!(err.contains("identical"), "{err}");
    // rename_column: new name not declared on the compiled table.
    let err = ctx
        .rename_column("Task", "title", "ghost")
        .unwrap_err()
        .to_string();
    assert!(err.contains("not declared on the compiled table"), "{err}");
    // rename_column: new name already present in the stored layout.
    let err = ctx
        .rename_column("Task", "done", "title")
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists in the stored"), "{err}");
    // rename_column: a rename may not change the column type
    // (stored `done` is Bool, compiled `priority` is U8).
    let err = ctx
        .rename_column("Task", "done", "priority")
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot change the column type"), "{err}");

    // The real step: add the column, then fix data through ctx.tx().
    ctx.add_column("Task", "priority", RowValue::U8(0))?;
    let task_id = TableId::of("Task");
    let tx = ctx.tx();
    tx.upsert(
        task_id,
        vec![
            RowValue::U64(1),
            RowValue::Str("probe".into()),
            RowValue::Bool(false),
            RowValue::U8(9),
        ],
    )?;
    Ok(())
}
static PROBE_ERRORS_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_probe_context_errors",
    run: migrate_probe_context_errors,
};

#[tokio::test(flavor = "multi_thread")]
async fn migration_context_rejects_invalid_ddl_calls() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", false)).unwrap();
            })
            .await;
    }
    {
        // Compiled v2: Task gains `priority`, plus the brand-new AuditEvent
        // table (present in the compiled schema, absent from the stored
        // catalog until the startup diff runs).
        let shard = boot(dir.path(), &[&TASK_V2, &AUDIT]);
        let report = shard.migrate(2, &[&PROBE_ERRORS_V2], &[]).await.unwrap();
        assert_eq!(report.applied.len(), 1);
        // The data fixup through ctx.tx() landed.
        let rows = shard.rows("Task");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][3], RowValue::U8(9));
        assert_eq!(shard.meta_version(), 2);
    }
}

// --- Stored catalog / data divergence (append_column arity check) -------------

fn migrate_arity_divergence(ctx: &mut MigrationContext<'_, '_>) -> fluxum_core::Result<()> {
    // The (corrupted) stored catalog claims Task has 2 columns, but the
    // committed rows carry 3 values — the append must refuse to rewrite.
    ctx.add_column("Task", "done", RowValue::Bool(false))
}
static ARITY_V2: MigrationDef = MigrationDef {
    version: 2,
    name: "migrate_arity_divergence",
    run: migrate_arity_divergence,
};

#[tokio::test(flavor = "multi_thread")]
async fn diverged_catalog_and_data_abort_the_migration() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        let task_id = shard.store.table_id("Task").unwrap();
        shard
            .commit(|tx| {
                tx.insert(task_id, task(1, "a", true)).unwrap();
            })
            .await;
        // Corrupt the stored catalog: drop `done` from Task's layout while
        // the committed rows keep 3 values.
        let mut catalog = shard.meta_catalog();
        let layout = catalog.tables.get_mut("Task").unwrap();
        layout.columns.pop();
        let bytes = catalog.encode().unwrap();
        let meta_id = shard.store.table_id("__schema_meta__").unwrap();
        shard
            .commit(|tx| {
                tx.upsert(
                    meta_id,
                    vec![
                        RowValue::Str("schema_catalog".into()),
                        RowValue::Bytes(bytes.clone()),
                    ],
                )
                .unwrap();
            })
            .await;
    }
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        let err = shard
            .migrate(2, &[&ARITY_V2], &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("diverged"), "{err}");
        assert_eq!(shard.meta_version(), 1, "step rolled back");
    }
}

// --- Runner input validation ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn schema_version_zero_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V1]);
    let err = shard.migrate(0, &[], &[]).await.unwrap_err().to_string();
    assert!(err.contains("SCHEMA_VERSION 0 is invalid"), "{err}");
}

static BELOW_TWO: MigrationDef = MigrationDef {
    version: 1,
    name: "migrate_below_two",
    run: migrate_task_priority,
};

#[tokio::test(flavor = "multi_thread")]
async fn migration_version_below_two_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let shard = boot(dir.path(), &[&TASK_V2]);
    let err = shard
        .migrate(2, &[&BELOW_TWO], &[])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("version 1 is the initial schema"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn runner_rejects_a_store_missing_the_meta_table() {
    let dir = tempfile::tempdir().unwrap();
    // Schema WITH __schema_meta__, store assembled WITHOUT it.
    let schema_with_meta = Schema::from_tables([&TASK_V1, &SCHEMA_META]).unwrap();
    let schema_without = Schema::from_tables([&TASK_V1]).unwrap();
    let store = MemStore::new(&schema_without).unwrap();
    let log = CommitLog::open(
        &dir.path().join("log"),
        SHARD,
        EPOCH,
        CommitLogOptions::default(),
    )
    .unwrap();
    let err = match MigrationRunner::new(&store, &log, &schema_with_meta) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("runner must reject a store without __schema_meta__"),
    };
    assert!(err.contains("assembled without"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_catalog_with_a_version_is_reported_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        shard.migrate(1, &[], &[]).await.unwrap();
        // Delete schema_catalog, keeping schema_version — corrupt metadata.
        let meta_id = shard.store.table_id("__schema_meta__").unwrap();
        shard
            .commit(|tx| {
                assert!(
                    tx.delete(meta_id, &[RowValue::Str("schema_catalog".into())])
                        .unwrap()
                );
            })
            .await;
    }
    {
        let shard = boot(dir.path(), &[&TASK_V1]);
        let err = shard.migrate(2, &[], &[]).await.unwrap_err().to_string();
        assert!(err.contains("schema_catalog is missing"), "{err}");
        assert!(err.contains("corrupt"), "{err}");
    }
}
