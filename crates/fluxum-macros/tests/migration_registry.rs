//! T3.6 macro-side tests: `#[fluxum::migration(version = N)]` registers a
//! `MigrationDef` at link time, `fluxum_core::schema_version!` declares the
//! module version (MIG-001/MIG-010), and `#[default]` / `#[rename(from)]`
//! field attributes emit the `TableColumnMeta` the SPEC-010 schema diff
//! consumes (MIG-020/MIG-021) — including end-to-end classification through
//! `diff_catalogs`.
#![allow(dead_code)]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use fluxum_core::Result;
use fluxum_core::migration::{
    MigrationContext, SchemaChange, StoredCatalog, declared_schema_version, diff_catalogs,
    registered_column_meta, registered_migrations,
};
use fluxum_core::schema::Schema;
use fluxum_core::store::RowValue;
use fluxum_macros as fluxum;

fluxum_core::schema_version!(3);

#[fluxum::table(public)]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub title: String,
    pub done: bool,
    /// Added in v2 — existing rows are backfilled with 3.
    #[default(3)]
    pub priority: u8,
    /// Added in v3 — backfilled with None.
    #[default(None)]
    pub note: Option<String>,
}

#[fluxum::table(public, primary_key(grid_x, grid_y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    /// Renamed from `reading` in v3.
    #[rename(from = "reading")]
    pub value: f64,
}

#[fluxum::migration(version = 2)]
fn migrate_v2(ctx: &mut MigrationContext<'_, '_>) -> Result<()> {
    ctx.add_column("Task", "priority", RowValue::U8(3))
}

#[fluxum::migration(version = 3)]
fn migrate_v3(ctx: &mut MigrationContext<'_, '_>) -> Result<()> {
    ctx.rename_column("Sensor", "reading", "value")
}

#[test]
fn schema_version_macro_registers_the_module_version() {
    let version = declared_schema_version().expect("exactly one declaration");
    assert_eq!(version, 3);
}

#[test]
fn migration_attribute_registers_defs_at_link_time() {
    let mut defs: Vec<(u32, &str)> = registered_migrations()
        .map(|def| (def.version, def.name))
        .collect();
    defs.sort_unstable();
    assert_eq!(defs, [(2, "migrate_v2"), (3, "migrate_v3")]);
}

#[test]
fn default_and_rename_attributes_emit_column_meta() {
    let task = registered_column_meta()
        .find(|meta| meta.table == "Task")
        .expect("Task registered TableColumnMeta");
    let mut defaults: Vec<&str> = task.defaults.iter().map(|d| d.column).collect();
    defaults.sort_unstable();
    assert_eq!(defaults, ["note", "priority"]);
    assert!(task.renames.is_empty());

    // The generated constructors produce the declared values, typed.
    let priority = task
        .defaults
        .iter()
        .find(|d| d.column == "priority")
        .expect("priority default");
    assert_eq!((priority.value)(), RowValue::U8(3));
    let note = task
        .defaults
        .iter()
        .find(|d| d.column == "note")
        .expect("note default");
    assert_eq!((note.value)(), RowValue::Optional(None));

    let sensor = registered_column_meta()
        .find(|meta| meta.table == "Sensor")
        .expect("Sensor registered TableColumnMeta");
    assert!(sensor.defaults.is_empty());
    assert_eq!(sensor.renames.len(), 1);
    assert_eq!(sensor.renames[0].column, "value");
    assert_eq!(sensor.renames[0].from, "reading");
}

#[test]
fn attributes_are_stripped_from_the_emitted_struct() {
    // The struct still constructs normally — #[default]/#[rename] left no
    // trace on the fields.
    let task = Task {
        id: 1,
        title: "t".into(),
        done: false,
        priority: 9,
        note: None,
    };
    assert_eq!(task.priority, 9);
}

#[test]
fn diff_consumes_the_registered_meta_end_to_end() {
    // Stored: v1 layouts (no priority/note; Sensor still says `reading`).
    let compiled = StoredCatalog::from_schema(
        &Schema::from_tables([
            <Task as fluxum_core::schema::Table>::SCHEMA,
            <Sensor as fluxum_core::schema::Table>::SCHEMA,
        ])
        .expect("schema assembles"),
    );
    let mut stored = compiled.clone();
    {
        let task = stored.tables.get_mut("Task").expect("Task tracked");
        task.columns
            .retain(|c| c.name != "priority" && c.name != "note");
        let sensor = stored.tables.get_mut("Sensor").expect("Sensor tracked");
        for column in &mut sensor.columns {
            if column.name == "value" {
                column.name = "reading".into();
            }
        }
    }

    // diff_catalogs reads the link-time registered meta of THIS binary:
    // both additions carry defaults, the rename is annotated — every
    // change classifies as safe auto-apply (MIG-021).
    let changes = diff_catalogs(&stored, &compiled);
    assert_eq!(changes.len(), 3, "{changes:?}");
    assert!(changes.iter().all(SchemaChange::is_safe), "{changes:?}");
    assert!(changes.iter().any(|c| matches!(
        c,
        SchemaChange::RenameColumn { table, from, to }
            if table == "Sensor" && from == "reading" && to == "value"
    )));
}
