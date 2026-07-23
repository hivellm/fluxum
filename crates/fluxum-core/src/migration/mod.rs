//! Schema migration (SPEC-010, T3.6, FR-80): the `#[fluxum::migration]`
//! runner, the `__schema_meta__` system table, automatic schema diffing, and
//! safe auto-apply for additive changes.
//!
//! # How a schema change reaches a running store
//!
//! The compiled schema arrives as a new binary (link-time registry,
//! SPEC-001); recovery (SPEC-002 STG-030) loads the stored rows first, then
//! [`MigrationRunner::run`] executes **after recovery and before the shard
//! serves any traffic**:
//!
//! 1. Read `schema_version` + `schema_catalog` from [`SCHEMA_META`]
//!    (first boot writes them and returns).
//! 2. Refuse a downgrade (`code < stored`) with the MIG-003 `FATAL` error.
//! 3. Run every pending `#[fluxum::migration(version = N)]` function in
//!    ascending order — one transaction per step, `schema_version` and the
//!    updated catalog persisted **inside the same transaction** (MIG-012),
//!    so a crash between steps resumes at the correct point.
//! 4. Diff the (post-migration) stored catalog against the compiled schema
//!    (MIG-020). Safe additive changes — new table, new column with
//!    `#[default(value)]`, rename with `#[rename(from = "old")]` — are
//!    auto-applied in one startup transaction and logged (MIG-021). Any
//!    other change aborts startup listing every offending entry, with the
//!    stored data untouched (MIG-022). A non-empty diff without a
//!    `SCHEMA_VERSION` bump also aborts (MIG-023).
//!
//! # Layout rules (why "additive" means *appended*)
//!
//! Rows are positional value vectors and recovery rebuilds primary keys and
//! indexes from the **compiled** column ordinals before the runner can
//! rewrite anything, so pre-migration rows are only interpreted correctly
//! when the stored layout is a prefix of the compiled one. New columns must
//! therefore be declared **after** every existing column; a mid-layout
//! insertion or reorder is reported as an incompatible change and aborts
//! startup. Renames never move values (metadata-only), and index or
//! `#[unique]` additions on existing columns need no migration at all —
//! secondary structures are rebuilt from rows on every recovery.
//!
//! # T3.6 scope
//!
//! [`MigrationContext`] exposes the additive/rename DDL surface
//! (`add_column`, `rename_column`) plus raw [`crate::store::Tx`] access for
//! data fixups; the destructive MIG-011 operations (`drop_column`,
//! `drop_table`, `create_table`) and MIG-013 alias types land with the
//! follow-up migration tasks — until then the corresponding diff entries
//! abort startup, which is the fail-closed behavior MIG-022 requires.

pub mod catalog;
pub mod context;
pub mod diff;
pub mod runner;

pub use catalog::{StoredCatalog, StoredColumn, StoredTable, StoredType};
pub use context::MigrationContext;
pub use diff::{SchemaChange, diff_catalogs};
pub use runner::{
    AppliedMigration, MigrationPlan, MigrationReport, MigrationRunner, PlanVerdict, plan, plan_with,
};

use crate::error::{FluxumError, Result};
use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
use crate::store::RowValue;
use crate::types::{ConnectionId, EntityId, Identity, Timestamp};

/// Stored name of the schema metadata system table (MIG-002).
pub const META_TABLE: &str = "__schema_meta__";

/// `__schema_meta__` key holding the MessagePack-encoded `u32` schema
/// version (MIG-001/MIG-002).
pub const META_KEY_VERSION: &str = "schema_version";

/// `__schema_meta__` key holding the MessagePack-encoded [`StoredCatalog`]
/// (MIG-020).
pub const META_KEY_CATALOG: &str = "schema_catalog";

static SCHEMA_META_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "key",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::Bytes,
    },
];

/// The `__schema_meta__` system table (MIG-002): `key` (primary key) →
/// MessagePack-encoded `value`.
///
/// Defined by the runtime, not by application code. The server assembly
/// must include it when building the [`crate::schema::Schema`] a
/// [`MigrationRunner`] operates on ([`MigrationRunner::new`] verifies it).
/// Access is `Private` — never sent to clients; replicating the schema
/// version across shards is SPEC-007 territory.
pub static SCHEMA_META: TableSchema = TableSchema {
    name: META_TABLE,
    columns: SCHEMA_META_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

// ---------------------------------------------------------------------------
// Link-time registries (MIG-001, MIG-010, MIG-020 metadata)
// ---------------------------------------------------------------------------

/// A migration function pointer, as registered by
/// `#[fluxum::migration(version = N)]`.
pub type MigrationFn = fn(&mut MigrationContext<'_, '_>) -> Result<()>;

/// One registered migration step (MIG-010). Submitted by
/// `#[fluxum::migration(version = N)]` via the link-time registry;
/// collected by [`registered_migrations`].
pub struct MigrationDef {
    /// The schema version this step migrates *to*: it runs when the stored
    /// version is lower than `version` (MIG-010).
    pub version: u32,
    /// Function name, for duplicate-version diagnostics.
    pub name: &'static str,
    /// The migration body (MIG-011).
    pub run: MigrationFn,
}

inventory::collect!(MigrationDef);

/// Iterate every `#[fluxum::migration]` registered in this binary, in
/// linker order (the runner sorts by version).
pub fn registered_migrations() -> impl Iterator<Item = &'static MigrationDef> {
    inventory::iter::<MigrationDef>()
}

/// One `fluxum::schema_version!(N)` declaration (MIG-001).
pub struct SchemaVersionDef(pub u32);

inventory::collect!(SchemaVersionDef);

/// The module's declared `SCHEMA_VERSION` (MIG-001): the value of the single
/// `fluxum::schema_version!` declaration, defaulting to `1` when absent.
/// More than one declaration — or a declared `0` — is a startup error.
pub fn declared_schema_version() -> Result<u32> {
    let mut versions = inventory::iter::<SchemaVersionDef>().map(|def| def.0);
    match (versions.next(), versions.next()) {
        (None, _) => Ok(1),
        (Some(0), None) => Err(FluxumError::Schema(
            "fluxum::schema_version!(0) is invalid: versions start at 1 (MIG-001)".into(),
        )),
        (Some(version), None) => Ok(version),
        (Some(a), Some(b)) => Err(FluxumError::Schema(format!(
            "multiple fluxum::schema_version! declarations ({a} and {b}): a module declares \
             its schema version exactly once (MIG-001)"
        ))),
    }
}

/// Declare the module's schema version (MIG-001):
/// `fluxum_core::schema_version!(3);` registers `SCHEMA_VERSION = 3` into
/// the link-time registry. Without a declaration the version defaults to 1.
#[macro_export]
macro_rules! schema_version {
    ($version:expr) => {
        $crate::schema::inventory::submit! {
            $crate::migration::SchemaVersionDef($version)
        }
    };
}

/// A `#[default(value)]` declaration on one column (MIG-020/MIG-021): the
/// value existing rows are backfilled with when the column is auto-applied.
pub struct ColumnDefault {
    /// Column (field) name as declared on the struct.
    pub column: &'static str,
    /// Constructor of the backfill value; called once per auto-apply.
    pub value: fn() -> RowValue,
}

/// A `#[rename(from = "old")]` declaration on one column (MIG-020/MIG-021):
/// the stored column `from` is renamed to `column` in place.
pub struct ColumnRename {
    /// New column (field) name as declared on the struct.
    pub column: &'static str,
    /// The stored name this column had before the rename.
    pub from: &'static str,
}

/// Per-table migration metadata emitted by `#[fluxum::table]` when any
/// field carries `#[default(value)]` or `#[rename(from = "old")]`.
/// Collected at link time; consumed by the schema diff (MIG-020).
pub struct TableColumnMeta {
    /// Table (struct) name.
    pub table: &'static str,
    /// Every `#[default(value)]` field of the table.
    pub defaults: &'static [ColumnDefault],
    /// Every `#[rename(from = "old")]` field of the table.
    pub renames: &'static [ColumnRename],
}

inventory::collect!(TableColumnMeta);

/// Iterate every [`TableColumnMeta`] registered in this binary.
pub fn registered_column_meta() -> impl Iterator<Item = &'static TableColumnMeta> {
    inventory::iter::<TableColumnMeta>()
}

// ---------------------------------------------------------------------------
// IntoRowValue — bridge for macro-generated #[default(expr)] constructors
// ---------------------------------------------------------------------------

/// Conversion from a Rust column value to the store's dynamic [`RowValue`].
///
/// `#[fluxum::table]` type-ascribes the `#[default(expr)]` expression to the
/// field's Rust type and routes it through this trait, so a default value
/// that does not inhabit the column type is a compile error. Implemented for
/// the closed SPEC-001 §3 universe; list defaults other than `Vec<u8>` are
/// not supported (declare such columns via an explicit migration instead).
pub trait IntoRowValue {
    /// The dynamic row value carrying `self`.
    fn into_row_value(self) -> RowValue;
}

macro_rules! impl_into_row_value {
    ($($ty:ty => $variant:ident),+ $(,)?) => {
        $(impl IntoRowValue for $ty {
            fn into_row_value(self) -> RowValue {
                RowValue::$variant(self)
            }
        })+
    };
}

impl_into_row_value! {
    bool => Bool,
    i8 => I8,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    u8 => U8,
    u16 => U16,
    u32 => U32,
    u64 => U64,
    f32 => F32,
    f64 => F64,
    String => Str,
    Vec<u8> => Bytes,
    Identity => Identity,
    ConnectionId => ConnectionId,
    EntityId => EntityId,
    Timestamp => Timestamp,
}

impl IntoRowValue for &str {
    fn into_row_value(self) -> RowValue {
        RowValue::Str(self.to_owned())
    }
}

impl<T: IntoRowValue> IntoRowValue for Option<T> {
    fn into_row_value(self) -> RowValue {
        RowValue::Optional(self.map(|value| Box::new(value.into_row_value())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    #[test]
    fn schema_meta_assembles_as_a_valid_table() {
        let schema =
            Schema::from_tables([&SCHEMA_META]).unwrap_or_else(|e| panic!("must assemble: {e}"));
        let meta = match schema.table(META_TABLE) {
            Some(table) => table,
            None => panic!("__schema_meta__ missing"),
        };
        assert_eq!(meta.primary_key, &[0]);
        assert_eq!(meta.columns[0].ty, FluxType::Str);
        assert_eq!(meta.columns[1].ty, FluxType::Bytes);
        assert_eq!(meta.access, TableAccess::Private);
    }

    #[test]
    fn declared_schema_version_defaults_to_one() {
        // fluxum-core itself declares no schema_version!.
        match declared_schema_version() {
            Ok(version) => assert_eq!(version, 1),
            Err(e) => panic!("{e}"),
        }
    }

    #[test]
    fn into_row_value_covers_the_closed_universe() {
        assert_eq!(true.into_row_value(), RowValue::Bool(true));
        assert_eq!(7u8.into_row_value(), RowValue::U8(7));
        assert_eq!((-3i64).into_row_value(), RowValue::I64(-3));
        assert_eq!(1.5f64.into_row_value(), RowValue::F64(1.5));
        assert_eq!("hi".into_row_value(), RowValue::Str("hi".into()));
        assert_eq!(vec![1u8, 2].into_row_value(), RowValue::Bytes(vec![1u8, 2]));
        assert_eq!(
            Some(4u32).into_row_value(),
            RowValue::Optional(Some(Box::new(RowValue::U32(4))))
        );
        assert_eq!(
            Option::<u32>::None.into_row_value(),
            RowValue::Optional(None)
        );
        assert_eq!(
            Timestamp::from_micros(9).into_row_value(),
            RowValue::Timestamp(Timestamp::from_micros(9))
        );
    }
}
