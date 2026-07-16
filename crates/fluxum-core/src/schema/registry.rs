//! Link-time table registry and startup schema assembly (DM-040).
//!
//! Every `#[fluxum::table]` expansion submits a [`TableDef`] to the
//! [`inventory`] registry of the final binary. [`Schema::assemble`] collects
//! and validates them; `ServerBuilder::build()` (later phase) aborts startup
//! on the first validation failure with a descriptive [`FluxumError::Schema`].
//!
//! Constraints checkable from a single struct are *also* rejected at compile
//! time by the proc macro (SPEC-001 acceptance 1); the checks here are the
//! runtime backstop and cover whole-schema properties such as duplicate table
//! names across crates.

use std::collections::BTreeMap;

use crate::error::{FluxumError, Result};
use crate::schema::{FluxType, IndexSchema, SpatialKind, TableAccess, TableSchema, VisibilityRule};

/// One link-time table registration. Submitted by `#[fluxum::table]` via
/// `::fluxum_core::schema::inventory::submit!`.
pub struct TableDef(pub &'static TableSchema);

inventory::collect!(TableDef);

/// Iterate every table registered in this binary, in linker order.
///
/// Note (OQ-1 spike): a crate that is linked but never *referenced* is
/// dropped by the linker together with its registrations — application
/// binaries must reference their module crates (e.g. `use my_module;`).
pub fn registered_tables() -> impl Iterator<Item = &'static TableSchema> {
    inventory::iter::<TableDef>().map(|def| def.0)
}

/// The validated, assembled schema of one server binary (DM-040).
///
/// Fixed for the lifetime of the process (DM-041).
#[derive(Debug, Clone, Default)]
pub struct Schema {
    tables: BTreeMap<&'static str, &'static TableSchema>,
}

impl Schema {
    /// Collect every link-time registered table and validate the result.
    ///
    /// Called by `ServerBuilder::build()` before any transport opens; a
    /// [`FluxumError::Schema`] here must abort startup.
    pub fn assemble() -> Result<Self> {
        Self::from_tables(registered_tables())
    }

    /// Build a schema from an explicit table list (test seam; `assemble` is
    /// the production path).
    pub fn from_tables(tables: impl IntoIterator<Item = &'static TableSchema>) -> Result<Self> {
        let mut map: BTreeMap<&'static str, &'static TableSchema> = BTreeMap::new();
        for table in tables {
            validate_table(table)?;
            if let Some(advisory) = spatial_stream_advisory(table) {
                // SPX-040: advisory, never a rejection — bounded-rate
                // location tracking is a fully supported workload.
                tracing::warn!(target: "fluxum::schema", "{advisory}");
            }
            if map.insert(table.name, table).is_some() {
                return Err(FluxumError::Schema(format!(
                    "duplicate table name `{}`: every #[fluxum::table] struct must have a \
                     unique name across all linked crates (DM-040)",
                    table.name
                )));
            }
        }
        let schema = Self { tables: map };
        // SPEC-017 CT-051: registered column transforms must resolve against
        // this schema (runtime backstop behind the macro's compile-time
        // rejections; covers hand-registered defs).
        crate::transform::validate_registered(&schema)?;
        Ok(schema)
    }

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> Option<&'static TableSchema> {
        self.tables.get(name).copied()
    }

    /// Iterate all tables in name order.
    pub fn tables(&self) -> impl Iterator<Item = &'static TableSchema> + '_ {
        self.tables.values().copied()
    }

    /// Number of registered tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Whether no table is registered.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// The SPX-040 event-stream advisory for a `#[spatial]` table, if its name
/// matches a common event-stream pattern (`*Log`, `*Stream`, `*Tick`,
/// `*Trace`, `*History`).
///
/// Non-fatal by design: spatial indexes serve persistent geospatial state
/// updated at a bounded cadence; an unbounded high-frequency position stream
/// pays O(log n) index maintenance per append and should be downsampled or
/// aggregated before persisting. Emitted as a `tracing` warning during
/// schema assembly; exposed so tooling and tests can evaluate it directly.
pub fn spatial_stream_advisory(table: &TableSchema) -> Option<String> {
    let has_spatial = table
        .indexes
        .iter()
        .any(|index| matches!(index, IndexSchema::Spatial { .. }));
    if !has_spatial {
        return None;
    }
    const STREAM_SUFFIXES: &[&str] = &["log", "stream", "tick", "trace", "history"];
    let name = table.name.to_ascii_lowercase();
    let suffix = STREAM_SUFFIXES.iter().find(|s| name.ends_with(*s))?;
    Some(format!(
        "table `{}`: #[spatial] on a `*{suffix}`-named table — spatial indexes are for \
         persistent geospatial state with a bounded update cadence, not for unbounded \
         high-frequency position streams; downsample or aggregate the stream before \
         persisting (SPX-040, advisory only)",
        table.name
    ))
}

/// Validate one table against the DM-040 startup checks.
fn validate_table(t: &'static TableSchema) -> Result<()> {
    let err = |msg: String| Err(FluxumError::Schema(format!("table `{}`: {msg}", t.name)));

    if t.columns.is_empty() {
        return err("a table must have at least one column (DM-001)".into());
    }
    let column = |ordinal: u16, referent: &str| -> Result<&'static super::ColumnSchema> {
        t.column(ordinal).ok_or_else(|| {
            FluxumError::Schema(format!(
                "table `{}`: {referent} references column ordinal {ordinal}, but the table \
                 has {} columns",
                t.name,
                t.columns.len()
            ))
        })
    };

    // Primary key: exactly one declaration, all ordinals valid and distinct.
    if t.primary_key.is_empty() {
        return err("zero primary key declarations (DM-002)".into());
    }
    for ordinal in t.primary_key {
        column(*ordinal, "primary_key")?;
    }
    let mut pk = t.primary_key.to_vec();
    pk.sort_unstable();
    pk.dedup();
    if pk.len() != t.primary_key.len() {
        return err("primary key lists the same column twice (DM-002)".into());
    }

    // Auto-increment: single-column u64 primary key only.
    if let Some(ordinal) = t.auto_inc {
        if t.primary_key != [ordinal] {
            return err(format!(
                "#[auto_inc] on column {ordinal} requires that column to be the single \
                 #[primary_key] column (DM-004)"
            ));
        }
        let col = column(ordinal, "#[auto_inc]")?;
        if col.ty != FluxType::U64 {
            return err(format!(
                "#[auto_inc] column `{}` must be u64, found {:?} (DM-004)",
                col.name, col.ty
            ));
        }
    }

    // Partitioning: valid column, never combined with `global`.
    if let Some(ordinal) = t.partition_by {
        column(ordinal, "partition_by")?;
        if t.access == TableAccess::Global {
            return err(
                "partition_by cannot be combined with `global`: global tables are \
                 replicated to every shard, not partitioned (DM-008)"
                    .into(),
            );
        }
    }

    // Unique constraints: non-empty, valid ordinals.
    for set in t.unique {
        if set.is_empty() {
            return err("#[unique] with an empty column list (DM-006)".into());
        }
        for ordinal in *set {
            column(*ordinal, "#[unique]")?;
        }
    }

    // Indexes: valid ordinals, spatial arity + float columns, one spatial
    // family per table, no duplicate same-type index on one column set.
    let mut spatial_kind: Option<SpatialKind> = None;
    for (i, index) in t.indexes.iter().enumerate() {
        match index {
            IndexSchema::BTree { columns } => {
                if columns.is_empty() {
                    return err("#[index(btree())] with an empty column list (DM-030)".into());
                }
                for ordinal in *columns {
                    column(*ordinal, "#[index(btree)]")?;
                }
            }
            IndexSchema::Spatial { kind, columns } => {
                if let Some(existing) = spatial_kind
                    && existing != *kind
                {
                    return err(
                        "a table cannot declare both quadtree and rtree spatial indexes \
                         (DM-033)"
                            .into(),
                    );
                }
                spatial_kind = Some(*kind);
                let arity = match kind {
                    SpatialKind::QuadTree => 2,
                    SpatialKind::RTree => 4,
                };
                if columns.len() != arity {
                    return err(format!(
                        "{kind:?} spatial index needs exactly {arity} coordinate columns, \
                         found {} (DM-032)",
                        columns.len()
                    ));
                }
                for ordinal in *columns {
                    let col = column(*ordinal, "#[spatial]")?;
                    if !col.ty.is_float() {
                        return err(format!(
                            "spatial index column `{}` must be f32 or f64, found {:?} (DM-032)",
                            col.name, col.ty
                        ));
                    }
                }
            }
        }
        if t.indexes[..i].contains(index) {
            return err(format!(
                "duplicate index declaration {index:?}: a column set cannot be indexed \
                 twice with the same index type (DM-033)"
            ));
        }
    }

    // Visibility: owner column must exist and be Identity-typed.
    if let VisibilityRule::OwnerOnly { owner } = t.visibility {
        let col = column(owner, "#[visibility(owner_only)]")?;
        if col.ty != FluxType::Identity {
            return err(format!(
                "owner_only column `{}` must be of type Identity, found {:?} (DM-060)",
                col.name, col.ty
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    // Hand-built static schemas standing in for macro output; the end-to-end
    // macro → registry path is covered by the fluxum-macros integration tests.
    static USER_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "identity",
            ty: FluxType::Identity,
        },
        ColumnSchema {
            name: "name",
            ty: FluxType::Str,
        },
    ];

    const fn user_schema() -> TableSchema {
        TableSchema {
            name: "User",
            columns: USER_COLS,
            primary_key: &[0],
            auto_inc: Some(0),
            access: TableAccess::Public,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        }
    }

    static USER: TableSchema = user_schema();

    fn assert_schema_err(table: &'static TableSchema, needle: &str) {
        match Schema::from_tables([table]) {
            Err(FluxumError::Schema(msg)) => assert!(
                msg.contains(needle),
                "error `{msg}` does not contain `{needle}`"
            ),
            other => panic!("expected Schema error containing `{needle}`, got {other:?}"),
        }
    }

    #[test]
    fn valid_table_assembles_and_introspects() {
        let schema = Schema::from_tables([&USER]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(schema.len(), 1);
        assert!(!schema.is_empty());
        let user = match schema.table("User") {
            Some(t) => t,
            None => panic!("User not found"),
        };
        assert_eq!(user.primary_key, &[0]);
        assert_eq!(user.auto_inc, Some(0));
        assert_eq!(user.access, TableAccess::Public);
        assert!(schema.table("Nope").is_none());
        assert_eq!(schema.tables().count(), 1);
    }

    #[test]
    fn duplicate_table_name_is_a_descriptive_error() {
        static DUP: TableSchema = user_schema();
        match Schema::from_tables([&USER, &DUP]) {
            Err(FluxumError::Schema(msg)) => {
                assert!(msg.contains("duplicate table name `User`"), "{msg}");
                assert!(msg.contains("DM-040"), "{msg}");
            }
            other => panic!("expected duplicate-name error, got {other:?}"),
        }
    }

    #[test]
    fn empty_pk_rejected() {
        static T: TableSchema = TableSchema {
            primary_key: &[],
            ..user_schema()
        };
        assert_schema_err(&T, "zero primary key");
    }

    #[test]
    fn out_of_range_ordinals_rejected() {
        static T: TableSchema = TableSchema {
            primary_key: &[9],
            auto_inc: None,
            ..user_schema()
        };
        assert_schema_err(&T, "references column ordinal 9");
    }

    #[test]
    fn auto_inc_must_match_single_pk() {
        static T: TableSchema = TableSchema {
            auto_inc: Some(1),
            ..user_schema()
        };
        assert_schema_err(&T, "requires that column to be the single #[primary_key]");
    }

    #[test]
    fn auto_inc_requires_u64() {
        static T: TableSchema = TableSchema {
            primary_key: &[1],
            auto_inc: Some(1),
            ..user_schema()
        };
        assert_schema_err(&T, "must be u64");
    }

    #[test]
    fn partition_by_global_rejected() {
        static T: TableSchema = TableSchema {
            access: TableAccess::Global,
            partition_by: Some(1),
            ..user_schema()
        };
        assert_schema_err(&T, "cannot be combined with `global`");
    }

    #[test]
    fn spatial_rules_enforced() {
        static NOT_FLOAT: TableSchema = TableSchema {
            indexes: &[IndexSchema::Spatial {
                kind: SpatialKind::QuadTree,
                columns: &[0, 1],
            }],
            ..user_schema()
        };
        assert_schema_err(&NOT_FLOAT, "must be f32 or f64");

        static BAD_ARITY: TableSchema = TableSchema {
            indexes: &[IndexSchema::Spatial {
                kind: SpatialKind::RTree,
                columns: &[0, 1],
            }],
            ..user_schema()
        };
        assert_schema_err(&BAD_ARITY, "exactly 4 coordinate columns");
    }

    #[test]
    fn mixed_spatial_kinds_rejected() {
        static FLOATS: &[ColumnSchema] = &[
            ColumnSchema {
                name: "id",
                ty: FluxType::U64,
            },
            ColumnSchema {
                name: "x",
                ty: FluxType::F32,
            },
            ColumnSchema {
                name: "y",
                ty: FluxType::F32,
            },
            ColumnSchema {
                name: "x2",
                ty: FluxType::F64,
            },
            ColumnSchema {
                name: "y2",
                ty: FluxType::F64,
            },
        ];
        static T: TableSchema = TableSchema {
            name: "Geo",
            columns: FLOATS,
            primary_key: &[0],
            auto_inc: None,
            access: TableAccess::Public,
            partition_by: None,
            unique: &[],
            indexes: &[
                IndexSchema::Spatial {
                    kind: SpatialKind::QuadTree,
                    columns: &[1, 2],
                },
                IndexSchema::Spatial {
                    kind: SpatialKind::RTree,
                    columns: &[1, 2, 3, 4],
                },
            ],
            visibility: VisibilityRule::PublicAll,
        };
        assert_schema_err(&T, "both quadtree and rtree");
    }

    #[test]
    fn duplicate_same_type_index_rejected() {
        static T: TableSchema = TableSchema {
            indexes: &[
                IndexSchema::BTree { columns: &[1] },
                IndexSchema::BTree { columns: &[1] },
            ],
            ..user_schema()
        };
        assert_schema_err(&T, "duplicate index");
    }

    #[test]
    fn owner_only_requires_identity_column() {
        static T: TableSchema = TableSchema {
            visibility: VisibilityRule::OwnerOnly { owner: 2 },
            ..user_schema()
        };
        assert_schema_err(&T, "must be of type Identity");

        static OK: TableSchema = TableSchema {
            visibility: VisibilityRule::OwnerOnly { owner: 1 },
            ..user_schema()
        };
        assert!(Schema::from_tables([&OK]).is_ok());
    }

    #[test]
    fn assemble_from_inventory_is_empty_in_this_crate() {
        // fluxum-core itself declares no tables; the macro integration tests
        // in fluxum-macros exercise the populated path.
        let schema = Schema::assemble().unwrap_or_else(|e| panic!("{e}"));
        assert!(schema.is_empty());
    }
}
