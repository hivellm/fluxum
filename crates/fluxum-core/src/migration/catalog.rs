//! The persisted schema catalog (MIG-020): a serializable snapshot of the
//! layout-relevant part of the compiled schema, stored MessagePack-encoded
//! in `__schema_meta__` under [`super::META_KEY_CATALOG`].
//!
//! Only what determines row layout is persisted — column names, column
//! types, and primary-key ordinals. Secondary indexes, `#[unique]`
//! constraints, and visibility rules are rebuilt from the compiled schema
//! on every recovery, so their evolution never needs a migration.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{FluxumError, Result};
use crate::schema::{FluxType, Schema, TableSchema};

/// Serializable mirror of the closed [`FluxType`] column universe.
/// Variant order freezes at G5 with the rest of the persisted formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredType {
    /// `bool`.
    Bool,
    /// `i8`.
    I8,
    /// `i16`.
    I16,
    /// `i32`.
    I32,
    /// `i64`.
    I64,
    /// `u8`.
    U8,
    /// `u16`.
    U16,
    /// `u32`.
    U32,
    /// `u64`.
    U64,
    /// `f32`.
    F32,
    /// `f64`.
    F64,
    /// `String`.
    Str,
    /// `Vec<u8>`.
    Bytes,
    /// [`crate::types::Identity`].
    Identity,
    /// [`crate::types::ConnectionId`].
    ConnectionId,
    /// [`crate::types::EntityId`].
    EntityId,
    /// [`crate::types::Timestamp`].
    Timestamp,
    /// [`crate::types::Decimal`] — exact fixed-point (SPEC-017 CT-020).
    Decimal,
    /// [`crate::types::BlobRef`] — content-hash blob reference (DMX-040).
    Blob,
    /// `Option<T>`.
    Option(Box<StoredType>),
    /// `Vec<T>`.
    List(Box<StoredType>),
    /// A `#[derive(FluxType)]` tagged union (SPEC-023 DMX-030).
    Enum {
        /// Enum type name.
        name: String,
        /// Variants in declaration order.
        variants: Vec<StoredVariant>,
    },
    /// A `#[derive(FluxType)]` nested struct (SPEC-023 DMX-030).
    Struct {
        /// Struct type name.
        name: String,
        /// Fields in declaration order.
        fields: Vec<StoredColumn>,
    },
}

/// One variant of a stored [`StoredType::Enum`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredVariant {
    /// Variant name.
    pub name: String,
    /// Payload field types in declaration order; empty for a unit variant.
    pub payload: Vec<StoredType>,
}

impl From<&FluxType> for StoredType {
    fn from(ty: &FluxType) -> Self {
        match ty {
            FluxType::Bool => Self::Bool,
            FluxType::I8 => Self::I8,
            FluxType::I16 => Self::I16,
            FluxType::I32 => Self::I32,
            FluxType::I64 => Self::I64,
            FluxType::U8 => Self::U8,
            FluxType::U16 => Self::U16,
            FluxType::U32 => Self::U32,
            FluxType::U64 => Self::U64,
            FluxType::F32 => Self::F32,
            FluxType::F64 => Self::F64,
            FluxType::Str => Self::Str,
            FluxType::Bytes => Self::Bytes,
            FluxType::Identity => Self::Identity,
            FluxType::ConnectionId => Self::ConnectionId,
            FluxType::EntityId => Self::EntityId,
            FluxType::Timestamp => Self::Timestamp,
            FluxType::Decimal => Self::Decimal,
            FluxType::Blob => Self::Blob,
            FluxType::Option(inner) => Self::Option(Box::new(Self::from(*inner))),
            FluxType::List(inner) => Self::List(Box::new(Self::from(*inner))),
            FluxType::Enum(schema) => Self::Enum {
                name: schema.name.to_owned(),
                variants: schema
                    .variants
                    .iter()
                    .map(|v| StoredVariant {
                        name: v.name.to_owned(),
                        payload: v.payload.iter().map(Self::from).collect(),
                    })
                    .collect(),
            },
            FluxType::Struct(schema) => Self::Struct {
                name: schema.name.to_owned(),
                fields: schema
                    .fields
                    .iter()
                    .map(|f| StoredColumn {
                        name: f.name.to_owned(),
                        ty: Self::from(&f.ty),
                    })
                    .collect(),
            },
        }
    }
}

impl fmt::Display for StoredType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Option(inner) => write!(f, "Option<{inner}>"),
            Self::List(inner) => write!(f, "Vec<{inner}>"),
            other => write!(f, "{other:?}"),
        }
    }
}

/// One column of a stored table layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredColumn {
    /// Column name as declared on the struct at the time of persistence.
    pub name: String,
    /// Column type.
    pub ty: StoredType,
}

/// One table's persisted layout: columns in declaration order plus the
/// primary-key ordinals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTable {
    /// Columns in declaration (== row value) order.
    pub columns: Vec<StoredColumn>,
    /// Primary-key column ordinals, in key declaration order.
    pub primary_key: Vec<u16>,
}

impl StoredTable {
    /// Whether a column named `name` exists in this layout.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|column| column.name == name)
    }
}

impl From<&TableSchema> for StoredTable {
    fn from(table: &TableSchema) -> Self {
        Self {
            columns: table
                .columns
                .iter()
                .map(|column| StoredColumn {
                    name: column.name.to_owned(),
                    ty: StoredType::from(&column.ty),
                })
                .collect(),
            primary_key: table.primary_key.to_vec(),
        }
    }
}

/// The persisted schema catalog: every application table's layout, keyed by
/// table name (MIG-020). System tables (`__…__`) are never part of the
/// catalog — their layout is owned by the runtime, not by migrations.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StoredCatalog {
    /// Table name → persisted layout.
    pub tables: BTreeMap<String, StoredTable>,
}

/// Whether `name` is a runtime-owned system table (`__…__`), excluded from
/// catalog tracking and diffing.
pub fn is_system_table(name: &str) -> bool {
    name.starts_with("__") && name.ends_with("__")
}

impl StoredCatalog {
    /// Snapshot the layout of every application table of an assembled
    /// schema.
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            tables: schema
                .tables()
                .filter(|table| !is_system_table(table.name))
                .map(|table| (table.name.to_owned(), StoredTable::from(table)))
                .collect(),
        }
    }

    /// MessagePack-encode this catalog for `__schema_meta__` (MIG-002).
    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self)
            .map_err(|e| FluxumError::Storage(format!("schema catalog encoding failed: {e}")))
    }

    /// Decode a catalog persisted by [`StoredCatalog::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(|e| {
            FluxumError::Storage(format!(
                "__schema_meta__.{} failed MessagePack decode: {e}",
                super::META_KEY_CATALOG
            ))
        })
    }
}

/// MessagePack-encode a schema version for `__schema_meta__` (MIG-002).
pub(crate) fn encode_version(version: u32) -> Result<Vec<u8>> {
    rmp_serde::to_vec(&version)
        .map_err(|e| FluxumError::Storage(format!("schema version encoding failed: {e}")))
}

/// Decode a schema version persisted by [`encode_version`].
pub(crate) fn decode_version(bytes: &[u8]) -> Result<u32> {
    rmp_serde::from_slice(bytes).map_err(|e| {
        FluxumError::Storage(format!(
            "__schema_meta__.{} failed MessagePack decode: {e}",
            super::META_KEY_VERSION
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::SCHEMA_META;
    use crate::schema::{ColumnSchema, TableAccess, VisibilityRule};

    static TASK_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "title",
            ty: FluxType::Str,
        },
        ColumnSchema {
            name: "tags",
            ty: FluxType::List(&FluxType::U16),
        },
        ColumnSchema {
            name: "note",
            ty: FluxType::Option(&FluxType::Str),
        },
    ];

    static TASK: TableSchema = TableSchema {
        name: "Task",
        columns: TASK_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    #[test]
    fn catalog_snapshots_layout_and_round_trips() {
        let schema = Schema::from_tables([&TASK, &SCHEMA_META])
            .unwrap_or_else(|e| panic!("must assemble: {e}"));
        let catalog = StoredCatalog::from_schema(&schema);
        // System tables are excluded.
        assert_eq!(catalog.tables.len(), 1);
        let task = &catalog.tables["Task"];
        assert_eq!(task.primary_key, vec![0]);
        assert_eq!(task.columns.len(), 4);
        assert_eq!(
            task.columns[2].ty,
            StoredType::List(Box::new(StoredType::U16))
        );
        assert_eq!(
            task.columns[3].ty,
            StoredType::Option(Box::new(StoredType::Str))
        );
        assert!(task.has_column("title"));
        assert!(!task.has_column("nope"));

        let bytes = catalog.encode().unwrap_or_else(|e| panic!("{e}"));
        let decoded = StoredCatalog::decode(&bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(decoded, catalog);
    }

    #[test]
    fn version_round_trips_and_rejects_garbage() {
        let bytes = encode_version(42).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(decode_version(&bytes).unwrap_or_else(|e| panic!("{e}")), 42);
        assert!(decode_version(&[0xC1]).is_err()); // reserved MessagePack byte
    }

    #[test]
    fn stored_type_displays_readably() {
        assert_eq!(StoredType::F32.to_string(), "F32");
        assert_eq!(
            StoredType::Option(Box::new(StoredType::Str)).to_string(),
            "Option<Str>"
        );
        assert_eq!(
            StoredType::List(Box::new(StoredType::U16)).to_string(),
            "Vec<U16>"
        );
    }

    #[test]
    fn system_table_detection() {
        assert!(is_system_table("__schema_meta__"));
        assert!(is_system_table("__schedule__"));
        assert!(!is_system_table("Task"));
        assert!(!is_system_table("__weird"));
    }

    #[test]
    fn stored_type_maps_the_full_flux_type_universe() {
        let pairs = [
            (FluxType::Bool, StoredType::Bool),
            (FluxType::I8, StoredType::I8),
            (FluxType::I16, StoredType::I16),
            (FluxType::I32, StoredType::I32),
            (FluxType::I64, StoredType::I64),
            (FluxType::U8, StoredType::U8),
            (FluxType::U16, StoredType::U16),
            (FluxType::U32, StoredType::U32),
            (FluxType::U64, StoredType::U64),
            (FluxType::F32, StoredType::F32),
            (FluxType::F64, StoredType::F64),
            (FluxType::Str, StoredType::Str),
            (FluxType::Bytes, StoredType::Bytes),
            (FluxType::Identity, StoredType::Identity),
            (FluxType::ConnectionId, StoredType::ConnectionId),
            (FluxType::EntityId, StoredType::EntityId),
            (FluxType::Timestamp, StoredType::Timestamp),
            (FluxType::Decimal, StoredType::Decimal),
        ];
        for (flux, stored) in pairs {
            assert_eq!(StoredType::from(&flux), stored, "{flux:?}");
        }
        assert_eq!(
            StoredType::from(&FluxType::Option(&FluxType::I8)),
            StoredType::Option(Box::new(StoredType::I8))
        );
        assert_eq!(
            StoredType::from(&FluxType::List(&FluxType::Bool)),
            StoredType::List(Box::new(StoredType::Bool))
        );

        // Rich types (SPEC-023 DMX-030) map to structured stored shapes.
        static POINT_FIELDS: &[crate::schema::FieldSchema] = &[
            crate::schema::FieldSchema {
                name: "x",
                ty: FluxType::I32,
            },
            crate::schema::FieldSchema {
                name: "y",
                ty: FluxType::I32,
            },
        ];
        static POINT: crate::schema::StructSchema = crate::schema::StructSchema {
            name: "Point",
            fields: POINT_FIELDS,
        };
        static STATUS_VARIANTS: &[crate::schema::VariantSchema] = &[
            crate::schema::VariantSchema {
                name: "Todo",
                payload: &[],
            },
            crate::schema::VariantSchema {
                name: "Done",
                payload: &[FluxType::Identity],
            },
        ];
        static STATUS: crate::schema::EnumSchema = crate::schema::EnumSchema {
            name: "Status",
            variants: STATUS_VARIANTS,
        };
        assert_eq!(
            StoredType::from(&FluxType::Struct(&POINT)),
            StoredType::Struct {
                name: "Point".into(),
                fields: vec![
                    StoredColumn {
                        name: "x".into(),
                        ty: StoredType::I32,
                    },
                    StoredColumn {
                        name: "y".into(),
                        ty: StoredType::I32,
                    },
                ],
            }
        );
        assert_eq!(
            StoredType::from(&FluxType::Enum(&STATUS)),
            StoredType::Enum {
                name: "Status".into(),
                variants: vec![
                    StoredVariant {
                        name: "Todo".into(),
                        payload: vec![],
                    },
                    StoredVariant {
                        name: "Done".into(),
                        payload: vec![StoredType::Identity],
                    },
                ],
            }
        );
    }

    #[test]
    fn catalog_decode_rejects_garbage() {
        let err = match StoredCatalog::decode(&[0xC1]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("garbage must not decode"),
        };
        assert!(err.contains("schema_catalog"), "{err}");
        assert!(err.contains("MessagePack decode"), "{err}");
    }
}
