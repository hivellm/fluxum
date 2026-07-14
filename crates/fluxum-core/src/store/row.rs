//! Dynamic row representation and FluxBIN primary-key encoding.
//!
//! [`RowValue`] mirrors the closed [`FluxType`] column universe (SPEC-001
//! §3) one variant per type, so a row is a value vector in column
//! declaration order and the store can hold any registered table without
//! codegen. The typed `TxHandle` accessors (SPEC-004, T3.x) layer on top.
//!
//! Primary keys are the FluxBIN encoding of the PK column values in
//! `TableSchema::primary_key` order (see the module docs in
//! [`super`] for the trade-offs of that decision).

use std::fmt;
use std::sync::Arc;

use fluxum_protocol::{FluxBinError, FluxBinWriter};

use crate::error::{FluxumError, Result};
use crate::schema::{FluxType, TableSchema};
use crate::types::{ConnectionId, EntityId, Identity, Timestamp};

/// One column value, from the closed SPEC-001 type universe.
///
/// `PartialEq` is derived, so `F32`/`F64` follow IEEE semantics: a `NaN`
/// never equals itself. Consequence: reinserting a tx-deleted row whose
/// float column holds `NaN` does not *cancel* (STG-007) — it merges as an
/// update with identical bytes, which is semantically equivalent.
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    /// `bool` column.
    Bool(bool),
    /// `i8` column.
    I8(i8),
    /// `i16` column.
    I16(i16),
    /// `i32` column.
    I32(i32),
    /// `i64` column.
    I64(i64),
    /// `u8` column.
    U8(u8),
    /// `u16` column.
    U16(u16),
    /// `u32` column.
    U32(u32),
    /// `u64` column.
    U64(u64),
    /// `f32` column.
    F32(f32),
    /// `f64` column.
    F64(f64),
    /// `String` column.
    Str(String),
    /// `Vec<u8>` column.
    Bytes(Vec<u8>),
    /// [`Identity`] column.
    Identity(Identity),
    /// [`ConnectionId`] column.
    ConnectionId(ConnectionId),
    /// [`EntityId`] column.
    EntityId(EntityId),
    /// [`Timestamp`] column.
    Timestamp(Timestamp),
    /// `Option<T>` column (DM-012).
    Optional(Option<Box<RowValue>>),
    /// `Vec<T>` column (DM-012).
    List(Vec<RowValue>),
}

impl RowValue {
    /// Whether this value inhabits column type `ty`.
    pub fn matches_type(&self, ty: &FluxType) -> bool {
        match (self, ty) {
            (Self::Bool(_), FluxType::Bool)
            | (Self::I8(_), FluxType::I8)
            | (Self::I16(_), FluxType::I16)
            | (Self::I32(_), FluxType::I32)
            | (Self::I64(_), FluxType::I64)
            | (Self::U8(_), FluxType::U8)
            | (Self::U16(_), FluxType::U16)
            | (Self::U32(_), FluxType::U32)
            | (Self::U64(_), FluxType::U64)
            | (Self::F32(_), FluxType::F32)
            | (Self::F64(_), FluxType::F64)
            | (Self::Str(_), FluxType::Str)
            | (Self::Bytes(_), FluxType::Bytes)
            | (Self::Identity(_), FluxType::Identity)
            | (Self::ConnectionId(_), FluxType::ConnectionId)
            | (Self::EntityId(_), FluxType::EntityId)
            | (Self::Timestamp(_), FluxType::Timestamp)
            | (Self::Optional(None), FluxType::Option(_)) => true,
            (Self::Optional(Some(inner)), FluxType::Option(inner_ty)) => {
                inner.matches_type(inner_ty)
            }
            (Self::List(items), FluxType::List(inner_ty)) => {
                items.iter().all(|item| item.matches_type(inner_ty))
            }
            _ => false,
        }
    }

    /// FluxBIN-encode this value (SPEC-006 RPC-040 rules).
    fn encode(&self, w: &mut FluxBinWriter) -> Result<()> {
        match self {
            Self::Bool(v) => w.write_bool(*v),
            Self::I8(v) => w.write_i8(*v),
            Self::I16(v) => w.write_i16(*v),
            Self::I32(v) => w.write_i32(*v),
            Self::I64(v) => w.write_i64(*v),
            Self::U8(v) => w.write_u8(*v),
            Self::U16(v) => w.write_u16(*v),
            Self::U32(v) => w.write_u32(*v),
            Self::U64(v) => w.write_u64(*v),
            Self::F32(v) => w.write_f32(*v),
            Self::F64(v) => w.write_f64(*v),
            Self::Str(v) => w.write_str(v).map_err(codec_err)?,
            Self::Bytes(v) => w.write_bytes(v).map_err(codec_err)?,
            Self::Identity(v) => w.write_identity(v.as_bytes()),
            Self::ConnectionId(v) => w.write_connection_id(v.as_u128()),
            Self::EntityId(v) => w.write_entity_id(v.as_u64()),
            Self::Timestamp(v) => w.write_timestamp(v.as_micros()),
            Self::Optional(None) => w.write_option_tag(false),
            Self::Optional(Some(inner)) => {
                w.write_option_tag(true);
                inner.encode(w)?;
            }
            Self::List(items) => {
                let count = u32::try_from(items.len()).map_err(|_| {
                    FluxumError::Storage(format!(
                        "list value of {} items exceeds the u32 FluxBIN count prefix",
                        items.len()
                    ))
                })?;
                w.write_seq_len(count);
                for item in items {
                    item.encode(w)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for RowValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(v) => write!(f, "{v}"),
            Self::I8(v) => write!(f, "{v}"),
            Self::I16(v) => write!(f, "{v}"),
            Self::I32(v) => write!(f, "{v}"),
            Self::I64(v) => write!(f, "{v}"),
            Self::U8(v) => write!(f, "{v}"),
            Self::U16(v) => write!(f, "{v}"),
            Self::U32(v) => write!(f, "{v}"),
            Self::U64(v) => write!(f, "{v}"),
            Self::F32(v) => write!(f, "{v}"),
            Self::F64(v) => write!(f, "{v}"),
            Self::Str(v) => write!(f, "{v:?}"),
            Self::Bytes(v) => write!(f, "0x{}", hex(v)),
            Self::Identity(v) => write!(f, "{v}"),
            Self::ConnectionId(v) => write!(f, "{v}"),
            Self::EntityId(v) => write!(f, "{v}"),
            Self::Timestamp(v) => write!(f, "{v}"),
            Self::Optional(None) => write!(f, "null"),
            Self::Optional(Some(inner)) => write!(f, "{inner}"),
            Self::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn codec_err(e: FluxBinError) -> FluxumError {
    FluxumError::Storage(format!("primary-key FluxBIN encoding failed: {e}"))
}

/// One stored row: column values in declaration order, `Arc`-shared so
/// snapshots, `TxDiff`s, and copy-on-write table clones never copy payloads.
#[derive(Debug, Clone, PartialEq)]
pub struct Row(Arc<[RowValue]>);

impl Row {
    /// Build a row from column values in declaration order.
    pub fn new(values: Vec<RowValue>) -> Self {
        Self(values.into())
    }

    /// All column values in declaration order.
    pub fn values(&self) -> &[RowValue] {
        &self.0
    }

    /// The value of the column at `ordinal`, if in range.
    pub fn value(&self, ordinal: u16) -> Option<&RowValue> {
        self.0.get(usize::from(ordinal))
    }

    /// Whether two `Row`s share the same allocation — i.e. the same committed
    /// row identity, not merely equal content (STG-007 rule 1).
    pub fn same_identity(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// An encoded primary key: FluxBIN bytes of the PK columns in
/// `TableSchema::primary_key` order. Byte equality is row identity;
/// `BTreeMap` ordering over these bytes is deterministic but not numeric
/// (see the module docs of [`super`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PkBytes(Arc<[u8]>);

impl PkBytes {
    /// The raw encoded key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for PkBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex(&self.0))
    }
}

/// Validate `values` as a full row of `schema`: exact arity, every value
/// inhabiting its column type.
pub(crate) fn check_row(schema: &TableSchema, values: &[RowValue]) -> Result<()> {
    if values.len() != schema.columns.len() {
        return Err(FluxumError::Storage(format!(
            "table `{}`: row has {} values but the schema declares {} columns",
            schema.name,
            values.len(),
            schema.columns.len()
        )));
    }
    for (value, column) in values.iter().zip(schema.columns) {
        if !value.matches_type(&column.ty) {
            return Err(FluxumError::Storage(format!(
                "table `{}`: column `{}` expects {:?}, got {value}",
                schema.name, column.name, column.ty
            )));
        }
    }
    Ok(())
}

/// Encode the primary key of a full row (values in column declaration
/// order), reading the PK columns per `schema.primary_key`.
pub(crate) fn encode_pk_of_row(schema: &TableSchema, values: &[RowValue]) -> Result<PkBytes> {
    let mut w = FluxBinWriter::new();
    for &ordinal in schema.primary_key {
        let value = values.get(usize::from(ordinal)).ok_or_else(|| {
            FluxumError::Storage(format!(
                "table `{}`: primary key ordinal {ordinal} out of range",
                schema.name
            ))
        })?;
        value.encode(&mut w)?;
    }
    Ok(PkBytes(w.into_bytes().into()))
}

/// Encode a primary key given only the key column values, in
/// `schema.primary_key` declaration order (e.g. `(grid_x, grid_y)` for a
/// composite PK). Each value is type-checked against its PK column.
pub(crate) fn encode_pk_values(schema: &TableSchema, pk_values: &[RowValue]) -> Result<PkBytes> {
    if pk_values.len() != schema.primary_key.len() {
        return Err(FluxumError::Storage(format!(
            "table `{}`: primary key takes {} value(s), got {}",
            schema.name,
            schema.primary_key.len(),
            pk_values.len()
        )));
    }
    let mut w = FluxBinWriter::new();
    for (&ordinal, value) in schema.primary_key.iter().zip(pk_values) {
        let column = schema.column(ordinal).ok_or_else(|| {
            FluxumError::Storage(format!(
                "table `{}`: primary key ordinal {ordinal} out of range",
                schema.name
            ))
        })?;
        if !value.matches_type(&column.ty) {
            return Err(FluxumError::Storage(format!(
                "table `{}`: primary key column `{}` expects {:?}, got {value}",
                schema.name, column.name, column.ty
            )));
        }
        value.encode(&mut w)?;
    }
    Ok(PkBytes(w.into_bytes().into()))
}

/// Human-readable PK for error messages: `(v1, v2, …)` from the row's PK
/// columns.
pub(crate) fn display_pk_of_row(schema: &TableSchema, values: &[RowValue]) -> String {
    let parts: Vec<String> = schema
        .primary_key
        .iter()
        .filter_map(|&ordinal| values.get(usize::from(ordinal)))
        .map(ToString::to_string)
        .collect();
    format!("({})", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    static SENSOR_COLS: &[ColumnSchema] = &[
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
        ColumnSchema {
            name: "label",
            ty: FluxType::Option(&FluxType::Str),
        },
        ColumnSchema {
            name: "history",
            ty: FluxType::List(&FluxType::U16),
        },
    ];

    static SENSOR: TableSchema = TableSchema {
        name: "Sensor",
        columns: SENSOR_COLS,
        primary_key: &[0, 1],
        auto_inc: None,
        access: crate::schema::TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: crate::schema::VisibilityRule::PublicAll,
    };

    fn sensor_row() -> Vec<RowValue> {
        vec![
            RowValue::I32(-2),
            RowValue::I32(9),
            RowValue::F64(101.25),
            RowValue::Optional(Some(Box::new(RowValue::Str("north".into())))),
            RowValue::List(vec![RowValue::U16(1), RowValue::U16(2)]),
        ]
    }

    #[test]
    fn check_row_accepts_matching_types_including_nested() {
        check_row(&SENSOR, &sensor_row()).unwrap_or_else(|e| panic!("{e}"));
        // None inhabits Option<T> regardless of T.
        let mut row = sensor_row();
        row[3] = RowValue::Optional(None);
        check_row(&SENSOR, &row).unwrap_or_else(|e| panic!("{e}"));
        // Empty list inhabits List<T>.
        row[4] = RowValue::List(vec![]);
        check_row(&SENSOR, &row).unwrap_or_else(|e| panic!("{e}"));
    }

    #[test]
    fn check_row_rejects_arity_and_type_mismatches() {
        let short = vec![RowValue::I32(1)];
        let err = check_row(&SENSOR, &short).map(|()| "ok");
        assert!(format!("{err:?}").contains("declares 5 columns"), "{err:?}");

        let mut wrong = sensor_row();
        wrong[2] = RowValue::Str("not a float".into());
        let err = check_row(&SENSOR, &wrong).map(|()| "ok");
        assert!(format!("{err:?}").contains("column `reading`"), "{err:?}");

        // A list with one mistyped element is rejected.
        let mut bad_list = sensor_row();
        bad_list[4] = RowValue::List(vec![RowValue::U16(1), RowValue::Bool(true)]);
        assert!(check_row(&SENSOR, &bad_list).is_err());
    }

    #[test]
    fn composite_pk_encodes_in_declaration_order_as_fluxbin() {
        let row = sensor_row();
        let pk = encode_pk_of_row(&SENSOR, &row).unwrap_or_else(|e| panic!("{e}"));
        // i32 LE (-2), i32 LE (9) back-to-back.
        assert_eq!(
            pk.as_bytes(),
            [0xFE, 0xFF, 0xFF, 0xFF, 0x09, 0x00, 0x00, 0x00]
        );
        // Key-values form matches the full-row form.
        let same = encode_pk_values(&SENSOR, &[RowValue::I32(-2), RowValue::I32(9)])
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(pk, same);
        assert_eq!(pk.to_string(), "0xfeffffff09000000");
    }

    #[test]
    fn encode_pk_values_validates_arity_and_types() {
        assert!(encode_pk_values(&SENSOR, &[RowValue::I32(1)]).is_err());
        assert!(encode_pk_values(&SENSOR, &[RowValue::I32(1), RowValue::Str("x".into())]).is_err());
    }

    #[test]
    fn row_identity_is_allocation_identity() {
        let a = Row::new(sensor_row());
        let b = a.clone();
        let c = Row::new(sensor_row());
        assert!(a.same_identity(&b));
        assert!(!a.same_identity(&c));
        assert_eq!(a, c); // equal content, distinct identity
        assert_eq!(a.value(0), Some(&RowValue::I32(-2)));
        assert_eq!(a.value(99), None);
        assert_eq!(a.values().len(), 5);
    }

    #[test]
    fn display_forms_are_stable() {
        let row = sensor_row();
        assert_eq!(display_pk_of_row(&SENSOR, &row), "(-2, 9)");
        assert_eq!(RowValue::Bytes(vec![0xAB, 0x01]).to_string(), "0xab01");
        assert_eq!(RowValue::Optional(None).to_string(), "null");
        assert_eq!(
            RowValue::List(vec![RowValue::U16(1), RowValue::U16(2)]).to_string(),
            "[1, 2]"
        );
        assert_eq!(RowValue::Str("hi".into()).to_string(), "\"hi\"");
    }
}
