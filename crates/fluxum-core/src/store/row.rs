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

use fluxum_protocol::{FluxBinError, FluxBinReader, FluxBinWriter};

use crate::error::{FluxumError, Result};
use crate::schema::{FluxType, TableSchema};
use crate::types::{BlobRef, ConnectionId, Decimal, EntityId, Identity, Timestamp};

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
    /// [`Decimal`] column — exact fixed-point (SPEC-017 CT-020).
    Decimal(Decimal),
    /// [`BlobRef`] column — 32-byte content-hash reference to an out-of-row
    /// large object (SPEC-023 DMX-040).
    Blob(BlobRef),
    /// `Option<T>` column (DM-012).
    Optional(Option<Box<RowValue>>),
    /// `Vec<T>` column (DM-012).
    List(Vec<RowValue>),
    /// A `#[derive(FluxType)]` tagged-union value (SPEC-023 DMX-030): the
    /// variant ordinal (FluxBIN `u8` tag) plus its payload values.
    Enum {
        /// Variant ordinal — the FluxBIN tag; must fit in a `u8`.
        tag: u32,
        /// Payload values in the variant's declaration order.
        payload: Vec<RowValue>,
    },
    /// A `#[derive(FluxType)]` nested-struct value (SPEC-023 DMX-030): field
    /// values in declaration order.
    Struct(Vec<RowValue>),
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
            | (Self::Decimal(_), FluxType::Decimal)
            | (Self::Blob(_), FluxType::Blob)
            | (Self::Optional(None), FluxType::Option(_)) => true,
            (Self::Optional(Some(inner)), FluxType::Option(inner_ty)) => {
                inner.matches_type(inner_ty)
            }
            (Self::List(items), FluxType::List(inner_ty)) => {
                items.iter().all(|item| item.matches_type(inner_ty))
            }
            (Self::Enum { tag, payload }, FluxType::Enum(schema)) => {
                match schema.variants.get(*tag as usize) {
                    Some(variant) => {
                        variant.payload.len() == payload.len()
                            && payload
                                .iter()
                                .zip(variant.payload)
                                .all(|(value, ty)| value.matches_type(ty))
                    }
                    None => false,
                }
            }
            (Self::Struct(fields), FluxType::Struct(schema)) => {
                schema.fields.len() == fields.len()
                    && fields
                        .iter()
                        .zip(schema.fields)
                        .all(|(value, field)| value.matches_type(&field.ty))
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
            Self::Decimal(v) => w.write_decimal(v.unscaled(), v.scale()),
            Self::Blob(v) => w.write_identity(v.as_bytes()),
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
            Self::Enum { tag, payload } => {
                let tag = u8::try_from(*tag).map_err(|_| {
                    FluxumError::Storage(format!(
                        "enum variant tag {tag} exceeds the u8 FluxBIN tag width"
                    ))
                })?;
                w.write_u8(tag);
                for item in payload {
                    item.encode(w)?;
                }
            }
            Self::Struct(fields) => {
                for item in fields {
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
            Self::Decimal(v) => write!(f, "{v}"),
            Self::Blob(v) => write!(f, "blob:{v}"),
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
            Self::Enum { tag, payload } => {
                write!(f, "#{tag}")?;
                if !payload.is_empty() {
                    write!(f, "(")?;
                    for (i, item) in payload.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{item}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Self::Struct(fields) => {
                write!(f, "{{")?;
                for (i, item) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "}}")
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

    /// Build from raw encoded key bytes — the plugin/RPC boundary
    /// (SPEC-020): e.g. a retriever plugin returning candidate keys it
    /// received over Plugin RPC. The bytes must be a key previously encoded
    /// by this schema; nothing is validated here.
    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
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

/// FluxBIN-encode a full row: every column value in declaration order,
/// concatenated (SPEC-006 RPC-040). This is the leaf-page row encoding of
/// the paged cold tier (SPEC-015 TIER-021) — byte-identical to the wire
/// form, so pages, log entries, and diffs share one row representation.
pub(crate) fn encode_row(values: &[RowValue]) -> Result<Vec<u8>> {
    let mut w = FluxBinWriter::new();
    for value in values {
        value.encode(&mut w)?;
    }
    Ok(w.into_bytes())
}

/// Decode a FluxBIN row encoded by [`encode_row`], driven by the table
/// schema (FluxBIN is not self-describing). Verifies exact consumption —
/// trailing bytes are a decode error, never silently ignored.
/// FluxBIN-encode a single value to bytes — the plaintext serialization the
/// `#[encrypted]` executor seals (SPEC-017 CT-030).
pub fn encode_value_bytes(value: &RowValue) -> Result<Vec<u8>> {
    let mut w = FluxBinWriter::new();
    value.encode(&mut w)?;
    Ok(w.into_bytes())
}

/// Decode a single value of type `ty` from bytes produced by
/// [`encode_value_bytes`] (SPEC-017 CT-030 decrypt path). Rejects trailing
/// bytes.
pub fn decode_value_bytes(bytes: &[u8], ty: &FluxType) -> Result<RowValue> {
    let mut r = FluxBinReader::new(bytes);
    let value = decode_value(&mut r, ty)
        .map_err(|e| FluxumError::Storage(format!("encrypted field failed FluxBIN decode: {e}")))?;
    r.expect_eof()
        .map_err(|e| FluxumError::Storage(format!("encrypted field trailing bytes: {e}")))?;
    Ok(value)
}

pub(crate) fn decode_row(schema: &TableSchema, bytes: &[u8]) -> Result<Row> {
    let mut r = FluxBinReader::new(bytes);
    let mut values = Vec::with_capacity(schema.columns.len());
    for column in schema.columns {
        values.push(decode_value(&mut r, &column.ty).map_err(|e| {
            FluxumError::Storage(format!(
                "table `{}`: column `{}` failed FluxBIN decode: {e}",
                schema.name, column.name
            ))
        })?);
    }
    r.expect_eof().map_err(|e| {
        FluxumError::Storage(format!(
            "table `{}`: trailing bytes after the last column: {e}",
            schema.name
        ))
    })?;
    Ok(Row::new(values))
}

/// Decode one value of type `ty` (recursive for `Option`/`List`).
fn decode_value(r: &mut FluxBinReader<'_>, ty: &FluxType) -> Result<RowValue> {
    let map = |e: FluxBinError| FluxumError::Storage(e.to_string());
    Ok(match ty {
        FluxType::Bool => RowValue::Bool(r.read_bool().map_err(map)?),
        FluxType::I8 => RowValue::I8(r.read_i8().map_err(map)?),
        FluxType::I16 => RowValue::I16(r.read_i16().map_err(map)?),
        FluxType::I32 => RowValue::I32(r.read_i32().map_err(map)?),
        FluxType::I64 => RowValue::I64(r.read_i64().map_err(map)?),
        FluxType::U8 => RowValue::U8(r.read_u8().map_err(map)?),
        FluxType::U16 => RowValue::U16(r.read_u16().map_err(map)?),
        FluxType::U32 => RowValue::U32(r.read_u32().map_err(map)?),
        FluxType::U64 => RowValue::U64(r.read_u64().map_err(map)?),
        FluxType::F32 => RowValue::F32(r.read_f32().map_err(map)?),
        FluxType::F64 => RowValue::F64(r.read_f64().map_err(map)?),
        FluxType::Str => RowValue::Str(r.read_str().map_err(map)?.to_owned()),
        FluxType::Bytes => RowValue::Bytes(r.read_bytes().map_err(map)?.to_vec()),
        FluxType::Identity => {
            RowValue::Identity(Identity::from_bytes(r.read_identity().map_err(map)?))
        }
        FluxType::ConnectionId => {
            RowValue::ConnectionId(ConnectionId::new(r.read_connection_id().map_err(map)?))
        }
        FluxType::EntityId => RowValue::EntityId(EntityId::new(r.read_entity_id().map_err(map)?)),
        FluxType::Timestamp => {
            RowValue::Timestamp(Timestamp::from_micros(r.read_timestamp().map_err(map)?))
        }
        FluxType::Decimal => {
            let (unscaled, scale) = r.read_decimal().map_err(map)?;
            RowValue::Decimal(Decimal::from_parts(unscaled, scale))
        }
        FluxType::Blob => RowValue::Blob(BlobRef::from_bytes(r.read_identity().map_err(map)?)),
        FluxType::Option(inner) => {
            if r.read_option_tag().map_err(map)? {
                RowValue::Optional(Some(Box::new(decode_value(r, inner)?)))
            } else {
                RowValue::Optional(None)
            }
        }
        FluxType::List(inner) => {
            let count = r.read_seq_len().map_err(map)?;
            let mut items = Vec::with_capacity(usize::try_from(count).unwrap_or(0).min(4096));
            for _ in 0..count {
                items.push(decode_value(r, inner)?);
            }
            RowValue::List(items)
        }
        FluxType::Enum(schema) => {
            let tag = r.read_u8().map_err(map)?;
            let variant = schema.variants.get(usize::from(tag)).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "enum `{}`: variant tag {tag} out of range (0..{})",
                    schema.name,
                    schema.variants.len()
                ))
            })?;
            let mut payload = Vec::with_capacity(variant.payload.len());
            for ty in variant.payload {
                payload.push(decode_value(r, ty)?);
            }
            RowValue::Enum {
                tag: u32::from(tag),
                payload,
            }
        }
        FluxType::Struct(schema) => {
            let mut fields = Vec::with_capacity(schema.fields.len());
            for field in schema.fields {
                fields.push(decode_value(r, &field.ty)?);
            }
            RowValue::Struct(fields)
        }
    })
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
    use crate::schema::{ColumnSchema, EnumSchema, FieldSchema, StructSchema, VariantSchema};

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
    fn full_row_fluxbin_round_trips_and_rejects_trailing_bytes() {
        let row = sensor_row();
        let bytes = encode_row(&row).unwrap_or_else(|e| panic!("{e}"));
        let decoded = decode_row(&SENSOR, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(decoded.values(), &row[..]);

        let mut long = bytes.clone();
        long.push(0);
        assert!(
            decode_row(&SENSOR, &long).is_err(),
            "trailing byte accepted"
        );
        assert!(
            decode_row(&SENSOR, &bytes[..bytes.len() - 1]).is_err(),
            "truncated row accepted"
        );
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

    // --- The full closed type universe (SPEC-001 §3) -------------------------

    static UNIVERSE_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "c_bool",
            ty: FluxType::Bool,
        },
        ColumnSchema {
            name: "c_i8",
            ty: FluxType::I8,
        },
        ColumnSchema {
            name: "c_i16",
            ty: FluxType::I16,
        },
        ColumnSchema {
            name: "c_i32",
            ty: FluxType::I32,
        },
        ColumnSchema {
            name: "c_i64",
            ty: FluxType::I64,
        },
        ColumnSchema {
            name: "c_u8",
            ty: FluxType::U8,
        },
        ColumnSchema {
            name: "c_u16",
            ty: FluxType::U16,
        },
        ColumnSchema {
            name: "c_u32",
            ty: FluxType::U32,
        },
        ColumnSchema {
            name: "c_u64",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "c_f32",
            ty: FluxType::F32,
        },
        ColumnSchema {
            name: "c_f64",
            ty: FluxType::F64,
        },
        ColumnSchema {
            name: "c_str",
            ty: FluxType::Str,
        },
        ColumnSchema {
            name: "c_bytes",
            ty: FluxType::Bytes,
        },
        ColumnSchema {
            name: "c_identity",
            ty: FluxType::Identity,
        },
        ColumnSchema {
            name: "c_conn",
            ty: FluxType::ConnectionId,
        },
        ColumnSchema {
            name: "c_entity",
            ty: FluxType::EntityId,
        },
        ColumnSchema {
            name: "c_ts",
            ty: FluxType::Timestamp,
        },
        ColumnSchema {
            name: "c_decimal",
            ty: FluxType::Decimal,
        },
        ColumnSchema {
            name: "c_blob",
            ty: FluxType::Blob,
        },
        ColumnSchema {
            name: "c_opt",
            ty: FluxType::Option(&FluxType::I8),
        },
        ColumnSchema {
            name: "c_list",
            ty: FluxType::List(&FluxType::Str),
        },
    ];

    static UNIVERSE: TableSchema = TableSchema {
        name: "Universe",
        columns: UNIVERSE_COLS,
        primary_key: &[8],
        auto_inc: None,
        access: crate::schema::TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: crate::schema::VisibilityRule::PublicAll,
    };

    fn universe_row() -> Vec<RowValue> {
        vec![
            RowValue::Bool(true),
            RowValue::I8(-8),
            RowValue::I16(-16),
            RowValue::I32(-32),
            RowValue::I64(-64),
            RowValue::U8(8),
            RowValue::U16(16),
            RowValue::U32(32),
            RowValue::U64(64),
            RowValue::F32(0.5),
            RowValue::F64(0.25),
            RowValue::Str("s".into()),
            RowValue::Bytes(vec![0xFF]),
            RowValue::Identity(Identity::from_bytes([7u8; 32])),
            RowValue::ConnectionId(ConnectionId::new(11)),
            RowValue::EntityId(EntityId::new(13)),
            RowValue::Timestamp(Timestamp::from_micros(17)),
            RowValue::Decimal(Decimal::from_parts(-12345, 2)),
            RowValue::Blob(BlobRef::from_bytes([0xAB; 32])),
            RowValue::Optional(None),
            RowValue::List(vec![RowValue::Str("a".into()), RowValue::Str("b".into())]),
        ]
    }

    #[test]
    fn every_type_round_trips_through_fluxbin() {
        let row = universe_row();
        check_row(&UNIVERSE, &row).unwrap_or_else(|e| panic!("{e}"));
        let bytes = encode_row(&row).unwrap_or_else(|e| panic!("{e}"));
        let decoded = decode_row(&UNIVERSE, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(decoded.values(), &row[..]);

        // The PK encoding of the u64 column doubles as an all-types witness
        // for the key-values form.
        let pk = encode_pk_of_row(&UNIVERSE, &row).unwrap_or_else(|e| panic!("{e}"));
        let same =
            encode_pk_values(&UNIVERSE, &[RowValue::U64(64)]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(pk, same);
    }

    #[test]
    fn every_type_displays_without_panicking() {
        for value in universe_row() {
            assert!(!value.to_string().is_empty(), "{value:?}");
        }
        // The Some-wrapped optional renders its inner value.
        let some = RowValue::Optional(Some(Box::new(RowValue::I8(-3))));
        assert_eq!(some.to_string(), "-3");
        assert_eq!(RowValue::F32(1.5).to_string(), "1.5");
        assert_eq!(RowValue::I8(-8).to_string(), "-8");
        assert_eq!(RowValue::I16(-16).to_string(), "-16");
        assert_eq!(RowValue::U8(8).to_string(), "8");
    }

    #[test]
    fn out_of_range_pk_ordinals_are_reported() {
        static BROKEN: TableSchema = TableSchema {
            name: "Broken",
            columns: SENSOR_COLS,
            primary_key: &[9],
            auto_inc: None,
            access: crate::schema::TableAccess::Public,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: crate::schema::VisibilityRule::PublicAll,
        };
        let err = match encode_pk_of_row(&BROKEN, &sensor_row()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("ordinal 9 must be out of range"),
        };
        assert!(err.contains("ordinal 9 out of range"), "{err}");
        let err = match encode_pk_values(&BROKEN, &[RowValue::I32(1)]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("ordinal 9 must be out of range"),
        };
        assert!(err.contains("ordinal 9 out of range"), "{err}");
    }

    // --- Rich column types: enum + nested struct (SPEC-023 DMX-030) ----------

    static PRIORITY_VARIANTS: &[VariantSchema] = &[
        VariantSchema {
            name: "Low",
            payload: &[],
        },
        VariantSchema {
            name: "At",
            payload: &[FluxType::Timestamp],
        },
        VariantSchema {
            name: "Ranked",
            payload: &[FluxType::U16, FluxType::Str],
        },
    ];
    static PRIORITY_ENUM: EnumSchema = EnumSchema {
        name: "Priority",
        variants: PRIORITY_VARIANTS,
    };
    static POINT_FIELDS: &[FieldSchema] = &[
        FieldSchema {
            name: "x",
            ty: FluxType::I32,
        },
        FieldSchema {
            name: "y",
            ty: FluxType::I32,
        },
    ];
    static POINT_STRUCT: StructSchema = StructSchema {
        name: "Point",
        fields: POINT_FIELDS,
    };
    static RICH_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "priority",
            ty: FluxType::Enum(&PRIORITY_ENUM),
        },
        ColumnSchema {
            name: "origin",
            ty: FluxType::Struct(&POINT_STRUCT),
        },
    ];
    static RICH: TableSchema = TableSchema {
        name: "Rich",
        columns: RICH_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: crate::schema::TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: crate::schema::VisibilityRule::PublicAll,
    };

    #[test]
    fn rich_types_round_trip_byte_exact_through_fluxbin() {
        // Payload-carrying variant (Ranked) + nested struct.
        let row = vec![
            RowValue::U64(1),
            RowValue::Enum {
                tag: 2,
                payload: vec![RowValue::U16(5), RowValue::Str("hi".into())],
            },
            RowValue::Struct(vec![RowValue::I32(-3), RowValue::I32(4)]),
        ];
        check_row(&RICH, &row).unwrap_or_else(|e| panic!("{e}"));
        let bytes = encode_row(&row).unwrap_or_else(|e| panic!("{e}"));
        // u64 (8) + tag(1) + u16(2) + str(4 len + 2) + i32(4) + i32(4).
        assert_eq!(bytes.len(), 8 + 1 + 2 + 6 + 4 + 4);
        let decoded = decode_row(&RICH, &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(decoded.values(), &row[..]);

        // Unit variant (tag 0) with empty payload also round-trips.
        let unit = vec![
            RowValue::U64(2),
            RowValue::Enum {
                tag: 0,
                payload: vec![],
            },
            RowValue::Struct(vec![RowValue::I32(0), RowValue::I32(0)]),
        ];
        let unit_bytes = encode_row(&unit).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            decode_row(&RICH, &unit_bytes)
                .unwrap_or_else(|e| panic!("{e}"))
                .values(),
            &unit[..]
        );
    }

    #[test]
    fn rich_types_type_checking_and_limits() {
        // Tag out of the declared variant range is rejected.
        let bad_tag = vec![
            RowValue::U64(1),
            RowValue::Enum {
                tag: 9,
                payload: vec![],
            },
            RowValue::Struct(vec![RowValue::I32(0), RowValue::I32(0)]),
        ];
        assert!(check_row(&RICH, &bad_tag).is_err());

        // Wrong payload shape for the variant is rejected.
        let bad_payload = vec![
            RowValue::U64(1),
            RowValue::Enum {
                tag: 1, // At(Timestamp)
                payload: vec![RowValue::U16(0)],
            },
            RowValue::Struct(vec![RowValue::I32(0), RowValue::I32(0)]),
        ];
        assert!(check_row(&RICH, &bad_payload).is_err());

        // Wrong struct arity is rejected.
        let bad_struct = vec![
            RowValue::U64(1),
            RowValue::Enum {
                tag: 0,
                payload: vec![],
            },
            RowValue::Struct(vec![RowValue::I32(0)]),
        ];
        assert!(check_row(&RICH, &bad_struct).is_err());

        // A variant tag exceeding the u8 FluxBIN width fails to encode.
        assert!(
            encode_row(&[RowValue::Enum {
                tag: 300,
                payload: vec![],
            }])
            .is_err()
        );

        // Display never panics and renders both shapes.
        let enum_display = RowValue::Enum {
            tag: 1,
            payload: vec![RowValue::Timestamp(Timestamp::from_micros(5))],
        }
        .to_string();
        assert!(!enum_display.is_empty());
        assert_eq!(
            RowValue::Struct(vec![RowValue::I32(1), RowValue::I32(2)]).to_string(),
            "{1, 2}"
        );
        // A multi-value payload renders comma-separated.
        assert_eq!(
            RowValue::Enum {
                tag: 2,
                payload: vec![RowValue::U16(5), RowValue::Str("hi".into())],
            }
            .to_string(),
            "#2(5, \"hi\")"
        );
    }

    #[test]
    fn a_stored_enum_tag_out_of_the_variant_range_fails_decode() {
        // Encode a valid Rich row, then tamper the stored tag byte (offset
        // 8, right after the u64 id) to a variant that does not exist.
        let row = vec![
            RowValue::U64(1),
            RowValue::Enum {
                tag: 0,
                payload: vec![],
            },
            RowValue::Struct(vec![RowValue::I32(0), RowValue::I32(0)]),
        ];
        let mut bytes = encode_row(&row).unwrap_or_else(|e| panic!("{e}"));
        bytes[8] = 9;
        let err = match decode_row(&RICH, &bytes) {
            Ok(_) => panic!("out-of-range variant tag decoded"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("variant tag 9 out of range (0..3)"), "{err}");
    }
}
