//! [`TxRecord`] — the MessagePack body of one commit-log entry (STG-011) —
//! and [`LogValue`], the self-describing row value representation it stores.
//!
//! The body is encoded with `rmp-serde` (the FluxRPC body codec, SPEC-006),
//! so one codec covers wire encoding, commit log, checkpoint, and the
//! replication stream (STG-016). Structs serialize positionally (rmp-serde
//! default): **field order below is part of the on-disk format and freezes
//! at gate G5.**
//!
//! Rows are stored as [`LogValue`] vectors — self-describing at the decode
//! level, so replay can always skip or inspect a record without schema
//! knowledge (analysis `spacetimedb-code/03` §7). Deletes carry only the
//! FluxBIN-encoded primary key, byte-identical to the store's
//! [`PkBytes`](crate::store::PkBytes)
//! form.

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::error::{FluxumError, Result};
use crate::store::row::{Row, RowValue};
use crate::store::{TableId, TxDiff};
use crate::types::{ConnectionId, Decimal, EntityId, Identity, Timestamp};

/// One committed transaction, as persisted in the commit log (STG-011) and
/// consumed verbatim by the replication stream (STG-016).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxRecord {
    /// Monotonically increasing, per-shard (STG-015).
    pub tx_id: u64,
    /// Microseconds since the Unix epoch.
    pub timestamp: i64,
    /// The shard that committed this transaction.
    pub shard_id: u32,
    /// Per-table row changes; only touched tables appear.
    pub mutations: Vec<TableMutation>,
    /// Auto-inc high-water advances `(table_id, new_high_water)` carried by
    /// this commit (STG-040 batched allocation — the durable write that
    /// makes counters resume without reuse after recovery).
    pub auto_inc: Vec<(u32, u64)>,
}

impl TxRecord {
    /// Build a record from the T2.1 commit output ([`TxDiff`]).
    pub fn from_diff(diff: &TxDiff, shard_id: u32, timestamp: Timestamp) -> Self {
        Self {
            tx_id: diff.tx_id,
            timestamp: timestamp.as_micros(),
            shard_id,
            mutations: diff
                .tables
                .iter()
                .map(|t| TableMutation {
                    table_id: t.table_id.as_u32(),
                    inserts: t.inserts.iter().map(row_to_log).collect(),
                    deletes: t
                        .deletes
                        .iter()
                        .map(|(pk, _)| ByteBuf::from(pk.as_bytes().to_vec()))
                        .collect(),
                })
                .collect(),
            auto_inc: diff
                .auto_inc
                .iter()
                .map(|(table, hw)| (table.as_u32(), *hw))
                .collect(),
        }
    }

    /// MessagePack-encode this record (the STG-011 entry body).
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self)
            .map_err(|e| FluxumError::Storage(format!("commit-log record encoding failed: {e}")))
    }

    /// Decode an entry body. The error string feeds corruption reports.
    pub(crate) fn decode(body: &[u8]) -> std::result::Result<Self, String> {
        rmp_serde::from_slice(body).map_err(|e| format!("MessagePack body decode failed: {e}"))
    }
}

/// Row changes of one table within a [`TxRecord`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMutation {
    /// Stable table id (`crc32(name)`, STG-050) — replayable without a live
    /// schema lookup.
    pub table_id: u32,
    /// Inserted rows: full column values in declaration order.
    pub inserts: Vec<Vec<LogValue>>,
    /// Deleted rows: FluxBIN-encoded primary keys
    /// ([`PkBytes`](crate::store::PkBytes) bytes).
    pub deletes: Vec<ByteBuf>,
}

impl TableMutation {
    /// The typed table id.
    pub fn table(&self) -> TableId {
        TableId::from_raw(self.table_id)
    }

    /// Reconstruct the inserted rows as store [`Row`]s.
    pub fn insert_rows(&self) -> Result<Vec<Row>> {
        self.inserts
            .iter()
            .map(|values| {
                values
                    .iter()
                    .map(LogValue::to_row_value)
                    .collect::<Result<Vec<_>>>()
                    .map(Row::new)
            })
            .collect()
    }

    /// The deleted primary keys as raw FluxBIN bytes.
    pub fn delete_pks(&self) -> impl Iterator<Item = &[u8]> {
        self.deletes.iter().map(|pk| pk.as_slice())
    }
}

/// Convert a store row into its log representation.
pub(crate) fn row_to_log(row: &Row) -> Vec<LogValue> {
    row.values().iter().map(LogValue::from).collect()
}

/// One column value inside a logged row — the closed SPEC-001 type universe
/// (mirrors the store's [`RowValue`]), in a serde-derivable form.
///
/// `Identity` is stored as its raw 32 bytes and `ConnectionId` as 16
/// little-endian bytes (MessagePack has no 128-bit integer). Variant order
/// freezes at G5.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogValue {
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
    Bytes(ByteBuf),
    /// [`Identity`] column — raw 32 bytes.
    Identity(ByteBuf),
    /// [`ConnectionId`] column — 16 little-endian bytes.
    ConnectionId(ByteBuf),
    /// [`EntityId`] column.
    EntityId(u64),
    /// [`Timestamp`] column — microseconds since the Unix epoch.
    Timestamp(i64),
    /// [`Decimal`] column — 16 little-endian bytes (`i128` unscaled) + scale.
    Decimal {
        /// The `i128` coefficient, little-endian (16 bytes).
        unscaled: ByteBuf,
        /// Fractional decimal digits.
        scale: u8,
    },
    /// [`crate::types::BlobRef`] column — 32 raw content-hash bytes.
    Blob(ByteBuf),
    /// `Option<T>` column.
    Opt(Option<Box<LogValue>>),
    /// `Vec<T>` column.
    List(Vec<LogValue>),
    /// A `#[derive(FluxType)]` enum value (SPEC-023 DMX-030): variant tag +
    /// payload.
    Enum {
        /// Variant ordinal (the FluxBIN `u8` tag, widened for storage).
        tag: u32,
        /// Payload values in the variant's declaration order.
        payload: Vec<LogValue>,
    },
    /// A `#[derive(FluxType)]` nested-struct value (SPEC-023 DMX-030): field
    /// values in declaration order.
    Struct(Vec<LogValue>),
}

impl From<&RowValue> for LogValue {
    fn from(value: &RowValue) -> Self {
        match value {
            RowValue::Bool(v) => Self::Bool(*v),
            RowValue::I8(v) => Self::I8(*v),
            RowValue::I16(v) => Self::I16(*v),
            RowValue::I32(v) => Self::I32(*v),
            RowValue::I64(v) => Self::I64(*v),
            RowValue::U8(v) => Self::U8(*v),
            RowValue::U16(v) => Self::U16(*v),
            RowValue::U32(v) => Self::U32(*v),
            RowValue::U64(v) => Self::U64(*v),
            RowValue::F32(v) => Self::F32(*v),
            RowValue::F64(v) => Self::F64(*v),
            RowValue::Str(v) => Self::Str(v.clone()),
            RowValue::Bytes(v) => Self::Bytes(ByteBuf::from(v.clone())),
            RowValue::Identity(v) => Self::Identity(ByteBuf::from(v.as_bytes().to_vec())),
            RowValue::ConnectionId(v) => {
                Self::ConnectionId(ByteBuf::from(v.as_u128().to_le_bytes().to_vec()))
            }
            RowValue::EntityId(v) => Self::EntityId(v.as_u64()),
            RowValue::Timestamp(v) => Self::Timestamp(v.as_micros()),
            RowValue::Decimal(v) => Self::Decimal {
                unscaled: ByteBuf::from(v.unscaled().to_le_bytes().to_vec()),
                scale: v.scale(),
            },
            RowValue::Blob(v) => Self::Blob(ByteBuf::from(v.as_bytes().to_vec())),
            RowValue::Optional(v) => {
                Self::Opt(v.as_ref().map(|inner| Box::new(Self::from(inner.as_ref()))))
            }
            RowValue::List(items) => Self::List(items.iter().map(Self::from).collect()),
            RowValue::Enum { tag, payload } => Self::Enum {
                tag: *tag,
                payload: payload.iter().map(Self::from).collect(),
            },
            RowValue::Struct(fields) => Self::Struct(fields.iter().map(Self::from).collect()),
        }
    }
}

impl LogValue {
    /// Convert back into the store's [`RowValue`], validating fixed-width
    /// byte payloads.
    pub fn to_row_value(&self) -> Result<RowValue> {
        let bad_len = |what: &str, want: usize, got: usize| {
            FluxumError::Storage(format!(
                "commit-log record: {what} payload must be {want} bytes, got {got}"
            ))
        };
        Ok(match self {
            Self::Bool(v) => RowValue::Bool(*v),
            Self::I8(v) => RowValue::I8(*v),
            Self::I16(v) => RowValue::I16(*v),
            Self::I32(v) => RowValue::I32(*v),
            Self::I64(v) => RowValue::I64(*v),
            Self::U8(v) => RowValue::U8(*v),
            Self::U16(v) => RowValue::U16(*v),
            Self::U32(v) => RowValue::U32(*v),
            Self::U64(v) => RowValue::U64(*v),
            Self::F32(v) => RowValue::F32(*v),
            Self::F64(v) => RowValue::F64(*v),
            Self::Str(v) => RowValue::Str(v.clone()),
            Self::Bytes(v) => RowValue::Bytes(v.to_vec()),
            Self::Identity(v) => {
                let bytes: [u8; 32] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| bad_len("Identity", 32, v.len()))?;
                RowValue::Identity(Identity::from_bytes(bytes))
            }
            Self::ConnectionId(v) => {
                let bytes: [u8; 16] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| bad_len("ConnectionId", 16, v.len()))?;
                RowValue::ConnectionId(ConnectionId::new(u128::from_le_bytes(bytes)))
            }
            Self::EntityId(v) => RowValue::EntityId(EntityId::new(*v)),
            Self::Timestamp(v) => RowValue::Timestamp(Timestamp::from_micros(*v)),
            Self::Decimal { unscaled, scale } => {
                let bytes: [u8; 16] = unscaled
                    .as_slice()
                    .try_into()
                    .map_err(|_| bad_len("Decimal", 16, unscaled.len()))?;
                RowValue::Decimal(Decimal::from_parts(i128::from_le_bytes(bytes), *scale))
            }
            Self::Blob(v) => {
                let bytes: [u8; 32] = v
                    .as_slice()
                    .try_into()
                    .map_err(|_| bad_len("Blob", 32, v.len()))?;
                RowValue::Blob(crate::types::BlobRef::from_bytes(bytes))
            }
            Self::Opt(v) => RowValue::Optional(match v {
                None => None,
                Some(inner) => Some(Box::new(inner.to_row_value()?)),
            }),
            Self::List(items) => RowValue::List(
                items
                    .iter()
                    .map(Self::to_row_value)
                    .collect::<Result<Vec<_>>>()?,
            ),
            Self::Enum { tag, payload } => RowValue::Enum {
                tag: *tag,
                payload: payload
                    .iter()
                    .map(Self::to_row_value)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::Struct(fields) => RowValue::Struct(
                fields
                    .iter()
                    .map(Self::to_row_value)
                    .collect::<Result<Vec<_>>>()?,
            ),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn every_row_value() -> Vec<RowValue> {
        vec![
            RowValue::Bool(true),
            RowValue::I8(-8),
            RowValue::I16(-16),
            RowValue::I32(-32),
            RowValue::I64(-64),
            RowValue::U8(8),
            RowValue::U16(16),
            RowValue::U32(32),
            RowValue::U64(u64::MAX),
            RowValue::F32(1.5),
            RowValue::F64(-2.25),
            RowValue::Str("héllo".into()),
            RowValue::Bytes(vec![0, 1, 0xFF]),
            RowValue::Identity(Identity::from_token("t")),
            RowValue::ConnectionId(ConnectionId::new(u128::MAX - 7)),
            RowValue::EntityId(EntityId::new(99)),
            RowValue::Timestamp(Timestamp::from_micros(-5)),
            RowValue::Decimal(Decimal::from_parts(-123_456_789, 4)),
            RowValue::Optional(None),
            RowValue::Optional(Some(Box::new(RowValue::Str("inner".into())))),
            RowValue::List(vec![RowValue::U16(1), RowValue::U16(2)]),
            // Rich types (SPEC-023 DMX-030): enum (payload + unit) and struct.
            RowValue::Enum {
                tag: 3,
                payload: vec![RowValue::U16(7), RowValue::Str("x".into())],
            },
            RowValue::Enum {
                tag: 0,
                payload: vec![],
            },
            RowValue::Struct(vec![RowValue::I32(-1), RowValue::Bool(true)]),
        ]
    }

    #[test]
    fn log_value_roundtrips_every_variant() {
        for value in every_row_value() {
            let log = LogValue::from(&value);
            assert_eq!(log.to_row_value().unwrap(), value, "{value:?}");
        }
    }

    #[test]
    fn record_messagepack_roundtrips() {
        let record = TxRecord {
            tx_id: 7,
            timestamp: 123_456,
            shard_id: 2,
            mutations: vec![TableMutation {
                table_id: 0xDEAD_BEEF,
                inserts: vec![every_row_value().iter().map(LogValue::from).collect()],
                deletes: vec![ByteBuf::from(vec![1, 2, 3])],
            }],
            auto_inc: vec![(0xDEAD_BEEF, 4096)],
        };
        let bytes = record.encode().unwrap();
        let back = TxRecord::decode(&bytes).unwrap();
        assert_eq!(back, record);
        assert!(TxRecord::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn fixed_width_payloads_are_validated() {
        assert!(
            LogValue::Identity(ByteBuf::from(vec![1, 2, 3]))
                .to_row_value()
                .is_err()
        );
        assert!(
            LogValue::ConnectionId(ByteBuf::from(vec![0; 15]))
                .to_row_value()
                .is_err()
        );
    }
}
