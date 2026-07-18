//! [`FluxValue`] — the dynamic value type for reducer arguments and return
//! values (SPEC-006 RPC-010..RPC-012).
//!
//! # MessagePack encoding (RPC-011)
//!
//! | Variant | MessagePack encoding |
//! |---|---|
//! | `Null` | `nil` |
//! | `Bool(b)` | `bool` |
//! | `I64(n)` | `int` (compact) |
//! | `F64(f)` | `float 64` |
//! | `Bytes(b)` | `bin` (length-prefixed) |
//! | `Str(s)` | `str` |
//! | `Array(v)` | `array` of encoded FluxValues |
//! | `Map(kv)` | `map` of encoded key → value entries |
//! | `Identity(b)` | `fixarray[2]` of `["Identity", bin32]` |
//! | `EntityId(n)` | `fixarray[2]` of `["EntityId", uint]` |
//! | `Timestamp(t)` | `fixarray[2]` of `["Timestamp", int]` |
//!
//! Deviation note (flagged for a SPEC-006 amendment): RPC-011 words the `Map`
//! row as an "`array` of `[key, value]` pairs", but that encoding is
//! indistinguishable from `Array` of 2-element arrays on decode, which breaks
//! the round-trip acceptance criterion. `Map` therefore uses the native
//! MessagePack `map` format, which is unambiguous and round-trips.
//!
//! # Canonical tagged forms
//!
//! The three tagged variants are canonical: an `Array` whose encoding happens
//! to coincide byte-for-byte with a tagged form (e.g.
//! `Array([Str("EntityId"), I64(5)])`) decodes as the tagged variant
//! (`EntityId(5)`). Reducer argument values are schema-checked above this
//! layer, so the collision is harmless in practice.

use std::fmt;

use serde::de::{Deserializer, Error as _, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq, SerializeTuple, Serializer};
use serde::{Deserialize, Serialize};

/// Dynamic value for reducer arguments and return values (RPC-010).
#[derive(Debug, Clone, PartialEq)]
pub enum FluxValue {
    /// Absent value — MessagePack `nil`.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer (compact MessagePack `int` on the wire).
    I64(i64),
    /// IEEE 754 double (`float 64` on the wire).
    F64(f64),
    /// Raw bytes (`bin` on the wire).
    Bytes(Vec<u8>),
    /// UTF-8 string.
    Str(String),
    /// Ordered sequence of values.
    Array(Vec<FluxValue>),
    /// Ordered key → value entries (duplicates preserved on the wire).
    Map(Vec<(FluxValue, FluxValue)>),
    /// Stable 256-bit client identity (SPEC-009).
    Identity([u8; 32]),
    /// Row/entity primary key.
    EntityId(u64),
    /// Microseconds since the Unix epoch.
    Timestamp(i64),
}

const TAG_IDENTITY: &str = "Identity";
const TAG_ENTITY_ID: &str = "EntityId";
const TAG_TIMESTAMP: &str = "Timestamp";

impl Serialize for FluxValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::I64(n) => serializer.serialize_i64(*n),
            Self::F64(f) => serializer.serialize_f64(*f),
            Self::Bytes(b) => serializer.serialize_bytes(b),
            Self::Str(s) => serializer.serialize_str(s),
            Self::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(value)?;
                }
                seq.end()
            }
            Self::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Self::Identity(bytes) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(TAG_IDENTITY)?;
                tuple.serialize_element(serde_bytes::Bytes::new(bytes))?;
                tuple.end()
            }
            Self::EntityId(n) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(TAG_ENTITY_ID)?;
                tuple.serialize_element(n)?;
                tuple.end()
            }
            Self::Timestamp(t) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(TAG_TIMESTAMP)?;
                tuple.serialize_element(t)?;
                tuple.end()
            }
        }
    }
}

/// Decoding intermediate: a `FluxValue`, or an unsigned integer above
/// `i64::MAX` — legal on the wire only as an `EntityId` payload.
enum Element {
    Value(FluxValue),
    BigU64(u64),
}

impl Element {
    fn into_value(self) -> Result<FluxValue, &'static str> {
        match self {
            Self::Value(v) => Ok(v),
            Self::BigU64(_) => Err("unsigned integer above i64::MAX outside an EntityId payload"),
        }
    }
}

/// Resolve a decoded MessagePack array: either one of the tagged variants of
/// RPC-011 (`["Identity", bin32]`, `["EntityId", uint]`, `["Timestamp", int]`)
/// or a plain `Array`.
fn resolve_seq(mut items: Vec<Element>) -> Result<FluxValue, &'static str> {
    if items.len() == 2
        && let Element::Value(FluxValue::Str(tag)) = &items[0]
    {
        match (tag.as_str(), &items[1]) {
            (TAG_IDENTITY, Element::Value(FluxValue::Bytes(b))) if b.len() == 32 => {
                let mut identity = [0u8; 32];
                identity.copy_from_slice(b);
                return Ok(FluxValue::Identity(identity));
            }
            (TAG_ENTITY_ID, Element::Value(FluxValue::I64(n))) if *n >= 0 => {
                #[allow(clippy::cast_sign_loss)] // guarded by `*n >= 0`
                return Ok(FluxValue::EntityId(*n as u64));
            }
            (TAG_ENTITY_ID, Element::BigU64(n)) => return Ok(FluxValue::EntityId(*n)),
            (TAG_TIMESTAMP, Element::Value(FluxValue::I64(t))) => {
                return Ok(FluxValue::Timestamp(*t));
            }
            _ => {}
        }
    }
    let mut values = Vec::with_capacity(items.len());
    for item in items.drain(..) {
        values.push(item.into_value()?);
    }
    Ok(FluxValue::Array(values))
}

struct ElementVisitor;

impl<'de> Visitor<'de> for ElementVisitor {
    type Value = Element;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a FluxValue (nil, bool, int, float64, bin, str, array, or map)")
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Null))
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Bool(v)))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::I64(v)))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
        match i64::try_from(v) {
            Ok(n) => Ok(Element::Value(FluxValue::I64(n))),
            Err(_) => Ok(Element::BigU64(v)),
        }
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::F64(v)))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Str(v.to_owned())))
    }

    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Str(v)))
    }

    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Bytes(v.to_vec())))
    }

    fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(Element::Value(FluxValue::Bytes(v)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(64));
        while let Some(item) = seq.next_element::<Element>()? {
            items.push(item);
        }
        resolve_seq(items)
            .map(Element::Value)
            .map_err(A::Error::custom)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0).min(64));
        while let Some((key, value)) = map.next_entry::<Element, Element>()? {
            entries.push((
                key.into_value().map_err(A::Error::custom)?,
                value.into_value().map_err(A::Error::custom)?,
            ));
        }
        Ok(Element::Value(FluxValue::Map(entries)))
    }
}

impl<'de> Deserialize<'de> for Element {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(ElementVisitor)
    }
}

impl<'de> Deserialize<'de> for FluxValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Element::deserialize(deserializer)?
            .into_value()
            .map_err(D::Error::custom)
    }
}
