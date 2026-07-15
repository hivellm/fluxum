//! [`BTreeIndex`] — secondary B-tree index (DM-030/DM-031) and the
//! memcomparable key transform it is built on.
//!
//! # Memcomparable encoding
//!
//! An index key is the concatenation of each indexed column value encoded so
//! that **byte-wise `memcmp` order equals the natural value order** (the
//! transform T2.1 deferred to this task — FluxBIN's little-endian integers
//! are *not* order-preserving):
//!
//! | Type | Transform |
//! |---|---|
//! | `bool` | `0x00` / `0x01` |
//! | unsigned ints | big-endian bytes |
//! | signed ints, `Timestamp` | sign bit flipped, then big-endian |
//! | `f32` / `f64` | IEEE-754 totalOrder: flip all bits if negative, else flip the sign bit |
//! | `String` / `Vec<u8>` | content with `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` |
//! | `Identity` | raw 32 bytes |
//! | `ConnectionId` | big-endian `u128` |
//! | `EntityId` | big-endian `u64` |
//! | `Option<T>` | `0x00` for `None`; `0x01` + inner encoding for `Some` |
//! | `Vec<T>` | `0x01` + element encoding per element, terminated by `0x00` |
//!
//! Two properties carry the whole design:
//!
//! 1. **Order-preserving**: `enc(a) < enc(b)` (bytes) ⇔ `a < b` (values).
//!    Floats follow IEEE totalOrder, so `-NaN < -∞ … -0.0 < +0.0 … +∞ <
//!    +NaN` — every value, including NaN, has one deterministic place.
//! 2. **Prefix-free per column**: no encoding is a proper prefix of another
//!    encoding of the same column type (fixed width, or terminator-based
//!    for variable-length values). Concatenation therefore preserves
//!    tuple ordering, which is exactly what composite-index prefix scans
//!    (DM-031) rely on: all keys sharing an equality prefix form one
//!    contiguous byte range.
//!
//! Range planning ([`plan_scan`]) reduces every scan shape — point lookup,
//! open/closed/half-open range, prefix scan — to a single contiguous
//! `[start, end)` byte range over the index map.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::error::{FluxumError, Result};
use crate::store::row::{PkBytes, Row, RowValue};

/// One secondary B-tree index: memcomparable key bytes → the PKs of the
/// rows carrying that key (non-unique: one key maps to N rows).
///
/// Lives inside the committed `TableState` and is copy-on-write together
/// with the row map, so a snapshot's rows and indexes are always mutually
/// consistent (see the module docs of [`super`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BTreeIndex {
    /// Indexed column ordinals in declared key order (DM-030/DM-031).
    columns: &'static [u16],
    /// Memcomparable key → PKs of the rows with that key. PKs within one
    /// key are in encoded-PK byte order (deterministic, not numeric).
    map: BTreeMap<Vec<u8>, BTreeSet<PkBytes>>,
}

impl BTreeIndex {
    /// An empty index over `columns` (ordinals into the table's schema).
    pub(crate) fn new(columns: &'static [u16]) -> Self {
        Self {
            columns,
            map: BTreeMap::new(),
        }
    }

    /// The indexed column ordinals in declared key order.
    pub fn columns(&self) -> &'static [u16] {
        self.columns
    }

    /// The memcomparable index key of `row`.
    fn key_of(&self, row: &Row) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        for &ordinal in self.columns {
            let value = row.value(ordinal).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: index ordinal {ordinal} out of range for a \
                     row of {} columns",
                    row.values().len()
                ))
            })?;
            encode_value(value, &mut key);
        }
        Ok(key)
    }

    /// Add `row`'s index entry (commit merge, insert side).
    pub(crate) fn insert(&mut self, row: &Row, pk: PkBytes) -> Result<()> {
        let key = self.key_of(row)?;
        self.map.entry(key).or_default().insert(pk);
        Ok(())
    }

    /// Remove `row`'s index entry (commit merge, delete side). Empty key
    /// slots are dropped so the map stays bit-identical to a fresh rebuild.
    pub(crate) fn remove(&mut self, row: &Row, pk: &PkBytes) -> Result<()> {
        let key = self.key_of(row)?;
        if let Some(pks) = self.map.get_mut(&key) {
            pks.remove(pk);
            if pks.is_empty() {
                self.map.remove(&key);
            }
        }
        Ok(())
    }

    /// Iterate the PKs of every entry in `[start, end)` (`end: None` =
    /// unbounded), in index-key order then encoded-PK order within one key.
    pub(crate) fn scan_pks(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> impl Iterator<Item = &PkBytes> {
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        self.map
            .range((Bound::Included(start), upper))
            .flat_map(|(_, pks)| pks.iter())
    }
}

/// Memcomparable-encode one column value onto `out` (see the module docs
/// for the per-type transform and its two load-bearing properties).
pub(crate) fn encode_value(value: &RowValue, out: &mut Vec<u8>) {
    const SIGN16: u16 = 1 << 15;
    const SIGN32: u32 = 1 << 31;
    const SIGN64: u64 = 1 << 63;
    match value {
        RowValue::Bool(v) => out.push(u8::from(*v)),
        RowValue::I8(v) => out.push(v.cast_unsigned() ^ 0x80),
        RowValue::I16(v) => out.extend_from_slice(&(v.cast_unsigned() ^ SIGN16).to_be_bytes()),
        RowValue::I32(v) => out.extend_from_slice(&(v.cast_unsigned() ^ SIGN32).to_be_bytes()),
        RowValue::I64(v) => out.extend_from_slice(&(v.cast_unsigned() ^ SIGN64).to_be_bytes()),
        RowValue::U8(v) => out.push(*v),
        RowValue::U16(v) => out.extend_from_slice(&v.to_be_bytes()),
        RowValue::U32(v) => out.extend_from_slice(&v.to_be_bytes()),
        RowValue::U64(v) => out.extend_from_slice(&v.to_be_bytes()),
        RowValue::F32(v) => {
            let bits = v.to_bits();
            let ordered = if bits & SIGN32 != 0 {
                !bits
            } else {
                bits ^ SIGN32
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        RowValue::F64(v) => {
            let bits = v.to_bits();
            let ordered = if bits & SIGN64 != 0 {
                !bits
            } else {
                bits ^ SIGN64
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        RowValue::Str(v) => encode_terminated(v.as_bytes(), out),
        RowValue::Bytes(v) => encode_terminated(v, out),
        RowValue::Identity(v) => out.extend_from_slice(v.as_bytes()),
        RowValue::ConnectionId(v) => out.extend_from_slice(&v.as_u128().to_be_bytes()),
        RowValue::EntityId(v) => out.extend_from_slice(&v.as_u64().to_be_bytes()),
        RowValue::Timestamp(v) => {
            out.extend_from_slice(&(v.as_micros().cast_unsigned() ^ SIGN64).to_be_bytes());
        }
        // Decimal is not yet a valid B-tree index key (rejected at macro
        // expansion, SPEC-017 CT-020): a numerically order-preserving
        // memcomparable encoding across mixed scales is deferred. This arm is
        // therefore unreachable; it emits a deterministic, fixed-width form
        // (sign-flipped `i128` big-endian + scale) purely for totality.
        RowValue::Decimal(v) => {
            const SIGN128: u128 = 1 << 127;
            out.extend_from_slice(&(v.unscaled().cast_unsigned() ^ SIGN128).to_be_bytes());
            out.push(v.scale());
        }
        RowValue::Optional(None) => out.push(0x00),
        RowValue::Optional(Some(inner)) => {
            out.push(0x01);
            encode_value(inner, out);
        }
        RowValue::List(items) => {
            for item in items {
                out.push(0x01);
                encode_value(item, out);
            }
            out.push(0x00);
        }
    }
}

/// Variable-length content, order-preserving and prefix-free: `0x00` in the
/// content is escaped as `0x00 0xFF`; the terminator is `0x00 0x00`.
fn encode_terminated(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0x00 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// The least byte string strictly greater than *every* string with prefix
/// `bytes` (increment the last non-`0xFF` byte, truncating the tail).
/// `None` when no such string exists (empty or all-`0xFF` prefix).
fn prefix_successor(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    while bytes.last() == Some(&0xFF) {
        bytes.pop();
    }
    match bytes.last_mut() {
        Some(last) => {
            *last += 1;
            Some(bytes)
        }
        None => None,
    }
}

/// Reduce a scan shape — equality on `prefix` (already encoded), then
/// `lower`/`upper` bounds over the *next* column (encoded bound values) —
/// to one contiguous `[start, end)` byte range (`end: None` = unbounded).
///
/// Correct because the per-column encoding is order-preserving and
/// prefix-free (module docs): all keys extending `prefix` are contiguous,
/// and all keys extending `prefix + enc(v)` are contiguous within them.
/// A provably empty scan (e.g. inverted bounds) yields the canonical empty
/// range `[[], [])`.
pub(crate) fn plan_scan(
    prefix: Vec<u8>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
) -> (Vec<u8>, Option<Vec<u8>>) {
    let empty = || (Vec::new(), Some(Vec::new()));
    let concat = |value: &[u8]| {
        let mut key = Vec::with_capacity(prefix.len() + value.len());
        key.extend_from_slice(&prefix);
        key.extend_from_slice(value);
        key
    };
    // Keys with the bound value extend `prefix + enc(v)`, so:
    //   ≥ v  →  start at prefix+enc(v);      > v  →  start past that prefix;
    //   ≤ v  →  end   past prefix+enc(v);    < v  →  end   at prefix+enc(v).
    let start = match &lower {
        Bound::Unbounded => Some(prefix.clone()),
        Bound::Included(v) => Some(concat(v)),
        Bound::Excluded(v) => prefix_successor(concat(v)),
    };
    // `None` from `prefix_successor` here means "no byte string bounds the
    // range above" — i.e. unbounded, not empty.
    let end = match &upper {
        Bound::Unbounded => prefix_successor(prefix.clone()),
        Bound::Included(v) => prefix_successor(concat(v)),
        Bound::Excluded(v) => Some(concat(v)),
    };
    let Some(start) = start else {
        return empty(); // lower bound is above every representable key
    };
    if let Some(end_bytes) = &end
        && *end_bytes <= start
    {
        return empty(); // inverted or degenerate bounds
    }
    (start, end)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use proptest::prelude::*;

    use super::*;
    use crate::types::Timestamp;

    fn enc(value: &RowValue) -> Vec<u8> {
        let mut out = Vec::new();
        encode_value(value, &mut out);
        out
    }

    /// Assert the encodings of `values` are strictly increasing byte-wise —
    /// `values` must be listed in ascending natural order.
    fn assert_strictly_increasing(values: &[RowValue]) {
        for pair in values.windows(2) {
            let (a, b) = (enc(&pair[0]), enc(&pair[1]));
            assert!(
                a < b,
                "enc({}) >= enc({}): {a:?} vs {b:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn signed_ints_order_numerically() {
        assert_strictly_increasing(&[
            RowValue::I64(i64::MIN),
            RowValue::I64(-2),
            RowValue::I64(-1),
            RowValue::I64(0),
            RowValue::I64(1),
            RowValue::I64(i64::MAX),
        ]);
        assert_strictly_increasing(&[
            RowValue::I32(i32::MIN),
            RowValue::I32(-1),
            RowValue::I32(0),
            RowValue::I32(i32::MAX),
        ]);
        assert_strictly_increasing(&[
            RowValue::I16(i16::MIN),
            RowValue::I16(-1),
            RowValue::I16(0),
            RowValue::I16(i16::MAX),
        ]);
        assert_strictly_increasing(&[
            RowValue::I8(i8::MIN),
            RowValue::I8(-1),
            RowValue::I8(0),
            RowValue::I8(i8::MAX),
        ]);
        assert_strictly_increasing(&[
            RowValue::Timestamp(Timestamp::from_micros(-5)),
            RowValue::Timestamp(Timestamp::from_micros(0)),
            RowValue::Timestamp(Timestamp::from_micros(5)),
        ]);
    }

    #[test]
    fn unsigned_ints_and_bool_order_numerically() {
        assert_strictly_increasing(&[
            RowValue::U64(0),
            RowValue::U64(1),
            RowValue::U64(256),
            RowValue::U64(u64::MAX),
        ]);
        assert_strictly_increasing(&[RowValue::U8(0), RowValue::U8(255)]);
        assert_strictly_increasing(&[RowValue::Bool(false), RowValue::Bool(true)]);
    }

    #[test]
    fn floats_follow_ieee_total_order() {
        assert_strictly_increasing(&[
            RowValue::F64(f64::NEG_INFINITY),
            RowValue::F64(-1.5),
            RowValue::F64(-0.0),
            RowValue::F64(0.0),
            RowValue::F64(1.5),
            RowValue::F64(f64::INFINITY),
            RowValue::F64(f64::NAN), // positive NaN sorts above +∞
        ]);
        assert_strictly_increasing(&[
            RowValue::F32(f32::NEG_INFINITY),
            RowValue::F32(-0.0),
            RowValue::F32(0.0),
            RowValue::F32(f32::INFINITY),
        ]);
    }

    #[test]
    fn strings_order_bytewise_including_embedded_nul() {
        assert_strictly_increasing(&[
            RowValue::Str("".into()),
            RowValue::Str("a".into()),
            RowValue::Str("a\0".into()),
            RowValue::Str("a\0b".into()),
            RowValue::Str("a\u{1}".into()),
            RowValue::Str("aa".into()),
            RowValue::Str("ab".into()),
            RowValue::Str("b".into()),
        ]);
        assert_strictly_increasing(&[
            RowValue::Bytes(vec![]),
            RowValue::Bytes(vec![0x00]),
            RowValue::Bytes(vec![0x00, 0x00]),
            RowValue::Bytes(vec![0x01]),
            RowValue::Bytes(vec![0xFF]),
        ]);
    }

    #[test]
    fn option_and_list_order_structurally() {
        assert_strictly_increasing(&[
            RowValue::Optional(None),
            RowValue::Optional(Some(Box::new(RowValue::I32(i32::MIN)))),
            RowValue::Optional(Some(Box::new(RowValue::I32(7)))),
        ]);
        assert_strictly_increasing(&[
            RowValue::List(vec![]),
            RowValue::List(vec![RowValue::U16(1)]),
            RowValue::List(vec![RowValue::U16(1), RowValue::U16(2)]),
            RowValue::List(vec![RowValue::U16(2)]),
        ]);
    }

    #[test]
    fn prefix_successor_increments_and_truncates() {
        assert_eq!(prefix_successor(vec![1, 2]), Some(vec![1, 3]));
        assert_eq!(prefix_successor(vec![1, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_successor(vec![1, 0xFF, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_successor(vec![0xFF, 0xFF]), None);
        assert_eq!(prefix_successor(vec![]), None);
    }

    #[test]
    fn plan_scan_degenerate_bounds_are_the_canonical_empty_range() {
        // Inverted bounds.
        let (start, end) = plan_scan(
            vec![9],
            Bound::Included(enc(&RowValue::I64(7))),
            Bound::Included(enc(&RowValue::I64(3))),
        );
        assert_eq!((start, end), (vec![], Some(vec![])));
        // (v, v) exclusive on both sides.
        let (start, end) = plan_scan(
            vec![],
            Bound::Excluded(enc(&RowValue::I64(7))),
            Bound::Excluded(enc(&RowValue::I64(7))),
        );
        assert_eq!((start, end), (vec![], Some(vec![])));
    }

    proptest! {
        /// Order preservation: byte order of encodings == natural order.
        #[test]
        fn i64_encoding_preserves_order(a: i64, b: i64) {
            let (ea, eb) = (enc(&RowValue::I64(a)), enc(&RowValue::I64(b)));
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        #[test]
        fn u64_encoding_preserves_order(a: u64, b: u64) {
            let (ea, eb) = (enc(&RowValue::U64(a)), enc(&RowValue::U64(b)));
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        #[test]
        fn string_encoding_preserves_order(a: String, b: String) {
            let (ea, eb) = (enc(&RowValue::Str(a.clone())), enc(&RowValue::Str(b.clone())));
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        #[test]
        fn bytes_encoding_preserves_order(a: Vec<u8>, b: Vec<u8>) {
            let (ea, eb) = (enc(&RowValue::Bytes(a.clone())), enc(&RowValue::Bytes(b.clone())));
            prop_assert_eq!(a.cmp(&b), ea.cmp(&eb));
        }

        /// Composite keys: concatenated encodings order like tuples — the
        /// prefix-free property that composite prefix scans rely on
        /// (a variable-length first column must not bleed into the second).
        #[test]
        fn concatenation_orders_like_tuples(
            a: (String, i64),
            b: (String, i64),
        ) {
            let key = |t: &(String, i64)| {
                let mut k = enc(&RowValue::Str(t.0.clone()));
                k.extend_from_slice(&enc(&RowValue::I64(t.1)));
                k
            };
            prop_assert_eq!(a.cmp(&b), key(&a).cmp(&key(&b)));
        }
    }
}
