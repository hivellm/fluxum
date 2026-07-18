//! Flat row batches — [`RowList`] / [`RowSizeHint`] (SPEC-006 RPC-032).
//!
//! One contiguous buffer plus out-of-band boundaries, **not** `Vec<Vec<u8>>`:
//! one allocation per table update, no per-row MessagePack `bin` header, and
//! zero-copy row slicing on both sides. `rows_data` travels as a single `bin`
//! field of the MessagePack envelope.
//!
//! Encoders emit [`RowSizeHint::Fixed`] whenever the schema yields a
//! statically known row size, may start optimistically from the first row's
//! actual size otherwise, and degrade to [`RowSizeHint::Offsets`] on the
//! first size mismatch — [`RowListBuilder`] implements exactly that. Decoding
//! a `RowList` whose `row_count` / `size_hint` / `rows_data` length are
//! mutually inconsistent fails with a 400 error.

use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::codes;
use crate::tagged::tagged_enum;

/// A `RowList` whose `row_count`, `size_hint`, and `rows_data` length are
/// mutually inconsistent (RPC-032). Maps to wire error code 400.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("inconsistent RowList: {0}")]
pub struct RowListError(String);

impl RowListError {
    /// The RPC-034 wire error code for this failure: 400.
    pub const fn code(&self) -> u16 {
        codes::PROTO_MALFORMED
    }
}

tagged_enum! {
    /// How to slice [`RowList::rows_data`] into rows (RPC-032).
    pub enum RowSizeHint {
        /// Every row encodes to exactly `n` bytes:
        /// row `i` = `rows_data[i*n .. (i+1)*n]`. Zero per-row overhead,
        /// O(1) random access. `row_count` MUST equal `rows_data.len() / n`.
        "Fixed" => Fixed(u16),
        /// Variable-size rows: start offset of each row into `rows_data`;
        /// a row's end is the next row's start (or the end of `rows_data`
        /// for the last row).
        "Offsets" => Offsets(Vec<u64>),
    }
}

/// Flat row list: one contiguous FluxBIN buffer + out-of-band boundaries.
#[derive(Debug, Clone, PartialEq)]
pub struct RowList {
    /// Number of rows in the batch.
    pub row_count: u32,
    /// How to slice `rows_data` into rows.
    pub size_hint: RowSizeHint,
    /// ALL rows' FluxBIN bytes, back-to-back (one `bin` field on the wire).
    pub rows_data: Vec<u8>,
}

impl RowList {
    /// An empty batch (`Fixed(0)`, no bytes).
    pub fn empty() -> Self {
        Self {
            row_count: 0,
            size_hint: RowSizeHint::Fixed(0),
            rows_data: Vec::new(),
        }
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.row_count as usize
    }

    /// True if the batch holds no rows.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Check that `row_count`, `size_hint`, and `rows_data` are mutually
    /// consistent (RPC-032). Deserialization performs this check
    /// automatically; encoders built via [`RowListBuilder`] are consistent by
    /// construction.
    pub fn validate(&self) -> Result<(), RowListError> {
        let count = self.row_count as usize;
        let data_len = self.rows_data.len();
        match &self.size_hint {
            RowSizeHint::Fixed(n) => {
                let n = usize::from(*n);
                if n == 0 {
                    if count != 0 || data_len != 0 {
                        return Err(RowListError(format!(
                            "Fixed(0) requires an empty batch, got row_count={count} with {data_len} data bytes"
                        )));
                    }
                    return Ok(());
                }
                if count.checked_mul(n) != Some(data_len) {
                    return Err(RowListError(format!(
                        "Fixed({n}) with row_count={count} requires {} data bytes, got {data_len}",
                        count.saturating_mul(n)
                    )));
                }
                Ok(())
            }
            RowSizeHint::Offsets(offsets) => {
                if offsets.len() != count {
                    return Err(RowListError(format!(
                        "Offsets holds {} entries but row_count={count}",
                        offsets.len()
                    )));
                }
                if count == 0 && data_len != 0 {
                    return Err(RowListError(format!(
                        "row_count=0 with {data_len} data bytes"
                    )));
                }
                let mut prev = 0u64;
                for (i, &offset) in offsets.iter().enumerate() {
                    if i == 0 && offset != 0 {
                        return Err(RowListError(format!(
                            "first offset must be 0, got {offset}"
                        )));
                    }
                    if offset < prev {
                        return Err(RowListError(format!(
                            "offsets not monotonic: offsets[{i}]={offset} after {prev}"
                        )));
                    }
                    if offset > data_len as u64 {
                        return Err(RowListError(format!(
                            "offsets[{i}]={offset} exceeds rows_data length {data_len}"
                        )));
                    }
                    prev = offset;
                }
                Ok(())
            }
        }
    }

    /// Row `i` as a zero-copy slice of `rows_data`, or `None` when out of
    /// range. Assumes a consistent list (see [`Self::validate`]).
    pub fn row(&self, i: usize) -> Option<&[u8]> {
        if i >= self.len() {
            return None;
        }
        match &self.size_hint {
            RowSizeHint::Fixed(n) => {
                let n = usize::from(*n);
                self.rows_data.get(i * n..(i + 1) * n)
            }
            RowSizeHint::Offsets(offsets) => {
                let start = usize::try_from(*offsets.get(i)?).ok()?;
                let end = match offsets.get(i + 1) {
                    Some(&next) => usize::try_from(next).ok()?,
                    None => self.rows_data.len(),
                };
                self.rows_data.get(start..end)
            }
        }
    }

    /// Iterate the rows as zero-copy slices. Assumes a consistent list.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).filter_map(|i| self.row(i))
    }
}

impl Default for RowList {
    fn default() -> Self {
        Self::empty()
    }
}

impl Serialize for RowList {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("RowList", 3)?;
        s.serialize_field("row_count", &self.row_count)?;
        s.serialize_field("size_hint", &self.size_hint)?;
        s.serialize_field("rows_data", serde_bytes::Bytes::new(&self.rows_data))?;
        s.end()
    }
}

/// Wire shadow of [`RowList`]; decoding goes through [`RowList::validate`]
/// so inconsistent lists are rejected at the codec boundary (RPC-032).
#[derive(Deserialize)]
struct RowListWire {
    row_count: u32,
    size_hint: RowSizeHint,
    #[serde(with = "serde_bytes")]
    rows_data: Vec<u8>,
}

impl<'de> Deserialize<'de> for RowList {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RowListWire::deserialize(deserializer)?;
        let list = Self {
            row_count: wire.row_count,
            size_hint: wire.size_hint,
            rows_data: wire.rows_data,
        };
        list.validate().map_err(D::Error::custom)?;
        Ok(list)
    }
}

enum BuilderMode {
    /// No row pushed yet (and no schema hint).
    Start,
    /// All rows so far are exactly this many bytes.
    Fixed(usize),
    /// Variable row sizes: start offset of every row pushed so far.
    Offsets(Vec<u64>),
}

/// Incremental [`RowList`] encoder implementing the RPC-032 degradation
/// rule: start from a schema-known or first-row `Fixed` size, degrade to
/// `Offsets` on the first mismatch (retroactively synthesizing the offset
/// table). The result is always consistent per [`RowList::validate`].
pub struct RowListBuilder {
    mode: BuilderMode,
    row_count: u32,
    rows_data: Vec<u8>,
}

impl RowListBuilder {
    /// Builder with no size knowledge: the first row's size is taken as the
    /// optimistic `Fixed` hint.
    pub fn new() -> Self {
        Self {
            mode: BuilderMode::Start,
            row_count: 0,
            rows_data: Vec::new(),
        }
    }

    /// Builder for a schema-known static row size: emits `Fixed(n)` even for
    /// an empty batch. (`n = 0` falls back to first-row sizing — zero-size
    /// rows cannot exist, every table has at least one column.)
    pub fn with_fixed_size(n: u16) -> Self {
        Self {
            mode: if n == 0 {
                BuilderMode::Start
            } else {
                BuilderMode::Fixed(usize::from(n))
            },
            row_count: 0,
            rows_data: Vec::new(),
        }
    }

    /// Number of rows pushed so far.
    pub fn len(&self) -> usize {
        self.row_count as usize
    }

    /// True if no row has been pushed.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Append one FluxBIN-encoded row.
    pub fn push_row(&mut self, row: &[u8]) {
        let start = self.rows_data.len() as u64;
        match &mut self.mode {
            BuilderMode::Start => {
                // Rows longer than the Fixed(u16) hint can express go
                // straight to Offsets.
                self.mode = if u16::try_from(row.len()).is_ok() && !row.is_empty() {
                    BuilderMode::Fixed(row.len())
                } else {
                    BuilderMode::Offsets(vec![start])
                };
            }
            BuilderMode::Fixed(n) => {
                if row.len() != *n {
                    // Degrade: synthesize the offsets of the fixed-size rows
                    // already written (RPC-032).
                    let n = *n as u64;
                    let mut offsets: Vec<u64> =
                        (0..u64::from(self.row_count)).map(|i| i * n).collect();
                    offsets.push(start);
                    self.mode = BuilderMode::Offsets(offsets);
                }
            }
            BuilderMode::Offsets(offsets) => offsets.push(start),
        }
        self.rows_data.extend_from_slice(row);
        self.row_count += 1;
    }

    /// Finish the batch. The returned list is consistent by construction.
    pub fn finish(self) -> RowList {
        let size_hint = match self.mode {
            BuilderMode::Start => RowSizeHint::Fixed(0),
            // Cast is exact: Fixed mode is only entered for sizes that fit u16.
            BuilderMode::Fixed(n) => RowSizeHint::Fixed(u16::try_from(n).unwrap_or(u16::MAX)),
            BuilderMode::Offsets(offsets) => RowSizeHint::Offsets(offsets),
        };
        RowList {
            row_count: self.row_count,
            size_hint,
            rows_data: self.rows_data,
        }
    }
}

impl Default for RowListBuilder {
    fn default() -> Self {
        Self::new()
    }
}
