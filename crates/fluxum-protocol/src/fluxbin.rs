//! FluxBIN — the schema-driven binary row encoding (SPEC-006 RPC-040..RPC-042).
//!
//! No field names, no per-value type tags: the table schema (known to both
//! sides) provides all type context, so a row is just its field values
//! encoded back-to-back in column declaration order. All integers are
//! little-endian.
//!
//! | Type | Encoding |
//! |---|---|
//! | `bool` | 1 byte: `0x00` false / `0x01` true |
//! | `u8` / `i8` | 1 byte |
//! | `u16` / `i16` | 2 bytes LE |
//! | `u32` / `i32` | 4 bytes LE |
//! | `u64` / `i64` | 8 bytes LE |
//! | `f32` | 4 bytes IEEE 754 LE |
//! | `f64` | 8 bytes IEEE 754 LE |
//! | `String` | `u32` LE length + UTF-8 bytes |
//! | `Vec<u8>` | `u32` LE length + raw bytes |
//! | `Vec<T>` | `u32` LE count + N × encode(T) |
//! | `Option<T>` | `0x00` (None) / `0x01` + encode(T) |
//! | `Identity` | 32 bytes raw (no prefix) |
//! | `ConnectionId` | 16 bytes raw (`u128` LE, no prefix) |
//! | `EntityId` | 8 bytes LE (`u64` newtype) |
//! | `Timestamp` | 8 bytes LE (`i64` µs since Unix epoch) |
//! | struct | fields in declaration order, no separators, no names |
//! | enum | `u8` tag + encode(variant payload) |
//!
//! The codec is hand-rolled (PRD OQ-3, resolved: hand-rolled first — serde is
//! deliberately absent from this path). A `#[derive(FluxBin)]` proc macro in
//! `fluxum-macros`, generating `write_*`/`read_*` call sequences from struct
//! definitions, is the noted follow-up once the table schema macro (T1.1)
//! settles the attribute surface.

use crate::codes;

/// Errors produced while reading (or, for length overflow, writing) FluxBIN
/// data. All decode failures map to wire error code 400 (RPC-034).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FluxBinError {
    /// The input ended before the value was complete.
    #[error("unexpected end of FluxBIN input: needed {needed} bytes, {remaining} remaining")]
    UnexpectedEof {
        /// Bytes the current value still required.
        needed: usize,
        /// Bytes left in the input.
        remaining: usize,
    },
    /// A `bool` byte was neither `0x00` nor `0x01`.
    #[error("invalid FluxBIN bool byte 0x{0:02x}")]
    InvalidBool(u8),
    /// An `Option` tag byte was neither `0x00` nor `0x01`.
    #[error("invalid FluxBIN option tag 0x{0:02x}")]
    InvalidOptionTag(u8),
    /// A `String` payload was not valid UTF-8.
    #[error("FluxBIN string is not valid UTF-8")]
    InvalidUtf8,
    /// A length did not fit in the `u32` prefix.
    #[error("FluxBIN length {0} exceeds the u32 length prefix")]
    LengthOverflow(usize),
    /// Input bytes remained after the value was fully decoded.
    #[error("{0} trailing bytes after FluxBIN value")]
    TrailingBytes(usize),
}

impl FluxBinError {
    /// The RPC-034 wire error code for this failure: 400 (malformed body).
    pub const fn code(&self) -> u16 {
        codes::MALFORMED
    }
}

/// Append-only FluxBIN encoder over a growable byte buffer.
///
/// Struct fields are written by calling the `write_*` methods in column
/// declaration order; there are no separators, so the writer itself is the
/// whole "struct" rule of RPC-040.
#[derive(Debug, Default, Clone)]
pub struct FluxBinWriter {
    buf: Vec<u8>,
}

impl FluxBinWriter {
    /// New empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// New writer with pre-allocated capacity (e.g. a schema-known row size).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// True if nothing has been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The encoded bytes so far.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Consume the writer, yielding the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// `bool` → 1 byte (`0x00` / `0x01`).
    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(u8::from(v));
    }

    /// `u8` → 1 byte.
    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// `i8` → 1 byte.
    pub fn write_i8(&mut self, v: i8) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `u16` → 2 bytes LE.
    pub fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `i16` → 2 bytes LE.
    pub fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `u32` → 4 bytes LE.
    pub fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `i32` → 4 bytes LE.
    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `u64` → 8 bytes LE.
    pub fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `i64` → 8 bytes LE.
    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `f32` → 4 bytes IEEE 754 LE.
    pub fn write_f32(&mut self, v: f32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `f64` → 8 bytes IEEE 754 LE.
    pub fn write_f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `String` → `u32` LE length + UTF-8 bytes.
    pub fn write_str(&mut self, v: &str) -> Result<(), FluxBinError> {
        self.write_len_prefixed(v.as_bytes())
    }

    /// `Vec<u8>` → `u32` LE length + raw bytes.
    pub fn write_bytes(&mut self, v: &[u8]) -> Result<(), FluxBinError> {
        self.write_len_prefixed(v)
    }

    /// `Vec<T>` count header → `u32` LE. The caller then writes each element.
    pub fn write_seq_len(&mut self, count: u32) {
        self.write_u32(count);
    }

    /// `Option<T>` tag → `0x00` (None) / `0x01` (Some). For `Some`, the caller
    /// then writes the payload.
    pub fn write_option_tag(&mut self, is_some: bool) {
        self.buf.push(u8::from(is_some));
    }

    /// `Identity` → 32 raw bytes, no prefix.
    pub fn write_identity(&mut self, v: &[u8; 32]) {
        self.buf.extend_from_slice(v);
    }

    /// `ConnectionId` → 16 raw bytes (the `u128` in LE), no prefix.
    pub fn write_connection_id(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// `EntityId` → 8 bytes LE (`u64` newtype).
    pub fn write_entity_id(&mut self, v: u64) {
        self.write_u64(v);
    }

    /// `Timestamp` → 8 bytes LE (`i64` µs since Unix epoch).
    pub fn write_timestamp(&mut self, v: i64) {
        self.write_i64(v);
    }

    /// enum tag → `u8`. The caller then writes the variant payload.
    pub fn write_enum_tag(&mut self, tag: u8) {
        self.buf.push(tag);
    }

    /// Escape hatch: splice pre-encoded FluxBIN bytes (e.g. a nested struct
    /// encoded elsewhere) into the stream verbatim.
    pub fn write_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn write_len_prefixed(&mut self, bytes: &[u8]) -> Result<(), FluxBinError> {
        let len =
            u32::try_from(bytes.len()).map_err(|_| FluxBinError::LengthOverflow(bytes.len()))?;
        self.write_u32(len);
        self.buf.extend_from_slice(bytes);
        Ok(())
    }
}

/// Zero-copy FluxBIN decoder over a byte slice.
///
/// Mirrors [`FluxBinWriter`] method-for-method; `read_str` / `read_bytes`
/// borrow from the input rather than allocating.
#[derive(Debug, Clone)]
pub struct FluxBinReader<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> FluxBinReader<'a> {
    /// New reader over `input`, positioned at the start.
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.input.len() - self.pos
    }

    /// True if every input byte has been consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Current read offset into the input.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Error with [`FluxBinError::TrailingBytes`] unless the input is fully
    /// consumed — call after decoding a complete row.
    pub fn expect_eof(&self) -> Result<(), FluxBinError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(FluxBinError::TrailingBytes(self.remaining()))
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], FluxBinError> {
        let remaining = self.remaining();
        if remaining < n {
            return Err(FluxBinError::UnexpectedEof {
                needed: n,
                remaining,
            });
        }
        let slice = &self.input[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], FluxBinError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// `bool` — rejects any byte other than `0x00` / `0x01`.
    pub fn read_bool(&mut self) -> Result<bool, FluxBinError> {
        match self.read_u8()? {
            0x00 => Ok(false),
            0x01 => Ok(true),
            other => Err(FluxBinError::InvalidBool(other)),
        }
    }

    /// `u8`.
    pub fn read_u8(&mut self) -> Result<u8, FluxBinError> {
        Ok(self.take(1)?[0])
    }

    /// `i8`.
    pub fn read_i8(&mut self) -> Result<i8, FluxBinError> {
        Ok(i8::from_le_bytes(self.take_array()?))
    }

    /// `u16` LE.
    pub fn read_u16(&mut self) -> Result<u16, FluxBinError> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    /// `i16` LE.
    pub fn read_i16(&mut self) -> Result<i16, FluxBinError> {
        Ok(i16::from_le_bytes(self.take_array()?))
    }

    /// `u32` LE.
    pub fn read_u32(&mut self) -> Result<u32, FluxBinError> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    /// `i32` LE.
    pub fn read_i32(&mut self) -> Result<i32, FluxBinError> {
        Ok(i32::from_le_bytes(self.take_array()?))
    }

    /// `u64` LE.
    pub fn read_u64(&mut self) -> Result<u64, FluxBinError> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    /// `i64` LE.
    pub fn read_i64(&mut self) -> Result<i64, FluxBinError> {
        Ok(i64::from_le_bytes(self.take_array()?))
    }

    /// `f32` IEEE 754 LE.
    pub fn read_f32(&mut self) -> Result<f32, FluxBinError> {
        Ok(f32::from_le_bytes(self.take_array()?))
    }

    /// `f64` IEEE 754 LE.
    pub fn read_f64(&mut self) -> Result<f64, FluxBinError> {
        Ok(f64::from_le_bytes(self.take_array()?))
    }

    /// `String` — `u32` LE length + UTF-8 bytes, borrowed from the input.
    ///
    /// The length is bounds-checked against the remaining input before any
    /// slicing, so a hostile length prefix cannot cause huge allocations.
    pub fn read_str(&mut self) -> Result<&'a str, FluxBinError> {
        let bytes = self.read_bytes()?;
        std::str::from_utf8(bytes).map_err(|_| FluxBinError::InvalidUtf8)
    }

    /// `Vec<u8>` — `u32` LE length + raw bytes, borrowed from the input.
    pub fn read_bytes(&mut self) -> Result<&'a [u8], FluxBinError> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    /// `Vec<T>` count header — the caller then reads each element.
    pub fn read_seq_len(&mut self) -> Result<u32, FluxBinError> {
        self.read_u32()
    }

    /// `Option<T>` tag — `Ok(true)` means a payload follows.
    pub fn read_option_tag(&mut self) -> Result<bool, FluxBinError> {
        match self.read_u8()? {
            0x00 => Ok(false),
            0x01 => Ok(true),
            other => Err(FluxBinError::InvalidOptionTag(other)),
        }
    }

    /// `Identity` — 32 raw bytes.
    pub fn read_identity(&mut self) -> Result<[u8; 32], FluxBinError> {
        self.take_array()
    }

    /// `ConnectionId` — 16 raw bytes (`u128` LE).
    pub fn read_connection_id(&mut self) -> Result<u128, FluxBinError> {
        Ok(u128::from_le_bytes(self.take_array()?))
    }

    /// `EntityId` — 8 bytes LE.
    pub fn read_entity_id(&mut self) -> Result<u64, FluxBinError> {
        self.read_u64()
    }

    /// `Timestamp` — 8 bytes LE.
    pub fn read_timestamp(&mut self) -> Result<i64, FluxBinError> {
        self.read_i64()
    }

    /// enum tag — the caller then reads the variant payload.
    pub fn read_enum_tag(&mut self) -> Result<u8, FluxBinError> {
        self.read_u8()
    }
}
