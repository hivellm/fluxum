//! FluxBIN golden vectors (SPEC-006 acceptance criterion 2): fixed input →
//! fixed expected bytes for every RPC-040 type, the RPC-041 `Sensor` row at
//! exactly 32 bytes, the RPC-042 delete entries at exactly 8 bytes, and the
//! RPC-032 flat `RowList` layouts.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{FluxBinReader, FluxBinWriter, RowList, RowListBuilder, RowSizeHint};

fn written(f: impl FnOnce(&mut FluxBinWriter)) -> Vec<u8> {
    let mut w = FluxBinWriter::new();
    f(&mut w);
    w.into_bytes()
}

#[test]
fn golden_bool() {
    assert_eq!(written(|w| w.write_bool(false)), [0x00]);
    assert_eq!(written(|w| w.write_bool(true)), [0x01]);
}

#[test]
fn golden_u8_i8() {
    assert_eq!(written(|w| w.write_u8(0xAB)), [0xAB]);
    assert_eq!(written(|w| w.write_i8(-2)), [0xFE]);
}

#[test]
fn golden_u16_i16() {
    assert_eq!(written(|w| w.write_u16(0x1234)), [0x34, 0x12]);
    assert_eq!(written(|w| w.write_i16(-2)), [0xFE, 0xFF]);
}

#[test]
fn golden_u32_i32() {
    assert_eq!(
        written(|w| w.write_u32(0x1234_5678)),
        [0x78, 0x56, 0x34, 0x12]
    );
    assert_eq!(written(|w| w.write_i32(-2)), [0xFE, 0xFF, 0xFF, 0xFF]);
}

#[test]
fn golden_u64_i64() {
    assert_eq!(
        written(|w| w.write_u64(0x0102_0304_0506_0708)),
        [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
    );
    assert_eq!(
        written(|w| w.write_i64(-2)),
        [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
}

#[test]
fn golden_f32_f64() {
    // 12.5f32 = 0x41480000 IEEE 754.
    assert_eq!(written(|w| w.write_f32(12.5)), [0x00, 0x00, 0x48, 0x41]);
    // 21.7f64 = 0x4035B33333333333 IEEE 754.
    assert_eq!(
        written(|w| w.write_f64(21.7)),
        [0x33, 0x33, 0x33, 0x33, 0x33, 0xB3, 0x35, 0x40]
    );
}

#[test]
fn golden_string() {
    // u32 LE length + UTF-8 bytes.
    assert_eq!(
        written(|w| w.write_str("hi").unwrap()),
        [0x02, 0x00, 0x00, 0x00, 0x68, 0x69]
    );
    assert_eq!(
        written(|w| w.write_str("").unwrap()),
        [0x00, 0x00, 0x00, 0x00]
    );
}

#[test]
fn golden_vec_u8() {
    // u32 LE length + raw bytes.
    assert_eq!(
        written(|w| w.write_bytes(&[0xDE, 0xAD, 0xBE]).unwrap()),
        [0x03, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE]
    );
}

#[test]
fn golden_vec_t() {
    // Vec<u16> [1, 2]: u32 LE count + N × encode(T).
    assert_eq!(
        written(|w| {
            w.write_seq_len(2);
            w.write_u16(1);
            w.write_u16(2);
        }),
        [0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00]
    );
}

#[test]
fn golden_option() {
    // Option<u8>: 0x00 (None) | 0x01 + encode(T).
    assert_eq!(written(|w| w.write_option_tag(false)), [0x00]);
    assert_eq!(
        written(|w| {
            w.write_option_tag(true);
            w.write_u8(7);
        }),
        [0x01, 0x07]
    );
}

#[test]
fn golden_identity() {
    // 32 bytes raw, no prefix.
    let identity: [u8; 32] = std::array::from_fn(|i| i as u8);
    let bytes = written(|w| w.write_identity(&identity));
    assert_eq!(bytes.len(), 32);
    assert_eq!(bytes, identity);
}

#[test]
fn golden_connection_id() {
    // 16 bytes raw: the u128 in little-endian, no prefix.
    assert_eq!(
        written(|w| w.write_connection_id(0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10)),
        [
            0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09, //
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        ]
    );
}

#[test]
fn golden_entity_id() {
    // 8 bytes LE (u64 newtype).
    assert_eq!(
        written(|w| w.write_entity_id(42)),
        [0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    );
}

#[test]
fn golden_timestamp() {
    // 8 bytes LE (i64 µs since Unix epoch).
    assert_eq!(
        written(|w| w.write_timestamp(-2)),
        [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
}

#[test]
fn golden_enum() {
    // u8 tag + encode(variant payload); e.g. variant 1 carrying a u32.
    assert_eq!(
        written(|w| {
            w.write_enum_tag(1);
            w.write_u32(7);
        }),
        [0x01, 0x07, 0x00, 0x00, 0x00]
    );
}

/// The canonical RPC-041 `Sensor` row:
/// `{ grid_x: i32, grid_y: i32, x: f32, y: f32, reading: f64, updated_at: Timestamp }`.
fn encode_sensor(
    grid_x: i32,
    grid_y: i32,
    x: f32,
    y: f32,
    reading: f64,
    updated_at: i64,
) -> Vec<u8> {
    let mut w = FluxBinWriter::with_capacity(32);
    w.write_i32(grid_x);
    w.write_i32(grid_y);
    w.write_f32(x);
    w.write_f32(y);
    w.write_f64(reading);
    w.write_timestamp(updated_at);
    w.into_bytes()
}

#[test]
fn golden_sensor_insert_row_is_exactly_32_bytes() {
    // Struct rule: fields in declaration order, no separators, no names.
    let bytes = encode_sensor(5, 3, 12.5, 8.0, 21.7, 0x0807_0605_0403_0201);
    assert_eq!(
        bytes,
        [
            0x05, 0x00, 0x00, 0x00, // grid_x: 5 i32 LE
            0x03, 0x00, 0x00, 0x00, // grid_y: 3 i32 LE
            0x00, 0x00, 0x48, 0x41, // x: 12.5 f32 LE
            0x00, 0x00, 0x00, 0x41, // y: 8.0 f32 LE
            0x33, 0x33, 0x33, 0x33, 0x33, 0xB3, 0x35, 0x40, // reading: 21.7 f64 LE
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // updated_at i64 LE
        ]
    );
    assert_eq!(bytes.len(), 32);

    // And it decodes back field-for-field.
    let mut r = FluxBinReader::new(&bytes);
    assert_eq!(r.read_i32().unwrap(), 5);
    assert_eq!(r.read_i32().unwrap(), 3);
    assert_eq!(r.read_f32().unwrap(), 12.5);
    assert_eq!(r.read_f32().unwrap(), 8.0);
    assert_eq!(r.read_f64().unwrap(), 21.7);
    assert_eq!(r.read_timestamp().unwrap(), 0x0807_0605_0403_0201);
    r.expect_eof().unwrap();
}

#[test]
fn golden_delete_single_pk_is_exactly_8_bytes() {
    // RPC-042: Task deleted, pk = 42 → [42 u64 LE].
    let bytes = written(|w| w.write_u64(42));
    assert_eq!(bytes, [0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_eq!(bytes.len(), 8);
}

#[test]
fn golden_delete_composite_pk_is_exactly_8_bytes() {
    // RPC-042: Sensor deleted, grid_x = 5, grid_y = 3 → [5 i32 LE][3 i32 LE].
    let bytes = written(|w| {
        w.write_i32(5);
        w.write_i32(3);
    });
    assert_eq!(bytes, [0x05, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00]);
    assert_eq!(bytes.len(), 8);
}

/// A variable-size `ChatMessage` row:
/// `{ id: u64, sender: u64, text: String, sent_at: Timestamp }`.
fn encode_chat_message(id: u64, sender: u64, text: &str, sent_at: i64) -> Vec<u8> {
    let mut w = FluxBinWriter::new();
    w.write_u64(id);
    w.write_u64(sender);
    w.write_str(text).unwrap();
    w.write_timestamp(sent_at);
    w.into_bytes()
}

#[test]
fn rowlist_three_sensor_rows_are_fixed_32_by_96() {
    // Acceptance 2: three Sensor rows → RowList { row_count: 3,
    // size_hint: Fixed(32), rows_data: 96 bytes } — zero per-row overhead.
    let mut b = RowListBuilder::new();
    for i in 0..3i32 {
        b.push_row(&encode_sensor(i, i + 1, 1.0, 2.0, 3.0, 4));
    }
    let list = b.finish();
    assert_eq!(list.row_count, 3);
    assert_eq!(list.size_hint, RowSizeHint::Fixed(32));
    assert_eq!(list.rows_data.len(), 96);
    list.validate().unwrap();
    assert_eq!(list.row(1).unwrap(), &list.rows_data[32..64]);
}

#[test]
fn rowlist_variable_chat_rows_degrade_to_offsets() {
    let rows = [
        encode_chat_message(1, 7, "hi", 100),
        encode_chat_message(2, 7, "a much longer chat message", 101),
        encode_chat_message(3, 8, "ok", 102),
    ];
    let mut b = RowListBuilder::new();
    for row in &rows {
        b.push_row(row);
    }
    let list = b.finish();
    assert_eq!(list.row_count, 3);
    // Started optimistically from the first row's size, degraded on row 2
    // with retroactively synthesized offsets.
    let first = rows[0].len() as u64;
    let second = first + rows[1].len() as u64;
    assert_eq!(list.size_hint, RowSizeHint::Offsets(vec![0, first, second]));
    list.validate().unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(list.row(i).unwrap(), row.as_slice());
    }
}

#[test]
fn rowlist_schema_known_fixed_size_survives_empty_batch() {
    let list = RowListBuilder::with_fixed_size(32).finish();
    assert_eq!(list.size_hint, RowSizeHint::Fixed(32));
    assert_eq!(list.row_count, 0);
    list.validate().unwrap();
}

#[test]
fn rowlist_inconsistent_is_rejected_with_400() {
    // Fixed(32) claiming 3 rows over 95 bytes of data.
    let bad = RowList {
        row_count: 3,
        size_hint: RowSizeHint::Fixed(32),
        rows_data: vec![0; 95],
    };
    let err = bad.validate().unwrap_err();
    assert_eq!(err.code(), 400);

    // Offset table shorter than row_count.
    let bad = RowList {
        row_count: 2,
        size_hint: RowSizeHint::Offsets(vec![0]),
        rows_data: vec![1, 2, 3],
    };
    assert_eq!(bad.validate().unwrap_err().code(), 400);

    // Offset beyond the data buffer.
    let bad = RowList {
        row_count: 2,
        size_hint: RowSizeHint::Offsets(vec![0, 99]),
        rows_data: vec![1, 2, 3],
    };
    assert!(bad.validate().is_err());

    // Non-monotonic offsets.
    let bad = RowList {
        row_count: 3,
        size_hint: RowSizeHint::Offsets(vec![0, 2, 1]),
        rows_data: vec![1, 2, 3],
    };
    assert!(bad.validate().is_err());

    // Inconsistent lists are rejected at the codec boundary too.
    let bad = RowList {
        row_count: 3,
        size_hint: RowSizeHint::Fixed(32),
        rows_data: vec![0; 95],
    };
    let bytes = rmp_serde::to_vec(&bad).unwrap();
    assert!(rmp_serde::from_slice::<RowList>(&bytes).is_err());
}

#[test]
fn reader_rejects_malformed_input() {
    use fluxum_protocol::FluxBinError;

    // Truncation surfaces as UnexpectedEof, never a panic.
    assert_eq!(
        FluxBinReader::new(&[0x01]).read_u32(),
        Err(FluxBinError::UnexpectedEof {
            needed: 4,
            remaining: 1
        })
    );
    // A hostile length prefix cannot over-read (or over-allocate: the reader
    // bounds-checks before slicing).
    assert_eq!(
        FluxBinReader::new(&[0xFF, 0xFF, 0xFF, 0xFF]).read_bytes(),
        Err(FluxBinError::UnexpectedEof {
            needed: u32::MAX as usize,
            remaining: 0
        })
    );
    // Domain bytes are validated.
    assert_eq!(
        FluxBinReader::new(&[0x02]).read_bool(),
        Err(FluxBinError::InvalidBool(0x02))
    );
    assert_eq!(
        FluxBinReader::new(&[0x07]).read_option_tag(),
        Err(FluxBinError::InvalidOptionTag(0x07))
    );
    assert_eq!(
        FluxBinReader::new(&[0x02, 0x00, 0x00, 0x00, 0xFF, 0xFE]).read_str(),
        Err(FluxBinError::InvalidUtf8)
    );
    // Trailing garbage after a complete row is an error on demand.
    let mut r = FluxBinReader::new(&[0x01, 0x02]);
    r.read_u8().unwrap();
    assert_eq!(r.expect_eof(), Err(FluxBinError::TrailingBytes(1)));
    // Every FluxBIN decode failure maps to wire code 400.
    assert_eq!(FluxBinError::InvalidUtf8.code(), 400);
}
