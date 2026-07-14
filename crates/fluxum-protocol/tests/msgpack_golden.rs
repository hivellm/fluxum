//! MessagePack golden vectors for the envelope layer: the RPC-011
//! `FluxValue` encodings byte-for-byte, and the `[tag, payload]` envelope
//! shape SDKs must produce. Cross-language SDKs (T5.x) pin against these.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{ClientMessage, FluxValue, Subscribe};

fn encode(value: &FluxValue) -> Vec<u8> {
    rmp_serde::to_vec(value).unwrap()
}

fn decode(bytes: &[u8]) -> FluxValue {
    rmp_serde::from_slice(bytes).unwrap()
}

#[test]
fn golden_null_bool_int_float() {
    assert_eq!(encode(&FluxValue::Null), [0xC0]); // nil
    assert_eq!(encode(&FluxValue::Bool(true)), [0xC3]);
    assert_eq!(encode(&FluxValue::Bool(false)), [0xC2]);
    // Compact int: 5 → positive fixint.
    assert_eq!(encode(&FluxValue::I64(5)), [0x05]);
    assert_eq!(encode(&FluxValue::I64(-1)), [0xFF]); // negative fixint
    // float 64: 0xCB + 8 bytes big-endian IEEE 754.
    assert_eq!(
        encode(&FluxValue::F64(1.5)),
        [0xCB, 0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    );
}

#[test]
fn golden_bytes_str() {
    // bin 8: 0xC4 + length + raw bytes.
    assert_eq!(
        encode(&FluxValue::Bytes(vec![1, 2])),
        [0xC4, 0x02, 0x01, 0x02]
    );
    // fixstr: 0xA0 | length.
    assert_eq!(encode(&FluxValue::Str("hi".into())), [0xA2, 0x68, 0x69]);
}

#[test]
fn golden_array_map() {
    // fixarray of encoded FluxValues.
    assert_eq!(
        encode(&FluxValue::Array(vec![FluxValue::I64(1), FluxValue::Null])),
        [0x92, 0x01, 0xC0]
    );
    // map of key → value entries.
    assert_eq!(
        encode(&FluxValue::Map(vec![(
            FluxValue::Str("k".into()),
            FluxValue::I64(7)
        )])),
        [0x81, 0xA1, 0x6B, 0x07]
    );
}

#[test]
fn golden_tagged_variants() {
    // Identity → fixarray[2] of ["Identity", bin32].
    let identity: [u8; 32] = std::array::from_fn(|i| i as u8);
    let mut expected = vec![0x92, 0xA8];
    expected.extend_from_slice(b"Identity");
    expected.extend_from_slice(&[0xC4, 0x20]);
    expected.extend_from_slice(&identity);
    assert_eq!(encode(&FluxValue::Identity(identity)), expected);

    // EntityId → fixarray[2] of ["EntityId", uint] (compact).
    let mut expected = vec![0x92, 0xA8];
    expected.extend_from_slice(b"EntityId");
    expected.push(0x07);
    assert_eq!(encode(&FluxValue::EntityId(7)), expected);

    // Timestamp → fixarray[2] of ["Timestamp", int].
    let mut expected = vec![0x92, 0xA9];
    expected.extend_from_slice(b"Timestamp");
    expected.push(0xFF); // -1 as negative fixint
    assert_eq!(encode(&FluxValue::Timestamp(-1)), expected);

    // All three decode back to the tagged variant (canonical form).
    for value in [
        FluxValue::Identity(identity),
        FluxValue::EntityId(u64::MAX), // above i64::MAX — EntityId keeps full u64 range
        FluxValue::Timestamp(-1),
    ] {
        assert_eq!(decode(&encode(&value)), value);
    }
}

#[test]
fn tagged_collision_decodes_as_canonical_tagged_form() {
    // Documented canonicalization: an Array whose encoding coincides with a
    // tagged form decodes as the tagged variant.
    let array = FluxValue::Array(vec![FluxValue::Str("EntityId".into()), FluxValue::I64(5)]);
    assert_eq!(decode(&encode(&array)), FluxValue::EntityId(5));
    // …but near-misses stay arrays: wrong payload type / arity / sign.
    let not_tagged = FluxValue::Array(vec![FluxValue::Str("EntityId".into()), FluxValue::I64(-5)]);
    assert_eq!(decode(&encode(&not_tagged)), not_tagged);
    let not_tagged = FluxValue::Array(vec![
        FluxValue::Str("Identity".into()),
        FluxValue::Bytes(vec![0; 31]), // not 32 bytes
    ]);
    assert_eq!(decode(&encode(&not_tagged)), not_tagged);
}

#[test]
fn golden_envelope_shape() {
    // Envelope = fixarray[2] [tag, payload]; payload struct is positional.
    let msg = ClientMessage::Subscribe(Subscribe {
        id: 1,
        queries: vec!["SELECT * FROM t".into()],
    });
    let mut expected = vec![0x92, 0xA9];
    expected.extend_from_slice(b"Subscribe");
    expected.extend_from_slice(&[0x92, 0x01, 0x91, 0xAF]);
    expected.extend_from_slice(b"SELECT * FROM t");
    assert_eq!(msg.encode().unwrap(), expected);
    assert_eq!(ClientMessage::decode(&expected).unwrap(), msg);
}

#[test]
fn nan_roundtrips_at_the_bit_level() {
    let bytes = encode(&FluxValue::F64(f64::NAN));
    match decode(&bytes) {
        FluxValue::F64(f) => assert!(f.is_nan()),
        other => panic!("expected F64, got {other:?}"),
    }
}
