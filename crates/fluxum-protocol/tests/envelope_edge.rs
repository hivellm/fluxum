//! Envelope-layer edge coverage (RPC-011/RPC-031/RPC-032): `RowList`
//! accessors and validation failures, tagged-envelope decode errors, the
//! `bin32`/`outcome` serde adapters, and `FluxValue` decode corner cases.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{
    AuthResult, ClientMessage, FluxValue, ReducerResult, RowList, RowListBuilder, RowSizeHint,
    ServerMessage,
};

// --- RowList accessors -------------------------------------------------------

#[test]
fn empty_rowlist_accessors_and_default() {
    let empty = RowList::empty();
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
    assert_eq!(empty.row(0), None);
    assert_eq!(empty.iter().count(), 0);
    assert_eq!(RowList::default(), empty);
    empty.validate().unwrap();
}

#[test]
fn rowlist_validate_rejects_inconsistent_lists() {
    // Fixed(0) must describe an empty batch.
    let bad = RowList {
        row_count: 1,
        size_hint: RowSizeHint::Fixed(0),
        rows_data: vec![1],
    };
    let err = bad.validate().unwrap_err();
    assert_eq!(err.code(), fluxum_protocol::codes::PROTO_MALFORMED);
    assert!(err.to_string().contains("Fixed(0)"), "{err}");

    // Offsets with row_count = 0 cannot carry data bytes.
    let bad = RowList {
        row_count: 0,
        size_hint: RowSizeHint::Offsets(vec![]),
        rows_data: vec![9],
    };
    let err = bad.validate().unwrap_err();
    assert!(err.to_string().contains("row_count=0"), "{err}");

    // The first offset must be 0.
    let bad = RowList {
        row_count: 2,
        size_hint: RowSizeHint::Offsets(vec![1, 2]),
        rows_data: vec![0, 0, 0],
    };
    let err = bad.validate().unwrap_err();
    assert!(err.to_string().contains("first offset"), "{err}");

    // Non-monotonic offsets.
    let bad = RowList {
        row_count: 2,
        size_hint: RowSizeHint::Offsets(vec![0, 3]),
        rows_data: vec![0, 0],
    };
    assert!(bad.validate().is_err());
}

#[test]
fn builder_fixed_zero_falls_back_to_first_row_sizing() {
    let mut b = RowListBuilder::with_fixed_size(0);
    assert!(b.is_empty());
    assert_eq!(b.len(), 0);
    b.push_row(&[1, 2]);
    b.push_row(&[3, 4]);
    assert!(!b.is_empty());
    assert_eq!(b.len(), 2);
    let list = b.finish();
    assert_eq!(list.size_hint, RowSizeHint::Fixed(2));
    let rows: Vec<&[u8]> = list.iter().collect();
    assert_eq!(rows, vec![&[1u8, 2][..], &[3u8, 4][..]]);
    assert_eq!(list.row(2), None);

    let default_builder = RowListBuilder::default();
    assert!(default_builder.is_empty());
    assert_eq!(default_builder.finish(), RowList::empty());
}

// --- Tagged envelopes ---------------------------------------------------------

#[test]
fn tagged_envelope_rejects_unknown_tags() {
    let bytes = rmp_serde::to_vec(&("Nope", ())).unwrap();
    let err = ClientMessage::decode(&bytes).unwrap_err().to_string();
    assert!(err.contains("Nope"), "{err}");
    let err = ServerMessage::decode(&bytes).unwrap_err().to_string();
    assert!(err.contains("Nope"), "{err}");
}

#[test]
fn tagged_envelope_rejects_malformed_arrays() {
    // fixarray 0 — no tag at all.
    assert!(ClientMessage::decode(&[0x90]).is_err());
    assert!(ServerMessage::decode(&[0x90]).is_err());
    // Not an array.
    assert!(ClientMessage::decode(&[0x2A]).is_err());
}

// --- bin32 / outcome serde adapters --------------------------------------------

#[test]
fn auth_result_rejects_wrong_identity_length() {
    // AuthResult is (id, identity: bin32, token: bin) positionally; a 31-byte
    // identity must fail with the adapter's "exactly 32 raw bytes" message.
    let wire = (
        7u32,
        serde_bytes::ByteBuf::from(vec![0u8; 31]),
        serde_bytes::ByteBuf::from(vec![1u8, 2]),
    );
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let err = rmp_serde::from_slice::<AuthResult>(&bytes)
        .unwrap_err()
        .to_string();
    assert!(err.contains("exactly 32 raw bytes"), "{err}");
}

#[test]
fn reducer_result_outcome_rejects_unknown_tag() {
    let wire = (3u32, ("Boom", "message"));
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let err = rmp_serde::from_slice::<ReducerResult>(&bytes)
        .unwrap_err()
        .to_string();
    assert!(err.contains("Boom"), "{err}");
}

#[test]
fn reducer_result_outcome_round_trips_both_arms() {
    for outcome in [
        Ok(()),
        Err(fluxum_protocol::ReducerError {
            code: 5001,
            app_code: None,
            message: "saldo insuficiente".to_owned(),
        }),
        Err(fluxum_protocol::ReducerError {
            code: 5002,
            app_code: Some("APP_LIMIT".to_owned()),
            message: "boom".to_owned(),
        }),
    ] {
        let msg = ReducerResult { id: 9, outcome };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let back: ReducerResult = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, msg);
    }
}

// --- FluxValue decode corner cases ----------------------------------------------

#[test]
fn fluxvalue_rejects_big_u64_outside_entity_id() {
    // [u64::MAX] — a plain array carrying an integer above i64::MAX.
    let bytes = [0x91, 0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    let err = rmp_serde::from_slice::<FluxValue>(&bytes)
        .unwrap_err()
        .to_string();
    assert!(err.contains("EntityId"), "{err}");
}

#[test]
fn fluxvalue_big_u64_is_legal_as_entity_id_payload() {
    // ["EntityId", u64::MAX] decodes as the tagged variant.
    let bytes = rmp_serde::to_vec(&("EntityId", u64::MAX)).unwrap();
    let value: FluxValue = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(value, FluxValue::EntityId(u64::MAX));
}

#[test]
fn fluxvalue_decodes_owned_strings_and_bytes_from_a_reader() {
    // from_read forces owned data through visit_string / visit_byte_buf.
    let value = FluxValue::Array(vec![
        FluxValue::Str("hello".into()),
        FluxValue::Bytes(vec![1, 2, 3]),
        FluxValue::Map(vec![(FluxValue::Str("k".into()), FluxValue::I64(-4))]),
    ]);
    let bytes = rmp_serde::to_vec(&value).unwrap();
    let back: FluxValue = rmp_serde::from_read(std::io::Cursor::new(bytes)).unwrap();
    assert_eq!(back, value);
}

#[test]
fn fluxvalue_rejects_msgpack_ext_values() {
    // fixext1 (type 5, one data byte) is outside the FluxValue universe.
    assert!(rmp_serde::from_slice::<FluxValue>(&[0xD4, 0x05, 0x2A]).is_err());
}

#[test]
fn reducer_result_outcome_rejects_truncated_tuples() {
    // An empty outcome array: the visitor's invalid_length error renders the
    // `expecting` description.
    let wire = (3u32, Vec::<String>::new());
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let err = rmp_serde::from_slice::<ReducerResult>(&bytes)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(r#"["Ok", nil] or ["Err", [code, app_code, message]]"#),
        "{err}"
    );

    // A lone tag without its payload element.
    let wire = (3u32, ("Ok",));
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let err = rmp_serde::from_slice::<ReducerResult>(&bytes)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(r#"["Ok", nil] or ["Err", [code, app_code, message]]"#),
        "{err}"
    );
}

// --- FluxValue visitor: owned/optional deserializer shapes ----------------------
//
// MessagePack never drives `visit_string`/`visit_byte_buf` (rmp-serde hands
// out transient slices) nor `visit_some`/`visit_none` (nil is `visit_unit`),
// but `FluxValue: Deserialize` is format-agnostic — a self-describing format
// that owns its data (or models optionality) must decode identically. A
// minimal hand-rolled deserializer exercises those visitor entry points.

#[derive(Clone)]
enum OwnedShape {
    Str(String),
    Bytes(Vec<u8>),
    Some(Box<OwnedShape>),
    None,
}

impl<'de> serde::Deserializer<'de> for OwnedShape {
    type Error = serde::de::value::Error;

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        match self {
            Self::Str(s) => visitor.visit_string(s),
            Self::Bytes(b) => visitor.visit_byte_buf(b),
            Self::Some(inner) => visitor.visit_some(*inner),
            Self::None => visitor.visit_none(),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

#[test]
fn fluxvalue_decodes_owned_strings_bytes_and_options() {
    use serde::Deserialize;

    let s = FluxValue::deserialize(OwnedShape::Str("owned".into())).unwrap();
    assert_eq!(s, FluxValue::Str("owned".into()));

    let b = FluxValue::deserialize(OwnedShape::Bytes(vec![1, 2, 3])).unwrap();
    assert_eq!(b, FluxValue::Bytes(vec![1, 2, 3]));

    // `Some(inner)` decodes as the inner value; `None` decodes as Null.
    let some =
        FluxValue::deserialize(OwnedShape::Some(Box::new(OwnedShape::Str("in".into())))).unwrap();
    assert_eq!(some, FluxValue::Str("in".into()));
    let none = FluxValue::deserialize(OwnedShape::None).unwrap();
    assert_eq!(none, FluxValue::Null);
}
