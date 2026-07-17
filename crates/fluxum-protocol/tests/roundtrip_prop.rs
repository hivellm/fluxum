//! Property tests (DAG T1.2 exit test / SPEC-006 acceptance criterion 1):
//! round-trip for every wire type — FluxValue, every FluxBIN type, flat
//! RowLists, every §4/§5 message, and framed messages.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{
    AuthResult, Authenticate, ClientMessage, ErrorMessage, FluxBinReader, FluxBinWriter, FluxValue,
    Frame, FrameCodec, InitialData, OneOffQuery, ReducerCall, ReducerResult, RowList,
    RowListBuilder, RowSizeHint, ServerMessage, Subscribe, SubscribeSingle, TableUpdate, TxUpdate,
    TxUpdateLight, Unsubscribe,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn flux_value() -> impl Strategy<Value = FluxValue> {
    let leaf = prop_oneof![
        Just(FluxValue::Null),
        any::<bool>().prop_map(FluxValue::Bool),
        any::<i64>().prop_map(FluxValue::I64),
        any::<f64>()
            .prop_filter("NaN breaks PartialEq", |f| !f.is_nan())
            .prop_map(FluxValue::F64),
        prop::collection::vec(any::<u8>(), 0..48).prop_map(FluxValue::Bytes),
        ".{0,24}".prop_map(FluxValue::Str),
        prop::array::uniform32(any::<u8>()).prop_map(FluxValue::Identity),
        any::<u64>().prop_map(FluxValue::EntityId),
        any::<i64>().prop_map(FluxValue::Timestamp),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(FluxValue::Array),
            prop::collection::vec((inner.clone(), inner), 0..5).prop_map(FluxValue::Map),
        ]
    })
}

fn rows() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..40), 0..8)
}

fn row_list() -> impl Strategy<Value = RowList> {
    rows().prop_map(|rows| {
        let mut b = RowListBuilder::new();
        for row in &rows {
            b.push_row(row);
        }
        b.finish()
    })
}

fn table_update() -> impl Strategy<Value = TableUpdate> {
    (
        any::<u32>(),
        ".{0,16}",
        any::<u32>(),
        row_list(),
        row_list(),
    )
        .prop_map(
            |(table_id, table_name, query_id, inserts, deletes)| TableUpdate {
                table_id,
                table_name,
                query_id,
                inserts,
                deletes,
            },
        )
}

fn opt_string() -> impl Strategy<Value = Option<String>> {
    prop::option::of(prop_oneof![
        Just("none".to_owned()),
        Just("gzip".to_owned()),
        Just("brotli".to_owned()),
        Just("full".to_owned()),
        Just("light".to_owned()),
        ".{0,8}".prop_map(String::from),
    ])
}

fn client_message() -> impl Strategy<Value = ClientMessage> {
    prop_oneof![
        (
            any::<u32>(),
            prop::collection::vec(any::<u8>(), 0..48),
            opt_string(),
            opt_string()
        )
            .prop_map(|(id, token, compression, tx_updates)| {
                ClientMessage::Authenticate(Authenticate {
                    id,
                    token,
                    compression,
                    tx_updates,
                })
            }),
        (
            any::<u32>(),
            ".{0,16}",
            prop::option::of(any::<u32>()),
            prop::collection::vec(flux_value(), 0..4),
            prop::option::of(".{0,24}".prop_map(String::from))
        )
            .prop_map(|(id, reducer, version, args, idempotency_key)| {
                ClientMessage::ReducerCall(ReducerCall {
                    id,
                    reducer,
                    version,
                    args,
                    // SPEC-021 CS-030 addition roundtrips too.
                    idempotency_key,
                })
            }),
        (
            any::<u32>(),
            prop::collection::vec(".{0,24}".prop_map(String::from), 0..4)
        )
            .prop_map(|(id, queries)| ClientMessage::Subscribe(Subscribe { id, queries })),
        (any::<u32>(), ".{0,24}")
            .prop_map(|(id, query)| ClientMessage::SubscribeSingle(SubscribeSingle { id, query })),
        (any::<u32>(), prop::collection::vec(any::<u32>(), 0..6))
            .prop_map(|(id, query_ids)| ClientMessage::Unsubscribe(Unsubscribe { id, query_ids })),
        (any::<u32>(), ".{0,24}")
            .prop_map(|(id, sql)| ClientMessage::OneOffQuery(OneOffQuery { id, sql })),
        // SPEC-021 CS-021: the additive Resume message.
        (any::<u32>(), any::<u32>(), any::<u64>()).prop_map(|(id, query_id, from_offset)| {
            ClientMessage::Resume(fluxum_protocol::Resume {
                id,
                query_id,
                from_offset,
            })
        }),
    ]
}

fn server_message() -> impl Strategy<Value = ServerMessage> {
    prop_oneof![
        (
            any::<u32>(),
            prop::array::uniform32(any::<u8>()),
            prop::collection::vec(any::<u8>(), 0..48)
        )
            .prop_map(|(id, identity, token)| {
                ServerMessage::AuthResult(AuthResult {
                    id,
                    identity,
                    token,
                })
            }),
        (
            any::<u32>(),
            prop::option::of((any::<u16>(), prop::option::of(".{0,8}"), ".{0,24}"))
        )
            .prop_map(|(id, err)| {
                let outcome = match err {
                    None => Ok(()),
                    Some((code, app_code, message)) => Err(fluxum_protocol::ReducerError {
                        code,
                        app_code,
                        message,
                    }),
                };
                ServerMessage::ReducerResult(ReducerResult { id, outcome })
            }),
        (
            any::<u32>(),
            any::<u32>(),
            any::<u64>(),
            any::<bool>(),
            prop::collection::vec(table_update(), 0..3)
        )
            .prop_map(|(id, schema_version, tx_offset, cache_reset, tables)| {
                ServerMessage::InitialData(InitialData {
                    id,
                    schema_version,
                    // SPEC-021 CS-020/CS-022 additions roundtrip too.
                    tx_offset,
                    cache_reset,
                    tables,
                })
            }),
        (
            any::<u64>(),
            any::<i64>(),
            ".{0,16}",
            prop::array::uniform32(any::<u8>()),
            any::<u32>(),
            any::<u32>(),
            any::<u64>(),
            prop::collection::vec(table_update(), 0..3)
        )
            .prop_map(
                |(
                    tx_id,
                    timestamp,
                    reducer_name,
                    caller,
                    duration_us,
                    shard_id,
                    tx_offset,
                    tables,
                )| {
                    ServerMessage::TxUpdate(TxUpdate {
                        tx_id,
                        timestamp,
                        reducer_name,
                        caller,
                        duration_us,
                        // SHD-051 + SPEC-021 CS-020 additions roundtrip too.
                        shard_id,
                        tx_offset,
                        tables,
                    })
                }
            ),
        (
            any::<u64>(),
            any::<i64>(),
            prop::collection::vec(table_update(), 0..3)
        )
            .prop_map(|(tx_id, timestamp, tables)| {
                ServerMessage::TxUpdateLight(TxUpdateLight {
                    tx_id,
                    timestamp,
                    tables,
                })
            }),
        (
            prop::option::of(any::<u32>()),
            prop::sample::select(
                fluxum_protocol::codes::CATALOG
                    .iter()
                    .map(|e| e.code)
                    .collect::<Vec<_>>(),
            ),
            ".{0,24}",
            prop::option::of(any::<u32>()),
        )
            .prop_map(|(id, code, message, retry_after_ms)| {
                ServerMessage::Error(
                    ErrorMessage::from_catalog(id, code, message, Vec::new())
                        .with_retry_after(retry_after_ms),
                )
            }),
    ]
}

// ---------------------------------------------------------------------------
// FluxValue (envelope MessagePack)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn flux_value_roundtrips(value in flux_value()) {
        let bytes = rmp_serde::to_vec(&value).unwrap();
        let back: FluxValue = rmp_serde::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, value);
    }
}

// ---------------------------------------------------------------------------
// FluxBIN: every RPC-040 type round-trips
// ---------------------------------------------------------------------------

macro_rules! fluxbin_prim_roundtrip {
    ($($test:ident: $ty:ty => $write:ident / $read:ident;)+) => {
        proptest! {
            $(
                #[test]
                fn $test(v in any::<$ty>()) {
                    let mut w = FluxBinWriter::new();
                    w.$write(v);
                    let mut r = FluxBinReader::new(w.as_bytes());
                    prop_assert_eq!(r.$read().unwrap(), v);
                    r.expect_eof().unwrap();
                }
            )+
        }
    };
}

fluxbin_prim_roundtrip! {
    fluxbin_bool_roundtrips: bool => write_bool / read_bool;
    fluxbin_u8_roundtrips: u8 => write_u8 / read_u8;
    fluxbin_i8_roundtrips: i8 => write_i8 / read_i8;
    fluxbin_u16_roundtrips: u16 => write_u16 / read_u16;
    fluxbin_i16_roundtrips: i16 => write_i16 / read_i16;
    fluxbin_u32_roundtrips: u32 => write_u32 / read_u32;
    fluxbin_i32_roundtrips: i32 => write_i32 / read_i32;
    fluxbin_u64_roundtrips: u64 => write_u64 / read_u64;
    fluxbin_i64_roundtrips: i64 => write_i64 / read_i64;
    fluxbin_connection_id_roundtrips: u128 => write_connection_id / read_connection_id;
    fluxbin_entity_id_roundtrips: u64 => write_entity_id / read_entity_id;
    fluxbin_timestamp_roundtrips: i64 => write_timestamp / read_timestamp;
    fluxbin_enum_tag_roundtrips: u8 => write_enum_tag / read_enum_tag;
}

proptest! {
    #[test]
    fn fluxbin_f32_roundtrips_bit_exact(v in any::<f32>()) {
        let mut w = FluxBinWriter::new();
        w.write_f32(v);
        let back = FluxBinReader::new(w.as_bytes()).read_f32().unwrap();
        prop_assert_eq!(back.to_bits(), v.to_bits());
    }

    #[test]
    fn fluxbin_f64_roundtrips_bit_exact(v in any::<f64>()) {
        let mut w = FluxBinWriter::new();
        w.write_f64(v);
        let back = FluxBinReader::new(w.as_bytes()).read_f64().unwrap();
        prop_assert_eq!(back.to_bits(), v.to_bits());
    }

    #[test]
    fn fluxbin_str_roundtrips(v in ".{0,64}") {
        let mut w = FluxBinWriter::new();
        w.write_str(&v).unwrap();
        let mut r = FluxBinReader::new(w.as_bytes());
        prop_assert_eq!(r.read_str().unwrap(), v);
        r.expect_eof().unwrap();
    }

    #[test]
    fn fluxbin_bytes_roundtrips(v in prop::collection::vec(any::<u8>(), 0..128)) {
        let mut w = FluxBinWriter::new();
        w.write_bytes(&v).unwrap();
        let mut r = FluxBinReader::new(w.as_bytes());
        prop_assert_eq!(r.read_bytes().unwrap(), v.as_slice());
        r.expect_eof().unwrap();
    }

    #[test]
    fn fluxbin_vec_t_roundtrips(v in prop::collection::vec(any::<u16>(), 0..32)) {
        // Vec<T>: u32 LE count + N × encode(T).
        let mut w = FluxBinWriter::new();
        w.write_seq_len(v.len() as u32);
        for &item in &v {
            w.write_u16(item);
        }
        let mut r = FluxBinReader::new(w.as_bytes());
        let count = r.read_seq_len().unwrap();
        prop_assert_eq!(count as usize, v.len());
        for &item in &v {
            prop_assert_eq!(r.read_u16().unwrap(), item);
        }
        r.expect_eof().unwrap();
    }

    #[test]
    fn fluxbin_option_roundtrips(v in prop::option::of(any::<u64>())) {
        let mut w = FluxBinWriter::new();
        w.write_option_tag(v.is_some());
        if let Some(inner) = v {
            w.write_u64(inner);
        }
        let mut r = FluxBinReader::new(w.as_bytes());
        let back = if r.read_option_tag().unwrap() {
            Some(r.read_u64().unwrap())
        } else {
            None
        };
        prop_assert_eq!(back, v);
        r.expect_eof().unwrap();
    }

    #[test]
    fn fluxbin_identity_roundtrips(v in prop::array::uniform32(any::<u8>())) {
        let mut w = FluxBinWriter::new();
        w.write_identity(&v);
        prop_assert_eq!(w.len(), 32);
        let mut r = FluxBinReader::new(w.as_bytes());
        prop_assert_eq!(r.read_identity().unwrap(), v);
        r.expect_eof().unwrap();
    }

    /// Struct rule: any mix of field writes concatenates and reads back in
    /// declaration order.
    #[test]
    fn fluxbin_struct_concat_roundtrips(
        a in any::<i32>(),
        b in ".{0,16}",
        c in prop::option::of(any::<f64>()),
        d in any::<bool>(),
    ) {
        let mut w = FluxBinWriter::new();
        w.write_i32(a);
        w.write_str(&b).unwrap();
        w.write_option_tag(c.is_some());
        if let Some(inner) = c {
            w.write_f64(inner);
        }
        w.write_bool(d);

        let mut r = FluxBinReader::new(w.as_bytes());
        prop_assert_eq!(r.read_i32().unwrap(), a);
        prop_assert_eq!(r.read_str().unwrap(), b);
        let back_c = if r.read_option_tag().unwrap() {
            Some(r.read_f64().unwrap())
        } else {
            None
        };
        prop_assert_eq!(back_c.map(f64::to_bits), c.map(f64::to_bits));
        prop_assert_eq!(r.read_bool().unwrap(), d);
        r.expect_eof().unwrap();
    }

    /// Truncating any FluxBIN payload yields an error, never a panic.
    #[test]
    fn fluxbin_truncation_never_panics(
        v in prop::collection::vec(any::<u8>(), 0..64),
        cut in 0usize..64,
    ) {
        let mut w = FluxBinWriter::new();
        w.write_bytes(&v).unwrap();
        let bytes = w.as_bytes();
        let cut = cut.min(bytes.len());
        let _ = FluxBinReader::new(&bytes[..cut]).read_bytes();
    }
}

// ---------------------------------------------------------------------------
// RowList (RPC-032)
// ---------------------------------------------------------------------------

proptest! {
    /// Builder output is always consistent, slices back to the input rows,
    /// and picks Fixed exactly when all rows share one nonzero size.
    #[test]
    fn rowlist_builder_roundtrips(rows in rows()) {
        let mut b = RowListBuilder::new();
        for row in &rows {
            b.push_row(row);
        }
        let list = b.finish();
        list.validate().unwrap();
        prop_assert_eq!(list.len(), rows.len());
        for (i, row) in rows.iter().enumerate() {
            prop_assert_eq!(list.row(i).unwrap(), row.as_slice());
        }
        let uniform_nonzero = !rows.is_empty()
            && !rows[0].is_empty()
            && rows.iter().all(|r| r.len() == rows[0].len());
        match &list.size_hint {
            RowSizeHint::Fixed(n) => {
                prop_assert!(rows.is_empty() || uniform_nonzero);
                if uniform_nonzero {
                    prop_assert_eq!(usize::from(*n), rows[0].len());
                }
            }
            RowSizeHint::Offsets(offsets) => {
                prop_assert!(!uniform_nonzero);
                prop_assert_eq!(offsets.len(), rows.len());
            }
        }
    }

    /// RowList survives the MessagePack envelope (single bin field for
    /// rows_data) with validation on decode.
    #[test]
    fn rowlist_msgpack_roundtrips(list in row_list()) {
        let bytes = rmp_serde::to_vec(&list).unwrap();
        let back: RowList = rmp_serde::from_slice(&bytes).unwrap();
        prop_assert_eq!(back, list);
    }
}

// ---------------------------------------------------------------------------
// Messages + frames (RPC-001, RPC-002, §4/§5)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn client_message_roundtrips(msg in client_message()) {
        let bytes = msg.encode().unwrap();
        prop_assert_eq!(ClientMessage::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn server_message_roundtrips(msg in server_message()) {
        let bytes = msg.encode().unwrap();
        prop_assert_eq!(ServerMessage::decode(&bytes).unwrap(), msg);
    }

    /// Full wire path: envelope → frame → bytes → frame → envelope.
    #[test]
    fn framed_message_roundtrips(msg in server_message()) {
        let codec = FrameCodec::default();
        let body = msg.encode().unwrap();
        let framed = codec.encode(&body).unwrap();
        let (frame, consumed) = codec.decode(&framed).unwrap().unwrap();
        prop_assert_eq!(consumed, framed.len());
        match frame {
            Frame::Body(bytes) => {
                prop_assert_eq!(ServerMessage::decode(bytes).unwrap(), msg);
            }
            Frame::KeepAlive => prop_assert!(false, "non-empty body decoded as keep-alive"),
        }
    }
}
