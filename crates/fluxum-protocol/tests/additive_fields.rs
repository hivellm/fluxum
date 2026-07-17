//! RPC-011 / SPEC-021 CS-023 — the additive-field contract.
//!
//! The envelope is *positional*: a payload encodes as a MessagePack array in
//! field-declaration order (`rmp_serde::to_vec`, the compact form). So a new
//! field is only backward compatible at the **tail**, where `#[serde(default)]`
//! fills it in for a frame that predates it. Inserting one mid-struct shifts
//! every later field and makes older frames undecodable — silently, until a
//! real client hits it.
//!
//! These tests pin that contract for the fields added ahead of the G5 wire
//! freeze (`shard_id`, `tx_offset`, `cache_reset`) by decoding frames shaped
//! exactly as a pre-field encoder would have written them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use serde::Serialize;

use fluxum_protocol::{InitialData, ReducerCall, TxUpdate};

/// `TxUpdate` as it was before `shard_id`/`tx_offset` existed.
#[derive(Serialize)]
struct LegacyTxUpdate {
    tx_id: u64,
    timestamp: i64,
    reducer_name: String,
    caller: serde_bytes::ByteBuf,
    duration_us: u32,
    tables: Vec<()>,
}

/// `InitialData` as it was before `tx_offset`/`cache_reset` existed.
#[derive(Serialize)]
struct LegacyInitialData {
    id: u32,
    schema_version: u32,
    tables: Vec<()>,
}

#[test]
fn a_tx_update_frame_without_the_new_fields_still_decodes() {
    let legacy = LegacyTxUpdate {
        tx_id: 42,
        timestamp: 1_700_000_000,
        reducer_name: "send_chat".into(),
        caller: serde_bytes::ByteBuf::from(vec![9u8; 32]),
        duration_us: 17,
        tables: vec![],
    };
    let bytes = rmp_serde::to_vec(&legacy).unwrap();

    let decoded: TxUpdate = rmp_serde::from_slice(&bytes)
        .expect("a pre-field frame must still decode (RPC-011 additive tail)");
    // The pre-existing fields survive verbatim...
    assert_eq!(decoded.tx_id, 42);
    assert_eq!(decoded.reducer_name, "send_chat");
    assert_eq!(decoded.duration_us, 17);
    assert_eq!(decoded.caller, [9u8; 32]);
    // ...and the additions default rather than corrupting the parse.
    assert_eq!(decoded.shard_id, 0, "SHD-051 default");
    assert_eq!(decoded.tx_offset, 0, "CS-020 default");
}

/// `ReducerCall` as it was before `idempotency_key` existed.
#[derive(Serialize)]
struct LegacyReducerCall {
    id: u32,
    reducer: String,
    version: Option<u32>,
    args: Vec<()>,
}

#[test]
fn a_reducer_call_frame_without_the_idempotency_key_still_decodes() {
    let legacy = LegacyReducerCall {
        id: 5,
        reducer: "transfer".into(),
        version: None,
        args: vec![],
    };
    let bytes = rmp_serde::to_vec(&legacy).unwrap();

    let decoded: ReducerCall = rmp_serde::from_slice(&bytes)
        .expect("a pre-field frame must still decode (RPC-011 additive tail)");
    assert_eq!(decoded.id, 5);
    assert_eq!(decoded.reducer, "transfer");
    assert_eq!(
        decoded.idempotency_key, None,
        "CS-030 default: an old client simply opts out of exactly-once"
    );
}

#[test]
fn an_initial_data_frame_without_the_new_fields_still_decodes() {
    let legacy = LegacyInitialData {
        id: 3,
        schema_version: 11,
        tables: vec![],
    };
    let bytes = rmp_serde::to_vec(&legacy).unwrap();

    let decoded: InitialData = rmp_serde::from_slice(&bytes)
        .expect("a pre-field frame must still decode (RPC-011 additive tail)");
    assert_eq!(decoded.id, 3);
    assert_eq!(decoded.schema_version, 11);
    assert_eq!(decoded.tx_offset, 0, "CS-020 default");
    assert!(!decoded.cache_reset, "CS-022 default");
}
