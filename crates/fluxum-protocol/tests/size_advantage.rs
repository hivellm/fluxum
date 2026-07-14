//! FluxBIN size advantage (FR-41, SPEC-006 acceptance criterion 3): the
//! canonical `Sensor` and `ChatMessage` rows must encode measurably smaller
//! in FluxBIN than as self-describing MessagePack maps (field names +
//! per-value type tags), with the ~40% target hit by fixed-width typed rows.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::FluxBinWriter;
use serde::Serialize;

const UPDATED_AT: i64 = 1_700_000_000_000_000; // µs since epoch

#[derive(Serialize)]
struct SensorMsg {
    grid_x: i32,
    grid_y: i32,
    x: f32,
    y: f32,
    reading: f64,
    updated_at: i64,
}

#[derive(Serialize)]
struct ChatMessageMsg {
    id: u64,
    sender: u64,
    text: &'static str,
    sent_at: i64,
}

#[test]
fn sensor_row_hits_the_40_percent_target() {
    let mut w = FluxBinWriter::new();
    w.write_i32(5);
    w.write_i32(3);
    w.write_f32(12.5);
    w.write_f32(8.0);
    w.write_f64(21.7);
    w.write_timestamp(UPDATED_AT);
    let fluxbin = w.into_bytes();
    assert_eq!(fluxbin.len(), 32); // RPC-041

    // Self-describing MessagePack: map with field names + per-value tags
    // (what a schemaless encoding of the same row costs — ≈68 bytes).
    let msgpack = rmp_serde::to_vec_named(&SensorMsg {
        grid_x: 5,
        grid_y: 3,
        x: 12.5,
        y: 8.0,
        reading: 21.7,
        updated_at: UPDATED_AT,
    })
    .unwrap();

    let saving = 1.0 - (fluxbin.len() as f64 / msgpack.len() as f64);
    println!(
        "Sensor: FluxBIN {} bytes vs MessagePack map {} bytes ({:.0}% smaller)",
        fluxbin.len(),
        msgpack.len(),
        saving * 100.0
    );
    assert!(
        saving >= 0.40,
        "expected >= 40% saving on the fixed-width Sensor row, got {:.0}%",
        saving * 100.0
    );
}

#[test]
fn chat_message_row_is_measurably_smaller() {
    let text = "hello from fluxum";

    let mut w = FluxBinWriter::new();
    w.write_u64(7_000_000);
    w.write_u64(42_000);
    w.write_str(text).unwrap();
    w.write_timestamp(UPDATED_AT);
    let fluxbin = w.into_bytes();

    let msgpack = rmp_serde::to_vec_named(&ChatMessageMsg {
        id: 7_000_000,
        sender: 42_000,
        text,
        sent_at: UPDATED_AT,
    })
    .unwrap();

    let saving = 1.0 - (fluxbin.len() as f64 / msgpack.len() as f64);
    println!(
        "ChatMessage: FluxBIN {} bytes vs MessagePack map {} bytes ({:.0}% smaller)",
        fluxbin.len(),
        msgpack.len(),
        saving * 100.0
    );
    // Variable-size rows carry their string payload in both encodings, so
    // the saving is smaller than the fixed-width case but must still be
    // measurable.
    assert!(
        saving >= 0.15,
        "expected a measurable (>= 15%) saving on ChatMessage, got {:.0}%",
        saving * 100.0
    );
}
