//! Byte-for-byte parity with the HiveLLM family wire layer (SPEC-001).
//!
//! Fluxum's framing *is* Thunder's framing — `u32 LE length prefix +
//! MessagePack body` (SPEC-006 RPC-001). The TypeScript SDK proves that by
//! delegating to `@hivehub/thunder`'s `FrameReader`; the Rust side cannot yet,
//! because `thunder::wire` only decodes a frame by deserializing the body into
//! its own `Request`/`Response` and offers no borrowed-body variant
//! (hivellm/thunder#6). Fluxum's bodies are its own `[tag, payload]` catalog
//! decoded from borrowed slices, so `FrameCodec` stays hand-written for now.
//!
//! "For now" is the risk this file removes: as long as these assertions hold,
//! the two implementations cannot drift, and the day Thunder grows
//! `decode_frame_raw` the switch is a deletion rather than a re-derivation.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{FRAME_HEADER_LEN, Frame, FrameCodec};
use thunder::wire::{Request, Value, encode_frame};

/// Thunder frames a body; Fluxum frames the same body. The bytes must match.
#[test]
fn fluxum_framing_reproduces_thunder_framing_byte_for_byte() {
    let messages = [
        Request {
            id: 1,
            command: "PING".to_owned(),
            args: vec![],
        },
        Request {
            id: 7,
            command: "GET".to_owned(),
            args: vec![Value::Str("key".to_owned())],
        },
        Request {
            id: u32::MAX,
            command: "SUBSCRIBE".to_owned(),
            args: vec![Value::Bytes(vec![0, 1, 2, 255]), Value::Int(-1)],
        },
    ];

    let codec = FrameCodec::default();
    for msg in &messages {
        let thunder_frame = encode_frame(msg).unwrap();
        // The same body, framed by Fluxum instead.
        let body = &thunder_frame[FRAME_HEADER_LEN..];
        let fluxum_frame = codec.encode(body).unwrap();
        assert_eq!(
            fluxum_frame, thunder_frame,
            "framing diverged for command {}",
            msg.command
        );
    }
}

/// The family golden vector from Thunder's own suite (corpus request-ping),
/// asserted here so a change on either side breaks this test.
#[test]
fn family_golden_ping_vector_holds_on_the_fluxum_side() {
    const PING_FRAME: [u8; 12] = [
        0x08, 0x00, 0x00, 0x00, // length = 8, u32 LE
        0x93, 0x01, 0xa4, 0x50, 0x49, 0x4e, 0x47, 0x90, // [1, "PING", []]
    ];

    let codec = FrameCodec::default();
    assert_eq!(codec.encode(&PING_FRAME[FRAME_HEADER_LEN..]).unwrap(), PING_FRAME);

    let (frame, consumed) = codec.decode(&PING_FRAME).unwrap().unwrap();
    assert_eq!(consumed, PING_FRAME.len());
    assert_eq!(frame, Frame::Body(&PING_FRAME[FRAME_HEADER_LEN..]));
}

/// Fluxum decodes what Thunder encodes, including back-to-back frames sharing
/// one buffer — the streaming case where a prefix/slice disagreement would
/// desynchronize the whole connection rather than fail one message.
#[test]
fn fluxum_decodes_a_stream_of_thunder_frames() {
    let a = encode_frame(&Request {
        id: 1,
        command: "PING".to_owned(),
        args: vec![],
    })
    .unwrap();
    let b = encode_frame(&Request {
        id: 2,
        command: "QUIT".to_owned(),
        args: vec![],
    })
    .unwrap();

    let mut buf = a.clone();
    buf.extend_from_slice(&b);

    let codec = FrameCodec::default();
    let (first, used) = codec.decode(&buf).unwrap().unwrap();
    assert_eq!(used, a.len());
    assert_eq!(first, Frame::Body(&a[FRAME_HEADER_LEN..]));

    let (second, used2) = codec.decode(&buf[used..]).unwrap().unwrap();
    assert_eq!(used2, b.len());
    assert_eq!(second, Frame::Body(&b[FRAME_HEADER_LEN..]));
}

/// The one *intended* divergence, pinned so it stays intentional: Fluxum reads
/// a zero-length frame as a keep-alive (RPC-001/006), Thunder's typed decode
/// rejects it. Both SDKs work around this today; hivellm/thunder#6 asks the
/// family standard to define it instead.
#[test]
fn zero_length_frame_is_a_keepalive_here_and_an_error_in_thunder() {
    let keepalive = FrameCodec::keepalive();
    assert_eq!(keepalive, [0; FRAME_HEADER_LEN]);

    let (frame, consumed) = FrameCodec::default().decode(&keepalive).unwrap().unwrap();
    assert_eq!(frame, Frame::KeepAlive);
    assert_eq!(consumed, FRAME_HEADER_LEN);

    assert!(
        thunder::wire::decode_frame::<Request>(&keepalive).is_err(),
        "if Thunder starts accepting zero-length frames, drop Fluxum's workaround"
    );
}
