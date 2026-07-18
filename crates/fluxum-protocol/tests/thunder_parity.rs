//! Byte-for-byte parity with the HiveLLM family wire layer (SPEC-001).
//!
//! Fluxum's framing *is* Thunder's framing — `u32 LE length prefix +
//! MessagePack body` (SPEC-006 RPC-001). Since Thunder 0.2.0 shipped
//! `decode_frame_raw` (hivellm/thunder#6), `FrameCodec::decode` delegates
//! outright, so decoding cannot diverge: there is only one implementation.
//!
//! What remains worth asserting is the seam. Encoding is still four lines in
//! `frame.rs`, because Thunder's `encode_frame` serializes a *value* while
//! Fluxum already holds encoded body bytes — so those four lines are checked
//! byte-for-byte against Thunder's encoder here. And the round trip is
//! checked end to end: Fluxum must read what Thunder writes, including
//! back-to-back frames in one buffer, where a prefix or slicing disagreement
//! would desynchronize a whole connection rather than fail one message.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{DEFAULT_MAX_FRAME_BYTES, FRAME_HEADER_LEN, Frame, FrameCodec, FrameError};
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
            args: vec![Value::Bytes(vec![0, 1, 2, 255].into()), Value::Int(-1)],
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

/// The keep-alive, which used to be Fluxum's private extension and is now
/// WIRE-024. Thunder's raw decode hands a zero-length frame back as an empty
/// body; naming it [`Frame::KeepAlive`] is all Fluxum still does.
#[test]
fn zero_length_frame_is_the_family_keepalive() {
    let keepalive = FrameCodec::keepalive();
    assert_eq!(keepalive, [0; FRAME_HEADER_LEN]);

    let (frame, consumed) = FrameCodec::default().decode(&keepalive).unwrap().unwrap();
    assert_eq!(frame, Frame::KeepAlive);
    assert_eq!(consumed, FRAME_HEADER_LEN);

    // The same bytes, straight from Thunder: a valid frame with no body.
    let (body, total) = thunder::wire::decode_frame_raw(&keepalive, 16 * 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(body.is_empty());
    assert_eq!(total, FRAME_HEADER_LEN);
}

/// Fluxum's 16 MB cap (RPC-061) overrides Thunder's 64 MiB default, and the
/// rejection still fires from the 4-byte prefix alone — the property that
/// keeps a hostile prefix from allocating anything.
#[test]
fn fluxum_cap_overrides_the_thunder_default_and_fires_on_the_prefix() {
    let codec = FrameCodec::default();
    let over = DEFAULT_MAX_FRAME_BYTES + 1;
    let prefix_only = over.to_le_bytes();

    let err = codec.decode(&prefix_only).unwrap_err();
    assert_eq!(
        err,
        FrameError::TooLarge {
            len: u64::from(over),
            max: DEFAULT_MAX_FRAME_BYTES,
        }
    );

    // Thunder's own default would have accepted this length.
    assert!(
        (over as usize) < thunder::wire::DEFAULT_MAX_FRAME_BYTES,
        "the test is only meaningful while Fluxum's cap is the stricter one"
    );
}
