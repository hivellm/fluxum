//! Frame codec edge cases and fuzz (T1.2): length prefix 0 (keep-alive),
//! at-max, over-max (RPC-061), truncation, and arbitrary garbage — the
//! decoder must never panic and must reject oversized frames from the
//! 4-byte header alone.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_protocol::{DEFAULT_MAX_FRAME_BYTES, FRAME_HEADER_LEN, Frame, FrameCodec, FrameError};
use proptest::prelude::*;

#[test]
fn codec_reports_its_configured_limit() {
    assert_eq!(FrameCodec::new(512).max_frame_bytes(), 512);
    assert_eq!(
        FrameCodec::default().max_frame_bytes(),
        DEFAULT_MAX_FRAME_BYTES
    );
}

#[test]
fn incomplete_header_needs_more_bytes() {
    let codec = FrameCodec::default();
    for len in 0..FRAME_HEADER_LEN {
        assert_eq!(
            codec.decode(&vec![0xFF; len]).unwrap(),
            None,
            "header len {len}"
        );
    }
}

#[test]
fn length_zero_is_keepalive() {
    let codec = FrameCodec::default();
    assert_eq!(FrameCodec::keepalive(), [0, 0, 0, 0]);
    // Alone…
    assert_eq!(
        codec.decode(&FrameCodec::keepalive()).unwrap(),
        Some((Frame::KeepAlive, FRAME_HEADER_LEN))
    );
    // …and followed by more data: only the 4 header bytes are consumed.
    let mut buf = FrameCodec::keepalive().to_vec();
    buf.extend_from_slice(&[9, 9, 9]);
    assert_eq!(
        codec.decode(&buf).unwrap(),
        Some((Frame::KeepAlive, FRAME_HEADER_LEN))
    );
}

#[test]
fn incomplete_body_needs_more_bytes() {
    let codec = FrameCodec::default();
    let framed = codec.encode(&[1, 2, 3, 4, 5]).unwrap();
    for cut in FRAME_HEADER_LEN..framed.len() {
        assert_eq!(codec.decode(&framed[..cut]).unwrap(), None, "cut at {cut}");
    }
    assert_eq!(
        codec.decode(&framed).unwrap(),
        Some((Frame::Body(&[1, 2, 3, 4, 5]), framed.len()))
    );
}

#[test]
fn length_at_max_is_accepted() {
    let codec = FrameCodec::new(8);
    let body = [7u8; 8];
    let framed = codec.encode(&body).unwrap();
    assert_eq!(
        codec.decode(&framed).unwrap(),
        Some((Frame::Body(&body[..]), FRAME_HEADER_LEN + 8))
    );
}

#[test]
fn length_over_max_is_rejected_from_header_alone() {
    let codec = FrameCodec::new(8);
    // Header declares 9 bytes; no body bytes present — 413 fires immediately,
    // the server never buffers an oversized frame.
    let header = 9u32.to_le_bytes();
    assert_eq!(
        codec.decode(&header),
        Err(FrameError::TooLarge { len: 9, max: 8 })
    );
    assert_eq!(FrameError::TooLarge { len: 9, max: 8 }.code(), 413);

    // Default codec: a u32::MAX length prefix is rejected the same way.
    let codec = FrameCodec::default();
    assert_eq!(
        codec.decode(&u32::MAX.to_le_bytes()),
        Err(FrameError::TooLarge {
            len: u64::from(u32::MAX),
            max: DEFAULT_MAX_FRAME_BYTES,
        })
    );
}

#[test]
fn length_prefix_max_with_permissive_codec_does_not_overflow() {
    // max_frame_bytes = u32::MAX: a u32::MAX-length header is legal and the
    // decoder just waits for the (absurd) body without arithmetic overflow.
    let codec = FrameCodec::new(u32::MAX);
    assert_eq!(codec.decode(&u32::MAX.to_le_bytes()).unwrap(), None);
}

#[test]
fn encode_over_max_is_rejected() {
    let codec = FrameCodec::new(4);
    assert_eq!(
        codec.encode(&[0; 5]),
        Err(FrameError::TooLarge { len: 5, max: 4 })
    );
    // Empty body encodes as the keep-alive frame (length = 0).
    assert_eq!(codec.encode(&[]).unwrap(), FrameCodec::keepalive());
}

#[test]
fn back_to_back_frames_stream_decode() {
    // Streamable HTTP carries frames concatenated back-to-back (RPC-004);
    // drive the sans-IO decoder the way a transport loop would.
    let codec = FrameCodec::default();
    let bodies: [&[u8]; 3] = [b"first", b"", b"third"];
    let mut stream = Vec::new();
    for body in bodies {
        codec.encode_into(body, &mut stream).unwrap();
    }

    let mut offset = 0;
    let mut decoded = Vec::new();
    while let Some((frame, consumed)) = codec.decode(&stream[offset..]).unwrap() {
        decoded.push(match frame {
            Frame::KeepAlive => Vec::new(),
            Frame::Body(bytes) => bytes.to_vec(),
        });
        offset += consumed;
    }
    assert_eq!(offset, stream.len());
    // The empty body surfaced as a keep-alive (length = 0 is the keep-alive
    // encoding by definition).
    assert_eq!(decoded, [b"first".to_vec(), Vec::new(), b"third".to_vec()]);
}

proptest! {
    /// Decoding arbitrary garbage never panics; on success the consumed count
    /// is in-bounds and body slices are real subslices.
    #[test]
    fn decode_arbitrary_bytes_never_panics(
        buf in prop::collection::vec(any::<u8>(), 0..4096),
        max in 0u32..=u32::MAX,
    ) {
        let codec = FrameCodec::new(max);
        match codec.decode(&buf) {
            Ok(Some((Frame::KeepAlive, consumed))) => prop_assert_eq!(consumed, FRAME_HEADER_LEN),
            Ok(Some((Frame::Body(body), consumed))) => {
                prop_assert!(consumed <= buf.len());
                prop_assert_eq!(consumed, FRAME_HEADER_LEN + body.len());
            }
            Ok(None) | Err(FrameError::TooLarge { .. }) => {}
        }
    }

    /// encode → decode is the identity for any body within the limit.
    #[test]
    fn frame_roundtrips(body in prop::collection::vec(any::<u8>(), 1..2048)) {
        let codec = FrameCodec::default();
        let framed = codec.encode(&body).unwrap();
        prop_assert_eq!(framed.len(), FRAME_HEADER_LEN + body.len());
        let (frame, consumed) = codec.decode(&framed).unwrap().unwrap();
        prop_assert_eq!(consumed, framed.len());
        prop_assert_eq!(frame, Frame::Body(&body[..]));
    }

    /// Any split point of a valid frame either yields the full frame (at the
    /// end) or a clean "need more bytes".
    #[test]
    fn partial_frames_never_yield_partial_bodies(
        body in prop::collection::vec(any::<u8>(), 1..256),
        cut_fraction in 0.0f64..1.0,
    ) {
        let codec = FrameCodec::default();
        let framed = codec.encode(&body).unwrap();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let cut = ((framed.len() as f64) * cut_fraction) as usize;
        let decoded = codec.decode(&framed[..cut]).unwrap();
        prop_assert!(decoded.is_none(), "cut {cut} of {} yielded a frame", framed.len());
    }
}
