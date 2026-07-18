//! Wire framing (SPEC-006 RPC-001, RPC-061) — the HiveLLM standard:
//! `u32 LE length prefix + MessagePack body`.
//!
//! ```text
//! ┌───────────────────┬────────────────────────────────────────────┐
//! │ length: u32 (LE)  │ body: MessagePack bytes (message envelope) │
//! └───────────────────┴────────────────────────────────────────────┘
//!      4 bytes                    `length` bytes
//! ```
//!
//! `length` counts the body only. A frame with `length = 0` is a keep-alive
//! frame (RPC-001/RPC-006): receivers ignore it. Frames whose `length`
//! exceeds `max_frame_bytes` (RPC-061, default 16 MB) are rejected — on
//! decode this fires from the header alone, before any body bytes arrive.
//!
//! The codec is sans-IO: [`FrameCodec::decode`] reads from a caller-owned
//! buffer and reports how many bytes it consumed, so the same code drives
//! TCP and Streamable HTTP.
//!
//! # Why this is hand-written and the TypeScript SDK's is not
//!
//! The framing above is the family standard (SPEC-001, `thunder`), so the
//! rule is to delegate to `thunder::wire` rather than keep a private copy.
//! The TypeScript SDK does exactly that, via Thunder's `FrameReader`. Rust
//! cannot yet: `thunder::wire` decodes a frame only by deserializing the
//! body into its own `Request`/`Response`, and exposes no borrowed-body
//! variant — which is precisely what the sans-IO API above needs, because
//! Fluxum's bodies are its own `[tag, payload]` catalog decoded from
//! borrowed slices. Asked upstream in hivellm/thunder#6; when it lands,
//! this module becomes a thin wrapper (keeping the 16 MB cap and [`Frame`])
//! rather than an implementation.
//!
//! Until then `tests/thunder_parity.rs` asserts these bytes against
//! `thunder::wire::encode_frame`, so the duplication cannot become a
//! divergence.

use crate::codes;

/// Size of the length prefix, in bytes.
pub const FRAME_HEADER_LEN: usize = 4;

/// Default `max_frame_bytes` (RPC-061): 16 MB.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;

/// Framing violations. Maps to wire error code 413 (`frame too large`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The frame length exceeds `max_frame_bytes` (RPC-061).
    #[error("frame too large: {len} bytes exceeds max_frame_bytes {max}")]
    TooLarge {
        /// Declared (decode) or actual (encode) body length.
        len: u64,
        /// The configured `max_frame_bytes`.
        max: u32,
    },
}

impl FrameError {
    /// The RPC-034 wire error code for this failure: 413.
    pub const fn code(&self) -> u16 {
        codes::PROTO_FRAME_TOO_LARGE
    }
}

/// One decoded frame, borrowing the body from the input buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame<'a> {
    /// `length = 0` — ignore (RPC-001).
    KeepAlive,
    /// A message envelope body (MessagePack bytes).
    Body(&'a [u8]),
}

/// Sans-IO frame encoder/decoder with `max_frame_bytes` enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameCodec {
    max_frame_bytes: u32,
}

impl FrameCodec {
    /// Codec enforcing the given `max_frame_bytes` (RPC-061).
    pub const fn new(max_frame_bytes: u32) -> Self {
        Self { max_frame_bytes }
    }

    /// The configured limit.
    pub const fn max_frame_bytes(&self) -> u32 {
        self.max_frame_bytes
    }

    /// The 4-byte keep-alive frame (`length = 0`).
    pub const fn keepalive() -> [u8; FRAME_HEADER_LEN] {
        [0; FRAME_HEADER_LEN]
    }

    /// Frame `body`, appending header + body to `out`.
    pub fn encode_into(&self, body: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
        let len = u32::try_from(body.len())
            .ok()
            .filter(|len| *len <= self.max_frame_bytes)
            .ok_or(FrameError::TooLarge {
                len: body.len() as u64,
                max: self.max_frame_bytes,
            })?;
        out.reserve(FRAME_HEADER_LEN + body.len());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(body);
        Ok(())
    }

    /// Frame `body` into a fresh buffer.
    pub fn encode(&self, body: &[u8]) -> Result<Vec<u8>, FrameError> {
        let mut out = Vec::new();
        self.encode_into(body, &mut out)?;
        Ok(out)
    }

    /// Decode the frame at the start of `buf`.
    ///
    /// - `Ok(None)` — `buf` does not yet hold a complete frame; read more
    ///   bytes and retry.
    /// - `Ok(Some((frame, consumed)))` — one frame plus the number of bytes
    ///   it occupied (header included); drain `consumed` bytes and repeat.
    /// - `Err(TooLarge)` — the declared length exceeds `max_frame_bytes`;
    ///   fires from the 4-byte header alone (RPC-061), respond with a 413
    ///   `Error` and close.
    pub fn decode<'a>(&self, buf: &'a [u8]) -> Result<Option<(Frame<'a>, usize)>, FrameError> {
        let Some(header) = buf.first_chunk::<FRAME_HEADER_LEN>() else {
            return Ok(None);
        };
        let len = u32::from_le_bytes(*header);
        if len > self.max_frame_bytes {
            return Err(FrameError::TooLarge {
                len: u64::from(len),
                max: self.max_frame_bytes,
            });
        }
        if len == 0 {
            return Ok(Some((Frame::KeepAlive, FRAME_HEADER_LEN)));
        }
        let len = len as usize;
        if buf.len() - FRAME_HEADER_LEN < len {
            return Ok(None);
        }
        let total = FRAME_HEADER_LEN + len;
        Ok(Some((Frame::Body(&buf[FRAME_HEADER_LEN..total]), total)))
    }
}

impl Default for FrameCodec {
    /// Codec with the RPC-061 default limit (16 MB).
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_BYTES)
    }
}
