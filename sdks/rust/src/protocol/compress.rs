//! RPC-008 stream decompression (the client half), vendored like the rest
//! of `src/protocol/` and compiled only under the `compression` feature.
//!
//! The server compresses its push stream as one raw-DEFLATE context per
//! ordered byte stream, each frame a sync-flushed chunk; frames tagged
//! `0x00` bypass the context on both sides. See the server crate's
//! `fluxum-protocol/src/compress.rs` for the full rationale.

use flate2::{Decompress, FlushDecompress, Status};

/// The RPC-008 per-frame compression tag: uncompressed, context untouched.
pub const TAG_UNCOMPRESSED: u8 = 0x00;
/// The RPC-008 per-frame compression tag: next chunk of the stream.
pub const TAG_GZIP_STREAM: u8 = 0x01;

/// Decompression failed. Always session-fatal: a stream context that
/// errored or overflowed cannot be resynchronized — reconnect.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
    /// The chunk is not valid deflate against the current window.
    #[error("inflate stream error: {0}")]
    Corrupt(String),
    /// Inflating would exceed the caller's ceiling (RPC-061 guards the
    /// *compressed* frame; this guards the expansion).
    #[error("inflated frame exceeds {max} bytes")]
    TooLarge {
        /// The ceiling that was exceeded.
        max: usize,
    },
}

/// The decompression half of one server→client stream (RPC-008).
#[derive(Debug)]
pub struct StreamDecompressor {
    ctx: Decompress,
}

impl Default for StreamDecompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamDecompressor {
    /// A fresh stream context (raw DEFLATE, matching the server's stream).
    #[must_use]
    pub fn new() -> Self {
        Self {
            ctx: Decompress::new(false),
        }
    }

    /// Inflate one sync-flushed chunk, bounded by `max_out`.
    pub fn inflate_chunk(
        &mut self,
        chunk: &[u8],
        max_out: usize,
    ) -> Result<Vec<u8>, DecompressError> {
        let mut out = Vec::with_capacity((chunk.len() * 4).min(max_out).max(64));
        let mut consumed = 0usize;
        loop {
            if out.len() == out.capacity() {
                if out.capacity() >= max_out {
                    return Err(DecompressError::TooLarge { max: max_out });
                }
                let grow = (out.capacity()).clamp(64, max_out - out.capacity());
                out.reserve(grow);
            }
            let before_in = self.ctx.total_in();
            let status = self
                .ctx
                .decompress_vec(&chunk[consumed..], &mut out, FlushDecompress::Sync)
                .map_err(|e| DecompressError::Corrupt(e.to_string()))?;
            consumed += usize::try_from(self.ctx.total_in() - before_in).unwrap_or(usize::MAX);
            if out.len() > max_out {
                return Err(DecompressError::TooLarge { max: max_out });
            }
            debug_assert!(
                status != Status::StreamEnd,
                "chunks are sync-flushed, never stream-final"
            );
            if consumed == chunk.len() && out.len() < out.capacity() {
                return Ok(out);
            }
        }
    }
}
