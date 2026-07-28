//! RPC-008 stream compression: one raw-DEFLATE context per ordered
//! server→client byte stream, each compressed frame a sync-flushed chunk.
//!
//! Per-frame compression cannot pay at realtime frame sizes — a ~90-byte
//! position update has too little self-redundancy. What repeats is *across*
//! frames: identities, table names, near-identical rows. A shared 32 KiB
//! sliding window turns those into back-references, which is why RPC-008
//! defines `gzip` as a connection-lifetime stream (the generic delta layer)
//! rather than N independent compressions.
//!
//! Sans-IO: the server's per-connection writer owns a [`StreamCompressor`];
//! an SDK's reader owns a [`StreamDecompressor`]. Frames tagged `0x00`
//! bypass both contexts entirely (RPC-008), so skipping small frames never
//! desynchronizes the window — only tag-`0x01` payloads pass through here.

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};

/// The RPC-008 per-frame compression tag: uncompressed, context untouched.
pub const TAG_UNCOMPRESSED: u8 = 0x00;
/// The RPC-008 per-frame compression tag: next chunk of the stream.
pub const TAG_GZIP_STREAM: u8 = 0x01;
/// The RPC-008 tag reserved for brotli (unimplemented — see RPC-008).
pub const TAG_BROTLI: u8 = 0x02;

/// Compression failed — deflate state corrupted (a bug, not an input
/// property: deflate accepts arbitrary bytes).
#[derive(Debug, thiserror::Error)]
#[error("deflate stream error: {0}")]
pub struct CompressError(String);

/// Decompression failed. Always connection-fatal for the caller: a stream
/// context that errored or overflowed cannot be resynchronized.
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

/// The compression half of one server→client stream (RPC-008).
#[derive(Debug)]
pub struct StreamCompressor {
    ctx: Compress,
}

impl Default for StreamCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamCompressor {
    /// A fresh stream context (fastest level: the win is the shared window,
    /// not the entropy stage, and this runs per connection on the push path).
    #[must_use]
    pub fn new() -> Self {
        Self {
            // `false` = raw DEFLATE (RFC 1951): no zlib header/trailer to
            // special-case at stream start, same bytes every SDK's stdlib
            // inflater accepts with a raw window.
            ctx: Compress::new(Compression::fast(), false),
        }
    }

    /// Compress `body` as the next sync-flushed chunk of this stream. The
    /// returned bytes end at a byte-aligned boundary (`00 00 FF FF` tail),
    /// so the peer can fully inflate the frame the moment it arrives.
    pub fn compress_chunk(&mut self, body: &[u8]) -> Result<Vec<u8>, CompressError> {
        let mut out = Vec::with_capacity(body.len() / 2 + 24);
        let mut consumed = 0usize;
        loop {
            if out.len() == out.capacity() {
                out.reserve(64.max(body.len() / 2));
            }
            let before_in = self.ctx.total_in();
            let status = self
                .ctx
                .compress_vec(&body[consumed..], &mut out, FlushCompress::Sync)
                .map_err(|e| CompressError(e.to_string()))?;
            consumed += usize::try_from(self.ctx.total_in() - before_in).unwrap_or(usize::MAX);
            debug_assert!(
                status != Status::StreamEnd,
                "sync flush never ends a stream"
            );
            // The flush is complete when deflate consumed everything and
            // still left spare output room (it writes all it can per call).
            if consumed == body.len() && out.len() < out.capacity() {
                return Ok(out);
            }
        }
    }
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
    /// A fresh stream context (raw DEFLATE, matching [`StreamCompressor`]).
    #[must_use]
    pub fn new() -> Self {
        Self {
            ctx: Decompress::new(false),
        }
    }

    /// Inflate one sync-flushed chunk, bounded by `max_out` (the RPC-061
    /// frame ceiling applies to the *inflated* body too — without the bound
    /// a hostile stream could expand without limit).
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_round_trips() {
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        let body = b"hello, subscription world".repeat(10);
        let chunk = c.compress_chunk(&body).unwrap();
        assert!(
            chunk.ends_with(&[0x00, 0x00, 0xFF, 0xFF]),
            "sync-flush tail"
        );
        let back = d.inflate_chunk(&chunk, 1 << 20).unwrap();
        assert_eq!(back, body);
    }

    #[test]
    fn the_window_carries_across_chunks() {
        // The whole point of stream mode: a frame repeating the previous
        // frame's bytes compresses to back-references. The second identical
        // frame must be dramatically smaller than the first.
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        let frame: Vec<u8> = (0u16..120).flat_map(u16::to_le_bytes).collect();
        let first = c.compress_chunk(&frame).unwrap();
        let second = c.compress_chunk(&frame).unwrap();
        assert!(
            second.len() * 4 < first.len(),
            "carryover missing: first {} bytes, second {} bytes",
            first.len(),
            second.len()
        );
        assert_eq!(d.inflate_chunk(&first, 1 << 20).unwrap(), frame);
        assert_eq!(d.inflate_chunk(&second, 1 << 20).unwrap(), frame);
    }

    #[test]
    fn many_small_frames_stream_like_a_session() {
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        for i in 0u32..500 {
            let body = format!(
                "{{\"player\":\"abcdef{:04}\",\"x\":{i},\"y\":{}}}",
                i % 7,
                i * 3
            );
            let chunk = c.compress_chunk(body.as_bytes()).unwrap();
            let back = d.inflate_chunk(&chunk, 1 << 16).unwrap();
            assert_eq!(back, body.as_bytes());
        }
    }

    #[test]
    fn an_empty_body_still_flushes_a_valid_chunk() {
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        let chunk = c.compress_chunk(b"").unwrap();
        assert!(!chunk.is_empty());
        assert_eq!(d.inflate_chunk(&chunk, 1 << 10).unwrap(), b"");
    }

    #[test]
    fn inflation_is_bounded() {
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        let body = vec![0u8; 100_000]; // hyper-compressible
        let chunk = c.compress_chunk(&body).unwrap();
        assert!(chunk.len() < 1_000, "premise: the chunk is tiny");
        let err = d.inflate_chunk(&chunk, 4096).unwrap_err();
        assert!(matches!(err, DecompressError::TooLarge { max: 4096 }));
    }

    #[test]
    fn garbage_is_a_corrupt_stream_not_a_panic() {
        let mut d = StreamDecompressor::new();
        // 0xFF opens a dynamic-huffman block with impossible code lengths.
        let err = d.inflate_chunk(&[0xFF; 32], 1 << 10).unwrap_err();
        assert!(matches!(err, DecompressError::Corrupt(_)));
    }

    #[test]
    fn a_large_frame_grows_the_buffer_to_fit() {
        let mut c = StreamCompressor::new();
        let mut d = StreamDecompressor::new();
        let body: Vec<u8> = (0..200_000u32).flat_map(u32::to_le_bytes).collect();
        let chunk = c.compress_chunk(&body).unwrap();
        let back = d.inflate_chunk(&chunk, body.len() + 1).unwrap();
        assert_eq!(back, body);
    }
}
