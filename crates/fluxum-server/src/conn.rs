//! Connection fan-out plumbing: [`OutFrame`], [`ConnHandle`] and the
//! [`ConnectionRegistry`] (SUB-042 backpressure tiers) — split from
//! `lib.rs` to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

/// One encoded, framed message ready for a connection's socket.
/// One outbound frame plus its enqueue instant, so the writer can attribute
/// queue + task-wake latency (OBS-023 `queue_wait`). The bytes stay shared:
/// a fan-out clones the `Arc`, never the frame.
#[derive(Debug, Clone)]
pub struct OutFrame {
    /// When the sending side queued it.
    pub enqueued_at: Instant,
    /// The encoded frame bytes.
    pub bytes: Arc<Vec<u8>>,
    /// Bypass the RPC-008 compression transform for this frame. Set only on
    /// the `AuthResult` that *accepts* a compression negotiation: it is
    /// enqueued before the writer's transform arms, but a lagging writer may
    /// dequeue it after — the flag pins the spec's boundary ("compression
    /// takes effect with the first frame after the accepting AuthResult")
    /// against that race.
    pub raw: bool,
}

impl OutFrame {
    /// Wrap `bytes` stamped with the current instant.
    #[must_use]
    pub fn now(bytes: Arc<Vec<u8>>) -> Self {
        Self {
            enqueued_at: Instant::now(),
            bytes,
            raw: false,
        }
    }
}

/// One drain's worth of writer coalescing (F-018/OBS-024), shared by both
/// transports: at most this many frames per opportunistic drain, and at
/// most this many assembled bytes per socket write. An empty queue behaves
/// exactly as before — the drain takes what is ALREADY queued, never waits
/// for more, so batching is latency-neutral by construction.
pub(crate) const WRITE_COALESCE_FRAMES: usize = 64;
/// See [`WRITE_COALESCE_FRAMES`].
pub(crate) const WRITE_COALESCE_BYTES: usize = 256 * 1024;

/// Apply the RPC-008 per-connection send transform to one framed message:
/// strip the RPC-001 prefix, run the body through the connection's stream
/// context (or tag it `0x00` below `threshold`), and re-frame as
/// `u32 LE (1 + payload)` + tag + payload. Zero-length keep-alive frames
/// pass through untouched (they have no body to tag).
///
/// Returns the bytes to write, or `None` when the frame is a keep-alive
/// (write the original), and an error when the deflate context is broken —
/// connection-fatal, the stream cannot resynchronize.
pub(crate) fn wire_transform(
    compressor: &mut fluxum_protocol::StreamCompressor,
    framed: &[u8],
    threshold: usize,
    metrics: &fluxum_core::metrics::Metrics,
) -> Result<Option<Vec<u8>>, fluxum_protocol::CompressError> {
    let body = &framed[fluxum_protocol::FRAME_HEADER_LEN.min(framed.len())..];
    if body.is_empty() {
        return Ok(None); // keep-alive: no body, no tag (RPC-008)
    }
    let mut out;
    if body.len() >= threshold {
        let began = Instant::now();
        let chunk = compressor.compress_chunk(body)?;
        let cpu = u64::try_from(began.elapsed().as_micros()).unwrap_or(u64::MAX);
        metrics.note_wire_compression(
            body.len() as u64,
            u64::try_from(chunk.len()).unwrap_or(u64::MAX),
            cpu,
        );
        out = Vec::with_capacity(fluxum_protocol::FRAME_HEADER_LEN + 1 + chunk.len());
        let len = u32::try_from(1 + chunk.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.push(fluxum_protocol::TAG_GZIP_STREAM);
        out.extend_from_slice(&chunk);
    } else {
        // Below threshold: tagged uncompressed, outside the stream context.
        out = Vec::with_capacity(fluxum_protocol::FRAME_HEADER_LEN + 1 + body.len());
        let len = u32::try_from(1 + body.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.push(fluxum_protocol::TAG_UNCOMPRESSED);
        out.extend_from_slice(body);
    }
    Ok(Some(out))
}

/// A live connection's fan-out handle: a bounded outbound queue (drained by
/// the connection's writer task) plus a shutdown signal. A full queue is the
/// SUB-042 "Full" tier — the fan-out notifies shutdown and drops the
/// connection rather than ever blocking the commit path.
#[derive(Clone)]
pub struct ConnHandle {
    /// Outbound frame queue (bounded — the per-client send buffer, SUB-042).
    pub sink: mpsc::Sender<OutFrame>,
    /// Forces the connection to close (slow-consumer drop, SUB-042).
    pub shutdown: Arc<Notify>,
    /// The wire options this connection negotiated (RPC-008/RPC-035).
    /// Registration happens at authentication, after the options pinned, so
    /// the fan-out can partition a delivery group by form without a lookup
    /// — and `GET /sessions` can report the posture per connection.
    pub wire: fluxum_protocol::WireOptions,
}

/// Live connection registry: `connection_id` → its fan-out handle. The
/// fan-out task looks a subscriber up here to push a `TxUpdate` without ever
/// touching the connection's read/route path.
#[derive(Default)]
pub struct ConnectionRegistry {
    handles: Mutex<HashMap<u128, ConnHandle>>,
}

impl ConnectionRegistry {
    /// Register a connection's fan-out handle at authentication time.
    pub async fn insert(&self, connection_id: u128, handle: ConnHandle) {
        self.handles.lock().await.insert(connection_id, handle);
    }

    /// Remove a connection on disconnect.
    pub async fn remove(&self, connection_id: u128) {
        self.handles.lock().await.remove(&connection_id);
    }

    /// Outbound-queue occupancy per live connection, for the SEC-053 admin
    /// session listing: `(connection, queued frames, queue capacity)`. A
    /// queue near capacity is a slow consumer approaching the SUB-042 drop.
    pub async fn queue_depths(&self) -> Vec<(u128, usize, usize)> {
        let guard = self.handles.lock().await;
        guard
            .iter()
            .map(|(connection, handle)| {
                let capacity = handle.sink.max_capacity();
                let queued = capacity.saturating_sub(handle.sink.capacity());
                (*connection, queued, capacity)
            })
            .collect()
    }

    /// Per-connection negotiated wire options, for the SEC-053 session
    /// listing: `(connection, options)`.
    pub async fn wire_options(&self) -> Vec<(u128, fluxum_protocol::WireOptions)> {
        let guard = self.handles.lock().await;
        guard
            .iter()
            .map(|(connection, handle)| (*connection, handle.wire))
            .collect()
    }

    /// Handles for a set of subscriber ids (fan-out targets).
    pub(crate) async fn handles_for(&self, connections: &[u128]) -> Vec<(u128, ConnHandle)> {
        let guard = self.handles.lock().await;
        connections
            .iter()
            .filter_map(|conn| guard.get(conn).map(|h| (*conn, h.clone())))
            .collect()
    }
}
