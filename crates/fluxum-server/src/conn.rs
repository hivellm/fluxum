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
}

impl OutFrame {
    /// Wrap `bytes` stamped with the current instant.
    #[must_use]
    pub fn now(bytes: Arc<Vec<u8>>) -> Self {
        Self {
            enqueued_at: Instant::now(),
            bytes,
        }
    }
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

    /// Handles for a set of subscriber ids (fan-out targets).
    pub(crate) async fn handles_for(&self, connections: &[u128]) -> Vec<(u128, ConnHandle)> {
        let guard = self.handles.lock().await;
        connections
            .iter()
            .filter_map(|conn| guard.get(conn).map(|h| (*conn, h.clone())))
            .collect()
    }
}
