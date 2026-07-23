//! # fluxum-sdk — Fluxum Rust client SDK
//!
//! The Rust client mandated by SPEC-011 (SDK-050): typed table access, reducer calls,
//! and live subscriptions speaking FluxRPC (`u32 LE` frame + MessagePack envelope +
//! FluxBIN rows) over raw TCP (`fluxum://host:15801`) or Streamable HTTP
//! (`http://host:15800`).
//!
//! The pieces, bottom up:
//!
//! - [`protocol`] — the vendored wire layer (byte-identical to the server's crate;
//!   `tests/protocol_sync.rs` enforces it).
//! - [`RowCache`] — the byte-keyed, reference-counted local row store (SDK-040/044),
//!   with net-difference reconnect reconciliation (SDK-047).
//! - [`SyncedCache`] — the cache plus the **optimistic overlay** (SPEC-021 CS-010..012):
//!   layered local mutations reconciled against authoritative updates without flicker.
//! - [`OfflineQueue`] — queued reducer calls under stable idempotency keys (CS-032),
//!   replayed exactly-once after an outage; snapshot/restore for durable persistence.
//! - [`persist`] — the opt-in durable local store (CS-040/CS-041): subscribed rows and
//!   the queue written through to a [`PersistenceBackend`], hydrated on restart and
//!   reconciled to the net difference.
//! - [`ResumeTracker`] — per-subscription applied offsets (CS-020/CS-022), driving the
//!   HTTP blip `Resume` instead of a full re-download.
//! - [`Connection`] — the blocking client an application holds: authenticate,
//!   [`Connection::subscribe`], [`Connection::call_reducer`] /
//!   [`Connection::call_reducer_async`] (write pipelining, SDK-032),
//!   [`Connection::call_optimistic`] for instant local application with offline replay,
//!   and [`Connection::connect_persistent`] for the durable variant.

pub mod cache;
pub mod client;
mod http;
pub mod idempotency;
pub mod optimistic;
pub mod persist;
pub mod protocol;
pub mod resume;

pub use cache::{RowCache, RowEvent, TableDiff, TableSchema, TableSnapshot};
pub use client::{
    Connection, Error as ClientError, PendingReducer, ReconnectPolicy, RejectedListener,
    RowListener,
};
pub use idempotency::{OfflineQueue, QueueSnapshot, QueuedCall};
pub use optimistic::{OptimisticOp, OptimisticStore, SyncedCache};
pub use persist::{
    ClientStore, FileBackend, MemoryBackend, PersistedMeta, PersistedQuery, PersistenceBackend,
};
pub use resume::{Reconnect, ResumeTracker};

// The vendored protocol files are byte-for-byte copies of the server-side
// crate, where these modules sit at the crate root and refer to each other as
// `crate::codes`, `crate::value`, and so on. Re-exporting them here makes
// those paths resolve inside this crate too, which is what lets the copies
// stay literal — a sync that had to rewrite paths could not be checked by
// comparing bytes.
pub(crate) use protocol::{codes, rowlist, tagged, value};

#[cfg(test)]
mod tests {
    use super::protocol::{FRAME_HEADER_LEN, FluxValue};

    #[test]
    fn wire_layer_is_reachable_through_the_sdk() {
        // The SDK speaks the HiveLLM wire standard: u32 LE length prefix.
        assert_eq!(FRAME_HEADER_LEN, 4);
        let v = FluxValue::I64(42);
        assert_eq!(v, FluxValue::I64(42));
    }
}
