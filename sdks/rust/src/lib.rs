//! # fluxum-sdk — Fluxum Rust client SDK (home crate)
//!
//! Workspace slot for the Rust client SDK mandated by SPEC-011 (SDK-050): typed table
//! access, reducer calls, and live subscriptions speaking FluxRPC (`u32 LE` frame +
//! MessagePack envelope + FluxBIN rows) over TCP.
//!
//! The client connection, cache, and codegen surface land with DAG task **T6.2** (after
//! the gate-G5 wire freeze). Until then this crate pins the `sdks/rust` layout slot from
//! ROADMAP M0 and re-exports the shared wire layer so the quality gate covers the
//! SDK-facing protocol surface from day one (DAG T0.1, NFR-09).
//!
//! [`ResumeTracker`] is the exception: the resume bookkeeping SPEC-021 CS-020/CS-022
//! puts on the client is exactly what the gate-G5 wire freeze constrains, so it ships
//! ahead of the connection as a transport-free unit. T6.2 wires it to a real socket —
//! feed it each `InitialData`/`TxUpdate`, and ask it what to send on reconnect.

pub mod cache;
pub mod client;
pub mod idempotency;
pub mod protocol;
pub mod resume;

pub use cache::{RowCache, RowEvent, TableDiff, TableSchema};
pub use client::{Connection, Error as ClientError, RowListener};
pub use idempotency::{OfflineQueue, QueuedCall};
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
