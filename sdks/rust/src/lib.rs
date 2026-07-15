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

pub use fluxum_protocol as protocol;

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
