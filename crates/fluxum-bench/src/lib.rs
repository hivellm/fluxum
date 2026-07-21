//! # fluxum-bench — comparative parity harness (T6.3)
//!
//! Permanent home of the PostgreSQL/SQLite parity harness mandated by PRD
//! NFR-11 and SPEC-013 §10 (TST-090..TST-096): the *same* application
//! implemented on Fluxum and on an app-server + database stack, run on equal
//! hardware with honest durability settings on both sides, producing the
//! comparative report published with every release.
//!
//! Layout:
//! - [`workload`] — the demo-app workloads (TST-092), written once against
//!   the [`workload::Side`] trait so every side runs identical client
//!   behavior;
//! - [`measure`] — latency/throughput reduction with multi-run variance
//!   (TST-091);
//! - [`fluxum_side`] — the Fluxum side, driven through the published Rust
//!   SDK against a real `fluxum-server`;
//! - [`baseline`] — the incumbent side: the same app on axum + sqlx over
//!   PostgreSQL (LISTEN/NOTIFY fan-out) or SQLite, served by its own
//!   process; [`baseline_side`] is the driver's HTTP/WS client for it;
//! - the report generator lands with the remaining T6.3 items.

pub mod baseline;
pub mod baseline_side;
pub mod fluxum_side;
pub mod measure;
pub mod workload;

/// Version of the parity harness, tied to the workspace version so reports can name the
/// exact Fluxum release they were produced for.
#[must_use]
pub fn harness_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::harness_version;

    #[test]
    fn harness_version_matches_workspace_version() {
        assert_eq!(harness_version(), "0.1.0");
    }
}
