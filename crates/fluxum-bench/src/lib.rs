//! # fluxum-bench — comparative parity harness (home crate)
//!
//! Permanent home of the PostgreSQL/SQLite parity harness mandated by PRD NFR-11 and
//! SPEC-013 §15 (TST-090..): the *same* application implemented on Fluxum and on an
//! app-server + PostgreSQL stack, run on equal hardware with honest durability settings
//! on both sides, producing the comparative report published with every release.
//!
//! The workload drivers, incumbent-stack implementations, and report generator land with
//! DAG task **T6.3** (gate G6). Until then this crate only pins the workspace slot so the
//! quality gate (fmt, clippy `-D warnings`, tests on 3 OSes) covers it from day one
//! (DAG T0.1, NFR-09).

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
