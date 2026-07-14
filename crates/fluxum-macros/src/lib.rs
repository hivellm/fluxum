//! Fluxum procedural macros: `#[fluxum::table]`, `#[fluxum::reducer]`,
//! `#[tick]`, `#[schedule]`, lifecycle hooks, `#[view]`, `#[procedure]`,
//! `#[migration]`.
//!
//! T0.1 skeleton crate; macros land per [`docs/DAG.md`] phase order.

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-macros");
    }
}
