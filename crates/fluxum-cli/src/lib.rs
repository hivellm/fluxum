//! Fluxum CLI library: `generate`, schema export, backup, and admin
//! subcommand implementations backing the `fluxum` binary.
//!
//! T0.1 skeleton crate; subcommands land per [`docs/DAG.md`].

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-cli");
    }
}
