//! Fluxum pure wire layer (no storage deps): `FluxValue`, the FluxBIN row
//! codec, and FluxRPC framing + message types — shared with the SDKs.
//!
//! T0.1 skeleton crate; codec and message types land per [`docs/DAG.md`].

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-protocol");
    }
}
