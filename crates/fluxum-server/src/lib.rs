//! Fluxum server presentation layer: FluxRPC TCP transport, Streamable HTTP
//! `/rpc` + admin endpoints, auth providers, metrics, and `ServerBuilder`.
//!
//! T0.1 skeleton crate; transports land per [`docs/DAG.md`] Phase 5.

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-server");
    }
}
