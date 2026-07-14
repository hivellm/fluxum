//! Fluxum core: storage engine, transactions, indexes, reducer runtime,
//! subscriptions, sharding, and migration — no network dependencies.
//!
//! T0.1 skeleton crate; modules land per [`docs/DAG.md`] phase order.

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-core");
    }
}
