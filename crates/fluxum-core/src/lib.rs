//! Fluxum core: storage engine, transactions, indexes, reducer runtime,
//! subscriptions, sharding, and migration — no network dependencies.
//!
//! T0.2 foundation modules; further modules land per [`docs/DAG.md`] phase
//! order:
//!
//! - [`error`] — the workspace-wide [`FluxumError`] / [`Result`] pair
//! - [`types`] — [`Identity`], [`ConnectionId`], [`EntityId`], [`Timestamp`]
//! - [`config`] — layered YAML + `FLUXUM_*` env configuration ([`Config`])
//! - [`hw`] — boot-time hardware probe and adaptive-default derivation
//!   ([`hw::HardwareProfile`], [`hw::EffectiveConfig`])
//! - [`auth`] — pluggable [`AuthProvider`] trait, built-in `token`/`jwt`/
//!   `none` providers, server-peer registry ([`Authenticator`], SPEC-009)
//! - [`schema`] — [`schema::TableSchema`] introspection, the [`schema::Table`]
//!   trait, and the link-time registry behind `#[fluxum::table]` (SPEC-001)
//! - [`store`] — [`store::MemStore`]: MVCC committed/tx state, lock-free
//!   snapshot reads, single-writer commit/rollback (SPEC-002 §2, T2.1)
//! - [`index`] — [`index::BTreeIndex`]: secondary B-tree indexes over
//!   memcomparable keys, maintained inside the commit merge (SPEC-001 §5,
//!   T2.4)

pub mod auth;
pub mod config;
pub mod error;
pub mod hw;
pub mod index;
pub mod schema;
pub mod store;
pub mod types;

pub use auth::{AuthClaims, AuthOutcome, AuthProvider, Authenticator};
pub use config::Config;
pub use error::{FluxumError, Result};
pub use types::{ConnectionId, EntityId, Identity, Timestamp};

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-core");
    }
}
