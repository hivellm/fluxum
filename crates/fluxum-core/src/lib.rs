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

pub mod auth;
pub mod config;
pub mod error;
pub mod hw;
pub mod schema;
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
