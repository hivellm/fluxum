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
//!   snapshot reads, single-writer commit/rollback (SPEC-002 §2, T2.1);
//!   [`store::pager`]: the paged cold tier — own page format with per-page
//!   CRC32C, clock-LRU buffer pool under `memory.budget`, paged evictable
//!   B-trees for data and indexes (SPEC-015, T2.8)
//! - [`commitlog`] — [`commitlog::CommitLog`]: append-only durability log
//!   with group-commit flush actor, rotation, replay, and non-destructive
//!   torn-tail quarantine (SPEC-002 §3/§5, T2.2)
//! - [`checkpoint`] — incremental content-addressed checkpoints
//!   ([`checkpoint::CheckpointRepo`], [`checkpoint::SnapshotWorker`]),
//!   checkpoint+replay recovery with fallback, and log truncation through
//!   an archival hook (SPEC-002 §4/§5, T2.3)
//! - [`index`] — [`index::BTreeIndex`]: secondary B-tree indexes over
//!   memcomparable keys, maintained inside the commit merge (SPEC-001 §5,
//!   T2.4); [`index::QuadTree`]: the SPEC-008 spatial point index behind
//!   `#[spatial(quadtree(x, y))]` (T2.5)
//! - [`simd`] — runtime-dispatched SIMD kernels with scalar oracles
//!   ([`simd::Dispatch`], SPEC-016 §5–§8, T2.10)

pub mod auth;
pub mod checkpoint;
pub mod commitlog;
pub mod config;
pub mod error;
pub mod hw;
pub mod index;
pub mod schema;
pub mod simd;
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
