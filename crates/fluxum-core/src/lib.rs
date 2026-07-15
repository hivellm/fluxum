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
//!   T2.4); [`index::QuadTree`] / [`index::RTree`]: the SPEC-008 spatial
//!   indexes behind `#[spatial(...)]`, queried through
//!   [`index::SpatialPredicate`] (`IN REGION` / `WITHIN RADIUS`) with the
//!   400/503 error contract (T2.5/T2.6)
//! - [`simd`] — runtime-dispatched SIMD kernels with scalar oracles
//!   ([`simd::Dispatch`], SPEC-016 §5–§8, T2.10)
//! - [`txn`] — [`txn::TxPipeline`]: the per-shard transaction pipeline
//!   (validate → merge → append → respond), bounded reducer queue with
//!   immediate `503 "shard busy"` backpressure, panic-isolated rollback
//!   (SPEC-003, T3.1)
//! - [`reducer`] — [`reducer::ReducerContext`] + the typed
//!   [`reducer::TxHandle`] every reducer uses: committed-snapshot reads,
//!   explicit intra-transaction reads (`scan_pending`/`scan_all`, FR-17),
//!   and same-transaction nested reducer calls via
//!   [`reducer::ReducerRegistry`] (SPEC-004 §2, T3.2)
//! - [`migration`] — the SPEC-010 schema-migration runner
//!   ([`migration::MigrationRunner`]): `__schema_meta__` version tracking,
//!   ordered `#[fluxum::migration]` execution, automatic schema diff with
//!   safe auto-apply, and fail-closed aborts for incompatible changes
//!   (T3.6)
//! - [`scheduler`] — the SPEC-004 §4 scheduled-execution runtime
//!   ([`scheduler::Scheduler`]): `#[fluxum::tick]` fixed-timestep clocks
//!   and the durable `__schedule__` deferred-reducer worker (T3.4)

pub mod auth;
pub mod checkpoint;
pub mod commitlog;
pub mod config;
pub mod error;
pub mod hw;
pub mod index;
pub mod migration;
pub mod reducer;
pub mod scheduler;
pub mod schema;
pub mod simd;
pub mod store;
pub mod txn;
pub mod types;

pub use auth::{AuthClaims, AuthOutcome, AuthProvider, Authenticator};
pub use config::Config;
pub use error::{FluxumError, Result};
pub use reducer::{ReducerCaller, ReducerContext, ReducerRegistry, TxHandle};
pub use types::{ConnectionId, EntityId, Identity, Timestamp};

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(env!("CARGO_PKG_NAME"), "fluxum-core");
    }
}
