//! Database namespaces (SPEC-025 §6, OPS-050/051): one binary hosting
//! several independent named databases.
//!
//! A namespace owns a complete database — its own [`MemStore`] + commit log
//! behind a [`ReducerEngine`], its own schema, its own subscription set, and
//! its own commit fan-out. Nothing is shared across namespaces except the
//! process: no transaction, subscription, or query can cross the boundary,
//! because a connection is *bound* to one namespace and every read and write
//! it makes is routed through that namespace's engine and subscription
//! manager. There is no code path that takes a namespace name at query time,
//! so cross-namespace access is not "forbidden and checked" — it is
//! unrepresentable.
//!
//! # The default namespace
//!
//! A server with no registered namespaces behaves exactly as before: the
//! [`ShardContext`]'s own engine/subscriptions/fan-out *are* the default
//! database, and a client that names no namespace on `Authenticate` lands
//! there. Named namespaces are additive, so single-database deployments and
//! every existing call site are untouched (OPS-050 is not a breaking change).
//!
//! # Attribution (OPS-051)
//!
//! Each namespace carries its own [`Metrics`] (its engine's), so
//! `fluxum_*` series are attributable per namespace — the admin `/metrics`
//! renders a named namespace's series with a `namespace` label. Storage and
//! backups are per-namespace by construction: each is opened over its own
//! store directory and commit log.
//!
//! [`MemStore`]: fluxum_core::store::MemStore
//! [`ShardContext`]: crate::ShardContext
//! [`Metrics`]: fluxum_core::metrics::Metrics

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast};

use fluxum_core::reducer::ReducerEngine;
use fluxum_core::store::{MemStore, TxDiff};
use fluxum_core::subscription::SubscriptionManager;
use fluxum_core::txn::CommitMeta;

/// The name of the implicit database a connection binds to when it names
/// none (OPS-050): the [`crate::ShardContext`]'s own engine/subscriptions.
pub const DEFAULT_NAMESPACE: &str = "default";

/// One named database hosted by this process (OPS-050): independent storage,
/// schema, subscriptions, and commit fan-out.
pub struct Namespace {
    name: String,
    engine: ReducerEngine,
    subscriptions: Mutex<SubscriptionManager>,
    commit_tx: broadcast::Sender<(std::time::Instant, Arc<TxDiff>, Arc<CommitMeta>)>,
    last_tx_id: AtomicU64,
    /// This tenant's resource ceilings and their live state (OPS-060).
    /// Unbounded unless [`Namespace::with_quotas`] set them.
    quotas: crate::quota::QuotaState,
}

impl std::fmt::Debug for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Namespace")
            .field("name", &self.name)
            .field("last_tx_id", &self.last_tx_id())
            .finish()
    }
}

impl Namespace {
    /// Assemble a namespace over its own engine and subscription manager.
    /// `commit_capacity` sizes its commit broadcast, exactly as the default
    /// database's.
    pub fn new(
        name: impl Into<String>,
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        commit_capacity: usize,
    ) -> Arc<Self> {
        Self::with_quotas(
            name,
            engine,
            subscriptions,
            commit_capacity,
            crate::quota::TenantQuotas::default(),
        )
    }

    /// [`Namespace::new`] with per-tenant resource ceilings (SPEC-025
    /// OPS-060). An all-`None` quota set is exactly [`Namespace::new`].
    pub fn with_quotas(
        name: impl Into<String>,
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        commit_capacity: usize,
        quotas: crate::quota::TenantQuotas,
    ) -> Arc<Self> {
        let (commit_tx, _) = broadcast::channel(commit_capacity.max(1));
        let ns = Arc::new(Self {
            name: name.into(),
            engine,
            subscriptions: Mutex::new(subscriptions),
            commit_tx,
            last_tx_id: AtomicU64::new(0),
            quotas: crate::quota::QuotaState::new(quotas),
        });
        // P0-A 1.3 (TXN-021 steps 9/10): this database's single writer
        // publishes every commit to *its own* fan-out at commit visibility —
        // tenant isolation is preserved because each namespace owns its
        // engine, pipeline, and broadcast. Weak breaks the ns → engine →
        // pipeline → hook → ns cycle.
        let hook_ns = Arc::downgrade(&ns);
        ns.engine
            .pipeline()
            .set_commit_hook(Box::new(move |diff, meta| {
                if let Some(ns) = hook_ns.upgrade() {
                    ns.publish_commit_meta(diff.clone(), meta.clone());
                }
            }));
        ns
    }

    /// This tenant's quota ceilings and live state (OPS-060/061).
    pub fn quotas(&self) -> &crate::quota::QuotaState {
        &self.quotas
    }

    /// The tenant's estimated in-memory footprint: rows × a coarse per-column
    /// width, the same gauge `fluxum_memstore_bytes` reports. Lock-free.
    pub fn memory_bytes(&self) -> u64 {
        let store = self.store();
        let snapshot = store.snapshot();
        let mut total = 0u64;
        for table in store.table_schemas() {
            let rows = snapshot
                .row_count(fluxum_core::store::TableId::of(table.name))
                .unwrap_or(0);
            let rows = u64::try_from(rows).unwrap_or(u64::MAX);
            let width = u64::try_from(table.columns.len()).unwrap_or(0) * 24;
            total = total.saturating_add(rows.saturating_mul(width));
        }
        total
    }

    /// The tenant's durable commit-log footprint, sampled through the quota
    /// state's short cache (stating the log per call would be silly for a
    /// figure that moves slowly).
    pub fn storage_bytes(&self) -> u64 {
        self.quotas
            .sampled_storage(|| self.engine.pipeline().log().disk_bytes().ok())
    }

    /// This namespace's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its reducer engine (and, through it, its pipeline, store and log).
    pub fn engine(&self) -> &ReducerEngine {
        &self.engine
    }

    /// Its subscription registry, behind the SUB-041 async mutex.
    pub fn subscriptions(&self) -> &Mutex<SubscriptionManager> {
        &self.subscriptions
    }

    /// Its store — the lock-free read surface.
    pub fn store(&self) -> &Arc<MemStore> {
        self.engine.pipeline().store()
    }

    /// Its `fluxum_*` metrics registry (OPS-051 per-namespace attribution).
    pub fn metrics(&self) -> &Arc<fluxum_core::metrics::Metrics> {
        self.engine.metrics()
    }

    /// Publish a committed diff to *this namespace's* fan-out. A diff never
    /// reaches another namespace's subscribers. The send instant rides
    /// along for the OBS-023 stage attribution. Anonymous provenance — the
    /// commit hook uses [`Namespace::publish_commit_meta`].
    pub fn publish_commit(&self, diff: TxDiff) {
        self.publish_commit_meta(diff, CommitMeta::anonymous());
    }

    /// [`Namespace::publish_commit`] carrying the commit's provenance, so
    /// the fan-out can stamp RPC-033 `reducer_name`/`caller` (SPEC-021
    /// CS-011).
    pub fn publish_commit_meta(&self, diff: TxDiff, meta: CommitMeta) {
        self.last_tx_id.fetch_max(diff.tx_id, Ordering::Relaxed);
        let _ = self
            .commit_tx
            .send((std::time::Instant::now(), Arc::new(diff), Arc::new(meta)));
    }

    /// A receiver for this namespace's commit broadcast (one per fan-out).
    pub fn subscribe_commits(
        &self,
    ) -> broadcast::Receiver<(std::time::Instant, Arc<TxDiff>, Arc<CommitMeta>)> {
        self.commit_tx.subscribe()
    }

    /// The highest committed `tx_id` published here.
    pub fn last_tx_id(&self) -> u64 {
        self.last_tx_id.load(Ordering::Relaxed)
    }
}
