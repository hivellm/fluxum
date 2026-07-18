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

/// The name of the implicit database a connection binds to when it names
/// none (OPS-050): the [`crate::ShardContext`]'s own engine/subscriptions.
pub const DEFAULT_NAMESPACE: &str = "default";

/// One named database hosted by this process (OPS-050): independent storage,
/// schema, subscriptions, and commit fan-out.
pub struct Namespace {
    name: String,
    engine: ReducerEngine,
    subscriptions: Mutex<SubscriptionManager>,
    commit_tx: broadcast::Sender<Arc<TxDiff>>,
    last_tx_id: AtomicU64,
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
        let (commit_tx, _) = broadcast::channel(commit_capacity.max(1));
        Arc::new(Self {
            name: name.into(),
            engine,
            subscriptions: Mutex::new(subscriptions),
            commit_tx,
            last_tx_id: AtomicU64::new(0),
        })
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
    /// reaches another namespace's subscribers.
    pub fn publish_commit(&self, diff: TxDiff) {
        self.last_tx_id.fetch_max(diff.tx_id, Ordering::Relaxed);
        let _ = self.commit_tx.send(Arc::new(diff));
    }

    /// A receiver for this namespace's commit broadcast (one per fan-out).
    pub fn subscribe_commits(&self) -> broadcast::Receiver<Arc<TxDiff>> {
        self.commit_tx.subscribe()
    }

    /// The highest committed `tx_id` published here.
    pub fn last_tx_id(&self) -> u64 {
        self.last_tx_id.load(Ordering::Relaxed)
    }
}
