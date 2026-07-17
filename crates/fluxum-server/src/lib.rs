//! Fluxum server presentation layer (SPEC-006): the FluxRPC TCP transport
//! (:15801), the per-connection session state machine, message routing, and
//! the post-commit `TxUpdate` fan-out onto subscribed connections.
//!
//! # Layers
//!
//! - [`ShardContext`] — the shared per-shard state a connection needs: the
//!   [`ReducerEngine`](fluxum_core::reducer::ReducerEngine), the
//!   [`SubscriptionManager`](fluxum_core::subscription::SubscriptionManager)
//!   behind its SUB-041 async mutex, the
//!   [`Authenticator`](fluxum_core::auth::Authenticator), a connection
//!   registry, and a commit broadcast that drives live updates.
//! - [`session`] — the sans-socket router: turns one decoded
//!   [`ClientMessage`](fluxum_protocol::ClientMessage) into the
//!   [`ServerMessage`](fluxum_protocol::ServerMessage)s to send back,
//!   enforcing the pre-auth `401` gate (AUTH-020) and the SPEC-006 error
//!   mapping. Independent of any socket, so it is unit-testable directly.
//! - [`tcp`] — the tokio listener that drives sessions over real sockets:
//!   frame decode with the RPC-061 size limit (`413`), the RPC-060 idle
//!   timeout (`408`), a per-connection writer that multiplexes responses by
//!   echoed id (RPC-002), and the fan-out task that pushes `TxUpdate`s.

pub mod admin;
pub mod http;
pub mod session;
pub mod tcp;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, Notify, broadcast, mpsc};

use fluxum_core::auth::Authenticator;
use fluxum_core::reducer::{ReducerEngine, ViewRegistry};
use fluxum_core::store::{MemStore, TxDiff};
use fluxum_core::subscription::SubscriptionManager;
use fluxum_core::types::Identity;

/// One encoded, framed message ready for a connection's socket.
pub type OutFrame = Arc<Vec<u8>>;

/// A live connection's fan-out handle: a bounded outbound queue (drained by
/// the connection's writer task) plus a shutdown signal. A full queue is the
/// SUB-042 "Full" tier — the fan-out notifies shutdown and drops the
/// connection rather than ever blocking the commit path.
#[derive(Clone)]
pub struct ConnHandle {
    /// Outbound frame queue (bounded — the per-client send buffer, SUB-042).
    pub sink: mpsc::Sender<OutFrame>,
    /// Forces the connection to close (slow-consumer drop, SUB-042).
    pub shutdown: Arc<Notify>,
}

/// Live connection registry: `connection_id` → its fan-out handle. The
/// fan-out task looks a subscriber up here to push a `TxUpdate` without ever
/// touching the connection's read/route path.
#[derive(Default)]
pub struct ConnectionRegistry {
    handles: Mutex<HashMap<u128, ConnHandle>>,
}

impl ConnectionRegistry {
    /// Register a connection's fan-out handle at authentication time.
    pub async fn insert(&self, connection_id: u128, handle: ConnHandle) {
        self.handles.lock().await.insert(connection_id, handle);
    }

    /// Remove a connection on disconnect.
    pub async fn remove(&self, connection_id: u128) {
        self.handles.lock().await.remove(&connection_id);
    }

    /// Handles for a set of subscriber ids (fan-out targets).
    async fn handles_for(&self, connections: &[u128]) -> Vec<(u128, ConnHandle)> {
        let guard = self.handles.lock().await;
        connections
            .iter()
            .filter_map(|conn| guard.get(conn).map(|h| (*conn, h.clone())))
            .collect()
    }
}

/// The shared per-shard state every connection session reads from (SPEC-006
/// server assembly; the full multi-shard `ShardHost` is T5.4).
pub struct ShardContext {
    /// The reducer engine (admission + dispatch through the T3.1 pipeline).
    pub engine: ReducerEngine,
    /// The subscription registry + fan-out, behind the SUB-041 async mutex.
    pub subscriptions: Mutex<SubscriptionManager>,
    /// The single authentication entry point (AUTH-020/021).
    pub authenticator: Authenticator,
    /// Live connections, for the commit fan-out.
    pub connections: ConnectionRegistry,
    /// The `#[fluxum::view]` registry for the HTTP admin `GET /view/:name`
    /// (RED-030). Empty unless the assembly installs views.
    pub views: ViewRegistry,
    /// This shard's id (carried in every `ReducerCaller`).
    pub shard_id: u32,
    /// The server (admin) identity every HTTP admin call runs under
    /// (bypasses RLS, AUTH-062) — admin tooling is a trusted operator.
    pub admin_identity: Identity,
    /// Broadcast of every committed [`TxDiff`]; the fan-out task evaluates
    /// subscriptions against each and pushes `TxUpdate`s (SUB-021).
    commit_tx: broadcast::Sender<Arc<TxDiff>>,
    /// Monotonic `ConnectionId` allocator (ephemeral, never reused within a
    /// process; `0` is reserved for scheduled/system callers, RED-025).
    next_connection_id: AtomicU64,
    /// Last committed `tx_id` (atomic, for the lock-free `/health` — RPC-053
    /// forbids taking storage locks on the health path).
    last_tx_id: AtomicU64,
    /// Whether the DMX-011 ephemeral TTL sweeper has been spawned (both
    /// transports request it on serve; only the first call spawns).
    sweeper_started: std::sync::atomic::AtomicBool,
    /// Whether the DMX-020 row-TTL sweeper has been spawned (idempotent, as
    /// above).
    ttl_sweeper_started: std::sync::atomic::AtomicBool,
    /// The shard's blob store (SPEC-023 DMX-040), once installed.
    blob_store: std::sync::OnceLock<Arc<fluxum_core::commitlog::BlobStore>>,
}

/// A lock-free health snapshot (RPC-053 / OBS-060): read from atomics only,
/// never touching a storage lock, so `/health` answers in < 50 ms even
/// under sustained write load.
#[derive(Debug, Clone, Copy)]
pub struct Health {
    /// This shard's id.
    pub shard_id: u32,
    /// Last committed transaction id (`0` before the first commit).
    pub last_tx_id: u64,
}

impl ShardContext {
    /// Assemble a shard context. `commit_capacity` bounds the commit
    /// broadcast backlog (a slow fan-out task lags, never blocks commits).
    pub fn new(
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        authenticator: Authenticator,
        shard_id: u32,
        commit_capacity: usize,
    ) -> Arc<Self> {
        Self::with_views(
            engine,
            subscriptions,
            authenticator,
            ViewRegistry::new(),
            shard_id,
            commit_capacity,
        )
    }

    /// [`ShardContext::new`] with a `#[fluxum::view]` registry installed.
    pub fn with_views(
        engine: ReducerEngine,
        subscriptions: SubscriptionManager,
        authenticator: Authenticator,
        views: ViewRegistry,
        shard_id: u32,
        commit_capacity: usize,
    ) -> Arc<Self> {
        let (commit_tx, _) = broadcast::channel(commit_capacity.max(1));
        let admin_identity = fluxum_core::auth::server_identity("__admin__");
        Arc::new(Self {
            engine,
            subscriptions: Mutex::new(subscriptions),
            authenticator,
            connections: ConnectionRegistry::default(),
            views,
            shard_id,
            admin_identity,
            commit_tx,
            next_connection_id: AtomicU64::new(1),
            last_tx_id: AtomicU64::new(0),
            sweeper_started: std::sync::atomic::AtomicBool::new(false),
            ttl_sweeper_started: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::OnceLock::new(),
        })
    }

    /// Install the shard's blob store (SPEC-023 DMX-040): attaches it to the
    /// store (write validation + commit refcounts, rebuilding counts from
    /// the current snapshot) and enables the `/blob` HTTP endpoints. Call
    /// after recovery, before serving. A second call is ignored.
    pub fn set_blob_store(&self, blobs: Arc<fluxum_core::commitlog::BlobStore>) {
        self.store().attach_blob_store(Arc::clone(&blobs));
        let _ = self.blob_store.set(blobs);
    }

    /// The installed blob store, if any.
    pub fn blob_store(&self) -> Option<&Arc<fluxum_core::commitlog::BlobStore>> {
        self.blob_store.get()
    }

    /// Start the ephemeral TTL sweeper (SPEC-023 DMX-011) if any registered
    /// ephemeral table declares `expire_after`. Idempotent — both transports
    /// call this on serve; only the first call spawns. The sweep's delete
    /// diffs are published to the shard fan-out like any commit.
    pub fn start_ephemeral_sweeper(self: &Arc<Self>) {
        if self
            .sweeper_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let Some(sweeper) = fluxum_core::scheduler::EphemeralSweeper::from_registered(
            self.engine.pipeline().clone(),
        ) else {
            return;
        };
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            let cadence = sweeper.cadence();
            loop {
                tokio::time::sleep(cadence).await;
                match sweeper.sweep_once().await {
                    Ok(Some(receipt)) => ctx.publish_commit(receipt.diff),
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(target: "fluxum::server", error = %e, "ephemeral sweep failed");
                    }
                }
            }
        });
    }

    /// Start the row-TTL sweeper (SPEC-023 DMX-020) if any registered table
    /// declares `#[ttl(...)]`. Idempotent (only the first call spawns). A
    /// backlog that hits the batch cap keeps sweeping without the full cadence
    /// wait, so a mass expiry drains promptly without one giant delete (DMX-021);
    /// its delete diffs fan out like any commit.
    pub fn start_ttl_sweeper(self: &Arc<Self>) {
        if self
            .ttl_sweeper_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let Some(sweeper) =
            fluxum_core::scheduler::TtlSweeper::from_registered(self.engine.pipeline().clone())
        else {
            return;
        };
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            let cadence = sweeper.cadence();
            loop {
                tokio::time::sleep(cadence).await;
                // Drain the backlog: keep sweeping while a pass hits the cap.
                loop {
                    match sweeper.sweep_once().await {
                        Ok((receipt, more)) => {
                            if let Some(receipt) = receipt {
                                ctx.publish_commit(receipt.diff);
                            }
                            if !more {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(target: "fluxum::server", error = %e, "row-TTL sweep failed");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// A lock-free health snapshot (RPC-053): reads only atomics.
    pub fn health(&self) -> Health {
        Health {
            shard_id: self.shard_id,
            last_tx_id: self.last_tx_id.load(Ordering::Relaxed),
        }
    }

    /// The shard's committed store (lock-free snapshots for InitialData /
    /// one-off queries).
    pub fn store(&self) -> &Arc<MemStore> {
        self.engine.pipeline().store()
    }

    /// Allocate the next ephemeral `ConnectionId` (RPC-002).
    pub fn allocate_connection_id(&self) -> u128 {
        u128::from(self.next_connection_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Publish a committed diff to the fan-out (called after a reducer
    /// commit). A lagging fan-out drops old diffs rather than block the
    /// commit path — clients recover missed updates on reconnect via the
    /// `tx_id` gap (SPEC-006 acceptance 14).
    pub fn publish_commit(&self, diff: TxDiff) {
        self.last_tx_id.fetch_max(diff.tx_id, Ordering::Relaxed);
        let _ = self.commit_tx.send(Arc::new(diff));
    }

    /// A receiver for the commit broadcast (one per fan-out task).
    pub fn subscribe_commits(&self) -> broadcast::Receiver<Arc<TxDiff>> {
        self.commit_tx.subscribe()
    }
}

/// Spawn the shard-wide commit fan-out task (SUB-021/024): evaluate each
/// committed diff against the subscription manager once (mutex held only
/// across evaluation, SUB-041) and push the shared, once-encoded `TxUpdate`
/// frame to every subscriber's queue, dropping a slow consumer on a full
/// queue (SUB-042).
///
/// A standalone `tcp::serve` / `http::serve` spawns one so a single-transport
/// deployment works out of the box. The combined multi-transport assembly
/// (the T5.4 `ShardHost`) instead spawns exactly one and starts each
/// transport without its own — two fan-out tasks over one broadcast would
/// double-deliver to a subscriber registered in the shared registry.
pub(crate) fn spawn_fanout(ctx: Arc<ShardContext>, shutdown: Arc<Notify>) {
    use fluxum_protocol::{FrameCodec, ServerMessage};

    tokio::spawn(async move {
        let mut commits = ctx.subscribe_commits();
        let codec = FrameCodec::default();
        loop {
            let diff = tokio::select! {
                _ = shutdown.notified() => break,
                recv = commits.recv() => match recv {
                    Ok(diff) => diff,
                    // Lagged: the fan-out fell behind; clients recover on
                    // reconnect via the tx_id gap (SPEC-006 acceptance 14).
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            };

            // Evaluate once (SUB-041: mutex held only across evaluation).
            let deltas = {
                let manager = ctx.subscriptions.lock().await;
                match manager.on_commit(&diff) {
                    Ok(deltas) => deltas,
                    Err(e) => {
                        tracing::error!(target: "fluxum::fanout", error = %e,
                            "fan-out evaluation failed");
                        continue;
                    }
                }
            };

            for delta in deltas {
                let tx_update = SubscriptionManager::tx_update(&diff, &delta);
                let body = match ServerMessage::TxUpdate(tx_update).encode() {
                    Ok(body) => body,
                    Err(_) => continue,
                };
                let Ok(framed) = codec.encode(&body) else {
                    continue;
                };
                let frame: OutFrame = Arc::new(framed);
                for (conn_id, handle) in ctx.connections.handles_for(&delta.subscribers).await {
                    match handle.sink.try_send(Arc::clone(&frame)) {
                        Ok(()) => {}
                        // SUB-042 Full tier: never block — drop the consumer.
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(target: "fluxum::fanout", connection = conn_id,
                                "subscriber dropped: send buffer full");
                            handle.shutdown.notify_waiters();
                            ctx.connections.remove(conn_id).await;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            ctx.connections.remove(conn_id).await;
                        }
                    }
                }
            }
        }
    });
}
