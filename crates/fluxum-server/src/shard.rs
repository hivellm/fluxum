//! ShardCoord + ShardHost (SPEC-007 §3/§4, T5.4): the multi-shard
//! composition over the per-shard [`ShardContext`] — partition routing,
//! the shard registry, `#[fluxum::table(global)]` replication, and the
//! drain half of the SHD-061 shutdown.
//!
//! # Deployment shape (OQ-2, decided)
//!
//! Every shard runs as a tokio task inside one process — its own
//! `MemStore` + `CommitLog` + `SubscriptionManager` + `TxPipeline` worker,
//! composed here. Shards share nothing but the router (SHD-020); a panic
//! or a saturated queue on one shard cannot touch another (the per-shard
//! pipeline already isolates both, T3.1).

use std::collections::BTreeMap;
use std::sync::Arc;

use fluxum_core::error::{FluxumError, Result};
use fluxum_core::reducer::{FluxValue, ReducerCaller};
use fluxum_core::schema::Schema;
use fluxum_core::shard::{ShardId, ShardRouter};
use fluxum_core::store::TxDiff;
use fluxum_core::txn::CommitReceipt;
use fluxum_core::types::Identity;

use crate::ShardContext;

/// One shard's host: its identity plus the fully-independent per-shard
/// context (SHD-020). The context owns the store, pipeline, subscriptions,
/// and fan-out; this wrapper adds the shard-registry identity.
pub struct ShardHost {
    /// The shard's stable id.
    pub shard_id: ShardId,
    /// The per-shard context (store + engine + subscriptions + fan-out).
    pub ctx: Arc<ShardContext>,
}

/// The global router (SHD-010/011): owns the partition map and the live
/// shard registry, routes reducer calls to the owning shard, and applies
/// global-table replication (SHD-030) synchronously after authoritative
/// commits.
pub struct ShardCoord {
    schema: Arc<Schema>,
    router: ShardRouter,
    hosts: BTreeMap<ShardId, Arc<ShardContext>>,
    shard_count: u32,
}

impl ShardCoord {
    /// Assemble the coordinator over independent shard hosts. Every shard
    /// except the router's authoritative one is marked a global-table
    /// replica (SHD-031: local reducer writes to global tables error).
    pub fn new(schema: Arc<Schema>, router: ShardRouter, hosts: Vec<ShardHost>) -> Result<Self> {
        if hosts.is_empty() {
            return Err(FluxumError::Storage(
                "ShardCoord needs at least one shard (SHD-010)".into(),
            ));
        }
        let authoritative = router.authoritative_global();
        let mut map = BTreeMap::new();
        for host in hosts {
            if host.shard_id != authoritative {
                host.ctx.store().set_global_replica();
            }
            if map.insert(host.shard_id, host.ctx).is_some() {
                return Err(FluxumError::Storage(format!(
                    "duplicate shard id {} in the registry (SHD-010)",
                    host.shard_id
                )));
            }
        }
        #[allow(clippy::cast_possible_truncation)] // registry sizes are tiny
        let shard_count = map.len() as u32;
        Ok(Self {
            schema,
            router,
            hosts: map,
            shard_count,
        })
    }

    /// The registered shard ids, ascending.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.hosts.keys().copied()
    }

    /// The host of `shard`, if registered.
    pub fn host(&self, shard: ShardId) -> Option<&Arc<ShardContext>> {
        self.hosts.get(&shard)
    }

    /// The shard a caller acquires affinity to (SHD-011): identity-hash
    /// for partitioned deployments, the default shard otherwise.
    pub fn affinity_of(&self, identity: &Identity) -> ShardId {
        let shard = self.router.affinity_of(identity, self.shard_count);
        if self.hosts.contains_key(&shard) {
            shard
        } else {
            // The registry is non-empty by construction (`new` rejects an
            // empty host list); fall back to the lowest shard id.
            self.hosts.keys().next().copied().unwrap_or_default()
        }
    }

    /// The routing seam for row placement (SHD-012): the shard owning a
    /// row of `table` — the table's partition strategy over the row's key
    /// columns, shard 0 for unpartitioned tables (SHD-004), the
    /// authoritative shard for global tables (SHD-030).
    pub fn shard_of_row(
        &self,
        table: fluxum_core::store::TableId,
        values: &[fluxum_core::store::RowValue],
    ) -> Result<ShardId> {
        self.router.shard_of_row(&self.schema, table, values)
    }

    /// Execute a reducer on `shard` and, when the commit touched
    /// `#[fluxum::table(global)]` tables, replicate those mutations to
    /// every other shard **before** returning (SHD-030: the committed
    /// write is readable on every shard before the `ReducerResult` does) —
    /// replicas apply to `CommittedState` directly, producing no commit-log
    /// entries; their subscribers still receive the fan-out.
    pub async fn call(
        &self,
        shard: ShardId,
        caller: ReducerCaller,
        reducer: &str,
        args: &[FluxValue],
    ) -> Result<CommitReceipt> {
        let host = self.hosts.get(&shard).ok_or_else(|| {
            FluxumError::Storage(format!("unknown shard {shard} (SHD-010)"))
        })?;
        let receipt = host.engine.call(caller, reducer, args.to_vec()).await?;
        self.replicate_globals(shard, &receipt.diff)?;
        Ok(receipt)
    }

    /// SHD-030: apply the global-table slice of `diff` to every shard but
    /// the originating one, synchronously, and fan it out to each replica's
    /// subscribers.
    pub fn replicate_globals(&self, origin: ShardId, diff: &TxDiff) -> Result<()> {
        let has_global = diff.tables.iter().any(|table| {
            self.schema
                .tables()
                .find(|t| fluxum_core::store::TableId::of(t.name) == table.table_id)
                .is_some_and(|t| t.access == fluxum_core::schema::TableAccess::Global)
        });
        if !has_global {
            return Ok(());
        }
        for (&shard, host) in &self.hosts {
            if shard == origin {
                continue;
            }
            host.store().apply_replicated_diff(diff)?;
            // Replica subscribers see the change like any commit; the
            // replica's log stays untouched (SHD-030).
            host.publish_commit(diff.clone());
        }
        Ok(())
    }

    /// The drain half of graceful shutdown (SHD-061 steps 1–2): submit a
    /// barrier job to every shard's FIFO single-writer queue and await it —
    /// when the barrier completes, every previously in-flight reducer has
    /// committed or rolled back. Snapshot + log close (steps 3–4) belong to
    /// the assembly that owns the repos.
    pub async fn drain(&self) -> Result<()> {
        for host in self.hosts.values() {
            host.engine
                .pipeline()
                .call(Box::new(|_tx| Ok(())))
                .await?;
        }
        Ok(())
    }
}
