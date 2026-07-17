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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use fluxum_core::error::{FluxumError, Result};
use fluxum_core::reducer::{FluxValue, ReducerCaller};
use fluxum_core::schema::Schema;
use fluxum_core::shard::{HANDOFF_TABLE, ShardId, ShardRouter, encode_entity_key};
use fluxum_core::store::{RowValue, TableId, TxDiff};
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

/// A reducer call held back while its entity's handoff is in flight
/// (SHD-044): delivered exactly once to whichever shard owns the entity
/// when the handoff settles (target on success, origin on abort).
struct QueuedCall {
    caller: ReducerCaller,
    reducer: String,
    args: Vec<FluxValue>,
    respond: tokio::sync::oneshot::Sender<Result<CommitReceipt>>,
}

/// A detected entity move: `(partition key, its FluxBIN identity, target
/// shard)` (SHD-040).
type EntityMove = (Vec<RowValue>, Vec<u8>, ShardId);

/// Handoff tuning + the test-only fault-injection hook (SHD-042
/// acceptance: inject a failure at each protocol step).
pub struct HandoffOptions {
    /// Import (steps 6–8) and cleanup (steps 9–10) attempt budget per
    /// handoff; exhausting it on import aborts the handoff (SHD-042).
    pub attempts: u32,
    /// Fail exactly one pipeline commit at this protocol step (5 =
    /// export+marker, 7 = import, 10 = cleanup); `-1` = no injection.
    pub fail_once_at: AtomicI64,
}

impl Default for HandoffOptions {
    fn default() -> Self {
        Self {
            attempts: 3,
            fail_once_at: AtomicI64::new(-1),
        }
    }
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
    handoff: HandoffOptions,
    /// Entities with a handoff in flight, keyed by the FluxBIN-encoded
    /// partition key; the value queues calls arriving mid-handoff
    /// (SHD-044).
    in_flight: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<QueuedCall>>>,
    handoffs_completed: AtomicU64,
    handoffs_aborted: AtomicU64,
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
            handoff: HandoffOptions::default(),
            in_flight: tokio::sync::Mutex::new(HashMap::new()),
            handoffs_completed: AtomicU64::new(0),
            handoffs_aborted: AtomicU64::new(0),
        })
    }

    /// Replace the handoff options (tests: retry budget, fault injection).
    #[must_use]
    pub fn with_handoff_options(mut self, options: HandoffOptions) -> Self {
        self.handoff = options;
        self
    }

    /// Arm the fault-injection hook: the next pipeline commit at protocol
    /// `step` (5 = export+marker, 7 = import, 10 = cleanup) fails once.
    pub fn fail_once_at(&self, step: i64) {
        self.handoff.fail_once_at.store(step, Ordering::SeqCst);
    }

    fn take_fail(&self, step: i64) -> bool {
        self.handoff
            .fail_once_at
            .compare_exchange(step, -1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Handoffs that reached step 11 (SHD-041).
    pub fn handoffs_completed(&self) -> u64 {
        self.handoffs_completed.load(Ordering::Relaxed)
    }

    /// Handoffs aborted after the retry budget (SHD-042).
    pub fn handoffs_aborted(&self) -> u64 {
        self.handoffs_aborted.load(Ordering::Relaxed)
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
        let receipt = self.call_raw(shard, caller, reducer, args).await?;
        // SHD-040: a committed row now resolving to another shard means the
        // entity moved — run the handoff before returning, so the client's
        // next call routes to a shard that already owns the row set.
        for (key, key_bytes, target) in self.detect_moves(shard, &receipt.diff)? {
            self.run_handoff(&key, key_bytes, shard, target).await;
        }
        Ok(receipt)
    }

    /// The routing entry for entity-keyed calls (SHD-011/044): resolve the
    /// owning shard from the partition key, or queue the call when that
    /// entity's handoff is in flight — queued calls execute exactly once,
    /// in arrival order, on whichever shard owns the entity afterwards.
    pub async fn call_entity(
        &self,
        key: &[RowValue],
        caller: ReducerCaller,
        reducer: &str,
        args: &[FluxValue],
    ) -> Result<CommitReceipt> {
        let key_bytes = encode_entity_key(key)?;
        let waiter = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(queue) = in_flight.get_mut(&key_bytes) {
                let (respond, waiter) = tokio::sync::oneshot::channel();
                queue.push(QueuedCall {
                    caller,
                    reducer: reducer.to_string(),
                    args: args.to_vec(),
                    respond,
                });
                Some(waiter)
            } else {
                None
            }
        };
        if let Some(waiter) = waiter {
            return waiter.await.map_err(|_| {
                FluxumError::Storage("handoff queue dropped the call (SHD-044)".into())
            })?;
        }
        let shard = self.router.shard_of_key(key)?;
        self.call(shard, caller, reducer, args).await
    }

    /// Execute on `shard` + replicate globals — no move detection (the
    /// handoff drain uses this to avoid unbounded recursion; a queued call
    /// that itself moves the entity is picked up by its own next call).
    async fn call_raw(
        &self,
        shard: ShardId,
        caller: ReducerCaller,
        reducer: &str,
        args: &[FluxValue],
    ) -> Result<CommitReceipt> {
        let host = self
            .hosts
            .get(&shard)
            .ok_or_else(|| FluxumError::Storage(format!("unknown shard {shard} (SHD-010)")))?;
        let receipt = host.engine.call(caller, reducer, args.to_vec()).await?;
        self.replicate_globals(shard, &receipt.diff)?;
        Ok(receipt)
    }

    /// SHD-040: scan a commit's inserts for partitioned rows whose key no
    /// longer resolves to the shard that committed them. One entry per
    /// distinct entity key.
    fn detect_moves(
        &self,
        origin: ShardId,
        diff: &TxDiff,
    ) -> Result<Vec<EntityMove>> {
        let mut moves: Vec<EntityMove> = Vec::new();
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for (table, key_ordinals) in self.router.partitioned_tables() {
            let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == table) else {
                continue;
            };
            for row in &table_diff.inserts {
                let key: Vec<RowValue> = key_ordinals
                    .iter()
                    .filter_map(|&ordinal| row.value(ordinal).cloned())
                    .collect();
                if key.len() != key_ordinals.len() {
                    continue;
                }
                let target = self.router.shard_of_key(&key)?;
                if target == origin || !self.hosts.contains_key(&target) {
                    continue;
                }
                let key_bytes = encode_entity_key(&key)?;
                if seen.insert(key_bytes.clone()) {
                    moves.push((key, key_bytes, target));
                }
            }
        }
        Ok(moves)
    }

    /// SHD-041/042/044: run one entity handoff — register the SHD-044
    /// queue, drive the 11-step protocol, then drain the queue to the
    /// entity's post-handoff owner (target on success, origin on abort).
    async fn run_handoff(
        &self,
        key: &[RowValue],
        key_bytes: Vec<u8>,
        origin: ShardId,
        target: ShardId,
    ) {
        {
            let mut in_flight = self.in_flight.lock().await;
            if in_flight.contains_key(&key_bytes) {
                return; // already migrating this entity
            }
            in_flight.insert(key_bytes.clone(), Vec::new());
        }
        let outcome = self.handoff_steps(key, &key_bytes, origin, target).await;
        let owner = if outcome.is_ok() { target } else { origin };
        if outcome.is_ok() {
            self.handoffs_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.handoffs_aborted.fetch_add(1, Ordering::Relaxed);
        }
        let queued = self
            .in_flight
            .lock()
            .await
            .remove(&key_bytes)
            .unwrap_or_default();
        for call in queued {
            let result = self
                .call_raw(owner, call.caller, &call.reducer, &call.args)
                .await;
            let _ = call.respond.send(result);
        }
    }

    /// The 11-step atomic handoff (SHD-041). Step numbers from SPEC-007 §6:
    /// 1 lock intent (the in-flight registration), 2–3 export the row set
    /// from A's committed state, 4–5 write the `__handoff__` marker and
    /// commit on A, 6–8 import the buffer on B (retried; exhaustion aborts,
    /// SHD-042), 9–10 delete the row set + marker on A (retried), 11
    /// release (the caller drains the queue).
    async fn handoff_steps(
        &self,
        key: &[RowValue],
        key_bytes: &[u8],
        origin: ShardId,
        target: ShardId,
    ) -> Result<()> {
        let origin_host =
            Arc::clone(self.hosts.get(&origin).ok_or_else(|| {
                FluxumError::Storage(format!("unknown shard {origin} (SHD-010)"))
            })?);
        let target_host =
            Arc::clone(self.hosts.get(&target).ok_or_else(|| {
                FluxumError::Storage(format!("unknown shard {target} (SHD-010)"))
            })?);
        let domain = self.router.partitioned_tables();
        let marker_table = TableId::of(HANDOFF_TABLE.name);
        let key_hex: String = key_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let attempts = self.handoff.attempts.max(1);

        // Steps 2–5: one commit on A — snapshot the row set + plant marker.
        let buffer_slot: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::default();
        {
            let fail = self.take_fail(5);
            let buffer_slot = Arc::clone(&buffer_slot);
            let domain = domain.clone();
            let key = key.to_vec();
            let key_hex = key_hex.clone();
            origin_host
                .engine
                .pipeline()
                .call(Box::new(move |tx| {
                    if fail {
                        return Err(FluxumError::Storage(
                            "injected handoff failure at step 5 (test)".into(),
                        ));
                    }
                    let buffer = tx.handoff_export(&domain, &key)?;
                    tx.upsert(
                        marker_table,
                        vec![RowValue::Str(key_hex), RowValue::Str("pending".into())],
                    )?;
                    *buffer_slot
                        .lock()
                        .map_err(|_| FluxumError::Storage("handoff buffer poisoned".into()))? =
                        Some(buffer);
                    Ok(())
                }))
                .await?;
        }
        let buffer = buffer_slot
            .lock()
            .map_err(|_| FluxumError::Storage("handoff buffer poisoned".into()))?
            .take()
            .ok_or_else(|| FluxumError::Storage("handoff export produced no buffer".into()))?;

        // Steps 6–8: import on B, retried; budget exhaustion aborts the
        // handoff — clear A's marker so the entity stays (whole) on A.
        let mut remaining = attempts;
        loop {
            let fail = self.take_fail(7);
            let buffer = buffer.clone();
            let result = target_host
                .engine
                .pipeline()
                .call(Box::new(move |tx| {
                    if fail {
                        return Err(FluxumError::Storage(
                            "injected handoff failure at step 7 (test)".into(),
                        ));
                    }
                    tx.handoff_import(&buffer)?;
                    Ok(())
                }))
                .await;
            match result {
                Ok(_) => break,
                Err(error) => {
                    remaining -= 1;
                    if remaining == 0 {
                        let key_hex = key_hex.clone();
                        origin_host
                            .engine
                            .pipeline()
                            .call(Box::new(move |tx| {
                                tx.delete(marker_table, &[RowValue::Str(key_hex)])?;
                                Ok(())
                            }))
                            .await?;
                        return Err(error);
                    }
                }
            }
        }

        // Steps 9–10: delete the row set + marker on A, retried (B already
        // owns the entity; cleanup must eventually succeed).
        let mut remaining = attempts;
        loop {
            let fail = self.take_fail(10);
            let domain = domain.clone();
            let key = key.to_vec();
            let key_hex = key_hex.clone();
            let result = origin_host
                .engine
                .pipeline()
                .call(Box::new(move |tx| {
                    if fail {
                        return Err(FluxumError::Storage(
                            "injected handoff failure at step 10 (test)".into(),
                        ));
                    }
                    tx.handoff_delete(&domain, &key)?;
                    tx.delete(marker_table, &[RowValue::Str(key_hex)])?;
                    Ok(())
                }))
                .await;
            match result {
                Ok(_) => return Ok(()),
                Err(error) => {
                    remaining -= 1;
                    if remaining == 0 {
                        return Err(error);
                    }
                }
            }
        }
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
            host.engine.pipeline().call(Box::new(|_tx| Ok(()))).await?;
        }
        Ok(())
    }
}
