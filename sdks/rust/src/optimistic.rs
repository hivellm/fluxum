//! Optimistic mutations with server reconciliation (SPEC-021 CS-010..012).
//!
//! An optimistic mutation shows its effect immediately and must later be
//! *reconciled*: replaced by the authoritative rows when the server confirms,
//! or rolled back when it rejects. The naive implementation — write the
//! optimistic rows into the cache and patch them later — cannot roll back
//! correctly once authoritative updates have interleaved. This module keeps
//! the two worlds separate instead:
//!
//! - the **base** is the authoritative [`RowCache`], mutated only by server
//!   data, exactly as before;
//! - each in-flight optimistic call is an **overlay layer**: an ordered list
//!   of upserts/deletes by primary key, applied over the base in submission
//!   order (CS-012).
//!
//! What the application sees — `rows()`, row callbacks — is the *effective
//! view*: base plus layers. Every state transition (a `TxUpdate` applying, a
//! layer being added, confirmed, or rolled back) recomputes the effective
//! view of the touched tables and emits the **net difference** as one atomic
//! event batch. That construction is what delivers the CS-011 guarantees: the
//! optimistic→authoritative swap is an `Update` (or nothing, when the bytes
//! match) rather than a delete/insert flicker, and a rollback restores
//! exactly the pre-mutation view. A rolled-back layer is *gone* — a later
//! authoritative update diffs against a view that no longer contains it, so
//! its rows cannot be resurrected (CS-012).
//!
//! # When a layer is dropped
//!
//! The authoritative rows for a call arrive in the `TxUpdate` its commit
//! broadcast — but only if the caller's own subscriptions cover the affected
//! rows, and possibly before or after the `ReducerResult` ack. Detection is
//! entirely client-side (the wire is unchanged):
//!
//! - a `TxUpdate` whose `caller` is this client and whose `reducer_name`
//!   matches is attributed FIFO to the oldest live layer for that reducer —
//!   commits on one connection happen in submission order — and drops it *in
//!   the same transition* that applies the authoritative rows, which is what
//!   makes the swap flicker-free;
//! - the `ReducerResult::Ok` ack drops a still-live layer only when holding
//!   it would be pointless: its ops are already shadowed by the base (the
//!   update landed under another attribution), or the client holds no
//!   subscriptions at all, so no update will ever come. Otherwise the layer
//!   holds until its update lands.
//!
//! Dropping a layer also drops every *older* confirmed layer — same-shard
//! commits are ordered, so an older call's update cannot still be in flight
//! once a newer call's has applied. `ReducerResult::Err` rolls the layer
//! back on the spot: a failed call commits nothing and broadcasts nothing.
//!
//! The FIFO attribution is per `(caller identity, reducer)`. Another
//! connection sharing this identity and calling the same reducer can steal a
//! match and drop a layer one update early — the cost is a transient
//! re-render, never divergence, and single-connection clients (the norm) are
//! exact.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::cache::{RowCache, RowEvent, TableDiff, TableSchema, TableSnapshot};

/// One local mutation recorded by an optimistic updater (CS-010).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptimisticOp {
    /// Insert or replace the row whose primary key `row` projects to.
    Insert {
        /// Table name.
        table: String,
        /// Full row bytes, in the table's wire layout.
        row: Vec<u8>,
    },
    /// Remove the row under this primary key, if visible.
    Delete {
        /// Table name.
        table: String,
        /// The primary-key bytes (as [`TableSchema::pk_of_row`] projects them).
        pk: Vec<u8>,
    },
}

impl OptimisticOp {
    fn table(&self) -> &str {
        match self {
            OptimisticOp::Insert { table, .. } | OptimisticOp::Delete { table, .. } => table,
        }
    }
}

/// The local store handed to an optimistic updater (CS-010): read the
/// effective view, record upserts and deletes. The recorded ops become one
/// overlay layer, applied atomically.
pub struct OptimisticStore<'a> {
    cache: &'a SyncedCache,
    ops: Vec<OptimisticOp>,
}

impl OptimisticStore<'_> {
    /// The effective rows of `table` as the mutation begins — base plus every
    /// overlay layer already in flight (earlier ops recorded on this store
    /// are not yet reflected; a layer applies atomically).
    pub fn rows(&self, table: &str) -> Vec<Vec<u8>> {
        self.cache.rows(table)
    }

    /// Record an insert-or-replace of `row` (upsert by primary key).
    pub fn insert(&mut self, table: impl Into<String>, row: Vec<u8>) {
        self.ops.push(OptimisticOp::Insert {
            table: table.into(),
            row,
        });
    }

    /// Record a delete of the row under `pk`.
    pub fn delete(&mut self, table: impl Into<String>, pk: Vec<u8>) {
        self.ops.push(OptimisticOp::Delete {
            table: table.into(),
            pk,
        });
    }
}

/// One in-flight optimistic call's overlay.
struct Layer {
    id: u64,
    reducer: String,
    ops: Vec<OptimisticOp>,
    /// `ReducerResult::Ok` received; held only until its update applies.
    confirmed: bool,
}

/// The authoritative [`RowCache`] plus the ordered optimistic overlay
/// (CS-010/CS-012) — the store a resilient client keeps behind its lock.
///
/// Every mutating method returns the app-facing events of that transition,
/// computed against the *effective* view. When no layers are in flight every
/// path short-circuits to the plain [`RowCache`] behaviour, so a client that
/// never calls anything optimistic pays nothing.
pub struct SyncedCache {
    base: RowCache,
    layers: Vec<Layer>,
    next_layer: u64,
}

impl SyncedCache {
    /// Build over a set of table schemas (same contract as [`RowCache::new`]).
    pub fn new(schemas: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            base: RowCache::new(schemas),
            layers: Vec::new(),
            next_layer: 1,
        }
    }

    /// The authoritative cache, untouched by any overlay.
    pub fn authoritative(&self) -> &RowCache {
        &self.base
    }

    /// How many optimistic layers are currently in flight.
    pub fn optimistic_len(&self) -> usize {
        self.layers.len()
    }

    /// Effective rows for `table`: the base rows in insertion order with the
    /// overlay applied — replaced rows stay in place, overlay-new rows append
    /// in submission order (CS-012).
    pub fn rows(&self, table: &str) -> Vec<Vec<u8>> {
        if self.layers.is_empty() {
            return self.base.rows(table);
        }
        let mut view: Vec<Option<(Vec<u8>, Vec<u8>)>> = self
            .base
            .pk_rows(table)
            .into_iter()
            .map(Some)
            .collect();
        let mut index: HashMap<Vec<u8>, usize> = view
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_ref().map(|(pk, _)| (pk.clone(), i)))
            .collect();
        for layer in &self.layers {
            for op in &layer.ops {
                if op.table() != table {
                    continue;
                }
                match op {
                    OptimisticOp::Insert { row, .. } => {
                        let Some(pk) = self.base.project_pk(table, row) else {
                            continue; // unregistered table: nothing to show
                        };
                        match index.get(&pk) {
                            Some(&i) => view[i] = Some((pk, row.clone())),
                            None => {
                                index.insert(pk.clone(), view.len());
                                view.push(Some((pk, row.clone())));
                            }
                        }
                    }
                    OptimisticOp::Delete { pk, .. } => {
                        if let Some(i) = index.remove(pk) {
                            view[i] = None;
                        }
                    }
                }
            }
        }
        view.into_iter().flatten().map(|(_, row)| row).collect()
    }

    /// Total effective rows across every registered table.
    pub fn size(&self) -> usize {
        if self.layers.is_empty() {
            return self.base.size();
        }
        self.base
            .table_names()
            .iter()
            .map(|t| self.rows(t).len())
            .sum()
    }

    /// Apply per-query authoritative diffs (a `TxUpdate` or `InitialData`),
    /// attributing rows to `query_id` exactly as [`RowCache::apply_query_diff`]
    /// does. When the update is this client's own commit, pass the reducer
    /// name as `own_reducer` so the matching layer drops **in the same event
    /// batch** — that single-transition diff is the no-flicker guarantee
    /// (CS-011).
    pub fn apply_tx(
        &mut self,
        by_query: &[(u32, Vec<TableDiff>)],
        own_reducer: Option<&str>,
    ) -> Vec<RowEvent> {
        if self.layers.is_empty() {
            let mut events = Vec::new();
            for (query_id, diffs) in by_query {
                events.extend(self.base.apply_query_diff(*query_id, diffs));
            }
            return events;
        }
        let tables = self.touched_tables();
        let before = self.snapshot_views(&tables);
        let mut base_events = Vec::new();
        for (query_id, diffs) in by_query {
            base_events.extend(self.base.apply_query_diff(*query_id, diffs));
        }
        if let Some(reducer) = own_reducer {
            self.note_own_tx(reducer);
        }
        self.finish(&before, &tables, base_events)
    }

    /// Drop a subscription ([`RowCache::release_query`]), translated through
    /// the overlay.
    pub fn release_query(&mut self, query_id: u32) -> Vec<RowEvent> {
        if self.layers.is_empty() {
            return self.base.release_query(query_id);
        }
        let tables = self.touched_tables();
        let before = self.snapshot_views(&tables);
        let base_events = self.base.release_query(query_id);
        self.finish(&before, &tables, base_events)
    }

    /// Rebuild from a fresh `InitialData` ([`RowCache::reconcile`]),
    /// translated through the overlay: queued optimistic rows stay visible on
    /// top of the reconciled base until their calls resolve.
    pub fn reconcile(&mut self, snapshots: &[TableSnapshot]) -> Vec<RowEvent> {
        if self.layers.is_empty() {
            return self.base.reconcile(snapshots);
        }
        let tables = self.touched_tables();
        let before = self.snapshot_views(&tables);
        let base_events = self.base.reconcile(snapshots);
        self.finish(&before, &tables, base_events)
    }

    /// Forward of [`RowCache::query_snapshot`] — the authoritative rows a
    /// subscription holds, the unit the durable client state persists per
    /// query (CS-040). Optimistic overlays are deliberately absent: they
    /// are in-flight feedback, not state to survive a restart.
    pub fn query_snapshot(&self, query_id: u32) -> Vec<TableSnapshot> {
        self.base.query_snapshot(query_id)
    }

    /// Forward of [`RowCache::reset_queries`] (no visible rows change).
    pub fn reset_queries(&mut self) {
        self.base.reset_queries();
    }

    /// Forward of [`RowCache::track_query`] (no visible rows change).
    pub fn track_query(&mut self, query_id: u32, snapshots: &[TableSnapshot]) {
        self.base.track_query(query_id, snapshots);
    }

    /// Run an optimistic updater and apply its ops as a new overlay layer
    /// (CS-010). Returns the layer id — the handle [`SyncedCache::confirm`] /
    /// [`SyncedCache::rollback`] take — and the events of the transition.
    pub fn apply_optimistic(
        &mut self,
        reducer: impl Into<String>,
        updater: impl FnOnce(&mut OptimisticStore<'_>),
    ) -> (u64, Vec<RowEvent>) {
        let mut store = OptimisticStore {
            cache: self,
            ops: Vec::new(),
        };
        updater(&mut store);
        let ops = store.ops;

        let mut tables = self.touched_tables();
        for op in &ops {
            tables.insert(op.table().to_owned());
        }
        let before = self.snapshot_views(&tables);
        let id = self.next_layer;
        self.next_layer += 1;
        self.layers.push(Layer {
            id,
            reducer: reducer.into(),
            ops,
            confirmed: false,
        });
        let events = self.finish(&before, &tables, Vec::new());
        (id, events)
    }

    /// Roll a layer back (CS-011, the `ReducerResult::Err` path): remove it
    /// and return the net events. Unknown ids are a no-op — the layer already
    /// resolved.
    pub fn rollback(&mut self, layer_id: u64) -> Vec<RowEvent> {
        let Some(pos) = self.layers.iter().position(|l| l.id == layer_id) else {
            return Vec::new();
        };
        let tables = self.touched_tables();
        let before = self.snapshot_views(&tables);
        self.layers.remove(pos);
        self.finish(&before, &tables, Vec::new())
    }

    /// Record the `ReducerResult::Ok` ack for a layer (CS-011). The layer
    /// drops now if its authoritative update has already applied (or can
    /// never arrive: `no_subscriptions`, or the base already shadows every
    /// op); otherwise it holds until [`SyncedCache::apply_tx`] matches it.
    pub fn confirm(&mut self, layer_id: u64, no_subscriptions: bool) -> Vec<RowEvent> {
        let Some(pos) = self.layers.iter().position(|l| l.id == layer_id) else {
            return Vec::new();
        };
        self.layers[pos].confirmed = true;
        let drop_now = no_subscriptions || self.fully_shadowed(&self.layers[pos]);
        if !drop_now {
            return Vec::new();
        }
        let tables = self.touched_tables();
        let before = self.snapshot_views(&tables);
        self.drop_layer_and_older(layer_id);
        self.finish(&before, &tables, Vec::new())
    }

    // --- Internals -----------------------------------------------------------

    /// FIFO-attribute one own-commit `TxUpdate` to the oldest live layer for
    /// `reducer` and drop that layer: its authoritative rows are in the base
    /// as of this very transition, so the drop diffs clean (CS-011). The ack
    /// arriving later finds the layer gone and is a no-op — a matched update
    /// implies the call committed.
    fn note_own_tx(&mut self, reducer: &str) {
        let Some(pos) = self.layers.iter().position(|l| l.reducer == reducer) else {
            return; // not one of ours (or a non-optimistic call of ours)
        };
        let id = self.layers[pos].id;
        self.drop_layer_and_older(id);
    }

    /// Remove `layer_id` plus every *older* confirmed layer: same-shard
    /// commits are ordered, so once a newer call's update has applied an
    /// older confirmed call's update is provably not still in flight.
    fn drop_layer_and_older(&mut self, layer_id: u64) {
        let Some(pos) = self.layers.iter().position(|l| l.id == layer_id) else {
            return;
        };
        let mut keep = Vec::with_capacity(self.layers.len());
        for (i, layer) in self.layers.drain(..).enumerate() {
            if i == pos || (i < pos && layer.confirmed) {
                continue;
            }
            keep.push(layer);
        }
        self.layers = keep;
    }

    /// Whether the base already reflects every op of `layer`: each inserted
    /// primary key resolves to a base row (bytes may differ — dropping then
    /// yields an update, not a flicker) and each deleted key is absent.
    fn fully_shadowed(&self, layer: &Layer) -> bool {
        layer.ops.iter().all(|op| match op {
            OptimisticOp::Insert { table, row } => match self.base.project_pk(table, row) {
                Some(pk) => self
                    .base
                    .pk_rows(table)
                    .iter()
                    .any(|(base_pk, _)| *base_pk == pk),
                None => true, // unregistered table: never visible anyway
            },
            OptimisticOp::Delete { table, pk } => !self
                .base
                .pk_rows(table)
                .iter()
                .any(|(base_pk, _)| base_pk == pk),
        })
    }

    /// Every table any live layer touches.
    fn touched_tables(&self) -> BTreeSet<String> {
        self.layers
            .iter()
            .flat_map(|l| l.ops.iter().map(|op| op.table().to_owned()))
            .collect()
    }

    /// The effective `pk → row` view of each named table.
    fn snapshot_views(&self, tables: &BTreeSet<String>) -> HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut views = HashMap::new();
        for table in tables {
            let mut view: BTreeMap<Vec<u8>, Vec<u8>> =
                self.base.pk_rows(table).into_iter().collect();
            for layer in &self.layers {
                for op in &layer.ops {
                    if op.table() != table {
                        continue;
                    }
                    match op {
                        OptimisticOp::Insert { row, .. } => {
                            if let Some(pk) = self.base.project_pk(table, row) {
                                view.insert(pk, row.clone());
                            }
                        }
                        OptimisticOp::Delete { pk, .. } => {
                            view.remove(pk);
                        }
                    }
                }
            }
            views.insert(table.clone(), view);
        }
        views
    }

    /// Close a transition: diff the touched tables' effective views against
    /// `before`, pass base events for untouched tables through unchanged, and
    /// order the batch inserts → deletes → updates (SDK-045).
    fn finish(
        &self,
        before: &HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
        tables: &BTreeSet<String>,
        base_events: Vec<RowEvent>,
    ) -> Vec<RowEvent> {
        let after = self.snapshot_views(tables);
        let mut events = Vec::new();
        for table in tables {
            let empty = BTreeMap::new();
            let old = before.get(table).unwrap_or(&empty);
            let new = after.get(table).unwrap_or(&empty);
            for (pk, row) in new {
                match old.get(pk) {
                    None => events.push(RowEvent::Insert {
                        table: table.clone(),
                        row: row.clone(),
                    }),
                    Some(prev) if prev != row => events.push(RowEvent::Update {
                        table: table.clone(),
                        old: prev.clone(),
                        row: row.clone(),
                    }),
                    Some(_) => {}
                }
            }
            for (pk, prev) in old {
                if !new.contains_key(pk) {
                    events.push(RowEvent::Delete {
                        table: table.clone(),
                        row: prev.clone(),
                    });
                }
            }
        }
        events.extend(
            base_events
                .into_iter()
                .filter(|event| !tables.contains(event_table(event))),
        );
        events.sort_by_key(|event| match event {
            RowEvent::Insert { .. } => 0u8,
            RowEvent::Delete { .. } => 1,
            RowEvent::Update { .. } => 2,
        });
        events
    }
}

fn event_table(event: &RowEvent) -> &str {
    match event {
        RowEvent::Insert { table, .. }
        | RowEvent::Delete { table, .. }
        | RowEvent::Update { table, .. } => table,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Row is `[pk, payload]`; a delete entry is `[pk]`.
    fn task_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.into(),
            pk_of_row: Box::new(|r| vec![r[0]]),
            pk_of_delete: Box::new(|e| vec![e[0]]),
        }
    }

    fn cache() -> SyncedCache {
        SyncedCache::new([task_schema("Task")])
    }

    fn row(pk: u8, payload: u8) -> Vec<u8> {
        vec![pk, payload]
    }

    fn tx(inserts: Vec<Vec<u8>>, deletes: Vec<Vec<u8>>) -> Vec<(u32, Vec<TableDiff>)> {
        vec![(
            1,
            vec![TableDiff {
                table: "Task".into(),
                inserts,
                deletes,
            }],
        )]
    }

    #[test]
    fn an_optimistic_insert_is_visible_immediately() {
        let mut c = cache();
        let (_, events) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 7)));
        assert_eq!(
            events,
            vec![RowEvent::Insert {
                table: "Task".into(),
                row: row(1, 7)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 7)]);
        assert!(c.authoritative().rows("Task").is_empty(), "base untouched");
    }

    #[test]
    fn confirm_after_own_tx_swaps_without_flicker() {
        // CS-011 scenario 1: the authoritative row replaces the optimistic one
        // with no flicker or duplicate. Identical bytes: zero events.
        let mut c = cache();
        let (id, _) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 7)));

        let events = c.apply_tx(&tx(vec![row(1, 7)], vec![]), Some("add"));
        assert!(events.is_empty(), "same bytes, no re-render: {events:?}");
        assert_eq!(c.optimistic_len(), 0, "matched tx dropped the layer");
        assert!(c.confirm(id, false).is_empty(), "late ack is a no-op");
        assert_eq!(c.rows("Task"), vec![row(1, 7)]);
    }

    #[test]
    fn differing_authoritative_bytes_arrive_as_one_update() {
        // The server transformed the row (assigned fields): the swap is one
        // Update in one batch — never delete+insert.
        let mut c = cache();
        c.apply_optimistic("add", |s| s.insert("Task", row(1, 0)));
        let events = c.apply_tx(&tx(vec![row(1, 9)], vec![]), Some("add"));
        assert_eq!(
            events,
            vec![RowEvent::Update {
                table: "Task".into(),
                old: row(1, 0),
                row: row(1, 9)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 9)]);
    }

    #[test]
    fn ack_before_tx_holds_the_overlay_until_the_update_lands() {
        // Ok first, TxUpdate later: dropping on the ack would delete the row
        // and re-insert it when the update arrives — the flicker CS-011
        // forbids. The layer holds.
        let mut c = cache();
        let (id, _) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 7)));
        assert!(c.confirm(id, false).is_empty());
        assert_eq!(c.optimistic_len(), 1, "confirmed but held");
        assert_eq!(c.rows("Task"), vec![row(1, 7)], "still rendered");

        let events = c.apply_tx(&tx(vec![row(1, 7)], vec![]), Some("add"));
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(c.optimistic_len(), 0);
    }

    #[test]
    fn a_rejected_mutation_rolls_back_to_the_exact_prior_state() {
        // CS-011 scenario 2: Err removes the optimistic row; the cache
        // matches server state exactly.
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        let (id, _) = c.apply_optimistic("edit", |s| {
            s.insert("Task", row(1, 9));
            s.insert("Task", row(2, 2));
        });
        assert_eq!(c.rows("Task"), vec![row(1, 9), row(2, 2)]);

        let events = c.rollback(id);
        assert_eq!(
            events,
            vec![
                RowEvent::Delete {
                    table: "Task".into(),
                    row: row(2, 2)
                },
                RowEvent::Update {
                    table: "Task".into(),
                    old: row(1, 9),
                    row: row(1, 1)
                },
            ]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 1)]);
    }

    #[test]
    fn a_rolled_back_row_is_never_resurrected() {
        // CS-012: after rollback the layer is gone; an unrelated authoritative
        // update must not bring its row back.
        let mut c = cache();
        let (id, _) = c.apply_optimistic("add", |s| s.insert("Task", row(5, 5)));
        c.rollback(id);
        let events = c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        assert_eq!(
            events,
            vec![RowEvent::Insert {
                table: "Task".into(),
                row: row(1, 1)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 1)], "row 5 stays gone");
    }

    #[test]
    fn concurrent_layers_reconcile_in_submission_order() {
        // CS-012: two optimistic writes to one pk — the later submission wins
        // the effective view; rolling back the LATER one re-exposes the
        // earlier, still-pending value, not the base.
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 0)], vec![]), None);
        let (_a, _) = c.apply_optimistic("edit", |s| s.insert("Task", row(1, 5)));
        let (b, _) = c.apply_optimistic("edit", |s| s.insert("Task", row(1, 8)));
        assert_eq!(c.rows("Task"), vec![row(1, 8)], "submission order");

        let events = c.rollback(b);
        assert_eq!(
            events,
            vec![RowEvent::Update {
                table: "Task".into(),
                old: row(1, 8),
                row: row(1, 5)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 5)]);
    }

    #[test]
    fn an_optimistic_delete_hides_the_base_row_until_confirmed() {
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        let (_, events) = c.apply_optimistic("remove", |s| s.delete("Task", vec![1]));
        assert_eq!(
            events,
            vec![RowEvent::Delete {
                table: "Task".into(),
                row: row(1, 1)
            }]
        );
        assert!(c.rows("Task").is_empty());

        // The authoritative delete lands: nothing visible changes.
        let events = c.apply_tx(&tx(vec![], vec![vec![1]]), Some("remove"));
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(c.optimistic_len(), 0);
    }

    #[test]
    fn confirm_drops_immediately_when_the_base_already_shadows_the_ops() {
        // The update raced ahead under a different attribution (or another
        // client wrote the same row): the ack finds every op shadowed and
        // drops the layer without waiting.
        let mut c = cache();
        let (id, _) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 7)));
        c.apply_tx(&tx(vec![row(1, 7)], vec![]), None); // no attribution
        let events = c.confirm(id, false);
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(c.optimistic_len(), 0);
    }

    #[test]
    fn confirm_with_no_subscriptions_drops_the_layer() {
        // No subscriptions → no TxUpdate will ever come; holding would leak
        // the overlay forever.
        let mut c = cache();
        let (id, _) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 7)));
        let events = c.confirm(id, true);
        assert_eq!(
            events,
            vec![RowEvent::Delete {
                table: "Task".into(),
                row: row(1, 7)
            }]
        );
        assert_eq!(c.optimistic_len(), 0);
    }

    #[test]
    fn dropping_a_layer_drops_older_confirmed_layers_too() {
        // Call A's update never reached us (its rows are outside our
        // subscriptions); call B's did. Commits are ordered, so once B's
        // update applied A's cannot still be in flight: A drops with B.
        let mut c = cache();
        let (a, _) = c.apply_optimistic("a", |s| s.insert("Task", row(1, 1)));
        let (_b, _) = c.apply_optimistic("b", |s| s.insert("Task", row(2, 2)));
        assert!(c.confirm(a, false).is_empty(), "A held");

        let events = c.apply_tx(&tx(vec![row(2, 2)], vec![]), Some("b"));
        assert_eq!(c.optimistic_len(), 0, "B matched, A force-dropped");
        // A's row was optimistic-only and its update never arrived: it leaves.
        assert_eq!(
            events,
            vec![RowEvent::Delete {
                table: "Task".into(),
                row: row(1, 1)
            }]
        );
    }

    #[test]
    fn fifo_attribution_matches_same_reducer_calls_in_order() {
        let mut c = cache();
        let (_a, _) = c.apply_optimistic("add", |s| s.insert("Task", row(1, 1)));
        let (_b, _) = c.apply_optimistic("add", |s| s.insert("Task", row(2, 2)));

        // First own-tx for "add" belongs to the first call.
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), Some("add"));
        assert_eq!(c.optimistic_len(), 1, "only the first layer matched");
        c.apply_tx(&tx(vec![row(2, 2)], vec![]), Some("add"));
        assert_eq!(c.optimistic_len(), 0);
        assert_eq!(c.rows("Task"), vec![row(1, 1), row(2, 2)]);
    }

    #[test]
    fn unregistered_tables_are_ignored_not_fatal() {
        let mut c = cache();
        let (id, events) = c.apply_optimistic("add", |s| s.insert("Ghost", row(1, 1)));
        assert!(events.is_empty(), "nothing visible: {events:?}");
        assert!(c.rows("Ghost").is_empty());
        assert!(c.confirm(id, false).is_empty());
        assert_eq!(c.optimistic_len(), 0, "shadowed-by-vacuity drops it");
    }

    #[test]
    fn reconcile_keeps_pending_optimistic_rows_on_top() {
        // Reconnect: the base rebuilds from a fresh snapshot while a queued
        // optimistic call is still unresolved — its row keeps rendering.
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        c.apply_optimistic("add", |s| s.insert("Task", row(9, 9)));

        let events = c.reconcile(&[TableSnapshot {
            table: "Task".into(),
            rows: vec![row(1, 1), row(2, 2)],
        }]);
        assert_eq!(
            events,
            vec![RowEvent::Insert {
                table: "Task".into(),
                row: row(2, 2)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(1, 1), row(2, 2), row(9, 9)]);
    }

    #[test]
    fn effective_size_and_rows_account_for_the_overlay() {
        let mut c = SyncedCache::new([task_schema("Task"), task_schema("Other")]);
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        c.apply_optimistic("mix", |s| {
            s.insert("Task", row(2, 2));
            s.insert("Other", row(9, 9));
            s.delete("Task", vec![1]);
        });
        // Task: row 1 hidden, row 2 overlaid; Other: row 9 overlaid.
        assert_eq!(c.rows("Task"), vec![row(2, 2)]);
        assert_eq!(c.rows("Other"), vec![row(9, 9)]);
        assert_eq!(c.size(), 2, "effective, not base");
        assert_eq!(c.authoritative().size(), 1, "base untouched");
    }

    #[test]
    fn resolving_an_unknown_layer_is_a_no_op() {
        // The ack raced the tx match (or a rollback already ran): the layer
        // is gone, and resolving it again must not fire anything.
        let mut c = cache();
        assert!(c.rollback(99).is_empty());
        assert!(c.confirm(99, true).is_empty());
    }

    #[test]
    fn the_updater_reads_the_effective_view() {
        // CS-010: the local store an updater sees includes earlier in-flight
        // overlays — what the user is looking at, not just the base.
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        c.apply_optimistic("a", |s| s.insert("Task", row(2, 2)));
        let mut seen = 0;
        c.apply_optimistic("b", |s| {
            seen = s.rows("Task").len();
            s.delete("Task", vec![2]);
        });
        assert_eq!(seen, 2, "base row + overlay row");
        assert_eq!(c.rows("Task"), vec![row(1, 1)], "b's delete hides a's row");
    }

    #[test]
    fn query_bookkeeping_forwards_under_an_overlay() {
        // reset_queries/track_query are refcount bookkeeping only; releasing
        // the re-attributed query then diffs THROUGH the overlay.
        let mut c = cache();
        c.apply_tx(&tx(vec![row(1, 1)], vec![]), None);
        c.apply_optimistic("add", |s| s.insert("Task", row(2, 2)));

        c.reset_queries();
        c.track_query(
            7,
            &[TableSnapshot {
                table: "Task".into(),
                rows: vec![row(1, 1)],
            }],
        );
        let events = c.release_query(7);
        assert_eq!(
            events,
            vec![RowEvent::Delete {
                table: "Task".into(),
                row: row(1, 1)
            }]
        );
        assert_eq!(c.rows("Task"), vec![row(2, 2)], "the overlay row survives");
    }

    // --- CS property test (task 1.8) ------------------------------------------
    //
    // Random sequences of optimistic apply / confirm / reject interleaved
    // with authoritative updates must leave the local cache bit-identical to
    // the server model once every call has resolved.

    /// Deterministic xorshift64* — no rand dependency, reproducible failures.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// The server: an authoritative pk → row map plus the calls it has not
    /// yet resolved, in submission order.
    struct ServerModel {
        rows: BTreeMap<Vec<u8>, Vec<u8>>,
        inflight: Vec<(u64, Vec<OptimisticOp>)>,
    }

    impl ServerModel {
        /// Commit `ops` and return the wire diff exactly as the server
        /// broadcasts it: the NET effect of the whole transaction — a
        /// replacement is a delete+insert pair, an insert-then-delete of one
        /// pk inside one call cancels out, and a no-op write emits nothing.
        fn commit(&mut self, ops: &[OptimisticOp]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
            let before = self.rows.clone();
            for op in ops {
                match op {
                    OptimisticOp::Insert { row, .. } => {
                        self.rows.insert(vec![row[0]], row.clone());
                    }
                    OptimisticOp::Delete { pk, .. } => {
                        self.rows.remove(pk);
                    }
                }
            }
            let mut inserts = Vec::new();
            let mut deletes = Vec::new();
            for (pk, row) in &self.rows {
                match before.get(pk) {
                    Some(prev) if prev == row => {}
                    Some(_) => {
                        deletes.push(pk.clone());
                        inserts.push(row.clone());
                    }
                    None => inserts.push(row.clone()),
                }
            }
            for pk in before.keys() {
                if !self.rows.contains_key(pk) {
                    deletes.push(pk.clone());
                }
            }
            (inserts, deletes)
        }
    }

    #[test]
    fn property_random_interleavings_converge_bit_identical() {
        for seed in 1..=40u64 {
            let mut rng = Rng(seed);
            let mut c = cache();
            let mut server = ServerModel {
                rows: BTreeMap::new(),
                inflight: Vec::new(),
            };

            for _step in 0..60 {
                match rng.below(3) {
                    // Submit an optimistic call: 1–3 random upserts/deletes.
                    0 => {
                        let mut ops = Vec::new();
                        for _ in 0..=rng.below(2) {
                            let pk = u8::try_from(rng.below(6)).unwrap();
                            if rng.below(4) == 0 {
                                ops.push(OptimisticOp::Delete {
                                    table: "Task".into(),
                                    pk: vec![pk],
                                });
                            } else {
                                let payload = u8::try_from(rng.below(250)).unwrap();
                                ops.push(OptimisticOp::Insert {
                                    table: "Task".into(),
                                    row: vec![pk, payload],
                                });
                            }
                        }
                        let cloned = ops.clone();
                        let (id, _) = c.apply_optimistic("mutate", move |s| {
                            for op in cloned {
                                match op {
                                    OptimisticOp::Insert { table, row } => s.insert(table, row),
                                    OptimisticOp::Delete { table, pk } => s.delete(table, pk),
                                }
                            }
                        });
                        server.inflight.push((id, ops));
                    }
                    // Resolve the oldest in-flight call.
                    1 if !server.inflight.is_empty() => {
                        let (id, ops) = server.inflight.remove(0);
                        if rng.below(4) == 0 {
                            // Rejected: nothing commits, overlay rolls back.
                            c.rollback(id);
                            continue;
                        }
                        // Committed: the server broadcasts the diff — unless
                        // it is empty, in which case no TxUpdate is fanned
                        // out at all (no subscriber row changed). Ack and
                        // update arrive in either order.
                        let (inserts, deletes) = server.commit(&ops);
                        if inserts.is_empty() && deletes.is_empty() {
                            c.confirm(id, false);
                            continue;
                        }
                        let update = tx(inserts, deletes);
                        if rng.below(2) == 0 {
                            c.apply_tx(&update, Some("mutate"));
                            c.confirm(id, false);
                        } else {
                            c.confirm(id, false);
                            c.apply_tx(&update, Some("mutate"));
                        }
                    }
                    // Some OTHER client commits a write we are subscribed to.
                    _ => {
                        let pk = u8::try_from(rng.below(6)).unwrap();
                        let payload = u8::try_from(rng.below(250)).unwrap();
                        let (inserts, deletes) = server.commit(&[OptimisticOp::Insert {
                            table: "Task".into(),
                            row: vec![pk, payload],
                        }]);
                        if !inserts.is_empty() || !deletes.is_empty() {
                            c.apply_tx(&tx(inserts, deletes), None);
                        }
                    }
                }
            }

            // Drain: resolve everything still in flight as committed.
            while !server.inflight.is_empty() {
                let (id, ops) = server.inflight.remove(0);
                let (inserts, deletes) = server.commit(&ops);
                if !inserts.is_empty() || !deletes.is_empty() {
                    c.apply_tx(&tx(inserts, deletes), Some("mutate"));
                }
                c.confirm(id, false);
            }

            assert_eq!(c.optimistic_len(), 0, "seed {seed}: layers must drain");
            let mut got: Vec<Vec<u8>> = c.rows("Task");
            got.sort();
            let mut want: Vec<Vec<u8>> = server.rows.values().cloned().collect();
            want.sort();
            assert_eq!(got, want, "seed {seed}: cache != server state");
        }
    }
}
