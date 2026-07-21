//! The local row cache (SPEC-011 SDK-040/042/044/045) — the Rust port of the
//! TypeScript SDK's `cache.ts`, with the same observable behaviour so the
//! shared conformance corpus (TST-052) sees one client, not two.
//!
//! Row identity is the row's full FluxBIN bytes as received on the wire
//! (SDK-040): byte-keying gives map semantics for column types Rust cannot
//! hash (an `f64` column is a fine part of a row and a terrible map key) and
//! makes row equality a byte compare rather than a field-by-field decode.
//!
//! The cache is schema-agnostic on purpose — it never decodes a row. Generated
//! code (or a hand-written [`TableSchema`]) supplies the primary-key
//! projection per table, the only schema knowledge the diff algorithm needs.

use std::collections::{HashMap, HashSet};

/// A primary-key projection: row (or delete-entry) bytes → stable key bytes.
///
/// A byte vector rather than a typed value because primary keys are not always
/// hashable — composite keys are tuples and `Identity` keys are byte arrays.
/// The bytes only need to be stable and collision-free per table.
pub type PkProjection = Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// Per-table hooks the generated bindings supply (SDK-040).
pub struct TableSchema {
    /// Table name as it appears in `TableUpdate`.
    pub name: String,
    /// Projects a full row to its primary key.
    pub pk_of_row: PkProjection,
    /// Projects a delete entry to its primary key — the wire carries
    /// **primary-key fields only** for deletes (SPEC-006), a different layout
    /// from a full row.
    pub pk_of_delete: PkProjection,
}

/// One table's inserts and deletes within a `TxUpdate`.
#[derive(Debug, Clone, Default)]
pub struct TableDiff {
    /// Table name.
    pub table: String,
    /// Full inserted rows.
    pub inserts: Vec<Vec<u8>>,
    /// Primary-key-only delete entries (SPEC-006).
    pub deletes: Vec<Vec<u8>>,
}

/// One table's full rows from a fresh `InitialData` — the reconnect
/// reconciliation input ([`RowCache::reconcile`]). Duplicated rows within one
/// snapshot are the overlap between subscriptions and become the refcount.
#[derive(Debug, Clone, Default)]
pub struct TableSnapshot {
    /// Table name.
    pub table: String,
    /// Full rows, wire bytes.
    pub rows: Vec<Vec<u8>>,
}

/// A semantic row event. `Update` is the primary-key-coalesced pair (SDK-042).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowEvent {
    /// A row became visible (refcount 0 → 1).
    Insert {
        /// Table name.
        table: String,
        /// The row's bytes.
        row: Vec<u8>,
    },
    /// A row left the cache (refcount 1 → 0).
    Delete {
        /// Table name.
        table: String,
        /// The row's last-cached bytes.
        row: Vec<u8>,
    },
    /// A delete+insert of the same primary key in one update (SDK-042).
    Update {
        /// Table name.
        table: String,
        /// The previously cached row.
        old: Vec<u8>,
        /// The replacing row.
        row: Vec<u8>,
    },
}

struct Entry {
    bytes: Vec<u8>,
    /// How many active subscription queries currently see this row (SDK-044).
    refs: u32,
}

struct TableState {
    schema: TableSchema,
    /// Byte key → entry. The authoritative store, insertion-ordered by a
    /// parallel `Vec` so `rows()` is deterministic.
    by_key: HashMap<Vec<u8>, Entry>,
    order: Vec<Vec<u8>>,
    /// Primary key → byte key. What deletes and updates resolve through.
    by_pk: HashMap<Vec<u8>, Vec<u8>>,
}

impl TableState {
    fn insert_row(&mut self, key: Vec<u8>, row: Vec<u8>) {
        let pk = (self.schema.pk_of_row)(&row);
        self.order.push(key.clone());
        self.by_pk.insert(pk, key.clone());
        self.by_key.insert(
            key,
            Entry {
                bytes: row,
                refs: 1,
            },
        );
    }

    fn remove_key(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.by_key.remove(key)?;
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        let pk = (self.schema.pk_of_row)(&entry.bytes);
        if self.by_pk.get(&pk).map(|k| k.as_slice()) == Some(key) {
            self.by_pk.remove(&pk);
        }
        Some(entry.bytes)
    }
}

/// The byte-keyed, reference-counted row store.
///
/// Mutation and notification are separated by construction: the `apply_*`
/// methods finish every change and then *return* the events. They never
/// invoke a callback themselves, which is what makes SDK-045's "callbacks
/// always observe the full post-commit state" a property of the code's shape
/// rather than a rule to remember.
pub struct RowCache {
    tables: HashMap<String, TableState>,
    /// `query_id → table → byte-keys` (SDK-044): which cached rows each
    /// subscription holds, so unsubscribe releases exactly its rows.
    query_keys: HashMap<u32, HashMap<String, HashSet<Vec<u8>>>>,
}

impl RowCache {
    /// Build a cache over a set of table schemas.
    pub fn new(schemas: impl IntoIterator<Item = TableSchema>) -> Self {
        let tables = schemas
            .into_iter()
            .map(|schema| {
                (
                    schema.name.clone(),
                    TableState {
                        schema,
                        by_key: HashMap::new(),
                        order: Vec::new(),
                        by_pk: HashMap::new(),
                    },
                )
            })
            .collect();
        Self {
            tables,
            query_keys: HashMap::new(),
        }
    }

    /// Rows currently cached for `table`, in insertion order. Empty for an
    /// unknown table (the caller asked about a table it did not register).
    pub fn rows(&self, table: &str) -> Vec<Vec<u8>> {
        match self.tables.get(table) {
            Some(state) => state
                .order
                .iter()
                .filter_map(|key| state.by_key.get(key).map(|e| e.bytes.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// How many subscriptions currently see this exact row. 0 when absent.
    pub fn refcount(&self, table: &str, row: &[u8]) -> u32 {
        self.tables
            .get(table)
            .and_then(|s| s.by_key.get(row))
            .map_or(0, |e| e.refs)
    }

    /// Total cached rows across every table.
    pub fn size(&self) -> usize {
        self.tables.values().map(|s| s.by_key.len()).sum()
    }

    /// Apply a `TxUpdate` (or `InitialData`) belonging to a KNOWN subscription
    /// query, tracking which rows the query holds so a later
    /// [`RowCache::release_query`] can drop exactly them (SDK-044).
    ///
    /// The refcount is still by byte identity across queries: a row two
    /// queries both deliver is cached once at refcount 2, and each query
    /// records it. The gate is per-query — a query increments a row's refcount
    /// only the FIRST time it delivers that row, so a reconnect replay is
    /// idempotent rather than an inflated count.
    pub fn apply_query_diff(&mut self, query_id: u32, diffs: &[TableDiff]) -> Vec<RowEvent> {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();

        for diff in diffs {
            let Some(state) = self.tables.get_mut(&diff.table) else {
                continue;
            };
            let held = self
                .query_keys
                .entry(query_id)
                .or_default()
                .entry(diff.table.clone())
                .or_default();

            // Resolve deletes to their rows before inserts run — an insert
            // under the same PK repoints the projection this lookup depends on.
            let mut doomed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            for entry in &diff.deletes {
                let pk = (state.schema.pk_of_delete)(entry);
                if let Some(key) = state
                    .by_pk
                    .get(&pk)
                    .filter(|k| state.by_key.contains_key(*k))
                {
                    doomed.push((pk.clone(), key.clone()));
                }
            }

            for row in &diff.inserts {
                let key = row.clone();
                if held.contains(&key) {
                    continue; // this query already holds it: idempotent
                }
                held.insert(key.clone());
                if let Some(existing) = state.by_key.get_mut(&key) {
                    existing.refs += 1; // visible through another query too
                    continue;
                }
                state.insert_row(key, row.clone());
                inserts.push(RowEvent::Insert {
                    table: diff.table.clone(),
                    row: row.clone(),
                });
            }

            for (_pk, key) in doomed {
                if !held.remove(&key) {
                    continue; // this query never held it
                }
                let still_held = match state.by_key.get_mut(&key) {
                    Some(entry) => {
                        entry.refs -= 1;
                        entry.refs > 0
                    }
                    None => continue,
                };
                if still_held {
                    continue;
                }
                if let Some(bytes) = state.remove_key(&key) {
                    deletes.push(RowEvent::Delete {
                        table: diff.table.clone(),
                        row: bytes,
                    });
                }
            }
        }

        self.coalesce(inserts, deletes)
    }

    /// Drop a subscription: release every row the query held (SDK-044) and
    /// return the net-difference events. A row still held by another query
    /// survives at a lower refcount and fires nothing; a row only this query
    /// held reaches refcount 0, leaves the cache, and fires one `Delete`.
    pub fn release_query(&mut self, query_id: u32) -> Vec<RowEvent> {
        let Some(held) = self.query_keys.remove(&query_id) else {
            return Vec::new();
        };
        let mut deletes = Vec::new();
        for (table, keys) in held {
            let Some(state) = self.tables.get_mut(&table) else {
                continue;
            };
            for key in keys {
                let gone = match state.by_key.get_mut(&key) {
                    Some(entry) => {
                        entry.refs -= 1;
                        entry.refs == 0
                    }
                    None => false,
                };
                if gone && let Some(bytes) = state.remove_key(&key) {
                    deletes.push(RowEvent::Delete {
                        table: table.clone(),
                        row: bytes,
                    });
                }
            }
        }
        deletes
    }

    /// Forget all per-query membership without touching cached rows or their
    /// refcounts. Used on reconnect, where [`RowCache::reconcile`] rebuilds
    /// refcounts from the fresh snapshot and [`RowCache::track_query`] then
    /// re-establishes which query holds what (the server reassigns query ids
    /// on resubscribe).
    pub fn reset_queries(&mut self) {
        self.query_keys.clear();
    }

    /// Record that `query_id` holds these rows, by byte identity, WITHOUT
    /// changing refcounts — the rows are already cached (post-reconcile). The
    /// companion of [`RowCache::reset_queries`] on the reconnect path.
    pub fn track_query(&mut self, query_id: u32, snapshots: &[TableSnapshot]) {
        let held = self.query_keys.entry(query_id).or_default();
        for snapshot in snapshots {
            let keys = held.entry(snapshot.table.clone()).or_default();
            for row in &snapshot.rows {
                keys.insert(row.clone());
            }
        }
    }

    /// Rebuild from a fresh `InitialData` and return only the net difference
    /// (SDK-047).
    ///
    /// The naive reconnect — clear the cache, reinsert everything — is a
    /// callback storm that tells the application every row it already had was
    /// deleted and recreated. What an application actually needs to know is
    /// what *changed* while it was disconnected, which is what this computes.
    ///
    /// Refcounts are rebuilt from the fresh data rather than carried over
    /// (SDK-047): the old counts describe subscriptions from a session that no
    /// longer exists. A table with an active subscription that came back empty
    /// sends an empty snapshot; a table absent from the snapshots entirely is
    /// no longer subscribed, and its rows are gone rather than unmentioned.
    pub fn reconcile(&mut self, snapshots: &[TableSnapshot]) -> Vec<RowEvent> {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();

        for snapshot in snapshots {
            let Some(state) = self.tables.get_mut(&snapshot.table) else {
                continue;
            };
            seen.insert(snapshot.table.as_str());

            let fresh: HashSet<&[u8]> = snapshot.rows.iter().map(Vec::as_slice).collect();
            for (key, entry) in &state.by_key {
                if !fresh.contains(key.as_slice()) {
                    deletes.push(RowEvent::Delete {
                        table: snapshot.table.clone(),
                        row: entry.bytes.clone(),
                    });
                }
            }

            // Rebuild the table from the snapshot: insertion order follows the
            // snapshot, duplicates within it become the refcount.
            let mut by_key: HashMap<Vec<u8>, Entry> = HashMap::new();
            let mut by_pk: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            let mut order: Vec<Vec<u8>> = Vec::new();
            for row in &snapshot.rows {
                if let Some(entry) = by_key.get_mut(row.as_slice()) {
                    entry.refs += 1;
                    continue;
                }
                if !state.by_key.contains_key(row.as_slice()) {
                    inserts.push(RowEvent::Insert {
                        table: snapshot.table.clone(),
                        row: row.clone(),
                    });
                }
                order.push(row.clone());
                by_pk.insert((state.schema.pk_of_row)(row), row.clone());
                by_key.insert(
                    row.clone(),
                    Entry {
                        bytes: row.clone(),
                        refs: 1,
                    },
                );
            }
            state.by_key = by_key;
            state.by_pk = by_pk;
            state.order = order;
        }

        for (name, state) in &mut self.tables {
            if seen.contains(name.as_str()) {
                continue;
            }
            for entry in state.by_key.values() {
                deletes.push(RowEvent::Delete {
                    table: name.clone(),
                    row: entry.bytes.clone(),
                });
            }
            state.by_key = HashMap::new();
            state.by_pk = HashMap::new();
            state.order = Vec::new();
        }

        self.coalesce(inserts, deletes)
    }

    /// Fold delete/insert pairs sharing a primary key into single `Update`
    /// events (SDK-042), then order the result: inserts, deletes, updates
    /// (SDK-045).
    fn coalesce(&self, inserts: Vec<RowEvent>, deletes: Vec<RowEvent>) -> Vec<RowEvent> {
        if inserts.is_empty() || deletes.is_empty() {
            let mut out = inserts;
            out.extend(deletes);
            return out;
        }

        // Index deletes by (table, pk) so each insert can ask whether its key
        // also left in this transaction.
        let mut pending: HashMap<(String, Vec<u8>), Vec<u8>> = HashMap::new();
        for event in &deletes {
            if let RowEvent::Delete { table, row } = event
                && let Some(state) = self.tables.get(table)
            {
                let pk = (state.schema.pk_of_row)(row);
                pending.insert((table.clone(), pk), row.clone());
            }
        }

        let mut final_inserts = Vec::new();
        let mut updates = Vec::new();
        let mut matched: HashSet<(String, Vec<u8>)> = HashSet::new();
        for event in inserts {
            if let RowEvent::Insert { table, row } = event {
                let pk = self
                    .tables
                    .get(&table)
                    .map(|s| (s.schema.pk_of_row)(&row))
                    .unwrap_or_default();
                let id = (table.clone(), pk);
                if let Some(old) = pending.get(&id) {
                    matched.insert(id);
                    updates.push(RowEvent::Update {
                        table,
                        old: old.clone(),
                        row,
                    });
                    continue;
                }
                final_inserts.push(RowEvent::Insert { table, row });
            }
        }

        let mut out = final_inserts;
        for event in deletes {
            if let RowEvent::Delete { table, row } = &event {
                let pk = self
                    .tables
                    .get(table)
                    .map(|s| (s.schema.pk_of_row)(row))
                    .unwrap_or_default();
                if matched.contains(&(table.clone(), pk)) {
                    continue;
                }
            }
            out.push(event);
        }
        out.extend(updates);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_schema() -> TableSchema {
        // Row is [pk, payload]; a delete entry is [pk].
        TableSchema {
            name: "Task".into(),
            pk_of_row: Box::new(|r| vec![r[0]]),
            pk_of_delete: Box::new(|e| vec![e[0]]),
        }
    }

    fn cache() -> RowCache {
        RowCache::new([task_schema()])
    }

    fn row(pk: u8, payload: u8) -> Vec<u8> {
        vec![pk, payload]
    }

    fn del(pk: u8) -> Vec<u8> {
        vec![pk]
    }

    fn diff(inserts: Vec<Vec<u8>>, deletes: Vec<Vec<u8>>) -> Vec<TableDiff> {
        vec![TableDiff {
            table: "Task".into(),
            inserts,
            deletes,
        }]
    }

    #[test]
    fn a_first_insert_fires_one_event_and_caches_the_row() {
        let mut c = cache();
        let events = c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], RowEvent::Insert { .. }));
        assert_eq!(c.refcount("Task", &row(1, 0)), 1);
        assert_eq!(c.size(), 1);
    }

    #[test]
    fn two_queries_hold_one_row_dropping_one_keeps_it() {
        // SDK-044: an overlapping row survives the loss of one subscription
        // and fires nothing, then leaves on the last.
        let mut c = cache();
        let a = c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        let b = c.apply_query_diff(2, &diff(vec![row(1, 0)], vec![]));
        assert_eq!(a.len(), 1);
        assert!(b.is_empty(), "second query dedupes");
        assert_eq!(c.refcount("Task", &row(1, 0)), 2);

        assert!(c.release_query(1).is_empty(), "still held by query 2");
        assert_eq!(c.refcount("Task", &row(1, 0)), 1);

        let drop_b = c.release_query(2);
        assert_eq!(drop_b.len(), 1);
        assert!(matches!(drop_b[0], RowEvent::Delete { .. }));
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn unsubscribe_drops_only_the_rows_that_query_held() {
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0), row(2, 0)], vec![]));
        c.apply_query_diff(2, &diff(vec![row(2, 0), row(3, 0)], vec![]));
        assert_eq!(c.size(), 3);

        let events = c.release_query(1);
        assert_eq!(events.len(), 1, "only row 1 was query 1's alone");
        assert_eq!(c.refcount("Task", &row(1, 0)), 0);
        assert_eq!(
            c.refcount("Task", &row(2, 0)),
            1,
            "row 2 survives on query 2"
        );
        assert_eq!(c.refcount("Task", &row(3, 0)), 1);
    }

    #[test]
    fn a_delete_insert_pair_for_one_pk_coalesces_to_an_update() {
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        let events = c.apply_query_diff(1, &diff(vec![row(1, 9)], vec![del(1)]));
        assert_eq!(events.len(), 1);
        match &events[0] {
            RowEvent::Update { old, row, .. } => {
                assert_eq!(old, &vec![1, 0]);
                assert_eq!(row, &vec![1, 9]);
            }
            other => panic!("expected Update, got {other:?}"),
        }
        assert_eq!(c.size(), 1);
    }

    fn snapshot(rows: Vec<Vec<u8>>) -> Vec<TableSnapshot> {
        vec![TableSnapshot {
            table: "Task".into(),
            rows,
        }]
    }

    #[test]
    fn reconcile_reports_only_the_net_difference() {
        // SDK-047: an unchanged row fires nothing; a row that vanished while
        // disconnected fires one delete; a new row fires one insert; a row
        // whose bytes changed under the same pk coalesces to one update.
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0), row(2, 0), row(3, 0)], vec![]));

        let events = c.reconcile(&snapshot(vec![row(1, 0), row(3, 9), row(4, 0)]));
        assert_eq!(events.len(), 3, "{events:?}");
        assert!(events.contains(&RowEvent::Insert {
            table: "Task".into(),
            row: row(4, 0)
        }));
        assert!(events.contains(&RowEvent::Delete {
            table: "Task".into(),
            row: row(2, 0)
        }));
        assert!(events.contains(&RowEvent::Update {
            table: "Task".into(),
            old: row(3, 0),
            row: row(3, 9)
        }));
        assert_eq!(c.rows("Task"), vec![row(1, 0), row(3, 9), row(4, 0)]);
    }

    #[test]
    fn reconcile_rebuilds_refcounts_and_track_query_reattributes() {
        // The old session's refcounts are discarded: the duplicate row in the
        // fresh snapshot IS the overlap between the two resubscribed queries.
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        c.apply_query_diff(2, &diff(vec![row(1, 0), row(2, 0)], vec![]));
        assert_eq!(c.refcount("Task", &row(1, 0)), 2);

        c.reset_queries();
        let events = c.reconcile(&snapshot(vec![row(1, 0), row(1, 0), row(2, 0)]));
        assert!(events.is_empty(), "nothing changed while away: {events:?}");
        assert_eq!(c.refcount("Task", &row(1, 0)), 2);
        assert_eq!(c.refcount("Task", &row(2, 0)), 1);

        // The server reassigned ids on resubscribe: 1→7, 2→8.
        c.track_query(7, &snapshot(vec![row(1, 0)]));
        c.track_query(8, &snapshot(vec![row(1, 0), row(2, 0)]));
        assert!(c.release_query(7).is_empty(), "row 1 still held by query 8");
        let drop8 = c.release_query(8);
        assert_eq!(drop8.len(), 2, "last holder: both rows leave");
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn a_table_absent_from_the_snapshots_is_cleared() {
        // Absent means "no longer subscribed", not "unmentioned" — its rows
        // are gone and each fires a delete.
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        let events = c.reconcile(&[]);
        assert_eq!(
            events,
            vec![RowEvent::Delete {
                table: "Task".into(),
                row: row(1, 0)
            }]
        );
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn a_query_redelivering_a_held_row_is_idempotent() {
        let mut c = cache();
        c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        let again = c.apply_query_diff(1, &diff(vec![row(1, 0)], vec![]));
        assert!(again.is_empty());
        assert_eq!(c.refcount("Task", &row(1, 0)), 1, "held once, not twice");
    }
}
