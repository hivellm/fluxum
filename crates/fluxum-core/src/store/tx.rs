//! [`TxState`] — the single in-flight transaction's write buffer (STG-003),
//! plus the commit output [`TxDiff`] and the STG-007 undo-log hook.

use std::collections::BTreeMap;

use crate::store::TableId;
use crate::store::row::{PkBytes, Row};

/// The pending effect of the in-flight transaction on one primary key.
///
/// Keying by PK (rather than STG-003's illustrative `Vec` pair) makes the
/// STG-007 correctness rules structural: a key is never simultaneously
/// inserted and deleted, delete-then-reinsert of identical content cancels
/// by *removing* the entry, and the constraint overlay is a single map probe.
/// The logical content is exactly STG-003's `inserts` + `deletes`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingOp {
    /// Insert of a key absent from `CommittedState`.
    Insert(Row),
    /// Delete of a committed row.
    Delete,
    /// Delete of a committed row followed by a reinsert with *different*
    /// content — merges as an in-place replacement (delete + insert in the
    /// diff). An identical-content reinsert never reaches this state: it
    /// cancels to no entry at all (STG-007 rule 1).
    Update(Row),
}

/// Which mutation fired a declarative trigger (SPEC-022 RV-031).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerKind {
    /// A row was written to a previously unoccupied primary key.
    Insert,
    /// A visible row was replaced in place (upsert over an occupied key).
    Update,
    /// A visible row was deleted (including RV-032 cascade deletes).
    Delete,
}

/// One recorded mutation awaiting `#[fluxum::on_insert/on_update/on_delete]`
/// dispatch (SPEC-022 RV-031). Recorded by the write/delete paths only for
/// tables with registered triggers; drained by the reducer layer's
/// `TxHandle`, which runs the hooks inside this same transaction. Rows are
/// as stored (`#[encrypted]` columns sealed) — dispatch decrypts.
#[derive(Debug, Clone)]
pub struct TriggerEvent {
    /// The mutated table.
    pub table: TableId,
    /// Insert / Update / Delete.
    pub kind: TriggerKind,
    /// The visible row before the mutation (`Update`/`Delete`).
    pub old: Option<Row>,
    /// The row as stored after the mutation (`Insert`/`Update`).
    pub new: Option<Row>,
}

/// Write buffer for the single in-flight transaction (STG-003).
///
/// At most one exists per shard at any time; the guarantee is enforced by
/// the writer mutex held by [`super::Tx`].
#[derive(Debug)]
pub struct TxState {
    /// The tx id this transaction will receive if it commits (TXN-030:
    /// rolled-back transactions do not consume an id).
    pub(crate) tx_id: u64,
    /// Pending operations, keyed by table then encoded PK.
    pub(crate) tables: BTreeMap<TableId, BTreeMap<PkBytes, PendingOp>>,
    /// Undo records for effects applied eagerly to committed structures
    /// (STG-007 rule 3). Replayed in reverse on rollback.
    pub(crate) undo_log: Vec<UndoRecord>,
    /// Trigger events awaiting dispatch (RV-031), in mutation order.
    /// Discarded with the `TxState` on rollback; a commit with undrained
    /// events (raw-`Tx` callers outside the reducer layer) drops them.
    pub(crate) trigger_events: Vec<TriggerEvent>,
}

impl TxState {
    pub(crate) fn new(tx_id: u64) -> Self {
        Self {
            tx_id,
            tables: BTreeMap::new(),
            undo_log: Vec::new(),
            trigger_events: Vec::new(),
        }
    }

    /// Roll back every eagerly applied effect, newest first (STG-007).
    ///
    /// Nothing is applied eagerly — buffered writes cover row effects
    /// (T2.1), and T2.4's secondary-index maintenance rides the commit
    /// merge on the private pre-swap copy (see [`crate::index`] for why
    /// eager maintenance would violate STG-004/FR-10) — so the log is empty
    /// and rollback is pure discard (STG-006). SPEC-010's transactional DDL
    /// pushes its revert records here; the reverse replay order is already
    /// the contract.
    pub(crate) fn revert_eager_effects(&mut self) {
        // `UndoRecord` is uninhabited: dispatching one (impossible) record
        // proves the empty match diverges, so no loop is needed yet. When
        // variants land, replay MUST pop newest-first
        // (`while let Some(...) = pop()`) — the reverse order is the
        // load-bearing part of the STG-007 contract.
        if let Some(record) = self.undo_log.pop() {
            match record {} // no eager effects exist yet (see UndoRecord)
        }
    }
}

/// One undo record for an effect applied eagerly to `CommittedState`
/// structures during a transaction (STG-007 rule 3).
///
/// Currently uninhabited: every row effect is buffered in [`TxState`]
/// (T2.1) and secondary-index maintenance happens inside the commit merge
/// on the private pre-swap state (T2.4 decision, [`crate::index`]) — both
/// are discarded for free on rollback, which the T2.4 property suite
/// verifies against a fresh index rebuild (STG-007 rule 2). SPEC-010
/// (transactional DDL) adds the first variants; rollback replays them in
/// reverse push order via [`TxState::revert_eager_effects`].
#[derive(Debug)]
#[non_exhaustive]
pub enum UndoRecord {}

/// The committed transaction's effect, for the commit log (T2.2) and
/// subscription evaluation (SPEC-005).
///
/// Deterministic order: tables ascend by [`TableId`]; rows within a table
/// ascend by encoded PK.
#[derive(Debug, Clone, PartialEq)]
pub struct TxDiff {
    /// The committed transaction's id (strictly increasing per shard).
    pub tx_id: u64,
    /// Per-table row changes; only touched tables appear.
    pub tables: Vec<TableDiff>,
    /// Auto-inc high-water marks that advanced since the last commit
    /// (STG-040 batched allocation): `(table, new_high_water)`. Persisted by
    /// the commit log as part of this entry (T2.2). Advances made by a
    /// rolled-back transaction ride the *next* commit.
    pub auto_inc: Vec<(TableId, u64)>,
}

impl TxDiff {
    /// True when the transaction changed nothing (no row effects, no
    /// counter advances).
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.auto_inc.is_empty()
    }
}

/// Row changes of one table within a [`TxDiff`].
#[derive(Debug, Clone, PartialEq)]
pub struct TableDiff {
    /// The table.
    pub table_id: TableId,
    /// Rows inserted (full values; an in-place replacement contributes the
    /// new row here and the old row to `deletes`).
    pub inserts: Vec<Row>,
    /// Rows deleted: encoded PK plus the full pre-delete row (subscription
    /// evaluation needs the old values, SPEC-005).
    pub deletes: Vec<(PkBytes, Row)>,
}
