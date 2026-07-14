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
}

impl TxState {
    pub(crate) fn new(tx_id: u64) -> Self {
        Self {
            tx_id,
            tables: BTreeMap::new(),
            undo_log: Vec::new(),
        }
    }

    /// Roll back every eagerly applied effect, newest first (STG-007).
    ///
    /// T2.1 applies nothing eagerly — buffered writes cover everything — so
    /// the log is empty and rollback is pure discard (STG-006). T2.4's index
    /// maintenance and SPEC-010's transactional DDL push their revert
    /// records here; the reverse replay order is already the contract.
    pub(crate) fn revert_eager_effects(&mut self) {
        // `UndoRecord` is uninhabited until T2.4 adds eager index
        // maintenance: dispatching one (impossible) record proves the empty
        // match diverges, so no loop is needed yet. When variants land,
        // replay MUST pop newest-first (`while let Some(...) = pop()`) — the
        // reverse order is the load-bearing part of the STG-007 contract.
        if let Some(record) = self.undo_log.pop() {
            match record {} // no eager effects exist yet (see UndoRecord)
        }
    }
}

/// One undo record for an effect applied eagerly to `CommittedState`
/// structures during a transaction (STG-007 rule 3).
///
/// Currently uninhabited: every T2.1 effect is buffered in [`TxState`] and
/// therefore discarded for free. T2.4 (secondary/spatial index maintenance)
/// and SPEC-010 (transactional DDL) add variants; rollback replays them in
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
