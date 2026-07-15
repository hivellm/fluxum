//! [`UniqueIndex`] — the committed-side lookup structure behind `#[unique]`
//! constraints (DM-006, TXN-041) — the secondary-constraint work T2.1
//! deferred to T3.1 (see [`super`]'s "Constraint overlay" decision).
//!
//! One `UniqueIndex` exists per declared constraint (single or multi-column),
//! mapping the memcomparable encoding of the constraint columns to the PK of
//! the row owning that value. It lives inside the committed
//! [`super::TableState`] and follows exactly the T2.4 B-tree index
//! discipline: checked eagerly at write time against the STG-007 overlay
//! (`CommittedState` ⊕ `TxState`), maintained inside the commit merge on the
//! private pre-swap copy — never eagerly — so rollback leaves it bit-identical
//! to a fresh rebuild over `CommittedState` (STG-007 rule 2).
//!
//! Keys reuse the [`crate::index::btree`] memcomparable transform: equality
//! is all a constraint needs, and the transform already gives every value —
//! including `NaN` and `None` — one deterministic, prefix-free encoding.

use std::collections::BTreeMap;

use crate::error::{FluxumError, Result};
use crate::index::btree;
use crate::schema::TableSchema;
use crate::store::row::{PkBytes, Row, RowValue};

/// One `#[unique]` constraint's committed value map: memcomparable key of
/// the constraint columns → the PK of the row owning that value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UniqueIndex {
    /// Constraint column ordinals in declared order (DM-006).
    columns: &'static [u16],
    map: BTreeMap<Vec<u8>, PkBytes>,
}

impl UniqueIndex {
    /// An empty constraint map over `columns` (ordinals into the table's
    /// schema, registry-validated).
    pub(crate) fn new(columns: &'static [u16]) -> Self {
        Self {
            columns,
            map: BTreeMap::new(),
        }
    }

    /// The constraint's column ordinals in declared order.
    pub(crate) fn columns(&self) -> &'static [u16] {
        self.columns
    }

    /// The memcomparable constraint key of a full row's `values`.
    pub(crate) fn key_of_values(&self, values: &[RowValue]) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        for &ordinal in self.columns {
            let value = values.get(usize::from(ordinal)).ok_or_else(|| {
                FluxumError::Storage(format!(
                    "internal invariant violated: #[unique] ordinal {ordinal} out of range \
                     for a row of {} columns",
                    values.len()
                ))
            })?;
            btree::encode_value(value, &mut key);
        }
        Ok(key)
    }

    /// The PK owning `key` in the committed state, if any.
    pub(crate) fn owner(&self, key: &[u8]) -> Option<&PkBytes> {
        self.map.get(key)
    }

    /// Claim `row`'s constraint value for `pk` (commit merge, insert side).
    ///
    /// Violations are rejected eagerly at write time (TXN-041), so an
    /// occupied key here is an internal invariant failure, never a user
    /// error.
    pub(crate) fn insert(&mut self, row: &Row, pk: PkBytes) -> Result<()> {
        let key = self.key_of_values(row.values())?;
        if let Some(existing) = self.map.insert(key, pk.clone())
            && existing != pk
        {
            return Err(FluxumError::Storage(format!(
                "internal invariant violated: unique key claimed by pk {existing} while \
                 merging pk {pk} — eager TXN-041 validation missed a conflict"
            )));
        }
        Ok(())
    }

    /// Release `row`'s constraint value if `pk` owns it (commit merge,
    /// delete side). Releasing a key owned by another PK is a no-op: the
    /// two-pass merge removes every vacated key before any claim, so a
    /// same-transaction value move never drops the new owner's entry.
    pub(crate) fn remove(&mut self, row: &Row, pk: &PkBytes) -> Result<()> {
        let key = self.key_of_values(row.values())?;
        if self.map.get(&key).is_some_and(|owner| owner == pk) {
            self.map.remove(&key);
        }
        Ok(())
    }
}

/// The TXN-041 violation error: names the table, the constraint columns,
/// and the conflicting values.
pub(crate) fn violation_error(
    schema: &TableSchema,
    columns: &[u16],
    values: &[RowValue],
) -> FluxumError {
    let names: Vec<&str> = columns
        .iter()
        .filter_map(|&ordinal| schema.column(ordinal).map(|c| c.name))
        .collect();
    let shown: Vec<String> = columns
        .iter()
        .filter_map(|&ordinal| values.get(usize::from(ordinal)))
        .map(ToString::to_string)
        .collect();
    FluxumError::Storage(format!(
        "unique constraint violation: table={} columns=({}) value=({})",
        schema.name,
        names.join(", "),
        shown.join(", ")
    ))
}
