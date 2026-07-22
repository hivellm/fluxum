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
    /// Persistent map: O(1) clone under the commit merge's copy-on-write
    /// (phase6_memstore-structural-sharing).
    map: imbl::OrdMap<Vec<u8>, PkBytes>,
}

impl UniqueIndex {
    /// An empty constraint map over `columns` (ordinals into the table's
    /// schema, registry-validated).
    pub(crate) fn new(columns: &'static [u16]) -> Self {
        Self {
            columns,
            map: imbl::OrdMap::new(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, VisibilityRule};
    use crate::store::row::encode_pk_values;

    static COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "email",
            ty: FluxType::Str,
        },
    ];

    static T: TableSchema = TableSchema {
        name: "CovUnique",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Private,
        partition_by: None,
        unique: &[&[1]],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    fn pk(id: u64) -> PkBytes {
        encode_pk_values(&T, &[RowValue::U64(id)]).unwrap_or_else(|e| panic!("{e}"))
    }

    fn row(id: u64, email: &str) -> Row {
        Row::new(vec![RowValue::U64(id), RowValue::Str(email.into())])
    }

    #[test]
    fn out_of_range_constraint_ordinals_are_an_invariant_breach() {
        let index = UniqueIndex::new(&[9]);
        let err = match index.key_of_values(&[RowValue::U64(1)]) {
            Ok(_) => panic!("out-of-range ordinal keyed"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("#[unique] ordinal 9 out of range"), "{err}");
    }

    #[test]
    fn merging_a_key_claimed_by_another_pk_is_an_invariant_breach() {
        let mut index = UniqueIndex::new(&[1]);
        index
            .insert(&row(1, "a@example.com"), pk(1))
            .unwrap_or_else(|e| panic!("{e}"));
        // Reclaiming a key for the SAME pk is idempotent (update merges).
        index
            .insert(&row(1, "a@example.com"), pk(1))
            .unwrap_or_else(|e| panic!("{e}"));
        // A different pk claiming the same value means the eager TXN-041
        // check missed a conflict — an invariant error, never silent.
        let err = match index.insert(&row(2, "a@example.com"), pk(2)) {
            Ok(()) => panic!("conflicting unique claim merged"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("eager TXN-041 validation missed a conflict"),
            "{err}"
        );
    }
}
