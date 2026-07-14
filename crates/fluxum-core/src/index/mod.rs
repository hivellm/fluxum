//! Secondary indexes (SPEC-001 §5, SPEC-002 §2/§7, T2.4).
//!
//! [`BTreeIndex`] implements `#[index(btree(...))]` declarations — single
//! and composite column — over the store's committed rows.
//!
//! # Design decisions (T2.4)
//!
//! - **Indexes live inside `TableState`** (the committed snapshot), exactly
//!   as STG-002 sketches. That is what keeps index reads lock-free (FR-10)
//!   and MVCC-consistent: a [`crate::store::Snapshot`] pins rows *and*
//!   indexes of the same published state, so an index scan can never observe
//!   a row set different from what a full scan of the same snapshot returns.
//! - **Maintenance rides the commit merge** (STG-005 steps 2–4): the commit
//!   builds the next `CommittedState` off to the side — row map and index
//!   updates applied together on the private copy — and publishes both with
//!   one atomic swap. Nothing is applied eagerly to shared structures during
//!   the transaction, so rollback remains pure `TxState` discard and the
//!   STG-007 rule-2 property ("after rollback every index is bit-identical
//!   to a freshly rebuilt index over `CommittedState`") holds by
//!   construction; `verify_index_integrity` and the T2.4 property suite
//!   prove it. The [`crate::store::UndoRecord`] hook therefore stays
//!   uninhabited — an eager design would leak uncommitted index entries to
//!   concurrent snapshot readers (violating STG-004) or force index reads
//!   through the writer lock (violating FR-10).
//! - **Memcomparable keys** (deferred from T2.1): index keys are an
//!   order-preserving byte transform of the indexed column values —
//!   `memcmp` on encoded keys equals the natural ordering of the values —
//!   so range scans and composite prefix scans are plain byte-range
//!   iteration. See [`btree`] for the per-type transform. This is deliberate
//!   groundwork for T2.8 (SPEC-015 TIER-050): pages of a byte-ordered map
//!   can be evicted and compared without decoding, and the index map is
//!   reached only through the [`BTreeIndex`] API, so the paged
//!   implementation replaces the in-memory `BTreeMap` behind the same
//!   surface.
//! - **Stable [`IndexId`]s** (STG-051): CRC32 over
//!   `table_name \0 col_1 \0 … col_n`, so an index id survives restarts and
//!   is derivable from the schema alone.

pub mod btree;

pub use btree::BTreeIndex;

/// Stable `u32` index identifier (STG-051): CRC32 (IEEE) of
/// `table_name \0 column_1 \0 … \0 column_n`.
///
/// Deterministic from the schema, so commit-log entries and paged-index
/// metadata (T2.8) can reference an index without a live schema lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(u32);

impl IndexId {
    /// The stable id of a B-tree index on `table_name` over `column_names`
    /// (in declared key order).
    pub fn of(table_name: &str, column_names: &[&str]) -> Self {
        let mut bytes = Vec::with_capacity(
            table_name.len() + column_names.iter().map(|c| c.len() + 1).sum::<usize>(),
        );
        bytes.extend_from_slice(table_name.as_bytes());
        for column in column_names {
            bytes.push(0);
            bytes.extend_from_slice(column.as_bytes());
        }
        Self(crate::store::crc32(&bytes))
    }

    /// Wrap a raw index id (e.g. decoded from paged-index metadata).
    pub const fn from_raw(id: u32) -> Self {
        Self(id)
    }

    /// The raw `u32` value.
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#010x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_id_is_stable_and_order_sensitive() {
        assert_eq!(
            IndexId::of("Msg", &["channel", "sent_at"]),
            IndexId::of("Msg", &["channel", "sent_at"])
        );
        // Column order is part of the identity (a btree(a, b) is not a
        // btree(b, a)).
        assert_ne!(
            IndexId::of("Msg", &["channel", "sent_at"]),
            IndexId::of("Msg", &["sent_at", "channel"])
        );
        // The table name is part of the identity.
        assert_ne!(
            IndexId::of("Msg", &["channel"]),
            IndexId::of("Log", &["channel"])
        );
        // The separator prevents concatenation ambiguity.
        assert_ne!(
            IndexId::of("Msg", &["ab", "c"]),
            IndexId::of("Msg", &["a", "bc"])
        );
        assert_eq!(IndexId::from_raw(0xAB).as_u32(), 0xAB);
        assert_eq!(IndexId::from_raw(0xAB).to_string(), "0x000000ab");
    }
}
