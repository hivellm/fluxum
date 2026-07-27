//! MemStore — the per-shard transactional hot tier (SPEC-002 §2, T2.1).
//!
//! Two logical regions per STG-001: an immutable, atomically swapped
//! [`CommittedState`] snapshot readable by everyone, and at most one in-flight
//! [`TxState`] write buffer (single-writer guarantee, STG-003 / FR-12).
//!
//! # Design decisions (T2.1)
//!
//! - **Lock-free committed reads** (STG-004, FR-10): the committed snapshot
//!   lives in an [`arc_swap::ArcSwap`]. Readers call [`MemStore::snapshot`]
//!   (a wait-free `load_full`) and keep a consistent point-in-time view for
//!   as long as they hold the [`Snapshot`] — they never block on the writer,
//!   and a commit mid-read is invisible (TXN-060/TXN-061 view semantics fall
//!   out for free).
//! - **Copy-on-write at table granularity**: `CommittedState` maps
//!   [`TableId`] → `Arc<TableState>`. Commit clones the table map (cheap Arc
//!   bumps), deep-clones only the tables the transaction touched, applies the
//!   merge, and swaps the root pointer — atomic for readers per STG-005.
//!   Commit cost is O(touched-table size); rows are `Arc`-shared so the clone
//!   copies the key map, not row payloads. This is the documented Phase-2
//!   milestone trade-off (analysis `spacetimedb-code/02`, "What Fluxum will
//!   face" §1): SPEC-015's pager replaces the physical layout under this same
//!   logical API without changing MVCC semantics.
//! - **PK encoding = FluxBIN** (decision per T2.1): primary keys are the
//!   FluxBIN encoding of the PK columns in `TableSchema::primary_key` order,
//!   produced by `fluxum-protocol`'s hand-rolled codec. `fluxum-protocol` is
//!   a pure encoding crate (no network, no I/O, no dependency on this crate),
//!   so SPEC-002's "no network dependencies" holds. Reusing FluxBIN means the
//!   commit log (T2.2) and wire diffs (SPEC-005/006) share one byte-identical
//!   PK form with the store. Note: FluxBIN integers are little-endian, so
//!   `BTreeMap` iteration order over [`PkBytes`] is deterministic byte order,
//!   **not** numeric order — value-ordered range scans go through the T2.4
//!   secondary indexes ([`crate::index`]), whose keys use the memcomparable
//!   transform.
//! - **Single writer**: [`MemStore::begin`] takes a `Mutex` whose guard is
//!   held by the [`Tx`] handle; a second `begin` on the same shard blocks
//!   until the first commits or rolls back (STG-003).
//! - **Rollback** (STG-006/STG-007): nothing is applied eagerly to committed
//!   structures, so discarding `TxState` is exact by construction — deleted
//!   rows were never removed from the snapshot (undelete is free), and
//!   secondary indexes (T2.4) are maintained during the commit merge on the
//!   private pre-swap copy, never eagerly, so after any rollback every index
//!   is bit-identical to a fresh rebuild over `CommittedState` (STG-007
//!   rule 2 — see [`crate::index`] for why eager maintenance would break
//!   STG-004/FR-10). The hook for genuinely eager effects remains in place:
//!   [`UndoRecord`] entries are replayed in reverse on rollback (STG-007
//!   rule 3); SPEC-010's transactional DDL is its expected first user.
//! - **Delete-then-reinsert cancellation** (STG-007 rule 1): `TxState` keys
//!   pending operations by PK as `Insert` / `Delete` / `Update`, so
//!   reinserting a tx-deleted committed row with identical content cancels to
//!   a structural no-op (the committed row is preserved — the commit produces
//!   no diff for it; with the paged store rows are serialized, so "preserved"
//!   is value equality, not `Arc` pointer identity), and insert-then-delete of
//!   a pending row vanishes entirely.
//! - **Constraint overlay** (STG-007 tail): PK-uniqueness (TXN-040) and
//!   `#[unique]` checks (TXN-041, T3.1 — the `unique` submodule) run eagerly at
//!   `insert`/`upsert` time against `CommittedState` ⊕ `TxState` — a
//!   committed row tx-deleted in the same transaction does not conflict,
//!   pending inserts do. Because checks are eager and the writer is single,
//!   the commit merge is validated by construction (TXN-021 step 1 happens
//!   at write time). `upsert` is the TXN-040 exception: an occupied primary
//!   key replaces instead of erroring.
//! - **Auto-inc** (STG-040): per-table counters hand out values from a
//!   pre-allocated batch (`auto_inc_allocation_step`, default 4096). The
//!   high-water mark advances a batch at a time and rides the next commit's
//!   [`TxDiff`] so T2.2 can persist it as an ordinary logged write. Values
//!   consumed by rolled-back transactions are not returned — gaps are normal
//!   and documented; IDs are unique and monotonic, never dense.
//!
//! **Tiered live store (SPEC-015, phase2_tiered-live-store-integration).**
//! [`CommittedState`]'s primary row map is a paged copy-on-write B-tree
//! ([`pager::PagedTree`]), not a resident `imbl::OrdMap`: rows live in the
//! [`pager`]'s on-disk page format and fault in / evict through the buffer
//! pool under `memory.budget`, so steady-state RSS is a function of the budget,
//! never of the resident row count (TIER-004) — the SpacetimeDB-differentiating
//! pillar, and the precondition for the billion-row soak (T7.7). MVCC is
//! preserved by treating each published version as a copy-on-write root: the
//! commit merge rewrites only the touched root-to-leaf paths to fresh pages and
//! hands the superseded pages to a version-scoped reclaimer ([`pager::Reclaimer`],
//! TIER-061), so a snapshot on the old root keeps reading a consistent old tree
//! and its pages free only once no live version can reach them — the paged
//! analogue of the `imbl`+`Arc` structural sharing this map used to give. Rows
//! are stored in the same self-describing MessagePack-over-`LogValue` form the
//! commit log and checkpoints use, so `#[encrypted]`/`#[signed]` columns and
//! mid-migration layouts (SPEC-010) round-trip exactly. Secondary B-tree
//! indexes and `#[unique]` maps are paged onto the same substrate
//! (TIER-050): each is a [`pager::PagedTree`] of
//! `memcomparable index key ++ encoded PK → encoded PK` entries — the exact
//! layout of the checkpoint spill target — maintained copy-on-write inside
//! the commit merge, so index memory counts against the one budget and an
//! index-dominated dataset stays bounded (TIER-070). Spatial and full-text
//! indexes remain resident: they are unpersisted rebuild-from-rows
//! structures (quadtree/R-tree geometry, BM25 posting lists) whose paging
//! (TIER-051) needs a linear-key redesign — the tracked follow-up
//! `phase2_paged-spatial-fulltext-indexes`.
//! [`pager::ColdTable::spill_snapshot`] materializes a published snapshot as a
//! standalone paged copy for the checkpoint path.
//!
//! [`Snapshot`]: committed::Snapshot
//! [`CommittedState`]: committed::CommittedState
//! [`TxState`]: tx::TxState
//! [`UndoRecord`]: tx::UndoRecord
//! [`Tx`]: memstore::Tx

pub mod committed;
pub mod memstore;
pub mod pager;
pub mod row;
pub mod tx;
pub(crate) mod unique;

pub use committed::{CommittedState, Snapshot, TableState};
pub use memstore::{AsOfPoint, MemStore, StoreOptions, Tx};
pub use row::{PkBytes, Row, RowValue};
pub use tx::{TableDiff, TriggerEvent, TriggerKind, TxDiff, TxState, UndoRecord};

/// Stable `u32` table identifier: CRC32 (IEEE) of the table name (STG-050).
///
/// The same table name always produces the same `TableId`, so commit-log
/// entries replay without a live schema lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(u32);

impl TableId {
    /// The stable id of a table name: `crc32(name)` (STG-050).
    pub const fn of(name: &str) -> Self {
        Self(crc32(name.as_bytes()))
    }

    /// Wrap a raw table id (e.g. decoded from a commit-log entry).
    pub const fn from_raw(id: u32) -> Self {
        Self(id)
    }

    /// The raw `u32` value.
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#010x}", self.0)
    }
}

/// CRC32 (IEEE 802.3, reflected, polynomial `0xEDB88320`) — the standard
/// `crc32` most tools compute. Bitwise (table-free): runs once per table or
/// index at startup, so throughput is irrelevant. Shared with
/// [`crate::index::IndexId`] (STG-051).
pub(crate) const fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    let mut i = 0;
    while i < bytes.len() {
        crc ^= bytes[i] as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            bit += 1;
        }
        i += 1;
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_ieee_check_vector() {
        // The canonical CRC32 check value.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn table_id_is_stable_and_name_derived() {
        assert_eq!(TableId::of("User"), TableId::of("User"));
        assert_ne!(TableId::of("User"), TableId::of("Task"));
        assert_eq!(TableId::of("User").as_u32(), crc32(b"User"));
        assert_eq!(TableId::from_raw(7).as_u32(), 7);
        assert_eq!(TableId::from_raw(0xAB).to_string(), "0x000000ab");
    }
}
