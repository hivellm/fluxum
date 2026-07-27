//! [`PagedTree`] — a B-tree whose every node is one cold-tier page, faulted
//! and evicted through the buffer pool (TIER-050).
//!
//! This is the novel piece relative to SpacetimeDB: its indexes are
//! RAM-bound heap structures with no fault-in seam, whereas every node here
//! — interior or leaf, primary or secondary, B-tree or spatial linear key —
//! participates in the pool, eviction, and the memory budget exactly like a
//! data page. Node references are logical `page_id`s (never heap pointers),
//! so an evicted-and-refaulted page reappears at the same logical
//! coordinates (eviction-safe addressing, TIER-050).
//!
//! One tree maps byte keys to byte values:
//!
//! - **primary tree**: key = FluxBIN-encoded PK, value = FluxBIN row
//!   (TIER-021 "FluxBIN rows in leaf payloads");
//! - **secondary/spatial index tree**: key = memcomparable index key ++
//!   encoded PK, value = encoded PK (SPEC-008's linear-quadtree keys map
//!   onto the same structure, TIER-051). Index trees set the TIER-021 index
//!   flag on **all** their nodes; the primary tree only on interior nodes.
//!
//! # Node payload encoding (freeze surface, versioned with the page format)
//!
//! The first payload byte is the node kind; entries follow back to back,
//! sorted by (full) key, `row_count` of them (TIER-021 header field):
//!
//! - **Leaf** (`0x4C`, `'L'`): entry = `key_len: u16 | tag: u8 | key |` then
//!   per tag: `0` (inline value): `val_len: u32 | value`; `1` (overflow
//!   value): `total_len: u64 | head_page_id: u64` — the value lives in a
//!   chain of overflow pages (TIER-026). Format version 2 adds the
//!   **overflow-key** tags: `2` (inline value) and `3` (overflow value) —
//!   `key_len`/`key` hold a bounded, order-preserving *routing prefix* of
//!   the full key, followed by `full_key_len: u32 | key_head_page_id: u64`
//!   (the full key in its own overflow chain), then the value part as for
//!   tags 0/1.
//! - **Interior** (`0x49`, `'I'`): entry = `key_len: u16 | key |
//!   child_page_id: u64`. Entry *i* routes keys in `[key_i, key_{i+1})`;
//!   keys below `key_0` route to entry 0. Version 2: `key_len` bit 15 set
//!   marks an overflow separator — `key_len & 0x7FFF` prefix bytes, then
//!   `full_key_len: u32 | key_head_page_id: u64 | child_page_id: u64`.
//!   (Bit 15 was never set in version-1 pages: keys were capped far below
//!   32 KiB.)
//! - **Overflow** (header flag bit 3): payload = `next_page_id: u64 |
//!   chunk` (`next = 0` ends the chain). `row_count = 0`. Key and value
//!   chains share this format.
//!
//! # Long keys (overflow keys)
//!
//! Keys longer than [`max_inline_key`](PagedTree::max_inline_key)
//! (`node_budget/8`) keep only their routing prefix in the node, so entry
//! sizes stay bounded and **interior fan-out is ≥ 2 for any key mix** —
//! `bulk_load` level building strictly shrinks and depth stays logarithmic
//! even for uniformly multi-KB keys (the livelock that motivated the old
//! ~2 KB key cap). Ordering is exact, not approximate: a probe compares
//! against the prefix and is decided there unless it *extends* the prefix,
//! and only that tie faults the full-key chain. A same-key value update
//! reuses the entry's existing key chain (no chain churn); replacing or
//! deleting the entry retires the chain through the same superseded-page
//! protocol as value chains (TIER-061).
//!
//! Deletion removes entries without rebalancing (a sparse node stays valid
//! and is compacted by the next checkpoint rewrite); correctness never
//! depends on fill factor.

use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::store::TableId;

use super::Pager;
use super::format::{FLAG_INDEX, FLAG_OVERFLOW, PAGE_HEADER_LEN, PageHeader, encode_page};
use super::pool::PageGuard;

/// Leaf node-kind byte (`'L'`).
const NODE_LEAF: u8 = 0x4C;
/// Interior node-kind byte (`'I'`).
const NODE_INTERIOR: u8 = 0x49;

/// Nil page id (chain terminator / no page).
const NIL: u64 = 0;

/// Interior `key_len` bit 15: the separator is an overflow key (its full
/// bytes live in a chain). Never set by version-1 writers — keys were
/// capped far below 32 KiB — so the bit is a safe format extension.
const KEY_LEN_OVERFLOW: u16 = 1 << 15;
/// Mask for the stored (prefix) length in an interior `key_len`.
const KEY_LEN_MASK: u16 = KEY_LEN_OVERFLOW - 1;

/// A decoded leaf value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeafValue {
    /// Value bytes stored in the leaf itself.
    Inline(Vec<u8>),
    /// Value stored in an overflow chain (TIER-026).
    Overflow { total_len: u64, head: u64 },
}

/// A decoded entry key: stored inline, or — for keys longer than
/// [`PagedTree::max_inline_key`] — a bounded routing prefix in the node
/// plus the full key in an overflow chain (module docs, "Long keys").
///
/// Invariant: an overflow key's full length is strictly greater than its
/// prefix length, so a probe that is `<=` the prefix in length and matches
/// it bytewise always sorts *before* the full key.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EntryKey {
    Inline(Vec<u8>),
    Overflow {
        prefix: Vec<u8>,
        total_len: u32,
        head: u64,
    },
}

impl EntryKey {
    /// The bytes stored in the node (full key, or routing prefix).
    fn stored(&self) -> &[u8] {
        match self {
            EntryKey::Inline(k) => k,
            EntryKey::Overflow { prefix, .. } => prefix,
        }
    }
}

type LeafEntries = Vec<(EntryKey, LeafValue)>;
type InteriorEntries = Vec<(EntryKey, u64)>;

/// Scan visitor: `(key, value) -> keep_going`.
pub type ScanFn<'a> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'a;

/// A decoded B-tree node.
#[derive(Debug)]
enum Node {
    Leaf(LeafEntries),
    Interior(InteriorEntries),
}

/// How a copy-on-write rewrite replaced a subtree root: a single fresh page,
/// or a split into two fresh pages the parent must adopt.
#[derive(Debug)]
enum Cow {
    /// The subtree root was rewritten to this fresh page id (old superseded).
    Node(u64),
    /// The subtree root split; `sep` is the right half's first key.
    Split { left: u64, sep: Vec<u8>, right: u64 },
}

/// A paged B-tree over one table's page file (TIER-050).
///
/// Reads (`get`, `scan`) are `&self`; mutations are `&mut self` (single-writer,
/// matching STG-003). Writes are **copy-on-write**: every node from the root
/// to the touched leaf is rewritten to a *fresh* page id, producing a new
/// [`root_page_id`](Self::root_page_id); the pages the write superseded are
/// never overwritten in place. A [`PagedTree`] handle is therefore a cheap,
/// immutable *version* — `clone` shares the pager and points at the same root,
/// so a snapshot taken before a commit keeps reading a consistent old tree
/// while a later version writes new pages. The superseded page ids are
/// reported to the caller ([`insert_cow`](Self::insert_cow) /
/// [`delete_cow`](Self::delete_cow)) so a version-scoped reclaimer frees them
/// only once no snapshot pins the version that still reaches them — the paged
/// analogue of the `imbl`+`Arc` structural sharing the resident store used
/// (SPEC-015 TIER-061, SPEC-002 STG-004).
///
/// The `insert`/`delete` convenience wrappers free the superseded pages
/// immediately; they are for single-version callers (the checkpoint spill
/// target, tests) with no concurrent readers of the old root.
#[derive(Debug, Clone)]
pub struct PagedTree {
    pager: Arc<Pager>,
    table_id: TableId,
    /// Whether leaf nodes carry the TIER-021 index flag (secondary/spatial
    /// index trees). Interior nodes always do.
    index_tree: bool,
    /// This version's root page id. Plain (not atomic): each handle is one
    /// immutable version; a `&mut self` write publishes a new root on this
    /// handle alone, leaving clones (older versions) on their own roots.
    root: u64,
}

impl PagedTree {
    /// Create an empty tree: one empty leaf as root.
    pub fn create(pager: &Arc<Pager>, table_id: TableId, index_tree: bool) -> Result<Self> {
        let mut tree = Self {
            pager: Arc::clone(pager),
            table_id,
            index_tree,
            root: NIL,
        };
        tree.root = tree.write_new_node(&Node::Leaf(Vec::new()))?;
        Ok(tree)
    }

    /// The current root page id (the version identity; diagnostics / tests).
    pub fn root_page_id(&self) -> u64 {
        self.root
    }

    /// Adopt `root` as this handle's version root — the recovery seam
    /// (checkpoint manifest restores the persisted root, TIER-061).
    pub fn with_root(pager: &Arc<Pager>, table_id: TableId, index_tree: bool, root: u64) -> Self {
        Self {
            pager: Arc::clone(pager),
            table_id,
            index_tree,
            root,
        }
    }

    /// The table this tree's pages belong to.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Usable payload bytes per node (page minus header minus kind byte).
    fn node_budget(&self) -> usize {
        self.pager.page_size() - PAGE_HEADER_LEN - 1
    }

    /// Largest leaf entry stored inline; bigger values go to overflow
    /// chains so any node always fits at least four entries.
    fn max_inline_entry(&self) -> usize {
        self.node_budget() / 4
    }

    /// Largest key stored fully inline in a node; longer keys keep this
    /// many routing-prefix bytes inline and the full key in an overflow
    /// chain (module docs, "Long keys"). Bounding the in-node key bytes at
    /// `budget/8` keeps every entry small enough that two interior entries
    /// always fit one node — fan-out ≥ 2 for any key mix, so `bulk_load`
    /// level building strictly shrinks and depth stays logarithmic. There
    /// is no key-length cap: arbitrarily long keys are exact (full-key
    /// tie-break), never truncated.
    fn max_inline_key(&self) -> usize {
        self.node_budget() / 8
    }

    /// Bytes of value chunk per overflow page.
    fn overflow_chunk(&self) -> usize {
        self.pager.page_size() - PAGE_HEADER_LEN - 8
    }

    // --- reads ---------------------------------------------------------

    /// Point lookup: the value stored under `key`, faulting node (and
    /// overflow) pages on demand.
    ///
    /// This is the TIER-014 hot path: on pool hits the descent is
    /// allocation-free until the value copy — nodes are searched in place
    /// over the pinned frame's bytes, never parsed into owned entries
    /// (that keeps a resident point lookup < 1 µs, NFR-02).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut page_id = self.root;
        loop {
            let guard = self.pager.fault(self.table_id, page_id)?;
            let payload = payload_of(&guard);
            let (kind, rest) = payload
                .split_first()
                .ok_or_else(|| node_err(self.table_id, page_id, "empty node payload"))?;
            match *kind {
                NODE_INTERIOR => {
                    match raw_interior_route(rest, key)
                        .ok_or_else(|| node_err(self.table_id, page_id, "malformed interior"))?
                    {
                        RawRoute::Child(child) => page_id = child,
                        // An overflow separator tied on its prefix: decide
                        // with full keys on the parsed (chain-faulting) path.
                        RawRoute::NeedsFullKeys => {
                            let node = parse_node(&guard)?;
                            drop(guard);
                            let Node::Interior(entries) = node else {
                                return Err(node_err(self.table_id, page_id, "kind mismatch"));
                            };
                            page_id = entries[self.route_index(&entries, key)?].1;
                        }
                    }
                }
                NODE_LEAF => {
                    let found = raw_leaf_find(rest, key)
                        .ok_or_else(|| node_err(self.table_id, page_id, "malformed leaf"))?;
                    let found = match found {
                        RawFound::Absent => return Ok(None),
                        RawFound::Found(value) => value,
                        // A prefix tie against an overflow key: re-search
                        // this leaf with full-key comparisons.
                        RawFound::NeedsFullKeys => {
                            let node = parse_node(&guard)?;
                            drop(guard);
                            let Node::Leaf(entries) = node else {
                                return Err(node_err(self.table_id, page_id, "kind mismatch"));
                            };
                            return match self.search_leaf(&entries, key)? {
                                Ok(idx) => self.materialize(&entries[idx].1).map(Some),
                                Err(_) => Ok(None),
                            };
                        }
                    };
                    return match found {
                        RawLeafValue::Inline(bytes) => Ok(Some(bytes.to_vec())),
                        RawLeafValue::Overflow { total_len, head } => {
                            drop(guard);
                            self.materialize(&LeafValue::Overflow { total_len, head })
                                .map(Some)
                        }
                    };
                }
                other => {
                    return Err(node_err(
                        self.table_id,
                        page_id,
                        &format!("unknown node kind {other:#04x}"),
                    ));
                }
            }
        }
    }

    /// Compare a probe key against a stored entry key, faulting the full-key
    /// chain only when the probe extends an overflow key's routing prefix
    /// (the sole undecidable-by-prefix case — module docs, "Long keys").
    fn cmp_probe(&self, probe: &[u8], stored: &EntryKey) -> Result<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match stored {
            EntryKey::Inline(k) => Ok(probe.cmp(k.as_slice())),
            EntryKey::Overflow {
                prefix,
                total_len,
                head,
            } => {
                let l = prefix.len().min(probe.len());
                match probe[..l].cmp(&prefix[..l]) {
                    Ordering::Equal if probe.len() <= prefix.len() => {
                        // The full key strictly extends its prefix, so a
                        // probe no longer than the prefix sorts first.
                        Ok(Ordering::Less)
                    }
                    Ordering::Equal => {
                        let full = self.materialize_chain(u64::from(*total_len), *head)?;
                        Ok(probe.cmp(full.as_slice()))
                    }
                    other => Ok(other),
                }
            }
        }
    }

    /// The full bytes of an entry key (inline slice, or the materialized
    /// overflow chain).
    fn full_key<'k>(&self, key: &'k EntryKey) -> Result<std::borrow::Cow<'k, [u8]>> {
        match key {
            EntryKey::Inline(k) => Ok(std::borrow::Cow::Borrowed(k.as_slice())),
            EntryKey::Overflow {
                total_len, head, ..
            } => Ok(std::borrow::Cow::Owned(
                self.materialize_chain(u64::from(*total_len), *head)?,
            )),
        }
    }

    /// Binary search of a parsed leaf by full key order.
    fn search_leaf(
        &self,
        entries: &LeafEntries,
        probe: &[u8],
    ) -> Result<std::result::Result<usize, usize>> {
        use std::cmp::Ordering;
        let (mut lo, mut hi) = (0usize, entries.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.cmp_probe(probe, &entries[mid].0)? {
                Ordering::Less => hi = mid,
                Ordering::Greater => lo = mid + 1,
                Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(lo))
    }

    /// In-order scan of `[start, end)` (`end: None` = unbounded). `f`
    /// returns `false` to stop early; the scan result is whether it ran to
    /// completion. Faulted scan pages enter the pool scan-resistant
    /// (TIER-015 is handled by the pool's insert policy for bulk loads;
    /// scans fault with normal reference semantics — frequently scanned
    /// ranges staying hot is intended).
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, f: &mut ScanFn<'_>) -> Result<bool> {
        self.scan_node(self.root, start, end, f)
    }

    fn scan_node(
        &self,
        page_id: u64,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut ScanFn<'_>,
    ) -> Result<bool> {
        use std::cmp::Ordering;
        let node = {
            let guard = self.pager.fault(self.table_id, page_id)?;
            parse_node(&guard)?
        };
        match node {
            Node::Leaf(entries) => {
                for (k, v) in &entries {
                    // stored-key comparisons via cmp_probe (probe = bound):
                    // k < start  ⟺  cmp_probe(start, k) == Greater
                    if self.cmp_probe(start, k)? == Ordering::Greater {
                        continue;
                    }
                    if let Some(end) = end
                        && self.cmp_probe(end, k)? != Ordering::Greater
                    {
                        break;
                    }
                    let key = self.full_key(k)?;
                    let value = self.materialize(v)?;
                    if !f(&key, &value)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Node::Interior(entries) => {
                // Rightmost child whose separator is <= start (route_index),
                // then forward until a separator passes `end`.
                let from = self.route_index(&entries, start)?;
                for (i, (k, child)) in entries.iter().enumerate().skip(from) {
                    if i > from
                        && let Some(end) = end
                        && self.cmp_probe(end, k)? != Ordering::Greater
                    {
                        break;
                    }
                    if !self.scan_node(*child, start, end, f)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }

    /// Read a leaf value, following the overflow chain when needed.
    fn materialize(&self, value: &LeafValue) -> Result<Vec<u8>> {
        match value {
            LeafValue::Inline(bytes) => Ok(bytes.clone()),
            LeafValue::Overflow { total_len, head } => self.materialize_chain(*total_len, *head),
        }
    }

    /// Read a whole overflow chain (key or value) into owned bytes.
    fn materialize_chain(&self, total_len: u64, head: u64) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(usize::try_from(total_len).unwrap_or(0));
        let mut page_id = head;
        while page_id != NIL {
            let guard = self.pager.fault(self.table_id, page_id)?;
            let payload = payload_of(&guard);
            if payload.len() < 8 {
                return Err(node_err(self.table_id, page_id, "overflow page too short"));
            }
            page_id = u64::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ]);
            out.extend_from_slice(&payload[8..]);
        }
        if out.len() as u64 != total_len {
            return Err(node_err(
                self.table_id,
                head,
                "overflow chain length mismatch",
            ));
        }
        Ok(out)
    }

    // --- writes (copy-on-write) ----------------------------------------

    /// Reject an out-of-contract key before any page is touched. Length is
    /// unbounded (long keys overflow, module docs); only emptiness is
    /// rejected — an empty key cannot be routed (the low sentinel of a root
    /// split is empty by design and never collides with a real key).
    fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(FluxumError::Storage(
                "paged tree keys must be non-empty".into(),
            ));
        }
        if u32::try_from(key.len()).is_err() {
            return Err(FluxumError::Storage(
                "paged tree keys are limited to u32 length".into(),
            ));
        }
        Ok(())
    }

    /// Build the stored representation of `key`: inline when it fits, else
    /// a routing prefix + a fresh full-key overflow chain.
    fn make_key(&self, key: &[u8]) -> Result<EntryKey> {
        if key.len() <= self.max_inline_key() {
            return Ok(EntryKey::Inline(key.to_vec()));
        }
        let head = self.store_chain(key)?;
        Ok(EntryKey::Overflow {
            prefix: key[..self.max_inline_key()].to_vec(),
            total_len: key.len() as u32,
            head,
        })
    }

    /// Record a superseded entry key's overflow chain (if any) for deferred
    /// reclaim — mirror of [`supersede_value`](Self::supersede_value).
    fn supersede_key(&self, key: &EntryKey, superseded: &mut Vec<u64>) -> Result<()> {
        if let EntryKey::Overflow { head, .. } = key {
            self.supersede_chain(*head, superseded)?;
        }
        Ok(())
    }

    /// Copy-on-write insert/replace of `key → value`. The root-to-leaf path is
    /// rewritten to fresh pages (a new [`root_page_id`](Self::root_page_id));
    /// the pages it superseded — old path nodes and any replaced overflow
    /// chain — are appended to `superseded` for the caller's version-scoped
    /// reclaimer, never freed here (a snapshot on the old root still reads
    /// them). Splits propagate up and may grow a new root.
    pub fn insert_cow(
        &mut self,
        key: &[u8],
        value: &[u8],
        superseded: &mut Vec<u64>,
    ) -> Result<()> {
        self.check_key(key)?;
        let root = self.root;
        self.root = match self.insert_into_cow(root, key, value, superseded)? {
            Cow::Node(page) => page,
            Cow::Split { left, sep, right } => {
                // Root split: a new interior root whose low-sentinel first
                // entry routes everything below `sep` left.
                let sep_key = self.make_key(&sep)?;
                let entries: InteriorEntries =
                    vec![(EntryKey::Inline(Vec::new()), left), (sep_key, right)];
                self.write_new_node(&Node::Interior(entries))?
            }
        };
        Ok(())
    }

    /// Insert/replace, freeing superseded pages immediately (single-version
    /// callers with no concurrent readers of the old root).
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut superseded = Vec::new();
        self.insert_cow(key, value, &mut superseded)?;
        self.free_superseded(&superseded)?;
        Ok(())
    }

    /// Recursive CoW insert; returns how the rewritten subtree root replaced
    /// `page_id` (a fresh page, or a split into two fresh pages). `page_id`
    /// itself is recorded superseded.
    fn insert_into_cow(
        &mut self,
        page_id: u64,
        key: &[u8],
        value: &[u8],
        superseded: &mut Vec<u64>,
    ) -> Result<Cow> {
        let node = parse_node(&self.pager.fault(self.table_id, page_id)?)?;
        match node {
            Node::Leaf(mut entries) => {
                match self.search_leaf(&entries, key)? {
                    Ok(idx) => {
                        // Replace: the entry key (and its chain, if any) is
                        // identical — reuse it; only the superseded value's
                        // overflow chain (if any) is retired, not freed — an
                        // old snapshot's leaf still points at it.
                        let leaf_value = self.store_value(&entries[idx].0, value)?;
                        let old = std::mem::replace(&mut entries[idx].1, leaf_value);
                        self.supersede_value(&old, superseded)?;
                    }
                    Err(idx) => {
                        let entry_key = self.make_key(key)?;
                        let leaf_value = self.store_value(&entry_key, value)?;
                        entries.insert(idx, (entry_key, leaf_value));
                    }
                }
                superseded.push(page_id);
                self.write_or_split_new(Node::Leaf(entries))
            }
            Node::Interior(mut entries) => {
                let idx = self.route_index(&entries, key)?;
                let child = entries[idx].1;
                match self.insert_into_cow(child, key, value, superseded)? {
                    Cow::Node(new_child) => entries[idx].1 = new_child,
                    Cow::Split { left, sep, right } => {
                        entries[idx].1 = left;
                        let sep_key = self.make_key(&sep)?;
                        entries.insert(idx + 1, (sep_key, right));
                    }
                }
                superseded.push(page_id);
                self.write_or_split_new(Node::Interior(entries))
            }
        }
    }

    /// The interior entry index routing `key`: the rightmost entry with
    /// `sep_i <= key`, or entry 0 when `key` sorts below every separator.
    fn route_index(&self, entries: &InteriorEntries, key: &[u8]) -> Result<usize> {
        use std::cmp::Ordering;
        if entries.is_empty() {
            return Err(FluxumError::Storage(
                "interior node with zero entries".into(),
            ));
        }
        // sep <= key  ⟺  cmp_probe(key, sep) != Less
        let (mut lo, mut hi) = (0usize, entries.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.cmp_probe(key, &entries[mid].0)? == Ordering::Less {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(lo.saturating_sub(1))
    }

    /// Copy-on-write delete of `key`. Returns whether it was present; when it
    /// was, the root-to-leaf path is rewritten to fresh pages and the pages it
    /// superseded (old path nodes + the deleted value's overflow chain) are
    /// appended to `superseded`. A missing key rewrites nothing. No
    /// rebalancing (module docs); an emptied leaf stays a valid routable node.
    pub fn delete_cow(&mut self, key: &[u8], superseded: &mut Vec<u64>) -> Result<bool> {
        let root = self.root;
        match self.delete_into_cow(root, key, superseded)? {
            Some(new_root) => {
                self.root = new_root;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Delete, freeing superseded pages immediately (single-version callers).
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut superseded = Vec::new();
        let hit = self.delete_cow(key, &mut superseded)?;
        self.free_superseded(&superseded)?;
        Ok(hit)
    }

    /// Recursive CoW delete; `Ok(None)` = key absent (nothing rewritten),
    /// `Ok(Some(page))` = the fresh page replacing `page_id` on the path.
    /// Delete only shrinks nodes, so it never splits.
    fn delete_into_cow(
        &mut self,
        page_id: u64,
        key: &[u8],
        superseded: &mut Vec<u64>,
    ) -> Result<Option<u64>> {
        let node = parse_node(&self.pager.fault(self.table_id, page_id)?)?;
        match node {
            Node::Leaf(mut entries) => {
                let Ok(idx) = self.search_leaf(&entries, key)? else {
                    return Ok(None);
                };
                let (old_key, old) = entries.remove(idx);
                self.supersede_key(&old_key, superseded)?;
                self.supersede_value(&old, superseded)?;
                superseded.push(page_id);
                Ok(Some(self.write_new_node(&Node::Leaf(entries))?))
            }
            Node::Interior(mut entries) => {
                let idx = self.route_index(&entries, key)?;
                let child = entries[idx].1;
                let Some(new_child) = self.delete_into_cow(child, key, superseded)? else {
                    return Ok(None);
                };
                entries[idx].1 = new_child;
                superseded.push(page_id);
                Ok(Some(self.write_new_node(&Node::Interior(entries))?))
            }
        }
    }

    /// Free a batch of superseded page ids (the convenience wrappers' immediate
    /// reclaim, and the version reclaimer's deferred reclaim).
    fn free_superseded(&self, page_ids: &[u64]) -> Result<()> {
        for &page_id in page_ids {
            self.pager.free_page(self.table_id, page_id)?;
        }
        Ok(())
    }

    /// Bulk-load a **sorted, unique-key** stream into an **empty** tree:
    /// leaves are packed full left to right, then interior levels are built
    /// bottom-up. Pages enter the pool scan-resistant (`referenced` clear,
    /// TIER-015), so a load larger than the pool evicts its own tail, not
    /// the resident working set.
    pub fn bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let budget = self.node_budget();
        let mut level: InteriorEntries = Vec::new(); // (first_key, page_id)
        let mut leaf: LeafEntries = Vec::new();
        let mut leaf_bytes = 0usize;
        let mut last_key: Option<Vec<u8>> = None;

        for (key, value) in entries {
            self.check_key(&key).map_err(|_| {
                FluxumError::Storage(format!(
                    "bulk_load key of {} bytes is empty or exceeds u32",
                    key.len()
                ))
            })?;
            if let Some(last) = &last_key
                && *last >= key
            {
                return Err(FluxumError::Storage(
                    "bulk_load input must be strictly sorted by key".into(),
                ));
            }
            last_key = Some(key.clone());
            let entry_key = self.make_key(&key)?;
            let leaf_value = self.store_value(&entry_key, &value)?;
            let size = leaf_entry_size(&entry_key, &leaf_value);
            if leaf_bytes + size > budget && !leaf.is_empty() {
                // The level entry key is a fresh copy of the node's first
                // key (a separator needs its own chain when long).
                let first = self.make_key(&self.full_key(&leaf[0].0)?)?;
                let page = self.write_new_node(&Node::Leaf(std::mem::take(&mut leaf)))?;
                level.push((first, page));
                leaf_bytes = 0;
            }
            leaf_bytes += size;
            leaf.push((entry_key, leaf_value));
        }
        let first = match leaf.first() {
            Some((k, _)) => self.make_key(&self.full_key(k)?)?,
            None => EntryKey::Inline(Vec::new()),
        };
        let page = self.write_new_node(&Node::Leaf(leaf))?;
        level.push((first, page));

        // Build interior levels until a single node remains.
        while level.len() > 1 {
            let mut next: InteriorEntries = Vec::new();
            let mut node: InteriorEntries = Vec::new();
            let mut node_bytes = 0usize;
            for (key, child) in level {
                let size = interior_entry_size(&key);
                if node_bytes + size > budget && !node.is_empty() {
                    let first = self.make_key(&self.full_key(&node[0].0)?)?;
                    let page = self.write_new_node(&Node::Interior(std::mem::take(&mut node)))?;
                    next.push((first, page));
                    node_bytes = 0;
                }
                node_bytes += size;
                node.push((key, child));
            }
            let first = match node.first() {
                Some((k, _)) => self.make_key(&self.full_key(k)?)?,
                None => EntryKey::Inline(Vec::new()),
            };
            let page = self.write_new_node(&Node::Interior(node))?;
            next.push((first, page));
            level = next;
        }
        let (_, new_root) = level.remove(0);
        let old_root = self.root;
        self.root = new_root;
        // bulk_load initializes a fresh tree (create → bulk_load) or a spill
        // target; no snapshot reads the throwaway empty-leaf root, so freeing
        // it at once is safe.
        if old_root != NIL {
            self.pager.free_page(self.table_id, old_root)?;
        }
        Ok(())
    }

    // --- node plumbing --------------------------------------------------

    /// Store a value for a leaf entry: inline when it fits, otherwise as an
    /// overflow chain (TIER-026).
    fn store_value(&self, key: &EntryKey, value: &[u8]) -> Result<LeafValue> {
        let inline = LeafValue::Inline(value.to_vec());
        if leaf_entry_size(key, &inline) <= self.max_inline_entry() {
            return Ok(inline);
        }
        Ok(LeafValue::Overflow {
            total_len: value.len() as u64,
            head: self.store_chain(value)?,
        })
    }

    /// Write `bytes` as a fresh overflow chain and return its head page id.
    /// Built back to front so each page knows its successor (TIER-026);
    /// shared by long values and long keys.
    fn store_chain(&self, bytes: &[u8]) -> Result<u64> {
        let chunk = self.overflow_chunk();
        let mut next = NIL;
        let chunks: Vec<&[u8]> = bytes.chunks(chunk).collect();
        for part in chunks.iter().rev() {
            let page_id = self.pager.allocate_page_id(self.table_id);
            let mut payload = Vec::with_capacity(8 + part.len());
            payload.extend_from_slice(&next.to_le_bytes());
            payload.extend_from_slice(part);
            let header = PageHeader::new(page_id, self.table_id.as_u32(), 0, FLAG_OVERFLOW);
            let image = encode_page(&header, &payload)?;
            drop(self.pager.install(self.table_id, page_id, image)?);
            next = page_id;
        }
        Ok(next)
    }

    /// Record a superseded leaf value's overflow-chain pages (if any) for
    /// deferred reclaim — the CoW analogue of freeing: a snapshot on the old
    /// leaf still points at the chain, so it must outlive that version.
    fn supersede_value(&self, value: &LeafValue, superseded: &mut Vec<u64>) -> Result<()> {
        let LeafValue::Overflow { head, .. } = value else {
            return Ok(());
        };
        self.supersede_chain(*head, superseded)
    }

    /// Walk an overflow chain, recording every page for deferred reclaim.
    fn supersede_chain(&self, head: u64, superseded: &mut Vec<u64>) -> Result<()> {
        let mut page_id = head;
        while page_id != NIL {
            let next = {
                let guard = self.pager.fault(self.table_id, page_id)?;
                let payload = payload_of(&guard);
                if payload.len() < 8 {
                    return Err(node_err(self.table_id, page_id, "overflow page too short"));
                }
                u64::from_le_bytes([
                    payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                    payload[6], payload[7],
                ])
            };
            superseded.push(page_id);
            page_id = next;
        }
        Ok(())
    }

    /// Write `node` to a fresh page (never touching the old one), splitting
    /// into two fresh pages when it no longer fits — the copy-on-write step.
    fn write_or_split_new(&mut self, node: Node) -> Result<Cow> {
        if node_size(&node) <= self.node_budget() {
            return Ok(Cow::Node(self.write_new_node(&node)?));
        }
        let (left, sep, right) = split_node(node)?;
        // The separator is handed up as full bytes; the parent re-stores it
        // (a long separator gets its own chain — the right half keeps its).
        let sep = self.full_key(&sep)?.into_owned();
        let left_page = self.write_new_node(&left)?;
        let right_page = self.write_new_node(&right)?;
        Ok(Cow::Split {
            left: left_page,
            sep,
            right: right_page,
        })
    }

    /// Allocate a page id and install `node` as a fresh dirty page.
    fn write_new_node(&self, node: &Node) -> Result<u64> {
        let page_id = self.pager.allocate_page_id(self.table_id);
        let image = self.encode_node(page_id, node)?;
        drop(self.pager.install(self.table_id, page_id, image)?);
        Ok(page_id)
    }

    /// Encode a node into a page image with the right TIER-021 flags.
    fn encode_node(&self, page_id: u64, node: &Node) -> Result<Vec<u8>> {
        let (kind, count, index_flagged) = match node {
            Node::Leaf(entries) => (NODE_LEAF, entries.len(), self.index_tree),
            Node::Interior(entries) => (NODE_INTERIOR, entries.len(), true),
        };
        let mut payload = Vec::with_capacity(1 + node_size(node));
        payload.push(kind);
        match node {
            Node::Leaf(entries) => {
                for (key, value) in entries {
                    let stored = key.stored();
                    // Tag: bit 0 = overflow value, bit 1 = overflow key
                    // (0 inline/inline, 1 inline-key/overflow-value,
                    //  2 overflow-key/inline-value, 3 both).
                    let tag = match (key, value) {
                        (EntryKey::Inline(_), LeafValue::Inline(_)) => 0u8,
                        (EntryKey::Inline(_), LeafValue::Overflow { .. }) => 1,
                        (EntryKey::Overflow { .. }, LeafValue::Inline(_)) => 2,
                        (EntryKey::Overflow { .. }, LeafValue::Overflow { .. }) => 3,
                    };
                    payload.extend_from_slice(&(stored.len() as u16).to_le_bytes());
                    payload.push(tag);
                    payload.extend_from_slice(stored);
                    if let EntryKey::Overflow {
                        total_len, head, ..
                    } = key
                    {
                        payload.extend_from_slice(&total_len.to_le_bytes());
                        payload.extend_from_slice(&head.to_le_bytes());
                    }
                    match value {
                        LeafValue::Inline(bytes) => {
                            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                            payload.extend_from_slice(bytes);
                        }
                        LeafValue::Overflow { total_len, head } => {
                            payload.extend_from_slice(&total_len.to_le_bytes());
                            payload.extend_from_slice(&head.to_le_bytes());
                        }
                    }
                }
            }
            Node::Interior(entries) => {
                for (key, child) in entries {
                    let stored = key.stored();
                    match key {
                        EntryKey::Inline(_) => {
                            payload.extend_from_slice(&(stored.len() as u16).to_le_bytes());
                            payload.extend_from_slice(stored);
                        }
                        EntryKey::Overflow {
                            total_len, head, ..
                        } => {
                            // Bit 15 of key_len marks an overflow separator.
                            payload.extend_from_slice(
                                &((stored.len() as u16) | KEY_LEN_OVERFLOW).to_le_bytes(),
                            );
                            payload.extend_from_slice(stored);
                            payload.extend_from_slice(&total_len.to_le_bytes());
                            payload.extend_from_slice(&head.to_le_bytes());
                        }
                    }
                    payload.extend_from_slice(&child.to_le_bytes());
                }
            }
        }
        let row_count = u32::try_from(count)
            .map_err(|_| FluxumError::Storage("node entry count exceeds u32".into()))?;
        let flags = if index_flagged { FLAG_INDEX } else { 0 };
        let header = PageHeader::new(page_id, self.table_id.as_u32(), row_count, flags);
        encode_page(&header, &payload)
    }
}

/// The page image's payload slice (header already CRC-verified on fault).
fn payload_of(guard: &PageGuard) -> &[u8] {
    &guard.image()[PAGE_HEADER_LEN..]
}

/// A leaf value referenced in place inside a pinned frame (hot path).
enum RawLeafValue<'p> {
    Inline(&'p [u8]),
    Overflow { total_len: u64, head: u64 },
}

/// Outcome of an in-place leaf search.
enum RawFound<'p> {
    /// No entry matches (the walk passed the probe or ran out).
    Absent,
    /// The matching entry's value, referenced in the pinned frame.
    Found(RawLeafValue<'p>),
    /// An overflow key's routing prefix could not decide the comparison —
    /// the caller must re-search with full keys (faulting key chains).
    NeedsFullKeys,
}

/// Outcome of an in-place interior route.
enum RawRoute {
    /// The child page to descend into.
    Child(u64),
    /// An overflow separator tied on its prefix — route with full keys.
    NeedsFullKeys,
}

/// Fixed-width little-endian reads for the in-place walkers; `None` on a
/// truncated buffer (malformed node).
#[inline]
fn raw_u16(rest: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes([*rest.get(at)?, *rest.get(at + 1)?]))
}

#[inline]
fn raw_u32(rest: &[u8], at: usize) -> Option<u32> {
    let bytes = rest.get(at..at + 4)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[inline]
fn raw_u64(rest: &[u8], at: usize) -> Option<u64> {
    let bytes = rest.get(at..at + 8)?;
    Some(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Allocation-free leaf search (TIER-014 hot path): walk the sorted entry
/// stream in place, early-exiting once entry keys pass `key`. `None` =
/// malformed payload. An entry whose *routing prefix* cannot decide the
/// comparison (the probe extends an overflow key's prefix) yields
/// [`RawFound::NeedsFullKeys`] so the caller re-runs the search on the
/// parsed, chain-faulting path.
fn raw_leaf_find<'p>(payload: &'p [u8], key: &[u8]) -> Option<RawFound<'p>> {
    use std::cmp::Ordering;
    let mut at = 0usize;
    while at < payload.len() {
        let key_len = raw_u16(payload, at)? as usize;
        let tag = *payload.get(at + 2)?;
        if tag > 3 {
            return None;
        }
        let entry_key = payload.get(at + 3..at + 3 + key_len)?;
        at += 3 + key_len;
        let key_overflow = tag & 2 != 0;
        if key_overflow {
            at += 12; // full_key_len: u32 | key_head: u64
        }
        let value_len = if tag & 1 == 0 {
            4 + raw_u32(payload, at)? as usize
        } else {
            16
        };
        let ordering = if key_overflow {
            // Prefix comparison; only a probe that extends the prefix is
            // undecidable here (the full key strictly extends it).
            let l = entry_key.len().min(key.len());
            match entry_key[..l].cmp(&key[..l]) {
                Ordering::Equal if key.len() <= entry_key.len() => Ordering::Greater,
                Ordering::Equal => return Some(RawFound::NeedsFullKeys),
                other => other,
            }
        } else {
            entry_key.cmp(key)
        };
        match ordering {
            Ordering::Less => at += value_len,
            Ordering::Greater => return Some(RawFound::Absent), // sorted: past it
            Ordering::Equal => {
                return Some(RawFound::Found(if tag & 1 == 0 {
                    RawLeafValue::Inline(payload.get(at + 4..at + value_len)?)
                } else {
                    RawLeafValue::Overflow {
                        total_len: raw_u64(payload, at)?,
                        head: raw_u64(payload, at + 8)?,
                    }
                }));
            }
        }
    }
    Some(RawFound::Absent)
}

/// Allocation-free interior routing (TIER-014 hot path): the rightmost
/// entry with `entry_key <= key`, or the first entry when `key` sorts below
/// every separator. `None` = malformed/empty payload.
fn raw_interior_route(payload: &[u8], key: &[u8]) -> Option<RawRoute> {
    use std::cmp::Ordering;
    let mut at = 0usize;
    let mut chosen: Option<u64> = None;
    while at < payload.len() {
        let raw_len = raw_u16(payload, at)?;
        let key_overflow = raw_len & KEY_LEN_OVERFLOW != 0;
        let key_len = (raw_len & KEY_LEN_MASK) as usize;
        let entry_key = payload.get(at + 2..at + 2 + key_len)?;
        let mut cursor = at + 2 + key_len;
        if key_overflow {
            cursor += 12; // full_key_len: u32 | key_head: u64
        }
        let child = raw_u64(payload, cursor)?;
        // `sep <= key`, decided on the prefix unless the probe extends it.
        let le = if key_overflow {
            let l = entry_key.len().min(key.len());
            match entry_key[..l].cmp(&key[..l]) {
                // The full separator strictly extends its prefix, so a probe
                // no longer than the prefix sorts below it.
                Ordering::Equal if key.len() <= entry_key.len() => false,
                Ordering::Equal => return Some(RawRoute::NeedsFullKeys),
                Ordering::Less => true,
                Ordering::Greater => false,
            }
        } else {
            entry_key <= key
        };
        if le {
            chosen = Some(child);
        } else if chosen.is_some() {
            break; // sorted: nothing further can route lower
        } else {
            chosen = Some(child); // key below every separator: first child
            break;
        }
        at = cursor + 8;
    }
    chosen.map(RawRoute::Child)
}

/// Parse a pooled node page.
fn parse_node(guard: &PageGuard) -> Result<Node> {
    let key = guard.key();
    let table_id = TableId::from_raw(key.table_id);
    let payload = payload_of(guard);
    let Some((&kind, mut rest)) = payload.split_first() else {
        return Err(node_err(table_id, key.page_id, "empty node payload"));
    };
    let take = |rest: &mut &[u8], n: usize| -> Result<Vec<u8>> {
        if rest.len() < n {
            return Err(node_err(table_id, key.page_id, "truncated node entry"));
        }
        let (head, tail) = rest.split_at(n);
        let out = head.to_vec();
        *rest = tail;
        Ok(out)
    };
    let take_u16 = |rest: &mut &[u8]| -> Result<u16> {
        let b = take(rest, 2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    };
    let take_u32 = |rest: &mut &[u8]| -> Result<u32> {
        let b = take(rest, 4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    };
    let take_u64 = |rest: &mut &[u8]| -> Result<u64> {
        let b = take(rest, 8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    };
    match kind {
        NODE_LEAF => {
            let mut entries: LeafEntries = Vec::new();
            while !rest.is_empty() {
                let key_len = take_u16(&mut rest)? as usize;
                let tag = take(&mut rest, 1)?[0];
                if tag > 3 {
                    return Err(node_err(
                        table_id,
                        key.page_id,
                        &format!("unknown leaf value tag {tag}"),
                    ));
                }
                let stored = take(&mut rest, key_len)?;
                let entry_key = if tag & 2 == 0 {
                    EntryKey::Inline(stored)
                } else {
                    EntryKey::Overflow {
                        prefix: stored,
                        total_len: take_u32(&mut rest)?,
                        head: take_u64(&mut rest)?,
                    }
                };
                let value = if tag & 1 == 0 {
                    let val_len = take_u32(&mut rest)? as usize;
                    LeafValue::Inline(take(&mut rest, val_len)?)
                } else {
                    LeafValue::Overflow {
                        total_len: take_u64(&mut rest)?,
                        head: take_u64(&mut rest)?,
                    }
                };
                entries.push((entry_key, value));
            }
            Ok(Node::Leaf(entries))
        }
        NODE_INTERIOR => {
            let mut entries: InteriorEntries = Vec::new();
            while !rest.is_empty() {
                let raw_len = take_u16(&mut rest)?;
                let stored = take(&mut rest, (raw_len & KEY_LEN_MASK) as usize)?;
                let entry_key = if raw_len & KEY_LEN_OVERFLOW == 0 {
                    EntryKey::Inline(stored)
                } else {
                    EntryKey::Overflow {
                        prefix: stored,
                        total_len: take_u32(&mut rest)?,
                        head: take_u64(&mut rest)?,
                    }
                };
                let child = take_u64(&mut rest)?;
                entries.push((entry_key, child));
            }
            Ok(Node::Interior(entries))
        }
        other => Err(node_err(
            table_id,
            key.page_id,
            &format!("unknown node kind {other:#04x}"),
        )),
    }
}

fn node_err(table_id: TableId, page_id: u64, what: &str) -> FluxumError {
    FluxumError::Storage(format!(
        "paged tree node {page_id} of table {table_id}: {what}"
    ))
}

/// Encoded size of one leaf entry.
fn leaf_entry_size(key: &EntryKey, value: &LeafValue) -> usize {
    2 + 1
        + key.stored().len()
        + match key {
            EntryKey::Inline(_) => 0,
            EntryKey::Overflow { .. } => 12, // full_key_len | key_head
        }
        + match value {
            LeafValue::Inline(bytes) => 4 + bytes.len(),
            LeafValue::Overflow { .. } => 16,
        }
}

/// Encoded size of one interior entry.
fn interior_entry_size(key: &EntryKey) -> usize {
    2 + key.stored().len()
        + match key {
            EntryKey::Inline(_) => 0,
            EntryKey::Overflow { .. } => 12,
        }
        + 8
}

/// Total encoded entry bytes of a node (excluding the kind byte).
fn node_size(node: &Node) -> usize {
    match node {
        Node::Leaf(entries) => entries.iter().map(|(k, v)| leaf_entry_size(k, v)).sum(),
        Node::Interior(entries) => entries.iter().map(|(k, _)| interior_entry_size(k)).sum(),
    }
}

/// Both halves of a split entry list.
type Halves<T> = (Vec<(EntryKey, T)>, Vec<(EntryKey, T)>);

/// Split an overfull node near its byte midpoint; the separator is the
/// right half's first key (returned as the stored key — the caller
/// materializes and re-stores it, so both halves keep valid chains).
fn split_node(node: Node) -> Result<(Node, EntryKey, Node)> {
    fn split_at_bytes<T>(
        entries: Vec<(EntryKey, T)>,
        size: impl Fn(&(EntryKey, T)) -> usize,
    ) -> Halves<T> {
        let total: usize = entries.iter().map(&size).sum();
        let mut acc = 0usize;
        let mut split = entries.len() / 2; // fallback
        for (i, entry) in entries.iter().enumerate() {
            acc += size(entry);
            if acc * 2 >= total {
                split = i + 1;
                break;
            }
        }
        let split = split.clamp(1, entries.len() - 1);
        let mut left = entries;
        let right = left.split_off(split);
        (left, right)
    }
    match node {
        Node::Leaf(entries) => {
            if entries.len() < 2 {
                return Err(FluxumError::Storage(
                    "cannot split a leaf with fewer than two entries — a single entry \
                     exceeded the page budget (inline cap enforces this cannot happen)"
                        .into(),
                ));
            }
            let (left, right) = split_at_bytes(entries, |(k, v)| leaf_entry_size(k, v));
            let sep = right[0].0.clone();
            Ok((Node::Leaf(left), sep, Node::Leaf(right)))
        }
        Node::Interior(entries) => {
            if entries.len() < 2 {
                return Err(FluxumError::Storage(
                    "cannot split an interior node with fewer than two entries".into(),
                ));
            }
            let (left, right) = split_at_bytes(entries, |(k, _)| interior_entry_size(k));
            let sep = right[0].0.clone();
            Ok((Node::Interior(left), sep, Node::Interior(right)))
        }
    }
}

#[cfg(test)]
mod tests;
