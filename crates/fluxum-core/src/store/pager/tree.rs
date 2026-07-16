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
//! sorted by key, `row_count` of them (TIER-021 header field):
//!
//! - **Leaf** (`0x4C`, `'L'`): entry = `key_len: u16 | tag: u8 | key |`
//!   then `tag 0` (inline): `val_len: u32 | value`, or `tag 1` (overflow):
//!   `total_len: u64 | head_page_id: u64` — the value lives in a chain of
//!   overflow pages (TIER-026).
//! - **Interior** (`0x49`, `'I'`): entry = `key_len: u16 | key |
//!   child_page_id: u64`. Entry *i* routes keys in `[key_i, key_{i+1})`;
//!   keys below `key_0` route to entry 0.
//! - **Overflow** (header flag bit 3): payload = `next_page_id: u64 |
//!   chunk` (`next = 0` ends the chain). `row_count = 0`.
//!
//! Deletion removes entries without rebalancing (a sparse node stays valid
//! and is compacted by the next checkpoint rewrite); correctness never
//! depends on fill factor.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// A decoded leaf value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeafValue {
    /// Value bytes stored in the leaf itself.
    Inline(Vec<u8>),
    /// Value stored in an overflow chain (TIER-026).
    Overflow { total_len: u64, head: u64 },
}

type LeafEntries = Vec<(Vec<u8>, LeafValue)>;
type InteriorEntries = Vec<(Vec<u8>, u64)>;

/// Scan visitor: `(key, value) -> keep_going`.
pub type ScanFn<'a> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'a;

/// A decoded B-tree node.
#[derive(Debug)]
enum Node {
    Leaf(LeafEntries),
    Interior(InteriorEntries),
}

/// A paged B-tree over one table's page file. Reads (`get`, `scan`) are
/// `&self` and safe to run concurrently; mutations are `&mut self`
/// (single-writer, matching STG-003).
#[derive(Debug)]
pub struct PagedTree {
    pager: Arc<Pager>,
    table_id: TableId,
    /// Whether leaf nodes carry the TIER-021 index flag (secondary/spatial
    /// index trees). Interior nodes always do.
    index_tree: bool,
    root: AtomicU64,
}

impl PagedTree {
    /// Create an empty tree: one empty leaf as root.
    pub fn create(pager: &Arc<Pager>, table_id: TableId, index_tree: bool) -> Result<Self> {
        let tree = Self {
            pager: Arc::clone(pager),
            table_id,
            index_tree,
            root: AtomicU64::new(NIL),
        };
        let root = tree.write_new_node(&Node::Leaf(Vec::new()))?;
        tree.root.store(root, Ordering::Release);
        Ok(tree)
    }

    /// The current root page id (diagnostics / tests).
    pub fn root_page_id(&self) -> u64 {
        self.root.load(Ordering::Acquire)
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

    /// Largest accepted key (keys are PKs or index keys; anything larger is
    /// a schema-abuse error, not an overflow case).
    fn max_key(&self) -> usize {
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
        let mut page_id = self.root.load(Ordering::Acquire);
        loop {
            let guard = self.pager.fault(self.table_id, page_id)?;
            let payload = payload_of(&guard);
            let (kind, rest) = payload
                .split_first()
                .ok_or_else(|| node_err(self.table_id, page_id, "empty node payload"))?;
            match *kind {
                NODE_INTERIOR => {
                    page_id = raw_interior_route(rest, key)
                        .ok_or_else(|| node_err(self.table_id, page_id, "malformed interior"))?;
                }
                NODE_LEAF => {
                    let found = raw_leaf_find(rest, key)
                        .ok_or_else(|| node_err(self.table_id, page_id, "malformed leaf"))?;
                    let Some(found) = found else {
                        return Ok(None);
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

    /// In-order scan of `[start, end)` (`end: None` = unbounded). `f`
    /// returns `false` to stop early; the scan result is whether it ran to
    /// completion. Faulted scan pages enter the pool scan-resistant
    /// (TIER-015 is handled by the pool's insert policy for bulk loads;
    /// scans fault with normal reference semantics — frequently scanned
    /// ranges staying hot is intended).
    pub fn scan(&self, start: &[u8], end: Option<&[u8]>, f: &mut ScanFn<'_>) -> Result<bool> {
        self.scan_node(self.root.load(Ordering::Acquire), start, end, f)
    }

    fn scan_node(
        &self,
        page_id: u64,
        start: &[u8],
        end: Option<&[u8]>,
        f: &mut ScanFn<'_>,
    ) -> Result<bool> {
        let guard = self.pager.fault(self.table_id, page_id)?;
        match parse_node(&guard)? {
            Node::Leaf(entries) => {
                let from = entries.partition_point(|(k, _)| k.as_slice() < start);
                for (k, v) in &entries[from..] {
                    if let Some(end) = end
                        && k.as_slice() >= end
                    {
                        break;
                    }
                    let value = self.materialize(v)?;
                    if !f(k, &value)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Node::Interior(entries) => {
                let from = entries
                    .partition_point(|(k, _)| k.as_slice() <= start)
                    .saturating_sub(1);
                for (i, (k, child)) in entries.iter().enumerate().skip(from) {
                    if i > from
                        && let Some(end) = end
                        && k.as_slice() >= end
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
            LeafValue::Overflow { total_len, head } => {
                let mut out = Vec::with_capacity(usize::try_from(*total_len).unwrap_or(0));
                let mut page_id = *head;
                while page_id != NIL {
                    let guard = self.pager.fault(self.table_id, page_id)?;
                    let payload = payload_of(&guard);
                    if payload.len() < 8 {
                        return Err(node_err(self.table_id, page_id, "overflow page too short"));
                    }
                    page_id = u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    out.extend_from_slice(&payload[8..]);
                }
                if out.len() as u64 != *total_len {
                    return Err(node_err(
                        self.table_id,
                        *head,
                        "overflow chain length mismatch",
                    ));
                }
                Ok(out)
            }
        }
    }

    // --- writes --------------------------------------------------------

    /// Insert or replace (`upsert`) `key → value`. Node splits propagate up
    /// and may grow a new root.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(FluxumError::Storage(
                "paged tree keys must be non-empty".into(),
            ));
        }
        if key.len() > self.max_key() {
            return Err(FluxumError::Storage(format!(
                "key of {} bytes exceeds the {}-byte limit for {}-byte pages",
                key.len(),
                self.max_key(),
                self.pager.page_size()
            )));
        }
        let root = self.root.load(Ordering::Acquire);
        if let Some((sep, right)) = self.insert_into(root, key, value)? {
            // Root split: new interior root with a low-sentinel first entry
            // (the empty key routes everything below `sep` left).
            let entries: InteriorEntries = vec![(Vec::new(), root), (sep, right)];
            let new_root = self.write_new_node(&Node::Interior(entries))?;
            self.root.store(new_root, Ordering::Release);
        }
        Ok(())
    }

    /// Recursive insert; returns the `(separator, new_right_page)` of a
    /// split, to be applied by the parent.
    fn insert_into(
        &mut self,
        page_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>> {
        let mut guard = self.pager.fault(self.table_id, page_id)?;
        match parse_node(&guard)? {
            Node::Leaf(mut entries) => {
                let leaf_value = self.store_value(key, value)?;
                match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(idx) => {
                        // Replace: free the superseded overflow chain first.
                        let old = std::mem::replace(&mut entries[idx].1, leaf_value);
                        self.free_value(&old)?;
                    }
                    Err(idx) => entries.insert(idx, (key.to_vec(), leaf_value)),
                }
                self.write_or_split(&mut guard, page_id, Node::Leaf(entries))
            }
            Node::Interior(entries) => {
                let idx = route_index(&entries, key)?;
                let child = entries[idx].1;
                // The parent stays pinned across the recursion — split
                // application must land on this exact node (single writer).
                let split = self.insert_into(child, key, value)?;
                let Some((sep, right)) = split else {
                    return Ok(None);
                };
                let mut entries = entries;
                entries.insert(idx + 1, (sep, right));
                self.write_or_split(&mut guard, page_id, Node::Interior(entries))
            }
        }
    }

    /// Remove `key`. Returns whether it was present. No rebalancing (see
    /// module docs); an emptied leaf stays as a valid, routable node.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut page_id = self.root.load(Ordering::Acquire);
        loop {
            let mut guard = self.pager.fault(self.table_id, page_id)?;
            match parse_node(&guard)? {
                Node::Interior(entries) => page_id = route(&entries, key)?,
                Node::Leaf(mut entries) => {
                    let Ok(idx) = entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) else {
                        return Ok(false);
                    };
                    let (_, old) = entries.remove(idx);
                    self.free_value(&old)?;
                    let image = self.encode_node(page_id, &Node::Leaf(entries))?;
                    self.pager.write_pinned(&mut guard, image)?;
                    return Ok(true);
                }
            }
        }
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
            if key.is_empty() || key.len() > self.max_key() {
                return Err(FluxumError::Storage(format!(
                    "bulk_load key of {} bytes is empty or exceeds {}",
                    key.len(),
                    self.max_key()
                )));
            }
            if let Some(last) = &last_key
                && *last >= key
            {
                return Err(FluxumError::Storage(
                    "bulk_load input must be strictly sorted by key".into(),
                ));
            }
            last_key = Some(key.clone());
            let leaf_value = self.store_value(&key, &value)?;
            let size = leaf_entry_size(&key, &leaf_value);
            if leaf_bytes + size > budget && !leaf.is_empty() {
                let first = leaf[0].0.clone();
                let page = self.write_new_node(&Node::Leaf(std::mem::take(&mut leaf)))?;
                level.push((first, page));
                leaf_bytes = 0;
            }
            leaf_bytes += size;
            leaf.push((key, leaf_value));
        }
        let first = leaf.first().map(|(k, _)| k.clone()).unwrap_or_default();
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
                    let first = node[0].0.clone();
                    let page = self.write_new_node(&Node::Interior(std::mem::take(&mut node)))?;
                    next.push((first, page));
                    node_bytes = 0;
                }
                node_bytes += size;
                node.push((key, child));
            }
            let first = node.first().map(|(k, _)| k.clone()).unwrap_or_default();
            let page = self.write_new_node(&Node::Interior(node))?;
            next.push((first, page));
            level = next;
        }
        let (_, new_root) = level.remove(0);
        let old_root = self.root.swap(new_root, Ordering::AcqRel);
        if old_root != NIL {
            self.pager.free_page(self.table_id, old_root)?;
        }
        Ok(())
    }

    // --- node plumbing --------------------------------------------------

    /// Store a value for a leaf entry: inline when it fits, otherwise as an
    /// overflow chain (TIER-026).
    fn store_value(&self, key: &[u8], value: &[u8]) -> Result<LeafValue> {
        let inline = LeafValue::Inline(value.to_vec());
        if leaf_entry_size(key, &inline) <= self.max_inline_entry() {
            return Ok(inline);
        }
        // Build the chain back to front so each page knows its successor.
        let chunk = self.overflow_chunk();
        let mut next = NIL;
        let chunks: Vec<&[u8]> = value.chunks(chunk).collect();
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
        Ok(LeafValue::Overflow {
            total_len: value.len() as u64,
            head: next,
        })
    }

    /// Free a superseded leaf value (its overflow chain, if any).
    fn free_value(&self, value: &LeafValue) -> Result<()> {
        let LeafValue::Overflow { head, .. } = value else {
            return Ok(());
        };
        let mut page_id = *head;
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
            self.pager.free_page(self.table_id, page_id)?;
            page_id = next;
        }
        Ok(())
    }

    /// Rewrite `page_id` with `node`, splitting when it no longer fits.
    fn write_or_split(
        &mut self,
        guard: &mut PageGuard,
        page_id: u64,
        node: Node,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        if node_size(&node) <= self.node_budget() {
            let image = self.encode_node(page_id, &node)?;
            self.pager.write_pinned(guard, image)?;
            return Ok(None);
        }
        let (left, sep, right) = split_node(node)?;
        let right_page = self.write_new_node(&right)?;
        let image = self.encode_node(page_id, &left)?;
        self.pager.write_pinned(guard, image)?;
        Ok(Some((sep, right_page)))
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
                    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
                    match value {
                        LeafValue::Inline(bytes) => {
                            payload.push(0);
                            payload.extend_from_slice(key);
                            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                            payload.extend_from_slice(bytes);
                        }
                        LeafValue::Overflow { total_len, head } => {
                            payload.push(1);
                            payload.extend_from_slice(key);
                            payload.extend_from_slice(&total_len.to_le_bytes());
                            payload.extend_from_slice(&head.to_le_bytes());
                        }
                    }
                }
            }
            Node::Interior(entries) => {
                for (key, child) in entries {
                    payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
                    payload.extend_from_slice(key);
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
/// stream in place, early-exiting once entry keys pass `key`. Outer `None`
/// = malformed payload; inner `Option` = present/absent.
fn raw_leaf_find<'p>(payload: &'p [u8], key: &[u8]) -> Option<Option<RawLeafValue<'p>>> {
    let mut at = 0usize;
    while at < payload.len() {
        let key_len = raw_u16(payload, at)? as usize;
        let tag = *payload.get(at + 2)?;
        let entry_key = payload.get(at + 3..at + 3 + key_len)?;
        at += 3 + key_len;
        let value_len = match tag {
            0 => 4 + raw_u32(payload, at)? as usize,
            1 => 16,
            _ => return None,
        };
        match entry_key.cmp(key) {
            std::cmp::Ordering::Less => at += value_len,
            std::cmp::Ordering::Greater => return Some(None), // sorted: past it
            std::cmp::Ordering::Equal => {
                return Some(Some(if tag == 0 {
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
    Some(None)
}

/// Allocation-free interior routing (TIER-014 hot path): the rightmost
/// entry with `entry_key <= key`, or the first entry when `key` sorts below
/// every separator. `None` = malformed/empty payload.
fn raw_interior_route(payload: &[u8], key: &[u8]) -> Option<u64> {
    let mut at = 0usize;
    let mut chosen: Option<u64> = None;
    while at < payload.len() {
        let key_len = raw_u16(payload, at)? as usize;
        let entry_key = payload.get(at + 2..at + 2 + key_len)?;
        let child = raw_u64(payload, at + 2 + key_len)?;
        if entry_key <= key {
            chosen = Some(child);
        } else if chosen.is_some() {
            break; // sorted: nothing further can route lower
        } else {
            chosen = Some(child); // key below every separator: first child
            break;
        }
        at += 2 + key_len + 8;
    }
    chosen
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
                let entry_key = take(&mut rest, key_len)?;
                let value = match tag {
                    0 => {
                        let val_len = take_u32(&mut rest)? as usize;
                        LeafValue::Inline(take(&mut rest, val_len)?)
                    }
                    1 => LeafValue::Overflow {
                        total_len: take_u64(&mut rest)?,
                        head: take_u64(&mut rest)?,
                    },
                    other => {
                        return Err(node_err(
                            table_id,
                            key.page_id,
                            &format!("unknown leaf value tag {other}"),
                        ));
                    }
                };
                entries.push((entry_key, value));
            }
            Ok(Node::Leaf(entries))
        }
        NODE_INTERIOR => {
            let mut entries: InteriorEntries = Vec::new();
            while !rest.is_empty() {
                let key_len = take_u16(&mut rest)? as usize;
                let entry_key = take(&mut rest, key_len)?;
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
fn leaf_entry_size(key: &[u8], value: &LeafValue) -> usize {
    2 + 1
        + key.len()
        + match value {
            LeafValue::Inline(bytes) => 4 + bytes.len(),
            LeafValue::Overflow { .. } => 16,
        }
}

/// Encoded size of one interior entry.
fn interior_entry_size(key: &[u8]) -> usize {
    2 + key.len() + 8
}

/// Total encoded entry bytes of a node (excluding the kind byte).
fn node_size(node: &Node) -> usize {
    match node {
        Node::Leaf(entries) => entries.iter().map(|(k, v)| leaf_entry_size(k, v)).sum(),
        Node::Interior(entries) => entries.iter().map(|(k, _)| interior_entry_size(k)).sum(),
    }
}

/// Both halves of a split entry list.
type Halves<T> = (Vec<(Vec<u8>, T)>, Vec<(Vec<u8>, T)>);

/// Split an overfull node near its byte midpoint; the separator is the
/// right half's first key.
fn split_node(node: Node) -> Result<(Node, Vec<u8>, Node)> {
    fn split_at_bytes<T>(
        entries: Vec<(Vec<u8>, T)>,
        size: impl Fn(&(Vec<u8>, T)) -> usize,
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

/// The child page routing `key` in a sorted interior entry list.
fn route(entries: &InteriorEntries, key: &[u8]) -> Result<u64> {
    Ok(entries[route_index(entries, key)?].1)
}

/// The entry index routing `key`: the rightmost entry with `key_i <= key`,
/// or entry 0 when `key` sorts below every separator.
fn route_index(entries: &InteriorEntries, key: &[u8]) -> Result<usize> {
    if entries.is_empty() {
        return Err(FluxumError::Storage(
            "interior node with zero entries".into(),
        ));
    }
    Ok(entries
        .partition_point(|(k, _)| k.as_slice() <= key)
        .saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PageCompression;
    use crate::store::pager::{Pager, PagerOptions};

    const PAGE_SIZE: usize = 256; // budget 223, max_key 27, inline cap 55

    fn fixture() -> (tempfile::TempDir, Arc<Pager>, TableId) {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let pager = Pager::open(
            dir.path(),
            PagerOptions {
                shard_id: 0,
                page_size: PAGE_SIZE,
                pool_capacity_bytes: (64 * PAGE_SIZE) as u64,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"));
        (dir, pager, TableId::from_raw(1))
    }

    /// Overwrite the tree's root page with an arbitrary node payload
    /// (single-writer corruption seam for the malformed-node error paths).
    fn corrupt_root(pager: &Arc<Pager>, table: TableId, tree: &PagedTree, payload: &[u8]) {
        let root = tree.root_page_id();
        let header = PageHeader::new(root, table.as_u32(), 0, FLAG_INDEX);
        let image = encode_page(&header, payload).unwrap_or_else(|e| panic!("{e}"));
        let mut guard = pager.fault(table, root).unwrap_or_else(|e| panic!("{e}"));
        pager
            .write_pinned(&mut guard, image)
            .unwrap_or_else(|e| panic!("{e}"));
    }

    fn get_err(tree: &PagedTree, key: &[u8]) -> String {
        match tree.get(key) {
            Ok(v) => panic!("corrupt node served: {v:?}"),
            Err(e) => e.to_string(),
        }
    }

    fn scan_err(tree: &PagedTree) -> String {
        match tree.scan(&[], None, &mut |_, _| Ok(true)) {
            Ok(done) => panic!("corrupt node scanned to {done}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn key_validation_rejects_empty_and_oversized_keys() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

        let err = match tree.insert(b"", b"v") {
            Ok(()) => panic!("empty key accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("keys must be non-empty"), "{err}");

        let long = vec![b'k'; tree.max_key() + 1];
        let err = match tree.insert(&long, b"v") {
            Ok(()) => panic!("oversized key accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("exceeds the"), "{err}");
        assert!(err.contains("byte limit"), "{err}");
    }

    #[test]
    fn delete_of_a_missing_key_reports_false() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        tree.insert(b"present", b"v")
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!tree.delete(b"absent").unwrap_or_else(|e| panic!("{e}")));
        assert!(tree.delete(b"present").unwrap_or_else(|e| panic!("{e}")));
        assert!(!tree.delete(b"present").unwrap_or_else(|e| panic!("{e}")));
    }

    #[test]
    fn scans_stop_early_when_the_visitor_returns_false() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        // Enough entries to force leaf splits and an interior level, so the
        // early stop propagates through both scan_node arms.
        for i in 0..64u32 {
            let key = format!("key-{i:04}");
            tree.insert(key.as_bytes(), &i.to_le_bytes())
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let mut seen = 0usize;
        let completed = tree
            .scan(&[], None, &mut |_, _| {
                seen += 1;
                Ok(seen < 5)
            })
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!completed, "an early-stopped scan must report false");
        assert_eq!(seen, 5);
    }

    #[test]
    fn keys_below_every_separator_route_to_the_first_child() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        // bulk_load builds interior entries keyed by real first keys (no
        // low sentinel), so a probe below the smallest key exercises the
        // first-child routing fallback.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (10..74u32)
            .map(|i| (format!("k-{i:04}").into_bytes(), i.to_le_bytes().to_vec()))
            .collect();
        tree.bulk_load(entries).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            tree.get(b"a-below-everything")
                .unwrap_or_else(|e| panic!("{e}")),
            None
        );
        assert_eq!(
            tree.get(b"k-0010").unwrap_or_else(|e| panic!("{e}")),
            Some(10u32.to_le_bytes().to_vec())
        );
    }

    #[test]
    fn bulk_load_rejects_bad_keys_and_unsorted_input() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

        let err = match tree.bulk_load(vec![(Vec::new(), b"v".to_vec())]) {
            Ok(()) => panic!("empty bulk_load key accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("empty or exceeds"), "{err}");

        let err = match tree.bulk_load(vec![
            (b"b".to_vec(), b"1".to_vec()),
            (b"a".to_vec(), b"2".to_vec()),
        ]) {
            Ok(()) => panic!("unsorted bulk_load accepted"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("strictly sorted"), "{err}");
    }

    #[test]
    fn replacing_an_overflow_value_frees_the_superseded_chain() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        // Values above the inline cap (55 bytes at 256-byte pages) go to
        // overflow chains spanning multiple pages.
        let v1 = vec![0xA1u8; 700];
        let v2 = vec![0xB2u8; 500];
        tree.insert(b"big", &v1).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), Some(v1));
        // Replace: the old chain is freed page by page, the new one reads
        // back exactly.
        tree.insert(b"big", &v2).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), Some(v2));
        // Delete frees the remaining chain.
        assert!(tree.delete(b"big").unwrap_or_else(|e| panic!("{e}")));
        assert_eq!(tree.get(b"big").unwrap_or_else(|e| panic!("{e}")), None);
    }

    #[test]
    fn unknown_node_kinds_are_reported_not_served() {
        let (_dir, pager, table) = fixture();
        let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        corrupt_root(&pager, table, &tree, &[0xFF, 1, 2, 3]);
        // The allocation-free get path names the kind byte…
        let err = get_err(&tree, b"k");
        assert!(err.contains("unknown node kind 0xff"), "{err}");
        // …and the parsing scan path reports the same corruption.
        let err = scan_err(&tree);
        assert!(err.contains("unknown node kind 0xff"), "{err}");
    }

    #[test]
    fn empty_and_truncated_node_payloads_are_reported() {
        let (_dir, pager, table) = fixture();
        let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

        corrupt_root(&pager, table, &tree, &[]);
        let err = scan_err(&tree);
        assert!(err.contains("empty node payload"), "{err}");

        // A leaf entry declaring a 5-byte key with 1 byte present.
        corrupt_root(&pager, table, &tree, &[NODE_LEAF, 5, 0, 0, b'k']);
        let err = scan_err(&tree);
        assert!(err.contains("truncated node entry"), "{err}");
    }

    #[test]
    fn unknown_leaf_value_tags_are_reported_on_both_paths() {
        let (_dir, pager, table) = fixture();
        let tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        // One leaf entry: key_len=1, tag=2 (unknown), key 'k'.
        corrupt_root(&pager, table, &tree, &[NODE_LEAF, 1, 0, 2, b'k']);
        let err = get_err(&tree, b"k");
        assert!(err.contains("malformed leaf"), "{err}");
        let err = scan_err(&tree);
        assert!(err.contains("unknown leaf value tag 2"), "{err}");
    }

    #[test]
    fn an_interior_node_with_zero_entries_is_reported() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));
        corrupt_root(&pager, table, &tree, &[NODE_INTERIOR]);
        // get: the raw router finds no entry → malformed interior.
        let err = get_err(&tree, b"k");
        assert!(err.contains("malformed interior"), "{err}");
        // delete parses the node and routes through route_index.
        let err = match tree.delete(b"k") {
            Ok(hit) => panic!("corrupt interior deleted: {hit}"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("interior node with zero entries"), "{err}");
    }

    #[test]
    fn corrupt_overflow_chains_are_reported() {
        let (_dir, pager, table) = fixture();
        let mut tree = PagedTree::create(&pager, table, false).unwrap_or_else(|e| panic!("{e}"));

        // A short "overflow" page (payload < 8 bytes: no next pointer).
        let short_id = pager.allocate_page_id(table);
        let header = PageHeader::new(short_id, table.as_u32(), 0, FLAG_OVERFLOW);
        let image = encode_page(&header, &[1, 2, 3]).unwrap_or_else(|e| panic!("{e}"));
        drop(
            pager
                .install(table, short_id, image)
                .unwrap_or_else(|e| panic!("{e}")),
        );
        // A terminated chain page (next = NIL, no data bytes).
        let empty_id = pager.allocate_page_id(table);
        let header = PageHeader::new(empty_id, table.as_u32(), 0, FLAG_OVERFLOW);
        let image = encode_page(&header, &NIL.to_le_bytes()).unwrap_or_else(|e| panic!("{e}"));
        drop(
            pager
                .install(table, empty_id, image)
                .unwrap_or_else(|e| panic!("{e}")),
        );

        // Leaf with two overflow entries: "a" → short page, "b" → empty
        // chain claiming 10 bytes.
        let mut payload = vec![NODE_LEAF];
        for (key, head) in [(b'a', short_id), (b'b', empty_id)] {
            payload.extend_from_slice(&1u16.to_le_bytes());
            payload.push(1); // overflow tag
            payload.push(key);
            payload.extend_from_slice(&10u64.to_le_bytes()); // total_len
            payload.extend_from_slice(&head.to_le_bytes());
        }
        corrupt_root(&pager, table, &tree, &payload);

        let err = get_err(&tree, b"a");
        assert!(err.contains("overflow page too short"), "{err}");
        let err = get_err(&tree, b"b");
        assert!(err.contains("overflow chain length mismatch"), "{err}");
        // free_value walks the same chain on delete.
        let err = match tree.delete(b"a") {
            Ok(hit) => panic!("corrupt chain freed: {hit}"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("overflow page too short"), "{err}");
    }
}
