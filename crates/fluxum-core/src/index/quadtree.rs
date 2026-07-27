//! [`QuadTree`] — paged spatial point index (SPEC-008 §2,
//! SPX-001..SPX-004, T2.5; paged per SPEC-015 TIER-051).
//!
//! # Design (SPX-002)
//!
//! Nodes are stored flat in a paged B-tree ([`PagedTree`]) — so the index
//! faults in and evicts under `memory.budget` like every other index
//! (TIER-051/070) — keyed by their quadrant path from
//! the root packed into a sortable [`NodeKey`] — no pointer-chased node
//! graph, no `Box`/`Rc` child links, no unsafe code. Keys order by the path
//! bits *left-aligned* at [`MAX_DEPTH`] resolution (then by depth), so a
//! node sorts immediately before its descendants and a subtree visit is one
//! contiguous range scan of the map.
//!
//! # Canonical structure
//!
//! The tree shape is a pure function of the stored point multiset, never of
//! the operation order:
//!
//! - a region is a **leaf** iff it holds at most `bucket_size` entries or
//!   lies at [`MAX_DEPTH`] (coincident points cannot be separated by
//!   subdivision, so the deepest leaf is allowed to exceed the bucket);
//! - insert splits every leaf that outgrows its bucket; delete collapses the
//!   *highest* ancestor whose subtree fits one bucket again;
//! - leaf and overflow entries are kept sorted by `(x, y, pk)` under IEEE
//!   totalOrder.
//!
//! Consequence: after any commit or rollback the index compares
//! *bit-identical* to a fresh rebuild over `CommittedState` — the STG-007
//! rule-2 property the T2.4 suite established for B-tree indexes carries
//! over unchanged (`verify_index_integrity` covers spatial indexes too).
//! Like the B-tree indexes, maintenance rides the commit merge on the
//! private pre-swap copy (SPX-030), so rollback remains pure `TxState`
//! discard and the [`crate::store::UndoRecord`] hook stays uninhabited.
//!
//! # Geometry semantics
//!
//! - [`Rect`] covers `[x, x+w] × [y, y+h]`, **all edges inclusive**
//!   (SPX-020). A rect with negative or NaN extent contains nothing.
//! - [`QuadTree::query_point`] matches by IEEE `==` (so `-0.0` matches
//!   `0.0`), exactly like a full-scan `row.x == x && row.y == y` filter.
//! - Entry *identity* for [`QuadTree::insert`] / [`QuadTree::remove`] is the
//!   coordinate **bit pattern** (totalOrder) plus the PK — the store always
//!   removes with the exact values it inserted, so update coherence
//!   (SPX-032) is exact.
//! - Points outside the root bounds land in an **overflow bucket** that
//!   every query filters exactly (SPX-004: rows outside the configured
//!   bounds are still indexed correctly; only their lookup degrades to a
//!   linear scan of the overflow, never of the table).
//! - [`QuadTree::query_radius`] runs the SPX-021 recipe: prune with the
//!   bounding box `(x-r, y-r, 2r, 2r)`, then apply the exact squared
//!   Euclidean filter `dx² + dy² ≤ r²` to the candidates — rows at distance
//!   exactly `r` are included. Both filters use the same f64 arithmetic as a
//!   full-scan oracle would.
//!
//! | Operation | Complexity |
//! |---|---|
//! | Insert / delete | O(log n) |
//! | Point query | O(log n) |
//! | Region query | O(log n + k) |
//! | Radius query | O(log n + k′), k′ = bbox candidates |

use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::{FluxumError, Result};
use crate::store::TableId;
use crate::store::pager::{PagedTree, Pager};
use crate::store::row::PkBytes;

/// Axis-aligned rectangle covering `[x, x+w] × [y, y+h]`, bounds inclusive
/// (SPX-020). `w`/`h` are extents, not corners; a negative or NaN extent
/// yields an empty rectangle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rect {
    /// Bottom-left corner X.
    pub x: f64,
    /// Bottom-left corner Y.
    pub y: f64,
    /// Width (extent along X).
    pub w: f64,
    /// Height (extent along Y).
    pub h: f64,
}

impl Rect {
    /// A rectangle from its bottom-left corner and extents.
    pub const fn new(x: f64, y: f64, w: f64, h: f64) -> Self {
        Self { x, y, w, h }
    }

    /// Whether `(px, py)` lies inside (bounds inclusive). NaN anywhere is
    /// `false`; so is any point against a negative-extent rect.
    pub fn contains_point(&self, px: f64, py: f64) -> bool {
        px >= self.x && px <= self.x + self.w && py >= self.y && py <= self.y + self.h
    }

    /// Whether the two closed rectangles share at least one point.
    fn intersects(&self, other: &Rect) -> bool {
        self.x <= other.x + other.w
            && other.x <= self.x + self.w
            && self.y <= other.y + other.h
            && other.y <= self.y + self.h
    }

    /// Whether `other` lies entirely inside `self` (bounds inclusive).
    fn contains_rect(&self, other: &Rect) -> bool {
        other.x >= self.x
            && other.x + other.w <= self.x + self.w
            && other.y >= self.y
            && other.y + other.h <= self.y + self.h
    }
}

/// Maximum subdivision depth: 2 bits per level in the `u64` path. Leaves at
/// this depth never split (coincident or near-coincident points would recurse
/// forever), so they may exceed `bucket_size`.
const MAX_DEPTH: u8 = 32;

/// Quadrant path from the root packed into a sortable key (2 bits per
/// level). Children of `path` are `path * 4 + quadrant` at `depth + 1`;
/// ordering compares the path bits left-aligned at [`MAX_DEPTH`] resolution
/// (then depth), so a node sorts immediately before its descendants and a
/// subtree is one contiguous key range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeKey {
    depth: u8,
    path: u64,
}

impl NodeKey {
    const ROOT: Self = Self { depth: 0, path: 0 };

    /// The child key in `quadrant` (0..4).
    fn child(self, quadrant: u8) -> Self {
        Self {
            depth: self.depth + 1,
            path: (self.path << 2) | u64::from(quadrant),
        }
    }

    /// The path bits left-aligned at [`MAX_DEPTH`] resolution — the primary
    /// sort key. The root (depth 0, path 0) aligns to 0.
    fn aligned(self) -> u64 {
        if self.depth == 0 {
            0
        } else {
            self.path << (2 * u32::from(MAX_DEPTH - self.depth))
        }
    }

    /// First aligned value past this node's subtree; `None` when the subtree
    /// extends to the end of the key space.
    fn subtree_end(self) -> Option<u64> {
        if self.depth == 0 {
            return None; // the root's subtree is the whole map
        }
        let width = 1u64 << (2 * u32::from(MAX_DEPTH - self.depth));
        self.aligned().checked_add(width)
    }
}

impl Ord for NodeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.aligned()
            .cmp(&other.aligned())
            .then(self.depth.cmp(&other.depth))
    }
}

impl PartialOrd for NodeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One flat node: a leaf bucket or an interior marker whose four children
/// exist in the map.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    /// At most `bucket_size` entries (except at [`MAX_DEPTH`]), sorted by
    /// [`entry_cmp`].
    Leaf(Vec<Entry>),
    Internal,
}

/// Node-payload tags (values are opaque bytes to the pager, so these are
/// plain little-endian records, not a memcomparable encoding).
const TAG_LEAF: u8 = 0x00;
const TAG_INTERNAL: u8 = 0x01;

/// Key namespaces inside the one paged tree: quadrant-path nodes, and the
/// SPX-004 out-of-bounds overflow bucket. The prefix keeps both in a single
/// page file (one root, one version) while leaving each its own contiguous,
/// order-preserving key range.
const NS_NODE: u8 = 0x00;
const NS_OVERFLOW: u8 = 0x01;

/// `NodeKey` as memcomparable bytes: the same `(aligned, depth)` order the
/// [`Ord`] impl defines, so a subtree stays one contiguous key range.
fn node_key_bytes(key: NodeKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    out.push(NS_NODE);
    out.extend_from_slice(&key.aligned().to_be_bytes());
    out.push(key.depth);
    out
}

/// The first key past `key`'s subtree, or the end of the node namespace.
fn subtree_end_bytes(key: NodeKey) -> Vec<u8> {
    match key.subtree_end() {
        Some(end) => {
            let mut out = Vec::with_capacity(10);
            out.push(NS_NODE);
            out.extend_from_slice(&end.to_be_bytes());
            out
        }
        // The root's subtree is the whole namespace: stop at the next one.
        None => vec![NS_OVERFLOW],
    }
}

/// IEEE totalOrder byte encoding — the same transform the memcomparable
/// index keys use, so byte order equals `f64::total_cmp` order.
fn f64_bytes(v: f64) -> [u8; 8] {
    let bits = v.to_bits();
    let ordered = if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits ^ (1 << 63)
    };
    ordered.to_be_bytes()
}

/// An overflow entry's key: `(x, y, pk)` in [`cmp_key`] order.
fn overflow_key_bytes(x: f64, y: f64, pk: &PkBytes) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 16 + pk.as_bytes().len());
    out.push(NS_OVERFLOW);
    out.extend_from_slice(&f64_bytes(x));
    out.extend_from_slice(&f64_bytes(y));
    out.extend_from_slice(pk.as_bytes());
    out
}

/// Encode a node for its leaf payload.
fn encode_node(node: &Node) -> Vec<u8> {
    match node {
        Node::Internal => vec![TAG_INTERNAL],
        Node::Leaf(entries) => {
            let mut out = vec![TAG_LEAF];
            for entry in entries {
                out.extend_from_slice(&entry.x.to_le_bytes());
                out.extend_from_slice(&entry.y.to_le_bytes());
                let pk = entry.pk.as_bytes();
                out.extend_from_slice(&(pk.len() as u32).to_le_bytes());
                out.extend_from_slice(pk);
            }
            out
        }
    }
}

/// Recover a [`NodeKey`] from its stored key bytes.
fn decode_node_key(raw: &[u8]) -> Result<NodeKey> {
    if raw.len() != 10 || raw[0] != NS_NODE {
        return Err(FluxumError::Storage(
            "malformed paged quadtree node key".into(),
        ));
    }
    let aligned = u64::from_be_bytes(
        raw[1..9]
            .try_into()
            .map_err(|_| FluxumError::Storage("malformed paged quadtree node key".into()))?,
    );
    let depth = raw[9];
    // `aligned` is the path left-shifted to MAX_DEPTH resolution; undo it.
    let path = if depth == 0 {
        0
    } else {
        aligned >> (2 * u32::from(MAX_DEPTH - depth))
    };
    Ok(NodeKey { depth, path })
}

/// An overflow entry stores its `(x, y, pk)` in the key (so the bucket stays
/// sorted by [`cmp_key`]) and nothing in the value.
fn decode_overflow(raw: &[u8], _value: &[u8]) -> Result<Entry> {
    let malformed = || FluxumError::Storage("malformed paged quadtree overflow key".into());
    if raw.len() < 17 || raw[0] != NS_OVERFLOW {
        return Err(malformed());
    }
    let unorder = |bytes: &[u8]| -> Result<f64> {
        let ordered = u64::from_be_bytes(bytes.try_into().map_err(|_| malformed())?);
        let bits = if ordered & (1 << 63) != 0 {
            ordered ^ (1 << 63)
        } else {
            !ordered
        };
        Ok(f64::from_bits(bits))
    };
    Ok(Entry {
        x: unorder(&raw[1..9])?,
        y: unorder(&raw[9..17])?,
        pk: PkBytes::from_bytes(raw[17..].to_vec()),
    })
}

/// Decode a node payload written by [`encode_node`].
fn decode_node(bytes: &[u8]) -> Result<Node> {
    let malformed = || FluxumError::Storage("malformed paged quadtree node".into());
    let (tag, mut rest) = bytes.split_first().ok_or_else(malformed)?;
    match *tag {
        TAG_INTERNAL => Ok(Node::Internal),
        TAG_LEAF => {
            let mut entries = Vec::new();
            while !rest.is_empty() {
                if rest.len() < 20 {
                    return Err(malformed());
                }
                let x = f64::from_le_bytes(rest[..8].try_into().map_err(|_| malformed())?);
                let y = f64::from_le_bytes(rest[8..16].try_into().map_err(|_| malformed())?);
                let len =
                    u32::from_le_bytes(rest[16..20].try_into().map_err(|_| malformed())?) as usize;
                let end = 20 + len;
                let pk = rest.get(20..end).ok_or_else(malformed)?.to_vec();
                entries.push(Entry {
                    x,
                    y,
                    pk: PkBytes::from_bytes(pk),
                });
                rest = &rest[end..];
            }
            Ok(Node::Leaf(entries))
        }
        _ => Err(malformed()),
    }
}

/// One indexed point: coordinates (widened to f64) plus the row's PK.
#[derive(Debug, Clone, PartialEq)]
struct Entry {
    x: f64,
    y: f64,
    pk: PkBytes,
}

/// Canonical entry order: `(x, y, pk)` with floats under IEEE totalOrder —
/// total, so sorted leaves are deterministic for any input.
fn entry_cmp(a: &Entry, b: &Entry) -> Ordering {
    cmp_key(a, b.x, b.y, &b.pk)
}

/// [`entry_cmp`] against an unpacked key (avoids building a probe `Entry`).
fn cmp_key(e: &Entry, x: f64, y: f64, pk: &PkBytes) -> Ordering {
    e.x.total_cmp(&x)
        .then_with(|| e.y.total_cmp(&y))
        .then_with(|| e.pk.cmp(pk))
}

/// The quadrant of `(x, y)` within `rect`: bit 0 = east of the X midline,
/// bit 1 = north of the Y midline. Points exactly on a midline go east /
/// north — deterministic, and identical for IEEE-equal values (`-0.0` routes
/// like `0.0`).
fn quadrant_of(rect: &Rect, x: f64, y: f64) -> u8 {
    let mid_x = rect.x + rect.w / 2.0;
    let mid_y = rect.y + rect.h / 2.0;
    (u8::from(y >= mid_y) << 1) | u8::from(x >= mid_x)
}

/// The sub-rectangle of `rect` for `quadrant`, built from corners so sibling
/// rects share their boundary exactly (a point on the midline is inside both
/// closed halves; routing picks one, queries check both).
fn child_rect(rect: &Rect, quadrant: u8) -> Rect {
    let (x0, x1) = (rect.x, rect.x + rect.w);
    let (y0, y1) = (rect.y, rect.y + rect.h);
    let mid_x = rect.x + rect.w / 2.0;
    let mid_y = rect.y + rect.h / 2.0;
    let (x, w) = if quadrant & 1 == 0 {
        (x0, mid_x - x0)
    } else {
        (mid_x, x1 - mid_x)
    };
    let (y, h) = if quadrant & 2 == 0 {
        (y0, mid_y - y0)
    } else {
        (mid_y, y1 - mid_y)
    };
    Rect::new(x, y, w, h)
}

/// The QuadTree spatial index (SPX-002): flat, sorted node storage served
/// through the paged cold tier, an overflow bucket for out-of-bounds points,
/// canonical structure (see the module docs).
///
/// Storage is a [`PagedTree`] (SPEC-015 TIER-051): nodes and overflow
/// entries are keys in one page file, so the index faults in and evicts
/// under `memory.budget` exactly like rows and B-tree indexes — a
/// spatial-dominated dataset stays bounded (TIER-070). The flat, sortable
/// [`NodeKey`] the design already used maps onto paged keys unchanged, so a
/// subtree remains one contiguous key range and every algorithm below is the
/// resident one with its map operations swapped for paged ones.
#[derive(Debug, Clone)]
pub struct QuadTree {
    /// Root bounds (SPX-004).
    bounds: Rect,
    /// Max entries per leaf before it splits, default 8 (SPX-003).
    bucket_size: usize,
    /// Paged node + overflow storage — no pointer chasing (SPX-002), and no
    /// residency (TIER-051).
    store: PagedTree,
    /// Total indexed entries (tree + overflow); the paged tree has no O(1)
    /// length, and this counts entries rather than keys anyway.
    len: usize,
}

impl QuadTree {
    /// The default leaf bucket size (SPX-003).
    pub const DEFAULT_BUCKET_SIZE: usize = 8;

    /// An empty QuadTree over `bounds`, storing its nodes in `table_id`'s
    /// page file. `bucket_size` below 1 is clamped to 1.
    pub fn new(
        bounds: Rect,
        bucket_size: usize,
        pager: &Arc<Pager>,
        table_id: TableId,
    ) -> Result<Self> {
        let mut tree = Self {
            bounds,
            bucket_size: bucket_size.max(1),
            store: PagedTree::create(pager, table_id, true)?,
            len: 0,
        };
        // The canonical empty tree is one empty root leaf.
        tree.put_node(NodeKey::ROOT, &Node::Leaf(Vec::new()), &mut Vec::new())?;
        Ok(tree)
    }

    // --- paged node storage ---------------------------------------------

    /// The node at `key`, if the slot exists.
    fn node(&self, key: NodeKey) -> Result<Option<Node>> {
        match self.store.get(&node_key_bytes(key))? {
            Some(bytes) => decode_node(&bytes).map(Some),
            None => Ok(None),
        }
    }

    /// Write `node` at `key` (copy-on-write; superseded pages are retired by
    /// the caller's version reclaimer, TIER-061).
    fn put_node(&mut self, key: NodeKey, node: &Node, sup: &mut Vec<u64>) -> Result<()> {
        self.store
            .insert_cow(&node_key_bytes(key), &encode_node(node), sup)
    }

    /// Delete the node slot at `key`.
    fn take_node(&mut self, key: NodeKey, sup: &mut Vec<u64>) -> Result<Option<Node>> {
        let existing = self.node(key)?;
        if existing.is_some() {
            self.store.delete_cow(&node_key_bytes(key), sup)?;
        }
        Ok(existing)
    }

    /// Visit `key`'s subtree in key order — one contiguous range scan of the
    /// node namespace. `f` returns `false` to stop early.
    fn for_each_in_subtree(
        &self,
        key: NodeKey,
        mut f: impl FnMut(NodeKey, Node) -> Result<bool>,
    ) -> Result<()> {
        let start = node_key_bytes(key);
        let end = subtree_end_bytes(key);
        self.store.scan(&start, Some(&end), &mut |raw, value| {
            let node = decode_node(value)?;
            f(decode_node_key(raw)?, node)
        })?;
        Ok(())
    }

    /// Visit every overflow entry (SPX-004). `f` returns `false` to stop.
    fn for_each_overflow(&self, mut f: impl FnMut(Entry) -> Result<bool>) -> Result<()> {
        let start = [NS_OVERFLOW];
        self.store.scan(&start, None, &mut |raw, value| {
            f(decode_overflow(raw, value)?)
        })?;
        Ok(())
    }

    /// The root bounds this tree was initialised with (SPX-004).
    pub fn bounds(&self) -> Rect {
        self.bounds
    }

    /// The configured leaf bucket size (SPX-003).
    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }

    /// Number of indexed entries.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no entry is indexed.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Index `(x, y) → pk`. Returns `false` (and changes nothing) when this
    /// exact entry — same coordinate bit patterns, same PK — is already
    /// present. O(log n).
    pub fn insert(&mut self, x: f64, y: f64, pk: PkBytes, sup: &mut Vec<u64>) -> Result<bool> {
        if !self.bounds.contains_point(x, y) {
            // The overflow bucket is keyed by `(x, y, pk)`, so presence is a
            // point lookup and the bucket stays sorted by construction.
            let key = overflow_key_bytes(x, y, &pk);
            if self.store.get(&key)?.is_some() {
                return Ok(false);
            }
            self.store.insert_cow(&key, &[], sup)?;
            self.len += 1;
            return Ok(true);
        }
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            // Descent-path nodes always exist under the canonical invariant;
            // a vacant slot is repaired as an empty leaf, never an error.
            match self.node(key)?.unwrap_or(Node::Leaf(Vec::new())) {
                Node::Internal => {
                    let q = quadrant_of(&rect, x, y);
                    rect = child_rect(&rect, q);
                    key = key.child(q);
                }
                Node::Leaf(mut entries) => {
                    match entries.binary_search_by(|e| cmp_key(e, x, y, &pk)) {
                        Ok(_) => return Ok(false),
                        Err(pos) => entries.insert(pos, Entry { x, y, pk }),
                    }
                    let must_split = entries.len() > self.bucket_size && key.depth < MAX_DEPTH;
                    self.put_node(key, &Node::Leaf(entries), sup)?;
                    self.len += 1;
                    if must_split {
                        self.split(key, rect, sup)?;
                    }
                    return Ok(true);
                }
            }
        }
    }

    /// Remove the entry `(x, y) → pk` (coordinates matched by bit pattern —
    /// remove with the exact values that were inserted). Returns whether an
    /// entry was removed. O(log n).
    pub fn remove(&mut self, x: f64, y: f64, pk: &PkBytes, sup: &mut Vec<u64>) -> Result<bool> {
        if !self.bounds.contains_point(x, y) {
            let key = overflow_key_bytes(x, y, pk);
            if self.store.get(&key)?.is_none() {
                return Ok(false);
            }
            self.store.delete_cow(&key, sup)?;
            self.len -= 1;
            return Ok(true);
        }
        let mut ancestors = Vec::new();
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            match self.node(key)? {
                None => return Ok(false),
                Some(Node::Internal) => {
                    ancestors.push(key);
                    let q = quadrant_of(&rect, x, y);
                    rect = child_rect(&rect, q);
                    key = key.child(q);
                }
                Some(Node::Leaf(mut entries)) => {
                    match entries.binary_search_by(|e| cmp_key(e, x, y, pk)) {
                        Ok(pos) => {
                            entries.remove(pos);
                        }
                        Err(_) => return Ok(false),
                    }
                    self.put_node(key, &Node::Leaf(entries), sup)?;
                    self.len -= 1;
                    break;
                }
            }
        }
        // Canonical collapse: the *highest* ancestor whose subtree fits one
        // bucket again becomes a leaf (deeper ancestors vanish with it).
        for ancestor in ancestors {
            if self.subtree_len_at_most(ancestor, self.bucket_size)? {
                self.collapse(ancestor, sup)?;
                break;
            }
        }
        Ok(true)
    }

    /// PKs of every entry at exactly `(x, y)` under IEEE `==`. O(log n) plus
    /// the coincident-point count.
    pub fn query_point(&self, x: f64, y: f64) -> Result<Vec<PkBytes>> {
        let mut out = Vec::new();
        if !self.bounds.contains_point(x, y) {
            self.filter_overflow(&mut out, |e| e.x == x && e.y == y)?;
            return Ok(out);
        }
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            match self.node(key)? {
                None => return Ok(out),
                Some(Node::Internal) => {
                    let q = quadrant_of(&rect, x, y);
                    rect = child_rect(&rect, q);
                    key = key.child(q);
                }
                Some(Node::Leaf(entries)) => {
                    out.extend(
                        entries
                            .iter()
                            .filter(|e| e.x == x && e.y == y)
                            .map(|e| e.pk.clone()),
                    );
                    return Ok(out);
                }
            }
        }
    }

    /// PKs of every entry inside `region` (bounds inclusive, SPX-020).
    /// O(log n + k). An empty (negative/NaN extent) region matches nothing.
    pub fn query_region(&self, region: Rect) -> Result<Vec<PkBytes>> {
        let mut out = Vec::new();
        self.filter_overflow(&mut out, |e| region.contains_point(e.x, e.y))?;
        let mut stack = vec![(NodeKey::ROOT, self.bounds)];
        while let Some((key, rect)) = stack.pop() {
            if !region.intersects(&rect) {
                continue;
            }
            match self.node(key)? {
                None => {}
                Some(Node::Leaf(entries)) => {
                    out.extend(
                        entries
                            .iter()
                            .filter(|e| region.contains_point(e.x, e.y))
                            .map(|e| e.pk.clone()),
                    );
                }
                Some(Node::Internal) => {
                    if region.contains_rect(&rect) {
                        // Every subtree entry matches: one contiguous range
                        // scan, no per-entry geometry (the O(k) arm).
                        self.collect_subtree(key, &mut out)?;
                    } else {
                        for q in 0..4u8 {
                            stack.push((key.child(q), child_rect(&rect, q)));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// PKs of every entry within Euclidean distance `r` of `(x, y)`,
    /// distance exactly `r` included (SPX-021): bounding-box prefilter, then
    /// the exact squared-distance filter on the candidates. A negative or
    /// NaN radius matches nothing. O(log n + k′).
    pub fn query_radius(&self, x: f64, y: f64, r: f64) -> Result<Vec<PkBytes>> {
        if r.is_nan() || r < 0.0 {
            return Ok(Vec::new());
        }
        let rr = r * r;
        let within = |e: &Entry| {
            let (dx, dy) = (e.x - x, e.y - y);
            dx * dx + dy * dy <= rr
        };
        let bbox = Rect::new(x - r, y - r, 2.0 * r, 2.0 * r);
        let mut out = Vec::new();
        self.filter_overflow(&mut out, within)?;
        let mut stack = vec![(NodeKey::ROOT, self.bounds)];
        while let Some((key, rect)) = stack.pop() {
            if !bbox.intersects(&rect) {
                continue;
            }
            match self.node(key)? {
                None => {}
                Some(Node::Leaf(entries)) => {
                    out.extend(entries.iter().filter(|e| within(e)).map(|e| e.pk.clone()));
                }
                Some(Node::Internal) => {
                    for q in 0..4u8 {
                        stack.push((key.child(q), child_rect(&rect, q)));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Push the PKs of overflow entries matching `keep` onto `out`.
    fn filter_overflow(&self, out: &mut Vec<PkBytes>, keep: impl Fn(&Entry) -> bool) -> Result<()> {
        self.for_each_overflow(|entry| {
            if keep(&entry) {
                out.push(entry.pk);
            }
            Ok(true)
        })
    }

    /// Whether `key`'s subtree holds at most `cap` entries (early exit).
    fn subtree_len_at_most(&self, key: NodeKey, cap: usize) -> Result<bool> {
        let mut total = 0usize;
        let mut within = true;
        self.for_each_in_subtree(key, |_, node| {
            if let Node::Leaf(entries) = node {
                total += entries.len();
                if total > cap {
                    within = false;
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        Ok(within)
    }

    /// Append every PK in `key`'s subtree to `out` (contiguous range scan).
    fn collect_subtree(&self, key: NodeKey, out: &mut Vec<PkBytes>) -> Result<()> {
        self.for_each_in_subtree(key, |_, node| {
            if let Node::Leaf(entries) = node {
                out.extend(entries.into_iter().map(|e| e.pk));
            }
            Ok(true)
        })
    }

    /// Split the leaf at `key` into four children, cascading while a child
    /// still overflows (coincident points stop at [`MAX_DEPTH`]).
    fn split(&mut self, key: NodeKey, rect: Rect, sup: &mut Vec<u64>) -> Result<()> {
        let mut work = vec![(key, rect)];
        while let Some((key, rect)) = work.pop() {
            let Some(Node::Leaf(entries)) = self.node(key)? else {
                continue;
            };
            if entries.len() <= self.bucket_size || key.depth >= MAX_DEPTH {
                continue;
            }
            self.put_node(key, &Node::Internal, sup)?;
            let mut children: [Vec<Entry>; 4] = [const { Vec::new() }; 4];
            for entry in entries {
                let q = quadrant_of(&rect, entry.x, entry.y);
                // Splitting a sorted leaf: each child keeps a subsequence,
                // so children stay sorted by `entry_cmp`.
                children[usize::from(q)].push(entry);
            }
            for (q, bucket) in children.into_iter().enumerate() {
                let q = u8::try_from(q).unwrap_or(3); // q < 4 by construction
                let child_key = key.child(q);
                let overflowing = bucket.len() > self.bucket_size;
                self.put_node(child_key, &Node::Leaf(bucket), sup)?;
                if overflowing && child_key.depth < MAX_DEPTH {
                    work.push((child_key, child_rect(&rect, q)));
                }
            }
        }
        Ok(())
    }

    /// Replace `key`'s whole subtree by one leaf holding its entries.
    fn collapse(&mut self, key: NodeKey, sup: &mut Vec<u64>) -> Result<()> {
        let mut keys = Vec::new();
        let mut entries = Vec::new();
        self.for_each_in_subtree(key, |k, node| {
            keys.push(k);
            if let Node::Leaf(mut leaf) = node {
                entries.append(&mut leaf);
            }
            Ok(true)
        })?;
        for k in keys {
            self.take_node(k, sup)?;
        }
        entries.sort_by(entry_cmp);
        self.put_node(key, &Node::Leaf(entries), sup)
    }

    /// Every `(x, y, pk)` this index holds, in canonical key order — the
    /// STG-007 rule-2 comparison surface (paged structures have no
    /// meaningful structural equality; contents are what must match a fresh
    /// rebuild).
    pub(crate) fn entries(&self) -> Result<Vec<(f64, f64, PkBytes)>> {
        let mut out = Vec::new();
        self.for_each_in_subtree(NodeKey::ROOT, |_, node| {
            if let Node::Leaf(entries) = node {
                out.extend(entries.into_iter().map(|e| (e.x, e.y, e.pk)));
            }
            Ok(true)
        })?;
        self.for_each_overflow(|e| {
            out.push((e.x, e.y, e.pk));
            Ok(true)
        })?;
        out.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then_with(|| a.1.total_cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        Ok(out)
    }
}

#[cfg(test)]
impl QuadTree {
    /// Test wrapper: insert, discarding the superseded-page sink.
    fn t_insert(&mut self, x: f64, y: f64, pk: PkBytes) -> bool {
        self.insert(x, y, pk, &mut Vec::new())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Test wrapper: remove, discarding the superseded-page sink.
    fn t_remove(&mut self, x: f64, y: f64, pk: &PkBytes) -> bool {
        self.remove(x, y, pk, &mut Vec::new())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn t_query_point(&self, x: f64, y: f64) -> Vec<PkBytes> {
        self.query_point(x, y).unwrap_or_else(|e| panic!("{e}"))
    }

    fn t_query_region(&self, region: Rect) -> Vec<PkBytes> {
        self.query_region(region).unwrap_or_else(|e| panic!("{e}"))
    }

    fn t_query_radius(&self, x: f64, y: f64, r: f64) -> Vec<PkBytes> {
        self.query_radius(x, y, r).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Every node in key order — the canonical *shape*, which paged storage
    /// cannot compare structurally but which the tree still guarantees.
    fn t_shape(&self) -> Vec<(NodeKey, Node)> {
        let mut out = Vec::new();
        self.for_each_in_subtree(NodeKey::ROOT, |key, node| {
            out.push((key, node));
            Ok(true)
        })
        .unwrap_or_else(|e| panic!("{e}"));
        out
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use proptest::prelude::*;

    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
    use crate::store::row::{RowValue, encode_pk_values};

    /// A distinct `PkBytes` per `n` (FluxBIN-encoded u64, like the store).
    fn pk(n: u64) -> PkBytes {
        static COLS: &[ColumnSchema] = &[ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        }];
        static T: TableSchema = TableSchema {
            name: "P",
            columns: COLS,
            primary_key: &[0],
            auto_inc: None,
            access: TableAccess::Private,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        };
        encode_pk_values(&T, &[RowValue::U64(n)]).unwrap()
    }

    fn bounds() -> Rect {
        Rect::new(0.0, 0.0, 100.0, 100.0)
    }

    /// A throwaway pager, one fresh directory per call (parallel test
    /// threads must never share a page file for the same table id).
    fn test_pager() -> Arc<Pager> {
        use crate::config::PageCompression;
        use crate::store::pager::PagerOptions;
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fluxum-qt-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        Pager::open(
            dir,
            PagerOptions {
                shard_id: 0,
                page_size: 4096,
                pool_capacity_bytes: 512 * 4096,
                high_watermark: 0.95,
                low_watermark: 0.90,
                compression: PageCompression::None,
                compression_min_bytes: 1024,
            },
        )
        .unwrap_or_else(|e| panic!("{e}"))
    }

    /// An empty paged QuadTree over [`bounds`] with `bucket` capacity.
    fn quadtree(b: Rect, bucket: usize) -> QuadTree {
        QuadTree::new(b, bucket, &test_pager(), TableId::of("QuadTreeTest"))
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn sorted(mut pks: Vec<PkBytes>) -> Vec<PkBytes> {
        pks.sort();
        pks
    }

    #[test]
    fn node_key_orders_parent_before_contiguous_subtree() {
        let root = NodeKey::ROOT;
        let c2 = root.child(2);
        let c2_0 = c2.child(0);
        let c2_3 = c2.child(3);
        let c3 = root.child(3);
        assert!(root < c2 && c2 < c2_0 && c2_0 < c2_3 && c2_3 < c3);
        // c2's subtree range excludes c3.
        let end = c2.subtree_end().unwrap();
        assert!(c2_3.aligned() < end);
        assert!(c3.aligned() >= end);
        // The root subtree is unbounded.
        assert!(root.subtree_end().is_none());
    }

    #[test]
    fn insert_query_remove_roundtrip_and_len() {
        let mut qt = quadtree(bounds(), 2);
        assert!(qt.is_empty());
        assert!(qt.t_insert(10.0, 20.0, pk(1)));
        assert!(qt.t_insert(10.0, 20.0, pk(2))); // coincident, distinct pk
        assert!(!qt.t_insert(10.0, 20.0, pk(1))); // exact duplicate: no-op
        assert_eq!(qt.len(), 2);
        assert_eq!(
            sorted(qt.t_query_point(10.0, 20.0)),
            sorted(vec![pk(1), pk(2)])
        );
        assert!(qt.t_query_point(10.0, 20.1).is_empty());
        assert!(qt.t_remove(10.0, 20.0, &pk(1)));
        assert!(!qt.t_remove(10.0, 20.0, &pk(1))); // already gone
        assert!(!qt.t_remove(99.0, 99.0, &pk(2))); // wrong coords
        assert_eq!(qt.t_query_point(10.0, 20.0), vec![pk(2)]);
        assert_eq!(qt.len(), 1);
    }

    #[test]
    fn coincident_points_beyond_bucket_size_stay_queryable() {
        // bucket_size 1 with 20 coincident points: subdivision cannot
        // separate them; the MAX_DEPTH cap must stop the split cascade.
        let mut qt = quadtree(bounds(), 1);
        for i in 0..20 {
            assert!(qt.t_insert(33.0, 66.0, pk(i)));
        }
        assert_eq!(qt.len(), 20);
        assert_eq!(qt.t_query_point(33.0, 66.0).len(), 20);
        assert_eq!(qt.t_query_radius(33.0, 66.0, 0.0).len(), 20);
        for i in 0..20 {
            assert!(qt.t_remove(33.0, 66.0, &pk(i)));
        }
        assert!(qt.is_empty());
        assert_eq!(qt.t_shape(), quadtree(bounds(), 1).t_shape()); // canonical empty shape
    }

    #[test]
    fn removals_collapse_split_subtrees_back_to_the_canonical_leaf() {
        let mut qt = quadtree(bounds(), 2);
        // Five spread points overflow the bucket and split the root.
        qt.t_insert(10.0, 10.0, pk(1));
        qt.t_insert(90.0, 10.0, pk(2));
        qt.t_insert(10.0, 90.0, pk(3));
        qt.t_insert(90.0, 90.0, pk(4));
        qt.t_insert(60.0, 60.0, pk(5));
        assert_eq!(qt.len(), 5);
        assert_eq!(
            sorted(qt.t_query_region(Rect::new(0.0, 0.0, 100.0, 100.0))),
            sorted((1u64..=5).map(pk).collect::<Vec<_>>())
        );

        // Dropping to bucket size collapses the subtree into one sorted
        // leaf; the result must be bit-identical to a fresh tree over the
        // surviving points (canonical structure).
        assert!(qt.t_remove(90.0, 10.0, &pk(2)));
        assert!(qt.t_remove(10.0, 90.0, &pk(3)));
        assert!(qt.t_remove(90.0, 90.0, &pk(4)));
        assert_eq!(qt.len(), 2);
        let mut fresh = quadtree(bounds(), 2);
        fresh.t_insert(10.0, 10.0, pk(1));
        fresh.t_insert(60.0, 60.0, pk(5));
        assert_eq!(
            qt.t_shape(),
            fresh.t_shape(),
            "collapse must restore the canonical shape"
        );
        assert_eq!(
            sorted(qt.t_query_region(Rect::new(0.0, 0.0, 100.0, 100.0))),
            sorted(vec![pk(1), pk(5)])
        );
    }

    #[test]
    fn root_edges_are_inclusive_and_outside_points_use_overflow() {
        let mut qt = quadtree(bounds(), 2);
        // All four corners and an edge midpoint are in bounds.
        qt.t_insert(0.0, 0.0, pk(1));
        qt.t_insert(100.0, 0.0, pk(2));
        qt.t_insert(0.0, 100.0, pk(3));
        qt.t_insert(100.0, 100.0, pk(4));
        qt.t_insert(50.0, 100.0, pk(5));
        // Outside the root bounds: overflow, still indexed (SPX-004).
        qt.t_insert(-1.0, 50.0, pk(6));
        qt.t_insert(101.0, 50.0, pk(7));
        assert_eq!(qt.len(), 7);
        let all = qt.t_query_region(Rect::new(-10.0, -10.0, 120.0, 120.0));
        assert_eq!(sorted(all), sorted((1..=7).map(pk).collect::<Vec<_>>()));
        assert_eq!(qt.t_query_point(-1.0, 50.0), vec![pk(6)]);
        assert!(qt.t_remove(-1.0, 50.0, &pk(6)));
        assert!(qt.t_query_point(-1.0, 50.0).is_empty());
    }

    #[test]
    fn region_edges_inclusive_degenerate_and_negative_extents() {
        let mut qt = quadtree(bounds(), 2);
        qt.t_insert(10.0, 10.0, pk(1));
        qt.t_insert(20.0, 20.0, pk(2));
        // Bounds inclusive on both edges.
        assert_eq!(
            sorted(qt.t_query_region(Rect::new(10.0, 10.0, 10.0, 10.0))),
            sorted(vec![pk(1), pk(2)])
        );
        // Degenerate zero-extent region is a point probe.
        assert_eq!(
            qt.t_query_region(Rect::new(10.0, 10.0, 0.0, 0.0)),
            vec![pk(1)]
        );
        // Negative extent matches nothing.
        assert!(
            qt.t_query_region(Rect::new(15.0, 15.0, -10.0, 10.0))
                .is_empty()
        );
        assert!(
            qt.t_query_region(Rect::new(15.0, 15.0, 10.0, -10.0))
                .is_empty()
        );
        // NaN extent matches nothing.
        assert!(
            qt.t_query_region(Rect::new(0.0, 0.0, f64::NAN, 10.0))
                .is_empty()
        );
    }

    #[test]
    fn radius_includes_exact_distance_and_rejects_negative_r() {
        let mut qt = quadtree(bounds(), 2);
        qt.t_insert(53.0, 50.0, pk(1)); // distance exactly 3 from (50, 50)
        qt.t_insert(50.0, 47.0, pk(2)); // distance exactly 3
        qt.t_insert(53.0, 53.0, pk(3)); // distance 3√2 > 3 (bbox candidate)
        assert_eq!(
            sorted(qt.t_query_radius(50.0, 50.0, 3.0)),
            sorted(vec![pk(1), pk(2)])
        );
        assert_eq!(qt.t_query_radius(53.0, 50.0, 0.0), vec![pk(1)]);
        assert!(qt.t_query_radius(50.0, 50.0, -1.0).is_empty());
        assert!(qt.t_query_radius(50.0, 50.0, f64::NAN).is_empty());
    }

    #[test]
    fn quadrant_midline_points_are_found_by_straddling_queries() {
        let mut qt = quadtree(bounds(), 1);
        // Force splits, then place points exactly on the root midlines.
        for i in 0u32..4 {
            qt.t_insert(10.0 + f64::from(i), 10.0, pk(100 + u64::from(i)));
        }
        qt.t_insert(50.0, 50.0, pk(1)); // dead centre
        qt.t_insert(50.0, 10.0, pk(2)); // on the X midline
        qt.t_insert(10.0, 50.0, pk(3)); // on the Y midline
        assert_eq!(qt.t_query_point(50.0, 50.0), vec![pk(1)]);
        // A region ending exactly on the midline still sees midline points.
        assert!(
            qt.t_query_region(Rect::new(0.0, 0.0, 50.0, 50.0))
                .contains(&pk(1))
        );
        assert!(
            qt.t_query_region(Rect::new(50.0, 50.0, 50.0, 50.0))
                .contains(&pk(1))
        );
        assert!(
            qt.t_query_region(Rect::new(40.0, 0.0, 10.0, 20.0))
                .contains(&pk(2))
        );
        assert!(
            qt.t_query_region(Rect::new(0.0, 40.0, 20.0, 10.0))
                .contains(&pk(3))
        );
    }

    #[test]
    fn bucket_size_zero_is_clamped_and_default_is_eight() {
        assert_eq!(quadtree(bounds(), 0).bucket_size(), 1);
        assert_eq!(QuadTree::DEFAULT_BUCKET_SIZE, 8);
        let qt = quadtree(bounds(), QuadTree::DEFAULT_BUCKET_SIZE);
        assert_eq!(qt.bucket_size(), 8);
        assert_eq!(qt.bounds(), bounds());
    }

    /// Brute-force oracle: the same predicates over a flat entry list.
    #[derive(Default)]
    struct Oracle(Vec<(f64, f64, u64)>);

    impl Oracle {
        fn insert(&mut self, x: f64, y: f64, id: u64) -> bool {
            if self.0.iter().any(|&(ex, ey, eid)| {
                ex.total_cmp(&x).is_eq() && ey.total_cmp(&y).is_eq() && eid == id
            }) {
                return false;
            }
            self.0.push((x, y, id));
            true
        }

        fn remove(&mut self, x: f64, y: f64, id: u64) -> bool {
            let before = self.0.len();
            self.0.retain(|&(ex, ey, eid)| {
                !(ex.total_cmp(&x).is_eq() && ey.total_cmp(&y).is_eq() && eid == id)
            });
            self.0.len() != before
        }

        fn region(&self, r: Rect) -> Vec<PkBytes> {
            self.0
                .iter()
                .filter(|&&(x, y, _)| r.contains_point(x, y))
                .map(|&(_, _, id)| pk(id))
                .collect()
        }

        fn radius(&self, cx: f64, cy: f64, r: f64) -> Vec<PkBytes> {
            self.0
                .iter()
                .filter(|&&(x, y, _)| {
                    let (dx, dy) = (x - cx, y - cy);
                    dx * dx + dy * dy <= r * r
                })
                .map(|&(_, _, id)| pk(id))
                .collect()
        }

        fn point(&self, px: f64, py: f64) -> Vec<PkBytes> {
            self.0
                .iter()
                .filter(|&&(x, y, _)| x == px && y == py)
                .map(|&(_, _, id)| pk(id))
                .collect()
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert { x: f64, y: f64, id: u64 },
        Remove { x: f64, y: f64, id: u64 },
    }

    /// Coordinates on a small grid (including bounds edges, midlines, and
    /// out-of-bounds values) to force coincidences, quadrant-boundary
    /// routing, and overflow usage.
    fn coord() -> impl Strategy<Value = f64> {
        prop_oneof![
            (0u8..=8).prop_map(|i| f64::from(i) * 12.5), // 0, 12.5, …, 100
            Just(-5.0),                                  // out of bounds
            Just(105.0),                                 // out of bounds
        ]
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (coord(), coord(), 0u64..24).prop_map(|(x, y, id)| Op::Insert { x, y, id }),
            2 => (coord(), coord(), 0u64..24).prop_map(|(x, y, id)| Op::Remove { x, y, id }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        /// Index answers ≡ brute-force oracle for point / region / radius
        /// (boundary values included), for default and stress bucket sizes,
        /// and the structure stays canonical (equal to a fresh rebuild)
        /// after every operation sequence.
        #[test]
        fn quadtree_equals_the_brute_force_oracle(
            ops in prop::collection::vec(op_strategy(), 1..120),
            bucket in prop_oneof![Just(1usize), Just(2), Just(8)],
        ) {
            let mut qt = quadtree(bounds(), bucket);
            let mut oracle = Oracle::default();
            for op in ops {
                match op {
                    Op::Insert { x, y, id } => {
                        prop_assert_eq!(qt.t_insert(x, y, pk(id)), oracle.insert(x, y, id));
                    }
                    Op::Remove { x, y, id } => {
                        prop_assert_eq!(qt.t_remove(x, y, &pk(id)), oracle.remove(x, y, id));
                    }
                }
                prop_assert_eq!(qt.len(), oracle.0.len());
            }

            // Canonical structure: bit-identical to a fresh rebuild.
            let mut rebuilt = quadtree(bounds(), bucket);
            for &(x, y, id) in &oracle.0 {
                rebuilt.t_insert(x, y, pk(id));
            }
            prop_assert_eq!(qt.t_shape(), rebuilt.t_shape());

            // Queries across boundary-heavy shapes.
            let regions = [
                Rect::new(0.0, 0.0, 100.0, 100.0),
                Rect::new(12.5, 12.5, 37.5, 50.0),
                Rect::new(50.0, 50.0, 0.0, 0.0),
                Rect::new(-10.0, -10.0, 200.0, 200.0),
                Rect::new(87.5, 0.0, 30.0, 30.0),
                Rect::new(30.0, 30.0, -5.0, 5.0),
            ];
            for r in regions {
                prop_assert_eq!(sorted(qt.t_query_region(r)), sorted(oracle.region(r)));
            }
            let radii = [
                (50.0, 50.0, 12.5),
                (0.0, 0.0, 25.0),
                (100.0, 100.0, 0.0),
                (62.5, 37.5, 100.0),
                (-5.0, 50.0, 10.0),
            ];
            for (cx, cy, r) in radii {
                prop_assert_eq!(
                    sorted(qt.t_query_radius(cx, cy, r)),
                    sorted(oracle.radius(cx, cy, r)),
                    "radius ({}, {}, {})",
                    cx,
                    cy,
                    r
                );
            }
            for (px, py) in [(50.0, 50.0), (12.5, 87.5), (0.0, 0.0), (-5.0, 105.0)] {
                prop_assert_eq!(sorted(qt.t_query_point(px, py)), sorted(oracle.point(px, py)));
            }
        }

        /// SPX-003: every bucket size answers identically (default 8 vs
        /// non-default) over the same content.
        #[test]
        fn bucket_size_never_changes_query_results(
            points in prop::collection::vec((coord(), coord()), 1..60),
        ) {
            let mut default_qt = quadtree(bounds(), QuadTree::DEFAULT_BUCKET_SIZE);
            let mut tiny_qt = quadtree(bounds(), 1);
            for (id, &(x, y)) in points.iter().enumerate() {
                let id = id as u64;
                default_qt.t_insert(x, y, pk(id));
                tiny_qt.t_insert(x, y, pk(id));
            }
            let region = Rect::new(10.0, 10.0, 55.0, 65.0);
            prop_assert_eq!(
                sorted(default_qt.t_query_region(region)),
                sorted(tiny_qt.t_query_region(region))
            );
            prop_assert_eq!(
                sorted(default_qt.t_query_radius(50.0, 50.0, 30.0)),
                sorted(tiny_qt.t_query_radius(50.0, 50.0, 30.0))
            );
        }
    }
}
