//! [`QuadTree`] — BTreeMap-backed spatial point index (SPEC-008 §2,
//! SPX-001..SPX-004, T2.5).
//!
//! # Design (SPX-002)
//!
//! Nodes are stored flat in a `BTreeMap`, keyed by their quadrant path from
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
use std::collections::BTreeMap;

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

/// The QuadTree spatial index (SPX-002): flat `BTreeMap` node storage, an
/// overflow bucket for out-of-bounds points, canonical structure (see the
/// module docs).
#[derive(Debug, Clone, PartialEq)]
pub struct QuadTree {
    /// Root bounds (SPX-004).
    bounds: Rect,
    /// Max entries per leaf before it splits, default 8 (SPX-003).
    bucket_size: usize,
    /// Flat sorted node storage — no pointer chasing (SPX-002).
    nodes: BTreeMap<NodeKey, Node>,
    /// Entries outside `bounds`, sorted by [`entry_cmp`] (SPX-004: still
    /// indexed correctly; every query filters this bucket exactly).
    overflow: Vec<Entry>,
    /// Total indexed entries (tree + overflow).
    len: usize,
}

impl QuadTree {
    /// The default leaf bucket size (SPX-003).
    pub const DEFAULT_BUCKET_SIZE: usize = 8;

    /// An empty QuadTree over `bounds`. `bucket_size` below 1 is clamped
    /// to 1.
    pub fn new(bounds: Rect, bucket_size: usize) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(NodeKey::ROOT, Node::Leaf(Vec::new()));
        Self {
            bounds,
            bucket_size: bucket_size.max(1),
            nodes,
            overflow: Vec::new(),
            len: 0,
        }
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
    pub fn insert(&mut self, x: f64, y: f64, pk: PkBytes) -> bool {
        if !self.bounds.contains_point(x, y) {
            return match self.overflow.binary_search_by(|e| cmp_key(e, x, y, &pk)) {
                Ok(_) => false,
                Err(pos) => {
                    self.overflow.insert(pos, Entry { x, y, pk });
                    self.len += 1;
                    true
                }
            };
        }
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            // Descent-path nodes always exist under the canonical invariant;
            // a vacant slot is repaired as an empty leaf, never a panic.
            let node = self
                .nodes
                .entry(key)
                .or_insert_with(|| Node::Leaf(Vec::new()));
            match node {
                Node::Internal => {
                    let q = quadrant_of(&rect, x, y);
                    rect = child_rect(&rect, q);
                    key = key.child(q);
                }
                Node::Leaf(entries) => {
                    match entries.binary_search_by(|e| cmp_key(e, x, y, &pk)) {
                        Ok(_) => return false,
                        Err(pos) => entries.insert(pos, Entry { x, y, pk }),
                    }
                    let must_split = entries.len() > self.bucket_size && key.depth < MAX_DEPTH;
                    self.len += 1;
                    if must_split {
                        self.split(key, rect);
                    }
                    return true;
                }
            }
        }
    }

    /// Remove the entry `(x, y) → pk` (coordinates matched by bit pattern —
    /// remove with the exact values that were inserted). Returns whether an
    /// entry was removed. O(log n).
    pub fn remove(&mut self, x: f64, y: f64, pk: &PkBytes) -> bool {
        if !self.bounds.contains_point(x, y) {
            return match self.overflow.binary_search_by(|e| cmp_key(e, x, y, pk)) {
                Ok(pos) => {
                    self.overflow.remove(pos);
                    self.len -= 1;
                    true
                }
                Err(_) => false,
            };
        }
        let mut ancestors = Vec::new();
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            match self.nodes.get_mut(&key) {
                None => return false,
                Some(Node::Internal) => {
                    ancestors.push(key);
                    let q = quadrant_of(&rect, x, y);
                    rect = child_rect(&rect, q);
                    key = key.child(q);
                }
                Some(Node::Leaf(entries)) => {
                    match entries.binary_search_by(|e| cmp_key(e, x, y, pk)) {
                        Ok(pos) => {
                            entries.remove(pos);
                        }
                        Err(_) => return false,
                    }
                    self.len -= 1;
                    break;
                }
            }
        }
        // Canonical collapse: the *highest* ancestor whose subtree fits one
        // bucket again becomes a leaf (deeper ancestors vanish with it).
        for ancestor in ancestors {
            if self.subtree_len_at_most(ancestor, self.bucket_size) {
                self.collapse(ancestor);
                break;
            }
        }
        true
    }

    /// PKs of every entry at exactly `(x, y)` under IEEE `==`. O(log n) plus
    /// the coincident-point count.
    pub fn query_point(&self, x: f64, y: f64) -> Vec<PkBytes> {
        let mut out = Vec::new();
        if !self.bounds.contains_point(x, y) {
            self.filter_overflow(&mut out, |e| e.x == x && e.y == y);
            return out;
        }
        let mut key = NodeKey::ROOT;
        let mut rect = self.bounds;
        loop {
            match self.nodes.get(&key) {
                None => return out,
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
                    return out;
                }
            }
        }
    }

    /// PKs of every entry inside `region` (bounds inclusive, SPX-020).
    /// O(log n + k). An empty (negative/NaN extent) region matches nothing.
    pub fn query_region(&self, region: Rect) -> Vec<PkBytes> {
        let mut out = Vec::new();
        self.filter_overflow(&mut out, |e| region.contains_point(e.x, e.y));
        let mut stack = vec![(NodeKey::ROOT, self.bounds)];
        while let Some((key, rect)) = stack.pop() {
            if !region.intersects(&rect) {
                continue;
            }
            match self.nodes.get(&key) {
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
                        self.collect_subtree(key, &mut out);
                    } else {
                        for q in 0..4u8 {
                            stack.push((key.child(q), child_rect(&rect, q)));
                        }
                    }
                }
            }
        }
        out
    }

    /// PKs of every entry within Euclidean distance `r` of `(x, y)`,
    /// distance exactly `r` included (SPX-021): bounding-box prefilter, then
    /// the exact squared-distance filter on the candidates. A negative or
    /// NaN radius matches nothing. O(log n + k′).
    pub fn query_radius(&self, x: f64, y: f64, r: f64) -> Vec<PkBytes> {
        if r.is_nan() || r < 0.0 {
            return Vec::new();
        }
        let rr = r * r;
        let within = |e: &Entry| {
            let (dx, dy) = (e.x - x, e.y - y);
            dx * dx + dy * dy <= rr
        };
        let bbox = Rect::new(x - r, y - r, 2.0 * r, 2.0 * r);
        let mut out = Vec::new();
        self.filter_overflow(&mut out, within);
        let mut stack = vec![(NodeKey::ROOT, self.bounds)];
        while let Some((key, rect)) = stack.pop() {
            if !bbox.intersects(&rect) {
                continue;
            }
            match self.nodes.get(&key) {
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
        out
    }

    /// Push the PKs of overflow entries matching `keep` onto `out`.
    fn filter_overflow(&self, out: &mut Vec<PkBytes>, keep: impl Fn(&Entry) -> bool) {
        out.extend(
            self.overflow
                .iter()
                .filter(|e| keep(e))
                .map(|e| e.pk.clone()),
        );
    }

    /// All nodes of `key`'s subtree — one contiguous range of the flat map.
    fn subtree_range(&self, key: NodeKey) -> impl Iterator<Item = (&NodeKey, &Node)> {
        let end = key.subtree_end();
        self.nodes
            .range(key..)
            .take_while(move |(k, _)| end.is_none_or(|e| k.aligned() < e))
    }

    /// Whether `key`'s subtree holds at most `cap` entries (early exit).
    fn subtree_len_at_most(&self, key: NodeKey, cap: usize) -> bool {
        let mut total = 0usize;
        for (_, node) in self.subtree_range(key) {
            if let Node::Leaf(entries) = node {
                total += entries.len();
                if total > cap {
                    return false;
                }
            }
        }
        true
    }

    /// Append every PK in `key`'s subtree to `out` (contiguous range scan).
    fn collect_subtree(&self, key: NodeKey, out: &mut Vec<PkBytes>) {
        for (_, node) in self.subtree_range(key) {
            if let Node::Leaf(entries) = node {
                out.extend(entries.iter().map(|e| e.pk.clone()));
            }
        }
    }

    /// Split the leaf at `key` into four children, cascading while a child
    /// still overflows (coincident points stop at [`MAX_DEPTH`]).
    fn split(&mut self, key: NodeKey, rect: Rect) {
        let mut work = vec![(key, rect)];
        while let Some((key, rect)) = work.pop() {
            let Some(node) = self.nodes.get_mut(&key) else {
                continue;
            };
            let Node::Leaf(entries) = node else {
                continue;
            };
            if entries.len() <= self.bucket_size || key.depth >= MAX_DEPTH {
                continue;
            }
            let entries = std::mem::take(entries);
            *node = Node::Internal;
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
                self.nodes.insert(child_key, Node::Leaf(bucket));
                if overflowing && child_key.depth < MAX_DEPTH {
                    work.push((child_key, child_rect(&rect, q)));
                }
            }
        }
    }

    /// Replace `key`'s whole subtree by one leaf holding its entries.
    fn collapse(&mut self, key: NodeKey) {
        let keys: Vec<NodeKey> = self.subtree_range(key).map(|(k, _)| *k).collect();
        let mut entries = Vec::new();
        for k in &keys {
            if let Some(Node::Leaf(mut leaf)) = self.nodes.remove(k) {
                entries.append(&mut leaf);
            }
        }
        entries.sort_by(entry_cmp);
        self.nodes.insert(key, Node::Leaf(entries));
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
        let mut qt = QuadTree::new(bounds(), 2);
        assert!(qt.is_empty());
        assert!(qt.insert(10.0, 20.0, pk(1)));
        assert!(qt.insert(10.0, 20.0, pk(2))); // coincident, distinct pk
        assert!(!qt.insert(10.0, 20.0, pk(1))); // exact duplicate: no-op
        assert_eq!(qt.len(), 2);
        assert_eq!(
            sorted(qt.query_point(10.0, 20.0)),
            sorted(vec![pk(1), pk(2)])
        );
        assert!(qt.query_point(10.0, 20.1).is_empty());
        assert!(qt.remove(10.0, 20.0, &pk(1)));
        assert!(!qt.remove(10.0, 20.0, &pk(1))); // already gone
        assert!(!qt.remove(99.0, 99.0, &pk(2))); // wrong coords
        assert_eq!(qt.query_point(10.0, 20.0), vec![pk(2)]);
        assert_eq!(qt.len(), 1);
    }

    #[test]
    fn coincident_points_beyond_bucket_size_stay_queryable() {
        // bucket_size 1 with 20 coincident points: subdivision cannot
        // separate them; the MAX_DEPTH cap must stop the split cascade.
        let mut qt = QuadTree::new(bounds(), 1);
        for i in 0..20 {
            assert!(qt.insert(33.0, 66.0, pk(i)));
        }
        assert_eq!(qt.len(), 20);
        assert_eq!(qt.query_point(33.0, 66.0).len(), 20);
        assert_eq!(qt.query_radius(33.0, 66.0, 0.0).len(), 20);
        for i in 0..20 {
            assert!(qt.remove(33.0, 66.0, &pk(i)));
        }
        assert!(qt.is_empty());
        assert_eq!(qt, QuadTree::new(bounds(), 1)); // canonical empty shape
    }

    #[test]
    fn removals_collapse_split_subtrees_back_to_the_canonical_leaf() {
        let mut qt = QuadTree::new(bounds(), 2);
        // Five spread points overflow the bucket and split the root.
        qt.insert(10.0, 10.0, pk(1));
        qt.insert(90.0, 10.0, pk(2));
        qt.insert(10.0, 90.0, pk(3));
        qt.insert(90.0, 90.0, pk(4));
        qt.insert(60.0, 60.0, pk(5));
        assert_eq!(qt.len(), 5);
        assert_eq!(
            sorted(qt.query_region(Rect::new(0.0, 0.0, 100.0, 100.0))),
            sorted((1u64..=5).map(pk).collect::<Vec<_>>())
        );

        // Dropping to bucket size collapses the subtree into one sorted
        // leaf; the result must be bit-identical to a fresh tree over the
        // surviving points (canonical structure).
        assert!(qt.remove(90.0, 10.0, &pk(2)));
        assert!(qt.remove(10.0, 90.0, &pk(3)));
        assert!(qt.remove(90.0, 90.0, &pk(4)));
        assert_eq!(qt.len(), 2);
        let mut fresh = QuadTree::new(bounds(), 2);
        fresh.insert(10.0, 10.0, pk(1));
        fresh.insert(60.0, 60.0, pk(5));
        assert_eq!(qt, fresh, "collapse must restore the canonical shape");
        assert_eq!(
            sorted(qt.query_region(Rect::new(0.0, 0.0, 100.0, 100.0))),
            sorted(vec![pk(1), pk(5)])
        );
    }

    #[test]
    fn root_edges_are_inclusive_and_outside_points_use_overflow() {
        let mut qt = QuadTree::new(bounds(), 2);
        // All four corners and an edge midpoint are in bounds.
        qt.insert(0.0, 0.0, pk(1));
        qt.insert(100.0, 0.0, pk(2));
        qt.insert(0.0, 100.0, pk(3));
        qt.insert(100.0, 100.0, pk(4));
        qt.insert(50.0, 100.0, pk(5));
        // Outside the root bounds: overflow, still indexed (SPX-004).
        qt.insert(-1.0, 50.0, pk(6));
        qt.insert(101.0, 50.0, pk(7));
        assert_eq!(qt.len(), 7);
        let all = qt.query_region(Rect::new(-10.0, -10.0, 120.0, 120.0));
        assert_eq!(sorted(all), sorted((1..=7).map(pk).collect::<Vec<_>>()));
        assert_eq!(qt.query_point(-1.0, 50.0), vec![pk(6)]);
        assert!(qt.remove(-1.0, 50.0, &pk(6)));
        assert!(qt.query_point(-1.0, 50.0).is_empty());
    }

    #[test]
    fn region_edges_inclusive_degenerate_and_negative_extents() {
        let mut qt = QuadTree::new(bounds(), 2);
        qt.insert(10.0, 10.0, pk(1));
        qt.insert(20.0, 20.0, pk(2));
        // Bounds inclusive on both edges.
        assert_eq!(
            sorted(qt.query_region(Rect::new(10.0, 10.0, 10.0, 10.0))),
            sorted(vec![pk(1), pk(2)])
        );
        // Degenerate zero-extent region is a point probe.
        assert_eq!(
            qt.query_region(Rect::new(10.0, 10.0, 0.0, 0.0)),
            vec![pk(1)]
        );
        // Negative extent matches nothing.
        assert!(
            qt.query_region(Rect::new(15.0, 15.0, -10.0, 10.0))
                .is_empty()
        );
        assert!(
            qt.query_region(Rect::new(15.0, 15.0, 10.0, -10.0))
                .is_empty()
        );
        // NaN extent matches nothing.
        assert!(
            qt.query_region(Rect::new(0.0, 0.0, f64::NAN, 10.0))
                .is_empty()
        );
    }

    #[test]
    fn radius_includes_exact_distance_and_rejects_negative_r() {
        let mut qt = QuadTree::new(bounds(), 2);
        qt.insert(53.0, 50.0, pk(1)); // distance exactly 3 from (50, 50)
        qt.insert(50.0, 47.0, pk(2)); // distance exactly 3
        qt.insert(53.0, 53.0, pk(3)); // distance 3√2 > 3 (bbox candidate)
        assert_eq!(
            sorted(qt.query_radius(50.0, 50.0, 3.0)),
            sorted(vec![pk(1), pk(2)])
        );
        assert_eq!(qt.query_radius(53.0, 50.0, 0.0), vec![pk(1)]);
        assert!(qt.query_radius(50.0, 50.0, -1.0).is_empty());
        assert!(qt.query_radius(50.0, 50.0, f64::NAN).is_empty());
    }

    #[test]
    fn quadrant_midline_points_are_found_by_straddling_queries() {
        let mut qt = QuadTree::new(bounds(), 1);
        // Force splits, then place points exactly on the root midlines.
        for i in 0u32..4 {
            qt.insert(10.0 + f64::from(i), 10.0, pk(100 + u64::from(i)));
        }
        qt.insert(50.0, 50.0, pk(1)); // dead centre
        qt.insert(50.0, 10.0, pk(2)); // on the X midline
        qt.insert(10.0, 50.0, pk(3)); // on the Y midline
        assert_eq!(qt.query_point(50.0, 50.0), vec![pk(1)]);
        // A region ending exactly on the midline still sees midline points.
        assert!(
            qt.query_region(Rect::new(0.0, 0.0, 50.0, 50.0))
                .contains(&pk(1))
        );
        assert!(
            qt.query_region(Rect::new(50.0, 50.0, 50.0, 50.0))
                .contains(&pk(1))
        );
        assert!(
            qt.query_region(Rect::new(40.0, 0.0, 10.0, 20.0))
                .contains(&pk(2))
        );
        assert!(
            qt.query_region(Rect::new(0.0, 40.0, 20.0, 10.0))
                .contains(&pk(3))
        );
    }

    #[test]
    fn bucket_size_zero_is_clamped_and_default_is_eight() {
        assert_eq!(QuadTree::new(bounds(), 0).bucket_size(), 1);
        assert_eq!(QuadTree::DEFAULT_BUCKET_SIZE, 8);
        let qt = QuadTree::new(bounds(), QuadTree::DEFAULT_BUCKET_SIZE);
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
            let mut qt = QuadTree::new(bounds(), bucket);
            let mut oracle = Oracle::default();
            for op in ops {
                match op {
                    Op::Insert { x, y, id } => {
                        prop_assert_eq!(qt.insert(x, y, pk(id)), oracle.insert(x, y, id));
                    }
                    Op::Remove { x, y, id } => {
                        prop_assert_eq!(qt.remove(x, y, &pk(id)), oracle.remove(x, y, id));
                    }
                }
                prop_assert_eq!(qt.len(), oracle.0.len());
            }

            // Canonical structure: bit-identical to a fresh rebuild.
            let mut rebuilt = QuadTree::new(bounds(), bucket);
            for &(x, y, id) in &oracle.0 {
                rebuilt.insert(x, y, pk(id));
            }
            prop_assert_eq!(&qt, &rebuilt);

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
                prop_assert_eq!(sorted(qt.query_region(r)), sorted(oracle.region(r)));
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
                    sorted(qt.query_radius(cx, cy, r)),
                    sorted(oracle.radius(cx, cy, r)),
                    "radius ({}, {}, {})",
                    cx,
                    cy,
                    r
                );
            }
            for (px, py) in [(50.0, 50.0), (12.5, 87.5), (0.0, 0.0), (-5.0, 105.0)] {
                prop_assert_eq!(sorted(qt.query_point(px, py)), sorted(oracle.point(px, py)));
            }
        }

        /// SPX-003: every bucket size answers identically (default 8 vs
        /// non-default) over the same content.
        #[test]
        fn bucket_size_never_changes_query_results(
            points in prop::collection::vec((coord(), coord()), 1..60),
        ) {
            let mut default_qt = QuadTree::new(bounds(), QuadTree::DEFAULT_BUCKET_SIZE);
            let mut tiny_qt = QuadTree::new(bounds(), 1);
            for (id, &(x, y)) in points.iter().enumerate() {
                let id = id as u64;
                default_qt.insert(x, y, pk(id));
                tiny_qt.insert(x, y, pk(id));
            }
            let region = Rect::new(10.0, 10.0, 55.0, 65.0);
            prop_assert_eq!(
                sorted(default_qt.query_region(region)),
                sorted(tiny_qt.query_region(region))
            );
            prop_assert_eq!(
                sorted(default_qt.query_radius(50.0, 50.0, 30.0)),
                sorted(tiny_qt.query_radius(50.0, 50.0, 30.0))
            );
        }
    }
}
