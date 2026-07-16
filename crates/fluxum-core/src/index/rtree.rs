//! [`RTree`] — bounding-box spatial index (SPEC-008 §3, SPX-010, T2.6).
//!
//! # Design
//!
//! A classic dynamic R-tree over axis-aligned bounding boxes ([`Aabb`]),
//! with quadratic-cost node splitting (Guttman) and condense-and-reinsert
//! deletion. Nodes live in a flat arena (`Vec<Node>` + free list, indices
//! instead of `Box`/`Rc` links — no pointer chasing, no unsafe code):
//!
//! - **Insert** (`O(log n)`): descend by least-enlargement (ties: smallest
//!   area, then lowest node index for determinism), split overflowing nodes
//!   with the quadratic seed heuristic, propagate MBR updates to the root.
//! - **Delete** (`O(log n)`): locate the leaf containing the exact entry,
//!   remove it, condense the path — underflowing nodes are dissolved and
//!   their entries reinserted — and shrink the root when it has one child.
//! - **Region query** (`O(log n + k)`): descend every child whose MBR
//!   intersects the query box; leaves report entries whose stored box
//!   intersects it (closed boxes — touching edges intersect, SPX-020).
//! - **Radius query** (`O(log n + k′)`): SPX-021 for extents — a row
//!   matches when the **minimum distance** from its box to the centre is
//!   `≤ r`; pruning uses the same min-distance on node MBRs, compared as
//!   squared distances (no square root).
//!
//! # Equality semantics (STG-007 rule 2)
//!
//! Unlike the [`super::QuadTree`], an R-tree's *shape* depends on insertion
//! order (split heuristics are history-sensitive), so `PartialEq` compares
//! the **logical content** — the sorted multiset of `(box bits, pk)`
//! entries — not the arena layout. `verify_index_integrity`'s
//! rebuild-comparison therefore still proves exactly what STG-007 needs:
//! after any commit or rollback the index holds precisely the committed
//! rows' boxes. Maintenance rides the commit merge on the private pre-swap
//! copy (SPX-030), identical to the B-tree and QuadTree indexes.
//!
//! # Box semantics
//!
//! - Boxes are **closed**: `[min_x, max_x] × [min_y, max_y]`. Degenerate
//!   boxes (`min == max`, points or segments) are fully supported.
//! - `min <= max` per axis is a *store-level* insert constraint (SPX-010,
//!   enforced eagerly by `Tx::insert`); the raw structure stores whatever
//!   it is given and matches by IEEE comparisons.
//! - Entry identity for insert/remove is the coordinate **bit pattern**
//!   (totalOrder) plus the PK, exactly like the QuadTree.

use std::cmp::Ordering;

use crate::store::row::PkBytes;

/// A closed axis-aligned bounding box `[min_x, max_x] × [min_y, max_y]`
/// (SPX-010). Degenerate boxes (`min == max`) are points/segments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Aabb {
    /// Lower X bound.
    pub min_x: f64,
    /// Lower Y bound.
    pub min_y: f64,
    /// Upper X bound.
    pub max_x: f64,
    /// Upper Y bound.
    pub max_y: f64,
}

impl Aabb {
    /// A box from its corner bounds.
    pub const fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    /// Whether the two closed boxes share at least one point (touching
    /// edges intersect). NaN anywhere is `false`.
    pub fn intersects(&self, other: &Aabb) -> bool {
        self.min_x <= other.max_x
            && other.min_x <= self.max_x
            && self.min_y <= other.max_y
            && other.min_y <= self.max_y
    }

    /// The smallest box covering both.
    fn union(&self, other: &Aabb) -> Aabb {
        Aabb {
            min_x: self.min_x.min(other.min_x),
            min_y: self.min_y.min(other.min_y),
            max_x: self.max_x.max(other.max_x),
            max_y: self.max_y.max(other.max_y),
        }
    }

    /// Area (0 for degenerate boxes).
    fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    /// How much this box's area grows to also cover `other`.
    fn enlargement(&self, other: &Aabb) -> f64 {
        self.union(other).area() - self.area()
    }

    /// Squared minimum Euclidean distance from `(x, y)` to this closed box
    /// (0 when the point lies inside). The SPX-021 R-tree radius metric —
    /// public so oracles and the T4.1 compiler share the exact semantics.
    pub fn min_dist2(&self, x: f64, y: f64) -> f64 {
        let dx = if x < self.min_x {
            self.min_x - x
        } else if x > self.max_x {
            x - self.max_x
        } else {
            0.0
        };
        let dy = if y < self.min_y {
            self.min_y - y
        } else if y > self.max_y {
            y - self.max_y
        } else {
            0.0
        };
        dx * dx + dy * dy
    }

    /// Total order over the box's bit patterns — entry identity and the
    /// canonical content order for [`RTree`] equality.
    fn total_cmp(&self, other: &Aabb) -> Ordering {
        self.min_x
            .total_cmp(&other.min_x)
            .then_with(|| self.min_y.total_cmp(&other.min_y))
            .then_with(|| self.max_x.total_cmp(&other.max_x))
            .then_with(|| self.max_y.total_cmp(&other.max_y))
    }
}

/// One indexed extent: the row's box plus its PK.
#[derive(Debug, Clone, PartialEq)]
struct Entry {
    aabb: Aabb,
    pk: PkBytes,
}

impl Entry {
    fn cmp_content(&self, other: &Entry) -> Ordering {
        self.aabb
            .total_cmp(&other.aabb)
            .then_with(|| self.pk.cmp(&other.pk))
    }
}

/// Arena slot index.
type NodeId = usize;

/// One arena node: MBR-tagged children (internal) or entries (leaf).
#[derive(Debug, Clone)]
enum Node {
    Leaf(Vec<Entry>),
    Internal(Vec<(Aabb, NodeId)>),
    /// Recycled slot (free list).
    Free(Option<NodeId>),
}

/// The R-tree bounding-box index (SPX-010): flat arena storage, quadratic
/// split, condense-and-reinsert deletion. See the module docs.
#[derive(Debug, Clone)]
pub struct RTree {
    nodes: Vec<Node>,
    root: NodeId,
    free: Option<NodeId>,
    /// Max entries per node before it splits (`M`), min occupancy `M * 2/5`
    /// clamped to `[1, M/2]`.
    max_entries: usize,
    len: usize,
}

impl RTree {
    /// The default node capacity.
    pub const DEFAULT_MAX_ENTRIES: usize = 8;

    /// An empty R-tree with node capacity `max_entries` (clamped to ≥ 2).
    pub fn new(max_entries: usize) -> Self {
        Self {
            nodes: vec![Node::Leaf(Vec::new())],
            root: 0,
            free: None,
            max_entries: max_entries.max(2),
            len: 0,
        }
    }

    /// The configured node capacity.
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Minimum node occupancy after deletion.
    fn min_entries(&self) -> usize {
        (self.max_entries * 2 / 5).clamp(1, self.max_entries / 2)
    }

    /// Number of indexed entries.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no entry is indexed.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // --- arena plumbing ---

    fn alloc(&mut self, node: Node) -> NodeId {
        if let Some(id) = self.free {
            let next = match self.nodes.get(id) {
                Some(Node::Free(next)) => *next,
                _ => None, // free-list invariant repaired defensively
            };
            self.free = next;
            self.nodes[id] = node;
            id
        } else {
            self.nodes.push(node);
            self.nodes.len() - 1
        }
    }

    fn release(&mut self, id: NodeId) {
        self.nodes[id] = Node::Free(self.free);
        self.free = Some(id);
    }

    /// The MBR of a node (`None` for an empty node).
    fn mbr(&self, id: NodeId) -> Option<Aabb> {
        match &self.nodes[id] {
            Node::Leaf(entries) => entries.iter().map(|e| e.aabb).reduce(|a, b| a.union(&b)),
            Node::Internal(children) => children
                .iter()
                .map(|(mbr, _)| *mbr)
                .reduce(|a, b| a.union(&b)),
            Node::Free(_) => None,
        }
    }

    // --- insert ---

    /// Index `aabb → pk`. Returns `false` (and changes nothing) when this
    /// exact entry — same coordinate bit patterns, same PK — is already
    /// present. O(log n).
    pub fn insert(&mut self, aabb: Aabb, pk: PkBytes) -> bool {
        if self.contains(&aabb, &pk) {
            return false;
        }
        self.insert_entry(Entry { aabb, pk });
        self.len += 1;
        true
    }

    /// Insert without the duplicate check (reinsertion path).
    fn insert_entry(&mut self, entry: Entry) {
        // Descend to the best leaf by least enlargement.
        let mut path: Vec<(NodeId, usize)> = Vec::new(); // (node, child slot)
        let mut node = self.root;
        loop {
            match &self.nodes[node] {
                Node::Leaf(_) => break,
                Node::Internal(children) => {
                    let slot = Self::choose_subtree(children, &entry.aabb);
                    path.push((node, slot));
                    node = children[slot].1;
                }
                Node::Free(_) => unreachable!("free node reached from the root"),
            }
        }
        let Node::Leaf(entries) = &mut self.nodes[node] else {
            return; // unreachable: the loop exits only on Leaf
        };
        entries.push(entry);
        let overflow = entries.len() > self.max_entries;

        // Split upward while nodes overflow; refresh MBRs on the way.
        let mut split = if overflow {
            self.split_node(node)
        } else {
            None
        };
        for (parent, slot) in path.into_iter().rev() {
            let child = {
                let Node::Internal(children) = &self.nodes[parent] else {
                    return; // unreachable: path holds internals only
                };
                children[slot].1
            };
            let child_mbr = self.mbr(child).unwrap_or(Aabb::new(0.0, 0.0, 0.0, 0.0));
            let Node::Internal(children) = &mut self.nodes[parent] else {
                return; // unreachable
            };
            children[slot].0 = child_mbr;
            if let Some(new_child) = split {
                let new_mbr = self.mbr(new_child).unwrap_or(Aabb::new(0.0, 0.0, 0.0, 0.0));
                let Node::Internal(children) = &mut self.nodes[parent] else {
                    return; // unreachable
                };
                children.push((new_mbr, new_child));
                split = if children.len() > self.max_entries {
                    self.split_node(parent)
                } else {
                    None
                };
            }
        }
        if let Some(sibling) = split {
            self.grow_root(sibling);
        }
    }

    /// Least-enlargement child choice (ties: smaller area, then lower slot).
    fn choose_subtree(children: &[(Aabb, NodeId)], aabb: &Aabb) -> usize {
        let mut best = 0usize;
        let mut best_enlargement = f64::INFINITY;
        let mut best_area = f64::INFINITY;
        for (slot, (mbr, _)) in children.iter().enumerate() {
            let enlargement = mbr.enlargement(aabb);
            let area = mbr.area();
            if enlargement < best_enlargement
                || (enlargement == best_enlargement && area < best_area)
            {
                best = slot;
                best_enlargement = enlargement;
                best_area = area;
            }
        }
        best
    }

    /// Quadratic split of an overflowing node; returns the new sibling.
    fn split_node(&mut self, node: NodeId) -> Option<NodeId> {
        match std::mem::replace(&mut self.nodes[node], Node::Free(None)) {
            Node::Leaf(entries) => {
                let boxes: Vec<Aabb> = entries.iter().map(|e| e.aabb).collect();
                let (left, right) = self.quadratic_partition(&boxes);
                let take = |idx: &[usize]| -> Vec<Entry> {
                    idx.iter().map(|&i| entries[i].clone()).collect()
                };
                self.nodes[node] = Node::Leaf(take(&left));
                Some(self.alloc(Node::Leaf(take(&right))))
            }
            Node::Internal(children) => {
                let boxes: Vec<Aabb> = children.iter().map(|(mbr, _)| *mbr).collect();
                let (left, right) = self.quadratic_partition(&boxes);
                let take = |idx: &[usize]| -> Vec<(Aabb, NodeId)> {
                    idx.iter().map(|&i| children[i]).collect()
                };
                self.nodes[node] = Node::Internal(take(&left));
                Some(self.alloc(Node::Internal(take(&right))))
            }
            free @ Node::Free(_) => {
                self.nodes[node] = free;
                None
            }
        }
    }

    /// Guttman quadratic partition over item boxes: pick the two seeds
    /// wasting the most area apart, then assign the rest by least
    /// enlargement (with the min-occupancy backstop). Deterministic for a
    /// given input order.
    fn quadratic_partition(&self, boxes: &[Aabb]) -> (Vec<usize>, Vec<usize>) {
        let min = self.min_entries();
        // Seeds: the pair with the largest dead area when joined.
        let (mut seed_a, mut seed_b, mut worst) = (0usize, 1usize.min(boxes.len() - 1), f64::MIN);
        for i in 0..boxes.len() {
            for j in (i + 1)..boxes.len() {
                let dead = boxes[i].union(&boxes[j]).area() - boxes[i].area() - boxes[j].area();
                if dead > worst {
                    (seed_a, seed_b, worst) = (i, j, dead);
                }
            }
        }
        let (mut left, mut right) = (vec![seed_a], vec![seed_b]);
        let (mut left_mbr, mut right_mbr) = (boxes[seed_a], boxes[seed_b]);
        for (i, b) in boxes.iter().enumerate() {
            if i == seed_a || i == seed_b {
                continue;
            }
            let remaining = boxes.len() - left.len() - right.len();
            // Occupancy backstop: a side short of `min` takes everything left.
            if left.len() + remaining <= min.max(left.len()) && left.len() < min {
                left.push(i);
                left_mbr = left_mbr.union(b);
                continue;
            }
            if right.len() + remaining <= min.max(right.len()) && right.len() < min {
                right.push(i);
                right_mbr = right_mbr.union(b);
                continue;
            }
            let grow_left = left_mbr.enlargement(b);
            let grow_right = right_mbr.enlargement(b);
            let go_left =
                grow_left < grow_right || (grow_left == grow_right && left.len() <= right.len());
            if go_left {
                left.push(i);
                left_mbr = left_mbr.union(b);
            } else {
                right.push(i);
                right_mbr = right_mbr.union(b);
            }
        }
        (left, right)
    }

    /// Replace the root with a new internal node over `{old root, sibling}`.
    fn grow_root(&mut self, sibling: NodeId) {
        let old_root = self.root;
        let a = self.mbr(old_root).unwrap_or(Aabb::new(0.0, 0.0, 0.0, 0.0));
        let b = self.mbr(sibling).unwrap_or(Aabb::new(0.0, 0.0, 0.0, 0.0));
        self.root = self.alloc(Node::Internal(vec![(a, old_root), (b, sibling)]));
    }

    // --- remove ---

    /// Remove the entry `aabb → pk` (coordinates matched by bit pattern).
    /// Returns whether an entry was removed. O(log n).
    pub fn remove(&mut self, aabb: &Aabb, pk: &PkBytes) -> bool {
        let Some(path) = self.find_leaf(self.root, aabb, pk) else {
            return false;
        };
        let leaf = *path.last().unwrap_or(&self.root);
        if let Node::Leaf(entries) = &mut self.nodes[leaf] {
            entries.retain(|e| !(e.aabb.total_cmp(aabb) == Ordering::Equal && e.pk == *pk));
        }
        self.len -= 1;
        self.condense(&path);
        true
    }

    /// Whether the exact entry exists.
    fn contains(&self, aabb: &Aabb, pk: &PkBytes) -> bool {
        self.find_leaf(self.root, aabb, pk).is_some()
    }

    /// Root-to-leaf path of the node holding the exact entry, if any.
    fn find_leaf(&self, node: NodeId, aabb: &Aabb, pk: &PkBytes) -> Option<Vec<NodeId>> {
        match &self.nodes[node] {
            Node::Leaf(entries) => entries
                .iter()
                .any(|e| e.aabb.total_cmp(aabb) == Ordering::Equal && e.pk == *pk)
                .then(|| vec![node]),
            Node::Internal(children) => {
                for (mbr, child) in children {
                    // total-order identity implies geometric containment in
                    // the child MBR only for well-formed boxes; use the
                    // closed-intersection test as the descent filter.
                    if mbr.intersects(aabb)
                        && let Some(mut path) = self.find_leaf(*child, aabb, pk)
                    {
                        let mut full = vec![node];
                        full.append(&mut path);
                        return Some(full);
                    }
                }
                None
            }
            Node::Free(_) => None,
        }
    }

    /// Condense after a removal along `path` (root first): dissolve
    /// underflowing non-root nodes, reinsert their entries, refresh MBRs,
    /// and shrink a single-child internal root.
    fn condense(&mut self, path: &[NodeId]) {
        let min = self.min_entries();
        let mut orphans: Vec<Entry> = Vec::new();
        // Walk leaf-to-root; the root itself never dissolves.
        for window in (1..path.len()).rev() {
            let node = path[window];
            let parent = path[window - 1];
            let occupancy = match &self.nodes[node] {
                Node::Leaf(entries) => entries.len(),
                Node::Internal(children) => children.len(),
                Node::Free(_) => 0,
            };
            if occupancy < min {
                // Dissolve: orphan all entries below `node`.
                self.collect_entries(node, &mut orphans);
                self.release_subtree(node);
                if let Node::Internal(children) = &mut self.nodes[parent] {
                    children.retain(|(_, c)| *c != node);
                }
            } else {
                // Refresh this child's MBR in the parent.
                let mbr = self.mbr(node);
                if let Some(mbr) = mbr
                    && let Node::Internal(children) = &mut self.nodes[parent]
                    && let Some(slot) = children.iter_mut().find(|(_, c)| *c == node)
                {
                    slot.0 = mbr;
                }
            }
        }
        // Shrink the root: single-child internal root is replaced by the
        // child; an empty internal root becomes an empty leaf.
        loop {
            match &self.nodes[self.root] {
                Node::Internal(children) if children.len() == 1 => {
                    let child = children[0].1;
                    self.release(self.root);
                    self.root = child;
                }
                Node::Internal(children) if children.is_empty() => {
                    self.nodes[self.root] = Node::Leaf(Vec::new());
                    break;
                }
                _ => break,
            }
        }
        for entry in orphans {
            self.insert_entry(entry);
        }
    }

    /// Append every entry of `node`'s subtree to `out`.
    fn collect_entries(&self, node: NodeId, out: &mut Vec<Entry>) {
        match &self.nodes[node] {
            Node::Leaf(entries) => out.extend(entries.iter().cloned()),
            Node::Internal(children) => {
                for &(_, child) in children {
                    self.collect_entries(child, out);
                }
            }
            Node::Free(_) => {}
        }
    }

    /// Release every arena slot of `node`'s subtree.
    fn release_subtree(&mut self, node: NodeId) {
        if let Node::Internal(children) = &self.nodes[node] {
            let children: Vec<NodeId> = children.iter().map(|&(_, c)| c).collect();
            for child in children {
                self.release_subtree(child);
            }
        }
        self.release(node);
    }

    // --- queries ---

    /// PKs of every stored box intersecting `query` (closed boxes — shared
    /// edges count, SPX-020). O(log n + k).
    pub fn query_region(&self, query: &Aabb) -> Vec<PkBytes> {
        let mut out = Vec::new();
        let mut stack = vec![self.root];
        while let Some(node) = stack.pop() {
            match &self.nodes[node] {
                Node::Leaf(entries) => out.extend(
                    entries
                        .iter()
                        .filter(|e| e.aabb.intersects(query))
                        .map(|e| e.pk.clone()),
                ),
                Node::Internal(children) => {
                    for (mbr, child) in children {
                        if mbr.intersects(query) {
                            stack.push(*child);
                        }
                    }
                }
                Node::Free(_) => {}
            }
        }
        out
    }

    /// PKs of every stored box whose minimum distance to `(x, y)` is `≤ r`
    /// (SPX-021 for extents; distance exactly `r` included; squared
    /// comparison, no square root). A negative or NaN radius matches
    /// nothing. O(log n + k′).
    pub fn query_radius(&self, x: f64, y: f64, r: f64) -> Vec<PkBytes> {
        if r.is_nan() || r < 0.0 {
            return Vec::new();
        }
        let rr = r * r;
        let mut out = Vec::new();
        let mut stack = vec![self.root];
        while let Some(node) = stack.pop() {
            match &self.nodes[node] {
                Node::Leaf(entries) => out.extend(
                    entries
                        .iter()
                        .filter(|e| e.aabb.min_dist2(x, y) <= rr)
                        .map(|e| e.pk.clone()),
                ),
                Node::Internal(children) => {
                    for (mbr, child) in children {
                        if mbr.min_dist2(x, y) <= rr {
                            stack.push(*child);
                        }
                    }
                }
                Node::Free(_) => {}
            }
        }
        out
    }

    /// PKs of every stored box containing the point `(x, y)` — the
    /// degenerate-region query.
    pub fn query_point(&self, x: f64, y: f64) -> Vec<PkBytes> {
        self.query_region(&Aabb::new(x, y, x, y))
    }

    /// All entries in canonical content order — the logical content the
    /// `PartialEq` impl compares.
    fn sorted_entries(&self) -> Vec<Entry> {
        let mut all = Vec::with_capacity(self.len);
        self.collect_entries(self.root, &mut all);
        all.sort_by(Entry::cmp_content);
        all
    }
}

/// Logical-content equality (see the module docs): two R-trees are equal
/// when they index the same `(box, pk)` multiset with the same node
/// capacity, regardless of arena layout or tree shape.
impl PartialEq for RTree {
    fn eq(&self, other: &Self) -> bool {
        self.max_entries == other.max_entries
            && self.len == other.len
            && self.sorted_entries() == other.sorted_entries()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use proptest::prelude::*;

    use super::*;
    use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
    use crate::store::row::{RowValue, encode_pk_values};

    fn pk(n: u64) -> PkBytes {
        static COLS: &[ColumnSchema] = &[ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        }];
        static T: TableSchema = TableSchema {
            name: "B",
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

    fn sorted(mut pks: Vec<PkBytes>) -> Vec<PkBytes> {
        pks.sort();
        pks
    }

    #[test]
    fn closed_box_geometry() {
        let a = Aabb::new(0.0, 0.0, 10.0, 10.0);
        // Touching edges and corners intersect (closed boxes).
        assert!(a.intersects(&Aabb::new(10.0, 0.0, 20.0, 10.0)));
        assert!(a.intersects(&Aabb::new(10.0, 10.0, 20.0, 20.0)));
        assert!(!a.intersects(&Aabb::new(10.1, 0.0, 20.0, 10.0)));
        // Degenerate point box.
        assert!(a.intersects(&Aabb::new(5.0, 5.0, 5.0, 5.0)));
        // min_dist2: inside = 0, edge = 0, outside = squared clamp distance.
        assert_eq!(a.min_dist2(5.0, 5.0), 0.0);
        assert_eq!(a.min_dist2(10.0, 10.0), 0.0);
        assert_eq!(a.min_dist2(13.0, 14.0), 9.0 + 16.0);
        assert_eq!(a.min_dist2(-3.0, 5.0), 9.0);
    }

    #[test]
    fn insert_query_remove_roundtrip() {
        let mut rt = RTree::new(4);
        assert!(rt.is_empty());
        assert!(rt.insert(Aabb::new(0.0, 0.0, 10.0, 10.0), pk(1)));
        assert!(rt.insert(Aabb::new(5.0, 5.0, 15.0, 15.0), pk(2)));
        assert!(rt.insert(Aabb::new(20.0, 20.0, 30.0, 30.0), pk(3)));
        assert!(!rt.insert(Aabb::new(0.0, 0.0, 10.0, 10.0), pk(1))); // dup
        assert_eq!(rt.len(), 3);

        assert_eq!(
            sorted(rt.query_region(&Aabb::new(4.0, 4.0, 6.0, 6.0))),
            sorted(vec![pk(1), pk(2)])
        );
        assert_eq!(rt.query_point(25.0, 25.0), vec![pk(3)]);
        // Radius from (15, 15): box 2 contains the centre (dist 0); boxes 1
        // and 3 both sit at min distance 5√2 ≈ 7.071 (corners (10,10) and
        // (20,20)), so r = 7.0 excludes them and r = 7.1 includes them.
        assert_eq!(rt.query_radius(15.0, 15.0, 7.0), vec![pk(2)]);
        assert_eq!(
            sorted(rt.query_radius(15.0, 15.0, 7.1)),
            sorted(vec![pk(1), pk(2), pk(3)])
        );

        assert!(rt.remove(&Aabb::new(5.0, 5.0, 15.0, 15.0), &pk(2)));
        assert!(!rt.remove(&Aabb::new(5.0, 5.0, 15.0, 15.0), &pk(2)));
        assert!(!rt.remove(&Aabb::new(0.0, 0.0, 10.0, 10.0), &pk(9)));
        assert_eq!(rt.len(), 2);
        assert_eq!(rt.query_region(&Aabb::new(12.0, 12.0, 14.0, 14.0)), vec![]);
    }

    #[test]
    fn radius_boundary_is_inclusive() {
        let mut rt = RTree::new(4);
        rt.insert(Aabb::new(3.0, 0.0, 5.0, 1.0), pk(1)); // mindist to origin = 3
        rt.insert(Aabb::new(0.0, 4.0, 1.0, 6.0), pk(2)); // mindist = 4
        assert_eq!(rt.query_radius(0.0, 0.0, 3.0), vec![pk(1)]);
        assert_eq!(
            sorted(rt.query_radius(0.0, 0.0, 4.0)),
            sorted(vec![pk(1), pk(2)])
        );
        assert!(rt.query_radius(0.0, 0.0, 2.999).is_empty());
        assert!(rt.query_radius(0.0, 0.0, -1.0).is_empty());
        assert!(rt.query_radius(0.0, 0.0, f64::NAN).is_empty());
        // Centre inside a box: distance 0.
        assert_eq!(rt.query_radius(4.0, 0.5, 0.0), vec![pk(1)]);
    }

    #[test]
    fn coincident_boxes_and_deep_splits_stay_consistent() {
        // Capacity 2 forces aggressive splitting; 40 coincident boxes plus
        // 40 spread boxes exercise split + condense heavily.
        let mut rt = RTree::new(2);
        for i in 0..40 {
            assert!(rt.insert(Aabb::new(5.0, 5.0, 6.0, 6.0), pk(i)));
        }
        for i in 40..80 {
            let f = f64::from(u32::try_from(i).unwrap_or(0));
            assert!(rt.insert(Aabb::new(f, f, f + 0.5, f + 0.5), pk(i)));
        }
        assert_eq!(rt.len(), 80);
        // Only the 40 coincident boxes [5,6]×[5,6] contain (5.5, 5.5); the
        // spread boxes start at (40, 40).
        assert_eq!(rt.query_region(&Aabb::new(5.5, 5.5, 5.5, 5.5)).len(), 40);
        for i in 0..80 {
            let aabb = if i < 40 {
                Aabb::new(5.0, 5.0, 6.0, 6.0)
            } else {
                let f = f64::from(u32::try_from(i).unwrap_or(0));
                Aabb::new(f, f, f + 0.5, f + 0.5)
            };
            assert!(rt.remove(&aabb, &pk(i)), "remove {i}");
        }
        assert!(rt.is_empty());
        assert_eq!(rt, RTree::new(2));
    }

    #[test]
    fn condense_dissolves_underflowing_nodes_and_reinserts_orphans() {
        // Capacity 5 → min occupancy 2: removals leave 1-entry leaves that
        // must dissolve, orphaning (and reinserting) their survivors, and
        // cascading dissolution through internal levels on the way up.
        let mut rt = RTree::new(5);
        for i in 0..48u64 {
            let f = f64::from(u32::try_from(i).unwrap_or(0));
            let (x, y) = ((f % 8.0) * 10.0, (f / 8.0).floor() * 10.0);
            assert!(rt.insert(Aabb::new(x, y, x + 4.0, y + 4.0), pk(i)));
        }
        assert_eq!(rt.len(), 48);

        // Remove the first half; every query over the survivors must stay
        // exact while condense churns the tree.
        for i in 0..24u64 {
            let f = f64::from(u32::try_from(i).unwrap_or(0));
            let (x, y) = ((f % 8.0) * 10.0, (f / 8.0).floor() * 10.0);
            assert!(rt.remove(&Aabb::new(x, y, x + 4.0, y + 4.0), &pk(i)), "{i}");
        }
        assert_eq!(rt.len(), 24);
        assert_eq!(
            sorted(rt.query_region(&Aabb::new(-1.0, -1.0, 101.0, 101.0))),
            sorted((24u64..48).map(pk).collect::<Vec<_>>())
        );

        // Removing the rest drains the tree back to the canonical empty
        // shape (single-child root shrink + empty root).
        for i in 24..48u64 {
            let f = f64::from(u32::try_from(i).unwrap_or(0));
            let (x, y) = ((f % 8.0) * 10.0, (f / 8.0).floor() * 10.0);
            assert!(rt.remove(&Aabb::new(x, y, x + 4.0, y + 4.0), &pk(i)), "{i}");
        }
        assert!(rt.is_empty());
        assert_eq!(rt, RTree::new(5));
    }

    /// Brute-force oracle over a flat list of boxes.
    #[derive(Default)]
    struct Oracle(Vec<(Aabb, u64)>);

    impl Oracle {
        fn insert(&mut self, aabb: Aabb, id: u64) -> bool {
            if self
                .0
                .iter()
                .any(|(b, i)| b.total_cmp(&aabb) == std::cmp::Ordering::Equal && *i == id)
            {
                return false;
            }
            self.0.push((aabb, id));
            true
        }

        fn remove(&mut self, aabb: &Aabb, id: u64) -> bool {
            let before = self.0.len();
            self.0
                .retain(|(b, i)| !(b.total_cmp(aabb) == std::cmp::Ordering::Equal && *i == id));
            self.0.len() != before
        }

        fn region(&self, q: &Aabb) -> Vec<PkBytes> {
            self.0
                .iter()
                .filter(|(b, _)| b.intersects(q))
                .map(|&(_, id)| pk(id))
                .collect()
        }

        fn radius(&self, x: f64, y: f64, r: f64) -> Vec<PkBytes> {
            self.0
                .iter()
                .filter(|(b, _)| b.min_dist2(x, y) <= r * r)
                .map(|&(_, id)| pk(id))
                .collect()
        }
    }

    /// Grid-aligned boxes (including degenerate ones and shared edges).
    fn small_box() -> impl Strategy<Value = Aabb> {
        ((0u8..=10), (0u8..=10), (0u8..=4), (0u8..=4)).prop_map(|(x, y, w, h)| {
            let (x, y, w, h) = (f64::from(x), f64::from(y), f64::from(w), f64::from(h));
            Aabb::new(x, y, x + w, y + h)
        })
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert { aabb: Aabb, id: u64 },
        Remove { aabb: Aabb, id: u64 },
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (small_box(), 0u64..24).prop_map(|(aabb, id)| Op::Insert { aabb, id }),
            2 => (small_box(), 0u64..24).prop_map(|(aabb, id)| Op::Remove { aabb, id }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        /// R-tree answers ≡ brute-force oracle for region and radius across
        /// random insert/remove workloads and node capacities, and logical
        /// content always equals a fresh rebuild (the STG-007 property).
        #[test]
        fn rtree_equals_the_brute_force_oracle(
            ops in prop::collection::vec(op_strategy(), 1..120),
            capacity in prop_oneof![Just(2usize), Just(3), Just(8)],
        ) {
            let mut rt = RTree::new(capacity);
            let mut oracle = Oracle::default();
            for op in ops {
                match op {
                    Op::Insert { aabb, id } => {
                        prop_assert_eq!(rt.insert(aabb, pk(id)), oracle.insert(aabb, id));
                    }
                    Op::Remove { aabb, id } => {
                        prop_assert_eq!(rt.remove(&aabb, &pk(id)), oracle.remove(&aabb, id));
                    }
                }
                prop_assert_eq!(rt.len(), oracle.0.len());
            }

            // Logical content == fresh rebuild (content-equality PartialEq).
            let mut rebuilt = RTree::new(capacity);
            for &(aabb, id) in &oracle.0 {
                rebuilt.insert(aabb, pk(id));
            }
            prop_assert_eq!(&rt, &rebuilt);

            let queries = [
                Aabb::new(0.0, 0.0, 14.0, 14.0),
                Aabb::new(3.0, 3.0, 7.0, 7.0),
                Aabb::new(5.0, 5.0, 5.0, 5.0),
                Aabb::new(10.0, 0.0, 12.0, 4.0),
            ];
            for q in queries {
                prop_assert_eq!(sorted(rt.query_region(&q)), sorted(oracle.region(&q)));
            }
            for (x, y, r) in [(5.0, 5.0, 3.0), (0.0, 0.0, 8.0), (12.0, 12.0, 0.0)] {
                prop_assert_eq!(
                    sorted(rt.query_radius(x, y, r)),
                    sorted(oracle.radius(x, y, r)),
                    "radius ({}, {}, {})", x, y, r
                );
            }
        }
    }
}
