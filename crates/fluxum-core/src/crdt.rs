//! Single-shard CRDT text column (SPEC-023 §7, DMX-060/061).
//!
//! [`CrdtText`] is an RGA (Replicated Growable Array) in its timestamped-
//! tree formulation: every character is a tree node identified by an
//! [`OpId`] (Lamport sequence + actor), parented on the character it was
//! typed after; document order is a depth-first walk with siblings visited
//! newest-id first. Concurrent inserts at the same position therefore land
//! in one deterministic order no matter which arrives first, and deletes
//! are tombstones — so the merge is commutative and idempotent, and every
//! replica of the value converges (DMX-060).
//!
//! # Why a CRDT inside a single-writer shard?
//!
//! Writes already serialize through the shard's single writer, but two
//! editors compose edits against **stale snapshots** of the document. Index-
//! based edits would corrupt each other under interleaving; position
//! *identifiers* keep both edits meaningful when the writer applies them
//! back-to-back. Scope is deliberately single-shard (SPEC-023 §8): there is
//! no multi-primary convergence machinery here.
//!
//! # Storage & wire encodings (DMX-061)
//!
//! A `CrdtText` column is a [`crate::schema::FluxType::CrdtText`] logical
//! type stored as `Bytes` — the tag-discriminated encodings:
//!
//! | first byte | payload | used for |
//! |---|---|---|
//! | `0x00` | MessagePack [`CrdtText`] state | stored rows, `InitialData`, fresh inserts |
//! | `0x01` | MessagePack `Vec<TextOp>` patch | `TxUpdate` of an existing row — the ops added by that commit, not the document |
//!
//! Subscribers hold the state from `InitialData` and apply each `TxUpdate`
//! patch with [`CrdtText::apply_patch_bytes`]; the tag byte tells them
//! which decode to use. Ops are expressed as reducer calls (DMX-061): a
//! reducer receives serialized [`TextOp`]s ([`encode_ops`]/[`decode_ops`]),
//! applies them with [`CrdtText::apply`], and upserts the row — riding the
//! existing single-writer serialization with no new write path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{FluxumError, Result};
use crate::types::Identity;

/// The state encoding's leading tag byte (full document).
pub const TAG_STATE: u8 = 0x00;
/// The patch encoding's leading tag byte (ops delta, DMX-061).
pub const TAG_PATCH: u8 = 0x01;

/// A character's (or delete op's) unique identity: a Lamport sequence plus
/// the editing actor. Ordering is `(seq, actor)` — the total order every
/// deterministic tie-break in this module derives from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct OpId {
    /// Lamport sequence: strictly greater than every op the actor had seen
    /// when generating this one (so a parent's seq < its children's).
    pub seq: u64,
    /// The editing actor (derive one with [`CrdtText::actor_of`]).
    pub actor: u64,
}

/// One character-level edit op (DMX-060).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TextOp {
    /// Insert `ch` after the character `after` (`None` = document start).
    Insert {
        /// The new character's identity.
        id: OpId,
        /// The character this was typed after (position identifier — never
        /// an index, so the op survives concurrent edits).
        after: Option<OpId>,
        /// The character.
        ch: char,
    },
    /// Tombstone the character `target`.
    Delete {
        /// This delete op's own identity (drives idempotence and deltas).
        id: OpId,
        /// The character being deleted.
        target: OpId,
    },
}

impl TextOp {
    /// The op's own identity.
    pub fn id(&self) -> OpId {
        match self {
            Self::Insert { id, .. } | Self::Delete { id, .. } => *id,
        }
    }
}

/// One character node in the RGA tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    parent: Option<OpId>,
    ch: char,
    /// The tombstoning delete op, if any (the smallest such op id wins so
    /// concurrent duplicate deletes converge to identical state).
    deleted_by: Option<OpId>,
}

/// A convergent collaborative text document (SPEC-023 DMX-060) — see the
/// module docs for the model. `PartialEq` is structural convergence: two
/// docs that applied the same op set compare equal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdtText {
    /// Every insert op ever applied, keyed by character id (tombstones
    /// included — RGA never forgets a character's position).
    entries: BTreeMap<OpId, Entry>,
}

impl CrdtText {
    /// An empty document.
    pub fn new() -> Self {
        Self::default()
    }

    /// A stable editing actor for `identity` (the caller's SPEC-009
    /// identity): the first 8 bytes, little-endian. Collisions only weaken
    /// tie-breaking aesthetics, never convergence.
    pub fn actor_of(identity: &Identity) -> u64 {
        let bytes = identity.as_bytes();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(buf)
    }

    /// The next Lamport sequence: greater than every op seen so far.
    fn next_seq(&self) -> u64 {
        let inserts = self.entries.keys().map(|id| id.seq).max().unwrap_or(0);
        let deletes = self
            .entries
            .values()
            .filter_map(|e| e.deleted_by.map(|d| d.seq))
            .max()
            .unwrap_or(0);
        inserts.max(deletes) + 1
    }

    /// The converged text (DMX-060): depth-first walk, siblings newest-id
    /// first, tombstones skipped.
    pub fn text(&self) -> String {
        let mut children: BTreeMap<Option<OpId>, Vec<OpId>> = BTreeMap::new();
        for (id, entry) in &self.entries {
            children.entry(entry.parent).or_default().push(*id);
        }
        for siblings in children.values_mut() {
            siblings.sort_unstable_by(|a, b| b.cmp(a)); // newest first
        }
        let mut out = String::new();
        let mut stack: Vec<OpId> = children.get(&None).cloned().unwrap_or_default();
        stack.reverse(); // visit highest-id sibling first
        while let Some(id) = stack.pop() {
            let entry = &self.entries[&id];
            if entry.deleted_by.is_none() {
                out.push(entry.ch);
            }
            if let Some(kids) = children.get(&Some(id)) {
                for kid in kids.iter().rev() {
                    stack.push(*kid);
                }
            }
        }
        out
    }

    /// The visible character ids, in document order.
    fn visible_ids(&self) -> Vec<OpId> {
        let mut children: BTreeMap<Option<OpId>, Vec<OpId>> = BTreeMap::new();
        for (id, entry) in &self.entries {
            children.entry(entry.parent).or_default().push(*id);
        }
        for siblings in children.values_mut() {
            siblings.sort_unstable_by(|a, b| b.cmp(a));
        }
        let mut out = Vec::new();
        let mut stack: Vec<OpId> = children.get(&None).cloned().unwrap_or_default();
        stack.reverse();
        while let Some(id) = stack.pop() {
            if self.entries[&id].deleted_by.is_none() {
                out.push(id);
            }
            if let Some(kids) = children.get(&Some(id)) {
                for kid in kids.iter().rev() {
                    stack.push(*kid);
                }
            }
        }
        out
    }

    /// Build the ops that insert `text` at visible character position
    /// `pos` (0 = document start), as `actor` — a local edit against THIS
    /// snapshot (DMX-061: send the ops to a reducer; do not mutate a stale
    /// copy and write it back). The ops are also applied to `self` so a
    /// caller composing several local edits sees its own effects.
    pub fn local_insert(&mut self, pos: usize, text: &str, actor: u64) -> Result<Vec<TextOp>> {
        let visible = self.visible_ids();
        if pos > visible.len() {
            return Err(FluxumError::Storage(format!(
                "CrdtText insert position {pos} beyond visible length {}",
                visible.len()
            )));
        }
        let mut after = if pos == 0 { None } else { Some(visible[pos - 1]) };
        let mut ops = Vec::with_capacity(text.chars().count());
        for (seq, ch) in (self.next_seq()..).zip(text.chars()) {
            let id = OpId { seq, actor };
            let op = TextOp::Insert { id, after, ch };
            self.apply(&op)?;
            ops.push(op);
            after = Some(id);
        }
        Ok(ops)
    }

    /// Build the ops that delete `len` visible characters starting at
    /// visible position `pos`, as `actor` (also applied to `self`).
    pub fn local_delete(&mut self, pos: usize, len: usize, actor: u64) -> Result<Vec<TextOp>> {
        let visible = self.visible_ids();
        if pos + len > visible.len() {
            return Err(FluxumError::Storage(format!(
                "CrdtText delete range {pos}..{} beyond visible length {}",
                pos + len,
                visible.len()
            )));
        }
        let mut ops = Vec::with_capacity(len);
        let targets = visible[pos..pos + len].to_vec();
        for (seq, target) in (self.next_seq()..).zip(targets) {
            let op = TextOp::Delete {
                id: OpId { seq, actor },
                target,
            };
            self.apply(&op)?;
            ops.push(op);
        }
        Ok(ops)
    }

    /// Apply one op (DMX-060): commutative within the RGA rules and
    /// idempotent — re-applying a seen op is a no-op, concurrent inserts at
    /// one position order by id, duplicate deletes keep the smallest delete
    /// id. An insert whose parent is unknown is an error: within the
    /// single-shard discipline the authoritative doc has every op, and
    /// patches are complete deltas in causal (Lamport) order.
    pub fn apply(&mut self, op: &TextOp) -> Result<()> {
        match op {
            TextOp::Insert { id, after, ch } => {
                if self.entries.contains_key(id) {
                    return Ok(()); // idempotent
                }
                if let Some(parent) = after
                    && !self.entries.contains_key(parent)
                {
                    return Err(FluxumError::Storage(format!(
                        "CrdtText op {id:?} references unknown parent {parent:?}"
                    )));
                }
                self.entries.insert(
                    *id,
                    Entry {
                        parent: *after,
                        ch: *ch,
                        deleted_by: None,
                    },
                );
            }
            TextOp::Delete { id, target } => {
                let Some(entry) = self.entries.get_mut(target) else {
                    return Err(FluxumError::Storage(format!(
                        "CrdtText delete {id:?} references unknown character {target:?}"
                    )));
                };
                match entry.deleted_by {
                    // Smallest delete-op id wins: any application order
                    // converges to identical state.
                    Some(existing) if existing <= *id => {}
                    _ => entry.deleted_by = Some(*id),
                }
            }
        }
        Ok(())
    }

    /// Apply many ops in Lamport order (parents before children), so a
    /// delta produced by [`CrdtText::ops_since`] applies regardless of the
    /// order it was generated in.
    pub fn apply_ops(&mut self, ops: &[TextOp]) -> Result<()> {
        let mut sorted: Vec<&TextOp> = ops.iter().collect();
        sorted.sort_unstable_by_key(|op| op.id());
        for op in sorted {
            self.apply(op)?;
        }
        Ok(())
    }

    /// Per-actor high-water marks over every op this doc has applied.
    fn version_vector(&self) -> BTreeMap<u64, u64> {
        let mut vv: BTreeMap<u64, u64> = BTreeMap::new();
        let mut see = |id: OpId| {
            let entry = vv.entry(id.actor).or_default();
            *entry = (*entry).max(id.seq);
        };
        for (id, entry) in &self.entries {
            see(*id);
            if let Some(deleted_by) = entry.deleted_by {
                see(deleted_by);
            }
        }
        vv
    }

    /// The compact delta from `older` to `self` (DMX-061): every op `self`
    /// has applied that `older` has not, in Lamport order. This is what a
    /// `TxUpdate` carries instead of the whole document.
    pub fn ops_since(&self, older: &Self) -> Vec<TextOp> {
        let vv = older.version_vector();
        let unseen = |id: &OpId| vv.get(&id.actor).copied().unwrap_or(0) < id.seq;
        let mut ops = Vec::new();
        for (id, entry) in &self.entries {
            if unseen(id) {
                ops.push(TextOp::Insert {
                    id: *id,
                    after: entry.parent,
                    ch: entry.ch,
                });
            }
            if let Some(deleted_by) = entry.deleted_by
                && unseen(&deleted_by)
            {
                ops.push(TextOp::Delete {
                    id: deleted_by,
                    target: *id,
                });
            }
        }
        ops.sort_unstable_by_key(TextOp::id);
        ops
    }

    /// Serialize as the tagged state encoding (`0x00` + MessagePack).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![TAG_STATE];
        // A BTreeMap of plain structs cannot fail MessagePack encoding.
        let body = rmp_serde::to_vec(self).unwrap_or_default();
        out.extend_from_slice(&body);
        out
    }

    /// Decode the tagged state encoding.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        match bytes.split_first() {
            Some((&TAG_STATE, body)) => rmp_serde::from_slice(body).map_err(|e| {
                FluxumError::Storage(format!("CrdtText state decode failed: {e}"))
            }),
            Some((&TAG_PATCH, _)) => Err(FluxumError::Storage(
                "expected CrdtText state bytes, found a patch — apply patches with \
                 apply_patch_bytes (DMX-061)"
                    .into(),
            )),
            _ => Err(FluxumError::Storage(
                "CrdtText bytes missing the encoding tag".into(),
            )),
        }
    }

    /// Apply a tagged patch (`0x01` + ops) to this state — the subscriber
    /// side of the DMX-061 fan-out.
    pub fn apply_patch_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let ops = decode_patch(bytes)?;
        self.apply_ops(&ops)
    }
}

/// Encode ops as the tagged patch encoding (`0x01` + MessagePack) — the
/// compact `TxUpdate` payload and the reducer-call argument shape (DMX-061).
pub fn encode_ops(ops: &[TextOp]) -> Vec<u8> {
    let mut out = vec![TAG_PATCH];
    out.extend_from_slice(&rmp_serde::to_vec(ops).unwrap_or_default());
    out
}

/// Decode the tagged patch encoding.
pub fn decode_patch(bytes: &[u8]) -> Result<Vec<TextOp>> {
    match bytes.split_first() {
        Some((&TAG_PATCH, body)) => rmp_serde::from_slice(body)
            .map_err(|e| FluxumError::Storage(format!("CrdtText patch decode failed: {e}"))),
        Some((&TAG_STATE, _)) => Err(FluxumError::Storage(
            "expected a CrdtText patch, found state bytes (DMX-061)".into(),
        )),
        _ => Err(FluxumError::Storage(
            "CrdtText patch bytes missing the encoding tag".into(),
        )),
    }
}

/// Alias for [`decode_patch`] at the reducer-argument boundary (DMX-061).
pub fn decode_ops(bytes: &[u8]) -> Result<Vec<TextOp>> {
    decode_patch(bytes)
}
