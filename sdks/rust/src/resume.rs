//! Client-side resume bookkeeping (SPEC-021 CS-020/CS-022).
//!
//! The SDK must retain the highest `tx_offset` it has *applied* per
//! subscription and, on reconnect, resume from it rather than re-download
//! the snapshot. This module is that bookkeeping as a pure, transport-free
//! unit: it decides what to send on reconnect and how to fold each server
//! message back in.
//!
//! The socket, the row cache, and codegen land with DAG task **T6.2**
//! (after the gate-G5 wire freeze); this type is the piece the wire freeze
//! actually constrains, so it ships now and T6.2 wires it to a connection.

use std::collections::HashMap;

use crate::protocol::{ClientMessage, InitialData, Resume, TxUpdate};

/// What a client should send for a subscription when a connection comes
/// back (SPEC-021 CS-021).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconnect {
    /// Nothing has been applied yet (or the query was never established):
    /// there is no offset to resume from — subscribe normally.
    Subscribe,
    /// Resume from the highest applied offset; the server replays only the
    /// deltas after it, or resets the cache if it was compacted away.
    Resume(Resume),
}

/// Per-subscription resume state: the highest applied offset per
/// `query_id` (CS-020).
///
/// Feed it every `InitialData` and `TxUpdate` the server sends
/// ([`ResumeTracker::apply_initial`] / [`ResumeTracker::apply_update`]) and
/// ask it what to send on reconnect ([`ResumeTracker::on_reconnect`]).
#[derive(Debug, Default)]
pub struct ResumeTracker {
    applied: HashMap<u32, u64>,
}

impl ResumeTracker {
    /// A tracker with nothing applied yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest offset applied for `query_id`, if any.
    pub fn applied_offset(&self, query_id: u32) -> Option<u64> {
        self.applied.get(&query_id).copied()
    }

    /// Fold in an `InitialData` snapshot, returning whether the caller must
    /// **clear its cached rows** for these queries before applying it
    /// (CS-022: the server compacted past our offset, so the snapshot
    /// replaces rather than merges).
    ///
    /// The snapshot's `tx_offset` becomes the applied offset for every
    /// query it carries.
    #[must_use]
    pub fn apply_initial(&mut self, initial: &InitialData) -> bool {
        for table in &initial.tables {
            self.record(table.query_id, initial.tx_offset);
        }
        initial.cache_reset
    }

    /// Fold in a `TxUpdate`, advancing the applied offset of every query it
    /// touches. Offsets never move backwards: a replayed or duplicated
    /// update cannot rewind the cursor.
    pub fn apply_update(&mut self, update: &TxUpdate) {
        for table in &update.tables {
            self.record(table.query_id, update.tx_offset);
        }
    }

    /// Forget a subscription (the client unsubscribed).
    pub fn forget(&mut self, query_id: u32) {
        self.applied.remove(&query_id);
    }

    /// What to send for `query_id` once the connection is back (CS-021):
    /// [`Reconnect::Resume`] from the highest applied offset, or
    /// [`Reconnect::Subscribe`] when nothing has been applied yet.
    pub fn on_reconnect(&self, id: u32, query_id: u32) -> Reconnect {
        match self.applied_offset(query_id) {
            Some(from_offset) => Reconnect::Resume(Resume {
                id,
                query_id,
                from_offset,
            }),
            None => Reconnect::Subscribe,
        }
    }

    /// [`ResumeTracker::on_reconnect`] as a ready-to-send message, for the
    /// resume case.
    pub fn resume_message(&self, id: u32, query_id: u32) -> Option<ClientMessage> {
        match self.on_reconnect(id, query_id) {
            Reconnect::Resume(resume) => Some(ClientMessage::Resume(resume)),
            Reconnect::Subscribe => None,
        }
    }

    fn record(&mut self, query_id: u32, offset: u64) {
        let slot = self.applied.entry(query_id).or_insert(offset);
        *slot = (*slot).max(offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{RowList, TableUpdate};

    fn table(query_id: u32) -> TableUpdate {
        TableUpdate {
            table_id: 1,
            table_name: "Sensor".into(),
            query_id,
            inserts: RowList::empty(),
            deletes: RowList::empty(),
        }
    }

    fn initial(query_id: u32, tx_offset: u64, cache_reset: bool) -> InitialData {
        InitialData {
            id: 1,
            schema_version: 0,
            tx_offset,
            cache_reset,
            tables: vec![table(query_id)],
        }
    }

    fn update(query_id: u32, tx_offset: u64) -> TxUpdate {
        TxUpdate {
            tx_id: tx_offset,
            timestamp: 0,
            reducer_name: String::new(),
            caller: [0u8; 32],
            duration_us: 0,
            shard_id: 0,
            tx_offset,
            tables: vec![table(query_id)],
        }
    }

    #[test]
    fn nothing_applied_means_subscribe_not_resume() {
        let tracker = ResumeTracker::new();
        assert_eq!(tracker.on_reconnect(1, 7), Reconnect::Subscribe);
        assert!(tracker.resume_message(1, 7).is_none());
        assert_eq!(tracker.applied_offset(7), None);
    }

    #[test]
    fn the_highest_applied_offset_drives_the_resume() {
        let mut tracker = ResumeTracker::new();
        assert!(!tracker.apply_initial(&initial(7, 10, false)));
        tracker.apply_update(&update(7, 11));
        tracker.apply_update(&update(7, 12));
        assert_eq!(tracker.applied_offset(7), Some(12));
        assert_eq!(
            tracker.on_reconnect(3, 7),
            Reconnect::Resume(Resume {
                id: 3,
                query_id: 7,
                from_offset: 12,
            }),
            "CS-021: resume from the highest applied offset"
        );
    }

    #[test]
    fn offsets_never_rewind_on_replay() {
        let mut tracker = ResumeTracker::new();
        tracker.apply_update(&update(7, 12));
        // A duplicated/replayed older update must not rewind the cursor,
        // or the client would ask the server to resend what it has.
        tracker.apply_update(&update(7, 5));
        assert_eq!(tracker.applied_offset(7), Some(12));
    }

    #[test]
    fn a_cache_reset_snapshot_is_signalled_and_reanchors_the_offset() {
        let mut tracker = ResumeTracker::new();
        tracker.apply_update(&update(7, 3));
        // CS-022: the server compacted past us and answered a snapshot.
        let reset = tracker.apply_initial(&initial(7, 99, true));
        assert!(reset, "the caller must clear its cached rows first");
        assert_eq!(
            tracker.applied_offset(7),
            Some(99),
            "the snapshot re-anchors the cursor"
        );
    }

    #[test]
    fn offsets_are_tracked_per_subscription_and_forgettable() {
        let mut tracker = ResumeTracker::new();
        tracker.apply_update(&update(1, 10));
        tracker.apply_update(&update(2, 20));
        assert_eq!(tracker.applied_offset(1), Some(10));
        assert_eq!(tracker.applied_offset(2), Some(20));
        tracker.forget(1);
        assert_eq!(tracker.on_reconnect(0, 1), Reconnect::Subscribe);
        assert_eq!(tracker.applied_offset(2), Some(20), "peers unaffected");
    }
}
