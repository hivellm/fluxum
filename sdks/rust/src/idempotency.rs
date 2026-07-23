//! Client-side exactly-once submission (SPEC-021 CS-032).
//!
//! A queued call that is replayed after a lost ack must carry the *same*
//! `idempotency_key` it was first sent with, or the server cannot tell the
//! retry from a new call and the effect double-applies. The key must
//! therefore be minted **once, when the call is enqueued** — never at send
//! time, or every retry would mint a fresh one and defeat the whole
//! mechanism.
//!
//! This is that queue's key discipline as a transport-free unit. The socket
//! and the durable queue land with DAG **T6.2** / the SDK offline-queue task
//! (both after the G5 wire freeze); the piece the freeze constrains — that
//! a replayed call reuses its key — ships here, tested.

use crate::protocol::{ClientMessage, ReducerCall};

/// A call queued for submission, carrying the stable key it will keep for
/// every retry (CS-032). Serializable so a durable queue (CS-040) can store
/// it and a restart can replay it under its original key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueuedCall {
    /// The reducer to run.
    pub reducer: String,
    /// Its positional arguments.
    pub args: Vec<crate::protocol::FluxValue>,
    /// The key minted when this call was enqueued. Stable across every
    /// resend: that stability *is* the exactly-once guarantee.
    pub idempotency_key: String,
    /// How many times it has been handed to the transport.
    pub attempts: u32,
}

impl QueuedCall {
    /// Render the call as a wire message. Called once per attempt; the key
    /// never changes, so a retry is recognisable as the same submission.
    pub fn to_message(&self, id: u32) -> ClientMessage {
        ClientMessage::ReducerCall(ReducerCall {
            id,
            reducer: self.reducer.clone(),
            version: None,
            args: self.args.clone(),
            idempotency_key: Some(self.idempotency_key.clone()),
        })
    }
}

/// An offline replay queue that mints a stable `idempotency_key` per call
/// (CS-032), so reconnect replay is safe.
///
/// Keys are minted from a monotonic counter plus the caller-supplied
/// `client_id`, which must be stable for the client's identity across
/// restarts if queued calls are persisted (SDK offline persistence, CS-04x)
/// — otherwise a call queued before a restart would be replayed under a new
/// key and apply twice.
#[derive(Debug)]
pub struct OfflineQueue {
    client_id: String,
    next_seq: u64,
    pending: Vec<QueuedCall>,
}

impl OfflineQueue {
    /// A queue whose keys are namespaced by `client_id`.
    pub fn new(client_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            next_seq: 0,
            pending: Vec::new(),
        }
    }

    /// Enqueue a call, minting its stable key now (CS-032). Returns the key.
    pub fn enqueue(
        &mut self,
        reducer: impl Into<String>,
        args: Vec<crate::protocol::FluxValue>,
    ) -> String {
        let key = format!("{}:{}", self.client_id, self.next_seq);
        self.next_seq += 1;
        self.pending.push(QueuedCall {
            reducer: reducer.into(),
            args,
            idempotency_key: key.clone(),
            attempts: 0,
        });
        key
    }

    /// The calls awaiting acknowledgement, oldest first.
    pub fn pending(&self) -> &[QueuedCall] {
        &self.pending
    }

    /// Hand the oldest unacked call to the transport again, bumping its
    /// attempt count. The key is untouched — that is the point.
    pub fn next_attempt(&mut self, id: u32) -> Option<ClientMessage> {
        let call = self.pending.first_mut()?;
        call.attempts += 1;
        Some(call.to_message(id))
    }

    /// Hand a SPECIFIC queued call to the transport (replay walks the queue
    /// in order but sends each call under its own request id), bumping its
    /// attempt count. The key is untouched. `None` if the key is not queued
    /// (already acknowledged).
    pub fn attempt(&mut self, idempotency_key: &str, id: u32) -> Option<ClientMessage> {
        let call = self
            .pending
            .iter_mut()
            .find(|c| c.idempotency_key == idempotency_key)?;
        call.attempts += 1;
        Some(call.to_message(id))
    }

    /// Drop a call once the server has acknowledged it. A `ReducerResult`
    /// for a deduplicated replay is an ack like any other: the server has
    /// applied it exactly once, so the client stops resending.
    pub fn acknowledge(&mut self, idempotency_key: &str) -> bool {
        let before = self.pending.len();
        self.pending
            .retain(|c| c.idempotency_key != idempotency_key);
        self.pending.len() != before
    }

    /// Whether anything is awaiting acknowledgement.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// A point-in-time image of the whole queue, for durable persistence
    /// (SPEC-021 CS-040): everything a restart needs to replay each call
    /// under its ORIGINAL key — a fresh key would double-apply (CS-032).
    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            client_id: self.client_id.clone(),
            next_seq: self.next_seq,
            pending: self.pending.clone(),
        }
    }

    /// Rebuild a queue from a persisted [`QueueSnapshot`]: the pending calls
    /// keep their minted keys, and `next_seq` resumes where it left off so
    /// no future call can reuse a key issued before the restart.
    pub fn restore(snapshot: QueueSnapshot) -> Self {
        Self {
            client_id: snapshot.client_id,
            next_seq: snapshot.next_seq,
            pending: snapshot.pending,
        }
    }
}

/// A serializable image of an [`OfflineQueue`] (CS-040): the client identity
/// namespace, the key counter, and every call still awaiting its ack.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueueSnapshot {
    /// The queue's key namespace.
    pub client_id: String,
    /// The next key sequence number.
    pub next_seq: u64,
    /// The calls awaiting acknowledgement, oldest first.
    pub pending: Vec<QueuedCall>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn every_queued_call_gets_its_own_stable_key() {
        let mut queue = OfflineQueue::new("client-a");
        let k1 = queue.enqueue("transfer", vec![]);
        let k2 = queue.enqueue("transfer", vec![]);
        assert_ne!(k1, k2, "distinct calls are distinct submissions");
        assert_eq!(queue.pending().len(), 2);
    }

    #[test]
    fn a_retry_reuses_the_key_it_was_enqueued_with() {
        let mut queue = OfflineQueue::new("client-a");
        let key = queue.enqueue("transfer", vec![]);

        // Three attempts — a lost ack, then a reconnect, then another.
        let mut seen = Vec::new();
        for id in 0..3 {
            let ClientMessage::ReducerCall(call) = queue.next_attempt(id).unwrap() else {
                panic!("expected a ReducerCall");
            };
            seen.push(call.idempotency_key.clone());
        }
        assert_eq!(
            seen,
            vec![Some(key.clone()), Some(key.clone()), Some(key)],
            "CS-032: the key is stable across retries, or the retry would double-apply"
        );
        assert_eq!(queue.pending()[0].attempts, 3);
    }

    #[test]
    fn keys_are_namespaced_per_client() {
        let mut a = OfflineQueue::new("client-a");
        let mut b = OfflineQueue::new("client-b");
        // Two clients' first calls must not share a key — the server scopes
        // per (Identity, reducer), but a shared identity across devices
        // would otherwise collide.
        assert_ne!(a.enqueue("transfer", vec![]), b.enqueue("transfer", vec![]));
    }

    #[test]
    fn acknowledging_removes_the_call_from_the_queue() {
        let mut queue = OfflineQueue::new("client-a");
        let k1 = queue.enqueue("transfer", vec![]);
        let k2 = queue.enqueue("refund", vec![]);
        assert!(queue.acknowledge(&k1));
        assert_eq!(queue.pending().len(), 1);
        assert_eq!(queue.pending()[0].idempotency_key, k2);
        // An ack for something already gone is a no-op, not a panic (a
        // duplicate ack after a deduplicated replay is normal).
        assert!(!queue.acknowledge(&k1));
        assert!(queue.acknowledge(&k2));
        assert!(queue.is_empty());
    }

    #[test]
    fn a_snapshot_round_trips_with_original_keys_and_counter() {
        // CS-040/CS-032: a call queued before a restart must replay under
        // its original key, and the counter must resume so no later call can
        // collide with a key issued before the restart.
        let mut queue = OfflineQueue::new("client-a");
        let k1 = queue.enqueue("transfer", vec![]);

        let bytes = rmp_serde::to_vec(&queue.snapshot()).unwrap();
        let restored: QueueSnapshot = rmp_serde::from_slice(&bytes).unwrap();
        let mut queue = OfflineQueue::restore(restored);

        assert_eq!(queue.pending().len(), 1);
        assert_eq!(queue.pending()[0].idempotency_key, k1);
        let k2 = queue.enqueue("transfer", vec![]);
        assert_ne!(k1, k2, "the counter resumed, not restarted");
    }

    #[test]
    fn attempt_by_key_renders_the_named_call_and_bumps_its_count() {
        let mut queue = OfflineQueue::new("c");
        let _k1 = queue.enqueue("a", vec![]);
        let k2 = queue.enqueue("b", vec![]);
        let ClientMessage::ReducerCall(call) = queue.attempt(&k2, 7).unwrap() else {
            panic!("expected a ReducerCall");
        };
        assert_eq!(call.reducer, "b");
        assert_eq!(call.idempotency_key, Some(k2.clone()));
        assert_eq!(queue.pending()[1].attempts, 1);
        assert_eq!(queue.pending()[0].attempts, 0, "only the named call");
        queue.acknowledge(&k2);
        assert!(queue.attempt(&k2, 8).is_none(), "acknowledged: gone");
    }

    #[test]
    fn the_message_carries_the_key_on_the_wire() {
        let mut queue = OfflineQueue::new("c");
        let key = queue.enqueue("transfer", vec![]);
        let ClientMessage::ReducerCall(call) = queue.next_attempt(9).unwrap() else {
            panic!("expected a ReducerCall");
        };
        assert_eq!(call.id, 9);
        assert_eq!(call.reducer, "transfer");
        assert_eq!(call.idempotency_key, Some(key));
    }
}
