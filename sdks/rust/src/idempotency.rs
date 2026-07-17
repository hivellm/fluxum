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

use fluxum_protocol::{ClientMessage, ReducerCall};

/// A call queued for submission, carrying the stable key it will keep for
/// every retry (CS-032).
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedCall {
    /// The reducer to run.
    pub reducer: String,
    /// Its positional arguments.
    pub args: Vec<fluxum_protocol::FluxValue>,
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
        args: Vec<fluxum_protocol::FluxValue>,
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
