//! Per-client send-buffer backpressure (SPEC-005 SUB-042, T4.4; FR-33): the
//! three-tier policy that keeps one slow consumer from ever stalling the
//! fan-out loop or the commit path.
//!
//! # The policy (SUB-042)
//!
//! Each connection owns an independent bounded buffer (default 2 MB,
//! `subscriptions.send_buffer_bytes`). The fan-out enqueues shared,
//! already-encoded bytes into it with a **non-blocking** check; the tier is
//! a pure function of the buffer's occupancy and how long a send has been
//! blocked:
//!
//! | Tier | Condition | Behaviour |
//! |------|-----------|-----------|
//! | Normal | `< 50%` | enqueue every update |
//! | Pressured | `50–90%` | enqueue inserts only; skip tick-sourced updates |
//! | Full | `> 90%` OR blocked `> 5 s` | drop the connection |
//!
//! # What this task owns
//!
//! The **buffer and the decision** — a self-contained, clock-injectable
//! type the phase-5 transport drives: the fan-out calls
//! [`SubscriberBuffer::offer`] per subscriber (never blocking), a
//! per-connection writer task drains it to the socket via
//! [`SubscriberBuffer::take`], and a dropped buffer bumps the shared
//! [`SubscriberDropCounter`] and logs `WARN`. The real socket lives in T5.x;
//! here a "blocked send" is modeled as bytes that are offered but never
//! taken, which is exactly what a stuck TCP send buffer looks like from the
//! shard's side.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::types::Timestamp;

/// The Full-tier time trigger: a send blocked this long is a drop (SUB-042).
pub const BLOCKED_DROP_AFTER: Duration = Duration::from_secs(5);

/// Occupancy tier of a [`SubscriberBuffer`] (SUB-042).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// `< 50%`: deliver everything.
    Normal,
    /// `50–90%`: inserts only; skip tick-sourced updates.
    Pressured,
    /// `> 90%` or blocked past [`BLOCKED_DROP_AFTER`]: drop the connection.
    Full,
}

/// Why a subscriber was dropped — the `reason` label of
/// `fluxum_subscriber_drops_total` (SPEC-012).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Buffer occupancy exceeded 90%.
    BufferFull,
    /// A send stayed blocked longer than [`BLOCKED_DROP_AFTER`].
    BlockedTimeout,
}

impl DropReason {
    /// The metric `reason` label value.
    pub const fn label(self) -> &'static str {
        match self {
            Self::BufferFull => "buffer_full",
            Self::BlockedTimeout => "blocked_timeout",
        }
    }
}

/// What [`SubscriberBuffer::offer`] did with one fan-out message (SUB-042).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offered {
    /// Enqueued for delivery.
    Enqueued,
    /// Skipped: a tick-sourced update dropped in the Pressured tier. The
    /// connection stays; the client simply misses this low-priority diff.
    SkippedPressured,
    /// The buffer is Full — the caller must drop the connection with this
    /// reason (bumping the metric, logging WARN).
    Drop(DropReason),
}

/// One fan-out message offered to a subscriber: the shared encoded bytes
/// plus the two flags SUB-042 keys the policy on.
#[derive(Debug, Clone, Copy)]
pub struct Message<'a> {
    /// The already-encoded `TxUpdate` bytes (shared across subscribers,
    /// SUB-024 — the buffer only tracks the length, never copies).
    pub bytes: &'a [u8],
    /// Whether this diff was produced by a `#[fluxum::tick]` reducer
    /// (skippable under pressure, SUB-042).
    pub tick_sourced: bool,
    /// SUB-043 high-priority tables are never dropped under pressure.
    pub high_priority: bool,
}

/// A shard-wide `fluxum_subscriber_drops_total{reason}` counter (SPEC-012).
#[derive(Debug, Default)]
pub struct SubscriberDropCounter {
    buffer_full: AtomicU64,
    blocked_timeout: AtomicU64,
}

impl SubscriberDropCounter {
    /// A zeroed counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one drop with its reason (called on the [`Offered::Drop`]
    /// path).
    pub fn record(&self, reason: DropReason) {
        match reason {
            DropReason::BufferFull => self.buffer_full.fetch_add(1, Ordering::Relaxed),
            DropReason::BlockedTimeout => self.blocked_timeout.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Drops recorded for `reason`.
    pub fn count(&self, reason: DropReason) -> u64 {
        match reason {
            DropReason::BufferFull => self.buffer_full.load(Ordering::Relaxed),
            DropReason::BlockedTimeout => self.blocked_timeout.load(Ordering::Relaxed),
        }
    }

    /// Total drops across every reason.
    pub fn total(&self) -> u64 {
        self.count(DropReason::BufferFull) + self.count(DropReason::BlockedTimeout)
    }
}

/// One connection's independent send buffer (SUB-042).
///
/// Bounded by `capacity_bytes`; occupancy is the sum of queued message
/// lengths. Time is injected (`now`) so the policy is deterministic in
/// tests and driven by the transport's clock in production. Not internally
/// synchronized — the transport owns one per connection.
#[derive(Debug)]
pub struct SubscriberBuffer {
    capacity_bytes: usize,
    queued_bytes: usize,
    queue: VecDeque<Vec<u8>>,
    /// When the buffer first became non-empty while nothing was draining —
    /// the start of a potential blocked-send window (SUB-042 5 s rule).
    blocked_since: Option<Timestamp>,
}

impl SubscriberBuffer {
    /// A buffer of `capacity_bytes` (from
    /// `subscriptions.send_buffer_bytes`). A zero capacity is treated as 1
    /// byte so the tier math never divides by zero — any offer then lands
    /// in the Full tier immediately.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(1),
            queued_bytes: 0,
            queue: VecDeque::new(),
            blocked_since: None,
        }
    }

    /// Bytes currently queued for delivery.
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    /// Buffer occupancy in `[0.0, 1.0+]` (can exceed 1.0 transiently for a
    /// single oversized message — that lands in Full).
    pub fn occupancy(&self) -> f64 {
        self.queued_bytes as f64 / self.capacity_bytes as f64
    }

    /// The occupancy tier at `now` (SUB-042). The time trigger only fires
    /// while a blocked window is open (the buffer has undrained bytes).
    pub fn tier(&self, now: Timestamp) -> Tier {
        if self.blocked_past_deadline(now) {
            return Tier::Full;
        }
        let occ = self.occupancy();
        if occ > 0.90 {
            Tier::Full
        } else if occ >= 0.50 {
            Tier::Pressured
        } else {
            Tier::Normal
        }
    }

    /// Offer one fan-out message at `now` (SUB-042) — **never blocks**.
    /// Applies the tier policy: Normal enqueues, Pressured enqueues unless
    /// the message is a skippable tick-sourced diff, Full returns a drop.
    /// High-priority messages (SUB-043) are enqueued unless the buffer is
    /// truly Full.
    pub fn offer(&mut self, message: &Message<'_>, now: Timestamp) -> Offered {
        match self.tier(now) {
            Tier::Full => {
                let reason = if self.blocked_past_deadline(now) {
                    DropReason::BlockedTimeout
                } else {
                    DropReason::BufferFull
                };
                Offered::Drop(reason)
            }
            Tier::Pressured if message.tick_sourced && !message.high_priority => {
                Offered::SkippedPressured
            }
            Tier::Normal | Tier::Pressured => {
                self.enqueue(message.bytes, now);
                Offered::Enqueued
            }
        }
    }

    /// Drain the next queued message toward the socket (the transport's
    /// writer task). Returns `None` when empty. Draining that empties the
    /// buffer closes any open blocked window.
    pub fn take(&mut self) -> Option<Vec<u8>> {
        let message = self.queue.pop_front()?;
        self.queued_bytes -= message.len();
        if self.queue.is_empty() {
            self.blocked_since = None;
        }
        Some(message)
    }

    fn enqueue(&mut self, bytes: &[u8], now: Timestamp) {
        if self.queue.is_empty() {
            // First byte into an empty buffer opens the blocked-send window:
            // if nothing drains it, the 5 s timer runs from here.
            self.blocked_since = Some(now);
        }
        self.queued_bytes += bytes.len();
        self.queue.push_back(bytes.to_vec());
    }

    fn blocked_past_deadline(&self, now: Timestamp) -> bool {
        self.blocked_since.is_some_and(|since| {
            let elapsed = now.as_micros().saturating_sub(since.as_micros());
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let deadline_us = BLOCKED_DROP_AFTER.as_micros() as i64;
            elapsed > deadline_us
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn ts(secs: i64) -> Timestamp {
        Timestamp::from_micros(secs * 1_000_000)
    }

    fn msg(len: usize) -> Vec<u8> {
        vec![0u8; len]
    }

    fn normal_msg<'a>(bytes: &'a [u8]) -> Message<'a> {
        Message {
            bytes,
            tick_sourced: false,
            high_priority: false,
        }
    }

    #[test]
    fn tier_boundaries_follow_occupancy() {
        let mut buf = SubscriberBuffer::new(100);
        assert_eq!(buf.tier(ts(0)), Tier::Normal);
        // 49% → Normal.
        buf.offer(&normal_msg(&msg(49)), ts(0));
        assert_eq!(buf.tier(ts(0)), Tier::Normal);
        // 50% → Pressured.
        buf.offer(&normal_msg(&msg(1)), ts(0));
        assert_eq!(buf.tier(ts(0)), Tier::Pressured);
        // 91% → Full.
        buf.offer(&normal_msg(&msg(41)), ts(0));
        assert_eq!(buf.tier(ts(0)), Tier::Full);
    }

    #[test]
    fn pressured_tier_skips_tick_sourced_but_keeps_inserts() {
        let mut buf = SubscriberBuffer::new(100);
        buf.offer(&normal_msg(&msg(60)), ts(0)); // → 60%, Pressured
        assert_eq!(buf.tier(ts(0)), Tier::Pressured);

        // A tick-sourced diff is skipped (connection stays).
        let tick = Message {
            bytes: &msg(5),
            tick_sourced: true,
            high_priority: false,
        };
        assert_eq!(buf.offer(&tick, ts(0)), Offered::SkippedPressured);
        assert_eq!(buf.queued_bytes(), 60, "tick diff not enqueued");

        // A regular insert is still delivered.
        assert_eq!(buf.offer(&normal_msg(&msg(5)), ts(0)), Offered::Enqueued);

        // A high-priority tick diff is NOT skipped (SUB-043).
        let hi = Message {
            bytes: &msg(5),
            tick_sourced: true,
            high_priority: true,
        };
        assert_eq!(buf.offer(&hi, ts(0)), Offered::Enqueued);
    }

    #[test]
    fn full_tier_returns_a_buffer_full_drop() {
        let mut buf = SubscriberBuffer::new(100);
        buf.offer(&normal_msg(&msg(95)), ts(0)); // → 95%, Full
        assert_eq!(
            buf.offer(&normal_msg(&msg(1)), ts(0)),
            Offered::Drop(DropReason::BufferFull)
        );
    }

    #[test]
    fn a_send_blocked_past_five_seconds_drops_with_timeout() {
        let mut buf = SubscriberBuffer::new(1_000);
        // A small message (10%) that is never drained opens the window at t0.
        assert_eq!(buf.offer(&normal_msg(&msg(100)), ts(0)), Offered::Enqueued);
        // Still Normal by occupancy, and within the 5 s window.
        assert_eq!(buf.tier(ts(4)), Tier::Normal);
        // Past 5 s blocked → Full → timeout drop.
        assert_eq!(buf.tier(ts(6)), Tier::Full);
        assert_eq!(
            buf.offer(&normal_msg(&msg(1)), ts(6)),
            Offered::Drop(DropReason::BlockedTimeout)
        );
    }

    #[test]
    fn draining_closes_the_blocked_window() {
        let mut buf = SubscriberBuffer::new(1_000);
        buf.offer(&normal_msg(&msg(100)), ts(0));
        // Drain everything: the window closes, so t+6 is no longer blocked.
        assert_eq!(buf.take().unwrap().len(), 100);
        assert!(buf.take().is_none());
        assert_eq!(
            buf.tier(ts(6)),
            Tier::Normal,
            "empty buffer never times out"
        );
        assert_eq!(buf.queued_bytes(), 0);
    }

    #[test]
    fn drop_counter_tracks_reasons() {
        let counter = SubscriberDropCounter::new();
        counter.record(DropReason::BufferFull);
        counter.record(DropReason::BufferFull);
        counter.record(DropReason::BlockedTimeout);
        assert_eq!(counter.count(DropReason::BufferFull), 2);
        assert_eq!(counter.count(DropReason::BlockedTimeout), 1);
        assert_eq!(counter.total(), 3);
        assert_eq!(DropReason::BufferFull.label(), "buffer_full");
        assert_eq!(DropReason::BlockedTimeout.label(), "blocked_timeout");
    }

    #[test]
    fn zero_capacity_buffer_is_immediately_full() {
        let mut buf = SubscriberBuffer::new(0);
        assert_eq!(buf.tier(ts(0)), Tier::Normal); // empty
        // Any non-empty offer overflows the 1-byte floor → Full next check.
        assert_eq!(buf.offer(&normal_msg(&msg(1)), ts(0)), Offered::Enqueued);
        assert_eq!(
            buf.offer(&normal_msg(&msg(1)), ts(0)),
            Offered::Drop(DropReason::BufferFull)
        );
    }
}
