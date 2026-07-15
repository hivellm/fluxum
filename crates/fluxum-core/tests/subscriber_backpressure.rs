//! T4.4 slow-consumer isolation (SPEC-005 SUB-042; FR-33; DAG exit test):
//! the per-client send-buffer tiers keep one blocked subscriber from
//! affecting the other 999 — the healthy buffers accept every fan-out
//! message immediately (non-blocking), the blocked one fills and is dropped
//! after the 5 s window, the drop is counted, and the fan-out never blocks
//! on the slow client.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::subscription::{
    DropReason, Message, Offered, SubscriberBuffer, SubscriberDropCounter, Tier,
};
use fluxum_core::types::Timestamp;

const CLIENTS: usize = 1_000;
const BUFFER_BYTES: usize = 2 << 20; // 2 MB default

fn ts(secs: i64) -> Timestamp {
    Timestamp::from_micros(secs * 1_000_000)
}

fn update(bytes: &[u8]) -> Message<'_> {
    Message {
        bytes,
        tick_sourced: false,
        high_priority: false,
    }
}

/// 1,000 subscribers, one whose socket is blocked (its buffer is never
/// drained). Repeated commits fan out to all: the 999 healthy buffers accept
/// every message immediately, and the blocked one is dropped once its 5 s
/// window elapses — the others are entirely unaffected.
#[test]
fn one_blocked_subscriber_does_not_affect_the_other_999() {
    let mut buffers: Vec<SubscriberBuffer> = (0..CLIENTS)
        .map(|_| SubscriberBuffer::new(BUFFER_BYTES))
        .collect();
    let counter = SubscriberDropCounter::new();
    let blocked = 0usize; // this connection never drains its buffer

    // A realistic per-commit TxUpdate payload (a few hundred bytes).
    let payload = vec![0u8; 256];
    let mut dropped: Option<usize> = None;

    // 20 commits over 10 seconds of virtual time (one every 500 ms).
    for step in 0..20i64 {
        let now = Timestamp::from_micros(step * 500_000);
        for (conn, buf) in buffers.iter_mut().enumerate() {
            if dropped == Some(conn) {
                continue; // already dropped — the transport stopped offering
            }
            match buf.offer(&update(&payload), now) {
                Offered::Enqueued | Offered::SkippedPressured => {}
                Offered::Drop(reason) => {
                    counter.record(reason);
                    assert_eq!(conn, blocked, "only the blocked client is dropped");
                    dropped = Some(conn);
                }
            }
            // The 999 healthy connections drain immediately (fast socket):
            // their buffer returns to empty every step, staying in Normal.
            if conn != blocked {
                while buf.take().is_some() {}
                assert_eq!(buf.tier(now), Tier::Normal, "healthy client stays Normal");
                assert_eq!(buf.queued_bytes(), 0);
            }
        }
    }

    // The blocked client was dropped exactly once, on the blocked-timeout
    // path (its 256-byte payload never approached 90% of 2 MB, so occupancy
    // alone would never trip Full — the 5 s window did).
    assert_eq!(dropped, Some(blocked), "the blocked client is dropped");
    assert_eq!(counter.total(), 1, "exactly one drop");
    assert_eq!(counter.count(DropReason::BlockedTimeout), 1);
    assert_eq!(counter.count(DropReason::BufferFull), 0);

    // Every other connection is still healthy and empty (never affected).
    for (conn, buf) in buffers.iter().enumerate() {
        if conn == blocked {
            continue;
        }
        assert_eq!(buf.queued_bytes(), 0);
        assert_eq!(buf.tier(ts(10)), Tier::Normal);
    }
}

/// A burst that overflows a small buffer trips the occupancy Full tier
/// (buffer_full reason), independently of the time window.
#[test]
fn a_fast_overflow_drops_on_buffer_full_not_timeout() {
    let mut buf = SubscriberBuffer::new(1_000);
    let counter = SubscriberDropCounter::new();
    let big = vec![0u8; 400];

    // Offer 400-byte messages at the SAME instant (no time window in play):
    // the buffer fills past 90% and the drop that follows is buffer_full,
    // never blocked_timeout.
    let mut drops = 0;
    for _ in 0..5 {
        if let Offered::Drop(reason) = buf.offer(&update(&big), ts(0)) {
            counter.record(reason);
            assert_eq!(reason, DropReason::BufferFull, "occupancy, not the clock");
            drops += 1;
            break;
        }
    }
    assert_eq!(drops, 1, "a same-instant overflow drops exactly once");
    assert_eq!(counter.count(DropReason::BufferFull), 1);
    assert_eq!(counter.count(DropReason::BlockedTimeout), 0);
}
