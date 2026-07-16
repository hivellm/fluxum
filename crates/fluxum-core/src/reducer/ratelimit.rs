//! Reducer rate limiting (SPEC-004 §7, T3.5; FR-24): the per-`(Identity,
//! reducer)` token buckets behind `#[fluxum::reducer(max_rate = "N/s")]`
//! (RED-050/RED-051) and the RED-052 global shard guard.
//!
//! Rejection happens at **admission**, on the engine's client path, before
//! any transaction or `TxState` exists — a rejected call costs one HashMap
//! probe and touches no storage. Buckets live in shard memory only (never
//! in `CommittedState`): they are ephemeral by design and reset on restart.
//! Scheduled and lifecycle executions dispatch past admission entirely, and
//! exempt identities — the shard's own server identity plus any registered
//! server peers (`SHA-256("SERVER:" + name)`, AUTH-062) — are never
//! limited.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};
use crate::types::Identity;

/// A classic token bucket: capacity `rate`, refilling continuously at
/// `rate` tokens per second (RED-051 — 1 token per `1/max_rate` seconds).
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64, now: Instant) -> Self {
        Self {
            tokens: rate,
            capacity: rate,
            refill_per_sec: rate,
            last_refill: now,
        }
    }

    /// Refill by elapsed time, then consume one token if available.
    fn try_consume(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Tuning knobs for a [`RateLimiter`].
#[derive(Debug, Clone, Copy)]
pub struct RateLimiterOptions {
    /// RED-052 global shard guard: total client reducer admissions per
    /// second before excess calls answer `503 "shard overloaded"`.
    /// `0` disables the guard. Default 200,000.
    pub shard_max_reducers_per_sec: u64,
}

impl Default for RateLimiterOptions {
    fn default() -> Self {
        Self {
            shard_max_reducers_per_sec: 200_000,
        }
    }
}

/// The shard's admission rate limiter (RED-050..RED-052).
pub struct RateLimiter {
    /// `(identity, reducer)` → bucket, created lazily on first call.
    buckets: Mutex<HashMap<(Identity, String), TokenBucket>>,
    /// RED-052 shard-wide bucket (`None` when disabled).
    global: Option<Mutex<TokenBucket>>,
    /// AUTH-062 exemptions: never rate-limited.
    exempt: HashSet<Identity>,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("global_enabled", &self.global.is_some())
            .field("exempt_identities", &self.exempt.len())
            .finish_non_exhaustive()
    }
}

impl RateLimiter {
    /// Build a limiter; `exempt` identities (server-to-server peers and the
    /// shard's own server identity, AUTH-062) bypass every limit.
    pub fn new(options: RateLimiterOptions, exempt: impl IntoIterator<Item = Identity>) -> Self {
        let now = Instant::now();
        Self {
            buckets: Mutex::new(HashMap::new()),
            global: (options.shard_max_reducers_per_sec > 0).then(|| {
                #[allow(clippy::cast_precision_loss)] // admission rates, not money
                Mutex::new(TokenBucket::new(
                    options.shard_max_reducers_per_sec as f64,
                    now,
                ))
            }),
            exempt: exempt.into_iter().collect(),
        }
    }

    /// Admit or reject one client call (RED-050/RED-052): the global shard
    /// guard answers `503 "shard overloaded"`, the per-`(identity,
    /// reducer)` bucket answers 429 — both before any `TxState` exists.
    /// `max_rate_per_sec == 0` means the reducer declares no limit.
    pub fn check(&self, identity: &Identity, reducer: &str, max_rate_per_sec: u32) -> Result<()> {
        if self.exempt.contains(identity) {
            return Ok(());
        }
        let now = Instant::now();
        if let Some(global) = &self.global {
            let mut bucket = global
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !bucket.try_consume(now) {
                return Err(FluxumError::query(
                    codes::SYS_OVERLOADED,
                    "shard overloaded",
                ));
            }
        }
        if max_rate_per_sec == 0 {
            return Ok(());
        }
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bucket = buckets
            .entry((*identity, reducer.to_owned()))
            .or_insert_with(|| TokenBucket::new(f64::from(max_rate_per_sec), now));
        if bucket.try_consume(now) {
            Ok(())
        } else {
            // SPEC-028 §4: advertise the refill estimate — the next token
            // arrives within one refill period (1000 ms / rate), so a client
            // honoring `retry_after_ms` never worsens the condition.
            let retry_after_ms = 1_000u32.div_ceil(max_rate_per_sec.max(1));
            Err(FluxumError::query_retryable(
                codes::REDUCER_RATE_LIMITED,
                format!("reducer `{reducer}` rate limit exceeded ({max_rate_per_sec}/s, RED-050)"),
                Some(retry_after_ms),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn id(seed: u8) -> Identity {
        Identity::from_bytes([seed; 32])
    }

    #[test]
    fn burst_over_capacity_rejects_the_excess_with_429() {
        let limiter = RateLimiter::new(RateLimiterOptions::default(), []);
        let caller = id(1);
        let mut accepted = 0;
        let mut rejected = 0;
        for _ in 0..10 {
            match limiter.check(&caller, "send_chat", 5) {
                Ok(()) => accepted += 1,
                Err(e) => {
                    assert_eq!(e.query_code(), Some(codes::REDUCER_RATE_LIMITED), "{e}");
                    rejected += 1;
                }
            }
        }
        assert_eq!((accepted, rejected), (5, 5), "RED-050 conformance");
    }

    #[test]
    fn buckets_are_independent_per_identity_and_reducer() {
        let limiter = RateLimiter::new(RateLimiterOptions::default(), []);
        for _ in 0..3 {
            limiter.check(&id(1), "send_chat", 3).unwrap();
        }
        assert!(limiter.check(&id(1), "send_chat", 3).is_err(), "exhausted");
        // Same identity, different reducer: fresh bucket.
        limiter.check(&id(1), "rename_user", 3).unwrap();
        // Different identity, same reducer: fresh bucket.
        limiter.check(&id(2), "send_chat", 3).unwrap();
    }

    #[test]
    fn refill_restores_capacity_after_the_window() {
        let limiter = RateLimiter::new(RateLimiterOptions::default(), []);
        let caller = id(3);
        for _ in 0..40 {
            limiter.check(&caller, "f", 40).unwrap();
        }
        assert!(limiter.check(&caller, "f", 40).is_err());
        // 40/s refills one token every 25 ms.
        std::thread::sleep(std::time::Duration::from_millis(120));
        assert!(
            limiter.check(&caller, "f", 40).is_ok(),
            "refill restores capacity (RED-051)"
        );
    }

    #[test]
    fn exempt_identities_are_never_limited() {
        let server = crate::auth::server_identity("peer");
        let limiter = RateLimiter::new(RateLimiterOptions::default(), [server]);
        for _ in 0..50 {
            limiter.check(&server, "send_chat", 1).unwrap();
        }
    }

    #[test]
    fn global_guard_answers_503_on_the_excess_only() {
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 3,
            },
            [],
        );
        let mut ok = 0;
        let mut overloaded = 0;
        for i in 0..5u8 {
            // Distinct identities and no per-reducer limit: only the
            // global bucket is in play (RED-052).
            match limiter.check(&id(i), "f", 0) {
                Ok(()) => ok += 1,
                Err(e) => {
                    assert_eq!(e.query_code(), Some(codes::SYS_OVERLOADED), "{e}");
                    assert!(e.to_string().contains("shard overloaded"), "{e}");
                    overloaded += 1;
                }
            }
        }
        assert_eq!((ok, overloaded), (3, 2));
    }

    #[test]
    fn disabled_global_guard_admits_everything() {
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 0,
            },
            [],
        );
        for i in 0..100u8 {
            limiter.check(&id(i), "f", 0).unwrap();
        }
    }
}
