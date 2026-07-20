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

    /// Credit the tokens earned since the last refill (shared by
    /// [`TokenBucket::try_consume`] and the SEC-047 idle-entry sweep).
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
    }

    /// Retune an existing bucket to `rate` without handing out a free burst
    /// (SPEC-025 OPS-040): credit the tokens earned under the *old* rate
    /// first, then adopt the new one and clamp the balance to the new
    /// capacity. Rebuilding the bucket instead would refill it to full,
    /// letting a reload be used to bypass the limit at will.
    fn retune(&mut self, rate: f64, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
        self.capacity = rate;
        self.refill_per_sec = rate;
        self.tokens = self.tokens.min(rate);
    }

    /// Refill by elapsed time, then consume one token if available.
    fn try_consume(&mut self, now: Instant) -> bool {
        self.refill(now);
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

impl RateLimiterOptions {
    /// The built-in RED-052 shard guard, also `reducer.shard_max_reducers_per_sec`'s
    /// config default — the two are one constant so they cannot drift.
    pub const DEFAULT_SHARD_MAX_REDUCERS_PER_SEC: u64 = 200_000;
}

impl Default for RateLimiterOptions {
    fn default() -> Self {
        Self {
            shard_max_reducers_per_sec: Self::DEFAULT_SHARD_MAX_REDUCERS_PER_SEC,
        }
    }
}

/// The shard's admission rate limiter (RED-050..RED-052).
pub struct RateLimiter {
    /// `(identity, reducer)` → bucket, created lazily on first call.
    buckets: Mutex<HashMap<(Identity, String), TokenBucket>>,
    /// RED-052 shard-wide bucket (`None` when disabled). The `Option` is
    /// *inside* the lock, not outside it, so a hot reload (OPS-040) can
    /// enable a guard that booted disabled — with the `Option` outside, a
    /// `0` at boot would freeze the guard off until restart.
    global: Mutex<Option<TokenBucket>>,
    /// AUTH-062 exemptions: never rate-limited.
    exempt: HashSet<Identity>,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field(
                "global_enabled",
                &self.shard_max_reducers_per_sec().is_some(),
            )
            .field("exempt_identities", &self.exempt.len())
            .finish_non_exhaustive()
    }
}

impl RateLimiter {
    /// Build a limiter; `exempt` identities (server-to-server peers and the
    /// shard's own server identity, AUTH-062) bypass every limit.
    pub fn new(options: RateLimiterOptions, exempt: impl IntoIterator<Item = Identity>) -> Self {
        let now = Instant::now();
        #[allow(clippy::cast_precision_loss)] // admission rates, not money
        let global = (options.shard_max_reducers_per_sec > 0)
            .then(|| TokenBucket::new(options.shard_max_reducers_per_sec as f64, now));
        Self {
            buckets: Mutex::new(HashMap::new()),
            global: Mutex::new(global),
            exempt: exempt.into_iter().collect(),
        }
    }

    /// The RED-052 guard's current rate, or `None` while it is disabled.
    pub fn shard_max_reducers_per_sec(&self) -> Option<u64> {
        let global = self
            .global
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        global.as_ref().map(|bucket| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            // Set from a u64 rate; never negative, never above u64::MAX.
            let rate = bucket.capacity as u64;
            rate
        })
    }

    /// Publish a new RED-052 shard guard rate to this *running* limiter
    /// (OPS-040 hot reload): `0` disables the guard, any other value tunes
    /// or enables it. Takes effect for the next admission; in-flight calls
    /// that already passed admission are unaffected.
    pub fn set_shard_max_reducers_per_sec(&self, rate: u64) {
        let now = Instant::now();
        let mut global = self
            .global
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rate == 0 {
            *global = None;
            return;
        }
        #[allow(clippy::cast_precision_loss)] // admission rates, not money
        let rate = rate as f64;
        match global.as_mut() {
            Some(bucket) => bucket.retune(rate, now),
            None => *global = Some(TokenBucket::new(rate, now)),
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
        {
            let mut global = self
                .global
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(bucket) = global.as_mut()
                && !bucket.try_consume(now)
            {
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

// --- SEC-047: query-admission limiter --------------------------------------

/// Where a query physically came from — the key of the SEC-047 secondary
/// bucket. Preferably the **resolved client IP** (SEC-035: the proxy-aware
/// address every per-IP defense keys on), falling back to the connection id
/// where no IP exists (embedded/in-process transports). Keying a second
/// bucket by source is what makes token rotation useless: a caller minting
/// fresh identities still drains one source-keyed budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuerySource {
    /// The resolved client IP (SEC-035).
    Ip(std::net::IpAddr),
    /// The connection id, when no client IP exists.
    Connection(u128),
}

/// A refused query admission (SEC-047): which bucket refused and the
/// client-facing retry estimate.
#[derive(Debug, Clone, Copy)]
pub struct QueryRejected {
    /// The bucket that refused (drives the metric label).
    pub bucket: crate::metrics::QueryRateBucket,
    /// The refill estimate to advertise (SPEC-028 §4).
    pub retry_after_ms: u32,
}

impl QueryRejected {
    /// The wire-ready retryable 429 (SPEC-028 code 6003).
    pub fn to_error(self) -> FluxumError {
        FluxumError::query_retryable(
            codes::SUB_QUERY_RATE_LIMITED,
            format!(
                "query admission rate exceeded ({} bucket, SEC-047); retry shortly",
                self.bucket.as_str()
            ),
            Some(self.retry_after_ms),
        )
    }
}

/// Cap on tracked buckets per keyspace (SEC-047): bounds the memory an
/// identity- or IP-rotating caller can pin. At the cap, idle (fully
/// refilled) entries are swept; if none is reclaimable the *new* key is
/// refused — fail closed under rotation pressure, never unbounded growth.
const QUERY_LIMITER_MAX_TRACKED: usize = 100_000;

/// SEC-047: token buckets in front of subscription registration and one-off
/// queries — per **identity** and, secondarily, per **source**
/// ([`QuerySource`]), so rotating tokens cannot mint fresh budget. Rates are
/// interior-mutable for OPS-040 hot reload; `0` disables that bucket.
///
/// Admission-time only, like the reducer limiter: a refused query costs two
/// HashMap probes and never touches a snapshot.
#[derive(Debug, Default)]
pub struct QueryLimiter {
    identity_rate: std::sync::atomic::AtomicU64,
    source_rate: std::sync::atomic::AtomicU64,
    identities: Mutex<HashMap<Identity, TokenBucket>>,
    sources: Mutex<HashMap<QuerySource, TokenBucket>>,
}

impl QueryLimiter {
    /// A limiter with the given per-second rates (`0` = that bucket off).
    pub fn new(identity_rate: u64, source_rate: u64) -> Self {
        let limiter = Self::default();
        limiter.set_rates(identity_rate, source_rate);
        limiter
    }

    /// Publish new rates (boot and OPS-040 hot reload). Existing buckets
    /// retune on their next check without a free burst (OPS-040).
    pub fn set_rates(&self, identity_rate: u64, source_rate: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.identity_rate.store(identity_rate, Relaxed);
        self.source_rate.store(source_rate, Relaxed);
    }

    /// The current `(identity, source)` rates (`0` = off).
    pub fn rates(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.identity_rate.load(Relaxed),
            self.source_rate.load(Relaxed),
        )
    }

    /// Admit or refuse one subscription registration / one-off query for
    /// `(identity, source)` (SEC-047). Both buckets are charged on success;
    /// server peers must be exempted by the caller (AUTH-062 — the limiter
    /// cannot tell a server identity from its bytes).
    pub fn check(
        &self,
        identity: &Identity,
        source: QuerySource,
    ) -> std::result::Result<(), QueryRejected> {
        use crate::metrics::QueryRateBucket;
        use std::sync::atomic::Ordering::Relaxed;
        let now = Instant::now();
        let identity_rate = self.identity_rate.load(Relaxed);
        let source_rate = self.source_rate.load(Relaxed);
        if !Self::charge(&self.identities, *identity, identity_rate, now) {
            return Err(QueryRejected {
                bucket: QueryRateBucket::Identity,
                retry_after_ms: retry_after_ms(identity_rate),
            });
        }
        if !Self::charge(&self.sources, source, source_rate, now) {
            return Err(QueryRejected {
                bucket: QueryRateBucket::Source,
                retry_after_ms: retry_after_ms(source_rate),
            });
        }
        Ok(())
    }

    /// Charge one token from `key`'s bucket in `map` (rate `0` = admit).
    fn charge<K: std::hash::Hash + Eq + Copy>(
        map: &Mutex<HashMap<K, TokenBucket>>,
        key: K,
        rate: u64,
        now: Instant,
    ) -> bool {
        if rate == 0 {
            return true;
        }
        #[allow(clippy::cast_precision_loss)] // admission rates, not money
        let rate = rate as f64;
        let mut map = map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !map.contains_key(&key) && map.len() >= QUERY_LIMITER_MAX_TRACKED {
            // Reclaim idle entries: a fully refilled bucket has not been
            // charged for at least one refill window.
            map.retain(|_, bucket| {
                bucket.refill(now);
                bucket.tokens < bucket.capacity
            });
            if map.len() >= QUERY_LIMITER_MAX_TRACKED {
                return false; // fail closed for brand-new keys under rotation pressure
            }
        }
        let bucket = map
            .entry(key)
            .or_insert_with(|| TokenBucket::new(rate, now));
        // OPS-040: adopt a reloaded rate without a free burst.
        if (bucket.refill_per_sec - rate).abs() > f64::EPSILON {
            bucket.retune(rate, now);
        }
        bucket.try_consume(now)
    }
}

/// The SPEC-028 §4 refill estimate for a `rate`-per-second bucket.
fn retry_after_ms(rate: u64) -> u32 {
    let rate = u32::try_from(rate).unwrap_or(u32::MAX);
    1_000u32.div_ceil(rate.max(1))
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
    fn reload_can_enable_a_guard_that_booted_disabled() {
        // OPS-040: `0` at boot must not freeze the guard off until restart.
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 0,
            },
            [],
        );
        assert_eq!(limiter.shard_max_reducers_per_sec(), None);
        limiter.set_shard_max_reducers_per_sec(2);
        assert_eq!(limiter.shard_max_reducers_per_sec(), Some(2));
        assert!(limiter.check(&id(1), "f", 0).is_ok());
        assert!(limiter.check(&id(2), "f", 0).is_ok());
        assert!(
            limiter.check(&id(3), "f", 0).is_err(),
            "the newly enabled guard limits the third call"
        );
    }

    #[test]
    fn reload_can_disable_a_running_guard() {
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 1,
            },
            [],
        );
        limiter.check(&id(1), "f", 0).unwrap();
        assert!(limiter.check(&id(2), "f", 0).is_err(), "guard is active");
        limiter.set_shard_max_reducers_per_sec(0);
        assert_eq!(limiter.shard_max_reducers_per_sec(), None);
        for i in 0..50u8 {
            limiter.check(&id(i), "f", 0).unwrap();
        }
    }

    #[test]
    fn retuning_the_guard_does_not_hand_out_a_free_burst() {
        // An exhausted bucket retuned to a higher rate must stay exhausted:
        // otherwise repeated reloads are a limit bypass.
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 2,
            },
            [],
        );
        limiter.check(&id(1), "f", 0).unwrap();
        limiter.check(&id(2), "f", 0).unwrap();
        assert!(limiter.check(&id(3), "f", 0).is_err(), "exhausted");
        limiter.set_shard_max_reducers_per_sec(1_000);
        assert_eq!(limiter.shard_max_reducers_per_sec(), Some(1_000));
        assert!(
            limiter.check(&id(4), "f", 0).is_err(),
            "a retune must not refill the bucket (OPS-040)"
        );
    }

    #[test]
    fn retuning_down_clamps_the_balance_to_the_new_capacity() {
        let limiter = RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 10_000,
            },
            [],
        );
        // Full bucket (10k tokens), retuned down to 1/s: the balance must
        // clamp to the new capacity, not stay at 10k.
        limiter.set_shard_max_reducers_per_sec(1);
        limiter.check(&id(1), "f", 0).unwrap();
        assert!(
            limiter.check(&id(2), "f", 0).is_err(),
            "the lowered limit binds immediately"
        );
    }

    // --- SEC-047: query-admission limiter ----------------------------------

    #[test]
    fn a_token_rotating_caller_cannot_exceed_the_source_bucket() {
        use crate::metrics::QueryRateBucket;
        // Identity bucket generous, source bucket 2/s: rotating identities
        // mints fresh identity budget but drains ONE source budget.
        let limiter = QueryLimiter::new(1_000, 2);
        let source = QuerySource::Ip("203.0.113.9".parse().unwrap());
        limiter.check(&id(1), source).unwrap();
        limiter.check(&id(2), source).unwrap();
        let rejected = limiter.check(&id(3), source).unwrap_err();
        assert_eq!(rejected.bucket, QueryRateBucket::Source);
        let err = rejected.to_error();
        assert_eq!(err.query_code(), Some(codes::SUB_QUERY_RATE_LIMITED));
        let wire = err.to_wire();
        assert!(wire.retry_after_ms.is_some(), "retryable with an estimate");
        // A different source still has its own budget.
        limiter
            .check(&id(4), QuerySource::Ip("203.0.113.10".parse().unwrap()))
            .unwrap();
    }

    #[test]
    fn the_identity_bucket_binds_across_sources() {
        use crate::metrics::QueryRateBucket;
        let limiter = QueryLimiter::new(2, 1_000);
        let caller = id(5);
        limiter.check(&caller, QuerySource::Connection(1)).unwrap();
        limiter.check(&caller, QuerySource::Connection(2)).unwrap();
        let rejected = limiter
            .check(&caller, QuerySource::Connection(3))
            .unwrap_err();
        assert_eq!(rejected.bucket, QueryRateBucket::Identity);
    }

    #[test]
    fn zero_rates_disable_the_query_limiter() {
        let limiter = QueryLimiter::new(0, 0);
        for i in 0..100u8 {
            limiter.check(&id(i), QuerySource::Connection(1)).unwrap();
        }
        assert_eq!(limiter.rates(), (0, 0));
    }

    #[test]
    fn reloaded_query_rates_retune_without_a_free_burst() {
        let limiter = QueryLimiter::new(2, 0);
        let caller = id(6);
        let source = QuerySource::Connection(9);
        limiter.check(&caller, source).unwrap();
        limiter.check(&caller, source).unwrap();
        assert!(limiter.check(&caller, source).is_err(), "exhausted");
        // OPS-040: raising the rate must not refill the drained bucket.
        limiter.set_rates(1_000, 0);
        assert!(
            limiter.check(&caller, source).is_err(),
            "no free burst on retune"
        );
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
