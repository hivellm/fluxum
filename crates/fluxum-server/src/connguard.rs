//! Pre-auth connection-abuse protection (SPEC-026 §4, SEC-030/031): the
//! shared per-IP tracker both transports gate on before a session exists.
//!
//! This defends the surface the reducer rate limiter cannot see. That limiter
//! (`fluxum_core::reducer::ratelimit`) keys buckets on `(Identity, reducer)`
//! and so only bites *after* a caller has authenticated; nothing there caps an
//! unauthenticated flood of TCP/HTTP connections, an `Authenticate`
//! brute-force, or a slowloris that never finishes its handshake. The guard
//! sits one layer out, keyed by peer IP:
//!
//! - **Concurrent-connection cap** (SEC-030) — a [`ConnPermit`] is held for a
//!   connection's whole life and decrements the peer's count on drop, so the
//!   cap tracks live connections without a sweeper.
//! - **Accept-rate limit** (SEC-030) — a per-IP token bucket, so a burst of
//!   short-lived connect/disconnect churn is throttled even though none of
//!   them is concurrent.
//! - **Failed-auth backoff** (SEC-031) — consecutive bad `Authenticate`s from
//!   an IP arm an exponential backoff that refuses that IP's *next* connection
//!   attempts; a success clears it. Gating the next connection (not the live
//!   one) is what contains a brute-force that reconnects per guess.
//!
//! The guard never touches metrics itself — [`ConnGuard::try_accept`] returns
//! the [`ConnRejectReason`] and the transport records it against its shard's
//! `fluxum_conn_rejected_total` (SEC-032), keeping this module free of a
//! `ShardContext` dependency and unit-testable in isolation.
//!
//! Every limit is opt-out at `0` and defaults permissively (see
//! [`fluxum_core::config::ConnectionLimitsConfig`]); a guard built from the
//! defaults leaves a normal deployment unaffected.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use fluxum_core::config::ConnectionLimitsConfig;
pub use fluxum_core::metrics::ConnRejectReason;

/// The resolved limits a [`ConnGuard`] enforces, in native units. Built from
/// the serde [`ConnectionLimitsConfig`]; a `0`/absent limit becomes `None`.
#[derive(Debug, Clone, Copy)]
pub struct ConnLimits {
    /// Concurrent connections per IP (`None` = uncapped).
    pub max_conns_per_ip: Option<u32>,
    /// Accept rate per IP, connections/sec, with an equal burst (`None` =
    /// unlimited).
    pub accept_rate_per_sec: Option<f64>,
    /// Handshake time budget (`None` = no deadline beyond the idle timeout).
    pub handshake_timeout: Option<Duration>,
    /// Pre-auth frame size cap (`None` = fall back to `max_frame_bytes`).
    pub handshake_max_bytes: Option<u32>,
    /// Consecutive failed `Authenticate`s before backoff (`None` = no
    /// throttle).
    pub failed_auth_threshold: Option<u32>,
    /// Base failed-auth backoff, doubled per failure past the threshold.
    pub failed_auth_backoff_base: Duration,
    /// Ceiling for the failed-auth backoff.
    pub failed_auth_backoff_max: Duration,
}

impl ConnLimits {
    /// Resolve from config: a `0` disables the corresponding limit.
    pub fn from_config(cfg: &ConnectionLimitsConfig) -> Self {
        let non_zero_u32 = |v: u32| (v != 0).then_some(v);
        Self {
            max_conns_per_ip: non_zero_u32(cfg.max_conns_per_ip),
            accept_rate_per_sec: (cfg.accept_rate_per_sec > 0.0).then_some(cfg.accept_rate_per_sec),
            handshake_timeout: (cfg.handshake_timeout_secs != 0)
                .then(|| Duration::from_secs(cfg.handshake_timeout_secs)),
            handshake_max_bytes: u32::try_from(cfg.handshake_max_bytes.0)
                .ok()
                .filter(|v| *v != 0),
            failed_auth_threshold: non_zero_u32(cfg.failed_auth_threshold),
            failed_auth_backoff_base: Duration::from_millis(cfg.failed_auth_backoff_base_ms),
            failed_auth_backoff_max: Duration::from_millis(cfg.failed_auth_backoff_max_ms),
        }
    }
}

impl Default for ConnLimits {
    fn default() -> Self {
        Self::from_config(&ConnectionLimitsConfig::default())
    }
}

/// A classic token bucket (mirrors the reducer limiter's): capacity `burst`,
/// refilling at `rate` tokens/sec.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    burst: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: f64, now: Instant) -> Self {
        Self {
            tokens: rate,
            burst: rate,
            refill_per_sec: rate,
            last: now,
        }
    }

    /// Try to spend one token, refilling for elapsed time first.
    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// One peer IP's live state. Dropped from the map once it is fully idle (no
/// live connections, no pending backoff, a full accept bucket) so the map
/// tracks only active/abusive peers, not every IP ever seen.
#[derive(Debug)]
struct IpState {
    active: u32,
    accept: Option<TokenBucket>,
    auth_failures: u32,
    backoff_until: Option<Instant>,
}

impl IpState {
    fn new(limits: &ConnLimits, now: Instant) -> Self {
        Self {
            active: 0,
            accept: limits.accept_rate_per_sec.map(|r| TokenBucket::new(r, now)),
            auth_failures: 0,
            backoff_until: None,
        }
    }

    /// Whether this entry can be forgotten: nothing live, no failure streak
    /// still counting, no pending backoff, and a replenished accept bucket —
    /// so dropping it loses no rate history and, crucially, no in-progress
    /// brute-force count (SEC-031). Reclaiming an entry mid-streak would let
    /// an attacker reset its failed-auth counter simply by disconnecting
    /// between guesses.
    fn is_idle(&self, now: Instant) -> bool {
        self.active == 0
            && self.auth_failures == 0
            && self.backoff_until.is_none_or(|until| now >= until)
            && self
                .accept
                .as_ref()
                .is_none_or(|b| b.tokens >= b.burst - f64::EPSILON)
    }
}

/// The shared per-IP connection guard (SEC-030/031). One per server, shared
/// by both transports through the `ShardContext` so the per-IP view is
/// unified across TCP and HTTP.
#[derive(Debug)]
pub struct ConnGuard {
    limits: ConnLimits,
    ips: Mutex<HashMap<IpAddr, IpState>>,
}

impl ConnGuard {
    /// A guard enforcing `limits`.
    pub fn new(limits: ConnLimits) -> Self {
        Self {
            limits,
            ips: Mutex::new(HashMap::new()),
        }
    }

    /// The resolved limits (transports read the handshake budget from here).
    pub fn limits(&self) -> &ConnLimits {
        &self.limits
    }

    fn ips(&self) -> std::sync::MutexGuard<'_, HashMap<IpAddr, IpState>> {
        self.ips.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Decide whether a new connection from `ip` may proceed. On success a
    /// [`ConnPermit`] is returned that holds the peer's concurrent-connection
    /// slot until it drops. The checks run in the order backoff → accept-rate
    /// → concurrency so a throttled or flooding peer is turned away before it
    /// consumes a slot.
    pub fn try_accept(
        self: &std::sync::Arc<Self>,
        ip: IpAddr,
    ) -> Result<ConnPermit, ConnRejectReason> {
        self.try_accept_at(ip, Instant::now())
    }

    fn try_accept_at(
        self: &std::sync::Arc<Self>,
        ip: IpAddr,
        now: Instant,
    ) -> Result<ConnPermit, ConnRejectReason> {
        let mut ips = self.ips();
        let state = ips
            .entry(ip)
            .or_insert_with(|| IpState::new(&self.limits, now));

        // SEC-031: an IP in failed-auth backoff is refused outright.
        if let Some(until) = state.backoff_until {
            if now < until {
                return Err(ConnRejectReason::FailedAuth);
            }
            state.backoff_until = None;
        }

        // SEC-030: accept-rate limit.
        if let Some(bucket) = state.accept.as_mut()
            && !bucket.try_take(now)
        {
            self.gc_if_idle(&mut ips, ip, now);
            return Err(ConnRejectReason::AcceptRate);
        }

        // SEC-030: concurrent-connection cap.
        if let Some(cap) = self.limits.max_conns_per_ip
            && state.active >= cap
        {
            self.gc_if_idle(&mut ips, ip, now);
            return Err(ConnRejectReason::ConnCap);
        }

        state.active += 1;
        Ok(ConnPermit {
            guard: std::sync::Arc::clone(self),
            ip,
        })
    }

    /// Record a failed `Authenticate` from `ip` (SEC-031): once the
    /// consecutive count reaches the threshold, arm exponential backoff so the
    /// peer's next connection attempts are refused for a growing window.
    pub fn note_auth_failure(&self, ip: IpAddr) {
        self.note_auth_failure_at(ip, Instant::now());
    }

    fn note_auth_failure_at(&self, ip: IpAddr, now: Instant) {
        let Some(threshold) = self.limits.failed_auth_threshold else {
            return;
        };
        let mut ips = self.ips();
        let state = ips
            .entry(ip)
            .or_insert_with(|| IpState::new(&self.limits, now));
        state.auth_failures = state.auth_failures.saturating_add(1);
        if state.auth_failures >= threshold {
            // Exponential in the overshoot past the threshold, capped. Uses
            // shifts on a saturating u32 so a wild failure count cannot
            // overflow the backoff computation.
            let steps = state.auth_failures - threshold;
            let factor = 1u64.checked_shl(steps.min(32)).unwrap_or(u64::MAX);
            let backoff = self
                .limits
                .failed_auth_backoff_base
                .saturating_mul(u32::try_from(factor).unwrap_or(u32::MAX))
                .min(self.limits.failed_auth_backoff_max);
            state.backoff_until = Some(now + backoff);
        }
    }

    /// Record a successful `Authenticate` from `ip` (SEC-031): clears the
    /// failure streak and any backoff, so a legitimate client that mistyped a
    /// token once is not punished after it gets in.
    pub fn note_auth_success(&self, ip: IpAddr) {
        let mut ips = self.ips();
        if let Some(state) = ips.get_mut(&ip) {
            state.auth_failures = 0;
            state.backoff_until = None;
        }
    }

    /// The current concurrent-connection count for `ip` (tests/introspection).
    pub fn active_conns(&self, ip: IpAddr) -> u32 {
        self.ips().get(&ip).map_or(0, |s| s.active)
    }

    fn release(&self, ip: IpAddr) {
        let mut ips = self.ips();
        if let Some(state) = ips.get_mut(&ip) {
            state.active = state.active.saturating_sub(1);
        }
        self.gc_if_idle(&mut ips, ip, Instant::now());
    }

    fn gc_if_idle(&self, ips: &mut HashMap<IpAddr, IpState>, ip: IpAddr, now: Instant) {
        if ips.get(&ip).is_some_and(|s| s.is_idle(now)) {
            ips.remove(&ip);
        }
    }
}

/// A held concurrent-connection slot for one peer IP (SEC-030). Returned by
/// [`ConnGuard::try_accept`] and dropped when the connection ends, releasing
/// the slot — so the cap needs no separate cleanup path.
#[derive(Debug)]
pub struct ConnPermit {
    guard: std::sync::Arc<ConnGuard>,
    ip: IpAddr,
}

impl ConnPermit {
    /// The peer IP this permit belongs to.
    pub fn ip(&self) -> IpAddr {
        self.ip
    }
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.guard.release(self.ip);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::net::Ipv4Addr;
    use std::sync::Arc;

    use super::*;

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, last))
    }

    fn guard(limits: ConnLimits) -> Arc<ConnGuard> {
        Arc::new(ConnGuard::new(limits))
    }

    fn limits() -> ConnLimits {
        ConnLimits::from_config(&ConnectionLimitsConfig::default())
    }

    #[test]
    fn a_permit_holds_a_slot_until_it_drops() {
        let mut l = limits();
        l.max_conns_per_ip = Some(2);
        l.accept_rate_per_sec = None; // isolate the concurrency cap
        let g = guard(l);

        let p1 = g.try_accept(ip(1)).unwrap();
        let p2 = g.try_accept(ip(1)).unwrap();
        assert_eq!(g.active_conns(ip(1)), 2);
        // Third concurrent connection from the same IP is refused.
        assert_eq!(g.try_accept(ip(1)).unwrap_err(), ConnRejectReason::ConnCap);
        // A different IP is unaffected.
        let _other = g.try_accept(ip(9)).unwrap();

        drop(p1);
        assert_eq!(g.active_conns(ip(1)), 1);
        // A slot freed, so a new one is admitted.
        let _p3 = g.try_accept(ip(1)).unwrap();
        drop(p2);
    }

    #[test]
    fn the_accept_rate_bucket_throttles_a_connect_churn() {
        let mut l = limits();
        l.accept_rate_per_sec = Some(3.0); // burst 3
        l.max_conns_per_ip = None;
        let g = guard(l);
        let now = Instant::now();

        // Burst of 3 is admitted, the 4th within the same instant is not.
        for _ in 0..3 {
            g.try_accept_at(ip(1), now).unwrap();
        }
        assert_eq!(
            g.try_accept_at(ip(1), now).unwrap_err(),
            ConnRejectReason::AcceptRate
        );
        // One second later a token has refilled.
        g.try_accept_at(ip(1), now + Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn failed_auth_arms_exponential_backoff_that_a_success_clears() {
        let mut l = limits();
        l.failed_auth_threshold = Some(3);
        l.failed_auth_backoff_base = Duration::from_millis(100);
        l.failed_auth_backoff_max = Duration::from_secs(10);
        l.accept_rate_per_sec = None;
        let g = guard(l);
        let t0 = Instant::now();

        // Under the threshold: connections still flow.
        g.note_auth_failure_at(ip(1), t0);
        g.note_auth_failure_at(ip(1), t0);
        g.try_accept_at(ip(1), t0).unwrap();

        // Crossing the threshold arms backoff; the next accept is refused.
        g.note_auth_failure_at(ip(1), t0);
        assert_eq!(
            g.try_accept_at(ip(1), t0).unwrap_err(),
            ConnRejectReason::FailedAuth
        );
        // 100 ms base backoff has elapsed → admitted again.
        g.try_accept_at(ip(1), t0 + Duration::from_millis(150))
            .unwrap();

        // One more failure doubles the window (200 ms): 150 ms is not enough.
        let t1 = t0 + Duration::from_secs(1);
        g.note_auth_failure_at(ip(1), t1);
        assert_eq!(
            g.try_accept_at(ip(1), t1 + Duration::from_millis(150))
                .unwrap_err(),
            ConnRejectReason::FailedAuth
        );

        // A success clears the streak and the backoff immediately.
        g.note_auth_success(ip(1));
        g.try_accept_at(ip(1), t1 + Duration::from_millis(151))
            .unwrap();
    }

    #[test]
    fn a_zeroed_limit_disables_that_check() {
        let l = ConnLimits::from_config(&ConnectionLimitsConfig {
            max_conns_per_ip: 0,
            accept_rate_per_sec: 0.0,
            handshake_timeout_secs: 0,
            handshake_max_bytes: fluxum_core::config::ByteSize(0),
            failed_auth_threshold: 0,
            failed_auth_backoff_base_ms: 100,
            failed_auth_backoff_max_ms: 1000,
        });
        assert!(l.max_conns_per_ip.is_none());
        assert!(l.accept_rate_per_sec.is_none());
        assert!(l.handshake_timeout.is_none());
        assert!(l.handshake_max_bytes.is_none());
        assert!(l.failed_auth_threshold.is_none());

        let g = guard(l);
        // No cap and no rate limit: many connections from one IP all pass,
        // and no failed-auth throttle ever arms.
        let mut permits = Vec::new();
        for _ in 0..1000 {
            permits.push(g.try_accept(ip(1)).unwrap());
        }
        for _ in 0..100 {
            g.note_auth_failure(ip(1));
        }
        g.try_accept(ip(1)).unwrap();
    }

    #[test]
    fn idle_ip_entries_are_reclaimed() {
        let mut l = limits();
        l.accept_rate_per_sec = None;
        l.max_conns_per_ip = Some(4);
        let g = guard(l);
        {
            let _p = g.try_accept(ip(1)).unwrap();
            assert_eq!(g.ips().len(), 1);
        }
        // Permit dropped, entry has nothing live/pending → reclaimed.
        assert_eq!(g.active_conns(ip(1)), 0);
        assert_eq!(g.ips().len(), 0, "a fully idle IP is forgotten");
    }
}
