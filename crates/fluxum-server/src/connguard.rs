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
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use fluxum_core::config::ConnectionLimitsConfig;
pub use fluxum_core::metrics::ConnRejectReason;
use fluxum_core::net::{IpNet, IpSet};

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
    /// Cap on tracked per-IP entries (SEC-040; `None` = unbounded).
    pub max_tracked_ips: Option<u32>,
    /// Load fraction that starts shedding pre-auth connections (SEC-041;
    /// `None` = admission control off).
    pub overload_shed: Option<f64>,
    /// Load fraction that sheds *all* new connections (SEC-041; `None` =
    /// no shed-all stage).
    pub overload_shed_all: Option<f64>,
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
            max_tracked_ips: non_zero_u32(cfg.max_tracked_ips),
            overload_shed: (cfg.overload_shed_fraction > 0.0).then_some(cfg.overload_shed_fraction),
            overload_shed_all: (cfg.overload_shed_all_fraction > 0.0)
                .then_some(cfg.overload_shed_all_fraction),
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

/// The static access lists (SEC-033): who is banned outright and — when the
/// allowlist is non-empty — who alone is admitted. The blocklist wins over
/// an allowlist hit, so an operator can carve exceptions out of an allowed
/// block.
#[derive(Debug, Default)]
struct AccessLists {
    block: IpSet,
    allow: IpSet,
    /// The raw blocklist entries, kept for `GET /admin/bans` listing.
    block_entries: Vec<String>,
}

/// One runtime ban (SEC-033): an entry from `POST /admin/bans`, optionally
/// expiring. Runtime state only — a restart clears it; the static config
/// list is the durable path.
#[derive(Debug, Clone, Copy)]
struct RuntimeBan {
    net: IpNet,
    expires: Option<Instant>,
}

/// A runtime ban as listed by `GET /admin/bans`.
#[derive(Debug, Clone)]
pub struct BanInfo {
    /// The entry as it was banned (IP or CIDR).
    pub entry: String,
    /// Remaining time, or `None` for a ban with no TTL.
    pub remaining: Option<Duration>,
}

/// The per-IP map plus the global live-connection count, one lock: the
/// SEC-034 ceiling check and the per-IP admission must be atomic with the
/// counter they read, or two racing accepts could both squeeze past the
/// last slot.
#[derive(Debug, Default)]
struct GuardState {
    ips: HashMap<IpAddr, IpState>,
    total_active: u32,
}

/// The shared per-IP connection guard (SEC-030/031/033/034). One per
/// server, shared by both transports through the `ShardContext` so the
/// per-IP view is unified across TCP and HTTP.
#[derive(Debug)]
pub struct ConnGuard {
    limits: ConnLimits,
    state: Mutex<GuardState>,
    /// SEC-033 static lists; hot-reloadable via `set_access_lists`.
    access: RwLock<AccessLists>,
    /// SEC-033 runtime bans, keyed by the entry string they were created
    /// with; expired entries are purged lazily on check/list.
    bans: Mutex<HashMap<String, RuntimeBan>>,
    /// SEC-034 global concurrent-connection ceiling (`0` = uncapped);
    /// hot-reloadable.
    max_total_conns: AtomicU32,
    /// SEC-040: lifetime count of pressure-evicted entries, for
    /// `fluxum_connguard_evictions_total`.
    evictions: std::sync::atomic::AtomicU64,
}

impl ConnGuard {
    /// A guard enforcing `limits`, with empty access lists and no global
    /// ceiling until [`ConnGuard::set_access_lists`] /
    /// [`ConnGuard::set_max_total_conns`] install them.
    pub fn new(limits: ConnLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(GuardState::default()),
            access: RwLock::new(AccessLists::default()),
            bans: Mutex::new(HashMap::new()),
            max_total_conns: AtomicU32::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The resolved limits (transports read the handshake budget from here).
    pub fn limits(&self) -> &ConnLimits {
        &self.limits
    }

    fn state(&self) -> std::sync::MutexGuard<'_, GuardState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Install the SEC-033 static lists (boot and every hot reload).
    ///
    /// # Errors
    /// An entry that is not an IP address or CIDR block; nothing is applied.
    pub fn set_access_lists(
        &self,
        blocklist: &[String],
        allowlist: &[String],
    ) -> fluxum_core::Result<()> {
        let block = IpSet::parse(blocklist)?;
        let allow = IpSet::parse(allowlist)?;
        *self
            .access
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = AccessLists {
            block,
            allow,
            block_entries: blocklist.to_vec(),
        };
        Ok(())
    }

    /// Install the SEC-034 global ceiling (`0` = uncapped). Lowering it
    /// never evicts live connections; it only gates new admissions.
    pub fn set_max_total_conns(&self, cap: u32) {
        self.max_total_conns.store(cap, Ordering::Relaxed);
    }

    /// Ban `entry` (IP or CIDR) at runtime (SEC-033), optionally expiring
    /// after `ttl`. Re-banning an entry replaces its TTL. Runtime state
    /// only: a restart clears it, the static blocklist is the durable path.
    ///
    /// # Errors
    /// `entry` is not an IP address or CIDR block.
    pub fn ban(&self, entry: &str, ttl: Option<Duration>) -> fluxum_core::Result<()> {
        let net: IpNet = entry.trim().parse()?;
        let ban = RuntimeBan {
            net,
            expires: ttl.map(|t| Instant::now() + t),
        };
        self.bans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(entry.trim().to_owned(), ban);
        Ok(())
    }

    /// Lift a runtime ban; `true` if the entry existed. Static blocklist
    /// entries cannot be lifted here — they are config, not runtime state.
    pub fn unban(&self, entry: &str) -> bool {
        self.bans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(entry.trim())
            .is_some()
    }

    /// The live runtime bans (expired ones purged), for `GET /admin/bans`.
    pub fn runtime_bans(&self) -> Vec<BanInfo> {
        let now = Instant::now();
        let mut bans = self.bans.lock().unwrap_or_else(|e| e.into_inner());
        bans.retain(|_, ban| ban.expires.is_none_or(|at| now < at));
        let mut out: Vec<BanInfo> = bans
            .iter()
            .map(|(entry, ban)| BanInfo {
                entry: entry.clone(),
                remaining: ban.expires.map(|at| at.saturating_duration_since(now)),
            })
            .collect();
        out.sort_by(|a, b| a.entry.cmp(&b.entry));
        out
    }

    /// The static blocklist entries currently installed, for listing.
    pub fn static_blocklist(&self) -> Vec<String> {
        self.access
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .block_entries
            .clone()
    }

    /// Whether SEC-033 refuses `ip`: statically blocked, runtime-banned
    /// (unexpired), or absent from a non-empty allowlist.
    fn is_blocked(&self, ip: IpAddr, now: Instant) -> bool {
        {
            let access = self
                .access
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if access.block.contains(ip) {
                return true;
            }
            if !access.allow.is_empty() && !access.allow.contains(ip) {
                return true;
            }
        }
        let mut bans = self.bans.lock().unwrap_or_else(|e| e.into_inner());
        bans.retain(|_, ban| ban.expires.is_none_or(|at| now < at));
        bans.values().any(|ban| ban.net.contains(ip))
    }

    /// The current total live-connection count (SEC-034 introspection).
    pub fn total_active(&self) -> u32 {
        self.state().total_active
    }

    /// Decide whether a new connection from `ip` may proceed. On success a
    /// [`ConnPermit`] is returned that holds the peer's concurrent-connection
    /// slot until it drops. The checks run in the order blocklist/allowlist
    /// → global ceiling → backoff → accept-rate → concurrency, so a banned
    /// or flooding peer is turned away before it touches (or allocates)
    /// per-IP state.
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
        // SEC-033: a banned peer is refused before any per-IP state exists —
        // under a many-IP flood the ban check must not be what grows the map.
        if self.is_blocked(ip, now) {
            return Err(ConnRejectReason::Blocked);
        }

        let mut guard_state = self.state();

        // SEC-034: the global ceiling, before per-IP state is touched.
        let cap = self.max_total_conns.load(Ordering::Relaxed);
        if cap != 0 && guard_state.total_active >= cap {
            return Err(ConnRejectReason::GlobalCap);
        }

        // SEC-040: bound the tracked-IP map. A brand-new IP that would push
        // the map past the cap first triggers a pressure sweep; if even that
        // frees nothing (every entry is live or mid-streak), the connection
        // is admitted *untracked* — per-IP checks are skipped for it, but
        // the global ceiling above still applies and, crucially, the guard
        // itself cannot become the OOM vector.
        let ips = &mut guard_state.ips;
        if !ips.contains_key(&ip)
            && let Some(cap) = self.limits.max_tracked_ips
            && ips.len() >= cap as usize
            && Self::evict_under_pressure(ips, now, &self.evictions) == 0
        {
            guard_state.total_active = guard_state.total_active.saturating_add(1);
            return Ok(ConnPermit {
                guard: std::sync::Arc::clone(self),
                ip,
                tracked: false,
            });
        }
        let ips = &mut guard_state.ips;
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
            self.gc_if_idle(ips, ip, now);
            return Err(ConnRejectReason::AcceptRate);
        }

        // SEC-030: concurrent-connection cap.
        if let Some(cap) = self.limits.max_conns_per_ip
            && state.active >= cap
        {
            self.gc_if_idle(ips, ip, now);
            return Err(ConnRejectReason::ConnCap);
        }

        state.active += 1;
        guard_state.total_active = guard_state.total_active.saturating_add(1);
        Ok(ConnPermit {
            guard: std::sync::Arc::clone(self),
            ip,
            tracked: true,
        })
    }

    /// SEC-040 pressure sweep: evict every entry with no live connections,
    /// no failed-auth streak, and no pending backoff — the *relaxed* idle
    /// test (a partially drained accept bucket is forfeited; under memory
    /// pressure, bounded memory beats perfect rate history). One O(n) sweep
    /// reclaims many slots at once, so a sustained distinct-IP flood pays
    /// it rarely, not per connection. Returns how many were evicted.
    ///
    /// What is *never* evicted — entries holding live connections or an
    /// armed/counting failed-auth streak — is exactly what SEC-031 needs
    /// preserved: an attacker cannot reset a brute-force counter by
    /// flooding the guard with strangers.
    fn evict_under_pressure(
        ips: &mut HashMap<IpAddr, IpState>,
        now: Instant,
        evictions: &std::sync::atomic::AtomicU64,
    ) -> usize {
        let before = ips.len();
        ips.retain(|_, s| {
            s.active > 0 || s.auth_failures > 0 || s.backoff_until.is_some_and(|until| now < until)
        });
        let evicted = before - ips.len();
        if evicted > 0 {
            evictions.fetch_add(evicted as u64, std::sync::atomic::Ordering::Relaxed);
        }
        evicted
    }

    /// Lifetime SEC-040 pressure evictions (`fluxum_connguard_evictions_total`).
    pub fn evictions_total(&self) -> u64 {
        self.evictions.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Currently tracked per-IP entries (`fluxum_connguard_tracked_ips`).
    pub fn tracked_ips(&self) -> usize {
        self.state().ips.len()
    }

    /// SEC-041: the admission-control verdict, computed instantaneously
    /// from live load — the highest of `total conns / max_total_conns` and
    /// `tracked IPs / max_tracked_ips` (only configured caps contribute).
    /// Instantaneous by design: the moment a flood stops and connections
    /// drain, the verdict is `Normal` again with no cool-down to wait out.
    pub fn overload_state(&self) -> fluxum_core::metrics::OverloadState {
        use fluxum_core::metrics::OverloadState;
        let Some(shed) = self.limits.overload_shed else {
            return OverloadState::Normal;
        };
        let (total_active, tracked) = {
            let state = self.state();
            (state.total_active, state.ips.len())
        };
        let mut load = 0.0_f64;
        let total_cap = self.max_total_conns.load(Ordering::Relaxed);
        if total_cap != 0 {
            load = load.max(f64::from(total_active) / f64::from(total_cap));
        }
        if let Some(tracked_cap) = self.limits.max_tracked_ips {
            #[allow(clippy::cast_precision_loss)]
            let pressure = tracked as f64 / f64::from(tracked_cap);
            load = load.max(pressure);
        }
        if let Some(shed_all) = self.limits.overload_shed_all
            && load >= shed_all
        {
            return OverloadState::ShedAllNew;
        }
        if load >= shed {
            OverloadState::ShedPreauth
        } else {
            OverloadState::Normal
        }
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
        let mut guard_state = self.state();
        // SEC-040: recording a failure may not grow the map past its cap
        // either. If a pressure sweep frees nothing, the failure goes
        // unrecorded — under an active distinct-IP flood, bounded memory
        // wins over a perfect streak count for a brand-new address.
        let ips = &mut guard_state.ips;
        if !ips.contains_key(&ip)
            && let Some(cap) = self.limits.max_tracked_ips
            && ips.len() >= cap as usize
            && Self::evict_under_pressure(ips, now, &self.evictions) == 0
        {
            return;
        }
        let state = guard_state
            .ips
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
        let mut guard_state = self.state();
        if let Some(state) = guard_state.ips.get_mut(&ip) {
            state.auth_failures = 0;
            state.backoff_until = None;
        }
    }

    /// The current concurrent-connection count for `ip` (tests/introspection).
    pub fn active_conns(&self, ip: IpAddr) -> u32 {
        self.state().ips.get(&ip).map_or(0, |s| s.active)
    }

    fn release(&self, ip: IpAddr, tracked: bool) {
        let mut guard_state = self.state();
        // An untracked permit (SEC-040 saturation) owns no per-IP slot: a
        // tracked entry for the same address created later must not be
        // decremented by a stranger's release.
        if tracked && let Some(state) = guard_state.ips.get_mut(&ip) {
            state.active = state.active.saturating_sub(1);
        }
        guard_state.total_active = guard_state.total_active.saturating_sub(1);
        if tracked {
            let ips = &mut guard_state.ips;
            self.gc_if_idle(ips, ip, Instant::now());
        }
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
    /// `false` when the guard admitted this connection without a per-IP
    /// entry (SEC-040 saturation): the drop then releases only the global
    /// slot, never a stranger's per-IP count.
    tracked: bool,
}

impl ConnPermit {
    /// The peer IP this permit belongs to.
    pub fn ip(&self) -> IpAddr {
        self.ip
    }
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.guard.release(self.ip, self.tracked);
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
            ..ConnectionLimitsConfig::default()
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
    fn a_blocked_ip_is_refused_before_any_state_exists() {
        let g = guard(limits());
        g.set_access_lists(&["10.0.0.0/8".into()], &[]).unwrap();

        assert_eq!(
            g.try_accept(ip_of("10.1.2.3")).unwrap_err(),
            ConnRejectReason::Blocked
        );
        // The ban check allocated nothing: a flood of banned IPs cannot grow
        // the map.
        assert_eq!(g.state().ips.len(), 0);
        // Anyone else still connects.
        g.try_accept(ip(1)).unwrap();
    }

    #[test]
    fn a_non_empty_allowlist_is_exclusive_and_the_blocklist_still_wins() {
        let g = guard(limits());
        g.set_access_lists(&["10.0.0.7".into()], &["10.0.0.0/8".into()])
            .unwrap();

        // In the allowed block: admitted.
        g.try_accept(ip_of("10.0.0.1")).unwrap();
        // Outside the allowlist: refused.
        assert_eq!(
            g.try_accept(ip_of("192.0.2.1")).unwrap_err(),
            ConnRejectReason::Blocked
        );
        // Allowed block but explicitly blocklisted: the ban wins.
        assert_eq!(
            g.try_accept(ip_of("10.0.0.7")).unwrap_err(),
            ConnRejectReason::Blocked
        );
    }

    #[test]
    fn runtime_bans_apply_expire_and_lift() {
        let g = guard(limits());

        g.ban("10.0.0.1", None).unwrap();
        g.ban("192.0.2.0/24", Some(Duration::from_millis(20)))
            .unwrap();
        assert_eq!(
            g.try_accept(ip_of("10.0.0.1")).unwrap_err(),
            ConnRejectReason::Blocked
        );
        assert_eq!(
            g.try_accept(ip_of("192.0.2.9")).unwrap_err(),
            ConnRejectReason::Blocked
        );
        assert_eq!(g.runtime_bans().len(), 2);

        // The TTL ban expires and readmits on its own.
        std::thread::sleep(Duration::from_millis(30));
        g.try_accept(ip_of("192.0.2.9")).unwrap();
        assert_eq!(g.runtime_bans().len(), 1, "the expired ban is purged");

        // Unban readmits; unbanning the unknown reports false.
        assert!(g.unban("10.0.0.1"));
        assert!(!g.unban("10.0.0.1"));
        g.try_accept(ip_of("10.0.0.1")).unwrap();

        // Garbage entries are rejected.
        g.ban("not-an-ip", None).unwrap_err();
    }

    #[test]
    fn the_global_ceiling_caps_across_distinct_ips() {
        let mut l = limits();
        l.accept_rate_per_sec = None;
        let g = guard(l);
        g.set_max_total_conns(2);

        let _p1 = g.try_accept(ip(1)).unwrap();
        let _p2 = g.try_accept(ip(2)).unwrap();
        assert_eq!(g.total_active(), 2);
        // A third connection from a *fresh* IP hits the global ceiling.
        assert_eq!(
            g.try_accept(ip(3)).unwrap_err(),
            ConnRejectReason::GlobalCap
        );

        // Releasing a slot readmits; 0 uncaps entirely.
        drop(_p1);
        let _p3 = g.try_accept(ip(3)).unwrap();
        g.set_max_total_conns(0);
        let _p4 = g.try_accept(ip(4)).unwrap();
        let _p5 = g.try_accept(ip(5)).unwrap();
    }

    #[test]
    fn hot_swapping_the_lists_applies_to_the_next_accept() {
        let g = guard(limits());
        g.try_accept(ip_of("10.0.0.1")).unwrap();
        g.set_access_lists(&["10.0.0.1".into()], &[]).unwrap();
        assert_eq!(
            g.try_accept(ip_of("10.0.0.1")).unwrap_err(),
            ConnRejectReason::Blocked
        );
        g.set_access_lists(&[], &[]).unwrap();
        g.try_accept(ip_of("10.0.0.1")).unwrap();
        // A bad entry applies nothing.
        g.set_access_lists(&["garbage".into()], &[]).unwrap_err();
    }

    fn ip_of(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn pressure_eviction_reclaims_lingering_entries_and_counts() {
        let mut l = limits();
        // A slow bucket (burst 1, ~1 s refill) keeps entries non-idle after
        // release, so they linger for the pressure sweep to find.
        l.accept_rate_per_sec = Some(1.0);
        l.max_conns_per_ip = None;
        l.max_tracked_ips = Some(2);
        let g = guard(l);

        drop(g.try_accept(ip(1)).unwrap());
        drop(g.try_accept(ip(2)).unwrap());
        assert_eq!(
            g.tracked_ips(),
            2,
            "released entries linger (drained buckets)"
        );

        // A third IP hits the cap: the sweep reclaims both idle entries.
        let _p3 = g.try_accept(ip(3)).unwrap();
        assert_eq!(g.evictions_total(), 2);
        assert_eq!(g.tracked_ips(), 1, "only the newcomer remains");
    }

    #[test]
    fn saturation_never_evicts_live_conns_or_armed_streaks() {
        let mut l = limits();
        l.accept_rate_per_sec = None;
        l.max_conns_per_ip = Some(1);
        l.failed_auth_threshold = Some(2);
        l.max_tracked_ips = Some(2);
        let g = guard(l);

        // Slot 1: a live connection. Slot 2: a counting failed-auth streak.
        let p1 = g.try_accept(ip(1)).unwrap();
        g.note_auth_failure(ip(2));
        assert_eq!(g.tracked_ips(), 2);

        // A newcomer finds every entry protected: admitted *untracked*, the
        // map does not grow, and nothing about ip(1)/ip(2) was lost.
        let p3 = g.try_accept(ip(3)).unwrap();
        assert_eq!(g.tracked_ips(), 2, "saturated map does not grow");
        assert_eq!(g.evictions_total(), 0);
        // The untracked release never decrements a stranger's count.
        drop(p3);
        assert_eq!(g.active_conns(ip(1)), 1);
        // SEC-031 preserved: ip(2)'s streak kept counting all along.
        g.note_auth_failure(ip(2));
        assert_eq!(
            g.try_accept(ip(2)).unwrap_err(),
            ConnRejectReason::FailedAuth
        );
        // An unrecordable failure from a fresh IP is dropped, not grown.
        g.note_auth_failure(ip(4));
        assert_eq!(g.tracked_ips(), 2);
        drop(p1);
    }

    #[test]
    fn overload_state_follows_load_and_recovers_instantly() {
        use fluxum_core::metrics::OverloadState;
        let mut l = limits();
        l.accept_rate_per_sec = None;
        l.max_conns_per_ip = None;
        l.overload_shed = Some(0.5);
        l.overload_shed_all = Some(0.9);
        let g = guard(l);
        g.set_max_total_conns(10);

        assert_eq!(g.overload_state(), OverloadState::Normal);
        let permits: Vec<_> = (0u8..5).map(|i| g.try_accept(ip(i)).unwrap()).collect();
        assert_eq!(g.overload_state(), OverloadState::ShedPreauth);
        let more: Vec<_> = (5u8..9).map(|i| g.try_accept(ip(i)).unwrap()).collect();
        assert_eq!(g.overload_state(), OverloadState::ShedAllNew);

        // The signal is instantaneous: drained load is Normal load.
        drop(more);
        assert_eq!(g.overload_state(), OverloadState::ShedPreauth);
        drop(permits);
        assert_eq!(g.overload_state(), OverloadState::Normal);

        // No overload_shed configured → always Normal.
        let mut off = limits();
        off.overload_shed = None;
        let g = guard(off);
        g.set_max_total_conns(1);
        let _p = g.try_accept(ip(1)).unwrap();
        assert_eq!(
            g.overload_state(),
            fluxum_core::metrics::OverloadState::Normal
        );
    }

    #[test]
    fn idle_ip_entries_are_reclaimed() {
        let mut l = limits();
        l.accept_rate_per_sec = None;
        l.max_conns_per_ip = Some(4);
        let g = guard(l);
        {
            let _p = g.try_accept(ip(1)).unwrap();
            assert_eq!(g.state().ips.len(), 1);
        }
        // Permit dropped, entry has nothing live/pending → reclaimed.
        assert_eq!(g.active_conns(ip(1)), 0);
        assert_eq!(g.state().ips.len(), 0, "a fully idle IP is forgotten");
    }
}
