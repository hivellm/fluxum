//! SPEC-012 observability — the shard-scoped `fluxum_*` metrics registry
//! (T5.6, OBS-010..OBS-051).
//!
//! One [`Metrics`] lives per shard (created by [`ReducerEngine`], shared as
//! an `Arc`). The reducer/transaction counters are recorded on the core
//! hot path; the subscription/fan-out and connection counters are recorded
//! by the server transport against the same `Arc`. [`Metrics::prometheus`]
//! renders the block this shard owns in Prometheus text exposition format
//! (OBS-001/002); the admin transport appends the storage- and
//! plugin-derived sections.
//!
//! [`ReducerEngine`]: crate::reducer::ReducerEngine

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// OBS-011 reducer-latency histogram bucket upper bounds, in microseconds.
/// Exactly the boundaries SPEC-012 acceptance 3 pins.
pub const REDUCER_DURATION_BUCKETS_US: [u64; 9] =
    [50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000];

/// The default slow-reducer WARN threshold (OBS-072): 5 ms.
pub const DEFAULT_SLOW_REDUCER_THRESHOLD_US: u64 = 5000;

/// OBS-010 reducer call outcome label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducerOutcome {
    /// The call committed.
    Ok,
    /// The reducer returned `Err` or panicked (rolled back).
    Err,
    /// Rejected by the admission rate limiter (RED-050/052).
    RateLimited,
    /// Rejected because the shard's writer queue was full (TXN-011).
    QueueFull,
}

impl ReducerOutcome {
    /// The Prometheus `outcome` label value.
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
            Self::RateLimited => "rate_limited",
            Self::QueueFull => "queue_full",
        }
    }
}

/// OBS-022 subscriber-drop reason label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// SUB-042 slow consumer: the per-client send buffer was full.
    BufferFull,
    /// The connection idled past its timeout.
    IdleTimeout,
    /// A `TxUpdate` exceeded the frame size limit (SPEC-006).
    FrameTooLarge,
}

impl DropReason {
    /// The Prometheus `reason` label value.
    pub fn label(self) -> &'static str {
        match self {
            Self::BufferFull => "buffer_full",
            Self::IdleTimeout => "idle_timeout",
            Self::FrameTooLarge => "frame_too_large",
        }
    }
}

/// OBS-050 shard lifecycle state (the `fluxum_shard_state` gauge value and
/// the `/health` `state` string).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Booting; not yet serving.
    Starting = 0,
    /// Replaying the commit log.
    Recovering = 1,
    /// Serving normally.
    Ready = 2,
    /// Draining for shutdown.
    ShuttingDown = 3,
}

impl ShardState {
    /// The `/health` state string (OBS-060).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::ShuttingDown => "shutting_down",
        }
    }

    fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Starting,
            1 => Self::Recovering,
            3 => Self::ShuttingDown,
            _ => Self::Ready,
        }
    }
}

/// Per-reducer counters (OBS-010) plus the latency histogram (OBS-011).
#[derive(Debug, Default, Clone)]
struct ReducerStat {
    ok: u64,
    err: u64,
    rate_limited: u64,
    queue_full: u64,
    /// Non-cumulative counts, one per [`REDUCER_DURATION_BUCKETS_US`] bound.
    buckets: [u64; 9],
    /// Observations above the largest bound.
    over: u64,
    sum_us: u64,
    count: u64,
}

impl ReducerStat {
    fn record(&mut self, outcome: ReducerOutcome, duration_us: u64) {
        match outcome {
            ReducerOutcome::Ok => self.ok += 1,
            ReducerOutcome::Err => self.err += 1,
            ReducerOutcome::RateLimited => self.rate_limited += 1,
            ReducerOutcome::QueueFull => self.queue_full += 1,
        }
        let mut placed = false;
        for (i, bound) in REDUCER_DURATION_BUCKETS_US.iter().enumerate() {
            if duration_us <= *bound {
                self.buckets[i] += 1;
                placed = true;
                break;
            }
        }
        if !placed {
            self.over += 1;
        }
        self.sum_us += duration_us;
        self.count += 1;
    }
}

/// A shard's live `fluxum_*` counters (SPEC-012). Cheap atomic increments on
/// the hot path; the per-reducer map is behind a mutex touched only at call
/// admission (off the single-writer commit path).
#[derive(Debug)]
pub struct Metrics {
    shard_id: u32,
    reducers: Mutex<BTreeMap<String, ReducerStat>>,
    tx_commits: AtomicU64,
    tx_rollbacks: AtomicU64,
    queue_depth: AtomicU64,
    slow_reducer_threshold_us: AtomicU64,
    shard_state: AtomicU8,
    recovered_tx_id: AtomicU64,
    subscriptions_active: AtomicI64,
    fanout_messages: AtomicU64,
    fanout_rows: AtomicU64,
    drops_buffer_full: AtomicU64,
    drops_idle: AtomicU64,
    drops_frame_too_large: AtomicU64,
    connections_active: AtomicI64,
    connections_total: AtomicU64,
    auth_success: AtomicU64,
    auth_failure: AtomicU64,
    conn_rejected_conn_cap: AtomicU64,
    conn_rejected_accept_rate: AtomicU64,
    conn_rejected_failed_auth: AtomicU64,
    conn_rejected_handshake_budget: AtomicU64,
    conn_rejected_proxy_preamble: AtomicU64,
    conn_rejected_proxy_header: AtomicU64,
    conn_rejected_blocked: AtomicU64,
    conn_rejected_global_cap: AtomicU64,
}

/// Why the transports refused a connection on the pre-auth surface
/// (SPEC-026 SEC-032) — the `reason` label of
/// `fluxum_conn_rejected_total{shard, reason}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnRejectReason {
    /// The peer IP was at its concurrent-connection cap (SEC-030).
    ConnCap,
    /// The peer IP exceeded its connection accept rate (SEC-030).
    AcceptRate,
    /// The peer IP is in failed-`Authenticate` backoff (SEC-031).
    FailedAuth,
    /// The handshake blew its time or size budget (SEC-031, slowloris).
    HandshakeBudget,
    /// A PROXY protocol preamble from an untrusted peer, or a malformed one
    /// from a trusted proxy (SEC-036).
    ProxyPreamble,
    /// A malformed `X-Forwarded-For` from a trusted proxy (SEC-035).
    ProxyHeader,
    /// The resolved client IP is banned — statically configured, runtime
    /// ban, or absent from a non-empty allowlist (SEC-033).
    Blocked,
    /// The global concurrent-connection ceiling is full (SEC-034).
    GlobalCap,
}

impl ConnRejectReason {
    /// The metric `reason` label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConnCap => "conn_cap",
            Self::AcceptRate => "accept_rate",
            Self::FailedAuth => "failed_auth",
            Self::HandshakeBudget => "handshake_budget",
            Self::ProxyPreamble => "proxy_preamble",
            Self::ProxyHeader => "proxy_header",
            Self::Blocked => "blocked",
            Self::GlobalCap => "global_cap",
        }
    }

    /// Every reason, so `/metrics` emits a zero series per label rather than
    /// have one first appear only when it fires.
    pub const ALL: [Self; 8] = [
        Self::ConnCap,
        Self::AcceptRate,
        Self::FailedAuth,
        Self::HandshakeBudget,
        Self::ProxyPreamble,
        Self::ProxyHeader,
        Self::Blocked,
        Self::GlobalCap,
    ];
}

impl Metrics {
    /// A fresh registry for `shard_id`, starting in [`ShardState::Ready`]
    /// with the default slow-reducer threshold (OBS-072).
    pub fn new(shard_id: u32) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            reducers: Mutex::new(BTreeMap::new()),
            tx_commits: AtomicU64::new(0),
            tx_rollbacks: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            slow_reducer_threshold_us: AtomicU64::new(DEFAULT_SLOW_REDUCER_THRESHOLD_US),
            shard_state: AtomicU8::new(ShardState::Ready as u8),
            recovered_tx_id: AtomicU64::new(0),
            subscriptions_active: AtomicI64::new(0),
            fanout_messages: AtomicU64::new(0),
            fanout_rows: AtomicU64::new(0),
            drops_buffer_full: AtomicU64::new(0),
            drops_idle: AtomicU64::new(0),
            drops_frame_too_large: AtomicU64::new(0),
            connections_active: AtomicI64::new(0),
            connections_total: AtomicU64::new(0),
            auth_success: AtomicU64::new(0),
            auth_failure: AtomicU64::new(0),
            conn_rejected_conn_cap: AtomicU64::new(0),
            conn_rejected_accept_rate: AtomicU64::new(0),
            conn_rejected_failed_auth: AtomicU64::new(0),
            conn_rejected_handshake_budget: AtomicU64::new(0),
            conn_rejected_proxy_preamble: AtomicU64::new(0),
            conn_rejected_proxy_header: AtomicU64::new(0),
            conn_rejected_blocked: AtomicU64::new(0),
            conn_rejected_global_cap: AtomicU64::new(0),
        })
    }

    /// The shard this registry belongs to.
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    fn reducers(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, ReducerStat>> {
        self.reducers.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record one `ReducerCall` outcome and its duration (OBS-010/011).
    pub fn record_reducer(&self, reducer: &str, outcome: ReducerOutcome, duration_us: u64) {
        let mut map = self.reducers();
        map.entry(reducer.to_owned())
            .or_default()
            .record(outcome, duration_us);
    }

    /// OBS-013: a transaction committed on this shard.
    pub fn note_commit(&self) {
        self.tx_commits.fetch_add(1, Ordering::Relaxed);
    }

    /// OBS-013: a transaction rolled back on this shard.
    pub fn note_rollback(&self) {
        self.tx_rollbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// OBS-012: publish the shard's pending-`ReducerCall` queue depth.
    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// The OBS-072 slow-reducer WARN threshold (µs).
    pub fn slow_reducer_threshold_us(&self) -> u64 {
        self.slow_reducer_threshold_us.load(Ordering::Relaxed)
    }

    /// Set the OBS-072 slow-reducer WARN threshold (µs).
    pub fn set_slow_reducer_threshold_us(&self, threshold_us: u64) {
        self.slow_reducer_threshold_us
            .store(threshold_us, Ordering::Relaxed);
    }

    /// Whether `duration_us` exceeds the slow-reducer threshold (OBS-072).
    pub fn is_slow(&self, duration_us: u64) -> bool {
        duration_us > self.slow_reducer_threshold_us()
    }

    /// OBS-050: set the shard lifecycle state gauge.
    pub fn set_shard_state(&self, state: ShardState) {
        self.shard_state.store(state as u8, Ordering::Relaxed);
    }

    /// The current shard lifecycle state (OBS-050, `/health`).
    pub fn shard_state(&self) -> ShardState {
        ShardState::from_u8(self.shard_state.load(Ordering::Relaxed))
    }

    /// OBS-050: the last tx id replayed during recovery.
    pub fn set_recovered_tx_id(&self, tx_id: u64) {
        self.recovered_tx_id.store(tx_id, Ordering::Relaxed);
    }

    /// OBS-020: set the active-subscription gauge (refreshed at scrape time
    /// from the subscription manager's live plan count).
    pub fn set_subscriptions_active(&self, count: i64) {
        self.subscriptions_active.store(count, Ordering::Relaxed);
    }

    /// OBS-021: one `TxUpdate` was delivered carrying `rows` insert+delete
    /// rows.
    pub fn note_fanout(&self, rows: u64) {
        self.fanout_messages.fetch_add(1, Ordering::Relaxed);
        self.fanout_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// OBS-022: a subscriber was dropped.
    pub fn note_drop(&self, reason: DropReason) {
        match reason {
            DropReason::BufferFull => &self.drops_buffer_full,
            DropReason::IdleTimeout => &self.drops_idle,
            DropReason::FrameTooLarge => &self.drops_frame_too_large,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// OBS-040: a client connected.
    pub fn note_connect(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// OBS-040: a client disconnected.
    pub fn note_disconnect(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// OBS-040: the current active-connection count (`/health`).
    pub fn connections_active(&self) -> u64 {
        u64::try_from(self.connections_active.load(Ordering::Relaxed).max(0)).unwrap_or(0)
    }

    /// OBS-040: an authentication attempt resolved.
    pub fn note_auth(&self, success: bool) {
        if success {
            &self.auth_success
        } else {
            &self.auth_failure
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// SEC-032: the transports refused a connection on the pre-auth surface.
    pub fn note_conn_rejected(&self, reason: ConnRejectReason) {
        self.conn_rejected_counter(reason)
            .fetch_add(1, Ordering::Relaxed);
    }

    /// The current reject count for `reason`.
    pub fn conn_rejected(&self, reason: ConnRejectReason) -> u64 {
        self.conn_rejected_counter(reason).load(Ordering::Relaxed)
    }

    fn conn_rejected_counter(&self, reason: ConnRejectReason) -> &AtomicU64 {
        match reason {
            ConnRejectReason::ConnCap => &self.conn_rejected_conn_cap,
            ConnRejectReason::AcceptRate => &self.conn_rejected_accept_rate,
            ConnRejectReason::FailedAuth => &self.conn_rejected_failed_auth,
            ConnRejectReason::HandshakeBudget => &self.conn_rejected_handshake_budget,
            ConnRejectReason::ProxyPreamble => &self.conn_rejected_proxy_preamble,
            ConnRejectReason::ProxyHeader => &self.conn_rejected_proxy_header,
            ConnRejectReason::Blocked => &self.conn_rejected_blocked,
            ConnRejectReason::GlobalCap => &self.conn_rejected_global_cap,
        }
    }

    /// Render the `fluxum_*` block this shard owns (OBS-010..OBS-050) in
    /// Prometheus text exposition format. `last_tx_id` is the shard's last
    /// committed transaction (the admin transport already holds it).
    /// [`Metrics::prometheus`] with a `namespace` label added to every series
    /// — per-namespace attribution for a multi-tenant process (SPEC-025
    /// OPS-051).
    ///
    /// Implemented by relabelling the standard exposition rather than
    /// duplicating it: every series here starts its label set with
    /// `{shard="N"`, so inserting the namespace right after it is exact for
    /// both the bare `{shard="N"}` and the multi-label
    /// `{shard="N", reason="…"}` forms, and the two renderings can never
    /// drift apart as series are added.
    pub fn prometheus_in_namespace(&self, namespace: &str, last_tx_id: u64) -> String {
        let bare = format!("{{shard=\"{}\"", self.shard_id);
        let labelled = format!("{{shard=\"{}\", namespace=\"{namespace}\"", self.shard_id);
        self.prometheus(last_tx_id).replace(&bare, &labelled)
    }

    pub fn prometheus(&self, last_tx_id: u64) -> String {
        let shard = self.shard_id;
        let mut out = String::with_capacity(2048);

        // --- shard gauges (OBS-050) ---
        let _ = writeln!(
            out,
            "# HELP fluxum_up Whether the shard is serving.\n\
             # TYPE fluxum_up gauge\n\
             fluxum_up{{shard=\"{shard}\"}} 1\n\
             # HELP fluxum_shard_state Shard lifecycle: 0 starting, 1 recovering, 2 ready, 3 shutting_down.\n\
             # TYPE fluxum_shard_state gauge\n\
             fluxum_shard_state{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_shard_recovered_tx_id Last tx id replayed during recovery.\n\
             # TYPE fluxum_shard_recovered_tx_id gauge\n\
             fluxum_shard_recovered_tx_id{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_shard_last_tx_id Last committed transaction id.\n\
             # TYPE fluxum_shard_last_tx_id gauge\n\
             fluxum_shard_last_tx_id{{shard=\"{shard}\"}} {last_tx_id}",
            self.shard_state.load(Ordering::Relaxed),
            self.recovered_tx_id.load(Ordering::Relaxed),
        );

        // --- reducer counters + latency histogram (OBS-010/011) ---
        let reducers = self.reducers().clone();
        out.push_str(
            "# HELP fluxum_reducer_calls_total Reducer calls by outcome.\n\
             # TYPE fluxum_reducer_calls_total counter\n",
        );
        for (name, stat) in &reducers {
            for (outcome, value) in [
                ("ok", stat.ok),
                ("err", stat.err),
                ("rate_limited", stat.rate_limited),
                ("queue_full", stat.queue_full),
            ] {
                let _ = writeln!(
                    out,
                    "fluxum_reducer_calls_total{{shard=\"{shard}\",reducer=\"{name}\",outcome=\"{outcome}\"}} {value}",
                );
            }
        }
        out.push_str(
            "# HELP fluxum_reducer_duration_us Reducer invocation-to-commit latency.\n\
             # TYPE fluxum_reducer_duration_us histogram\n",
        );
        for (name, stat) in &reducers {
            let mut cumulative = 0u64;
            for (i, bound) in REDUCER_DURATION_BUCKETS_US.iter().enumerate() {
                cumulative += stat.buckets[i];
                let _ = writeln!(
                    out,
                    "fluxum_reducer_duration_us_bucket{{shard=\"{shard}\",reducer=\"{name}\",le=\"{bound}\"}} {cumulative}",
                );
            }
            cumulative += stat.over;
            let _ = writeln!(
                out,
                "fluxum_reducer_duration_us_bucket{{shard=\"{shard}\",reducer=\"{name}\",le=\"+Inf\"}} {cumulative}\n\
                 fluxum_reducer_duration_us_sum{{shard=\"{shard}\",reducer=\"{name}\"}} {}\n\
                 fluxum_reducer_duration_us_count{{shard=\"{shard}\",reducer=\"{name}\"}} {}",
                stat.sum_us, stat.count,
            );
        }

        // --- transaction counters (OBS-013) ---
        let _ = writeln!(
            out,
            "# HELP fluxum_tx_commits_total Committed transactions.\n\
             # TYPE fluxum_tx_commits_total counter\n\
             fluxum_tx_commits_total{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_tx_rollbacks_total Rolled-back transactions.\n\
             # TYPE fluxum_tx_rollbacks_total counter\n\
             fluxum_tx_rollbacks_total{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_reducer_queue_depth Pending ReducerCall messages.\n\
             # TYPE fluxum_reducer_queue_depth gauge\n\
             fluxum_reducer_queue_depth{{shard=\"{shard}\"}} {}",
            self.tx_commits.load(Ordering::Relaxed),
            self.tx_rollbacks.load(Ordering::Relaxed),
            self.queue_depth.load(Ordering::Relaxed),
        );

        // --- subscription / fan-out (OBS-020/021/022) ---
        let _ = writeln!(
            out,
            "# HELP fluxum_subscriptions_active Registered subscription plans.\n\
             # TYPE fluxum_subscriptions_active gauge\n\
             fluxum_subscriptions_active{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_fanout_messages_total TxUpdate messages delivered.\n\
             # TYPE fluxum_fanout_messages_total counter\n\
             fluxum_fanout_messages_total{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_fanout_rows_total Insert+delete rows delivered.\n\
             # TYPE fluxum_fanout_rows_total counter\n\
             fluxum_fanout_rows_total{{shard=\"{shard}\"}} {}",
            self.subscriptions_active.load(Ordering::Relaxed).max(0),
            self.fanout_messages.load(Ordering::Relaxed),
            self.fanout_rows.load(Ordering::Relaxed),
        );
        out.push_str(
            "# HELP fluxum_subscriber_drops_total Dropped subscribers by reason.\n\
             # TYPE fluxum_subscriber_drops_total counter\n",
        );
        for (reason, counter) in [
            ("buffer_full", &self.drops_buffer_full),
            ("idle_timeout", &self.drops_idle),
            ("frame_too_large", &self.drops_frame_too_large),
        ] {
            let _ = writeln!(
                out,
                "fluxum_subscriber_drops_total{{shard=\"{shard}\",reason=\"{reason}\"}} {}",
                counter.load(Ordering::Relaxed),
            );
        }

        // --- connections (OBS-040) ---
        let _ = writeln!(
            out,
            "# HELP fluxum_connections_active Currently connected clients.\n\
             # TYPE fluxum_connections_active gauge\n\
             fluxum_connections_active{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_connections_total Connections accepted since startup.\n\
             # TYPE fluxum_connections_total counter\n\
             fluxum_connections_total{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_auth_success_total Successful authentications.\n\
             # TYPE fluxum_auth_success_total counter\n\
             fluxum_auth_success_total{{shard=\"{shard}\"}} {}\n\
             # HELP fluxum_auth_failure_total Failed authentications.\n\
             # TYPE fluxum_auth_failure_total counter\n\
             fluxum_auth_failure_total{{shard=\"{shard}\"}} {}",
            self.connections_active.load(Ordering::Relaxed).max(0),
            self.connections_total.load(Ordering::Relaxed),
            self.auth_success.load(Ordering::Relaxed),
            self.auth_failure.load(Ordering::Relaxed),
        );

        // --- pre-auth connection abuse rejections (SEC-032) ---
        // Every reason label is emitted, even at zero, so an alert on a rate
        // does not go stale-for-lack-of-series while the surface is healthy.
        let _ = writeln!(
            out,
            "# HELP fluxum_conn_rejected_total Connections refused on the pre-auth surface \
             by reason (SPEC-026 SEC-032).\n\
             # TYPE fluxum_conn_rejected_total counter"
        );
        for reason in ConnRejectReason::ALL {
            let _ = writeln!(
                out,
                "fluxum_conn_rejected_total{{shard=\"{shard}\", reason=\"{}\"}} {}",
                reason.as_str(),
                self.conn_rejected(reason),
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_places_observations_and_renders_pinned_buckets() {
        let m = Metrics::new(0);
        // 40µs → le=50 bucket; 300µs → le=500; 60000µs → +Inf only.
        m.record_reducer("send", ReducerOutcome::Ok, 40);
        m.record_reducer("send", ReducerOutcome::Ok, 300);
        m.record_reducer("send", ReducerOutcome::Err, 60_000);
        let text = m.prometheus(7);
        // Every pinned bucket boundary appears exactly once per reducer.
        for bound in REDUCER_DURATION_BUCKETS_US {
            assert!(
                text.contains(&format!("reducer=\"send\",le=\"{bound}\"")),
                "missing le={bound}"
            );
        }
        // Cumulative: le=50 has 1, le=500 has 2, +Inf has all 3.
        assert!(text.contains("reducer=\"send\",le=\"50\"} 1"));
        assert!(text.contains("reducer=\"send\",le=\"500\"} 2"));
        assert!(text.contains("reducer=\"send\",le=\"+Inf\"} 3"));
        assert!(text.contains("fluxum_reducer_duration_us_count{shard=\"0\",reducer=\"send\"} 3"));
        assert!(
            text.contains("fluxum_reducer_duration_us_sum{shard=\"0\",reducer=\"send\"} 60340")
        );
    }

    #[test]
    fn outcome_counters_and_tx_counters_render() {
        let m = Metrics::new(2);
        m.record_reducer("a", ReducerOutcome::Ok, 10);
        m.record_reducer("a", ReducerOutcome::RateLimited, 1);
        m.record_reducer("a", ReducerOutcome::QueueFull, 1);
        m.note_commit();
        m.note_rollback();
        m.note_commit();
        let text = m.prometheus(0);
        assert!(text.contains("reducer=\"a\",outcome=\"ok\"} 1"));
        assert!(text.contains("reducer=\"a\",outcome=\"rate_limited\"} 1"));
        assert!(text.contains("reducer=\"a\",outcome=\"queue_full\"} 1"));
        assert!(text.contains("fluxum_tx_commits_total{shard=\"2\"} 2"));
        assert!(text.contains("fluxum_tx_rollbacks_total{shard=\"2\"} 1"));
    }

    #[test]
    fn shard_state_and_slow_threshold_track() {
        let m = Metrics::new(0);
        assert_eq!(m.shard_state(), ShardState::Ready);
        assert!(!m.is_slow(4999));
        m.set_slow_reducer_threshold_us(1);
        assert!(m.is_slow(2));
        m.set_shard_state(ShardState::Recovering);
        assert_eq!(m.shard_state(), ShardState::Recovering);
        assert_eq!(m.shard_state().as_str(), "recovering");
        assert!(
            m.prometheus(0)
                .contains("fluxum_shard_state{shard=\"0\"} 1")
        );
    }

    #[test]
    fn fanout_connection_and_drop_counters_accumulate() {
        let m = Metrics::new(0);
        m.note_fanout(3);
        m.note_fanout(2);
        m.note_drop(DropReason::BufferFull);
        m.note_connect();
        m.note_connect();
        m.note_disconnect();
        m.note_auth(true);
        m.note_auth(false);
        m.set_subscriptions_active(3);
        let text = m.prometheus(0);
        assert!(text.contains("fluxum_fanout_messages_total{shard=\"0\"} 2"));
        assert!(text.contains("fluxum_fanout_rows_total{shard=\"0\"} 5"));
        assert!(text.contains("reason=\"buffer_full\"} 1"));
        assert!(text.contains("fluxum_connections_active{shard=\"0\"} 1"));
        assert!(text.contains("fluxum_connections_total{shard=\"0\"} 2"));
        assert!(text.contains("fluxum_auth_success_total{shard=\"0\"} 1"));
        assert!(text.contains("fluxum_auth_failure_total{shard=\"0\"} 1"));
        assert!(text.contains("fluxum_subscriptions_active{shard=\"0\"} 3"));
    }
}
