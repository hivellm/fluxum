//! Billion-row soak driver (T7.7; SPEC-013 TST-110/111/112, SPEC-015 tiered
//! storage; NFR-12/NFR-13): load a large dataset into a sharded + tiered
//! Fluxum deployment, then sustain writes **and** live subscriptions for a
//! duration while sampling the server's resident memory, and assert it stayed
//! within the configured budget throughout (FR-110/TIER-004).
//!
//! # What is here, and what is not
//!
//! This module is the *driver and report* — pure, unit-tested logic plus the
//! load/sustain orchestration over the [`Side`] trait. It does **not** run the
//! launch-defining billion-row soak or the 1 vCPU / 512 MB droplet validation
//! by itself: those are operator runs on real hardware (`fluxum-bench soak
//! --rows 1000000000 --duration-secs …` against a droplet profile), and the
//! [`SoakReport`] this produces is the release artifact they publish. A
//! small-scale smoke (`tests/soak_smoke.rs`) proves the driver end-to-end.
//!
//! The memory-within-budget check is the crux: `sample_rss` reads the **server
//! child's** RSS (not the driver's), the peak is compared against
//! `budget + tolerance` where the tolerance floor is `max(explicit, 10% of
//! budget)` (the TIER-002 accounting slack), and a breach fails the soak.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::measure::{RunResult, Summary};
use crate::report::Hardware;
use crate::workload::Side;

/// Knobs for one soak run.
#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// Rows to bulk-load before the sustain phase (≥ 1e9 for the real run).
    pub rows: u64,
    /// How long to sustain writes + subscriptions.
    pub duration: Duration,
    /// Concurrent writer connections during load and sustain.
    pub connections: usize,
    /// Un-acked writes each connection keeps in flight during the load phase.
    pub pipeline: usize,
    /// Live subscriptions held throughout the sustain phase.
    pub subscribers: usize,
    /// The server's `memory.budget` in bytes — the RSS ceiling under test.
    pub budget_bytes: u64,
    /// Explicit tolerance floor in bytes (the effective floor is
    /// `max(this, 10% of budget)`), mirroring `budget_tolerance_bytes`.
    pub tolerance_bytes: u64,
    /// RSS sampling cadence during the sustain phase.
    pub sample_interval: Duration,
    /// `host:port` of the server's admin HTTP listener, for scraping the
    /// TIER-080 buffer-pool gauges. `None` samples RSS only — the pool
    /// witnesses TST-111 wants are then absent, so the validation flags
    /// below cannot be satisfied.
    pub metrics_addr: Option<String>,
    /// Require eviction to have engaged (TST-111). Set by the validation
    /// profiles; false for smoke runs that never reach pool pressure.
    pub require_eviction: bool,
    /// Enforce the NFR-12 idle-RSS ceiling (< 100 MB). Set by the droplet
    /// profile, where the requirement applies.
    pub enforce_idle_ceiling: bool,
}

impl Default for SoakConfig {
    fn default() -> Self {
        Self {
            rows: 1_000_000_000,
            duration: Duration::from_secs(3600),
            connections: 8,
            pipeline: 64,
            subscribers: 16,
            budget_bytes: 512 * 1024 * 1024,
            tolerance_bytes: 0,
            sample_interval: Duration::from_secs(10),
            metrics_addr: None,
            require_eviction: false,
            enforce_idle_ceiling: false,
        }
    }
}

/// The idle-RSS ceiling a droplet run must clear (NFR-12 / TST-111: "idle
/// baseline RSS MUST be < 100 MB"). Decimal MB, as the requirement is written.
pub const IDLE_RSS_CEILING_BYTES: u64 = 100 * 1000 * 1000;

/// One resident-memory sample: seconds since the sustain phase began, and the
/// server's RSS in bytes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RssSample {
    /// Seconds since the sustain phase started.
    pub t_secs: f64,
    /// The server process RSS at that instant, bytes.
    pub rss_bytes: u64,
}

/// One buffer-pool sample scraped from the server's `/metrics` (SPEC-015
/// TIER-080). TST-111 requires these *alongside* process RSS: RSS is the
/// outside-in witness, these are the engine's own enforced accounting, and
/// `evictions` is what proves eviction actually engaged under pressure
/// rather than the dataset having quietly fit in memory.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PoolSample {
    /// Seconds since the sustain phase started.
    pub t_secs: f64,
    /// `fluxum_bufferpool_bytes` summed across shards.
    pub pool_bytes: u64,
    /// `fluxum_bufferpool_capacity_bytes` summed across shards.
    pub capacity_bytes: u64,
    /// `fluxum_bufferpool_evictions_total` (clean + spill) summed across
    /// shards — monotonic, so the last sample is the run total.
    pub evictions: u64,
}

/// Per-shard buffer-pool accounting at the end of the run — TST-112 requires
/// memory to stay within budget "on every shard", which a process-wide RSS
/// figure cannot show on its own (shards share one process).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ShardPool {
    /// The shard id from the metric's `shard` label.
    pub shard: u32,
    /// Peak `fluxum_bufferpool_bytes` observed for this shard.
    pub peak_pool_bytes: u64,
    /// This shard's `fluxum_bufferpool_capacity_bytes`.
    pub capacity_bytes: u64,
    /// Whether the peak stayed at or under this shard's own capacity.
    pub within_capacity: bool,
}

/// The soak report — the release artifact (JSON is source of truth; the
/// Markdown is rendered from it, mirroring the parity report convention).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoakReport {
    /// The bench harness version that produced the run.
    pub harness_version: String,
    /// The run date (`YYYY-MM-DD`).
    pub date: String,
    /// Host facts (CPU, RAM) the run executed on.
    pub hardware: Hardware,
    /// Rows loaded before the sustain phase.
    pub rows_loaded: u64,
    /// Sustain-phase length in seconds.
    pub duration_secs: f64,
    /// The configured RSS ceiling, bytes.
    pub budget_bytes: u64,
    /// The effective tolerance above the budget, bytes.
    pub tolerance_bytes: u64,
    /// Idle server RSS sampled before the load phase (TST-111: a droplet run
    /// asserts this is < 100 MB).
    pub idle_rss_bytes: u64,
    /// Peak server RSS across the whole run, bytes.
    pub peak_rss_bytes: u64,
    /// Whether the peak stayed within `budget + tolerance` (TIER-004).
    pub within_budget: bool,
    /// Whether the idle baseline cleared the NFR-12 < 100 MB ceiling. Always
    /// evaluated; only *enforced* on the droplet profile
    /// (`enforce_idle_ceiling`), so the number is in the artifact either way.
    pub idle_rss_ok: bool,
    /// Whether the NFR-12 idle ceiling was a pass/fail criterion for this run.
    pub idle_ceiling_enforced: bool,
    /// Whether eviction was observed engaging (TST-111).
    pub eviction_engaged: bool,
    /// Whether the eviction witness was a pass/fail criterion for this run.
    pub eviction_required: bool,
    /// The RSS samples taken during the sustain phase.
    pub rss_samples: Vec<RssSample>,
    /// The buffer-pool samples taken alongside them (TIER-080). Empty when
    /// no `metrics_addr` was configured.
    pub pool_samples: Vec<PoolSample>,
    /// Per-shard pool peaks against each shard's own capacity (TST-112:
    /// "within budget on every shard").
    pub shard_pools: Vec<ShardPool>,
    /// Sustained-write throughput + latency (the `send_chat` stream).
    pub write: Summary,
    /// TxUpdates delivered to the live subscriptions during the sustain phase.
    pub subscription_deliveries: u64,
    /// The overall verdict: within budget, writes flowed, subscriptions stayed
    /// live (TST-112).
    pub pass: bool,
}

impl SoakReport {
    /// Render the human-readable Markdown artifact from the report.
    #[must_use]
    pub fn markdown(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
        let _ = writeln!(out, "# Fluxum billion-row soak report\n");
        let _ = writeln!(
            out,
            "- harness `{}` · {} · {} ({} cores, {:.0} GiB RAM)",
            self.harness_version,
            self.date,
            self.hardware.cpu,
            self.hardware.cores,
            self.hardware.ram_gib
        );
        let _ = writeln!(
            out,
            "- **verdict: {}**",
            if self.pass { "PASS ✅" } else { "FAIL ❌" }
        );
        let _ = writeln!(out, "\n## Dataset & duration\n");
        let _ = writeln!(out, "- rows loaded: {}", self.rows_loaded);
        let _ = writeln!(out, "- sustain duration: {:.0}s", self.duration_secs);
        let _ = writeln!(out, "\n## Memory (TIER-004 / NFR-12)\n");
        let _ = writeln!(out, "- budget: {:.0} MiB", mib(self.budget_bytes));
        let _ = writeln!(out, "- tolerance: {:.0} MiB", mib(self.tolerance_bytes));
        let _ = writeln!(out, "- idle RSS: {:.1} MiB", mib(self.idle_rss_bytes));
        let _ = writeln!(out, "- peak RSS: {:.1} MiB", mib(self.peak_rss_bytes));
        let _ = writeln!(
            out,
            "- within budget: {}",
            if self.within_budget { "yes" } else { "NO" }
        );
        let _ = writeln!(
            out,
            "- idle RSS < 100 MB (NFR-12): {}{}",
            if self.idle_rss_ok { "yes" } else { "NO" },
            if self.idle_ceiling_enforced {
                ""
            } else {
                " *(recorded, not enforced for this profile)*"
            }
        );
        let _ = writeln!(
            out,
            "- eviction engaged (TST-111): {}{}",
            if self.eviction_engaged { "yes" } else { "NO" },
            if self.eviction_required {
                ""
            } else {
                " *(recorded, not required for this profile)*"
            }
        );
        if self.shard_pools.is_empty() {
            let _ = writeln!(
                out,
                "- per-shard buffer pool: *not sampled (no `--metrics-addr`)*"
            );
        } else {
            let _ = writeln!(out, "\n### Buffer pool per shard (TIER-080 / TST-112)\n");
            let _ = writeln!(out, "| shard | peak pool | capacity | within |");
            let _ = writeln!(out, "|---|---|---|---|");
            for s in &self.shard_pools {
                let _ = writeln!(
                    out,
                    "| {} | {:.1} MiB | {:.1} MiB | {} |",
                    s.shard,
                    mib(s.peak_pool_bytes),
                    mib(s.capacity_bytes),
                    if s.within_capacity { "yes" } else { "**NO**" }
                );
            }
        }
        let _ = writeln!(out, "\n## Sustained load\n");
        let _ = writeln!(
            out,
            "- write throughput: {:.0} ops/s (p99 {:.2} ms)",
            self.write.throughput_mean,
            self.write.p99_ns_mean / 1e6
        );
        let _ = writeln!(
            out,
            "- subscription deliveries: {}",
            self.subscription_deliveries
        );
        out
    }

    /// Write the JSON + Markdown artifacts as `{stem}.json` / `{stem}.md` into
    /// `out_dir`, creating it if needed.
    ///
    /// # Errors
    /// Directory creation, serialization, or file I/O failing.
    pub fn write_artifacts(&self, out_dir: &std::path::Path, stem: &str) -> Result<(), String> {
        std::fs::create_dir_all(out_dir)
            .map_err(|e| format!("create {}: {e}", out_dir.display()))?;
        let json = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(out_dir.join(format!("{stem}.json")), json).map_err(|e| e.to_string())?;
        std::fs::write(out_dir.join(format!("{stem}.md")), self.markdown())
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// Parse a byte-size string (`"512MiB"`, `"1GiB"`, `"512MB"`, `"536870912"`)
/// to bytes — the same shapes `memory.budget` accepts, so the soak's
/// within-budget assertion uses the exact ceiling handed to the server.
/// Binary suffixes (`KiB`/`MiB`/`GiB`/`TiB`) are powers of 1024; decimal
/// (`KB`/`MB`/`GB`/`TB`) powers of 1000; a bare number is bytes.
///
/// # Errors
/// An unparseable number or an unknown suffix.
pub fn parse_bytesize(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let (num, mult): (&str, u64) = if let Some(n) = strip_suffix_ci(t, "kib") {
        (n, 1024)
    } else if let Some(n) = strip_suffix_ci(t, "mib") {
        (n, 1024 * 1024)
    } else if let Some(n) = strip_suffix_ci(t, "gib") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = strip_suffix_ci(t, "tib") {
        (n, 1024u64.pow(4))
    } else if let Some(n) = strip_suffix_ci(t, "kb") {
        (n, 1000)
    } else if let Some(n) = strip_suffix_ci(t, "mb") {
        (n, 1000 * 1000)
    } else if let Some(n) = strip_suffix_ci(t, "gb") {
        (n, 1000 * 1000 * 1000)
    } else if let Some(n) = strip_suffix_ci(t, "tb") {
        (n, 1000u64.pow(4))
    } else if let Some(n) = strip_suffix_ci(t, "b") {
        (n, 1)
    } else {
        (t, 1)
    };
    let value: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("cannot parse byte size `{s}`"))?;
    value
        .checked_mul(mult)
        .ok_or_else(|| format!("byte size `{s}` overflows u64"))
}

/// Case-insensitive suffix strip returning the numeric prefix.
fn strip_suffix_ci<'a>(s: &'a str, suffix: &str) -> Option<&'a str> {
    let s_low = s.to_ascii_lowercase();
    if s_low.ends_with(suffix) {
        Some(&s[..s.len() - suffix.len()])
    } else {
        None
    }
}

/// The effective tolerance above the budget: `max(explicit, 10% of budget)` —
/// the TIER-002 accounting slack, so a run is not failed by the buffer pool's
/// unavoidable bookkeeping overhead.
#[must_use]
pub fn budget_tolerance(budget_bytes: u64, explicit_bytes: u64) -> u64 {
    explicit_bytes.max(budget_bytes / 10)
}

/// Whether the peak RSS stayed within the budget plus tolerance (TIER-004).
#[must_use]
pub fn within_budget(peak_rss: u64, budget_bytes: u64, tolerance_bytes: u64) -> bool {
    peak_rss <= budget_bytes.saturating_add(tolerance_bytes)
}

/// The highest RSS across the samples (0 if none).
#[must_use]
pub fn peak_rss(samples: &[RssSample]) -> u64 {
    samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0)
}

/// Parse the SPEC-015 TIER-080 buffer-pool series out of a Prometheus
/// exposition body into one aggregate sample plus the per-shard breakdown.
///
/// Values are summed across shards for the aggregate (the process-wide view
/// RSS is compared against) and kept separate per `shard` label for the
/// TST-112 per-shard assertion. Unlabelled series are tolerated and folded
/// into shard 0, so this keeps working against an older server.
#[must_use]
pub fn parse_pool_metrics(metrics: &str, t_secs: f64) -> (PoolSample, Vec<(u32, u64, u64)>) {
    use std::collections::BTreeMap;
    // shard -> (pool_bytes, capacity_bytes)
    let mut per_shard: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    let mut sample = PoolSample {
        t_secs,
        ..PoolSample::default()
    };
    for line in metrics.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else {
            continue;
        };
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (name, rest.trim_end_matches('}')),
            None => (series, ""),
        };
        let shard = label_value(labels, "shard")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);
        match name {
            "fluxum_bufferpool_bytes" => {
                sample.pool_bytes += value;
                per_shard.entry(shard).or_default().0 += value;
            }
            "fluxum_bufferpool_capacity_bytes" => {
                sample.capacity_bytes += value;
                per_shard.entry(shard).or_default().1 += value;
            }
            // Both `kind` variants land on the same total.
            "fluxum_bufferpool_evictions_total" => sample.evictions += value,
            _ => {}
        }
    }
    let shards = per_shard
        .into_iter()
        .map(|(shard, (pool, capacity))| (shard, pool, capacity))
        .collect();
    (sample, shards)
}

/// The value of `name` in a Prometheus label set (`a="1",b="2"`).
fn label_value<'a>(labels: &'a str, name: &str) -> Option<&'a str> {
    labels.split(',').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim().trim_matches('"'))
    })
}

/// Whether eviction engaged during the run — the TST-111 witness that the
/// dataset really exceeded the pool. A run whose data quietly fit proves
/// nothing about tiering.
#[must_use]
pub fn eviction_engaged(samples: &[PoolSample]) -> bool {
    samples.iter().any(|s| s.evictions > 0)
}

/// Whether the eviction witness satisfies the run's contract. `required` is
/// false for small smoke runs, where there is no pressure to engage eviction
/// and demanding it would fail a healthy run; the validation profiles
/// (TST-110/112) set it.
#[must_use]
pub fn eviction_ok(samples: &[PoolSample], required: bool) -> bool {
    !required || eviction_engaged(samples)
}

/// Whether the idle baseline cleared the NFR-12 ceiling. `enforced` is false
/// for runs that are not the droplet profile, where the requirement does not
/// apply — it is recorded either way so the artifact always shows the number.
#[must_use]
pub fn idle_rss_ok(idle_rss_bytes: u64, enforced: bool) -> bool {
    !enforced || idle_rss_bytes < IDLE_RSS_CEILING_BYTES
}

/// Reduce the per-sample shard readings to one [`ShardPool`] per shard,
/// carrying each shard's own peak and its own capacity (TST-112).
#[must_use]
pub fn shard_pools(readings: &[(u32, u64, u64)]) -> Vec<ShardPool> {
    use std::collections::BTreeMap;
    let mut peaks: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    for &(shard, pool, capacity) in readings {
        let entry = peaks.entry(shard).or_insert((0, 0));
        entry.0 = entry.0.max(pool);
        // Capacity is configuration, not a measurement; the last non-zero
        // reading wins so a scrape that raced startup cannot zero it.
        if capacity > 0 {
            entry.1 = capacity;
        }
    }
    peaks
        .into_iter()
        .map(|(shard, (peak_pool_bytes, capacity_bytes))| ShardPool {
            shard,
            peak_pool_bytes,
            capacity_bytes,
            // A shard with no capacity reading cannot be judged; treat the
            // missing witness as a failure rather than a silent pass.
            within_capacity: capacity_bytes > 0 && peak_pool_bytes <= capacity_bytes,
        })
        .collect()
}

/// The soak verdict (TST-111/112): process RSS within budget, every shard's
/// pool within its own capacity, eviction observed engaging, the idle
/// baseline under the NFR-12 ceiling, writes flowing and subscriptions live.
#[must_use]
pub fn soak_pass(
    within_budget: bool,
    write_ops: u64,
    sub_deliveries: u64,
    shards: &[ShardPool],
    eviction_engaged: bool,
    idle_rss_ok: bool,
) -> bool {
    within_budget
        && write_ops > 0
        && sub_deliveries > 0
        && shards.iter().all(|s| s.within_capacity)
        && eviction_engaged
        && idle_rss_ok
}

/// Run a soak: load `cfg.rows`, then sustain writes + subscriptions for
/// `cfg.duration` while sampling the server's RSS through `sample_rss`.
///
/// `sample_rss` reads the **server child's** resident memory (bytes); it is a
/// parameter so the driver is testable without a live server. `hardware`,
/// `harness_version`, and `date` are stamped into the report.
///
/// # Errors
/// A load or sustain write failing, or a worker thread panicking.
pub fn run_soak(
    side: &(dyn Side + Sync),
    cfg: &SoakConfig,
    sample_rss: &(dyn Fn() -> u64 + Send + Sync),
    hardware: Hardware,
    harness_version: &str,
    date: &str,
) -> Result<SoakReport, String> {
    let idle_rss_bytes = sample_rss();
    load_rows(side, cfg)?;
    let sustain = sustain(side, cfg, sample_rss)?;

    let peak = peak_rss(&sustain.samples).max(idle_rss_bytes);
    let tolerance_bytes = budget_tolerance(cfg.budget_bytes, cfg.tolerance_bytes);
    let within = within_budget(peak, cfg.budget_bytes, tolerance_bytes);
    let write = Summary::from_runs(&[sustain.write]);
    let shard_pools = shard_pools(&sustain.shard_readings);
    let evicted = eviction_engaged(&sustain.pool_samples);
    let idle_ok = idle_rss_ok(idle_rss_bytes, cfg.enforce_idle_ceiling);
    let pass = soak_pass(
        within,
        write.total_ops,
        sustain.deliveries,
        &shard_pools,
        eviction_ok(&sustain.pool_samples, cfg.require_eviction),
        idle_ok,
    );

    Ok(SoakReport {
        harness_version: harness_version.to_owned(),
        date: date.to_owned(),
        hardware,
        rows_loaded: cfg.rows,
        duration_secs: cfg.duration.as_secs_f64(),
        budget_bytes: cfg.budget_bytes,
        tolerance_bytes,
        idle_rss_bytes,
        peak_rss_bytes: peak,
        within_budget: within,
        idle_rss_ok: idle_rss_bytes < IDLE_RSS_CEILING_BYTES,
        idle_ceiling_enforced: cfg.enforce_idle_ceiling,
        eviction_engaged: evicted,
        eviction_required: cfg.require_eviction,
        rss_samples: sustain.samples,
        pool_samples: sustain.pool_samples,
        shard_pools,
        write,
        subscription_deliveries: sustain.deliveries,
        pass,
    })
}

/// Bulk-load `cfg.rows` tasks across `cfg.connections` pipelined writers.
fn load_rows(side: &(dyn Side + Sync), cfg: &SoakConfig) -> Result<(), String> {
    if cfg.rows == 0 {
        return Ok(());
    }
    let per = cfg.rows / cfg.connections.max(1) as u64;
    let pipeline = cfg.pipeline.max(1);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for c in 0..cfg.connections.max(1) {
            let side = &side;
            handles.push(scope.spawn(move || -> Result<(), String> {
                let mut client = side.client(c as u64)?;
                let mut inflight: std::collections::VecDeque<u64> = Default::default();
                for i in 0..per {
                    let token = client.start_task(&format!("soak-{c}-{i}"))?;
                    inflight.push_back(token);
                    if inflight.len() >= pipeline
                        && let Some(t) = inflight.pop_front()
                    {
                        client.finish_task(t)?;
                    }
                }
                while let Some(t) = inflight.pop_front() {
                    client.finish_task(t)?;
                }
                Ok(())
            }));
        }
        for h in handles {
            h.join().map_err(|_| "load worker panicked".to_owned())??;
        }
        Ok(())
    })
}

/// The output of the sustain phase.
struct Sustain {
    /// The sustained-write stream (send_chat acks) as one run.
    write: RunResult,
    /// TxUpdates delivered to the subscriptions.
    deliveries: u64,
    /// RSS samples over the phase.
    samples: Vec<RssSample>,
    /// Buffer-pool samples taken on the same cadence (TIER-080).
    pool_samples: Vec<PoolSample>,
    /// Raw `(shard, pool_bytes, capacity_bytes)` readings from every scrape.
    shard_readings: Vec<(u32, u64, u64)>,
}

/// Sustain writes + live subscriptions for `cfg.duration`, sampling RSS.
///
/// The write load is acked `add_task` (unthrottled — the throughput + memory
/// stress that exercises tiering). Subscription liveness is fed separately by
/// a single bounded-rate `send_chat` feeder, because the demo's `send_chat` is
/// rate-limited (RED-050) and would be a poor high-throughput write op.
fn sustain(
    side: &(dyn Side + Sync),
    cfg: &SoakConfig,
    sample_rss: &(dyn Fn() -> u64 + Send + Sync),
) -> Result<Sustain, String> {
    let channel = 1u32;
    let stop = Arc::new(AtomicBool::new(false));
    let deliveries = Arc::new(AtomicU64::new(0));
    let writers = cfg.connections.max(1);
    let feeders = usize::from(cfg.subscribers > 0);
    // Barrier: all writers + subscribers + the feeder + the sampler start
    // together.
    let barrier = Arc::new(Barrier::new(writers + cfg.subscribers + feeders + 1));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let pool_samples = Arc::new(Mutex::new(Vec::new()));
    let shard_readings = Arc::new(Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        // Live subscriptions (delivery lands on the SDK's read loop; the
        // thread just keeps its client alive until stop).
        for s in 0..cfg.subscribers {
            let side = &side;
            let stop = Arc::clone(&stop);
            let deliveries = Arc::clone(&deliveries);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || -> Result<(), String> {
                let mut client = side.client(10_000 + s as u64)?;
                let counter = Arc::clone(&deliveries);
                client.subscribe_chat(
                    channel,
                    Box::new(move |_content| {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }),
                )?;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(())
            });
        }

        // The subscription feeder: one client posting chat at ~12/s (under the
        // RED-050 20/s limit) so the live subscriptions above keep receiving.
        if feeders > 0 {
            let side = &side;
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || -> Result<(), String> {
                let mut client = side.client(30_000)?;
                barrier.wait();
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // A rate-limit rejection is not a soak failure — the feeder
                    // only has to keep the subscriptions warm.
                    let _ = client.send_chat(channel, &format!("feed-{n}"));
                    n += 1;
                    std::thread::sleep(Duration::from_millis(80));
                }
                Ok(())
            });
        }

        // Sustained writers (acked `add_task` — the unthrottled write load).
        let mut writer_handles = Vec::new();
        for w in 0..writers {
            let side = &side;
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            writer_handles.push(scope.spawn(move || -> Result<RunResult, String> {
                let mut client = side.client(20_000 + w as u64)?;
                let mut latencies = Vec::new();
                barrier.wait();
                let start = Instant::now();
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let op = Instant::now();
                    client.add_task(&format!("soak-{w}-{n}"))?;
                    latencies.push(u64::try_from(op.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    n += 1;
                }
                Ok(RunResult {
                    ops: n,
                    wall: start.elapsed(),
                    latencies_ns: latencies,
                })
            }));
        }

        // The sampler: this thread owns the duration and the stop signal. It
        // takes the RSS reading and the TIER-080 pool scrape on the same
        // cadence so the two witnesses TST-111 requires are time-aligned.
        let sampler_samples = Arc::clone(&samples);
        let sampler_pool = Arc::clone(&pool_samples);
        let sampler_shards = Arc::clone(&shard_readings);
        let sampler_stop = Arc::clone(&stop);
        let sampler_barrier = Arc::clone(&barrier);
        let sampler = scope.spawn(move || {
            sampler_barrier.wait();
            let start = Instant::now();
            loop {
                let elapsed = start.elapsed();
                let t_secs = elapsed.as_secs_f64();
                sampler_samples
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(RssSample {
                        t_secs,
                        rss_bytes: sample_rss(),
                    });
                // A failed scrape is skipped, not fatal: losing one sample
                // must not abort an hours-long soak. Losing *every* sample
                // shows up as an empty series, which fails the TST-111
                // witnesses rather than passing silently.
                if let Some(addr) = cfg.metrics_addr.as_deref()
                    && let Ok(body) = crate::load::scrape_metrics(addr)
                {
                    let (sample, shards) = parse_pool_metrics(&body, t_secs);
                    sampler_pool
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(sample);
                    sampler_shards
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .extend(shards);
                }
                if elapsed >= cfg.duration {
                    break;
                }
                std::thread::sleep(cfg.sample_interval.min(cfg.duration));
            }
            sampler_stop.store(true, Ordering::Relaxed);
        });

        sampler.join().map_err(|_| "sampler panicked".to_owned())?;
        // Aggregate the writer streams into one run (total ops over the window).
        let mut ops = 0u64;
        let mut all_latencies = Vec::new();
        let mut wall = Duration::ZERO;
        for h in writer_handles {
            let run = h.join().map_err(|_| "writer panicked".to_owned())??;
            ops += run.ops;
            wall = wall.max(run.wall);
            all_latencies.extend(run.latencies_ns);
        }
        Ok(Sustain {
            write: RunResult {
                ops,
                wall,
                latencies_ns: all_latencies,
            },
            deliveries: deliveries.load(Ordering::Relaxed),
            samples: take_shared(samples),
            pool_samples: take_shared(pool_samples),
            shard_readings: take_shared(shard_readings),
        })
    })
}

/// Take a scoped collector's contents once its producer thread has joined.
fn take_shared<T>(shared: Arc<Mutex<Vec<T>>>) -> Vec<T> {
    Arc::try_unwrap(shared)
        .map(|m| {
            m.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        })
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn hardware() -> Hardware {
        Hardware {
            cpu: "test".into(),
            cores: 4,
            ram_gib: 8.0,
            os: "test".into(),
            disk: "test".into(),
        }
    }

    #[test]
    fn parse_bytesize_handles_binary_decimal_and_bare() {
        assert_eq!(parse_bytesize("512MiB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_bytesize("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytesize("512mb").unwrap(), 512_000_000);
        assert_eq!(parse_bytesize("536870912").unwrap(), 536_870_912);
        assert_eq!(parse_bytesize("128 KiB").unwrap(), 128 * 1024);
        assert_eq!(parse_bytesize("42B").unwrap(), 42);
        assert!(parse_bytesize("bogus").is_err());
        assert!(parse_bytesize("12XB").is_err());
    }

    #[test]
    fn budget_tolerance_is_the_ten_percent_floor() {
        // 10% of budget dominates a smaller explicit floor.
        assert_eq!(budget_tolerance(1000, 0), 100);
        assert_eq!(budget_tolerance(1000, 50), 100);
        // An explicit floor above 10% wins.
        assert_eq!(budget_tolerance(1000, 250), 250);
    }

    #[test]
    fn within_budget_admits_the_tolerance_band_and_rejects_a_breach() {
        assert!(within_budget(1100, 1000, 100)); // exactly at budget+tolerance
        assert!(within_budget(900, 1000, 100));
        assert!(!within_budget(1101, 1000, 100)); // one byte over
    }

    #[test]
    fn peak_rss_and_pass_reduce_the_run() {
        let samples = [
            RssSample {
                t_secs: 0.0,
                rss_bytes: 10,
            },
            RssSample {
                t_secs: 1.0,
                rss_bytes: 42,
            },
            RssSample {
                t_secs: 2.0,
                rss_bytes: 30,
            },
        ];
        assert_eq!(peak_rss(&samples), 42);
        assert_eq!(peak_rss(&[]), 0);
        let ok_shards = [ShardPool {
            shard: 0,
            peak_pool_bytes: 10,
            capacity_bytes: 100,
            within_capacity: true,
        }];
        let bad_shard = [ShardPool {
            shard: 1,
            peak_pool_bytes: 200,
            capacity_bytes: 100,
            within_capacity: false,
        }];
        // Every criterion is load-bearing: budget, writes, deliveries, each
        // shard's own pool, the eviction witness, the idle ceiling.
        assert!(soak_pass(true, 100, 5, &ok_shards, true, true));
        assert!(!soak_pass(false, 100, 5, &ok_shards, true, true)); // over budget
        assert!(!soak_pass(true, 0, 5, &ok_shards, true, true)); // no writes
        assert!(!soak_pass(true, 100, 0, &ok_shards, true, true)); // subs silent
        assert!(!soak_pass(true, 100, 5, &bad_shard, true, true)); // a shard over
        assert!(!soak_pass(true, 100, 5, &ok_shards, false, true)); // never evicted
        assert!(!soak_pass(true, 100, 5, &ok_shards, true, false)); // idle too fat
        // One healthy shard does not excuse a sibling that blew its pool.
        let mixed = [ok_shards[0], bad_shard[0]];
        assert!(!soak_pass(true, 100, 5, &mixed, true, true));
    }

    #[test]
    fn pool_metrics_parse_per_shard_and_sum_across_them() {
        let body = "\
# HELP fluxum_bufferpool_bytes ignored
# TYPE fluxum_bufferpool_bytes gauge
fluxum_bufferpool_bytes{shard=\"0\"} 100
fluxum_bufferpool_bytes{shard=\"1\"} 250
fluxum_bufferpool_capacity_bytes{shard=\"0\"} 1000
fluxum_bufferpool_capacity_bytes{shard=\"1\"} 1000
fluxum_bufferpool_evictions_total{shard=\"0\",kind=\"clean\"} 7
fluxum_bufferpool_evictions_total{shard=\"0\",kind=\"spill\"} 3
fluxum_table_rows{shard=\"0\",table=\"Chat\"} 42
";
        let (sample, shards) = parse_pool_metrics(body, 1.5);
        assert_eq!(sample.t_secs, 1.5);
        assert_eq!(sample.pool_bytes, 350, "summed across shards");
        assert_eq!(sample.capacity_bytes, 2000);
        assert_eq!(sample.evictions, 10, "clean + spill");
        assert_eq!(shards, vec![(0, 100, 1000), (1, 250, 1000)]);
    }

    #[test]
    fn pool_metrics_tolerate_unlabelled_series_and_ignore_noise() {
        // An older server without the shard label still parses (folded into
        // shard 0), and unrelated series never leak into the totals.
        let body = "fluxum_bufferpool_bytes 64\n\
                    fluxum_bufferpool_capacity_bytes 128\n\
                    fluxum_memstore_bytes{shard=\"0\"} 999999\n\
                    garbage line\n";
        let (sample, shards) = parse_pool_metrics(body, 0.0);
        assert_eq!(sample.pool_bytes, 64);
        assert_eq!(sample.capacity_bytes, 128);
        assert_eq!(sample.evictions, 0);
        assert_eq!(shards, vec![(0, 64, 128)]);
    }

    #[test]
    fn shard_pools_take_each_shards_own_peak_and_capacity() {
        // Two scrapes per shard; the peak wins and a zero capacity reading
        // (a scrape racing startup) never overwrites a real one.
        let readings = [(0, 10, 0), (0, 90, 100), (1, 300, 100), (1, 50, 100)];
        let pools = shard_pools(&readings);
        assert_eq!(pools.len(), 2);
        assert_eq!(pools[0].peak_pool_bytes, 90);
        assert_eq!(pools[0].capacity_bytes, 100);
        assert!(pools[0].within_capacity);
        assert_eq!(pools[1].peak_pool_bytes, 300);
        assert!(!pools[1].within_capacity, "shard 1 blew its pool");
    }

    #[test]
    fn a_shard_with_no_capacity_witness_is_not_a_silent_pass() {
        // Capacity never scraped: there is nothing to judge against, and
        // treating that as "within" would let a broken scrape pass a soak.
        let pools = shard_pools(&[(3, 5, 0)]);
        assert_eq!(pools[0].capacity_bytes, 0);
        assert!(!pools[0].within_capacity);
    }

    #[test]
    fn eviction_and_idle_witnesses_are_gated_by_the_profile() {
        let never = [PoolSample::default()];
        let evicted = [PoolSample {
            evictions: 1,
            ..PoolSample::default()
        }];
        assert!(eviction_engaged(&evicted));
        assert!(!eviction_engaged(&never));
        assert!(!eviction_engaged(&[]), "no samples is no witness");
        // Required only on the validation profiles.
        assert!(!eviction_ok(&never, true));
        assert!(eviction_ok(&never, false));
        assert!(eviction_ok(&evicted, true));

        assert!(idle_rss_ok(99_000_000, true));
        assert!(!idle_rss_ok(100_000_000, true), "the ceiling is exclusive");
        assert!(
            idle_rss_ok(4_000_000_000, false),
            "not enforced off-profile"
        );
    }

    #[test]
    fn a_report_renders_json_and_markdown_artifacts() {
        let report = SoakReport {
            harness_version: "0.2.0".into(),
            date: "2026-07-26".into(),
            hardware: hardware(),
            rows_loaded: 1_000_000_000,
            duration_secs: 3600.0,
            budget_bytes: 512 * 1024 * 1024,
            tolerance_bytes: 51 * 1024 * 1024,
            idle_rss_bytes: 80 * 1024 * 1024,
            peak_rss_bytes: 500 * 1024 * 1024,
            within_budget: true,
            idle_rss_ok: true,
            idle_ceiling_enforced: true,
            eviction_engaged: true,
            eviction_required: true,
            rss_samples: vec![RssSample {
                t_secs: 0.0,
                rss_bytes: 80 * 1024 * 1024,
            }],
            pool_samples: vec![PoolSample {
                t_secs: 0.0,
                pool_bytes: 400 * 1024 * 1024,
                capacity_bytes: 410 * 1024 * 1024,
                evictions: 9001,
            }],
            shard_pools: vec![ShardPool {
                shard: 0,
                peak_pool_bytes: 400 * 1024 * 1024,
                capacity_bytes: 410 * 1024 * 1024,
                within_capacity: true,
            }],
            write: Summary::from_runs(&[RunResult {
                ops: 6000,
                wall: Duration::from_secs(60),
                latencies_ns: vec![1_000_000; 100],
            }]),
            subscription_deliveries: 4200,
            pass: true,
        };
        let md = report.markdown();
        assert!(md.contains("PASS"));
        assert!(md.contains("1000000000"));
        assert!(md.contains("within budget: yes"));
        // The TST-111/112 witnesses have to be legible in the artifact, not
        // only in the JSON.
        assert!(md.contains("eviction engaged (TST-111): yes"), "{md}");
        assert!(md.contains("idle RSS < 100 MB (NFR-12): yes"), "{md}");
        assert!(md.contains("Buffer pool per shard"), "{md}");

        let dir = std::env::temp_dir().join(format!("fluxum-soak-test-{}", std::process::id()));
        report.write_artifacts(&dir, "soak-report").unwrap();
        let json = std::fs::read_to_string(dir.join("soak-report.json")).unwrap();
        assert!(json.contains("\"pass\": true"));
        assert!(dir.join("soak-report.md").exists());
        // The JSON is the source of truth: it round-trips back into the
        // report and re-renders identically (the parity-report convention).
        let back: SoakReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back.rows_loaded, 1_000_000_000);
        assert_eq!(back.markdown(), md);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unsampled_pool_says_so_rather_than_implying_a_clean_run() {
        // No `--metrics-addr`: the artifact must not read as if the shards
        // were checked and found healthy.
        let mut report = SoakReport {
            harness_version: "0.2.0".into(),
            date: "2026-07-27".into(),
            hardware: hardware(),
            rows_loaded: 1000,
            duration_secs: 1.0,
            budget_bytes: 1024,
            tolerance_bytes: 102,
            idle_rss_bytes: 10,
            peak_rss_bytes: 20,
            within_budget: true,
            idle_rss_ok: true,
            idle_ceiling_enforced: false,
            eviction_engaged: false,
            eviction_required: false,
            rss_samples: Vec::new(),
            pool_samples: Vec::new(),
            shard_pools: Vec::new(),
            write: Summary::from_runs(&[RunResult {
                ops: 1,
                wall: Duration::from_secs(1),
                latencies_ns: vec![1],
            }]),
            subscription_deliveries: 1,
            pass: true,
        };
        let md = report.markdown();
        assert!(md.contains("not sampled"), "{md}");
        assert!(md.contains("not required for this profile"), "{md}");
        // And with the profile demanding the witnesses, the same absence is
        // a failure rather than a footnote.
        report.eviction_required = true;
        assert!(!soak_pass(
            report.within_budget,
            report.write.total_ops,
            report.subscription_deliveries,
            &report.shard_pools,
            eviction_ok(&report.pool_samples, true),
            report.idle_rss_ok,
        ));
    }

    // --- driver test over a mock side --------------------------------------

    /// The registered `subscribe_chat` callbacks a mock write fans out to.
    type Listeners = Arc<Mutex<Vec<Box<dyn Fn(&str) + Send + Sync>>>>;

    /// A mock side whose clients ack every write and fan `send_chat` out to
    /// the registered `subscribe_chat` listeners (so a soak run's writers feed
    /// its subscribers, exercising the whole orchestration without a server).
    #[derive(Default)]
    struct MockSide {
        listeners: Listeners,
    }

    struct MockClient {
        listeners: Listeners,
        next: u64,
    }

    impl Side for MockSide {
        fn name(&self) -> &'static str {
            "mock"
        }
        fn client(&self, _seed: u64) -> Result<Box<dyn crate::workload::BenchClient>, String> {
            Ok(Box::new(MockClient {
                listeners: Arc::clone(&self.listeners),
                next: 0,
            }))
        }
    }

    impl crate::workload::BenchClient for MockClient {
        fn add_task(&mut self, _title: &str) -> Result<(), String> {
            Ok(())
        }
        fn start_task(&mut self, _title: &str) -> Result<u64, String> {
            self.next += 1;
            Ok(self.next)
        }
        fn finish_task(&mut self, _token: u64) -> Result<(), String> {
            Ok(())
        }
        fn send_chat(&mut self, _channel: u32, content: &str) -> Result<(), String> {
            for listener in self
                .listeners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
            {
                listener(content);
            }
            Ok(())
        }
        fn subscribe_chat(
            &mut self,
            _channel: u32,
            on_message: Box<dyn Fn(&str) + Send + Sync>,
        ) -> Result<(), String> {
            self.listeners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(on_message);
            Ok(())
        }
        fn prepare_reads(&mut self, _rows: u32) -> Result<(), String> {
            Ok(())
        }
        fn hot_read(&mut self) -> Result<String, String> {
            Ok(String::new())
        }
        fn load_my_data(&mut self) -> Result<u32, String> {
            Ok(0)
        }
    }

    #[test]
    fn run_soak_loads_sustains_samples_and_passes() {
        let side = MockSide::default();
        let cfg = SoakConfig {
            rows: 200,
            duration: Duration::from_millis(150),
            connections: 2,
            pipeline: 8,
            subscribers: 2,
            budget_bytes: 1_000_000,
            tolerance_bytes: 0,
            sample_interval: Duration::from_millis(20),
            // No live server behind this mock, so no pool scrape — and
            // therefore no eviction/idle witness to demand of it.
            metrics_addr: None,
            require_eviction: false,
            enforce_idle_ceiling: false,
        };
        // A sampler that stays well under budget.
        let rss = |_: ()| 500_000u64;
        let report = run_soak(
            &side,
            &cfg,
            &move || rss(()),
            hardware(),
            "0.2.0-test",
            "2026-07-26",
        )
        .unwrap();
        assert_eq!(report.rows_loaded, 200);
        assert!(!report.rss_samples.is_empty(), "RSS was sampled");
        assert!(report.within_budget);
        assert!(report.write.total_ops > 0, "sustained writes happened");
        assert!(
            report.subscription_deliveries > 0,
            "subscriptions received the writers' messages"
        );
        assert!(report.pass, "a healthy soak passes");
    }

    #[test]
    fn run_soak_fails_when_rss_breaches_the_budget() {
        let side = MockSide::default();
        let cfg = SoakConfig {
            rows: 50,
            duration: Duration::from_millis(120),
            connections: 1,
            pipeline: 4,
            subscribers: 1,
            budget_bytes: 1000,
            tolerance_bytes: 0,
            sample_interval: Duration::from_millis(20),
            metrics_addr: None,
            require_eviction: false,
            enforce_idle_ceiling: false,
        };
        // RSS far over budget → the soak must fail even though load succeeds.
        let report = run_soak(
            &side,
            &cfg,
            &|| 10_000_000u64,
            hardware(),
            "0.2.0-test",
            "2026-07-26",
        )
        .unwrap();
        assert!(!report.within_budget);
        assert!(!report.pass, "an over-budget soak fails (TIER-004)");
    }
}
