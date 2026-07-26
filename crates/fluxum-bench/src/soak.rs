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

use serde::Serialize;

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
        }
    }
}

/// One resident-memory sample: seconds since the sustain phase began, and the
/// server's RSS in bytes.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RssSample {
    /// Seconds since the sustain phase started.
    pub t_secs: f64,
    /// The server process RSS at that instant, bytes.
    pub rss_bytes: u64,
}

/// The soak report — the release artifact (JSON is source of truth; the
/// Markdown is rendered from it, mirroring the parity report convention).
#[derive(Debug, Clone, Serialize)]
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
    /// The RSS samples taken during the sustain phase.
    pub rss_samples: Vec<RssSample>,
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

/// The soak verdict (TST-112): within budget, writes flowed, subscriptions
/// stayed live.
#[must_use]
pub fn soak_pass(within_budget: bool, write_ops: u64, sub_deliveries: u64) -> bool {
    within_budget && write_ops > 0 && sub_deliveries > 0
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
    let pass = soak_pass(within, write.total_ops, sustain.deliveries);

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
        rss_samples: sustain.samples,
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

        // The RSS sampler: this thread owns the duration and the stop signal.
        let sampler_samples = Arc::clone(&samples);
        let sampler_stop = Arc::clone(&stop);
        let sampler_barrier = Arc::clone(&barrier);
        let sampler = scope.spawn(move || {
            sampler_barrier.wait();
            let start = Instant::now();
            loop {
                let elapsed = start.elapsed();
                sampler_samples
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(RssSample {
                        t_secs: elapsed.as_secs_f64(),
                        rss_bytes: sample_rss(),
                    });
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
            samples: Arc::try_unwrap(samples)
                .map(|m| {
                    m.into_inner()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                })
                .unwrap_or_default(),
        })
    })
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
        // Pass needs all three: budget ok, writes, deliveries.
        assert!(soak_pass(true, 100, 5));
        assert!(!soak_pass(false, 100, 5)); // over budget
        assert!(!soak_pass(true, 0, 5)); // no writes
        assert!(!soak_pass(true, 100, 0)); // subscriptions silent
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
            rss_samples: vec![RssSample {
                t_secs: 0.0,
                rss_bytes: 80 * 1024 * 1024,
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

        let dir = std::env::temp_dir().join(format!("fluxum-soak-test-{}", std::process::id()));
        report.write_artifacts(&dir, "soak-report").unwrap();
        let json = std::fs::read_to_string(dir.join("soak-report.json")).unwrap();
        assert!(json.contains("\"pass\": true"));
        assert!(dir.join("soak-report.md").exists());
        // Round-trips as the report shape.
        let back: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back["rows_loaded"], 1_000_000_000i64);
        let _ = std::fs::remove_dir_all(&dir);
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
