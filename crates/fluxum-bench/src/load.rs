//! Sustained load test (SPEC-013 TST-060 / NFR-01): drive the small-write
//! reducer as hard as the shard accepts for a measured window and report
//! the throughput **the way TST-060 mandates — via the
//! `fluxum_reducer_calls_total` counter delta over the window**, with the
//! errored-call count (which must be zero).
//!
//! Connections pipeline their calls (SDK-032): an acked-serial connection
//! measures round-trip latency, not engine throughput (F-007). The window,
//! connection count and pipeline depth are recorded alongside the number.
//!
//! The fan-out companion (TST-061) is [`fanout_latency`]: 1,000 subscribers
//! on a hot channel while a sender commits, measuring commit→receipt p99.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::measure::percentile_ns;
use crate::workload::Side;

/// One `fluxum_reducer_calls_total` reading, split by outcome — the TST-060
/// measurement surface.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReducerCounts {
    /// Committed calls (`outcome="ok"`).
    pub ok: u64,
    /// Business-error + panic rollbacks (`outcome="err"`).
    pub err: u64,
    /// Admission refusals (`outcome="queue_full"` — shard busy, TXN-011).
    pub queue_full: u64,
}

impl ReducerCounts {
    /// The per-outcome delta since `earlier`.
    #[must_use]
    pub fn since(self, earlier: ReducerCounts) -> ReducerCounts {
        ReducerCounts {
            ok: self.ok.saturating_sub(earlier.ok),
            err: self.err.saturating_sub(earlier.err),
            queue_full: self.queue_full.saturating_sub(earlier.queue_full),
        }
    }
}

/// Sum `fluxum_reducer_calls_total{...,outcome="..."}` across every reducer
/// in a Prometheus exposition body. Counter-delta throughput (TST-060) reads
/// the totals, never the histogram.
#[must_use]
pub fn parse_reducer_counts(metrics: &str) -> ReducerCounts {
    let mut counts = ReducerCounts::default();
    for line in metrics.lines() {
        let line = line.trim();
        if !line.starts_with("fluxum_reducer_calls_total{") {
            continue;
        }
        let Some((labels, value)) = line.rsplit_once('}') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else {
            continue;
        };
        if labels.contains("outcome=\"ok\"") {
            counts.ok += value;
        } else if labels.contains("outcome=\"err\"") {
            counts.err += value;
        } else if labels.contains("outcome=\"queue_full\"") {
            counts.queue_full += value;
        }
    }
    counts
}

/// One-shot `GET /metrics` against `http_addr` (`host:port`), returning the
/// Prometheus body. The admin transport keeps the connection alive, so the
/// body is read to its `Content-Length` rather than to EOF (a `read_to_end`
/// would block on the kept-open socket until the read timeout).
pub fn scrape_metrics(http_addr: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(http_addr)
        .map_err(|e| format!("connect {http_addr}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    let request =
        format!("GET /metrics HTTP/1.1\r\nHost: {http_addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;

    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    let body_start = loop {
        let n = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("connection closed before /metrics headers".to_owned());
        }
        raw.extend_from_slice(&chunk[..n]);
        if let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break split + 4;
        }
    };
    let head = String::from_utf8_lossy(&raw[..body_start]).into_owned();
    let content_length: Option<usize> = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    });
    match content_length {
        Some(length) => {
            while raw.len() < body_start + length {
                let n = stream.read(&mut chunk).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&chunk[..n]);
            }
        }
        None => loop {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => raw.extend_from_slice(&chunk[..n]),
            }
        },
    }
    Ok(String::from_utf8_lossy(&raw[body_start..]).into_owned())
}

/// Load-test knobs (TST-060).
#[derive(Debug, Clone)]
pub struct LoadConfig {
    /// Concurrent pipelined connections.
    pub connections: usize,
    /// Acked writes each connection keeps in flight (SDK-032).
    pub pipeline: usize,
    /// Unmeasured warmup before the window.
    pub warmup: Duration,
    /// The measured window (TST-060 asks for ≥ 60 s).
    pub measure: Duration,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            connections: 16,
            pipeline: 32,
            warmup: Duration::from_secs(3),
            measure: Duration::from_secs(60),
        }
    }
}

/// The load-test result — what the report records.
#[derive(Debug, Clone)]
pub struct LoadResult {
    /// Committed calls/s over the window, by the counter delta (TST-060).
    pub calls_per_sec: f64,
    /// The raw counter delta across the window.
    pub delta: ReducerCounts,
    /// The measured wall time.
    pub wall: Duration,
    /// Whether NFR-01's ≥ 100,000/s is met AND no call errored.
    pub met: bool,
}

/// Run the sustained load test: `connections` pipelined writers hammer
/// `add_task` for the window; throughput is the `fluxum_reducer_calls_total`
/// delta / wall (TST-060), and the run only passes with **zero errored
/// calls**. `scrape` returns the current counts (the metrics scrape, or a
/// test double).
pub fn run_load(
    side: &dyn Side,
    cfg: &LoadConfig,
    mut scrape: impl FnMut() -> Result<ReducerCounts, String>,
) -> Result<LoadResult, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(std::sync::Barrier::new(cfg.connections + 1));
    let mut handles = Vec::with_capacity(cfg.connections);
    for c in 0..cfg.connections {
        let mut client = side.client(c as u64)?;
        let stop = Arc::clone(&stop);
        let started = Arc::clone(&started);
        let window = cfg.pipeline.max(1);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let mut inflight = std::collections::VecDeque::with_capacity(window);
            let mut i = 0u64;
            started.wait();
            while !stop.load(Ordering::Relaxed) {
                while inflight.len() < window {
                    match client.start_task(&format!("load {i}")) {
                        Ok(token) => inflight.push_back(token),
                        Err(e) => return Err(e),
                    }
                    i += 1;
                }
                if let Some(token) = inflight.pop_front() {
                    client.finish_task(token)?;
                }
            }
            while let Some(token) = inflight.pop_front() {
                let _ = client.finish_task(token);
            }
            Ok(())
        }));
    }

    started.wait();
    std::thread::sleep(cfg.warmup);
    let before = scrape()?;
    let window_start = Instant::now();
    std::thread::sleep(cfg.measure);
    let wall = window_start.elapsed();
    let after = scrape()?;
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle
            .join()
            .map_err(|_| "load worker panicked".to_owned())??;
    }

    let delta = after.since(before);
    let calls_per_sec = delta.ok as f64 / wall.as_secs_f64();
    Ok(LoadResult {
        calls_per_sec,
        delta,
        wall,
        met: calls_per_sec >= 100_000.0 && delta.err == 0,
    })
}

/// Fan-out latency knobs (TST-061).
#[derive(Debug, Clone)]
pub struct FanoutConfig {
    /// Concurrent subscribers on the hot channel.
    pub subscribers: usize,
    /// Chat messages the sender commits during the window.
    pub messages: u32,
    /// Sender rate (messages/second).
    pub rate_per_sec: u32,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self {
            subscribers: 1_000,
            messages: 200,
            rate_per_sec: 20,
        }
    }
}

/// The fan-out latency result (TST-061: p99 < 5 ms).
#[derive(Debug, Clone)]
pub struct FanoutResult {
    /// Delivered commit→receipt samples (subscribers × messages).
    pub deliveries: u64,
    /// p50 / p99 / max delivery latency (nanoseconds).
    pub p50_ns: u64,
    /// p99 delivery latency (ns).
    pub p99_ns: u64,
    /// Worst delivery latency (ns).
    pub max_ns: u64,
    /// Whether p99 < 5 ms (NFR-04).
    pub met: bool,
}

/// TST-061: `subscribers` clients subscribe to one hot channel, a sender
/// commits `messages` at `rate_per_sec`, and every delivery's commit→receipt
/// latency is measured from a shared in-process instant epoch (no cross-clock
/// reasoning). p99 must be < 5 ms.
pub fn fanout_latency(side: &dyn Side, cfg: &FanoutConfig) -> Result<FanoutResult, String> {
    let epoch = Instant::now();
    let channel = 42_000u32;
    let latencies: Arc<std::sync::Mutex<Vec<u64>>> = Arc::default();

    let mut subscribers = Vec::with_capacity(cfg.subscribers);
    for s in 0..cfg.subscribers {
        // Pace connection opens: a burst of 1,000 from one IP trips the
        // SEC-053 new-connection rate guard (a real defense — the harness
        // paces rather than the server relaxing it). One short yield every
        // 50 keeps well under the per-second budget while staying quick.
        if s > 0 && s % 50 == 0 {
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut client = side.client(1_000_000 + s as u64)?;
        let sink = Arc::clone(&latencies);
        client.subscribe_chat(
            channel,
            Box::new(move |content| {
                let now_ns = epoch.elapsed().as_nanos() as u64;
                if let Some(sent_ns) = content
                    .split(' ')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    sink.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(now_ns.saturating_sub(sent_ns));
                }
            }),
        )?;
        subscribers.push(client);
    }

    let mut sender = side.client(999_999)?;
    let gap = Duration::from_secs_f64(1.0 / f64::from(cfg.rate_per_sec.max(1)));
    let expected = u64::from(cfg.messages) * cfg.subscribers as u64;
    for n in 0..cfg.messages {
        let body = format!("{} fanout{n}", epoch.elapsed().as_nanos());
        sender.send_chat(channel, &body)?;
        std::thread::sleep(gap);
    }

    // Grace for the last commit to reach every subscriber.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let have = latencies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len() as u64;
        if have >= expected || Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let mut samples = std::mem::take(
        &mut *latencies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    if samples.is_empty() {
        return Err("fan-out delivered no messages".to_owned());
    }
    samples.sort_unstable();
    let p99_ns = percentile_ns(&samples, 0.99);
    Ok(FanoutResult {
        deliveries: samples.len() as u64,
        p50_ns: percentile_ns(&samples, 0.50),
        p99_ns,
        max_ns: *samples.last().unwrap_or(&0),
        met: p99_ns < 5_000_000,
    })
}

/// The counter reader used by the live scrape path: scrape `/metrics` and
/// parse the reducer-call totals. The test double for [`run_load`] returns
/// [`ReducerCounts`] directly.
pub fn live_scrape(http_addr: &str) -> Result<ReducerCounts, String> {
    Ok(parse_reducer_counts(&scrape_metrics(http_addr)?))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn reducer_counts_sum_across_reducers_and_split_by_outcome() {
        let metrics = "\
# HELP fluxum_reducer_calls_total calls
fluxum_reducer_calls_total{shard=\"0\",reducer=\"add_task\",outcome=\"ok\"} 900
fluxum_reducer_calls_total{shard=\"0\",reducer=\"send_chat\",outcome=\"ok\"} 100
fluxum_reducer_calls_total{shard=\"0\",reducer=\"add_task\",outcome=\"err\"} 3
fluxum_reducer_calls_total{shard=\"0\",reducer=\"add_task\",outcome=\"queue_full\"} 5
fluxum_up{shard=\"0\"} 1
";
        let counts = parse_reducer_counts(metrics);
        assert_eq!(counts.ok, 1_000);
        assert_eq!(counts.err, 3);
        assert_eq!(counts.queue_full, 5);
    }

    #[test]
    fn counter_delta_is_the_throughput_surface() {
        let before = ReducerCounts {
            ok: 1_000,
            err: 1,
            queue_full: 0,
        };
        let after = ReducerCounts {
            ok: 7_100_000,
            err: 1,
            queue_full: 9,
        };
        let delta = after.since(before);
        assert_eq!(delta.ok, 7_099_000);
        assert_eq!(delta.err, 0, "no NEW errored calls in the window");
        assert_eq!(delta.queue_full, 9);
        // 7.099M ok over 60 s ≈ 118k/s — a met run would look like this.
        assert!((delta.ok as f64 / 60.0) > 100_000.0);
    }

    #[test]
    fn malformed_metric_lines_are_skipped_not_panicked() {
        let metrics = "\
fluxum_reducer_calls_total{outcome=\"ok\"} notanumber
fluxum_reducer_calls_total no-brace-here 5
fluxum_reducer_calls_total{outcome=\"ok\"} 42
";
        assert_eq!(parse_reducer_counts(metrics).ok, 42);
    }
}
