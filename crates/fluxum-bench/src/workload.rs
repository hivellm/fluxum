//! The parity workloads (TST-092), written once against [`Side`] so both
//! stacks run **exactly the same client behavior** (TST-090) — the harness
//! cannot accidentally favor Fluxum by driving it differently.
//!
//! Every workload follows the honesty protocol (TST-091): a documented
//! warmup precedes each measured window, and each workload is run multiple
//! times so the report can state variance.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use crate::measure::RunResult;

/// One comparison side (Fluxum, or app-server + PostgreSQL/SQLite): a
/// factory for identically-behaving clients.
pub trait Side: Sync {
    /// Short stable name for reports ("fluxum", "postgres", "sqlite").
    fn name(&self) -> &'static str;
    /// Open a client session. `seed` distinguishes identities/users so the
    /// same seed means the same user on either side.
    fn client(&self, seed: u64) -> Result<Box<dyn BenchClient>, String>;
}

/// What every side's client can do — the demo application's operations
/// (chat + tasks + live subscriptions), nothing Fluxum-specific.
pub trait BenchClient: Send {
    /// The small-write operation: create a task, awaited until acknowledged
    /// (Fluxum: reducer ack; SQL side: INSERT committed and HTTP 2xx).
    fn add_task(&mut self, title: &str) -> Result<(), String>;
    /// Post a chat message to a channel, awaited like [`Self::add_task`].
    fn send_chat(&mut self, channel: u32, content: &str) -> Result<(), String>;
    /// Subscribe to a channel's messages; `on_message` fires once per
    /// delivered message with its content, as delivery happens (Fluxum:
    /// `TxUpdate`; SQL side: LISTEN/NOTIFY-driven push).
    fn subscribe_chat(
        &mut self,
        channel: u32,
        on_message: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<(), String>;
}

/// Shared knobs for a workload run (TST-091 warmup + multi-run).
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Concurrent client sessions.
    pub clients: usize,
    /// Unmeasured warmup preceding every measured window.
    pub warmup: Duration,
    /// The measured window.
    pub measure: Duration,
    /// Independent repetitions (variance is reported across these).
    pub runs: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig {
            clients: 8,
            warmup: Duration::from_secs(2),
            measure: Duration::from_secs(10),
            runs: 3,
        }
    }
}

/// TST-092 (a): write throughput — `clients` sessions each looping the
/// acknowledged small write as fast as the side allows. Returns one
/// [`RunResult`] per run with per-op latencies.
pub fn write_workload(side: &dyn Side, cfg: &RunConfig) -> Result<Vec<RunResult>, String> {
    let mut results = Vec::with_capacity(cfg.runs);
    for run in 0..cfg.runs {
        results.push(write_run(side, cfg, run as u64)?);
    }
    Ok(results)
}

fn write_run(side: &dyn Side, cfg: &RunConfig, run: u64) -> Result<RunResult, String> {
    // Connect every client BEFORE the clock starts: session setup is not
    // write throughput.
    let mut clients = Vec::with_capacity(cfg.clients);
    for c in 0..cfg.clients {
        clients.push(side.client(run * 10_000 + c as u64)?);
    }

    let start_gate = Arc::new(Barrier::new(cfg.clients + 1));
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let failed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let handles: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(idx, mut client)| {
            let start_gate = Arc::clone(&start_gate);
            let measuring = Arc::clone(&measuring);
            let stop = Arc::clone(&stop);
            let failed = Arc::clone(&failed);
            std::thread::spawn(move || -> Vec<u64> {
                let mut latencies = Vec::new();
                let mut i = 0u64;
                start_gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    let title = format!("bench task {idx}-{i}");
                    i += 1;
                    let began = Instant::now();
                    if let Err(e) = client.add_task(&title) {
                        *failed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some(e);
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                    if measuring.load(Ordering::Relaxed) {
                        latencies.push(began.elapsed().as_nanos() as u64);
                    }
                }
                latencies
            })
        })
        .collect();

    start_gate.wait();
    std::thread::sleep(cfg.warmup);
    measuring.store(true, Ordering::Relaxed);
    let window_start = Instant::now();
    std::thread::sleep(cfg.measure);
    measuring.store(false, Ordering::Relaxed);
    let wall = window_start.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut latencies_ns = Vec::new();
    for handle in handles {
        latencies_ns.extend(handle.join().map_err(|_| "client thread panicked")?);
    }
    if let Some(e) = failed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        return Err(format!("write workload client failed: {e}"));
    }
    Ok(RunResult {
        ops: latencies_ns.len() as u64,
        wall,
        latencies_ns,
    })
}

/// Knobs for the end-to-end fan-out workload.
#[derive(Debug, Clone)]
pub struct E2eConfig {
    /// Subscriber sessions receiving every message.
    pub subscribers: usize,
    /// Writer send rate, messages/second (kept under the demo module's
    /// 20/s per-identity chat rate limit on the Fluxum side — the SAME
    /// offered load is used on the baseline).
    pub rate_per_sec: u32,
    /// Messages sent inside the measured window, per run.
    pub messages: u32,
    /// Unmeasured warmup messages preceding each window.
    pub warmup_messages: u32,
    /// Independent repetitions.
    pub runs: usize,
}

impl Default for E2eConfig {
    fn default() -> Self {
        E2eConfig {
            subscribers: 50,
            rate_per_sec: 10,
            messages: 100,
            warmup_messages: 10,
            runs: 3,
        }
    }
}

/// TST-092 (b): end-to-end change→subscriber latency. One writer posts chat
/// messages carrying their send instant; every subscriber's push callback
/// timestamps receipt (same machine, same clock). `ops` counts deliveries
/// (messages × subscribers); latencies are per delivery.
pub fn e2e_workload(side: &dyn Side, cfg: &E2eConfig) -> Result<Vec<RunResult>, String> {
    let mut results = Vec::with_capacity(cfg.runs);
    for run in 0..cfg.runs {
        results.push(e2e_run(side, cfg, run as u64)?);
    }
    Ok(results)
}

fn e2e_run(side: &dyn Side, cfg: &E2eConfig, run: u64) -> Result<RunResult, String> {
    // Every message body is "<nanos-since-epoch> <padding>", the epoch being
    // an Instant shared by writer and subscribers — one process, one clock,
    // no cross-machine skew to argue about (TST-091 records this).
    let epoch = Instant::now();
    let channel = 7000 + run as u32; // fresh channel per run: no replayed history
    let measuring = Arc::new(AtomicBool::new(false));
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(AtomicU64::new(0));

    let mut subscribers = Vec::with_capacity(cfg.subscribers);
    for s in 0..cfg.subscribers {
        let mut client = side.client(run * 10_000 + 100 + s as u64)?;
        let measuring = Arc::clone(&measuring);
        let latencies = Arc::clone(&latencies);
        let delivered = Arc::clone(&delivered);
        client.subscribe_chat(
            channel,
            Box::new(move |content| {
                let now_ns = epoch.elapsed().as_nanos() as u64;
                let Some(sent_ns) = content
                    .split(' ')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                else {
                    return; // not a bench message
                };
                if measuring.load(Ordering::Relaxed) {
                    latencies
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(now_ns.saturating_sub(sent_ns));
                    delivered.fetch_add(1, Ordering::Relaxed);
                }
            }),
        )?;
        subscribers.push(client);
    }

    let mut writer = side.client(run * 10_000 + 99)?;
    let gap = Duration::from_secs_f64(1.0 / f64::from(cfg.rate_per_sec.max(1)));
    let mut send = |n: u32| -> Result<(), String> {
        let body = format!("{} m{n}", epoch.elapsed().as_nanos());
        writer.send_chat(channel, &body)?;
        std::thread::sleep(gap);
        Ok(())
    };

    for n in 0..cfg.warmup_messages {
        send(n)?;
    }
    measuring.store(true, Ordering::Relaxed);
    let window_start = Instant::now();
    for n in 0..cfg.messages {
        send(cfg.warmup_messages + n)?;
    }
    // Grace period for the tail of deliveries to land before the window
    // closes; deliveries slower than this are lost from the sample, which a
    // p99 target of milliseconds makes irrelevant.
    let expected = u64::from(cfg.messages) * cfg.subscribers as u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    while delivered.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let wall = window_start.elapsed();
    measuring.store(false, Ordering::Relaxed);

    let latencies_ns = std::mem::take(
        &mut *latencies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    let got = latencies_ns.len() as u64;
    if got < expected {
        return Err(format!(
            "e2e run delivered {got}/{expected} messages within the grace period"
        ));
    }
    Ok(RunResult {
        ops: got,
        wall,
        latencies_ns,
    })
}
