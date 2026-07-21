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
    /// Seed `rows` tasks for this user and make them readable: on Fluxum the
    /// client subscribes and materializes its live view (the app-side map a
    /// real app keeps); on the SQL side the rows just need to exist. Called
    /// once before a read loop; not measured.
    fn prepare_reads(&mut self, rows: u32) -> Result<(), String>;
    /// One hot single-row read of this user's data — the NFR-11 comparison
    /// as the PRD states it: **in-process** (Fluxum: local live-view lookup)
    /// vs **SQL round trip** (baseline: indexed single-row SELECT over
    /// HTTP). Returns the title read, so neither side can skip the work.
    fn hot_read(&mut self) -> Result<String, String>;
    /// Load ALL of this user's tasks in one operation — "open the app after
    /// a cold start". Fluxum: a fresh subscription's `InitialData`, applied;
    /// baseline: the indexed `SELECT` of every row over HTTP. Returns the
    /// row count so the driver can assert both sides read the same data.
    fn load_my_data(&mut self) -> Result<u32, String>;
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

/// Knobs for the hot-read workload.
#[derive(Debug, Clone)]
pub struct HotReadConfig {
    /// Concurrent reader sessions.
    pub clients: usize,
    /// Tasks seeded per reader before measurement.
    pub rows_per_client: u32,
    /// Unmeasured warmup preceding the window.
    pub warmup: Duration,
    /// The measured window.
    pub measure: Duration,
    /// Independent repetitions.
    pub runs: usize,
}

impl Default for HotReadConfig {
    fn default() -> Self {
        HotReadConfig {
            clients: 4,
            rows_per_client: 100,
            warmup: Duration::from_secs(1),
            measure: Duration::from_secs(5),
            runs: 3,
        }
    }
}

/// TST-092 (c): hot read latency — each client loops single-row reads of its
/// own seeded data. Seeding and (on Fluxum) subscription materialization
/// happen before the clock starts.
pub fn hot_read_workload(side: &dyn Side, cfg: &HotReadConfig) -> Result<Vec<RunResult>, String> {
    let mut results = Vec::with_capacity(cfg.runs);
    for run in 0..cfg.runs {
        results.push(hot_read_run(side, cfg, run as u64)?);
    }
    Ok(results)
}

fn hot_read_run(side: &dyn Side, cfg: &HotReadConfig, run: u64) -> Result<RunResult, String> {
    let mut clients = Vec::with_capacity(cfg.clients);
    for c in 0..cfg.clients {
        let mut client = side.client(run * 10_000 + 500 + c as u64)?;
        client.prepare_reads(cfg.rows_per_client)?;
        clients.push(client);
    }

    let start_gate = Arc::new(Barrier::new(cfg.clients + 1));
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let failed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            let start_gate = Arc::clone(&start_gate);
            let measuring = Arc::clone(&measuring);
            let stop = Arc::clone(&stop);
            let failed = Arc::clone(&failed);
            std::thread::spawn(move || -> Vec<u64> {
                let mut latencies = Vec::new();
                start_gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    let began = Instant::now();
                    match client.hot_read() {
                        Ok(title) => {
                            let elapsed = began.elapsed().as_nanos() as u64;
                            // The read's result flows into a hint the
                            // optimizer cannot see through, so the lookup
                            // is never dead code on either side.
                            std::hint::black_box(&title);
                            if measuring.load(Ordering::Relaxed) {
                                latencies.push(elapsed);
                            }
                        }
                        Err(e) => {
                            *failed
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
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
        latencies_ns.extend(
            handle
                .join()
                .map_err(|_| "reader thread panicked".to_owned())?,
        );
    }
    if let Some(e) = failed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        return Err(format!("hot-read workload client failed: {e}"));
    }
    Ok(RunResult {
        ops: latencies_ns.len() as u64,
        wall,
        latencies_ns,
    })
}

/// Knobs for the cold-read workload.
#[derive(Debug, Clone)]
pub struct ColdReadConfig {
    /// Users whose data is seeded. Sized (with `rows_per_user`) so the
    /// dataset overflows the Fluxum side's configured memory budget — that
    /// is what makes the post-restart reads page-in from the cold tier.
    pub users: u32,
    /// Tasks seeded per user.
    pub rows_per_user: u32,
    /// Users sampled for the measured cold loads (each on a fresh session).
    pub sample_users: u32,
    /// Independent repetitions. Every run re-restarts the servers; the seed
    /// is shared (seeding is idempotent enough for reads — rows only grow).
    pub runs: usize,
}

impl Default for ColdReadConfig {
    fn default() -> Self {
        ColdReadConfig {
            users: 64,
            rows_per_user: 500,
            sample_users: 16,
            runs: 3,
        }
    }
}

/// TST-092 (d): cold (page-in) reads. Seeds `users × rows_per_user` tasks,
/// then for each run: `restart` both server-side caches away, and measure
/// "load my data" for `sample_users` fresh sessions — first touch of each
/// user's pages. `restart` is environment-provided (kill/relaunch a spawned
/// server, `docker restart` for PostgreSQL): the workload cannot know how
/// its servers were started.
///
/// Honesty note for the report: a restart empties the *database's* caches
/// (Fluxum buffer pool / PG shared_buffers) on both sides symmetrically;
/// the OS page cache is not dropped on either side, so this measures
/// database-level page-in, not platter latency.
pub fn cold_read_workload(
    side: &dyn Side,
    restart: &dyn Fn() -> Result<(), String>,
    cfg: &ColdReadConfig,
) -> Result<Vec<RunResult>, String> {
    // Seed once: rows_per_user tasks under each user's identity.
    for user in 0..cfg.users {
        let mut client = side.client(u64::from(user))?;
        for i in 0..cfg.rows_per_user {
            client.add_task(&format!("cold seed {i}"))?;
        }
    }

    let mut results = Vec::with_capacity(cfg.runs);
    for run in 0..cfg.runs {
        restart()?;
        let mut latencies_ns = Vec::with_capacity(cfg.sample_users as usize);
        let window_start = Instant::now();
        for s in 0..cfg.sample_users {
            // Sample users rotate across runs so a run never re-reads pages
            // the previous run's samples just heated.
            let user = (run as u32 * cfg.sample_users + s) % cfg.users;
            // The session is opened OUTSIDE the timed op: connection setup
            // is not a page-in.
            let mut client = side.client(u64::from(user))?;
            let began = Instant::now();
            let rows = client.load_my_data()?;
            latencies_ns.push(began.elapsed().as_nanos() as u64);
            if rows < cfg.rows_per_user {
                return Err(format!(
                    "cold load for user {user} returned {rows} rows, seeded {}",
                    cfg.rows_per_user
                ));
            }
        }
        results.push(RunResult {
            ops: u64::from(cfg.sample_users),
            wall: window_start.elapsed(),
            latencies_ns,
        });
    }
    Ok(results)
}

/// Knobs for the mixed workload (TST-092 e): writes, reads, and live
/// subscribers at the same time — the shape of a real deployment, where
/// each class contends with the others.
#[derive(Debug, Clone)]
pub struct MixedConfig {
    /// Clients looping the acked small write.
    pub writers: usize,
    /// Clients looping the hot single-row read.
    pub readers: usize,
    /// Tasks seeded per reader before measurement.
    pub rows_per_reader: u32,
    /// Live subscribers to the chat channel.
    pub subscribers: usize,
    /// Chat sender rate, messages/second (under the 20/s limit).
    pub rate_per_sec: u32,
    /// Unmeasured warmup preceding the window.
    pub warmup: Duration,
    /// The measured window.
    pub measure: Duration,
    /// Independent repetitions.
    pub runs: usize,
}

impl Default for MixedConfig {
    fn default() -> Self {
        MixedConfig {
            writers: 4,
            readers: 4,
            rows_per_reader: 100,
            subscribers: 20,
            rate_per_sec: 10,
            warmup: Duration::from_secs(2),
            measure: Duration::from_secs(10),
            runs: 3,
        }
    }
}

/// One mixed run's results, per operation class.
#[derive(Debug, Clone)]
pub struct MixedRun {
    /// Acked small writes under contention.
    pub write: RunResult,
    /// Hot reads under contention.
    pub read: RunResult,
    /// Change→subscriber deliveries under contention.
    pub e2e: RunResult,
}

/// TST-092 (e): the mixed workload. Every class runs concurrently over the
/// same measured window; results come back per class so the report can show
/// what contention does to each.
pub fn mixed_workload(side: &dyn Side, cfg: &MixedConfig) -> Result<Vec<MixedRun>, String> {
    let mut results = Vec::with_capacity(cfg.runs);
    for run in 0..cfg.runs {
        results.push(mixed_run(side, cfg, run as u64)?);
    }
    Ok(results)
}

fn mixed_run(side: &dyn Side, cfg: &MixedConfig, run: u64) -> Result<MixedRun, String> {
    let epoch = Instant::now();
    let channel = 8000 + run as u32;

    // Sessions first; none of this is measured.
    let mut writers = Vec::with_capacity(cfg.writers);
    for c in 0..cfg.writers {
        writers.push(side.client(run * 10_000 + 1000 + c as u64)?);
    }
    let mut readers = Vec::with_capacity(cfg.readers);
    for c in 0..cfg.readers {
        let mut client = side.client(run * 10_000 + 2000 + c as u64)?;
        client.prepare_reads(cfg.rows_per_reader)?;
        readers.push(client);
    }
    let measuring = Arc::new(AtomicBool::new(false));
    let e2e_latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut subscribers = Vec::with_capacity(cfg.subscribers);
    for s in 0..cfg.subscribers {
        let mut client = side.client(run * 10_000 + 3000 + s as u64)?;
        let measuring = Arc::clone(&measuring);
        let e2e_latencies = Arc::clone(&e2e_latencies);
        client.subscribe_chat(
            channel,
            Box::new(move |content| {
                let now_ns = epoch.elapsed().as_nanos() as u64;
                let Some(sent_ns) = content
                    .split(' ')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                else {
                    return;
                };
                if measuring.load(Ordering::Relaxed) {
                    e2e_latencies
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(now_ns.saturating_sub(sent_ns));
                }
            }),
        )?;
        subscribers.push(client);
    }
    let mut chat_sender = side.client(run * 10_000 + 999)?;

    let participants = cfg.writers + cfg.readers + 1; // + the chat sender
    let start_gate = Arc::new(Barrier::new(participants + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let failed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let fail = |failed: &Arc<Mutex<Option<String>>>, stop: &Arc<AtomicBool>, e: String| {
        *failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
        stop.store(true, Ordering::Relaxed);
    };

    let mut write_handles = Vec::new();
    for (idx, mut client) in writers.into_iter().enumerate() {
        let (start_gate, measuring, stop, failed) = (
            Arc::clone(&start_gate),
            Arc::clone(&measuring),
            Arc::clone(&stop),
            Arc::clone(&failed),
        );
        write_handles.push(std::thread::spawn(move || -> Vec<u64> {
            let mut latencies = Vec::new();
            let mut i = 0u64;
            start_gate.wait();
            while !stop.load(Ordering::Relaxed) {
                let title = format!("mixed task {idx}-{i}");
                i += 1;
                let began = Instant::now();
                if let Err(e) = client.add_task(&title) {
                    fail(&failed, &stop, e);
                    break;
                }
                if measuring.load(Ordering::Relaxed) {
                    latencies.push(began.elapsed().as_nanos() as u64);
                }
            }
            latencies
        }));
    }

    let mut read_handles = Vec::new();
    for mut client in readers {
        let (start_gate, measuring, stop, failed) = (
            Arc::clone(&start_gate),
            Arc::clone(&measuring),
            Arc::clone(&stop),
            Arc::clone(&failed),
        );
        read_handles.push(std::thread::spawn(move || -> Vec<u64> {
            let mut latencies = Vec::new();
            start_gate.wait();
            while !stop.load(Ordering::Relaxed) {
                let began = Instant::now();
                match client.hot_read() {
                    Ok(title) => {
                        let elapsed = began.elapsed().as_nanos() as u64;
                        std::hint::black_box(&title);
                        if measuring.load(Ordering::Relaxed) {
                            latencies.push(elapsed);
                        }
                    }
                    Err(e) => {
                        fail(&failed, &stop, e);
                        break;
                    }
                }
            }
            latencies
        }));
    }

    let sender_handle = {
        let (start_gate, stop, failed) = (
            Arc::clone(&start_gate),
            Arc::clone(&stop),
            Arc::clone(&failed),
        );
        let gap = Duration::from_secs_f64(1.0 / f64::from(cfg.rate_per_sec.max(1)));
        std::thread::spawn(move || {
            start_gate.wait();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let body = format!("{} mixed{n}", epoch.elapsed().as_nanos());
                n += 1;
                if let Err(e) = chat_sender.send_chat(channel, &body) {
                    fail(&failed, &stop, e);
                    break;
                }
                std::thread::sleep(gap);
            }
        })
    };

    start_gate.wait();
    std::thread::sleep(cfg.warmup);
    measuring.store(true, Ordering::Relaxed);
    let window_start = Instant::now();
    std::thread::sleep(cfg.measure);
    measuring.store(false, Ordering::Relaxed);
    let wall = window_start.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut write_ns = Vec::new();
    for handle in write_handles {
        write_ns.extend(handle.join().map_err(|_| "writer thread panicked".to_owned())?);
    }
    let mut read_ns = Vec::new();
    for handle in read_handles {
        read_ns.extend(handle.join().map_err(|_| "reader thread panicked".to_owned())?);
    }
    sender_handle
        .join()
        .map_err(|_| "chat sender thread panicked".to_owned())?;
    if let Some(e) = failed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        return Err(format!("mixed workload client failed: {e}"));
    }
    let e2e_ns = std::mem::take(
        &mut *e2e_latencies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    let result = |ns: Vec<u64>| RunResult {
        ops: ns.len() as u64,
        wall,
        latencies_ns: ns,
    };
    Ok(MixedRun {
        write: result(write_ns),
        read: result(read_ns),
        e2e: result(e2e_ns),
    })
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
