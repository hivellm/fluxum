//! T3.4 tick-drift verification suite (SPEC-004 RED-020 acceptance 5; FR-21;
//! DAG exit test): 60 Hz over 10 s executes 600 ± 1 times with no cumulative
//! drift (absolute-target clock); a 1–3-period stall re-fires immediately
//! with no warning; a stall past 3 periods logs exactly one warning and
//! resets the clock with no catch-up burst; a tick never runs concurrently
//! with itself.
//!
//! One sequential test: the phases share a wall clock and per-process probe
//! statics, so running them in parallel test threads would contaminate both
//! the timing and the counters.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fluxum_core::Result;
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{FluxValue, ReducerContext, ReducerDef, ReducerRegistry};
use fluxum_core::scheduler::{Scheduler, SchedulerHandle, SchedulerOptions, TickDef};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

const SHARD: u32 = 13;

static COUNT: AtomicU64 = AtomicU64::new(0);
static STALL_ONCE_US: AtomicU64 = AtomicU64::new(0);
static IN_FLIGHT: AtomicBool = AtomicBool::new(false);
static OVERLAPPED: AtomicBool = AtomicBool::new(false);
static FIRE_TIMES: Mutex<Vec<Instant>> = Mutex::new(Vec::new());

fn counting_tick(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    if IN_FLIGHT.swap(true, Ordering::SeqCst) {
        OVERLAPPED.store(true, Ordering::SeqCst);
    }
    FIRE_TIMES.lock().unwrap().push(Instant::now());
    COUNT.fetch_add(1, Ordering::SeqCst);
    // A one-time injected stall (µs), armed by the drift phases.
    let stall = STALL_ONCE_US.swap(0, Ordering::SeqCst);
    if stall > 0 {
        std::thread::sleep(Duration::from_micros(stall));
    }
    IN_FLIGHT.store(false, Ordering::SeqCst);
    Ok(())
}

fn check_none(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static COUNTING: ReducerDef = ReducerDef {
    name: "counting_tick",
    handler: counting_tick,
    check_args: check_none,
    client_callable: false,
    max_rate_per_sec: 0,
};

static TICK_60HZ: TickDef = TickDef {
    name: "counting_tick",
    rate_hz: 60,
};
static TICK_20HZ: TickDef = TickDef {
    name: "counting_tick",
    rate_hz: 20,
};

fn reset_probes() {
    COUNT.store(0, Ordering::SeqCst);
    STALL_ONCE_US.store(0, Ordering::SeqCst);
    IN_FLIGHT.store(false, Ordering::SeqCst);
    OVERLAPPED.store(false, Ordering::SeqCst);
    FIRE_TIMES.lock().unwrap().clear();
}

async fn start_tick(dir: &std::path::Path, tick: &'static TickDef) -> SchedulerHandle {
    let schema = Schema::from_tables(std::iter::empty::<&'static _>()).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) = TxPipeline::new(store, log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(ReducerRegistry::from_defs([&COUNTING]).unwrap());
    Scheduler::new(
        pipeline,
        registry,
        SHARD,
        fluxum_core::auth::server_identity("tick-test"),
        SchedulerOptions::default(),
        vec![tick],
        vec![],
    )
    .unwrap()
    .start()
    .await
    .unwrap()
}

/// One 60 Hz × 10 s run: `Ok(count)` when it ran unperturbed (no warning);
/// `Err(())` when the host hiccuped hard enough (>3 periods) to reset the
/// clock — the caller retries, because that run measured the machine, not
/// the scheduler.
async fn run_600_phase() -> std::result::Result<u64, ()> {
    reset_probes();
    let dir = tempfile::tempdir().unwrap();
    let handle = start_tick(dir.path(), &TICK_60HZ).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    // Stop before sampling. Reading the two counters off a live scheduler
    // races it: a tick firing between the `COUNT` load and the `executions`
    // load makes the stats look one ahead of the firings, and the comparison
    // below fails on a scheduler that did nothing wrong.
    let stats = Arc::clone(handle.tick_stats("counting_tick").unwrap());
    handle.stop().await;
    let counted = COUNT.load(Ordering::SeqCst);
    let warnings = stats.warnings.load(Ordering::SeqCst);
    let executions = stats.executions.load(Ordering::SeqCst);
    assert_eq!(executions, counted, "stats mirror the firings");
    assert!(
        !OVERLAPPED.load(Ordering::SeqCst),
        "never concurrent with itself"
    );
    if warnings > 0 {
        return Err(());
    }
    Ok(counted)
}

#[tokio::test(flavor = "multi_thread")]
async fn fixed_timestep_drift_semantics() {
    // Real-time timing has no meaning under coverage instrumentation, which
    // slows every firing enough to falsely trip the stall detector. Skip
    // under `cargo llvm-cov` (which sets LLVM_PROFILE_FILE); the test runs
    // in full in the normal `cargo test` / CI test job.
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() {
        eprintln!("tick_drift: skipped under coverage instrumentation");
        return;
    }

    // --- Phase 1 (DAG exit): 60 Hz over 10 s = 600 ± 1, no cumulative
    // drift. The absolute-target clock makes the count exact regardless of
    // per-firing jitter; a >3-period host freeze resets the clock (that is
    // RED-020 behavior, not drift), so such a run is retried.
    let mut counted = None;
    for attempt in 1..=3 {
        match run_600_phase().await {
            Ok(count) => {
                counted = Some(count);
                break;
            }
            Err(()) => eprintln!("phase 1 attempt {attempt}: host stall reset the clock; retrying"),
        }
    }
    let counted = counted.expect("3 consecutive >3-period host stalls: not a scheduler bug");
    assert!(
        (599..=601).contains(&counted),
        "60 Hz over 10 s must execute 600 ± 1 times, got {counted}"
    );

    // --- Phase 2: a 1–3-period stall re-fires immediately, no warning.
    reset_probes();
    let dir = tempfile::tempdir().unwrap();
    // 20 Hz → 50 ms period; the firing after the 4th stalls 2 periods.
    let handle = start_tick(dir.path(), &TICK_20HZ).await;
    while COUNT.load(Ordering::SeqCst) < 4 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    STALL_ONCE_US.store(100_000, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(600)).await;
    let stats = handle.tick_stats("counting_tick").unwrap();
    let warnings = stats.warnings.load(Ordering::SeqCst);
    let executions = stats.executions.load(Ordering::SeqCst);
    handle.stop().await;
    assert_eq!(warnings, 0, "a <=3-period stall must not warn or reset");
    assert!(
        executions >= 12,
        "the clock must catch up with immediate re-fires, got {executions}"
    );
    assert!(
        !OVERLAPPED.load(Ordering::SeqCst),
        "never concurrent with itself"
    );

    // --- Phase 3: a >3-period stall warns exactly once, resets, no burst.
    reset_probes();
    let dir = tempfile::tempdir().unwrap();
    // 20 Hz → 50 ms period; one firing stalls 6 periods (300 ms).
    let handle = start_tick(dir.path(), &TICK_20HZ).await;
    while COUNT.load(Ordering::SeqCst) < 3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    STALL_ONCE_US.store(300_000, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(700)).await;
    let stats = handle.tick_stats("counting_tick").unwrap();
    let warnings = stats.warnings.load(Ordering::SeqCst);
    handle.stop().await;
    assert_eq!(warnings, 1, "exactly one warning per stall event (RED-020)");

    // No catch-up burst: after the stalled firing ends, the reset clock
    // fires once immediately and then returns to the period — an
    // accumulating scheduler would fire ~6 times back-to-back.
    let times = FIRE_TIMES.lock().unwrap();
    let stall_end = times
        .windows(2)
        .position(|w| w[1].duration_since(w[0]) >= Duration::from_millis(250))
        .expect("the 300 ms stall gap is visible in the fire times");
    let after = &times[stall_end + 1..];
    let burst = after
        .windows(2)
        .take(4)
        .filter(|w| w[1].duration_since(w[0]) < Duration::from_millis(25))
        .count();
    assert!(
        burst <= 1,
        "at most the single immediate post-reset re-fire may be sub-period, \
         got {burst} back-to-back firings"
    );
}
