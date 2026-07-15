//! T2.7 kill -9 harness (SPEC-013 TST-020/TST-021/TST-025; checklist 1.1,
//! 1.5, 1.7-kill leg) — process-level crash drills over the real durability
//! stack (`MemStore` + `CommitLog` + `CheckpointRepo`).
//!
//! # Architecture
//!
//! Each drill spawns **this same test binary** as a child process (libtest
//! `--exact child_entry`, selected via `FLUXUM_CRASH_*` env vars). The child
//! runs the deterministic workload of `crash_support` against a data
//! directory, durably records every acknowledged transaction in an `acks`
//! file (fsynced *after* `wait_durable` returns — the ack is the client-visible
//! commit), signals the instrumented boundary through a `ready` file, and
//! parks. The parent then terminates it with the OS equivalent of `kill -9`
//! (`TerminateProcess` on Windows, `SIGKILL` on Unix — `Child::kill`), runs
//! real recovery in-process, and asserts the TST-021 invariants:
//!
//! - **zero acknowledged-transaction loss**: every tx in the acks file is
//!   present after recovery;
//! - **atomicity**: the recovered state equals the deterministic oracle at a
//!   whole-transaction prefix — a partial transaction can never satisfy
//!   row-set equality;
//! - **bounded window**: transactions past the last acknowledgment may be
//!   lost, but only whole (`n` never exceeds what was enqueued);
//! - **index consistency** (TST-025): secondary indexes verify against a
//!   brute-force rebuild after every recovery.
//!
//! Boundary matrix (TST-020): (a) before the commit-log append, (b) after
//! the append but before the ack (the async window — a kill here may also
//! shear the actor's buffered write mid-entry, the torn-write case swept
//! deterministically in `crash_corruption`), (c) after the ack, (d) during a
//! checkpoint (snapshot) write, (e) during commit-log segment rotation, and
//! (f) randomized kills under a flooding workload.
//!
//! Restart/persistence drills (TST-140/TST-141 scope note): there is no
//! server binary yet (it lands with SPEC-006's transport), so the
//! process-level guard here spawns the storage stack directly: it kills the
//! process both gracefully and immediately, restarts it **on the same
//! preserved data directory** across generations, probes readiness via the
//! ready/ack files, captures child stdout/stderr for failed runs, and tears
//! the directory down on success — the TST-140 guard contract applied to
//! the durability layer under test.
//!
//! Runs on Linux, macOS, and Windows CI (TST-026) — `cargo test -p
//! fluxum-core --test crash_kill9`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_core::checkpoint::CheckpointRepo;
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::types::Timestamp;

use crash_support::{
    EPOCH, SHARD, StepOptions, apply_step, apply_step_diff, assert_equals_oracle, fingerprint,
    mem_store, oracle_store, recover_fresh,
};

const READY: &str = "ready";
const ACKS: &str = "acks";

// --- child side ---------------------------------------------------------------

/// Child-mode entry point. A no-op under normal test execution; the real
/// body runs only when the parent sets `FLUXUM_CRASH_ROLE=child`.
#[test]
fn child_entry() {
    if std::env::var("FLUXUM_CRASH_ROLE").as_deref() != Ok("child") {
        return;
    }
    child_main();
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn child_main() {
    let dir = PathBuf::from(std::env::var("FLUXUM_CRASH_DIR").expect("FLUXUM_CRASH_DIR"));
    let mode = std::env::var("FLUXUM_CRASH_MODE").expect("FLUXUM_CRASH_MODE");
    let target = env_u64("FLUXUM_CRASH_TARGET", 0);
    let steps = env_u64("FLUXUM_CRASH_STEPS", 8);
    let ack_every = env_u64("FLUXUM_CRASH_ACK_EVERY", 1);
    let wl = StepOptions {
        heavy: std::env::var("FLUXUM_CRASH_HEAVY").is_ok(),
        with_event: false,
    };
    let opts = CommitLogOptions {
        segment_max_bytes: env_u64("FLUXUM_CRASH_SEG_BYTES", 128 * 1024 * 1024),
        ..CommitLogOptions::default()
    };

    let log_dir = dir.join("log");
    let snap_dir = dir.join("snapshots");
    fs::create_dir_all(&log_dir).unwrap();
    fs::create_dir_all(&snap_dir).unwrap();

    // Generation start: recover whatever the previous generation left behind
    // (TST-141 restart-on-preserved-data-dir), then resume the workload at
    // the recovered prefix.
    let store = mem_store();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let outcome = fluxum_core::checkpoint::recover(&store, &repo, &log_dir, SHARD).unwrap();
    let start = outcome.next_tx_id;

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let append = |i: u64| {
        let diff = apply_step_diff(&store, i, wl);
        rt.block_on(log.append_diff(&diff, Timestamp::from_micros(i64::try_from(i).unwrap())))
            .unwrap();
    };
    let ack = |i: u64| {
        rt.block_on(log.wait_durable(i)).unwrap();
        durable_append_line(&dir.join(ACKS), i);
    };

    match mode.as_str() {
        "graceful" => {
            let last = start + steps - 1;
            for i in start..=last {
                append(i);
            }
            ack(last);
            log.close().unwrap();
            // Normal return: exit code 0 = clean shutdown observed by the
            // parent (the TST-141 graceful leg).
        }
        "flood" => {
            // Append continuously; ack every third tx. The parent watches
            // the ack file grow and kills at a moment of its choosing.
            for i in start..start + 100_000 {
                append(i);
                if i.is_multiple_of(3) {
                    ack(i);
                }
            }
            park();
        }
        boundary => {
            assert!(
                target >= start,
                "target {target} already recovered (start {start})"
            );
            for i in start..=target {
                if i < target {
                    append(i);
                    if i.is_multiple_of(ack_every) {
                        ack(i);
                    }
                    continue;
                }
                match boundary {
                    "before-append" => {
                        // Committed to the in-memory store, never appended:
                        // the tx must vanish atomically.
                        let _ = apply_step_diff(&store, i, wl);
                        signal_ready(&dir);
                        park();
                    }
                    "after-enqueue" => {
                        // Accepted by the flush actor's queue, not yet
                        // acknowledged: may survive or vanish, only whole.
                        append(i);
                        signal_ready(&dir);
                        park();
                    }
                    "after-ack" => {
                        append(i);
                        ack(i);
                        signal_ready(&dir);
                        park();
                    }
                    "during-snapshot" => {
                        append(i);
                        ack(i);
                        let snapshot = store.snapshot();
                        signal_ready(&dir);
                        // The parent's kill lands somewhere inside this
                        // two-phase checkpoint write (TST-020 boundary e).
                        let _ = repo.write(&snapshot, SHARD, i, EPOCH);
                        park();
                    }
                    other => panic!("unknown boundary {other}"),
                }
            }
        }
    }
}

/// Durably append one acknowledged tx id (write + fsync before returning —
/// the ack only counts if it could have been observed by a client).
fn durable_append_line(path: &Path, tx: u64) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{tx}").unwrap();
    file.sync_data().unwrap();
}

fn signal_ready(dir: &Path) {
    let path = dir.join(READY);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_all(b"ready").unwrap();
    file.sync_all().unwrap();
}

/// Park until the parent kills us.
fn park() -> ! {
    loop {
        std::thread::sleep(Duration::from_millis(20));
    }
}

// --- parent side ----------------------------------------------------------------

struct Scenario {
    mode: &'static str,
    target: u64,
    steps: u64,
    seg_bytes: Option<u64>,
    heavy: bool,
}

impl Scenario {
    fn boundary(mode: &'static str, target: u64) -> Self {
        Self {
            mode,
            target,
            steps: 0,
            seg_bytes: None,
            heavy: false,
        }
    }
}

fn spawn_child(dir: &Path, scenario: &Scenario) -> Child {
    let exe = std::env::current_exe().unwrap();
    let stdout = fs::File::create(dir.join("child.stdout")).unwrap();
    let stderr = fs::File::create(dir.join("child.stderr")).unwrap();
    let mut cmd = Command::new(exe);
    cmd.args(["child_entry", "--exact", "--nocapture", "--test-threads=1"])
        .env("FLUXUM_CRASH_ROLE", "child")
        .env("FLUXUM_CRASH_DIR", dir)
        .env("FLUXUM_CRASH_MODE", scenario.mode)
        .env("FLUXUM_CRASH_TARGET", scenario.target.to_string())
        .env("FLUXUM_CRASH_STEPS", scenario.steps.to_string())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if let Some(bytes) = scenario.seg_bytes {
        cmd.env("FLUXUM_CRASH_SEG_BYTES", bytes.to_string());
    }
    if scenario.heavy {
        cmd.env("FLUXUM_CRASH_HEAVY", "1");
    }
    cmd.spawn().unwrap()
}

/// Poll until `predicate` holds, failing with the child's captured output
/// (the TST-140 diagnostics contract) on timeout.
fn wait_until(dir: &Path, child: &mut Child, what: &str, predicate: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "child exited ({status}) before {what}; stderr:\n{}",
                fs::read_to_string(dir.join("child.stderr")).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    panic!(
        "timed out waiting for {what}; stderr:\n{}",
        fs::read_to_string(dir.join("child.stderr")).unwrap_or_default()
    );
}

/// Immediate process termination: `TerminateProcess` / `SIGKILL`.
fn kill9(child: &mut Child) {
    child.kill().unwrap();
    child.wait().unwrap();
}

fn acked(dir: &Path) -> Vec<u64> {
    match fs::read_to_string(dir.join(ACKS)) {
        Ok(text) => text.lines().filter_map(|l| l.trim().parse().ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Run real recovery over the killed process's data dir and assert the
/// TST-021/TST-025 invariants. Returns the recovered prefix length.
fn verify_recovery(dir: &Path, wl: StepOptions, context: &str) -> u64 {
    let (store, outcome) = recover_fresh(&dir.join("log"), &dir.join("snapshots"));
    let n = outcome.last_tx_id.unwrap_or(0);
    let max_ack = acked(dir).into_iter().max().unwrap_or(0);
    assert!(
        n >= max_ack,
        "{context}: acknowledged tx {max_ack} lost — recovery stopped at {n} (TST-021)"
    );
    assert_equals_oracle(&store, n, wl, context);
    n
}

// --- checklist 1.1: the commit-boundary matrix ---------------------------------

#[test]
fn kill_before_append_loses_the_unlogged_tx_atomically() {
    let tmp = tempfile::tempdir().unwrap();
    let scenario = Scenario::boundary("before-append", 7);
    let mut child = spawn_child(tmp.path(), &scenario);
    wait_until(tmp.path(), &mut child, "boundary", || {
        tmp.path().join(READY).exists()
    });
    kill9(&mut child);
    // Every tx < 7 was acked (ack_every = 1); tx 7 never reached the log:
    // recovery lands exactly on prefix 6.
    let n = verify_recovery(tmp.path(), StepOptions::default(), "before-append");
    assert_eq!(n, 6, "nothing past the last append can be recovered");
}

#[test]
fn kill_after_enqueue_is_atomic_within_the_async_window() {
    let tmp = tempfile::tempdir().unwrap();
    let scenario = Scenario::boundary("after-enqueue", 7);
    let mut child = spawn_child(tmp.path(), &scenario);
    wait_until(tmp.path(), &mut child, "boundary", || {
        tmp.path().join(READY).exists()
    });
    kill9(&mut child);
    // Tx 7 was in the async writer window at kill time (NFR-08): it may be
    // durable or lost, but only as a whole — verify_recovery's oracle
    // equality rejects any partial application.
    let n = verify_recovery(tmp.path(), StepOptions::default(), "after-enqueue");
    assert!(
        (6..=7).contains(&n),
        "recovered prefix {n} outside the async window [6, 7]"
    );
}

#[test]
fn kill_after_ack_never_loses_the_acked_tx() {
    let tmp = tempfile::tempdir().unwrap();
    let scenario = Scenario::boundary("after-ack", 7);
    let mut child = spawn_child(tmp.path(), &scenario);
    wait_until(tmp.path(), &mut child, "boundary", || {
        tmp.path().join(READY).exists()
    });
    kill9(&mut child);
    let n = verify_recovery(tmp.path(), StepOptions::default(), "after-ack");
    assert_eq!(
        n, 7,
        "acked tx 7 must be recovered, and nothing was appended past it"
    );
}

// --- checklist 1.1: kill during segment rotation (TST-020 f) --------------------

#[test]
fn kill_during_segment_rotation_keeps_every_acked_tx() {
    for (mode, expect_exact) in [("after-ack", true), ("after-enqueue", false)] {
        let tmp = tempfile::tempdir().unwrap();
        let scenario = Scenario {
            seg_bytes: Some(192), // rotate every couple of entries
            ..Scenario::boundary(mode, 13)
        };
        let mut child = spawn_child(tmp.path(), &scenario);
        wait_until(tmp.path(), &mut child, "boundary", || {
            tmp.path().join(READY).exists()
        });
        kill9(&mut child);
        let context = format!("rotation/{mode}");
        let n = verify_recovery(tmp.path(), StepOptions::default(), &context);
        if expect_exact {
            assert_eq!(n, 13, "{context}");
        } else {
            assert!((12..=13).contains(&n), "{context}: prefix {n}");
        }
        // The multi-segment log stayed appendable after recovery.
        let log = CommitLog::open(
            &tmp.path().join("log"),
            SHARD,
            EPOCH,
            CommitLogOptions {
                segment_max_bytes: 192,
                ..CommitLogOptions::default()
            },
        )
        .unwrap();
        assert_eq!(log.recovery().last_tx_id, Some(n).filter(|&n| n > 0));
        log.close().unwrap();
    }
}

// --- checklist 1.1 / 1.7: kill during a checkpoint write (TST-020 e) ------------

#[test]
fn kill_during_snapshot_write_is_always_recoverable() {
    let tmp = tempfile::tempdir().unwrap();
    let scenario = Scenario {
        heavy: true, // wide rows: the two-phase checkpoint write takes real time
        ..Scenario::boundary("during-snapshot", 5)
    };
    let mut child = spawn_child(tmp.path(), &scenario);
    wait_until(tmp.path(), &mut child, "boundary", || {
        tmp.path().join(READY).exists()
    });
    kill9(&mut child);

    let wl = StepOptions {
        heavy: true,
        with_event: false,
    };
    // Whether the kill landed mid-object, mid-manifest, or after completion:
    // a checkpoint either exists fully verified or not at all (STG-021), and
    // recovery lands on the acked prefix either way.
    let n = verify_recovery(tmp.path(), wl, "during-snapshot");
    assert_eq!(n, 5);

    // Checkpointing resumes after the kill: one more step, a fresh
    // checkpoint, and recovery through it stays exact.
    let (store, outcome) = recover_fresh(&tmp.path().join("log"), &tmp.path().join("snapshots"));
    assert_eq!(outcome.next_tx_id, n + 1);
    apply_step(&store, n + 1, wl);
    let repo = CheckpointRepo::open(&tmp.path().join("snapshots")).unwrap();
    repo.write(&store.snapshot(), SHARD, n + 1, EPOCH)
        .expect("checkpoint writes must resume after a mid-write kill");
    // The new checkpoint alone reproduces the state (log replay adds nothing
    // past it).
    let restored = mem_store();
    let outcome =
        fluxum_core::checkpoint::recover(&restored, &repo, &tmp.path().join("log"), SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(n + 1));
    assert_eq!(fingerprint(&restored), fingerprint(&store));
}

// --- checklist 1.1: randomized kills under flood (TST-020 driver loop) ----------

#[test]
fn randomized_kills_under_flood_lose_nothing_beyond_the_window() {
    // Three rounds with different kill delays after the ack threshold: the
    // kill lands at an arbitrary point of the append/fsync pipeline,
    // including mid-entry (a torn tail the recovery quarantine repairs).
    for (round, delay_ms) in [(1u32, 0u64), (2, 15), (3, 40)] {
        let tmp = tempfile::tempdir().unwrap();
        let scenario = Scenario {
            mode: "flood",
            target: 0,
            steps: 0,
            seg_bytes: Some(4 * 1024), // rotation under flood
            heavy: false,
        };
        let mut child = spawn_child(tmp.path(), &scenario);
        wait_until(tmp.path(), &mut child, "4 acks", || {
            acked(tmp.path()).len() >= 4
        });
        std::thread::sleep(Duration::from_millis(delay_ms));
        kill9(&mut child);
        let context = format!("flood round {round}");
        let n = verify_recovery(tmp.path(), StepOptions::default(), &context);
        assert!(n >= 4, "{context}: at least the acked prefix survives");
    }
}

// --- checklist 1.5: restart/persistence drills (TST-140/TST-141 scope) ----------

#[test]
fn restart_generations_preserve_and_extend_state() {
    let tmp = tempfile::tempdir().unwrap();

    // Generation 1: clean run, graceful shutdown (exit 0).
    let scenario = Scenario {
        mode: "graceful",
        target: 0,
        steps: 8,
        seg_bytes: None,
        heavy: false,
    };
    let mut child = spawn_child(tmp.path(), &scenario);
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "graceful generation failed; stderr:\n{}",
        fs::read_to_string(tmp.path().join("child.stderr")).unwrap_or_default()
    );
    assert_eq!(
        verify_recovery(tmp.path(), StepOptions::default(), "gen 1"),
        8
    );

    // Generation 2: restart on the same preserved data dir, then kill -9 at
    // an acked boundary.
    let scenario = Scenario::boundary("after-ack", 13);
    let mut child = spawn_child(tmp.path(), &scenario);
    wait_until(tmp.path(), &mut child, "boundary", || {
        acked(tmp.path()).contains(&13)
    });
    kill9(&mut child);
    assert_eq!(
        verify_recovery(tmp.path(), StepOptions::default(), "gen 2"),
        13
    );

    // Generation 3: restart again, run to completion, shut down cleanly.
    let scenario = Scenario {
        mode: "graceful",
        target: 0,
        steps: 4,
        seg_bytes: None,
        heavy: false,
    };
    let mut child = spawn_child(tmp.path(), &scenario);
    let status = child.wait().unwrap();
    assert!(status.success());

    // Final state: the exact oracle over all three generations' commits —
    // state written before each kill/restart is present and correct after
    // (TST-141).
    let n = verify_recovery(tmp.path(), StepOptions::default(), "gen 3");
    assert_eq!(n, 17, "8 + 5 (kill at 13) + 4 more steps");
    let oracle = oracle_store(17, StepOptions::default());
    let (store, _) = recover_fresh(&tmp.path().join("log"), &tmp.path().join("snapshots"));
    assert_eq!(fingerprint(&store), fingerprint(&oracle));
}
