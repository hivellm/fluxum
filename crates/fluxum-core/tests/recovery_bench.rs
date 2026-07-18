//! T2.7 recovery-time benchmark (SPEC-013 TST-024; SPEC-002 STG-032;
//! NFR-06; checklist 1.3) — timed cold restart (latest checkpoint + log
//! replay) of a large commit log.
//!
//! Per TST-024 the full 10 GB run executes **nightly and at G2/G6**, not per
//! PR: this test defaults to a scaled-down smoke volume that validates the
//! measurement end-to-end and reports extrapolated throughput. The full
//! gate run is one documented command (TST-007):
//!
//! ```text
//! FLUXUM_RECOVERY_BENCH_BYTES=10737418240 \
//!   cargo test --release -p fluxum-core --test recovery_bench -- --nocapture
//! ```
//!
//! The `< 30 s` assertion is enforced whenever the requested volume is
//! ≥ 10 GiB (release build on the reference runner). The report records the
//! probed hardware (cores, RAM, cgroup limits) and OS/arch per TST-024; the
//! runner's CPU model and disk class are recorded by the CI job that
//! archives the output.
//!
//! Workload shape: update-heavy small writes (the SPEC-002 target profile) —
//! transactions of 16 × 2 KiB row upserts cycling over a bounded key space,
//! so the log is large while the live state stays bounded; the checkpoint is
//! recent (covers the whole log), making replay scan-dominated exactly like
//! a production restart after a healthy checkpoint cadence.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::time::Instant;

use serde_bytes::ByteBuf;

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, LogValue, TableMutation, TxRecord};
use fluxum_core::hw::HardwareProfile;
use fluxum_core::store::TableId;

use crash_support::{EPOCH, SHARD, fingerprint, mem_store, segment_files};

const ROWS_PER_TX: u64 = 16;
const PAYLOAD_BYTES: usize = 2048;
const KEY_SPACE: u64 = 20_000;
const TEN_GIB: u64 = 10 * 1024 * 1024 * 1024;

fn target_bytes() -> u64 {
    std::env::var("FLUXUM_RECOVERY_BENCH_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024)
}

/// One update-heavy transaction: 16 upserts (insert replaces by PK on
/// replay) plus one delete, cycling the bounded key space.
fn bench_record(tx_id: u64, payload: &str) -> TxRecord {
    let base = tx_id * ROWS_PER_TX;
    let inserts = (0..ROWS_PER_TX)
        .map(|k| {
            let id = (base + k) % KEY_SPACE;
            vec![LogValue::U64(id), LogValue::Str(format!("{payload}-{id}"))]
        })
        .collect();
    let delete_id = (base + KEY_SPACE / 2) % KEY_SPACE;
    TxRecord {
        tx_id,
        timestamp: i64::try_from(tx_id).unwrap(),
        shard_id: SHARD,
        mutations: vec![TableMutation {
            table_id: TableId::of("User").as_u32(),
            inserts,
            deletes: vec![ByteBuf::from(delete_id.to_le_bytes().to_vec())],
        }],
        auto_inc: vec![],
        caller: Vec::new(),
        reducer_name: String::new(),
    }
}

#[tokio::test]
async fn recovery_meets_the_throughput_target() {
    let target = target_bytes();
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let snap_dir = dir.path().join("snapshots");

    // --- build the log ---------------------------------------------------
    let payload = "x".repeat(PAYLOAD_BYTES);
    let bytes_per_tx = (ROWS_PER_TX as usize * (PAYLOAD_BYTES + 24) + 64) as u64;
    let txs = (target / bytes_per_tx).max(64);
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
    for tx_id in 1..=txs {
        log.append(bench_record(tx_id, &payload)).await.unwrap();
    }
    log.wait_durable(txs).await.unwrap();
    log.close().unwrap();
    let on_disk: u64 = segment_files(&log_dir)
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum();

    // --- recent checkpoint (untimed reference recovery feeds it) ----------
    let reference = mem_store();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    let outcome = recover(&reference, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.last_tx_id, Some(txs));
    repo.write(&reference.snapshot(), SHARD, txs, EPOCH)
        .unwrap();

    // --- the timed cold restart: latest checkpoint + full log replay ------
    let started = Instant::now();
    let restored = mem_store();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    let outcome = recover(&restored, &repo, &log_dir, SHARD).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(outcome.checkpoint_tx_id, Some(txs));
    assert_eq!(outcome.last_tx_id, Some(txs));
    assert!(outcome.replay.corruption.is_none());
    assert_eq!(
        fingerprint(&restored),
        fingerprint(&reference),
        "timed recovery must land on the exact reference state"
    );

    // --- report (TST-024 measurement record) ------------------------------
    let secs = elapsed.as_secs_f64().max(1e-9);
    let mib = on_disk as f64 / (1024.0 * 1024.0);
    let extrapolated_10g = secs * (TEN_GIB as f64 / on_disk as f64);
    let hw = HardwareProfile::probe();
    eprintln!("--- recovery benchmark (TST-024 / NFR-06 / STG-032) ---");
    eprintln!(
        "log volume      : {mib:.1} MiB across {} segments",
        segment_files(&log_dir).len()
    );
    eprintln!("transactions    : {txs} ({ROWS_PER_TX} upserts + 1 delete each)");
    eprintln!("recovery time   : {elapsed:?} ({:.1} MiB/s)", mib / secs);
    eprintln!("10 GiB estimate : {extrapolated_10g:.1} s (budget: 30 s)");
    eprintln!(
        "hardware        : {} logical / {} physical cores, {:.1} GiB RAM \
         (cgroup limit: {:?}), os: {} {}",
        hw.logical_cores,
        hw.physical_cores,
        hw.total_ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        hw.cgroup_memory_limit_bytes,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );

    // The hard NFR-06 gate applies to the full-size nightly/G2 run on the
    // reference runner; the per-PR smoke only proves the harness.
    if target >= TEN_GIB {
        assert!(
            elapsed.as_secs_f64() < 30.0,
            "10 GiB recovery took {elapsed:?} — NFR-06 budget is 30 s"
        );
    }
}
