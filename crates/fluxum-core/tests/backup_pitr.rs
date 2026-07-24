//! T7.3 backup + PITR (SPEC-014 §8/§9, REP-060..REP-072; FR-103/FR-104):
//! hot backup round-trips to the exact head state, verification catches a
//! single bit-flip with a precise per-file report, PITR reproduces the
//! inclusive prefix at a tx-id or timestamp target and reports the boundary,
//! a chain gap fails loudly with the covered range, and the lineage marker
//! records the forked-history epoch.
//!
//! The oracle workload is the crash suite's: step `i` commits as tx `i`
//! with `timestamp = i µs`, so both target flavors are deterministic.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::fs;
use std::path::{Path, PathBuf};

use fluxum_core::backup::{self, BackupSource, PitrTarget, RestoreDirs, pitr_lineage_min_epoch};
use fluxum_core::checkpoint::{CheckpointRepo, DirectoryArchive, compact_covered};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};

use crash_support::{EPOCH, SHARD, StepOptions, commit_step, mem_store, recover_fresh};

const WL: StepOptions = StepOptions {
    heavy: false,
    with_event: true,
};

struct World {
    root: tempfile::TempDir,
}

impl World {
    fn log_dir(&self) -> PathBuf {
        self.root.path().join("log")
    }
    fn snap_dir(&self) -> PathBuf {
        self.root.path().join("snapshots")
    }
    fn archive_dir(&self) -> PathBuf {
        self.root.path().join("archive")
    }
    fn source(&self) -> BackupSource {
        BackupSource {
            checkpoint_dir: self.snap_dir(),
            log_dir: self.log_dir(),
        }
    }
    fn restore_dirs(&self, name: &str) -> RestoreDirs {
        RestoreDirs {
            checkpoint_dir: self.root.path().join(name).join("snapshots"),
            log_dir: self.root.path().join(name).join("log"),
        }
    }
}

/// Txs `1..=head` with a checkpoint at `ckpt` (0 = none), small segments so
/// the log rotates.
async fn build(head: u64, ckpt: u64) -> World {
    let world = World {
        root: tempfile::tempdir().unwrap(),
    };
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 256,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&world.log_dir(), SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&world.snap_dir()).unwrap();
    for i in 1..=head {
        commit_step(&store, &log, i, WL).await;
        if i == ckpt {
            log.wait_durable(i).await.unwrap();
            repo.write(&store.snapshot(), SHARD, i, EPOCH).unwrap();
        }
    }
    log.wait_durable(head).await.unwrap();
    log.close().unwrap();
    world
}

/// Recover a restored layout and assert it equals the oracle at prefix `n`.
fn assert_restored_state(dirs: &RestoreDirs, n: u64, context: &str) {
    let (store, outcome) = recover_fresh(&dirs.log_dir, &dirs.checkpoint_dir);
    assert_eq!(outcome.last_tx_id, Some(n), "{context}");
    crash_support::assert_equals_oracle(&store, n, WL, context);
}

// --- REP-060/061/063: hot backup round-trips to the exact head state -------------

#[tokio::test]
async fn backup_round_trips_to_the_exact_head_state() {
    let world = build(12, 6).await;
    let out = world.root.path().join("backup");
    let report = backup::create(&world.source(), &out).unwrap();
    assert_eq!(report.head_tx_id, 12);
    assert_eq!(report.shards, 1);
    assert!(report.segments >= 1, "{report:?}");

    // REP-064: a clean backup verifies.
    let verify = backup::verify(&out).unwrap();
    assert!(verify.ok(), "{:?}", verify.errors().collect::<Vec<_>>());

    // REP-063: restore + normal recovery reproduce the head exactly.
    let dirs = world.restore_dirs("restored");
    backup::restore(&out, &dirs, false).unwrap();
    assert_restored_state(&dirs, 12, "full restore");
}

#[tokio::test]
async fn a_backup_without_any_checkpoint_still_round_trips() {
    // A young deployment: log only. The manifest records no checkpoint and
    // the segment chain starts at tx 1.
    let world = build(5, 0).await;
    let out = world.root.path().join("backup");
    let report = backup::create(&world.source(), &out).unwrap();
    assert_eq!(report.head_tx_id, 5);
    assert!(backup::verify(&out).unwrap().ok());
    let dirs = world.restore_dirs("restored");
    backup::restore(&out, &dirs, false).unwrap();
    assert_restored_state(&dirs, 5, "log-only restore");
}

// --- REP-064: a single injected bit-flip fails verify with a precise report ------

#[tokio::test]
async fn verify_names_the_file_a_single_bit_flip_corrupted() {
    let world = build(12, 6).await;
    let out = world.root.path().join("backup");
    backup::create(&world.source(), &out).unwrap();

    // Read the manifest to find a segment artifact, then flip ONE bit.
    let manifest_bytes = fs::read(out.join("manifest.mpack")).unwrap();
    let manifest: fluxum_core::backup::BackupManifest =
        rmp_serde::from_slice(&manifest_bytes).unwrap();
    let victim_rel = manifest.shards[0].segments[0].file.clone();
    let victim = out.join(&victim_rel);
    let mut bytes = fs::read(&victim).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01;
    fs::write(&victim, &bytes).unwrap();

    let report = backup::verify(&out).unwrap();
    assert!(!report.ok());
    let errors: Vec<_> = report.errors().collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].file, victim_rel,
        "the report names the exact file"
    );
    assert!(
        errors[0].error.as_deref().unwrap_or("").contains("CRC32C"),
        "{errors:?}"
    );

    // REP-063: restore refuses a backup that fails verification.
    let err = backup::restore(&out, &world.restore_dirs("refused"), false).unwrap_err();
    assert!(err.to_string().contains(&victim_rel), "{err}");
}

#[tokio::test]
async fn restore_refuses_a_non_empty_target_without_force() {
    let world = build(8, 6).await;
    let out = world.root.path().join("backup");
    backup::create(&world.source(), &out).unwrap();

    let dirs = world.restore_dirs("occupied");
    fs::create_dir_all(&dirs.log_dir).unwrap();
    fs::write(dirs.log_dir.join("keep.txt"), b"precious").unwrap();
    let err = backup::restore(&out, &dirs, false).unwrap_err();
    assert!(err.to_string().contains("--force"), "{err}");
    // With force it proceeds.
    backup::restore(&out, &dirs, true).unwrap();
    assert_restored_state(&dirs, 8, "forced restore");
}

// --- REP-070/071/072: PITR to a tx id and a timestamp ----------------------------

#[tokio::test]
async fn pitr_reproduces_the_inclusive_prefix_and_reports_the_boundary() {
    let world = build(12, 6).await;
    let out = world.root.path().join("backup");
    backup::create(&world.source(), &out).unwrap();

    // Target by tx id: everything <= 9, nothing after (REP-070 inclusive).
    let dirs = world.restore_dirs("pitr-tx");
    let report = backup::pitr(&out, &dirs, None, PitrTarget::TxId(9), false).unwrap();
    assert_eq!(report.last_tx_id, 9, "boundary reported (REP-071)");
    assert_eq!(
        report.last_timestamp, 9,
        "workload stamps timestamp = tx id"
    );
    assert_restored_state(&dirs, 9, "pitr to tx 9");

    // REP-072: the lineage marker names an epoch strictly above the log's.
    assert_eq!(report.fork_min_epoch, EPOCH + 1);
    assert_eq!(
        pitr_lineage_min_epoch(&dirs.log_dir).unwrap(),
        Some(EPOCH + 1)
    );

    // Target by timestamp (µs): the workload stamps timestamp = tx id, so
    // the same boundary lands (the operator-error scenario of REP-071).
    let dirs = world.restore_dirs("pitr-ts");
    let report = backup::pitr(&out, &dirs, None, PitrTarget::TimestampMicros(9), false).unwrap();
    assert_eq!((report.last_tx_id, report.last_timestamp), (9, 9));
    assert_restored_state(&dirs, 9, "pitr to timestamp 9µs");

    // A full restore of the same backup is untouched by the PITR copies.
    let dirs = world.restore_dirs("full-after-pitr");
    backup::restore(&out, &dirs, false).unwrap();
    assert_restored_state(&dirs, 12, "full restore after pitr");
    assert_eq!(pitr_lineage_min_epoch(&dirs.log_dir).unwrap(), None);
}

/// One continuous history 1..=16 (single process — auto-inc allocation
/// stays oracle-exact), with the backup taken HOT at head 8 while the log
/// is live and appends continue after it, then archival compacting the
/// covered segments out of the live log (REP-062).
async fn build_archived_world() -> (World, PathBuf) {
    let world = World {
        root: tempfile::tempdir().unwrap(),
    };
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 256,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&world.log_dir(), SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&world.snap_dir()).unwrap();
    for i in 1..=8u64 {
        commit_step(&store, &log, i, WL).await;
        if i == 6 {
            log.wait_durable(6).await.unwrap();
            repo.write(&store.snapshot(), SHARD, 6, EPOCH).unwrap();
        }
    }
    log.wait_durable(8).await.unwrap();
    // REP-060: the backup is taken against the LIVE log — no lock, no
    // writer stall; appends continue immediately after.
    let out = world.root.path().join("backup");
    let report = backup::create(&world.source(), &out).unwrap();
    assert_eq!(report.head_tx_id, 8);

    for i in 9..=16u64 {
        commit_step(&store, &log, i, WL).await;
    }
    log.wait_durable(16).await.unwrap();
    repo.write(&store.snapshot(), SHARD, 16, EPOCH).unwrap();
    log.close().unwrap();

    let archive = DirectoryArchive::open(&world.archive_dir()).unwrap();
    let removed = compact_covered(&world.log_dir(), SHARD, 16, None, Some(&archive)).unwrap();
    assert!(!removed.is_empty(), "compaction must archive something");
    (world, out)
}

/// The REP-070 normal case: the target lies BEYOND the backup's own
/// segments, and archived segments (REP-062) continue the chain.
#[tokio::test]
async fn pitr_extends_past_the_backup_through_archived_segments() {
    let (world, out) = build_archived_world().await;

    // PITR to tx 14: the base backup covers 1..=8; the archive supplies the
    // continuation. The restored state is the exact oracle prefix at 14.
    let dirs = world.restore_dirs("pitr-archive");
    let report = backup::pitr(
        &out,
        &dirs,
        Some(&world.archive_dir()),
        PitrTarget::TxId(14),
        false,
    )
    .unwrap();
    assert_eq!(report.last_tx_id, 14);
    assert_restored_state(&dirs, 14, "pitr through the archive");
}

/// REP-071: a missing archived segment before the target is a hard failure
/// naming the covered range — never a silent early stop.
#[tokio::test]
async fn pitr_fails_loudly_on_an_archive_chain_gap() {
    let (world, out) = build_archived_world().await;

    // Remove an archived segment PAST the backup's head (first_tx_id > 8)
    // that is not the last of the chain — a hole the target lies beyond.
    let mut past_backup: Vec<PathBuf> = fs::read_dir(world.archive_dir())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("shard-3-"))
                .and_then(|n| n.strip_suffix(".log"))
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|first| first > 8)
        })
        .collect();
    past_backup.sort();
    assert!(
        past_backup.len() >= 2,
        "need a non-final post-backup segment to remove, got {past_backup:?}"
    );
    fs::remove_file(&past_backup[0]).unwrap();

    let err = backup::pitr(
        &out,
        &world.restore_dirs("pitr-gap"),
        Some(&world.archive_dir()),
        PitrTarget::TxId(16),
        false,
    )
    .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("gap"), "{message}");
    assert!(
        message.contains("covered") && message.contains("REP-071"),
        "the covered range is reported: {message}"
    );
}

/// REP-070 roll-forward guard: a target before the backup's checkpoint
/// cannot be reproduced from that backup (the checkpoint state already
/// exceeds it) — refused explicitly in both target flavors, never silently
/// wrong.
#[tokio::test]
async fn pitr_refuses_a_target_before_the_backup_checkpoint() {
    let world = build(12, 6).await;
    let out = world.root.path().join("backup");
    backup::create(&world.source(), &out).unwrap();

    let err = backup::pitr(
        &out,
        &world.restore_dirs("early-tx"),
        None,
        PitrTarget::TxId(4),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("precedes"), "{err}");

    let err = backup::pitr(
        &out,
        &world.restore_dirs("early-ts"),
        None,
        PitrTarget::TimestampMicros(4),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("precedes"), "{err}");
}

/// A PITR target before every transaction is an explicit error, not an
/// empty database.
#[tokio::test]
async fn pitr_before_history_is_an_explicit_error() {
    let world = build(8, 0).await;
    let out = world.root.path().join("backup");
    backup::create(&world.source(), &out).unwrap();
    let err = backup::pitr(
        &out,
        &world.restore_dirs("pitr-zero"),
        None,
        PitrTarget::TimestampMicros(0),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("precedes"), "{err}");
}

fn touch_old(path: &Path) {
    fs::write(path, b"x").unwrap();
}

/// REP-062 retention: the worker's sweep removes archived copies older than
/// the window, and only segment copies — never other files.
#[tokio::test]
async fn archive_retention_sweeps_only_old_segment_copies() {
    use fluxum_core::checkpoint::{LogCompaction, SnapshotWorker, WorkerOptions};
    use std::sync::Arc;

    let world = build(12, 0).await;
    fs::create_dir_all(world.archive_dir()).unwrap();
    // An "old" archived copy and an unrelated file parked by the operator.
    let stale = world.archive_dir().join("shard-3-00000000000000000099.log");
    touch_old(&stale);
    let unrelated = world.archive_dir().join("notes.txt");
    touch_old(&unrelated);
    std::thread::sleep(std::time::Duration::from_millis(60));

    let store = Arc::new(mem_store());
    let repo = Arc::new(CheckpointRepo::open(&world.snap_dir()).unwrap());
    fluxum_core::checkpoint::recover(&store, &repo, &world.log_dir(), SHARD).unwrap();
    let worker = SnapshotWorker::spawn(
        Arc::clone(&store),
        repo,
        SHARD,
        WorkerOptions {
            interval_tx: 1_000_000,
            epoch: EPOCH,
            compaction: Some(LogCompaction {
                log_dir: world.log_dir(),
                archive_dir: Some(world.archive_dir()),
                archive_retention: Some(std::time::Duration::from_millis(50)),
            }),
            ..WorkerOptions::default()
        },
    )
    .unwrap();
    worker.observe_commit(12);
    worker.checkpoint_now().unwrap();
    worker.close().unwrap();

    assert!(
        !stale.exists(),
        "the stale archived copy must be swept (REP-062 retention)"
    );
    assert!(
        unrelated.exists(),
        "non-segment files are never the sweeper's to delete"
    );
}
