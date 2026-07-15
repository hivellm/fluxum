//! T2.7 checkpoint durability boundaries (SPEC-015 acceptance 4; SPEC-002
//! STG-021; checklist 1.7) — a crash at **every** boundary of the two-phase
//! checkpoint loses zero acknowledged transactions.
//!
//! A kill -9 at boundary X is equivalent to the on-disk state where every
//! durable step ≤ X exists and every step > X does not (the checkpoint write
//! is a sequence of fsynced, atomically renamed files). Each test constructs
//! that exact state deterministically from a completed checkpoint's files
//! and drives the real recovery through it. The randomized-timing kill of a
//! live checkpoint writer is in `crash_kill9::kill_during_snapshot_write`.
//!
//! Boundary mapping onto the current STG-021 implementation (the TIER-060
//! paged checkpoint refines the same sequence when the pager's directory
//! persistence lands; its "mid page write" leg — a torn page extent — is
//! drilled in `crash_corruption`):
//!
//! | SPEC-015 acceptance 4 boundary       | on-disk state constructed        |
//! |--------------------------------------|----------------------------------|
//! | mid page/object write                | partial `objects/<hash>.tmp`, no manifest |
//! | after data, before manifest          | objects durable, no manifest     |
//! | after manifest, before `CURRENT` swap| fsynced `<manifest>.tmp`, not renamed |
//! | after swap, before log truncation    | manifest live, log uncompacted   |

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use fluxum_core::checkpoint::{CheckpointRepo, compact_covered, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};

use crash_support::{
    EPOCH, SHARD, StepOptions, apply_step, assert_equals_oracle, commit_step, fingerprint,
    mem_store, recover_fresh, segment_files,
};

const WL: StepOptions = StepOptions {
    heavy: false,
    with_event: true,
};

/// The base world: txs 1..=12 (small segments → rotation), checkpoint A at
/// tx 6 and checkpoint B at tx 12.
struct Base {
    root: tempfile::TempDir,
    manifest_a: PathBuf,
    manifest_b: PathBuf,
    /// Objects referenced only by checkpoint B.
    objects_only_b: Vec<String>,
}

impl Base {
    fn log_dir(&self) -> PathBuf {
        self.root.path().join("log")
    }
    fn snap_dir(&self) -> PathBuf {
        self.root.path().join("snapshots")
    }
}

fn object_names(snap_dir: &Path) -> BTreeSet<String> {
    fs::read_dir(snap_dir.join("objects"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

async fn build_base() -> Base {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join("log");
    let snap_dir = root.path().join("snapshots");
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 256,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();

    for i in 1..=6u64 {
        commit_step(&store, &log, i, WL).await;
    }
    log.wait_durable(6).await.unwrap();
    let stats_a = repo.write(&store.snapshot(), SHARD, 6, EPOCH).unwrap();
    let objects_a = object_names(&snap_dir);

    for i in 7..=12u64 {
        commit_step(&store, &log, i, WL).await;
    }
    log.wait_durable(12).await.unwrap();
    let stats_b = repo.write(&store.snapshot(), SHARD, 12, EPOCH).unwrap();
    log.close().unwrap();

    let objects_only_b: Vec<String> = object_names(&snap_dir)
        .difference(&objects_a)
        .cloned()
        .collect();
    assert!(!objects_only_b.is_empty(), "checkpoint B must add objects");
    Base {
        root,
        manifest_a: stats_a.manifest,
        manifest_b: stats_b.manifest,
        objects_only_b,
    }
}

/// Recover and assert the exact oracle state at prefix 12, returning the
/// adopted checkpoint tx.
fn recover_and_check(base: &Base, context: &str) -> Option<u64> {
    let (store, outcome) = recover_fresh(&base.log_dir(), &base.snap_dir());
    assert_eq!(outcome.last_tx_id, Some(12), "{context}");
    assert_eq!(outcome.next_tx_id, 13, "{context}");
    assert_equals_oracle(&store, 12, WL, context);
    outcome.checkpoint_tx_id
}

/// After surviving a boundary state, checkpointing must resume: one more
/// step, a fresh checkpoint, recovery through it stays exact.
fn checkpointing_resumes(base: &Base, context: &str) {
    let (store, _) = recover_fresh(&base.log_dir(), &base.snap_dir());
    apply_step(&store, 13, WL);
    let repo = CheckpointRepo::open(&base.snap_dir()).unwrap();
    repo.write(&store.snapshot(), SHARD, 13, EPOCH)
        .unwrap_or_else(|e| panic!("{context}: checkpointing must resume: {e}"));
    let restored = mem_store();
    let outcome = recover(&restored, &repo, &base.log_dir(), SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(13), "{context}");
    assert_eq!(fingerprint(&restored), fingerprint(&store), "{context}");
}

// --- boundary: crash mid object write -------------------------------------------

#[tokio::test]
async fn crash_mid_object_write_leaves_no_checkpoint_and_loses_nothing() {
    let base = build_base().await;
    // The kill landed while an object was streaming to its temp file: the
    // manifest never existed, and a partial `.tmp` litters the object store.
    fs::remove_file(&base.manifest_b).unwrap();
    let victim = base
        .snap_dir()
        .join("objects")
        .join(&base.objects_only_b[0]);
    let bytes = fs::read(&victim).unwrap();
    fs::remove_file(&victim).unwrap();
    fs::write(
        base.snap_dir()
            .join("objects")
            .join(format!("{}.tmp", base.objects_only_b[0])),
        &bytes[..bytes.len() / 2],
    )
    .unwrap();

    // A checkpoint whose manifest is absent does not exist (STG-021):
    // recovery adopts A and replays the log to the exact pre-crash state.
    assert_eq!(recover_and_check(&base, "mid-object-write"), Some(6));
    checkpointing_resumes(&base, "mid-object-write");
}

// --- boundary: crash after data objects, before the manifest ---------------------

#[tokio::test]
async fn crash_after_objects_before_manifest_falls_back_to_the_previous_checkpoint() {
    let base = build_base().await;
    // Every object of B is durable; the manifest (the commit record) is not.
    fs::remove_file(&base.manifest_b).unwrap();

    assert_eq!(recover_and_check(&base, "before-manifest"), Some(6));
    // The orphaned objects are inert: they break nothing and the next
    // checkpoint may share them by content hash.
    checkpointing_resumes(&base, "before-manifest");
}

// --- boundary: crash after the manifest, before the atomic swap ------------------

#[tokio::test]
async fn crash_before_the_swap_ignores_the_staged_manifest() {
    let base = build_base().await;
    // The manifest bytes are fsynced under the staging name but the atomic
    // rename (the `CURRENT` swap of TIER-060) never happened.
    let staged = PathBuf::from(format!("{}.tmp", base.manifest_b.display()));
    fs::rename(&base.manifest_b, &staged).unwrap();

    // The staged file is invisible to recovery: B does not exist yet.
    let repo = CheckpointRepo::open(&base.snap_dir()).unwrap();
    assert_eq!(
        repo.list(SHARD).unwrap().last().map(|r| r.last_tx_id),
        Some(6),
        "a staged manifest must not be listed as a checkpoint"
    );
    assert_eq!(recover_and_check(&base, "before-swap"), Some(6));

    // Completing the swap (the rename is the atomic commit point) makes B
    // real, and recovery adopts it with the identical resulting state.
    fs::rename(&staged, &base.manifest_b).unwrap();
    assert_eq!(recover_and_check(&base, "after-swap-replayed"), Some(12));
}

// --- boundary: crash after the swap, before log truncation -----------------------

#[tokio::test]
async fn crash_after_the_swap_before_truncation_recovers_and_truncation_resumes() {
    let base = build_base().await;

    // The checkpoint is live and the log was never truncated: recovery
    // adopts B; replayed entries ≤ 12 are convergent no-ops.
    assert_eq!(recover_and_check(&base, "before-truncation"), Some(12));

    // Truncation resumes after restart, honoring the worker's rule (compact
    // only up to the OLDEST retained checkpoint, so the STG-021 fallback
    // keeps the log suffix it needs).
    let removed = compact_covered(&base.log_dir(), SHARD, 6, None, None).unwrap();
    assert!(!removed.is_empty(), "covered segments must be truncatable");
    assert_eq!(
        recover_and_check(&base, "after-oldest-compaction"),
        Some(12)
    );

    // The fallback still works post-truncation: with B's manifest corrupted,
    // A + the retained log suffix reconstruct the same state.
    let mut bytes = fs::read(&base.manifest_b).unwrap();
    let len = bytes.len();
    bytes[len - 10] ^= 0xFF;
    fs::write(&base.manifest_b, &bytes).unwrap();
    let (store, outcome) = recover_fresh(&base.log_dir(), &base.snap_dir());
    assert_eq!(
        outcome.checkpoint_tx_id,
        Some(6),
        "fallback after truncation"
    );
    assert_eq!(outcome.rejected.len(), 1);
    assert_eq!(outcome.rejected[0].path, base.manifest_b);
    assert_equals_oracle(&store, 12, WL, "fallback after truncation");

    // Full-coverage truncation keeps only the active tail, and recovery
    // still lands exactly (restore B first so the newest checkpoint is
    // valid again). The older retained checkpoint A survived throughout.
    assert!(
        base.manifest_a.exists(),
        "retained checkpoint A must survive"
    );
    fs::write(&base.manifest_b, {
        let mut restored = bytes;
        restored[len - 10] ^= 0xFF;
        restored
    })
    .unwrap();
    let before = segment_files(&base.log_dir()).len();
    let removed = compact_covered(&base.log_dir(), SHARD, 12, None, None).unwrap();
    assert_eq!(removed.len(), before - 1, "everything but the tail goes");
    assert_eq!(recover_and_check(&base, "after-full-compaction"), Some(12));
}
