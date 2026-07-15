//! T2.7 corruption drills (SPEC-013 TST-022/TST-023; SPEC-002 acceptance 3;
//! SPEC-015 TIER-062; checklist 1.2, 1.6) — CRC bit-flip and truncation
//! sweeps over the commit log **and** the cold-tier page files, verified
//! through the full recovery orchestration (`CommitLog::open` quarantine +
//! checkpoint/replay `recover`), never through parsing shortcuts.
//!
//! The oracle is `crash_support`'s deterministic workload: after every
//! injected fault, the recovered store must be row-set-equal to the oracle at
//! the exact whole-transaction prefix that survives — proving replay stopped
//! at the first corrupt entry, kept everything before it, and replayed
//! nothing at or beyond it (STG-031), without ever panicking.
//!
//! Torn-page/mid-page-write faults on the pager (SPEC-015 acceptance 4 "mid
//! page write" leg) are covered here too: a partially written or bit-flipped
//! extent fails its mandatory fault-in CRC (`PageCorrupt`, TIER-032/062) and
//! the data is recovered from the retained checkpoint root + log replay
//! (TIER-061), because spilled pages are availability, not durability.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluxum_core::FluxumError;
use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::store::RowValue;
use fluxum_core::store::pager::{ColdTable, Pager, PagerOptions};

use crash_support::{
    EPOCH, SEGMENT_HEADER_LEN, SHARD, StepOptions, assert_equals_oracle, commit_step, copy_log_dir,
    entry_boundaries, event, fingerprint, flip_byte_at, mem_store, oracle_store, recover_fresh,
    segment_files, truncate_to, user,
};

const WL: StepOptions = StepOptions {
    heavy: false,
    with_event: true,
};

/// Build the base history: txs 1..=15 in the log, a checkpoint covering 8.
/// Returns (log_dir, snap_dir) under `root`.
async fn build_base(root: &Path, segment_max_bytes: u64) -> (PathBuf, PathBuf) {
    let log_dir = root.join("log");
    let snap_dir = root.join("snapshots");
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    for i in 1..=15u64 {
        commit_step(&store, &log, i, WL).await;
        if i == 8 {
            log.wait_durable(8).await.unwrap();
            repo.write(&store.snapshot(), SHARD, 8, EPOCH).unwrap();
        }
    }
    log.wait_durable(15).await.unwrap();
    log.close().unwrap();
    (log_dir, snap_dir)
}

/// The tail segment and the byte span `(start, end)` of its final entry.
fn final_entry_span(log_dir: &Path) -> (PathBuf, usize, usize) {
    let segments = segment_files(log_dir);
    let tail = segments.last().unwrap().clone();
    let bytes = fs::read(&tail).unwrap();
    let boundaries = entry_boundaries(&bytes);
    assert!(boundaries.len() >= 2, "tail segment must hold entries");
    let start = boundaries[boundaries.len() - 2];
    let end = *boundaries.last().unwrap();
    assert_eq!(end, bytes.len(), "pristine tail must end on a boundary");
    (tail, start, end)
}

// --- checklist 1.2: bit-flip sweep over the final log entry (TST-022) ----------

#[tokio::test]
async fn bitflip_at_every_byte_of_the_final_entry_recovers_the_prefix() {
    let base = tempfile::tempdir().unwrap();
    let (log_dir, snap_dir) = build_base(base.path(), u64::MAX).await;
    let (tail, start, end) = final_entry_span(&log_dir);
    let tail_name = tail.file_name().unwrap().to_owned();

    for pos in start..end {
        let run = tempfile::tempdir().unwrap();
        copy_log_dir(&log_dir, run.path());
        flip_byte_at(&run.path().join(&tail_name), pos);

        // Full recovery: quarantine on open, then checkpoint(8) + replay.
        let (store, outcome) = recover_fresh(run.path(), &snap_dir);
        assert_eq!(
            outcome.last_tx_id,
            Some(14),
            "flip at byte {pos}: replay must stop exactly at the corrupt tx 15"
        );
        assert_eq!(outcome.checkpoint_tx_id, Some(8), "flip at byte {pos}");
        assert_equals_oracle(&store, 14, WL, &format!("bitflip at byte {pos}"));

        // The quarantine preserved evidence: a sidecar exists next to the
        // segment (STG-031 non-destructive repair).
        let sidecar = fs::read_dir(run.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.to_string_lossy().contains(".torn"));
        assert!(
            sidecar.is_some(),
            "flip at byte {pos}: no quarantine sidecar"
        );

        // Appends resume at the repaired boundary and recover cleanly
        // (spot-checked across the sweep to bound runtime).
        if pos.is_multiple_of(5) {
            let hw_before = store.snapshot().auto_inc_high_water(event(&store)).unwrap();
            let log =
                CommitLog::open(run.path(), SHARD, EPOCH, CommitLogOptions::default()).unwrap();
            commit_step(&store, &log, 15, WL).await;
            log.wait_durable(15).await.unwrap();
            log.close().unwrap();
            let (resumed, outcome) = recover_fresh(run.path(), &snap_dir);
            assert_eq!(outcome.last_tx_id, Some(15));
            // The re-applied tx round-trips through the repaired log exactly
            // (a fresh oracle would differ only in the auto-inc id: after
            // recovery the Event counter resumes above the recovered
            // high-water — gaps are documented, reuse is forbidden, STG-040).
            assert_eq!(
                fingerprint(&resumed),
                fingerprint(&store),
                "resume after flip at {pos}"
            );
            let hw_after = resumed
                .snapshot()
                .auto_inc_high_water(event(&resumed))
                .unwrap();
            assert!(
                hw_after > hw_before,
                "flip at {pos}: the resumed auto-inc must advance past the \
                 recovered high-water {hw_before} (got {hw_after})"
            );
        }
    }
}

// --- checklist 1.2: truncation sweep over the final log entry (TST-023) --------

#[tokio::test]
async fn truncation_at_every_byte_of_the_final_entry_is_a_clean_end_of_log() {
    let base = tempfile::tempdir().unwrap();
    let (log_dir, snap_dir) = build_base(base.path(), u64::MAX).await;
    let (tail, start, end) = final_entry_span(&log_dir);
    let tail_name = tail.file_name().unwrap().to_owned();

    // Every cut inside the final entry — including inside the length prefix,
    // the epoch, the body, and the trailing CRC (TST-023).
    for cut in start..end {
        let run = tempfile::tempdir().unwrap();
        copy_log_dir(&log_dir, run.path());
        truncate_to(&run.path().join(&tail_name), cut);

        let (store, outcome) = recover_fresh(run.path(), &snap_dir);
        assert_eq!(
            outcome.last_tx_id,
            Some(14),
            "cut at byte {cut}: all prior entries must survive"
        );
        assert!(
            outcome.replay.corruption.is_none(),
            "cut at byte {cut}: the quarantined log must replay clean"
        );
        assert_equals_oracle(&store, 14, WL, &format!("truncation at byte {cut}"));
    }
}

// --- checklist 1.2: head / middle / tail positions (TST-022) --------------------

#[tokio::test]
async fn corruption_at_head_middle_and_tail_never_replays_past_the_fault() {
    let base = tempfile::tempdir().unwrap();
    // Small segments: 15 txs spread over several files.
    let (log_dir, snap_dir) = build_base(base.path(), 256).await;
    let segments = segment_files(&log_dir);
    assert!(
        segments.len() >= 3,
        "expected rotation, got {}",
        segments.len()
    );

    let first_tx_of = |seg: &Path| -> u64 {
        let name = seg.file_name().unwrap().to_string_lossy().into_owned();
        name.trim_start_matches(&format!("shard-{SHARD}-"))
            .trim_end_matches(".log")
            .parse()
            .unwrap()
    };

    // (segment index, description). Head, a middle segment, and the tail.
    let cases = [
        (0usize, "head"),
        (segments.len() / 2, "middle"),
        (segments.len() - 1, "tail"),
    ];
    for (seg_idx, position) in cases {
        let seg_name = segments[seg_idx].file_name().unwrap().to_owned();
        let is_tail = seg_idx == segments.len() - 1;
        // Flip inside the first entry of the chosen segment: everything from
        // that segment's first tx onward is unreplayable.
        let prefix = first_tx_of(&segments[seg_idx]) - 1;

        // (a) No checkpoint: recovery keeps exactly the prefix before the
        // corrupt entry.
        let run = tempfile::tempdir().unwrap();
        copy_log_dir(&log_dir, run.path());
        flip_byte_at(&run.path().join(&seg_name), SEGMENT_HEADER_LEN + 2);
        let empty_snaps = run.path().join("no-snapshots");
        if is_tail {
            let (store, outcome) = recover_fresh(run.path(), &empty_snaps);
            assert_eq!(
                outcome.last_tx_id,
                Some(prefix).filter(|&p| p > 0),
                "{position}"
            );
            assert_equals_oracle(&store, prefix, WL, position);
        } else {
            // Non-tail corruption refuses the append path — destructive
            // repair is reset_to territory (STG-031)…
            let err =
                CommitLog::open(run.path(), SHARD, EPOCH, CommitLogOptions::default()).unwrap_err();
            assert!(err.to_string().contains("non-tail"), "{position}: {err}");
            // …while read-only recovery still restores the valid prefix and
            // reports where and why it stopped.
            let repo = CheckpointRepo::open(&empty_snaps).unwrap();
            let store = mem_store();
            let outcome = recover(&store, &repo, run.path(), SHARD).unwrap();
            let corruption = outcome
                .replay
                .corruption
                .as_ref()
                .unwrap_or_else(|| panic!("{position}: corruption must be reported"));
            assert_eq!(corruption.offset, SEGMENT_HEADER_LEN as u64, "{position}");
            assert_eq!(
                outcome.last_tx_id,
                Some(prefix).filter(|&p| p > 0),
                "{position}"
            );
            assert_equals_oracle(&store, prefix, WL, position);
        }

        // (b) With the checkpoint at 8: recovery lands on max(prefix, 8) —
        // entries covered by the checkpoint don't need the log.
        let run = tempfile::tempdir().unwrap();
        copy_log_dir(&log_dir, run.path());
        flip_byte_at(&run.path().join(&seg_name), SEGMENT_HEADER_LEN + 2);
        let repo = CheckpointRepo::open(&snap_dir).unwrap();
        let store = mem_store();
        let outcome = recover(&store, &repo, run.path(), SHARD).unwrap();
        let expected = prefix.max(8);
        assert_eq!(
            outcome.last_tx_id,
            Some(expected),
            "{position} + checkpoint"
        );
        assert_equals_oracle(&store, expected, WL, &format!("{position} + checkpoint"));
    }
}

// --- checklist 1.2 / 1.7 (mid page write): cold-tier page-file drills -----------

/// A pager with the effective-config defaults (the drills need spill/fault
/// correctness, not eviction pressure — `pager_10x.rs` owns that axis).
fn test_pager(dir: &Path) -> Arc<Pager> {
    use fluxum_core::config::Config;
    use fluxum_core::hw::{HardwareProfile, derive};
    let hw = HardwareProfile {
        logical_cores: 2,
        physical_cores: 2,
        total_ram_bytes: 512 << 20,
        available_ram_bytes: 512 << 20,
        cgroup_cpu_quota: None,
        cgroup_memory_limit_bytes: None,
    };
    let lookup = |key: &str| (key == "FLUXUM_PROFILE").then(|| "development".to_owned());
    let config = Config::load_with(None, &lookup).unwrap();
    let effective = derive(&hw, &config).unwrap();
    Pager::open(
        dir,
        PagerOptions::from_effective(&config, &effective, SHARD),
    )
    .unwrap()
}

#[tokio::test]
async fn page_corruption_is_never_served_and_recovers_from_root_plus_replay() {
    let base = tempfile::tempdir().unwrap();
    let (log_dir, snap_dir) = build_base(base.path(), u64::MAX).await;

    // Materialize the committed state and spill it to the cold tier.
    let (store, _) = recover_fresh(&log_dir, &snap_dir);
    let table = user(&store);
    let pager_dir = base.path().join("pages");
    let pager = test_pager(&pager_dir);
    let snap = store.snapshot();
    let cold = ColdTable::spill_snapshot(&pager, &snap, table).unwrap();
    pager.flush().unwrap();

    let root = cold.primary_tree().root_page_id();
    let (offset, len) = pager.page_extent(table, root).unwrap().unwrap();
    let page_file = pager_dir
        .join(format!("shard-{SHARD}"))
        .join(format!("table-{}.pages", table.as_u32()));

    // Drill 1 — bit flip in a live extent (TST-022 on page files): the
    // mandatory fault-in CRC rejects the page, it is never served.
    pager.evict_all().unwrap();
    flip_byte_at(&page_file, usize::try_from(offset + len / 2).unwrap());
    match pager.fault(table, root) {
        Err(FluxumError::PageCorrupt {
            shard_id,
            table_id,
            page_id,
        }) => {
            assert_eq!((shard_id, table_id, page_id), (SHARD, table.as_u32(), root));
        }
        other => panic!("expected PageCorrupt, got {other:?}"),
    }
    assert!(matches!(
        cold.get(&[RowValue::U64(1)]),
        Err(FluxumError::PageCorrupt { .. })
    ));

    // Drill 2 — torn page write (kill -9 mid page write, SPEC-015
    // acceptance 4): only a prefix of the extent lands; the torn image fails
    // CRC exactly like the flip (CoW means no durable root referenced it).
    let torn_dir = base.path().join("pages-torn");
    let pager2 = test_pager(&torn_dir);
    let snap2 = store.snapshot();
    let cold2 = ColdTable::spill_snapshot(&pager2, &snap2, table).unwrap();
    pager2.flush().unwrap();
    let root2 = cold2.primary_tree().root_page_id();
    let (offset2, len2) = pager2.page_extent(table, root2).unwrap().unwrap();
    let page_file2 = torn_dir
        .join(format!("shard-{SHARD}"))
        .join(format!("table-{}.pages", table.as_u32()));
    pager2.evict_all().unwrap();
    {
        // Overwrite the second half of the extent with garbage — the state a
        // partial write leaves behind.
        use std::io::{Seek as _, SeekFrom, Write as _};
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&page_file2)
            .unwrap();
        file.seek(SeekFrom::Start(offset2 + len2 / 2)).unwrap();
        let garbage = vec![0xAA_u8; usize::try_from(len2 - len2 / 2).unwrap()];
        file.write_all(&garbage).unwrap();
        file.sync_data().unwrap();
    }
    assert!(matches!(
        pager2.fault(table, root2),
        Err(FluxumError::PageCorrupt { .. })
    ));

    // TIER-062 recovery: retained checkpoint root + log replay reconstruct
    // the data with zero loss; a fresh spill serves every row again.
    let (recovered, outcome) = recover_fresh(&log_dir, &snap_dir);
    assert_eq!(outcome.last_tx_id, Some(15));
    assert_equals_oracle(&recovered, 15, WL, "page-corruption recovery");
    let fresh_dir = base.path().join("pages-fresh");
    let pager3 = test_pager(&fresh_dir);
    let snap3 = recovered.snapshot();
    let cold3 = ColdTable::spill_snapshot(&pager3, &snap3, table).unwrap();
    pager3.flush().unwrap();
    pager3.evict_all().unwrap();
    for row in snap3.scan(table).unwrap() {
        let pk = row.value(0).unwrap().clone();
        let got = cold3.get(std::slice::from_ref(&pk)).unwrap().unwrap();
        assert_eq!(&got, row, "row {pk:?} diverged after re-spill");
    }
}

// --- checklist 1.6: tiered recovery equivalence (STG-030, SPEC-002 acc. 6) ------

#[tokio::test]
async fn recovery_is_identical_regardless_of_pre_crash_residency() {
    let base = tempfile::tempdir().unwrap();
    let log_dir = base.path().join("log");
    let snap_dir = base.path().join("snapshots");
    let store = mem_store();
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    for i in 1..=20u64 {
        commit_step(&store, &log, i, WL).await;
        if i == 12 {
            log.wait_durable(12).await.unwrap();
            repo.write(&store.snapshot(), SHARD, 12, EPOCH).unwrap();
        }
    }
    log.wait_durable(20).await.unwrap();
    log.close().unwrap();
    let table = user(&store);

    // Vary the pre-crash physical residency:
    // (a) all hot — the cold tier never existed;
    // (b) fully spilled to the cold tier, cleanly flushed;
    // (c) crash mid-eviction — the spill was torn (partial page write) and
    //     the crash strikes before the next flush.
    let spill_b = base.path().join("pages-b");
    {
        let pager = test_pager(&spill_b);
        let snap = store.snapshot();
        let _cold = ColdTable::spill_snapshot(&pager, &snap, table).unwrap();
        pager.flush().unwrap();
    }
    let spill_c = base.path().join("pages-c");
    {
        let pager = test_pager(&spill_c);
        let snap = store.snapshot();
        let cold = ColdTable::spill_snapshot(&pager, &snap, table).unwrap();
        pager.flush().unwrap();
        // Tear the primary root mid-write.
        let root = cold.primary_tree().root_page_id();
        let (offset, len) = pager.page_extent(table, root).unwrap().unwrap();
        let page_file = spill_c
            .join(format!("shard-{SHARD}"))
            .join(format!("table-{}.pages", table.as_u32()));
        drop(cold);
        drop(pager);
        truncate_to(&page_file, usize::try_from(offset + len / 3).unwrap());
    }

    // Crash + recovery for each variant: recovery is checkpoint root + log
    // replay (TIER-061) and never consults spilled pages, so the recovered
    // logical state — rows, indexes, tx_id, auto-inc — is identical no
    // matter what the cold tier held or how mangled it is.
    let mut outcomes = Vec::new();
    for variant in ["all-hot", "cold-clean", "cold-torn-eviction"] {
        let (recovered, outcome) = recover_fresh(&log_dir, &snap_dir);
        assert_eq!(outcome.last_tx_id, Some(20), "{variant}");
        assert_eq!(outcome.next_tx_id, 21, "{variant} (STG-015)");
        assert_eq!(outcome.checkpoint_tx_id, Some(12), "{variant}");
        assert_equals_oracle(&recovered, 20, WL, variant);
        outcomes.push(fingerprint(&recovered));
    }
    assert!(
        outcomes.windows(2).all(|w| w[0] == w[1]),
        "recovered state must not depend on pre-crash residency"
    );
    // …and equals the live pre-crash store bit-for-bit, auto-inc included
    // (nothing acknowledged was lost — the eviction crash lost zero txs).
    assert_eq!(outcomes[0], fingerprint(&store));

    let oracle = oracle_store(20, WL);
    assert_eq!(outcomes[0], fingerprint(&oracle));
}
