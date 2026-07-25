//! T7.1 phase A — the replica apply path (SPEC-014 REP-010/REP-014):
//! applying a primary's commit-log records to a live replica store converges
//! to the identical `CommittedState`, the replica's own log is
//! byte-identical over the shared `tx_id` range (same segment options ⇒
//! same rotation points ⇒ identical files), gaps/repeats abort, auto-inc
//! resumes without id reuse after a promotion, and `AS OF` answers match.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::fs;
use std::path::PathBuf;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions, TxRecord, replay};
use fluxum_core::store::MemStore;

use crash_support::{EPOCH, SHARD, StepOptions, commit_step, fingerprint, mem_store};

const WL: StepOptions = StepOptions {
    heavy: false,
    with_event: true,
};

fn small_segments() -> CommitLogOptions {
    CommitLogOptions {
        segment_max_bytes: 256,
        ..CommitLogOptions::default()
    }
}

/// A primary world: txs `1..=head` on its own store + log.
async fn primary_world(head: u64) -> (tempfile::TempDir, MemStore, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let log_dir = root.path().join("primary-log");
    let store = mem_store();
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, small_segments()).unwrap();
    for i in 1..=head {
        commit_step(&store, &log, i, WL).await;
    }
    log.wait_durable(head).await.unwrap();
    log.close().unwrap();
    (root, store, log_dir)
}

/// Collect the primary's records in order (the replication stream source).
fn stream_of(log_dir: &std::path::Path) -> Vec<(u64, TxRecord)> {
    let mut entries = Vec::new();
    replay(log_dir, SHARD, |epoch, record| {
        entries.push((epoch, record));
        Ok(())
    })
    .unwrap();
    entries
}

/// Apply a stream to a fresh replica: store merge (REP-014 step 4) + local
/// durable append (step 3), raising the log epoch when the stream does.
async fn apply_stream(replica: &MemStore, log: &CommitLog, stream: &[(u64, TxRecord)]) -> u64 {
    let mut last = 0;
    for (epoch, record) in stream {
        if *epoch > log.epoch() {
            log.set_epoch(*epoch).await.unwrap();
        }
        replica.apply_replica_record(record).unwrap();
        log.append(record.clone()).await.unwrap();
        last = record.tx_id;
    }
    log.wait_durable(last).await.unwrap();
    last
}

#[tokio::test]
async fn replica_apply_converges_and_the_log_is_byte_identical() {
    let (root, primary, primary_log_dir) = primary_world(12).await;
    let stream = stream_of(&primary_log_dir);
    assert_eq!(stream.len(), 12);

    let replica = mem_store();
    let replica_log_dir = root.path().join("replica-log");
    let log = CommitLog::open(&replica_log_dir, SHARD, EPOCH, small_segments()).unwrap();
    apply_stream(&replica, &log, &stream).await;
    log.close().unwrap();

    // REP-014: CommittedState equal (rows, indexes implied, auto-inc marks).
    assert_eq!(fingerprint(&replica), fingerprint(&primary));
    crash_support::assert_equals_oracle(&replica, 12, WL, "replica state");

    // REP-010: the replica's log is byte-identical over the shared range —
    // same entries, same envelope epochs, same rotation points ⇒ the
    // segment FILES are identical.
    let mut names: Vec<String> = fs::read_dir(&primary_log_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".log"))
        .collect();
    names.sort();
    assert!(!names.is_empty());
    for name in &names {
        let primary_bytes = fs::read(primary_log_dir.join(name)).unwrap();
        let replica_bytes = fs::read(replica_log_dir.join(name)).unwrap();
        assert_eq!(
            primary_bytes, replica_bytes,
            "segment {name} must be byte-identical (REP-010)"
        );
    }
}

#[tokio::test]
async fn an_unknown_table_in_a_record_aborts_the_apply() {
    let (_root, _primary, log_dir) = primary_world(3).await;
    let stream = stream_of(&log_dir);
    let replica = mem_store();
    replica.apply_replica_record(&stream[0].1).unwrap();
    // A record naming a table this schema does not have: loud abort, no
    // partial application, and the chain still continues afterwards.
    let mut forged = stream[1].1.clone();
    forged.mutations[0].table_id = 0xDEAD_BEEF;
    let err = replica.apply_replica_record(&forged).unwrap_err();
    assert!(err.to_string().contains("unknown table"), "{err}");
    replica.apply_replica_record(&stream[1].1).unwrap();
}

/// Convergent-replay edges (REP-014 mirrors recovery semantics): an insert
/// over an existing key replaces it; a delete of an absent key is a no-op;
/// an auto-inc advance naming an untracked table is tolerated; a torn tail
/// in the stream source ends the batch quietly (the writer finishes it).
#[tokio::test]
async fn forged_edge_records_apply_convergently() {
    use fluxum_core::commitlog::{LogValue, TableMutation, read_frames_after};
    use fluxum_core::store::RowValue;

    let (_root, _primary, log_dir) = primary_world(4).await;
    let stream = stream_of(&log_dir);
    let replica = mem_store();
    for (_, record) in &stream {
        replica.apply_replica_record(record).unwrap();
    }
    let user = replica.table_id("User").unwrap();

    // tx 5: a bare insert over an EXISTING pk (no delete first — the
    // convergent-replay shape) plus a delete of a pk that never existed,
    // plus an auto-inc advance for a table id nobody tracks.
    let replaced = [
        RowValue::U64(2),
        RowValue::Str("user-2-replaced".to_owned()),
    ];
    let forged = fluxum_core::commitlog::TxRecord {
        tx_id: 5,
        timestamp: 5,
        shard_id: SHARD,
        mutations: vec![TableMutation {
            table_id: user.as_u32(),
            inserts: vec![replaced.iter().map(LogValue::from).collect()],
            deletes: vec![serde_bytes::ByteBuf::from(vec![0xFF; 9])],
        }],
        auto_inc: vec![(0xDEAD_BEEF, 123)],
        caller: vec![0u8; 32],
        reducer_name: "forged".into(),
    };
    let diff = replica.apply_replica_record(&forged).unwrap();
    // The replace produced an insert diff; the ghost delete produced none.
    assert_eq!(diff.tables[0].inserts.len(), 1);
    assert!(diff.tables[0].deletes.is_empty());
    let snapshot = replica.snapshot();
    let row = snapshot
        .query_pk(user, &[RowValue::U64(2)])
        .unwrap()
        .unwrap();
    assert_eq!(row.values()[1], RowValue::Str("user-2-replaced".into()));

    // A torn tail in the source: everything before it still streams.
    let segment = fs::read_dir(&log_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .max()
        .unwrap();
    let bytes = fs::read(&segment).unwrap();
    let mut torn = bytes.clone();
    torn.extend_from_slice(&[0x04, 0x00, 0x00, 0x00, 0xAA]); // half a frame
    fs::write(&segment, &torn).unwrap();
    let (frames, last) = read_frames_after(&log_dir, SHARD, 0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 4, "the torn tail is excluded, not fatal");
    assert_eq!(last, 4);
}

#[tokio::test]
async fn gaps_and_repeats_abort_the_apply() {
    let (_root, _primary, primary_log_dir) = primary_world(6).await;
    let stream = stream_of(&primary_log_dir);

    let replica = mem_store();
    // Apply 1..=3, then feed tx 5 (a gap) and tx 3 again (a repeat).
    for (_, record) in &stream[..3] {
        replica.apply_replica_record(record).unwrap();
    }
    let gap = replica.apply_replica_record(&stream[4].1).unwrap_err();
    assert!(gap.to_string().contains("STG-015"), "{gap}");
    let repeat = replica.apply_replica_record(&stream[2].1).unwrap_err();
    assert!(repeat.to_string().contains("STG-015"), "{repeat}");
    // The chain continues where it left off.
    replica.apply_replica_record(&stream[3].1).unwrap();
}

#[tokio::test]
async fn auto_inc_resumes_without_reuse_after_promotion() {
    let (_root, primary, primary_log_dir) = primary_world(8).await;
    let stream = stream_of(&primary_log_dir);
    let replica = mem_store();
    for (_, record) in &stream {
        replica.apply_replica_record(record).unwrap();
    }

    // Promotion (REP-032 step 3): the replica opens its writer and commits
    // locally — tx ids continue at head+1 and auto-inc never reuses an id.
    let event = replica.table_id("Event").unwrap();
    let primary_hw = primary.snapshot().auto_inc_high_water(event).unwrap();
    let mut tx = replica.begin();
    assert_eq!(tx.tx_id(), 9, "tx ids continue past the applied head");
    tx.insert(
        event,
        vec![
            fluxum_core::store::RowValue::U64(0), // auto-assign
            fluxum_core::store::RowValue::Str("post-promotion".into()),
        ],
    )
    .unwrap();
    let diff = tx.commit().unwrap();
    let assigned = match diff.tables[0].inserts[0].values()[0] {
        fluxum_core::store::RowValue::U64(id) => id,
        ref other => panic!("auto-inc column must be u64, got {other:?}"),
    };
    assert!(
        assigned > primary_hw,
        "assigned id {assigned} must not reuse ids at or below the replicated \
         high-water {primary_hw} (STG-040)"
    );
}

/// The stream-source helpers: `read_frames_after` honors its byte budget
/// (never returning zero frames while entries remain), and
/// `decode_entry_frame` refuses truncation, trailing bytes, and emptiness.
#[tokio::test]
async fn stream_helpers_budget_and_reject_malformed_frames() {
    use fluxum_core::commitlog::{decode_entry_frame, encode_entry_frame, read_frames_after};

    let (_root, _primary, log_dir) = primary_world(6).await;
    // A 1-byte budget still yields exactly one frame per call (progress is
    // guaranteed), and consecutive calls walk the chain.
    let (frames, last) = read_frames_after(&log_dir, SHARD, 0, 1).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(last, 1);
    let (frames, last) = read_frames_after(&log_dir, SHARD, last, usize::MAX).unwrap();
    assert_eq!(frames.len(), 5, "the rest of the chain in one batch");
    assert_eq!(last, 6);
    // Nothing past the head.
    let (frames, last) = read_frames_after(&log_dir, SHARD, 6, usize::MAX).unwrap();
    assert!(frames.is_empty());
    assert_eq!(last, 6);

    // Frame round-trip + malformed rejections.
    let (epoch, record) = decode_entry_frame(&frames_of(&log_dir)[0]).unwrap();
    assert_eq!(epoch, EPOCH);
    assert_eq!(record.tx_id, 1);
    let reencoded = encode_entry_frame(epoch, &record).unwrap();
    assert_eq!(reencoded, frames_of(&log_dir)[0], "encode is byte-stable");
    assert!(decode_entry_frame(&[]).is_err(), "empty");
    assert!(
        decode_entry_frame(&reencoded[..reencoded.len() - 1]).is_err(),
        "truncated"
    );
    let mut trailing = reencoded.clone();
    trailing.push(0);
    assert!(decode_entry_frame(&trailing).is_err(), "trailing bytes");
}

/// The raw frames of a log directory, via the budget reader.
fn frames_of(log_dir: &std::path::Path) -> Vec<Vec<u8>> {
    fluxum_core::commitlog::read_frames_after(log_dir, SHARD, 0, usize::MAX)
        .unwrap()
        .0
}

/// The stream source refuses corruption (never streams garbage to a
/// replica), and the checkpoint transfer helpers surface their edges.
#[tokio::test]
async fn stream_source_and_transfer_helpers_surface_errors() {
    use fluxum_core::backup::{install_checkpoint_pack, pack_latest_checkpoint};
    use fluxum_core::commitlog::read_frames_after;

    let (root, _primary, log_dir) = primary_world(4).await;

    // Corrupt the trailing CRC byte of tx 1's frame — a COMPLETE frame in
    // the first segment, so the scan classifies it Corrupt (never Torn,
    // which the tail-tolerant reader would skip) on every platform,
    // independent of `read_dir` ordering.
    let frame_one = frames_of(&log_dir)[0].clone();
    let mut segments: Vec<PathBuf> = fs::read_dir(&log_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    segments.sort();
    let mut bytes = fs::read(&segments[0]).unwrap();
    let at = bytes
        .windows(frame_one.len())
        .position(|w| w == frame_one)
        .expect("tx 1's frame opens the first segment");
    bytes[at + frame_one.len() - 1] ^= 0xFF;
    fs::write(&segments[0], &bytes).unwrap();
    assert!(read_frames_after(&log_dir, SHARD, 0, usize::MAX).is_err());

    // No checkpoint yet → None (the primary then full-syncs nothing and
    // partial-streams from 1 instead).
    let ckpt_dir = root.path().join("no-checkpoints");
    assert!(
        pack_latest_checkpoint(&ckpt_dir, SHARD).unwrap().is_none(),
        "an empty repository packs nothing"
    );

    // A corrupt transferred pack is refused before touching the store.
    let store = mem_store();
    let err = install_checkpoint_pack(
        &store,
        &root.path().join("install-ckpt"),
        &root.path().join("install-log"),
        SHARD,
        b"not a checkpoint pack",
    )
    .unwrap_err();
    assert!(err.to_string().contains("decode failed"), "{err}");
}

#[tokio::test]
async fn as_of_reads_answer_identically_on_the_replica() {
    let (_root, primary, primary_log_dir) = primary_world(10).await;
    let stream = stream_of(&primary_log_dir);
    let replica = mem_store();
    for (_, record) in &stream {
        replica.apply_replica_record(record).unwrap();
    }
    let user = replica.table_id("User").unwrap();
    let at6_replica = replica
        .snapshot_as_of(fluxum_core::store::AsOfPoint::Tx(6))
        .unwrap()
        .row_count(user)
        .unwrap();
    let at6_primary = primary
        .snapshot_as_of(fluxum_core::store::AsOfPoint::Tx(6))
        .unwrap()
        .row_count(user)
        .unwrap();
    assert_eq!(at6_replica, at6_primary, "SPEC-022 parity on replicas");
}
