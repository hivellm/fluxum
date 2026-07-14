//! T2.2 verification suite (DAG exit tests; SPEC-002 acceptance 2/3/8):
//! write/replay roundtrips over insert/delete interleavings, torn-tail
//! quarantine at every byte offset of the last record, epoch fencing,
//! group-commit batching, rotation + compaction.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use serde_bytes::ByteBuf;

use crate::schema::{ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule};
use crate::store::{MemStore, RowValue, TableId};
use crate::types::Timestamp;

use super::CommitLogOptions;
use super::format::{SEGMENT_HEADER_LEN, ScannedEntry, scan_entry};
use super::record::{LogValue, TableMutation, TxRecord};
use super::replay::replay;
use super::writer::{CommitLog, DurableState};

const SHARD: u32 = 3;

// --- fixtures --------------------------------------------------------------

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
];

static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F64,
    },
];

static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0, 1],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn mem_store() -> MemStore {
    MemStore::new(&Schema::from_tables([&USER, &SENSOR]).unwrap()).unwrap()
}

/// A small synthetic record for log-level tests.
fn rec(tx_id: u64) -> TxRecord {
    TxRecord {
        tx_id,
        timestamp: 1_000 + i64::try_from(tx_id).unwrap(),
        shard_id: SHARD,
        mutations: vec![TableMutation {
            table_id: 0xAB,
            inserts: vec![vec![LogValue::U64(tx_id), LogValue::Str("row".into())]],
            deletes: vec![ByteBuf::from(tx_id.to_le_bytes().to_vec())],
        }],
        auto_inc: vec![],
    }
}

fn collect(dir: &Path) -> (Vec<(u64, TxRecord)>, super::ReplayReport) {
    let mut got = Vec::new();
    let report = replay(dir, SHARD, |epoch, record| {
        got.push((epoch, record));
        Ok(())
    })
    .unwrap();
    (got, report)
}

fn segment_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .collect();
    files.sort();
    files
}

/// Byte offsets of every entry boundary in a segment (header end first).
fn entry_boundaries(bytes: &[u8]) -> Vec<usize> {
    let mut offsets = vec![SEGMENT_HEADER_LEN];
    let mut offset = SEGMENT_HEADER_LEN;
    while let ScannedEntry::Entry { end, .. } = scan_entry(bytes, offset) {
        offset = end;
        offsets.push(offset);
    }
    offsets
}

// --- write / replay roundtrip (tasks 1.1, 1.6) -----------------------------

#[tokio::test]
async fn write_replay_roundtrip_over_insert_delete_interleavings() {
    let dir = tempfile::tempdir().unwrap();
    let store = mem_store();
    let user = store.table_id("User").unwrap();
    let sensor = store.table_id("Sensor").unwrap();

    let mut diffs = Vec::new();
    // tx1: inserts on both tables (auto-inc assigns user ids 1 and 2).
    let mut tx = store.begin();
    tx.insert(user, vec![RowValue::U64(0), RowValue::Str("ana".into())])
        .unwrap();
    tx.insert(user, vec![RowValue::U64(0), RowValue::Str("bo".into())])
        .unwrap();
    tx.insert(
        sensor,
        vec![RowValue::I32(-1), RowValue::I32(2), RowValue::F64(0.5)],
    )
    .unwrap();
    diffs.push(tx.commit().unwrap());
    // tx2: delete + in-place update interleaved with an insert.
    let mut tx = store.begin();
    tx.delete(user, &[RowValue::U64(1)]).unwrap();
    tx.delete(sensor, &[RowValue::I32(-1), RowValue::I32(2)])
        .unwrap();
    tx.insert(
        sensor,
        vec![RowValue::I32(-1), RowValue::I32(2), RowValue::F64(0.75)],
    )
    .unwrap();
    diffs.push(tx.commit().unwrap());
    // tx3: plain insert.
    let mut tx = store.begin();
    tx.insert(user, vec![RowValue::U64(0), RowValue::Str("cy".into())])
        .unwrap();
    diffs.push(tx.commit().unwrap());

    let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();
    for diff in &diffs {
        log.append_diff(diff, Timestamp::from_micros(42))
            .await
            .unwrap();
    }
    log.wait_durable(diffs.last().unwrap().tx_id).await.unwrap();
    log.close().unwrap();

    let (got, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    assert_eq!(report.records, diffs.len() as u64);
    assert_eq!(report.last_tx_id, Some(diffs.last().unwrap().tx_id));
    assert_eq!(report.next_tx_id(), diffs.last().unwrap().tx_id + 1);

    for (diff, (epoch, record)) in diffs.iter().zip(&got) {
        assert_eq!(*epoch, 1);
        assert_eq!(record.tx_id, diff.tx_id);
        assert_eq!(record.shard_id, SHARD);
        assert_eq!(record.timestamp, 42);
        assert_eq!(record.mutations.len(), diff.tables.len());
        for (table_diff, mutation) in diff.tables.iter().zip(&record.mutations) {
            assert_eq!(mutation.table(), table_diff.table_id);
            // Inserted rows replay to identical store rows.
            assert_eq!(mutation.insert_rows().unwrap(), table_diff.inserts);
            // Deletes carry the byte-identical FluxBIN PKs.
            let got_pks: Vec<&[u8]> = mutation.delete_pks().collect();
            let want_pks: Vec<&[u8]> = table_diff
                .deletes
                .iter()
                .map(|(pk, _)| pk.as_bytes())
                .collect();
            assert_eq!(got_pks, want_pks);
        }
        let want_auto: Vec<(u32, u64)> = diff
            .auto_inc
            .iter()
            .map(|(table, hw)| (table.as_u32(), *hw))
            .collect();
        assert_eq!(record.auto_inc, want_auto);
    }

    // Auto-inc counters resume without reuse (STG-040): the replayed
    // high-water mark equals the store's durable mark and covers every
    // assigned id, so post-recovery generation at high_water + 1 never
    // reuses an id.
    let store_hw = store.snapshot().auto_inc_high_water(user).unwrap();
    let mut replayed_hw = 0;
    for (_, record) in &got {
        for (table, hw) in &record.auto_inc {
            if TableId::from_raw(*table) == user {
                replayed_hw = *hw;
            }
        }
    }
    assert_eq!(replayed_hw, store_hw);
    assert!(replayed_hw >= 3, "high-water must cover assigned ids 1..=3");
}

// --- tx_id monotonicity across restart (STG-015, task 1.6) -----------------

#[tokio::test]
async fn tx_id_strictly_increases_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let opts = CommitLogOptions::default();
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    for tx in 1..=3 {
        log.append(rec(tx)).await.unwrap();
    }
    log.wait_durable(3).await.unwrap();
    assert_eq!(log.close().unwrap(), Some(3));

    // Restart: the counter resumes from the recovered tx id.
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    assert_eq!(log.recovery().last_tx_id, Some(3));
    assert_eq!(log.durable_tx_id().unwrap(), Some(3));
    // A repeat or decrease is rejected (STG-015).
    assert!(log.append(rec(3)).await.is_err());
    assert!(log.append(rec(2)).await.is_err());
    assert_eq!(log.append(rec(4)).await.unwrap(), 4);
    log.wait_durable(4).await.unwrap();
    log.close().unwrap();

    let (got, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    let ids: Vec<u64> = got.iter().map(|(_, r)| r.tx_id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    // A record for another shard is rejected outright.
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    let mut foreign = rec(5);
    foreign.shard_id = SHARD + 1;
    assert!(log.append(foreign).await.is_err());
    log.close().unwrap();
}

#[tokio::test]
async fn empty_log_opens_appends_and_distinguishes_no_durable_offset() {
    let dir = tempfile::tempdir().unwrap();
    let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();
    // "Empty log" is distinct from "tx 0 durable".
    assert_eq!(log.recovery().last_tx_id, None);
    assert_eq!(log.durable_tx_id().unwrap(), None);
    assert!(log.append(rec(0)).await.is_err(), "there is no tx 0");
    log.append(rec(1)).await.unwrap();
    log.wait_durable(1).await.unwrap();
    assert_eq!(log.close().unwrap(), Some(1));
}

// --- torn tail: quarantine + resume (STG-031, task 1.4) ---------------------

/// Build a 3-record base log and return its single segment's bytes.
async fn base_log(dir: &Path) -> Vec<u8> {
    let log = CommitLog::open(dir, SHARD, 1, CommitLogOptions::default()).unwrap();
    for tx in 1..=3 {
        log.append(rec(tx)).await.unwrap();
    }
    log.wait_durable(3).await.unwrap();
    log.close().unwrap();
    let segments = segment_files(dir);
    assert_eq!(segments.len(), 1);
    fs::read(&segments[0]).unwrap()
}

#[tokio::test]
async fn torn_tail_at_every_byte_offset_is_quarantined_and_resumable() {
    let base_dir = tempfile::tempdir().unwrap();
    let bytes = base_log(base_dir.path()).await;
    let name = segment_files(base_dir.path())[0]
        .file_name()
        .unwrap()
        .to_owned();
    let boundaries = entry_boundaries(&bytes);
    assert_eq!(boundaries.len(), 4); // header + 3 entries
    let last_start = boundaries[2];
    assert_eq!(*boundaries.last().unwrap(), bytes.len());

    // Truncate at every byte offset inside the last record (crash
    // mid-append) — SPEC-002 acceptance 3.
    for cut in last_start + 1..bytes.len() {
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join(&name);
        fs::write(&seg, &bytes[..cut]).unwrap();

        // Read-only replay stops at the torn entry, keeps all prior ones.
        let (got, report) = collect(dir.path());
        assert_eq!(report.last_tx_id, Some(2), "cut at {cut}");
        assert_eq!(got.len(), 2);
        let corruption = report.corruption.expect("torn tail must be reported");
        assert_eq!(corruption.offset, last_start as u64);

        // Recovery quarantines the tail non-destructively and resumes.
        let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();
        assert_eq!(log.recovery().last_tx_id, Some(2));
        let q = log.recovery().quarantine.clone().expect("quarantine");
        assert_eq!(q.from_offset, last_start as u64);
        // The sidecar preserves the torn tail byte-identically.
        assert_eq!(fs::read(&q.sidecar).unwrap(), &bytes[last_start..cut]);
        // The segment shrank back to the last valid entry boundary.
        assert_eq!(fs::metadata(&seg).unwrap().len(), last_start as u64);

        // Appends resume at the boundary; tx 3 is re-written cleanly.
        log.append(rec(3)).await.unwrap();
        log.wait_durable(3).await.unwrap();
        log.close().unwrap();
        let (got, report) = collect(dir.path());
        assert!(report.corruption.is_none(), "cut at {cut}");
        assert_eq!(report.last_tx_id, Some(3));
        assert_eq!(got.len(), 3);
    }

    // Cutting exactly at the boundary loses the whole record cleanly: no
    // corruption, no quarantine.
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join(&name), &bytes[..last_start]).unwrap();
    let (_, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    assert_eq!(report.last_tx_id, Some(2));
    let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();
    assert!(log.recovery().quarantine.is_none());
    log.close().unwrap();
}

#[tokio::test]
async fn bitflip_at_every_byte_of_the_last_record_is_quarantined() {
    let base_dir = tempfile::tempdir().unwrap();
    let bytes = base_log(base_dir.path()).await;
    let name = segment_files(base_dir.path())[0]
        .file_name()
        .unwrap()
        .to_owned();
    let last_start = entry_boundaries(&bytes)[2];

    for pos in last_start..bytes.len() {
        let mut flipped = bytes.clone();
        flipped[pos] ^= 0xFF;
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(&name), &flipped).unwrap();

        let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();
        assert_eq!(log.recovery().last_tx_id, Some(2), "flip at {pos}");
        let q = log.recovery().quarantine.clone().expect("quarantine");
        assert_eq!(q.from_offset, last_start as u64, "flip at {pos}");
        assert_eq!(fs::read(&q.sidecar).unwrap(), &flipped[last_start..]);
        log.close().unwrap();
    }
}

#[tokio::test]
async fn corruption_in_a_non_tail_segment_stops_replay_and_refuses_append() {
    let dir = tempfile::tempdir().unwrap();
    let opts = CommitLogOptions {
        segment_max_bytes: 128, // force rotation every couple of entries
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    for tx in 1..=8 {
        log.append(rec(tx)).await.unwrap();
    }
    log.wait_durable(8).await.unwrap();
    log.close().unwrap();
    let segments = segment_files(dir.path());
    assert!(segments.len() > 1, "expected rotation");

    // Flip a byte inside the first (non-tail) segment.
    let mut first = fs::read(&segments[0]).unwrap();
    first[SEGMENT_HEADER_LEN + 5] ^= 0xFF;
    fs::write(&segments[0], &first).unwrap();

    // Replay keeps everything before the corrupt entry and reports it.
    let (_, report) = collect(dir.path());
    let corruption = report.corruption.expect("corruption report");
    assert_eq!(corruption.segment, segments[0]);

    // Open refuses: destructive repair of a non-tail segment is reset_to
    // territory (STG-031), never an implicit side effect.
    let err = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap_err();
    assert!(err.to_string().contains("non-tail"), "{err}");
}

// --- epoch fencing (STG-011, task 1.1) --------------------------------------

#[tokio::test]
async fn epoch_lower_than_durable_is_rejected_and_bumps_are_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let opts = CommitLogOptions::default();
    let log = CommitLog::open(dir.path(), SHARD, 5, opts).unwrap();
    log.append(rec(1)).await.unwrap();
    log.append(rec(2)).await.unwrap();
    log.wait_durable(2).await.unwrap();
    log.close().unwrap();

    // Opening under a lower epoch than durably written is rejected.
    let err = CommitLog::open(dir.path(), SHARD, 3, opts).unwrap_err();
    assert!(err.to_string().contains("epoch"), "{err}");

    // Same epoch reopens; a lower set_epoch is rejected, a higher one takes
    // effect for subsequent entries.
    let log = CommitLog::open(dir.path(), SHARD, 5, opts).unwrap();
    assert_eq!(log.epoch(), 5);
    assert!(log.set_epoch(4).await.is_err());
    log.set_epoch(7).await.unwrap();
    assert_eq!(log.epoch(), 7);
    log.append(rec(3)).await.unwrap();
    log.wait_durable(3).await.unwrap();
    log.close().unwrap();

    let (got, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    let epochs: Vec<u64> = got.iter().map(|(epoch, _)| *epoch).collect();
    assert_eq!(epochs, vec![5, 5, 7]);

    // A newer leader lineage opens fine.
    let log = CommitLog::open(dir.path(), SHARD, 8, opts).unwrap();
    assert_eq!(log.recovery().epoch, 7);
    log.close().unwrap();
}

// --- group commit (STG-012, task 1.2) ---------------------------------------

#[tokio::test]
async fn group_commit_batches_fsyncs_and_advances_the_watermark_monotonically() {
    const N: u64 = 512;
    let dir = tempfile::tempdir().unwrap();
    let log = CommitLog::open(dir.path(), SHARD, 1, CommitLogOptions::default()).unwrap();

    let mut rx = log.subscribe_durable();
    let watcher = tokio::spawn(async move {
        let mut seen = Vec::new();
        loop {
            if let DurableState::Durable(Some(tx)) = &*rx.borrow_and_update() {
                seen.push(*tx);
            }
            if rx.changed().await.is_err() {
                break;
            }
        }
        seen
    });

    for tx in 1..=N {
        log.append(rec(tx)).await.unwrap();
    }
    log.wait_durable(N).await.unwrap();
    let fsyncs = log.fsync_count();
    assert!(
        fsyncs < N / 4,
        "fsync count must be far below tx count under load: {fsyncs} fsyncs for {N} txs"
    );
    assert_eq!(log.close().unwrap(), Some(N));

    // The published durable offset advances strictly monotonically.
    let seen = watcher.await.unwrap();
    assert!(!seen.is_empty());
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
    assert_eq!(*seen.last().unwrap(), N);
}

// --- rotation + compaction (STG-013/STG-014, task 1.3) ----------------------

#[tokio::test]
async fn rotation_produces_ordered_segments_and_compaction_respects_holds() {
    let dir = tempfile::tempdir().unwrap();
    let opts = CommitLogOptions {
        segment_max_bytes: 128,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    for tx in 1..=12 {
        log.append(rec(tx)).await.unwrap();
    }
    log.wait_durable(12).await.unwrap();

    let segments = super::segment::list_segments(dir.path(), SHARD).unwrap();
    assert!(
        segments.len() >= 3,
        "expected rotation, got {}",
        segments.len()
    );
    // Directory sort order equals offset order (zero-padded names).
    let firsts: Vec<u64> = segments.iter().map(|s| s.first_tx_id).collect();
    let mut sorted = firsts.clone();
    sorted.sort_unstable();
    assert_eq!(firsts, sorted);
    assert_eq!(firsts[0], 1);

    // Replay spans segment boundaries seamlessly.
    let (got, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    assert_eq!(got.len(), 12);

    // A replication retention hold blocks compaction of segments a replica
    // still needs (STG-013).
    let second_first = segments[1].first_tx_id;
    log.set_retention_hold(Some(second_first - 1));
    assert!(log.compact(12).unwrap().is_empty());

    // With the hold released, segments fully covered by the checkpoint go;
    // the active tail always survives.
    log.set_retention_hold(None);
    let deleted = log.compact(second_first - 1).unwrap();
    assert_eq!(deleted, vec![segments[0].path.clone()]);

    // The remaining log still replays cleanly, starting mid-history.
    let (got, report) = collect(dir.path());
    assert!(report.corruption.is_none());
    assert_eq!(report.last_tx_id, Some(12));
    assert_eq!(got.first().unwrap().1.tx_id, second_first);

    // And the compacted log still recovers for append.
    log.close().unwrap();
    let log = CommitLog::open(dir.path(), SHARD, 1, opts).unwrap();
    assert_eq!(log.recovery().last_tx_id, Some(12));
    log.append(rec(13)).await.unwrap();
    log.wait_durable(13).await.unwrap();
    log.close().unwrap();
}
