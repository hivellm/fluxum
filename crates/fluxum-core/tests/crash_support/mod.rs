//! Shared fixtures for the T2.7 crash & durability suite (SPEC-013 §3,
//! TST-020..TST-026).
//!
//! The suite's oracle is a **deterministic workload**: step `i` (which is
//! also the transaction id, asserted at `begin`) derives its mutations from
//! `i` alone, so the exact committed state after any whole-transaction
//! prefix `1..=n` can be rebuilt from scratch ([`oracle_store`]) and compared
//! row-for-row against a recovered store ([`fingerprint`]). Zero
//! committed-transaction loss and per-transaction atomicity (TST-021) reduce
//! to one check: the recovered state equals the oracle at the recovered
//! prefix, and the prefix covers every acknowledged transaction.
//!
//! `User` and `Sensor` carry explicit primary keys so the oracle stays exact
//! across process restarts; `Event` is `#[auto_inc]`-style so the
//! same-process drills also verify auto-inc counter recovery (STG-040).

#![allow(dead_code)] // each test binary uses its own subset of the fixtures

use std::fs;
use std::path::{Path, PathBuf};

use fluxum_core::checkpoint::{CheckpointRepo, RecoveryOutcome, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, Row, RowValue, TableId};
use fluxum_core::types::Timestamp;

pub const SHARD: u32 = 3;
pub const EPOCH: u64 = 1;

/// Segment header length in bytes — the frozen STG-011 on-disk format
/// (`magic 8 | version 2 | checksum 1 | reserved 1 | epoch 8 | crc32c 4`),
/// see `commitlog::format`.
pub const SEGMENT_HEADER_LEN: usize = 24;

/// Entry envelope overhead — `length u32 | epoch u64 | body | crc32c u32`
/// (STG-011).
pub const ENTRY_OVERHEAD: usize = 16;

// --- canonical schema --------------------------------------------------------

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

/// Explicit-PK table with a secondary index (post-recovery index-integrity
/// checks, TST-025).
static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::BTree { columns: &[1] }],
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

/// Composite-PK table (delete + reinsert exercises in-place updates).
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

static EVENT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "tag",
        ty: FluxType::Str,
    },
];

/// Auto-inc table: recovery must resume its counter without id reuse
/// (STG-040). Only the same-process drills use it — a fresh oracle rebuild
/// stays deterministic because it replays the identical commit history.
static EVENT: TableSchema = TableSchema {
    name: "Event",
    columns: EVENT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

pub fn mem_store() -> MemStore {
    let schema = Schema::from_tables([&USER, &SENSOR, &EVENT]).unwrap_or_else(|e| panic!("{e}"));
    MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"))
}

pub fn user(store: &MemStore) -> TableId {
    store.table_id("User").unwrap_or_else(|| panic!("User"))
}

pub fn sensor(store: &MemStore) -> TableId {
    store.table_id("Sensor").unwrap_or_else(|| panic!("Sensor"))
}

pub fn event(store: &MemStore) -> TableId {
    store.table_id("Event").unwrap_or_else(|| panic!("Event"))
}

// --- the deterministic workload ----------------------------------------------

/// Options shaping one workload step; the same options must be used to build
/// the oracle.
#[derive(Debug, Clone, Copy, Default)]
pub struct StepOptions {
    /// Additionally insert 100 wide `User` rows per step (makes checkpoint
    /// writes slow enough for a kill to land mid-write).
    pub heavy: bool,
    /// Additionally insert one auto-inc `Event` row per step (same-process
    /// drills only — see [`EVENT`]).
    pub with_event: bool,
}

/// Commit workload step `i` (must equal the store's next tx id) and return
/// its tx id. Ops are a pure function of `i`: inserts on both tables, a
/// `User` delete every 3rd step, an in-place `Sensor` update every 4th.
pub fn apply_step(store: &MemStore, i: u64, options: StepOptions) -> u64 {
    apply_step_diff(store, i, options).tx_id
}

/// [`apply_step`] + durable-log append (the T2.2 commit path).
pub async fn commit_step(store: &MemStore, log: &CommitLog, i: u64, options: StepOptions) -> u64 {
    let diff = apply_step_diff(store, i, options);
    log.append_diff(&diff, Timestamp::from_micros(i64::try_from(i).unwrap_or(0)))
        .await
        .unwrap_or_else(|e| panic!("step {i}: append: {e}"));
    diff.tx_id
}

/// One workload step, returning the full diff (for callers that append it).
pub fn apply_step_diff(
    store: &MemStore,
    i: u64,
    options: StepOptions,
) -> fluxum_core::store::TxDiff {
    let u = user(store);
    let s = sensor(store);
    let mut tx = store.begin();
    assert_eq!(tx.tx_id(), i, "workload step {i} must run as tx {i}");
    tx.insert(
        u,
        vec![RowValue::U64(i), RowValue::Str(format!("user-{i}"))],
    )
    .unwrap_or_else(|e| panic!("step {i}: user insert: {e}"));
    let gi = i32::try_from(i).unwrap_or_else(|_| panic!("step {i} overflows i32"));
    tx.insert(
        s,
        vec![
            RowValue::I32(gi),
            RowValue::I32(-gi),
            RowValue::F64(f64::from(gi) * 0.5),
        ],
    )
    .unwrap_or_else(|e| panic!("step {i}: sensor insert: {e}"));
    if i > 2 && i.is_multiple_of(3) {
        tx.delete(u, &[RowValue::U64(i - 2)])
            .unwrap_or_else(|e| panic!("step {i}: user delete: {e}"));
    }
    if i > 1 && i.is_multiple_of(4) {
        let prev = gi - 1;
        tx.delete(s, &[RowValue::I32(prev), RowValue::I32(-prev)])
            .unwrap_or_else(|e| panic!("step {i}: sensor delete: {e}"));
        tx.insert(
            s,
            vec![
                RowValue::I32(prev),
                RowValue::I32(-prev),
                RowValue::F64(999.0),
            ],
        )
        .unwrap_or_else(|e| panic!("step {i}: sensor reinsert: {e}"));
    }
    if options.heavy {
        for k in 0..100u64 {
            let id = 1_000_000 + i * 1_000 + k;
            tx.insert(
                u,
                vec![
                    RowValue::U64(id),
                    RowValue::Str(format!("bulk-{id}-{k:01000}")),
                ],
            )
            .unwrap_or_else(|e| panic!("step {i}: bulk insert {k}: {e}"));
        }
    }
    if options.with_event {
        tx.insert(
            event(store),
            vec![RowValue::U64(0), RowValue::Str(format!("event-{i}"))],
        )
        .unwrap_or_else(|e| panic!("step {i}: event insert: {e}"));
    }
    tx.commit()
        .unwrap_or_else(|e| panic!("step {i}: commit: {e}"))
}

/// Rebuild the exact committed state after whole-transaction prefix `1..=n`.
pub fn oracle_store(n: u64, options: StepOptions) -> MemStore {
    let store = mem_store();
    for i in 1..=n {
        apply_step(&store, i, options);
    }
    store
}

/// Logical state fingerprint: rows and auto-inc high-water per table, over
/// every table of the canonical schema (TST-025 row-set equality).
pub fn fingerprint(store: &MemStore) -> Vec<(u32, Vec<Row>, u64)> {
    let snapshot = store.snapshot();
    [user(store), sensor(store), event(store)]
        .into_iter()
        .map(|id| {
            (
                id.as_u32(),
                snapshot
                    .scan(id)
                    .unwrap_or_else(|e| panic!("{e}"))
                    .cloned()
                    .collect(),
                snapshot
                    .auto_inc_high_water(id)
                    .unwrap_or_else(|e| panic!("{e}")),
            )
        })
        .collect()
}

/// Full recovery into a fresh store: open the log for append first (the
/// STG-031 quarantine side of recovery), then checkpoint + replay (STG-030).
pub fn recover_fresh(log_dir: &Path, snap_dir: &Path) -> (MemStore, RecoveryOutcome) {
    let log = CommitLog::open(log_dir, SHARD, EPOCH, CommitLogOptions::default())
        .unwrap_or_else(|e| panic!("recovery open: {e}"));
    log.close()
        .unwrap_or_else(|e| panic!("recovery close: {e}"));
    let repo = CheckpointRepo::open(snap_dir).unwrap_or_else(|e| panic!("repo: {e}"));
    let store = mem_store();
    let outcome = recover(&store, &repo, log_dir, SHARD).unwrap_or_else(|e| panic!("recover: {e}"));
    (store, outcome)
}

/// Assert the recovered store is exactly the oracle at prefix `n`, with
/// consistent secondary indexes (TST-021 / TST-025).
pub fn assert_equals_oracle(store: &MemStore, n: u64, options: StepOptions, context: &str) {
    let oracle = oracle_store(n, options);
    assert_eq!(
        fingerprint(store),
        fingerprint(&oracle),
        "{context}: recovered state diverges from the oracle at prefix {n}"
    );
    store
        .snapshot()
        .verify_index_integrity(user(store))
        .unwrap_or_else(|e| panic!("{context}: index integrity after recovery: {e}"));
}

// --- on-disk helpers ----------------------------------------------------------

/// Shard segment files in `dir`, sorted by name (== offset order, STG-014).
pub fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{e}"))
        .map(|e| e.unwrap_or_else(|e| panic!("{e}")).path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .collect();
    files.sort();
    files
}

/// Byte offsets of every entry boundary in a pristine segment, decoded via
/// the frozen STG-011 framing (`length u32 LE` at each boundary). The first
/// boundary is the header end; the last is the file end.
pub fn entry_boundaries(bytes: &[u8]) -> Vec<usize> {
    let mut offsets = vec![SEGMENT_HEADER_LEN];
    let mut offset = SEGMENT_HEADER_LEN;
    while offset + ENTRY_OVERHEAD <= bytes.len() {
        let len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        let end = offset + ENTRY_OVERHEAD + len;
        if end > bytes.len() {
            break;
        }
        offset = end;
        offsets.push(offset);
    }
    offsets
}

/// Copy every commit-log file (segments only, no sidecars) into `dst`.
pub fn copy_log_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap_or_else(|e| panic!("{e}"));
    for seg in segment_files(src) {
        let name = seg.file_name().unwrap_or_else(|| panic!("segment name"));
        fs::copy(&seg, dst.join(name)).unwrap_or_else(|e| panic!("{e}"));
    }
}

/// Flip one byte in `path` at `offset`.
pub fn flip_byte_at(path: &Path, offset: usize) {
    let mut bytes = fs::read(path).unwrap_or_else(|e| panic!("{e}"));
    bytes[offset] ^= 0xFF;
    fs::write(path, bytes).unwrap_or_else(|e| panic!("{e}"));
}

/// Truncate `path` to `len` bytes.
pub fn truncate_to(path: &Path, len: usize) {
    let bytes = fs::read(path).unwrap_or_else(|e| panic!("{e}"));
    fs::write(path, &bytes[..len]).unwrap_or_else(|e| panic!("{e}"));
}
