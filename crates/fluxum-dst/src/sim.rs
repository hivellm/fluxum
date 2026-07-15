//! The deterministic simulation driver (TST-130..TST-133): a seeded op
//! stream drives the **real** storage engine — `MemStore`, `CommitLog`,
//! `CheckpointRepo`, real files, real quarantine, real recovery — and the
//! trivially correct [`Model`] in lockstep.
//!
//! Every simulation decision (mutations, commit/abort, acks, checkpoint
//! cadence, checkpoint corruption, compaction, crash points, and the fault
//! applied at each crash) derives from the run seed alone, so any failure
//! reproduces from its seed (TST-131). Crash faults model the physically
//! possible post-`kill -9` disk states within the STG-012 fsync model:
//!
//! - **lost fsync**: the un-acknowledged log suffix vanishes at an entry
//!   boundary;
//! - **torn write**: the first lost entry is additionally cut mid-frame;
//! - **bit flip**: the first lost entry is corrupted in place (recovery
//!   must quarantine from it and keep the prefix);
//! - **checkpoint corruption**: a manifest is flipped, forcing the STG-021
//!   fallback chain.
//!
//! Acknowledged transactions (`wait_durable` returned) are never cut — the
//! sim asserts they all survive, which is exactly TST-021's zero-loss
//! invariant under simulation.

use std::fs;
use std::path::PathBuf;

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions, replay};
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::types::Timestamp;

use crate::model::{Model, ModelState};
use crate::rng::SimRng;

const SHARD: u32 = 7;

/// Segment header length (frozen STG-011 format; see `commitlog::format`).
const SEGMENT_HEADER_LEN: usize = 24;
/// Entry envelope overhead: `length u32 | epoch u64 | crc32c u32` (STG-011).
const ENTRY_OVERHEAD: usize = 16;

// --- canonical schema ----------------------------------------------------------

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

fn fresh_store() -> MemStore {
    let schema = match Schema::from_tables([&USER, &SENSOR]) {
        Ok(s) => s,
        Err(e) => panic!("schema: {e}"),
    };
    match MemStore::new(&schema) {
        Ok(s) => s,
        Err(e) => panic!("store: {e}"),
    }
}

// --- report ---------------------------------------------------------------------

/// What one simulation run did, plus its determinism trace (TST-130).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    /// The seed that produced this run.
    pub seed: u64,
    /// Committed transactions.
    pub commits: u64,
    /// Rolled-back transactions.
    pub aborts: u64,
    /// Operations rejected by both engine and model (acceptance parity).
    pub rejections: u64,
    /// Simulated crash/recovery cycles.
    pub crashes: u64,
    /// Checkpoints written (including deliberately corrupted ones).
    pub checkpoints: u64,
    /// The determinism log: one chained hash per checkpointed observation.
    pub trace: Vec<u64>,
}

fn fnv1a(bytes: &[u8], mut hash: u64) -> u64 {
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

/// Append one chained determinism-log checkpoint (free function so it can
/// run while a transaction borrows the store).
fn trace_push(trace: &mut Vec<u64>, code: u64, payload: u64) {
    let prev = trace.last().copied().unwrap_or(0xcbf2_9ce4_8422_2325);
    let mut h = fnv1a(&code.to_le_bytes(), prev);
    h = fnv1a(&payload.to_le_bytes(), h);
    trace.push(h);
}

// --- the world --------------------------------------------------------------------

struct World {
    seed: u64,
    root: tempfile::TempDir,
    log_dir: PathBuf,
    store: MemStore,
    log: Option<CommitLog>,
    repo: CheckpointRepo,
    rt: tokio::runtime::Runtime,
    rng: SimRng,
    faults: SimRng,
    model: Model,
    epoch: u64,
    /// Highest tx `wait_durable` returned for (the client-visible ack).
    acked: u64,
    /// Highest committed tx id.
    last_committed: u64,
    /// Everything `<=` this is durable without waiting on the live log:
    /// the recovered prefix (checkpoint + replayed log) after a crash. A
    /// checkpoint may cover transactions the cut log no longer holds, so
    /// waiting on the log's watermark for them would wait forever.
    durable_floor: u64,
    /// Manifests we deliberately corrupted (their `last_tx_id`s).
    corrupted_ckpts: Vec<u64>,
    /// Highest tx the log has been compacted through. Recoverability
    /// invariant maintained by the sim (as production's SnapshotWorker
    /// does by compacting only up to retained checkpoints): the newest
    /// **valid** checkpoint always covers the compaction floor — corrupting
    /// the checkpoint *and* truncating the log below it is a double fault
    /// outside the STG-021 fallback contract.
    compact_floor: u64,
    report: SimReport,
}

impl World {
    fn new(seed: u64) -> Self {
        let root = match tempfile::tempdir() {
            Ok(d) => d,
            Err(e) => panic!("[seed {seed}] tempdir: {e}"),
        };
        let log_dir = root.path().join("log");
        let snap_dir = root.path().join("snapshots");
        let repo = match CheckpointRepo::open(&snap_dir) {
            Ok(r) => r,
            Err(e) => panic!("[seed {seed}] repo: {e}"),
        };
        let log = match CommitLog::open(&log_dir, SHARD, 1, Self::log_options()) {
            Ok(l) => l,
            Err(e) => panic!("[seed {seed}] log: {e}"),
        };
        let rt = match tokio::runtime::Builder::new_current_thread().build() {
            Ok(rt) => rt,
            Err(e) => panic!("[seed {seed}] runtime: {e}"),
        };
        let rng = SimRng::new(seed);
        let faults = rng.fork(0xFA01);
        Self {
            seed,
            root,
            log_dir,
            store: fresh_store(),
            log: Some(log),
            repo,
            rt,
            rng,
            faults,
            model: Model::default(),
            epoch: 1,
            acked: 0,
            last_committed: 0,
            durable_floor: 0,
            corrupted_ckpts: Vec::new(),
            compact_floor: 0,
            report: SimReport {
                seed,
                commits: 0,
                aborts: 0,
                rejections: 0,
                crashes: 0,
                checkpoints: 0,
                trace: Vec::new(),
            },
        }
    }

    fn log_options() -> CommitLogOptions {
        CommitLogOptions {
            segment_max_bytes: 512, // rotate often: crashes span segments
            ..CommitLogOptions::default()
        }
    }

    fn log(&self) -> &CommitLog {
        match &self.log {
            Some(log) => log,
            None => panic!("[seed {}] log used mid-crash", self.seed),
        }
    }

    fn trace(&mut self, code: u64, payload: u64) {
        trace_push(&mut self.report.trace, code, payload);
    }

    /// The engine's committed logical state, converted to the model domain.
    fn engine_state(&self) -> ModelState {
        let snapshot = self.store.snapshot();
        let user = TableId::of("User");
        let sensor = TableId::of("Sensor");
        let mut state = ModelState::default();
        let users = match snapshot.scan(user) {
            Ok(rows) => rows,
            Err(e) => panic!("[seed {}] scan User: {e}", self.seed),
        };
        for row in users {
            match (row.value(0), row.value(1)) {
                (Some(RowValue::U64(id)), Some(RowValue::Str(name))) => {
                    state.users.insert(*id, name.clone());
                }
                other => panic!("[seed {}] malformed User row: {other:?}", self.seed),
            }
        }
        let sensors = match snapshot.scan(sensor) {
            Ok(rows) => rows,
            Err(e) => panic!("[seed {}] scan Sensor: {e}", self.seed),
        };
        for row in sensors {
            match (row.value(0), row.value(1), row.value(2)) {
                (Some(RowValue::I32(x)), Some(RowValue::I32(y)), Some(RowValue::F64(reading))) => {
                    state.sensors.insert((*x, *y), reading.to_bits());
                }
                other => panic!("[seed {}] malformed Sensor row: {other:?}", self.seed),
            }
        }
        state
    }

    fn assert_states_equal(&self, context: &str) {
        let engine = self.engine_state();
        assert!(
            &engine == self.model.current(),
            "[seed {}] {context}: engine state diverged from the model oracle\n\
             engine: {engine:?}\nmodel:  {:?}",
            self.seed,
            self.model.current()
        );
    }

    // --- ops -----------------------------------------------------------------

    /// One transaction: 1..=4 mutations with acceptance parity against the
    /// model, then commit + append (75%) or rollback (25%) (TST-132).
    fn op_transaction(&mut self) {
        let user = TableId::of("User");
        let sensor = TableId::of("Sensor");
        let mut tx = self.store.begin();
        let mut pending = self.model.current().clone();
        let mutations = 1 + self.rng.below(4);
        for _ in 0..mutations {
            match self.rng.below(4) {
                0 => {
                    // User insert — may collide (duplicate PK must be
                    // rejected by both sides, including reinsert-after-
                    // tx-delete which both sides must ACCEPT).
                    let id = self.rng.below(32);
                    let name = format!("u{}-{}", id, self.rng.below(1000));
                    let predicted = !pending.users.contains_key(&id);
                    let got = tx.insert(user, vec![RowValue::U64(id), RowValue::Str(name.clone())]);
                    assert_eq!(
                        got.is_ok(),
                        predicted,
                        "[seed {}] User insert({id}) acceptance parity",
                        self.seed
                    );
                    if got.is_ok() {
                        pending.users.insert(id, name);
                    } else {
                        self.report.rejections += 1;
                    }
                    trace_push(&mut self.report.trace, 1, id);
                }
                1 => {
                    // User delete — presence parity on the returned bool.
                    let id = self.rng.below(32);
                    let predicted = pending.users.contains_key(&id);
                    let got = match tx.delete(user, &[RowValue::U64(id)]) {
                        Ok(present) => present,
                        Err(e) => panic!("[seed {}] User delete({id}): {e}", self.seed),
                    };
                    assert_eq!(
                        got, predicted,
                        "[seed {}] User delete({id}) presence parity",
                        self.seed
                    );
                    pending.users.remove(&id);
                    trace_push(&mut self.report.trace, 2, id);
                }
                2 => {
                    // Sensor upsert: in-place update = delete + reinsert.
                    let x = i32::try_from(self.rng.below(8)).unwrap_or(0);
                    let y = i32::try_from(self.rng.below(8)).unwrap_or(0);
                    let reading = self.rng.below(4000) as f64 * 0.25;
                    if pending.sensors.contains_key(&(x, y)) {
                        match tx.delete(sensor, &[RowValue::I32(x), RowValue::I32(y)]) {
                            Ok(true) => {}
                            other => panic!(
                                "[seed {}] Sensor delete-for-update({x},{y}): {other:?}",
                                self.seed
                            ),
                        }
                    }
                    if let Err(e) = tx.insert(
                        sensor,
                        vec![RowValue::I32(x), RowValue::I32(y), RowValue::F64(reading)],
                    ) {
                        panic!("[seed {}] Sensor upsert({x},{y}): {e}", self.seed);
                    }
                    pending.sensors.insert((x, y), reading.to_bits());
                    trace_push(&mut self.report.trace, 3, (x as u64) << 32 | y as u64);
                }
                _ => {
                    // Sensor delete.
                    let x = i32::try_from(self.rng.below(8)).unwrap_or(0);
                    let y = i32::try_from(self.rng.below(8)).unwrap_or(0);
                    let predicted = pending.sensors.contains_key(&(x, y));
                    let got = match tx.delete(sensor, &[RowValue::I32(x), RowValue::I32(y)]) {
                        Ok(present) => present,
                        Err(e) => panic!("[seed {}] Sensor delete({x},{y}): {e}", self.seed),
                    };
                    assert_eq!(
                        got, predicted,
                        "[seed {}] Sensor delete({x},{y}) presence parity",
                        self.seed
                    );
                    pending.sensors.remove(&(x, y));
                    trace_push(&mut self.report.trace, 4, (x as u64) << 32 | y as u64);
                }
            }
        }
        if self.rng.chance(75) {
            let diff = match tx.commit() {
                Ok(diff) => diff,
                Err(e) => panic!("[seed {}] commit: {e}", self.seed),
            };
            let tx_id = diff.tx_id;
            let timestamp = Timestamp::from_micros(i64::try_from(tx_id).unwrap_or(0));
            let append = self.rt.block_on(self.log().append_diff(&diff, timestamp));
            if let Err(e) = append {
                panic!("[seed {}] append tx {tx_id}: {e}", self.seed);
            }
            self.last_committed = tx_id;
            self.model.commit(tx_id, pending);
            self.report.commits += 1;
            // Full state equality at every commit (TST-132).
            self.assert_states_equal("post-commit");
            let state_hash = fnv1a(
                &self.engine_state().canonical_bytes(),
                0xcbf2_9ce4_8422_2325,
            );
            self.trace(10, state_hash);
        } else {
            tx.rollback();
            self.report.aborts += 1;
            // A rollback leaves no observable state change.
            self.assert_states_equal("post-rollback");
            self.trace(11, 0);
        }
    }

    /// Acknowledge the newest commit: `wait_durable` + record. Everything up
    /// to `acked` must survive every subsequent crash.
    fn op_ack(&mut self) {
        if self.last_committed == 0 {
            return;
        }
        let target = self.last_committed;
        // A recovered prefix is durable by construction (checkpoint + log);
        // only transactions appended after the last recovery gate on the
        // live log's watermark.
        if target > self.durable_floor
            && let Err(e) = self.rt.block_on(self.log().wait_durable(target))
        {
            panic!("[seed {}] wait_durable({target}): {e}", self.seed);
        }
        self.acked = target;
        self.trace(20, target);
    }

    /// Write a checkpoint; sometimes corrupt its manifest (STG-021 fallback
    /// pressure).
    fn op_checkpoint(&mut self) {
        let newest = match self.repo.list(SHARD) {
            Ok(refs) => refs.last().map_or(0, |r| r.last_tx_id),
            Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
        };
        if self.last_committed <= newest {
            return;
        }
        let stats = match self.repo.write(
            &self.store.snapshot(),
            SHARD,
            self.last_committed,
            self.epoch,
        ) {
            Ok(stats) => stats,
            Err(e) => panic!(
                "[seed {}] checkpoint at {}: {e}",
                self.seed, self.last_committed
            ),
        };
        self.report.checkpoints += 1;
        // Corrupting the new (newest) checkpoint is only a *survivable*
        // fault if recovery still has a full path: either an older valid
        // checkpoint covering the compaction floor, or the uncompacted log
        // from tx 1.
        let older_valid_max = match self.repo.list(SHARD) {
            Ok(refs) => refs
                .iter()
                .map(|r| r.last_tx_id)
                .filter(|tx| *tx != self.last_committed && !self.corrupted_ckpts.contains(tx))
                .max(),
            Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
        };
        let survivable =
            self.compact_floor == 0 || older_valid_max.is_some_and(|tx| tx >= self.compact_floor);
        let mut corrupted = 0u64;
        if survivable && self.faults.chance(30) {
            // Flip one manifest byte: the checkpoint must fail verification
            // and recovery must fall back past it.
            let mut bytes = match fs::read(&stats.manifest) {
                Ok(b) => b,
                Err(e) => panic!("[seed {}] read manifest: {e}", self.seed),
            };
            let pos = self.faults.index(bytes.len());
            bytes[pos] ^= 0xFF;
            if let Err(e) = fs::write(&stats.manifest, bytes) {
                panic!("[seed {}] corrupt manifest: {e}", self.seed);
            }
            self.corrupted_ckpts.push(self.last_committed);
            corrupted = 1;
        }
        self.trace(30, self.last_committed << 1 | corrupted);
    }

    /// Prune retention and compact the log up to the oldest retained
    /// checkpoint (the SnapshotWorker rule).
    fn op_prune_and_compact(&mut self) {
        let refs = match self.repo.list(SHARD) {
            Ok(refs) => refs,
            Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
        };
        if refs.len() > 2 && self.rng.chance(50) {
            // Pruning keeps the newest 2 checkpoints; skip it if that would
            // leave no valid checkpoint covering the compaction floor (the
            // recoverability invariant, see `compact_floor`).
            let retained_valid_max = refs[refs.len() - 2..]
                .iter()
                .map(|r| r.last_tx_id)
                .filter(|tx| !self.corrupted_ckpts.contains(tx))
                .max();
            let safe = self.compact_floor == 0
                || retained_valid_max.is_some_and(|tx| tx >= self.compact_floor);
            if safe {
                if let Err(e) = self.repo.prune(SHARD, 2) {
                    panic!("[seed {}] prune: {e}", self.seed);
                }
                let remaining: Vec<u64> = match self.repo.list(SHARD) {
                    Ok(refs) => refs.iter().map(|r| r.last_tx_id).collect(),
                    Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
                };
                self.corrupted_ckpts.retain(|tx| remaining.contains(tx));
            }
        }
        // Compact only through checkpoints that actually verify (production
        // drives compaction from checkpoints it wrote and fsynced; the sim
        // must not truncate the log against one it deliberately broke).
        let oldest_valid = match self.repo.list(SHARD) {
            Ok(refs) => refs
                .iter()
                .map(|r| r.last_tx_id)
                .filter(|tx| !self.corrupted_ckpts.contains(tx))
                .min(),
            Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
        };
        if let Some(covered) = oldest_valid {
            if let Err(e) = self.log().compact(covered) {
                panic!("[seed {}] compact({covered}): {e}", self.seed);
            }
            self.compact_floor = self.compact_floor.max(covered);
            self.trace(40, covered);
        }
    }

    /// Bump the fencing epoch (SPEC-014 lineage under simulation).
    fn op_epoch_bump(&mut self) {
        self.epoch += 1;
        let epoch = self.epoch;
        if let Err(e) = self.rt.block_on(self.log().set_epoch(epoch)) {
            panic!("[seed {}] set_epoch({epoch}): {e}", self.seed);
        }
        self.trace(50, epoch);
    }

    /// The newest checkpoint expected to verify (corrupted ones excluded).
    fn expected_adopted_checkpoint(&self) -> Option<u64> {
        let refs = match self.repo.list(SHARD) {
            Ok(refs) => refs,
            Err(e) => panic!("[seed {}] repo list: {e}", self.seed),
        };
        refs.iter()
            .map(|r| r.last_tx_id)
            .filter(|tx| !self.corrupted_ckpts.contains(tx))
            .max()
    }

    /// Every entry currently on disk: `(segment path, start, end, tx_id)`,
    /// decoded via the frozen STG-011 framing and cross-checked against a
    /// read-only replay.
    fn disk_entries(&self) -> Vec<(PathBuf, usize, usize, u64)> {
        let mut txs = Vec::new();
        let report = match replay(&self.log_dir, SHARD, |_, record| {
            txs.push(record.tx_id);
            Ok(())
        }) {
            Ok(report) => report,
            Err(e) => panic!("[seed {}] replay scan: {e}", self.seed),
        };
        assert!(
            report.corruption.is_none(),
            "[seed {}] pre-crash log must be clean: {:?}",
            self.seed,
            report.corruption
        );
        let mut segments: Vec<PathBuf> = match fs::read_dir(&self.log_dir) {
            Ok(dir) => dir
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
                .collect(),
            Err(e) => panic!("[seed {}] read log dir: {e}", self.seed),
        };
        segments.sort();
        let mut entries = Vec::new();
        for seg in segments {
            let bytes = match fs::read(&seg) {
                Ok(b) => b,
                Err(e) => panic!("[seed {}] read segment: {e}", self.seed),
            };
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
                entries.push((seg.clone(), offset, end));
                offset = end;
            }
        }
        assert_eq!(
            entries.len(),
            txs.len(),
            "[seed {}] frame scan and replay disagree on the entry count",
            self.seed
        );
        entries
            .into_iter()
            .zip(txs)
            .map(|((seg, start, end), tx)| (seg, start, end, tx))
            .collect()
    }

    /// Simulated `kill -9` + restart (TST-133): flush everything, construct
    /// a seed-chosen physically-possible crash image, run the real recovery,
    /// and require row-set equality with the model at the surviving prefix.
    fn op_crash(&mut self) {
        let log = match self.log.take() {
            Some(log) => log,
            None => panic!("[seed {}] crash without a log", self.seed),
        };
        if let Err(e) = log.close() {
            panic!("[seed {}] close: {e}", self.seed);
        }

        // Choose the surviving log prefix K (never below the ack watermark)
        // and the fault to apply at the cut.
        let entries = self.disk_entries();
        let victims: Vec<&(PathBuf, usize, usize, u64)> = entries
            .iter()
            .filter(|(_, _, _, tx)| *tx > self.acked)
            .collect();
        let mut kept = self.last_committed;
        if !victims.is_empty() && self.faults.chance(75) {
            let victim = victims[self.faults.index(victims.len())];
            let (seg, start, end, tx) = (&victim.0, victim.1, victim.2, victim.3);
            kept = tx - 1;
            // Segments after the victim's are wholly un-fsynced: gone. Walk
            // the directory, not `entries` — a previous crash may have left
            // a header-only (entry-less) tail segment that must go too.
            let listing = match fs::read_dir(&self.log_dir) {
                Ok(dir) => dir,
                Err(e) => panic!("[seed {}] read log dir: {e}", self.seed),
            };
            for other in listing.filter_map(|e| e.ok().map(|e| e.path())) {
                if other.extension().is_some_and(|ext| ext == "log")
                    && other > *seg
                    && let Err(e) = fs::remove_file(&other)
                {
                    panic!("[seed {}] drop segment: {e}", self.seed);
                }
            }
            let bytes = match fs::read(seg) {
                Ok(b) => b,
                Err(e) => panic!("[seed {}] read victim segment: {e}", self.seed),
            };
            match self.faults.below(3) {
                0 => {
                    // Lost fsync: the suffix vanishes at the entry boundary.
                    if let Err(e) = fs::write(seg, &bytes[..start]) {
                        panic!("[seed {}] cut segment: {e}", self.seed);
                    }
                }
                1 => {
                    // Torn write: the victim entry is cut mid-frame.
                    let cut = start + 1 + self.faults.index(end - start - 1);
                    if let Err(e) = fs::write(seg, &bytes[..cut]) {
                        panic!("[seed {}] tear segment: {e}", self.seed);
                    }
                }
                _ => {
                    // Bit flip inside the victim entry; the rest of the
                    // segment stays — recovery must quarantine from the
                    // first invalid entry, not just the file tail.
                    let mut flipped = bytes;
                    let pos = start + self.faults.index(end - start);
                    flipped[pos] ^= 0xFF;
                    if let Err(e) = fs::write(seg, flipped) {
                        panic!("[seed {}] flip segment: {e}", self.seed);
                    }
                }
            }
        }

        // Real recovery: open (quarantines the torn tail), then checkpoint +
        // replay into a fresh store.
        let reopened = match CommitLog::open(&self.log_dir, SHARD, self.epoch, Self::log_options())
        {
            Ok(log) => log,
            Err(e) => panic!("[seed {}] reopen after crash (kept {kept}): {e}", self.seed),
        };
        let fresh = fresh_store();
        let outcome = match recover(&fresh, &self.repo, &self.log_dir, SHARD) {
            Ok(outcome) => outcome,
            Err(e) => panic!("[seed {}] recover (kept {kept}): {e}", self.seed),
        };

        let expected_ckpt = self.expected_adopted_checkpoint();
        let expected_n = kept.max(expected_ckpt.unwrap_or(0));
        assert_eq!(
            outcome.checkpoint_tx_id, expected_ckpt,
            "[seed {}] adopted checkpoint (kept {kept})",
            self.seed
        );
        assert_eq!(
            outcome.last_tx_id,
            Some(expected_n).filter(|&n| n > 0),
            "[seed {}] recovered prefix (kept {kept}, ckpt {expected_ckpt:?})",
            self.seed
        );
        assert!(
            expected_n >= self.acked,
            "[seed {}] acknowledged tx {} lost (recovered {expected_n})",
            self.seed,
            self.acked
        );
        assert_eq!(
            outcome.next_tx_id,
            expected_n + 1,
            "[seed {}] STG-015 resume point",
            self.seed
        );

        // Crash-replay equivalence against the model (TST-133).
        self.model.retain_prefix(expected_n);
        self.store = fresh;
        self.log = Some(reopened);
        self.last_committed = expected_n;
        self.durable_floor = expected_n;
        self.report.crashes += 1;
        self.assert_states_equal("post-recovery");
        let state_hash = fnv1a(
            &self.engine_state().canonical_bytes(),
            0xcbf2_9ce4_8422_2325,
        );
        self.trace(60, state_hash);
        self.trace(61, expected_n);
    }

    fn run(mut self, ops: usize) -> SimReport {
        for _ in 0..ops {
            match self.rng.below(100) {
                0..=54 => self.op_transaction(),
                55..=69 => self.op_ack(),
                70..=79 => self.op_checkpoint(),
                80..=86 => self.op_prune_and_compact(),
                87..=89 => self.op_epoch_bump(),
                _ => self.op_crash(),
            }
        }
        // Always end on a crash/recovery cycle so every run exercises the
        // flagship property at least once.
        self.op_ack();
        self.op_crash();
        self.assert_states_equal("final");
        if let Some(log) = self.log.take()
            && let Err(e) = log.close()
        {
            panic!("[seed {}] final close: {e}", self.seed);
        }
        // Keep the tempdir alive until here.
        drop(self.root);
        self.report
    }
}

/// Run one simulation for `seed`. Panics (with the seed in the message) on
/// any divergence between the engine and the model oracle.
pub fn run_seed(seed: u64, ops: usize) -> SimReport {
    World::new(seed).run(ops)
}

/// Run `seed` twice and require identical determinism traces (TST-130: the
/// same-seed ⇒ identical-trace property is itself checked). Returns the
/// report of the first run.
pub fn run_seed_checked(seed: u64, ops: usize) -> SimReport {
    let first = run_seed(seed, ops);
    let second = run_seed(seed, ops);
    if first.trace != second.trace {
        let checkpoint = first
            .trace
            .iter()
            .zip(&second.trace)
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| first.trace.len().min(second.trace.len()));
        panic!(
            "non-determinism detected for seed {seed} at checkpoint {checkpoint} \
             (run 1: {} events, run 2: {} events)",
            first.trace.len(),
            second.trace.len()
        );
    }
    first
}
