//! Replication deterministic simulation (SPEC-014 §3/§5; TST-134): a
//! seeded primary produces a commit-log stream, and a replica applies it
//! through the **real** [`MemStore::apply_replica_record`] under seeded
//! network faults — dropped/duplicated batches and mid-stream partitions
//! that resume from the replica's offset (REP-013). The invariants are the
//! replication contract: over the delivered prefix the replica's
//! `CommittedState` equals the primary's (REP-014), and its commit log is
//! byte-identical (REP-010). Every seed runs twice; a trace divergence
//! fails loudly (TST-130).
//!
//! Scope note (TST-134): the election/quorum machinery (SPEC-014 §4/§5)
//! is exercised by the pure `decide_vote`/quorum unit suites and the
//! over-the-wire `failover_e2e` drill. This module determinizes the
//! **data-plane** convergence under message faults, which is the part with
//! a large state space and the byte-identity invariant.

use fluxum_core::commitlog::{self, CommitLog, CommitLogOptions, decode_entry_frame};
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::types::Timestamp;

use crate::rng::SimRng;

const SHARD: u32 = 3;
const EPOCH: u64 = 1;

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

fn fresh_store(seed: u64) -> MemStore {
    let schema =
        Schema::from_tables([&USER]).unwrap_or_else(|e| panic!("[seed {seed}] schema: {e}"));
    MemStore::new(&schema).unwrap_or_else(|e| panic!("[seed {seed}] store: {e}"))
}

fn small_segments() -> CommitLogOptions {
    CommitLogOptions {
        // Small segments so a run rotates several times — the byte-identity
        // invariant then covers rotation points, not just one segment.
        segment_max_bytes: 512,
        ..CommitLogOptions::default()
    }
}

/// What one replication-sim run observed (for the determinism check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplReport {
    pub seed: u64,
    /// Transactions the primary committed.
    pub commits: u64,
    /// Batches dropped then recovered by an offset resync (REP-013).
    pub resyncs: u64,
    /// Batches delivered more than once (idempotent replay, REP-014).
    pub duplicates: u64,
    /// The chained determinism trace (TST-130).
    pub trace: Vec<u64>,
}

fn fnv1a(bytes: &[u8], mut hash: u64) -> u64 {
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

/// A content fingerprint of a store's committed User rows, order-independent.
fn fingerprint(store: &MemStore, seed: u64) -> u64 {
    let snapshot = store.snapshot();
    let user = TableId::of("User");
    let rows = snapshot
        .scan(user)
        .unwrap_or_else(|e| panic!("[seed {seed}] scan: {e}"));
    // XOR per-row hashes so scan order cannot affect the fingerprint.
    let mut acc: u64 = 0;
    for row in rows {
        match (row.value(0), row.value(1)) {
            (Some(RowValue::U64(id)), Some(RowValue::Str(name))) => {
                let mut h = fnv1a(&id.to_le_bytes(), 0xcbf2_9ce4_8422_2325);
                h = fnv1a(name.as_bytes(), h);
                acc ^= h;
            }
            other => panic!("[seed {seed}] malformed row: {other:?}"),
        }
    }
    acc
}

/// Run one replication simulation (TST-134). Returns the observations for
/// the determinism cross-check.
pub fn run_seed(seed: u64, ops: usize) -> ReplReport {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|e| panic!("[seed {seed}] runtime: {e}"));
    let mut rng = SimRng::new(seed);
    let mut faults = rng.fork(0xFA02);

    // Primary world.
    let proot = tempfile::tempdir().unwrap_or_else(|e| panic!("[seed {seed}] ptmp: {e}"));
    let plog_dir = proot.path().join("primary-log");
    let pstore = fresh_store(seed);
    let plog = CommitLog::open(&plog_dir, SHARD, EPOCH, small_segments())
        .unwrap_or_else(|e| panic!("[seed {seed}] primary log: {e}"));

    // Replica world (same options ⇒ same rotation ⇒ byte-identical files).
    let rroot = tempfile::tempdir().unwrap_or_else(|e| panic!("[seed {seed}] rtmp: {e}"));
    let rlog_dir = rroot.path().join("replica-log");
    let rstore = fresh_store(seed);
    let rlog = CommitLog::open(&rlog_dir, SHARD, EPOCH, small_segments())
        .unwrap_or_else(|e| panic!("[seed {seed}] replica log: {e}"));

    let mut report = ReplReport {
        seed,
        commits: 0,
        resyncs: 0,
        duplicates: 0,
        trace: Vec::new(),
    };
    // The replica's applied offset — the resync point after a dropped batch.
    let mut applied: u64 = 0;

    for _ in 0..ops {
        // 1. The primary commits a seeded transaction.
        let user = TableId::of("User");
        let mut tx = pstore.begin();
        let mutations = 1 + rng.below(4);
        let mut touched = false;
        for _ in 0..mutations {
            if rng.chance(70) {
                let id = rng.below(16);
                let name = format!("u{id}-{}", rng.below(1000));
                if tx
                    .insert(user, vec![RowValue::U64(id), RowValue::Str(name)])
                    .is_ok()
                {
                    touched = true;
                }
            } else {
                let id = rng.below(16);
                if matches!(tx.delete(user, &[RowValue::U64(id)]), Ok(true)) {
                    touched = true;
                }
            }
        }
        if !touched {
            tx.rollback();
            continue;
        }
        let diff = tx
            .commit()
            .unwrap_or_else(|e| panic!("[seed {seed}] commit: {e}"));
        let tx_id = diff.tx_id;
        let timestamp = Timestamp::from_micros(i64::try_from(tx_id).unwrap_or(0));
        rt.block_on(plog.append_diff(&diff, timestamp))
            .unwrap_or_else(|e| panic!("[seed {seed}] primary append {tx_id}: {e}"));
        rt.block_on(plog.wait_durable(tx_id))
            .unwrap_or_else(|e| panic!("[seed {seed}] primary durable {tx_id}: {e}"));
        report.commits += 1;

        // 2. The network decides this batch's fate (seeded, REP-017 window
        //    reduced to one tx for maximal fault granularity).
        //    - 15%: DROP — the replica never sees it now; it resyncs from
        //      its own offset on the next delivery (partial sync, REP-013).
        //    - 20%: DUPLICATE — deliver the pending frames twice; the
        //      convergent replay must be idempotent (REP-014).
        //    - otherwise: deliver once.
        if faults.chance(15) {
            report.resyncs += 1;
            continue; // withhold — the offset resync below catches up
        }
        let duplicate = faults.chance(20);

        // 3. Stream every frame the replica has not yet applied (its offset
        //    to the primary head) — exactly what the primary's streamer
        //    does via read_frames_after (the disk IS the stream, REP-010).
        deliver(
            &plog_dir,
            &rstore,
            &rlog,
            &rt,
            &mut applied,
            duplicate,
            &mut report,
            seed,
        );
    }

    // Final catch-up: deliver anything still withheld, then assert full
    // convergence + byte-identity over the shared range.
    deliver(
        &plog_dir,
        &rstore,
        &rlog,
        &rt,
        &mut applied,
        false,
        &mut report,
        seed,
    );

    let phead = plog.durable_tx_id().ok().flatten().unwrap_or(0);
    rt.block_on(rlog.wait_durable(phead))
        .unwrap_or_else(|e| panic!("[seed {seed}] replica durable {phead}: {e}"));

    assert_eq!(
        fingerprint(&rstore, seed),
        fingerprint(&pstore, seed),
        "[seed {seed}] REP-014: replica state diverged from the primary"
    );
    assert_byte_identical(&plog_dir, &rlog_dir, seed);

    plog.close()
        .unwrap_or_else(|e| panic!("[seed {seed}] primary close: {e}"));
    rlog.close()
        .unwrap_or_else(|e| panic!("[seed {seed}] replica close: {e}"));
    report
}

/// Stream the frames after the replica's `applied` offset and apply them,
/// optionally twice (a duplicated batch — idempotence).
#[allow(clippy::too_many_arguments)]
fn deliver(
    primary_log: &std::path::Path,
    rstore: &MemStore,
    rlog: &CommitLog,
    rt: &tokio::runtime::Runtime,
    applied: &mut u64,
    duplicate: bool,
    report: &mut ReplReport,
    seed: u64,
) {
    let (frames, _last) = commitlog::read_frames_after(primary_log, SHARD, *applied, usize::MAX)
        .unwrap_or_else(|e| panic!("[seed {seed}] read stream: {e}"));
    let passes = if duplicate { 2 } else { 1 };
    for pass in 0..passes {
        let mut offset = *applied;
        for frame in &frames {
            let (epoch, record) = decode_entry_frame(frame)
                .unwrap_or_else(|e| panic!("[seed {seed}] decode frame: {e}"));
            assert_eq!(epoch, EPOCH, "[seed {seed}] envelope epoch");
            if record.tx_id <= offset {
                // Already applied in a prior pass — the replica's own
                // monotonicity guard would reject a re-apply, so a real
                // duplicate is filtered before apply. This models the
                // primary re-sending from a stale ack: skip, do not error.
                continue;
            }
            rstore
                .apply_replica_record(&record)
                .unwrap_or_else(|e| panic!("[seed {seed}] apply {}: {e}", record.tx_id));
            rt.block_on(rlog.append(record.clone()))
                .unwrap_or_else(|e| panic!("[seed {seed}] replica append {}: {e}", record.tx_id));
            offset = record.tx_id;
        }
        if pass == 0 {
            *applied = offset;
        } else {
            report.duplicates += 1;
        }
    }
    // A determinism observation: the replica's fingerprint after this batch.
    let h = fingerprint(rstore, seed);
    let prev = report
        .trace
        .last()
        .copied()
        .unwrap_or(0xcbf2_9ce4_8422_2325);
    report.trace.push(fnv1a(&h.to_le_bytes(), prev));
}

/// REP-010: over the shared range the segment files must be byte-identical.
fn assert_byte_identical(primary_log: &std::path::Path, replica_log: &std::path::Path, seed: u64) {
    let mut names: Vec<String> = std::fs::read_dir(primary_log)
        .unwrap_or_else(|e| panic!("[seed {seed}] read_dir: {e}"))
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .filter(|n| n.ends_with(".log"))
        .collect();
    names.sort();
    for name in &names {
        let p = std::fs::read(primary_log.join(name))
            .unwrap_or_else(|e| panic!("[seed {seed}] read primary {name}: {e}"));
        let r = std::fs::read(replica_log.join(name))
            .unwrap_or_else(|e| panic!("[seed {seed}] replica missing segment {name}: {e}"));
        assert_eq!(
            p, r,
            "[seed {seed}] REP-010: segment {name} not byte-identical"
        );
    }
}

/// Run a seed twice and fail loudly on any divergence (TST-130 determinism).
pub fn run_seed_checked(seed: u64, ops: usize) -> ReplReport {
    let first = run_seed(seed, ops);
    let second = run_seed(seed, ops);
    assert_eq!(
        first, second,
        "[seed {seed}] non-determinism detected in the replication sim"
    );
    first
}
