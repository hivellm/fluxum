//! fluxum-testkit — deterministic reducer testing for module authors
//! (SPEC-024 DEV-020/DEV-021, FR-136).
//!
//! A module author's test spins up a [`TestShard`]: the **real** engine —
//! [`MemStore`], [`CommitLog`], `TxPipeline` single writer, the reducer
//! registry their `#[fluxum::table]` / `#[fluxum::reducer]` declarations
//! populated at link time — in-process, against a temp directory, with a
//! **seeded clock and RNG** so every run of the same test is bit-identical:
//!
//! ```ignore
//! use fluxum_testkit::{FluxValue, TestShard};
//! use my_module as _; // link the module so its registrations survive
//!
//! let mut shard = TestShard::new(42)?; // the seed IS the whole run
//! let alice = shard.identity("alice");
//! let receipt = shard.call(alice, "add_task", vec![FluxValue::Str("write tests".into())])?;
//! assert_eq!(receipt.inserted("Task").len(), 1);   // the emitted diff
//! assert_eq!(shard.rows("Task").len(), 1);          // the resulting rows
//! ```
//!
//! What makes a run deterministic (DEV-020):
//!
//! - the clock is simulated: it starts at a fixed epoch and advances a fixed
//!   step per call (or by [`TestShard::advance`]); reducers see it as
//!   `ctx.timestamp` exactly as they would a production admission stamp;
//! - [`TestShard::rng`] is a seeded [`SimRng`] fork for the test's own
//!   input generation;
//! - every call is recorded — identity, timestamp, reducer, args, outcome —
//!   and [`TestShard::replay`] drives a fresh shard through the tape,
//!   failing loudly on the first divergence in commit/abort outcome.
//!   [`TestShard::fingerprint`] then asserts whole-state equality.
//!
//! Fault injection (DEV-021) reuses the DST harness's crash model:
//! [`TestShard::crash`] freezes the shard as a `kill -9` would, the
//! [`CrashedShard`] applies a physically-possible disk fault — the un-fsynced
//! tail vanishing at an entry boundary (mid-commit crash) or cut mid-frame
//! (torn tail) — and [`CrashedShard::recover`] runs the real checkpoint +
//! replay recovery, so authors can test recovery-affecting logic.
//!
//! # Fidelity notes
//!
//! Admission runs exactly as in production, including per-reducer
//! `max_rate` buckets. Buckets start full at their declared capacity, so a
//! burst of at most `rate` calls per `(identity, reducer)` never trips and
//! stays deterministic; sustained over-rate calls depend on the wall clock —
//! split them across identities instead. The RED-052 shard-wide guard is
//! disabled (it exists to protect servers, not tests).

mod crash;

use std::sync::Arc;

use fluxum_core::checkpoint::{CheckpointRepo, recover};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    LifecycleHooks, RateLimiter, RateLimiterOptions, ReducerCaller, ReducerEngine,
    ReducerRegistry, StartupReport,
};
use fluxum_core::schema::Schema;
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::{FluxumError, Result};

pub use crash::CrashedShard;
pub use fluxum_core::reducer::FluxValue;
pub use fluxum_core::store::RowValue as Value;
pub use fluxum_dst::SimRng;

/// The fixed shard id every test shard runs as.
const SHARD: u32 = 42;

/// The simulated clock's fixed origin (µs since the Unix epoch):
/// 2020-09-13T12:26:40Z. Fixed, not seed-derived, so timestamps in golden
/// assertions read the same across tests.
const CLOCK_EPOCH_US: i64 = 1_600_000_000_000_000;

/// How far the simulated clock advances before each call.
const CLOCK_STEP_US: i64 = 1_000;

/// One recorded reducer call — the unit of the deterministic replay tape
/// (DEV-020).
#[derive(Debug, Clone)]
pub struct RecordedCall {
    /// The caller identity.
    pub identity: Identity,
    /// The simulated admission timestamp the call ran under.
    pub timestamp_us: i64,
    /// The reducer name.
    pub reducer: String,
    /// The positional arguments.
    pub args: Vec<FluxValue>,
    /// Whether the call committed (`Ok`) or rolled back (`Err`).
    pub committed: bool,
}

/// One touched table's decoded diff: `(name, inserted rows, deleted rows)`,
/// each row in column declaration order.
type TableChanges = (String, Vec<Vec<RowValue>>, Vec<Vec<RowValue>>);

/// One committed call's outcome: its transaction id plus the emitted diff,
/// decoded to column values per table — the assertion surface for "what did
/// this reducer actually change" (DEV-020).
#[derive(Debug, Clone)]
pub struct CallReceipt {
    /// The committed transaction's id (strictly increasing per shard).
    pub tx_id: u64,
    /// Per touched table. Deleted rows carry their full pre-delete values.
    tables: Vec<TableChanges>,
}

impl CallReceipt {
    /// The rows this call inserted into `table` (empty if untouched).
    pub fn inserted(&self, table: &str) -> Vec<Vec<RowValue>> {
        self.tables
            .iter()
            .find(|(name, _, _)| name == table)
            .map(|(_, inserts, _)| inserts.clone())
            .unwrap_or_default()
    }

    /// The rows this call deleted from `table`, with pre-delete values.
    pub fn deleted(&self, table: &str) -> Vec<Vec<RowValue>> {
        self.tables
            .iter()
            .find(|(name, _, _)| name == table)
            .map(|(_, _, deletes)| deletes.clone())
            .unwrap_or_default()
    }

    /// Names of the tables this call touched, in diff order.
    pub fn touched(&self) -> Vec<&str> {
        self.tables.iter().map(|(name, _, _)| name.as_str()).collect()
    }
}

/// An in-process shard fixture driving the real reducer/transaction engine
/// deterministically (DEV-020). See the crate docs for the model.
pub struct TestShard {
    rt: tokio::runtime::Runtime,
    engine: ReducerEngine,
    store: Arc<MemStore>,
    log: Arc<CommitLog>,
    /// Owns the on-disk state; carried through crash/recover cycles.
    root: tempfile::TempDir,
    clock_us: i64,
    rng: SimRng,
    recording: Vec<RecordedCall>,
    last_tx_id: u64,
    /// What the RED-010/013 startup lifecycle ran, for authors asserting on
    /// `on_init` / `on_shard_start` behavior.
    pub startup: StartupReport,
}

impl TestShard {
    /// Boot a fresh shard from the link-time registry — every
    /// `#[fluxum::table]` / `#[fluxum::reducer]` in the test binary's
    /// dependency graph, exactly what a served binary would assemble. The
    /// seed drives the clock fork and [`TestShard::rng`]; the same seed and
    /// call sequence always produce the same state.
    pub fn new(seed: u64) -> Result<Self> {
        let root = tempfile::tempdir().map_err(FluxumError::from)?;
        Self::boot(seed, root, Vec::new(), None)
    }

    /// (Re)assemble the engine over `root`. Shared by [`TestShard::new`] and
    /// [`CrashedShard::recover`]; `carry` restores the recording/clock/rng of
    /// a pre-crash life.
    pub(crate) fn boot(
        seed: u64,
        root: tempfile::TempDir,
        recording: Vec<RecordedCall>,
        carry: Option<(i64, SimRng)>,
    ) -> Result<Self> {
        let schema = Schema::assemble()?;
        let store = Arc::new(MemStore::new(&schema)?);
        let log_dir = root.path().join("log");
        let ckpt_dir = root.path().join("checkpoints");
        std::fs::create_dir_all(&log_dir).map_err(FluxumError::from)?;
        let repo = CheckpointRepo::open(&ckpt_dir)?;
        // Recovery BEFORE the log opens (STG-030): a fresh directory replays
        // nothing; a post-crash one replays the surviving prefix.
        let recovery = recover(&store, &repo, &log_dir, SHARD)?;
        let fresh = recovery.last_tx_id.is_none();
        let last_tx_id = recovery.last_tx_id.unwrap_or(0);

        let log = Arc::new(CommitLog::open(
            &log_dir,
            SHARD,
            1,
            CommitLogOptions::default(),
        )?);
        let (pipeline, worker) = TxPipeline::new(Arc::clone(&store), Arc::clone(&log), {
            TxPipelineOptions::default()
        })?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(FluxumError::from)?;
        rt.spawn(worker.run());

        let engine = ReducerEngine::new(
            pipeline,
            Arc::new(ReducerRegistry::from_registered()?),
            LifecycleHooks::from_registered(),
            SHARD,
            fluxum_core::auth::server_identity("fluxum-testkit"),
        )
        // The RED-052 shard-wide guard protects servers, not tests; the
        // per-reducer max_rate buckets stay, exactly as in production.
        .with_rate_limiter(RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 0,
            },
            [],
        ));

        // RED-010/013: `on_init` on first boot, `on_shard_start` every boot —
        // module fidelity includes the lifecycle.
        let startup = rt.block_on(engine.start(fresh))?;

        let (clock_us, rng) = carry.unwrap_or((CLOCK_EPOCH_US, SimRng::new(seed).fork(1)));
        Ok(Self {
            rt,
            engine,
            store,
            log,
            root,
            clock_us,
            rng,
            recording,
            last_tx_id,
            startup,
        })
    }

    /// The seeded RNG for the test's own input generation. Deterministic:
    /// forked from the shard seed, independent of the clock.
    pub fn rng(&mut self) -> &mut SimRng {
        &mut self.rng
    }

    /// The simulated clock's current value.
    pub fn now(&self) -> Timestamp {
        Timestamp::from_micros(self.clock_us)
    }

    /// Advance the simulated clock by `micros` (e.g. to cross a scheduling
    /// or expiry boundary a reducer reads from `ctx.timestamp`).
    pub fn advance(&mut self, micros: i64) {
        self.clock_us = self.clock_us.saturating_add(micros);
    }

    /// A stable identity for `token` — the same derivation the dev `none`
    /// auth provider uses, so `identity("alice")` here IS the identity an
    /// e2e client authenticating with token `"alice"` gets.
    pub fn identity(&self, token: &str) -> Identity {
        Identity::from_token(token)
    }

    /// Drive one reducer call as `identity` (DEV-020): the clock ticks one
    /// step, the call executes on the real single-writer pipeline, and the
    /// outcome is recorded on the replay tape. `Ok` carries the committed
    /// diff; `Err` is the reducer's own rejection (the transaction rolled
    /// back — nothing changed).
    pub fn call(
        &mut self,
        identity: Identity,
        reducer: &str,
        args: Vec<FluxValue>,
    ) -> Result<CallReceipt> {
        self.clock_us += CLOCK_STEP_US;
        let result = self.call_at(identity, self.clock_us, reducer, args.clone());
        self.recording.push(RecordedCall {
            identity,
            timestamp_us: self.clock_us,
            reducer: reducer.to_owned(),
            args,
            committed: result.is_ok(),
        });
        result
    }

    /// One call at an explicit simulated timestamp (the replay path).
    fn call_at(
        &mut self,
        identity: Identity,
        timestamp_us: i64,
        reducer: &str,
        args: Vec<FluxValue>,
    ) -> Result<CallReceipt> {
        let caller = ReducerCaller {
            identity,
            // Stable per identity: the leading bytes of the identity itself,
            // so connect/disconnect-style logic keyed on connections stays
            // deterministic.
            connection_id: ConnectionId::new(u128::from_le_bytes(
                identity.as_bytes()[..16].try_into().unwrap_or([0u8; 16]),
            )),
            timestamp: Timestamp::from_micros(timestamp_us),
            shard_id: SHARD,
        };
        let receipt = self
            .rt
            .block_on(self.engine.call(caller, reducer, args))?;
        self.last_tx_id = self.last_tx_id.max(receipt.tx_id);

        let mut tables = Vec::new();
        for diff in &receipt.diff.tables {
            let Some(schema) = self.store.table_schema(diff.table_id) else {
                continue;
            };
            let inserts: Vec<Vec<RowValue>> =
                diff.inserts.iter().map(|row| row.values().to_vec()).collect();
            let deletes: Vec<Vec<RowValue>> = diff
                .deletes
                .iter()
                .map(|(_, row)| row.values().to_vec())
                .collect();
            tables.push((schema.name.to_owned(), inserts, deletes));
        }
        Ok(CallReceipt {
            tx_id: receipt.tx_id,
            tables,
        })
    }

    /// The committed rows of `table`, in column declaration order.
    ///
    /// # Panics
    /// On an unknown table name — in a test kit a typo must fail the test,
    /// not satisfy an `is_empty` assertion.
    #[track_caller]
    pub fn rows(&self, table: &str) -> Vec<Vec<RowValue>> {
        let Some(id) = self.store.table_id(table) else {
            panic!("fluxum-testkit: no table named `{table}` in the assembled schema");
        };
        match self.store.snapshot().scan(id) {
            Ok(rows) => rows.map(|row| row.values().to_vec()).collect(),
            Err(e) => panic!("fluxum-testkit: scan `{table}`: {e}"),
        }
    }

    /// A stable digest of the whole committed state: every table, every row.
    /// Two shards with equal fingerprints hold identical data — the
    /// replay-equality assertion (DEV-020).
    pub fn fingerprint(&self) -> u64 {
        let snapshot = self.store.snapshot();
        let mut names: Vec<&str> = self.store.table_schemas().map(|s| s.name).collect();
        names.sort_unstable();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut mix = |bytes: &[u8]| {
            for &b in bytes {
                hash ^= u64::from(b);
                hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
            }
        };
        for name in names {
            mix(name.as_bytes());
            let Some(id) = self.store.table_id(name) else {
                continue;
            };
            let Ok(rows) = snapshot.scan(id) else { continue };
            let mut printed: Vec<String> = rows.map(|row| format!("{:?}", row.values())).collect();
            printed.sort_unstable();
            for row in printed {
                mix(row.as_bytes());
            }
        }
        hash
    }

    /// The replay tape: every call made so far, in order (DEV-020).
    pub fn recording(&self) -> &[RecordedCall] {
        &self.recording
    }

    /// Drive a FRESH shard through a recorded tape (DEV-020). Each call runs
    /// under its recorded identity and timestamp; the first call whose
    /// commit/abort outcome diverges from the recording fails the replay
    /// with a `step N` error. On success the returned shard's
    /// [`TestShard::fingerprint`] can be asserted against the original's.
    pub fn replay(seed: u64, tape: &[RecordedCall]) -> Result<Self> {
        let root = tempfile::tempdir().map_err(FluxumError::from)?;
        let mut shard = Self::boot(seed, root, Vec::new(), None)?;
        for (step, call) in tape.iter().enumerate() {
            let outcome = shard.call_at(
                call.identity,
                call.timestamp_us,
                &call.reducer,
                call.args.clone(),
            );
            if outcome.is_ok() != call.committed {
                return Err(FluxumError::Reducer(format!(
                    "replay diverged at step {step} (`{}`): recorded {}, replayed {}",
                    call.reducer,
                    if call.committed { "commit" } else { "abort" },
                    if outcome.is_ok() { "commit" } else { "abort" },
                )));
            }
            shard.recording.push(call.clone());
            shard.clock_us = shard.clock_us.max(call.timestamp_us);
        }
        Ok(shard)
    }

    /// Freeze the shard as `kill -9` would (DEV-021): everything durably
    /// acknowledged is on disk; nothing is checkpointed or cleanly closed.
    /// The returned [`CrashedShard`] can corrupt the log tail the ways a
    /// real crash can, then [`CrashedShard::recover`] runs the real
    /// checkpoint+replay recovery.
    pub fn crash(self) -> CrashedShard {
        // Make the disk image deterministic: wait for the fsync watermark to
        // cover the last commit, then abandon everything mid-flight.
        if self.last_tx_id > 0 {
            let _ = self.rt.block_on(self.log.wait_durable(self.last_tx_id));
        }
        drop(self.engine); // the pipeline (and its writer) die un-shutdown
        drop(self.rt);
        drop(self.log);
        CrashedShard::new(self.root, self.recording, self.clock_us, self.rng)
    }
}
