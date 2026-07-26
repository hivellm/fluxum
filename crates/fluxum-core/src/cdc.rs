//! CDC stream-sink dispatch (SPEC-020 §6, PLG-050): feed a [`StreamSink`]
//! committed deltas **off the commit path**, reusing the replication stream
//! substrate (SPEC-014) rather than inventing a second one.
//!
//! # Why the durable log is the queue
//!
//! The primary already streams committed entries to replicas straight from
//! its durable segments ([`crate::commitlog::read_frames_after`], gated on
//! [`crate::commitlog::CommitLog::durable_tx_id`]). A CDC sink is the same
//! shape: a follower of the durable log that never touches the write path.
//! Making the durable log the queue — instead of an in-memory fan-out buffer
//! the sink drains — is what makes at-least-once delivery and restart resume
//! *structural*:
//!
//! - **At-least-once + resume (PLG-050):** the sink's progress is a single
//!   persisted [`Offset`] ([`SinkCheckpoint`]). A restarted sink resumes by
//!   re-reading the log after its checkpoint; a crash between delivery and
//!   the checkpoint write re-delivers one batch (at-least-once, never
//!   at-most-once).
//! - **Never back-pressures commits (SUB-041):** the pump only ever *reads*
//!   the durable log. A stalled sink cannot slow, block, or lose a commit —
//!   the commit is already durable before the pump ever sees it.
//! - **Bounded buffer + drop policy (PLG-050):** each delivery reads a
//!   bounded batch ([`CdcOptions::batch_budget_bytes`] — the "buffer"). A
//!   sink that keeps failing never advances its checkpoint, so its lag
//!   (`durable_tip − checkpoint`) grows; past [`CdcOptions::lag_drop_threshold`]
//!   it is **dropped** (disabled + metered, `fluxum_plugin_sink_lag`) so it
//!   cannot spin or grow memory. Its checkpoint persists, so re-enabling it
//!   resumes with no gap.
//!
//! The delta view a sink sees ([`record_to_diff`]) is the committed
//! [`TxRecord`]: inserts carry full rows, deletes carry the primary key only
//! (the log stores no pre-image — a CDC consumer keys deletes by pk).
//!
//! The async wake loop that drives [`CdcSink::drain_to`] on each durable
//! advance lives in the server (`fluxum-server::cdc`), so this module stays a
//! pure, synchronously testable state machine.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::commitlog::{self, TxRecord};
use crate::error::Result;
use crate::metrics::Metrics;
use crate::plugin::{CommitBatch, Offset, PluginState, StreamSink};
use crate::store::row::{PkBytes, Row};
use crate::store::{TableDiff, TableId, TxDiff};

/// Default bounded read-ahead handed to one `on_commit` — the PLG-050
/// "buffer". Mirrors the replication streamer's per-batch budget so a single
/// delivery can never balloon memory regardless of transaction size.
pub const DEFAULT_BATCH_BUDGET_BYTES: usize = 1 << 20; // 1 MiB

/// Default lag — `durable_tip − checkpoint`, in transactions — past which a
/// stalled or failing sink is dropped (disabled) rather than retried forever
/// (PLG-050 "drop past threshold"). Its checkpoint persists, so a later
/// re-enable resumes with no gap.
pub const DEFAULT_LAG_DROP_THRESHOLD: u64 = 100_000;

/// Tuning knobs for a [`CdcSink`] (PLG-050).
#[derive(Debug, Clone)]
pub struct CdcOptions {
    /// Bounded bytes of log frames read into one `on_commit` batch.
    pub batch_budget_bytes: usize,
    /// Lag in transactions past which the sink is dropped (disabled).
    pub lag_drop_threshold: u64,
}

impl Default for CdcOptions {
    fn default() -> Self {
        Self {
            batch_budget_bytes: DEFAULT_BATCH_BUDGET_BYTES,
            lag_drop_threshold: DEFAULT_LAG_DROP_THRESHOLD,
        }
    }
}

/// A sink's persisted resume point (PLG-050): the last `tx_id` the host has
/// durably handed to `on_commit`, stored one file per sink under a `cdc`
/// directory. Written in place with `fsync` (the same durability the REP-004
/// epoch marker uses); a torn or missing file resumes from `0`, which
/// re-delivers — safe under at-least-once, never lossy.
pub struct SinkCheckpoint {
    path: PathBuf,
}

impl SinkCheckpoint {
    /// The checkpoint file for `name` under `dir` (`<dir>/<name>.offset`).
    /// The name is sanitized to a filesystem-safe token so a manifest plugin
    /// name can never escape the directory.
    pub fn new(dir: &Path, name: &str) -> Self {
        Self {
            path: dir.join(format!("{}.offset", sanitize(name))),
        }
    }

    /// The persisted offset, or `Offset(0)` when there is none yet (or the
    /// marker is unreadable — resuming from `0` re-delivers, which
    /// at-least-once tolerates).
    pub fn load(&self) -> Offset {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return Offset(0);
        };
        match text.trim().parse::<u64>() {
            Ok(tx) => Offset(tx),
            Err(_) => {
                tracing::warn!(
                    target: "fluxum::cdc",
                    path = %self.path.display(),
                    "unreadable CDC checkpoint; resuming from 0 (at-least-once re-delivery)"
                );
                Offset(0)
            }
        }
    }

    /// Durably persist `offset` (in-place write + `fsync`, like the REP-004
    /// epoch marker).
    ///
    /// # Errors
    /// I/O failures creating the directory, writing, or syncing.
    pub fn store(&self, offset: Offset) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        std::io::Write::write_all(&mut file, offset.0.to_string().as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

/// Reduce a filesystem-unsafe plugin name to `[A-Za-z0-9._-]`, so a
/// checkpoint file name can never contain a path separator.
fn sanitize(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "sink".to_owned()
    } else {
        cleaned
    }
}

/// Convert a committed log [`TxRecord`] into the CDC delta view a
/// [`StreamSink`] receives (PLG-050). Only mutations to tables in `tables`
/// survive (an empty `tables` passes every table). Inserts carry full rows;
/// **deletes carry the primary key only** with an empty old row, because the
/// commit log stores no pre-image — a CDC consumer keys deletes by pk.
///
/// # Errors
/// A row value in the record that cannot be decoded back to a store value.
pub fn record_to_diff(record: &TxRecord, tables: &[u32]) -> Result<TxDiff> {
    let mut out = Vec::new();
    for mutation in &record.mutations {
        if !tables.is_empty() && !tables.contains(&mutation.table_id) {
            continue;
        }
        out.push(TableDiff {
            table_id: mutation.table(),
            inserts: mutation.insert_rows()?,
            deletes: mutation
                .delete_pks()
                .map(|pk| (PkBytes::from_bytes(pk.to_vec()), Row::new(Vec::new())))
                .collect(),
        });
    }
    let auto_inc = if tables.is_empty() {
        record
            .auto_inc
            .iter()
            .map(|(t, hw)| (TableId::from_raw(*t), *hw))
            .collect()
    } else {
        // Scoped sinks care only about their tables' counters.
        record
            .auto_inc
            .iter()
            .filter(|(t, _)| tables.contains(t))
            .map(|(t, hw)| (TableId::from_raw(*t), *hw))
            .collect()
    };
    Ok(TxDiff {
        tx_id: record.tx_id,
        tables: out,
        auto_inc,
    })
}

/// The result of one [`CdcSink::drain_to`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainOutcome {
    /// Transactions the checkpoint advanced past this pass (delivered or
    /// skipped as out-of-scope).
    pub advanced: u64,
    /// The checkpoint offset after this pass.
    pub offset: u64,
    /// The sink was dropped (disabled) this pass — its lag crossed the
    /// threshold or it panicked.
    pub dropped: bool,
}

/// One CDC sink's pump (PLG-050): reads committed records from the durable
/// log after its checkpoint and hands them to a [`StreamSink`] under panic
/// isolation, advancing a persisted [`Offset`] as it goes.
///
/// Synchronous by construction: [`drain_to`](Self::drain_to) does all its
/// work between calls, so the whole at-least-once / drop-policy state machine
/// is unit-testable without a runtime. The server calls it from a task woken
/// by the durable-log watch.
pub struct CdcSink {
    name: String,
    sink: Arc<dyn StreamSink>,
    state: Arc<PluginState>,
    checkpoint: SinkCheckpoint,
    log_dir: PathBuf,
    shard_id: u32,
    tables: Vec<u32>,
    metrics: Arc<Metrics>,
    opts: CdcOptions,
    offset: u64,
}

impl CdcSink {
    /// Build a pump. The resume point is the max of the host's persisted
    /// checkpoint and the sink's own reported [`StreamSink::checkpoint`], so a
    /// sink that durably tracks its downstream progress can skip redundant
    /// re-delivery while a stateless one still resumes safely from the host
    /// checkpoint. `tables` are the stable ids (`TableId::of(name).as_u32()`)
    /// the binding applies to — empty for an unscoped sink.
    ///
    /// # Errors
    /// A failure persisting the resolved resume offset.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        sink: Arc<dyn StreamSink>,
        state: Arc<PluginState>,
        checkpoint_dir: &Path,
        log_dir: PathBuf,
        shard_id: u32,
        tables: Vec<u32>,
        metrics: Arc<Metrics>,
        opts: CdcOptions,
    ) -> Result<Self> {
        let name = name.into();
        let checkpoint = SinkCheckpoint::new(checkpoint_dir, &name);
        let host = checkpoint.load().0;
        let sink_reported = sink.checkpoint().0;
        let offset = host.max(sink_reported);
        if offset != host {
            checkpoint.store(Offset(offset))?;
        }
        metrics.set_sink_lag(&name, 0);
        Ok(Self {
            name,
            sink,
            state,
            checkpoint,
            log_dir,
            shard_id,
            tables,
            metrics,
            opts,
            offset,
        })
    }

    /// The sink's manifest name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The current checkpoint offset (last `tx_id` durably handed on).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Deliver every committed delta after the checkpoint up to `durable_tip`,
    /// at-least-once and in commit order, persisting the checkpoint per batch.
    ///
    /// Stops early — without error — when the sink returns an error (the same
    /// batch retries on the next wake), when its lag crosses the drop
    /// threshold (the sink is disabled), or when the sink is already disabled
    /// (an admin hot-disable, PLG-061). A `StreamSink` panic is contained by
    /// [`PluginState::guard`] and disables the sink, never the shard.
    ///
    /// # Errors
    /// A corrupt log segment or a checkpoint-write failure — an operational
    /// fault the caller surfaces, distinct from a sink declining a batch.
    pub fn drain_to(&mut self, durable_tip: u64) -> Result<DrainOutcome> {
        let start = self.offset;
        loop {
            if self.state.is_disabled() {
                return Ok(self.outcome(start, true));
            }
            if self.offset >= durable_tip {
                self.metrics.set_sink_lag(&self.name, 0);
                return Ok(self.outcome(start, false));
            }
            self.metrics
                .set_sink_lag(&self.name, durable_tip - self.offset);

            let (frames, read_last) = commitlog::read_frames_after(
                &self.log_dir,
                self.shard_id,
                self.offset,
                self.opts.batch_budget_bytes,
            )?;
            if frames.is_empty() {
                // The next contiguous entry is not readable. Either the tip
                // is not yet flushed to a segment (retry on the next wake) or
                // the log was truncated past our checkpoint (a sink so far
                // behind its records aged out) — skip the unrecoverable gap
                // rather than spin.
                match commitlog::first_available_tx_id(&self.log_dir, self.shard_id)? {
                    Some(first) if first > self.offset + 1 => {
                        tracing::warn!(
                            target: "fluxum::cdc",
                            sink = %self.name, from = self.offset, first_available = first,
                            "CDC sink checkpoint fell below retained log; skipping the truncated gap"
                        );
                        self.metrics.note_sink_dropped(&self.name);
                        self.offset = (first - 1).min(durable_tip);
                        self.checkpoint.store(Offset(self.offset))?;
                        continue;
                    }
                    _ => return Ok(self.outcome(start, false)),
                }
            }

            // `read_frames_after` streams every contiguous on-disk frame up to
            // its byte budget, which can run ahead of `durable_tip`; a sink
            // must never see past the caller's durable watermark, so clamp the
            // batch there (`read_last >= offset+1 > offset`, so we advance).
            let batch_last = read_last.min(durable_tip);
            let mut diffs = Vec::with_capacity(frames.len());
            for frame in &frames {
                let (_epoch, record) = commitlog::decode_entry_frame(frame)?;
                if record.tx_id > batch_last {
                    break; // frames are contiguous; the rest are past the tip too
                }
                let diff = record_to_diff(&record, &self.tables)?;
                if !diff.is_empty() {
                    diffs.push(diff);
                }
            }

            if !diffs.is_empty() {
                let batch = CommitBatch { diffs };
                let sink = &self.sink;
                if let Err(err) = self.state.guard(&self.name, || sink.on_commit(&batch)) {
                    self.metrics.note_sink_error(&self.name);
                    // Do not advance: the batch retries on the next wake
                    // (at-least-once). A panic already disabled the sink via
                    // `guard`. A sink that keeps failing accumulates lag; once
                    // it is more than the threshold behind it is dropped
                    // (disabled + metered), so a broken sink cannot retry
                    // forever or grow memory — its checkpoint persists for a
                    // later resume (PLG-050 drop policy).
                    let lag = durable_tip.saturating_sub(self.offset);
                    let dropped = self.state.is_disabled() || lag > self.opts.lag_drop_threshold;
                    if dropped && !self.state.is_disabled() {
                        self.state.set_disabled(true);
                        self.metrics.note_sink_dropped(&self.name);
                    }
                    tracing::warn!(
                        target: "fluxum::cdc",
                        sink = %self.name, error = %err, lag, dropped,
                        "CDC sink on_commit failed (PLG-050); {}",
                        if dropped { "dropped past the lag threshold" } else { "retrying the batch on the next commit" }
                    );
                    return Ok(self.outcome(start, dropped));
                }
            }

            let advanced = batch_last - self.offset;
            self.offset = batch_last;
            self.checkpoint.store(Offset(self.offset))?;
            self.metrics.note_sink_delivered(&self.name, advanced);
            self.metrics
                .set_sink_lag(&self.name, durable_tip.saturating_sub(self.offset));
        }
    }

    fn outcome(&self, start: u64, dropped: bool) -> DrainOutcome {
        DrainOutcome {
            advanced: self.offset - start,
            offset: self.offset,
            dropped,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::commitlog::TableMutation;
    use crate::store::row::RowValue;
    use std::sync::Mutex;

    fn record(
        tx_id: u64,
        table: &str,
        inserts: Vec<Vec<RowValue>>,
        deletes: Vec<Vec<u8>>,
    ) -> TxRecord {
        use serde_bytes::ByteBuf;
        TxRecord {
            tx_id,
            timestamp: 0,
            shard_id: 0,
            mutations: vec![TableMutation {
                table_id: TableId::of(table).as_u32(),
                inserts: inserts
                    .iter()
                    .map(|row| row.iter().map(Into::into).collect())
                    .collect(),
                deletes: deletes.into_iter().map(ByteBuf::from).collect(),
            }],
            auto_inc: Vec::new(),
            caller: Vec::new(),
            reducer_name: String::new(),
        }
    }

    #[test]
    fn checkpoint_round_trips_and_defaults_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = SinkCheckpoint::new(dir.path(), "vectorizer_ingest");
        assert_eq!(ckpt.load(), Offset(0));
        ckpt.store(Offset(42)).unwrap();
        assert_eq!(ckpt.load(), Offset(42));
        ckpt.store(Offset(43)).unwrap();
        assert_eq!(ckpt.load(), Offset(43));
        // A garbage marker resumes from 0 (re-delivery, never a dead sink).
        std::fs::write(dir.path().join("vectorizer_ingest.offset"), b"not-a-number").unwrap();
        assert_eq!(ckpt.load(), Offset(0));
    }

    #[test]
    fn checkpoint_name_is_sanitized_to_stay_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        // A name with separators cannot escape the directory.
        let ckpt = SinkCheckpoint::new(dir.path(), "../../etc/passwd");
        ckpt.store(Offset(7)).unwrap();
        assert_eq!(ckpt.load(), Offset(7));
        assert_eq!(ckpt.path.parent().unwrap(), dir.path());
    }

    #[test]
    fn record_to_diff_carries_inserts_full_and_deletes_by_pk() {
        let rec = record(
            5,
            "Item",
            vec![vec![RowValue::U64(1), RowValue::Str("a".into())]],
            vec![vec![9, 9, 9]],
        );
        let diff = record_to_diff(&rec, &[]).unwrap();
        assert_eq!(diff.tx_id, 5);
        assert_eq!(diff.tables.len(), 1);
        let t = &diff.tables[0];
        assert_eq!(t.table_id, TableId::of("Item"));
        assert_eq!(t.inserts.len(), 1);
        assert_eq!(t.inserts[0].values()[1], RowValue::Str("a".into()));
        // The delete carries the pk bytes and an empty pre-image row.
        assert_eq!(t.deletes.len(), 1);
        assert_eq!(t.deletes[0].0.as_bytes(), &[9, 9, 9]);
        assert!(t.deletes[0].1.values().is_empty());
    }

    #[test]
    fn record_to_diff_filters_out_of_scope_tables() {
        let rec = record(1, "Secret", vec![vec![RowValue::U64(1)]], vec![]);
        // Scoped to a different table → nothing survives → empty diff.
        let scoped = record_to_diff(&rec, &[TableId::of("Item").as_u32()]).unwrap();
        assert!(scoped.is_empty());
        // The matching scope keeps it.
        let kept = record_to_diff(&rec, &[TableId::of("Secret").as_u32()]).unwrap();
        assert_eq!(kept.tables.len(), 1);
    }

    /// A recording sink whose behavior a test can steer.
    struct RecordingSink {
        seen: Mutex<Vec<u64>>,
        fail: std::sync::atomic::AtomicBool,
        panic: std::sync::atomic::AtomicBool,
        reported: Offset,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                fail: std::sync::atomic::AtomicBool::new(false),
                panic: std::sync::atomic::AtomicBool::new(false),
                reported: Offset(0),
            })
        }
        fn tx_ids(&self) -> Vec<u64> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl StreamSink for RecordingSink {
        fn on_commit(
            &self,
            batch: &CommitBatch,
        ) -> std::result::Result<(), crate::plugin::PluginError> {
            if self.panic.load(std::sync::atomic::Ordering::Relaxed) {
                panic!("sink boom");
            }
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(crate::plugin::PluginError("sink down".into()));
            }
            self.seen
                .lock()
                .unwrap()
                .extend(batch.diffs.iter().map(|d| d.tx_id));
            Ok(())
        }
        fn checkpoint(&self) -> Offset {
            self.reported
        }
    }

    /// Append records to a real commit log and return its dir + durable tip.
    async fn seed_log(records: &[TxRecord]) -> (tempfile::TempDir, u64) {
        use crate::commitlog::{CommitLog, CommitLogOptions};
        let dir = tempfile::tempdir().unwrap();
        let log = CommitLog::open(dir.path(), 0, 1, CommitLogOptions::default()).unwrap();
        let mut last = 0;
        for rec in records {
            last = log.append(rec.clone()).await.unwrap();
        }
        log.wait_durable(last).await.unwrap();
        (dir, last)
    }

    fn build(
        sink: Arc<RecordingSink>,
        state: Arc<PluginState>,
        log_dir: PathBuf,
        ckpt_dir: &Path,
        tables: Vec<u32>,
        opts: CdcOptions,
    ) -> (CdcSink, Arc<Metrics>) {
        let metrics = Metrics::new(0);
        let cdc = CdcSink::new(
            "cdc_sink",
            sink,
            state,
            ckpt_dir,
            log_dir,
            0,
            tables,
            Arc::clone(&metrics),
            opts,
        )
        .unwrap();
        (cdc, metrics)
    }

    #[tokio::test]
    async fn drains_every_committed_delta_in_order() {
        let recs: Vec<_> = (1..=3)
            .map(|i| record(i, "Item", vec![vec![RowValue::U64(i)]], vec![]))
            .collect();
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();
        let sink = RecordingSink::new();
        let (mut cdc, metrics) = build(
            Arc::clone(&sink),
            Arc::new(PluginState::default()),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![],
            CdcOptions::default(),
        );
        let outcome = cdc.drain_to(tip).unwrap();
        assert_eq!(sink.tx_ids(), vec![1, 2, 3]);
        assert_eq!(outcome.offset, 3);
        assert!(!outcome.dropped);
        assert_eq!(metrics.sink_lag("cdc_sink"), 0);
    }

    #[tokio::test]
    async fn restart_resumes_from_the_checkpoint_with_no_gap() {
        let recs: Vec<_> = (1..=4)
            .map(|i| record(i, "Item", vec![vec![RowValue::U64(i)]], vec![]))
            .collect();
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();

        // First run delivers 1..=2 only (tip clamped), then "crashes".
        {
            let sink = RecordingSink::new();
            let (mut cdc, _) = build(
                Arc::clone(&sink),
                Arc::new(PluginState::default()),
                log_dir.path().to_path_buf(),
                ckpt.path(),
                vec![],
                CdcOptions::default(),
            );
            cdc.drain_to(2).unwrap();
            assert_eq!(sink.tx_ids(), vec![1, 2]);
        }
        // A fresh sink instance resumes from the persisted checkpoint (3..=4),
        // never re-delivering 1..=2 nor skipping any.
        let sink = RecordingSink::new();
        let (mut cdc, _) = build(
            Arc::clone(&sink),
            Arc::new(PluginState::default()),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![],
            CdcOptions::default(),
        );
        assert_eq!(cdc.offset(), 2, "resumed from the checkpoint");
        cdc.drain_to(tip).unwrap();
        assert_eq!(sink.tx_ids(), vec![3, 4]);
    }

    #[tokio::test]
    async fn a_failing_sink_does_not_advance_and_retries() {
        let recs: Vec<_> = (1..=2)
            .map(|i| record(i, "Item", vec![vec![RowValue::U64(i)]], vec![]))
            .collect();
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();
        let sink = RecordingSink::new();
        sink.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        let (mut cdc, metrics) = build(
            Arc::clone(&sink),
            Arc::new(PluginState::default()),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![],
            CdcOptions::default(),
        );
        cdc.drain_to(tip).unwrap();
        assert_eq!(cdc.offset(), 0, "a failed batch never advances");
        assert!(metrics.sink_errors("cdc_sink") >= 1);
        // Recovering: the same records now deliver, no gap.
        sink.fail.store(false, std::sync::atomic::Ordering::Relaxed);
        cdc.drain_to(tip).unwrap();
        assert_eq!(sink.tx_ids(), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_stalled_sink_is_dropped_past_the_lag_threshold() {
        let recs: Vec<_> = (1..=5)
            .map(|i| record(i, "Item", vec![vec![RowValue::U64(i)]], vec![]))
            .collect();
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();
        let sink = RecordingSink::new();
        sink.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        let state = Arc::new(PluginState::default());
        let (mut cdc, metrics) = build(
            Arc::clone(&sink),
            Arc::clone(&state),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![],
            CdcOptions {
                lag_drop_threshold: 3,
                ..CdcOptions::default()
            },
        );
        let outcome = cdc.drain_to(tip).unwrap();
        assert!(outcome.dropped, "lag {} > threshold 3 drops the sink", tip);
        assert!(state.is_disabled());
        assert_eq!(metrics.sink_dropped("cdc_sink"), 1);
        // The checkpoint persisted, so a re-enable resumes from 0 with no gap.
        state.set_disabled(false);
        sink.fail.store(false, std::sync::atomic::Ordering::Relaxed);
        cdc.drain_to(tip).unwrap();
        assert_eq!(sink.tx_ids(), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn a_panicking_sink_is_contained_and_disabled() {
        let recs = vec![record(1, "Item", vec![vec![RowValue::U64(1)]], vec![])];
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();
        let sink = RecordingSink::new();
        sink.panic.store(true, std::sync::atomic::Ordering::Relaxed);
        let state = Arc::new(PluginState::default());
        let (mut cdc, _) = build(
            Arc::clone(&sink),
            Arc::clone(&state),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![],
            CdcOptions::default(),
        );
        let outcome = cdc.drain_to(tip).unwrap();
        assert!(outcome.dropped);
        assert!(
            state.is_disabled(),
            "a panic disables the sink, not the shard"
        );
        assert_eq!(state.panics(), 1);
        assert_eq!(cdc.offset(), 0, "a panicked batch never advances");
    }

    #[tokio::test]
    async fn an_out_of_scope_only_run_still_advances_the_checkpoint() {
        // Every record touches a table the sink is not scoped to: nothing is
        // delivered, but the checkpoint must still catch up (never re-scan).
        let recs: Vec<_> = (1..=3)
            .map(|i| record(i, "Other", vec![vec![RowValue::U64(i)]], vec![]))
            .collect();
        let (log_dir, tip) = seed_log(&recs).await;
        let ckpt = tempfile::tempdir().unwrap();
        let sink = RecordingSink::new();
        let (mut cdc, _) = build(
            Arc::clone(&sink),
            Arc::new(PluginState::default()),
            log_dir.path().to_path_buf(),
            ckpt.path(),
            vec![TableId::of("Item").as_u32()],
            CdcOptions::default(),
        );
        cdc.drain_to(tip).unwrap();
        assert!(sink.tx_ids().is_empty(), "no in-scope deltas delivered");
        assert_eq!(
            cdc.offset(),
            tip,
            "but the checkpoint still advanced past them"
        );
    }
}
