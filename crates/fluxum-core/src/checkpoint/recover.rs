//! Recovery orchestration (STG-030): latest valid checkpoint + commit-log
//! replay, folded into a fresh [`MemStore`] — with fallback to older
//! retained checkpoints on any verification failure (STG-021).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::commitlog::record::TxRecord;
use crate::commitlog::replay::{ReplayReport, replay};
use crate::error::{FluxumError, Result};
use crate::schema::TableSchema;
use crate::store::committed::{CommittedState, TableState};
use crate::store::row::{Row, encode_pk_of_row};
use crate::store::{MemStore, TableId};

/// A checkpoint that failed verification during recovery and was skipped
/// (STG-021 fallback).
#[derive(Debug, Clone)]
pub struct RejectedCheckpoint {
    /// The rejected manifest file.
    pub path: PathBuf,
    /// Why verification failed (manifest integrity, object hash, decode…).
    pub reason: String,
}

/// What recovery did (STG-030).
#[derive(Debug)]
pub struct RecoveryOutcome {
    /// `last_tx_id` of the adopted checkpoint; `None` when recovery started
    /// from an empty state (no valid checkpoint found).
    pub checkpoint_tx_id: Option<u64>,
    /// Checkpoints rejected before one verified (newest first).
    pub rejected: Vec<RejectedCheckpoint>,
    /// The log replay pass, including any corruption stop (STG-031).
    pub replay: ReplayReport,
    /// Log records actually applied (records `<=` the checkpoint's
    /// `last_tx_id` are visited but skipped).
    pub applied_records: u64,
    /// Highest recovered transaction id (checkpoint or replay).
    pub last_tx_id: Option<u64>,
    /// The id the next committed transaction receives (STG-015).
    pub next_tx_id: u64,
}

/// A table's recovered contents while checkpoint + replay fold into it.
struct WorkingTable {
    schema: &'static TableSchema,
    /// Keyed by raw encoded-PK bytes (byte-identical to
    /// [`crate::store::PkBytes`]), so replayed deletes — which carry only pk
    /// bytes — apply without re-encoding.
    rows: BTreeMap<Vec<u8>, Row>,
    auto_inc_high_water: u64,
}

/// Recover a shard into `store` (STG-030): adopt the newest checkpoint that
/// fully verifies (falling back to older retained checkpoints on manifest or
/// object corruption, STG-021), then replay every log record with
/// `tx_id > checkpoint.last_tx_id`, rebuild secondary indexes, and install
/// the result. The store must be freshly constructed (no committed
/// transactions); after recovery it is ready to accept new reducer calls
/// with `tx_id = last_tx_id + 1`.
///
/// Replay application is convergence-tolerant: inserts are upserts and
/// deletes of absent keys are no-ops. This makes a checkpoint stamped with a
/// *lower bound* of its snapshot's actual transaction (the
/// [`super::SnapshotWorker`] stamps the last commit it was notified of
/// before taking the snapshot) recover to exactly the full-log-replay state:
/// re-applied entries are re-ordered writes whose last writer per key
/// already matches the snapshot.
///
/// Replay corruption does not fail recovery: entries before the corrupt one
/// are kept and applied, and the stop is reported in the outcome (STG-031 —
/// quarantine itself happens on [`crate::commitlog::CommitLog::open`]).
pub fn recover(
    store: &MemStore,
    repo: &super::CheckpointRepo,
    log_dir: &Path,
    shard_id: u32,
) -> Result<RecoveryOutcome> {
    // Fresh-store table map: TableId -> schema (the assembled catalog view).
    let base = store.snapshot();
    let mut working: HashMap<TableId, WorkingTable> = base
        .state
        .tables
        .iter()
        .map(|(&id, table)| {
            (
                id,
                WorkingTable {
                    schema: table.schema,
                    rows: BTreeMap::new(),
                    auto_inc_high_water: 0,
                },
            )
        })
        .collect();

    // STG-030 steps 1-3: newest checkpoint that fully verifies wins;
    // permanent mismatch falls back to an older retained one (STG-021).
    let mut rejected = Vec::new();
    let mut adopted: Option<u64> = None;
    for checkpoint in repo.list(shard_id)?.iter().rev() {
        match repo.load(checkpoint) {
            Ok(loaded) => {
                apply_checkpoint(&mut working, &loaded)?;
                adopted = Some(loaded.last_tx_id);
                break;
            }
            Err(e) => {
                tracing::warn!(
                    manifest = %checkpoint.path.display(),
                    error = %e,
                    "checkpoint failed verification; falling back to an older retained \
                     checkpoint (STG-021)"
                );
                rejected.push(RejectedCheckpoint {
                    path: checkpoint.path.clone(),
                    reason: e.to_string(),
                });
            }
        }
    }

    // STG-030 steps 4-5: replay log records past the checkpoint.
    let covered = adopted.unwrap_or(0);
    let mut applied = 0u64;
    let report = replay(log_dir, shard_id, |_, record| {
        if record.tx_id <= covered {
            return Ok(());
        }
        apply_record(&mut working, &record)?;
        applied += 1;
        Ok(())
    })?;

    // Assemble the final CommittedState: PkBytes keys and secondary/spatial
    // indexes rebuilt from the recovered rows (bit-identical to a fresh
    // rebuild by construction, STG-007 rule 2). The empty index structures
    // are cloned from the fresh store's own tables, so recovery always uses
    // exactly the store's index configuration (B-tree columns, spatial
    // bounds and bucket size).
    let mut tables = HashMap::with_capacity(working.len());
    for (id, table) in working {
        let empty = base.state.table(id)?;
        let mut rows: imbl::OrdMap<_, Row> = imbl::OrdMap::new();
        let mut indexes = empty.indexes.clone();
        let mut spatial = empty.spatial.clone();
        let mut fulltext = empty.fulltext.clone();
        let mut unique = empty.unique.clone();
        for row in table.rows.into_values() {
            let pk = encode_pk_of_row(table.schema, row.values())?;
            for index in indexes.values_mut() {
                index.insert(&row, pk.clone())?;
            }
            if let Some(spatial) = &mut spatial {
                spatial.insert_row(&row, pk.clone())?;
            }
            for fulltext in &mut fulltext {
                fulltext.insert_row(&row, pk.clone())?;
            }
            for constraint in &mut unique {
                constraint.insert(&row, pk.clone())?;
            }
            rows.insert(pk, row);
        }
        tables.insert(
            id,
            Arc::new(TableState {
                schema: table.schema,
                rows,
                indexes,
                spatial,
                fulltext,
                unique,
                auto_inc_high_water: table.auto_inc_high_water,
            }),
        );
    }

    let last_tx_id = report.last_tx_id.max(adopted);
    let next_tx_id = last_tx_id.map_or(1, |tx| tx.saturating_add(1));
    store.install_recovered(CommittedState { tables }, next_tx_id)?;

    Ok(RecoveryOutcome {
        checkpoint_tx_id: adopted,
        rejected,
        replay: report,
        applied_records: applied,
        last_tx_id,
        next_tx_id,
    })
}

/// Fold a verified checkpoint into the working state (STG-030 step 2).
fn apply_checkpoint(
    working: &mut HashMap<TableId, WorkingTable>,
    loaded: &super::LoadedCheckpoint,
) -> Result<()> {
    for table in &loaded.tables {
        let id = TableId::of(&table.table_name);
        if id.as_u32() != table.table_id {
            return Err(FluxumError::Storage(format!(
                "checkpoint table `{}`: recorded id {:#010x} disagrees with crc32(name) = {id} \
                 (STG-050)",
                table.table_name, table.table_id
            )));
        }
        let slot = working.get_mut(&id).ok_or_else(|| {
            FluxumError::Storage(format!(
                "checkpoint table `{}` ({id}) is not in the assembled schema",
                table.table_name
            ))
        })?;
        slot.auto_inc_high_water = table.auto_inc_high_water;
        for row in &table.rows {
            let pk = encode_pk_of_row(slot.schema, row.values())?;
            slot.rows.insert(pk.as_bytes().to_vec(), row.clone());
        }
    }
    Ok(())
}

/// Apply one replayed log record (STG-030 step 5). Deletes apply before
/// inserts so an in-place replacement (same pk in both lists) lands the new
/// row.
fn apply_record(working: &mut HashMap<TableId, WorkingTable>, record: &TxRecord) -> Result<()> {
    for mutation in &record.mutations {
        let id = mutation.table();
        let slot = working.get_mut(&id).ok_or_else(|| {
            FluxumError::Storage(format!(
                "commit-log record tx {} references table {id} which is not in the \
                 assembled schema",
                record.tx_id
            ))
        })?;
        for pk in mutation.delete_pks() {
            slot.rows.remove(pk);
        }
        for row in mutation.insert_rows()? {
            let pk = encode_pk_of_row(slot.schema, row.values())?;
            slot.rows.insert(pk.as_bytes().to_vec(), row);
        }
    }
    for &(table_id, high_water) in &record.auto_inc {
        let id = TableId::from_raw(table_id);
        if let Some(slot) = working.get_mut(&id) {
            slot.auto_inc_high_water = slot.auto_inc_high_water.max(high_water);
        }
    }
    Ok(())
}
