//! Audit trail over the commit log (SPEC-025 OPS-020): "who changed this row,
//! and when", answered straight from the durable log — no separate audit
//! store.
//!
//! The commit log already records every committing transaction in order, and
//! since the audit foundation each record carries its `caller` and
//! `reducer_name` ([`TxRecord`]). An audit query filters that stream by table
//! (optionally one row key) and a `tx_id`/time window, returning the ordered
//! metadata of the calls that touched it.
//!
//! # The lightweight index (OPS-020)
//!
//! A full-log scan is avoided by pruning at segment granularity: each segment
//! file's name encodes its `first_tx_id` and [`list_segments`] returns them
//! sorted, so a segment covers `[first_tx_id, next.first_tx_id)`. A `tx_id`
//! window therefore selects a contiguous slice of segments and the rest are
//! never read. A time-only window cannot prune by `tx_id` (time and `tx_id`
//! are both monotone, but the filename indexes only the latter), so it scans
//! the retained segments and filters per record.
//!
//! # What is returned (OPS-021)
//!
//! Only metadata — `tx_id`, `timestamp`, `caller`, `reducer_name`, and
//! whether the row was inserted/deleted in that transaction. Row **column
//! values are never returned**, so a masked or field-encrypted column cannot
//! leak plaintext through an audit result by construction. Access control
//! (admin/server-peer only) is enforced by the caller (the admin transport).

use std::path::Path;

use crate::error::Result;
use crate::schema::TableSchema;
use crate::store::row::encode_pk_of_row;
use crate::store::{PkBytes, TableId};
use crate::types::Identity;

use super::record::TableMutation;
use super::segment::{ScanOutcome, list_segments, scan_segment};

/// A safety cap so an unbounded audit can never buffer the whole log.
pub const DEFAULT_AUDIT_LIMIT: usize = 1_000;

/// What to trace (OPS-020): a table, optionally one row key, within an
/// optional `tx_id` and/or time window.
#[derive(Debug, Clone)]
pub struct AuditQuery {
    /// The table whose history is traced (resolved from its name by the
    /// caller, via the stable `crc32(name)` id).
    pub table: TableId,
    /// A single encoded row key to trace, or `None` for the whole table.
    pub pk: Option<PkBytes>,
    /// Inclusive lower `tx_id` bound (`None` = from the log's start).
    pub tx_from: Option<u64>,
    /// Inclusive upper `tx_id` bound (`None` = to the log's end).
    pub tx_to: Option<u64>,
    /// Inclusive lower timestamp bound, micros since epoch (`None` = open).
    pub time_from: Option<i64>,
    /// Inclusive upper timestamp bound, micros (`None` = open).
    pub time_to: Option<i64>,
    /// Max entries to return (`0` → [`DEFAULT_AUDIT_LIMIT`]).
    pub limit: usize,
}

impl AuditQuery {
    /// Trace an entire table with no window.
    pub fn table(table: TableId) -> Self {
        Self {
            table,
            pk: None,
            tx_from: None,
            tx_to: None,
            time_from: None,
            time_to: None,
            limit: DEFAULT_AUDIT_LIMIT,
        }
    }

    fn effective_limit(&self) -> usize {
        if self.limit == 0 {
            DEFAULT_AUDIT_LIMIT
        } else {
            self.limit
        }
    }

    fn tx_in_window(&self, tx_id: u64) -> bool {
        self.tx_from.is_none_or(|lo| tx_id >= lo) && self.tx_to.is_none_or(|hi| tx_id <= hi)
    }

    fn time_in_window(&self, ts: i64) -> bool {
        self.time_from.is_none_or(|lo| ts >= lo) && self.time_to.is_none_or(|hi| ts <= hi)
    }
}

/// One audited transaction that touched the queried table/row (OPS-020).
/// Metadata only — never column values (OPS-021).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEntry {
    /// The committing transaction id (commit order).
    pub tx_id: u64,
    /// Commit timestamp, micros since the Unix epoch.
    pub timestamp: i64,
    /// The identity that committed it.
    pub caller: Identity,
    /// The reducer that produced it (empty for a system/anonymous commit).
    pub reducer_name: String,
    /// The queried row/table was inserted (or replaced) in this transaction.
    pub inserted: bool,
    /// The queried row/table was deleted in this transaction.
    pub deleted: bool,
}

/// Run an audit query against the shard's commit log in `dir`, returning the
/// matching entries in commit order (ascending `tx_id`). `schema` is the
/// queried table's schema, used only to derive an inserted row's primary key
/// for row-key matching.
pub fn audit(
    dir: &Path,
    shard_id: u32,
    schema: &TableSchema,
    query: &AuditQuery,
) -> Result<Vec<AuditEntry>> {
    let segments = list_segments(dir, shard_id)?;
    let limit = query.effective_limit();
    let mut out: Vec<AuditEntry> = Vec::new();

    // Cross-segment expectations threaded only across *scanned* segments;
    // skipping segments below the window keeps `prev_tx`/`min_epoch` no higher
    // than a scanned record's, so no spurious monotonicity/epoch fault fires.
    let mut prev_tx: Option<u64> = None;
    let mut min_epoch = 0u64;

    for (i, seg) in segments.iter().enumerate() {
        if out.len() >= limit {
            break;
        }
        // Segment covers [first_tx_id, next_first). Prune by the tx window.
        let next_first = segments.get(i + 1).map(|s| s.first_tx_id);
        if let Some(hi) = query.tx_to
            && seg.first_tx_id > hi
        {
            break; // this and every later segment start above the window
        }
        if let Some(lo) = query.tx_from
            && let Some(next_first) = next_first
            && next_first <= lo
        {
            continue; // the whole segment ends below the window
        }

        let outcome = scan_segment(
            &seg.path,
            shard_id,
            prev_tx,
            min_epoch,
            &mut |epoch, record| {
                prev_tx = Some(record.tx_id);
                min_epoch = min_epoch.max(epoch);
                if out.len() >= limit
                    || !query.tx_in_window(record.tx_id)
                    || !query.time_in_window(record.timestamp)
                {
                    return Ok(());
                }
                if let Some((inserted, deleted)) = match_table(&record.mutations, schema, query) {
                    out.push(AuditEntry {
                        tx_id: record.tx_id,
                        timestamp: record.timestamp,
                        caller: record.caller_identity(),
                        reducer_name: record.reducer_name.clone(),
                        inserted,
                        deleted,
                    });
                }
                Ok(())
            },
        )?;
        // A corrupt header/tail means nothing further in this file is
        // trustworthy; audit takes the readable prefix and moves on (the
        // recovery path is what repairs/quarantines — STG-031).
        if let ScanOutcome::Scanned(scan) = outcome {
            prev_tx = scan.last_tx.or(prev_tx);
            min_epoch = scan.max_epoch.max(min_epoch);
        }
    }

    Ok(out)
}

/// Whether `mutations` touched the queried table (and row, if given), and how.
/// Returns `None` when the transaction did not touch it.
fn match_table(
    mutations: &[TableMutation],
    schema: &TableSchema,
    query: &AuditQuery,
) -> Option<(bool, bool)> {
    let mutation = mutations.iter().find(|m| m.table() == query.table)?;
    let Some(pk) = &query.pk else {
        // Whole-table trace: any change to the table counts.
        let touched = !mutation.inserts.is_empty() || !mutation.deletes.is_empty();
        return touched.then_some((!mutation.inserts.is_empty(), !mutation.deletes.is_empty()));
    };

    // Row trace: an insert whose derived PK equals `pk`, or a delete of `pk`.
    let inserted = mutation
        .insert_rows()
        .ok()
        .into_iter()
        .flatten()
        .any(|row| {
            encode_pk_of_row(schema, row.values()).is_ok_and(|got| got.as_bytes() == pk.as_bytes())
        });
    let deleted = mutation.delete_pks().any(|got| got == pk.as_bytes());
    (inserted || deleted).then_some((inserted, deleted))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use serde_bytes::ByteBuf;

    use crate::commitlog::record::{LogValue, TableMutation, TxRecord};
    use crate::commitlog::{CommitLog, CommitLogOptions};
    use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
    use crate::store::{RowValue, TableId};
    use crate::types::Identity;

    use super::*;

    const SHARD: u32 = 7;
    // The synthetic table id both the record and the query use.
    const TABLE: u32 = 0xA11D;

    static COLS: &[ColumnSchema] = &[ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    }];
    static ITEM: TableSchema = TableSchema {
        name: "Item",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    fn pk_of(id: u64) -> PkBytes {
        encode_pk_of_row(&ITEM, &[RowValue::U64(id)]).unwrap()
    }

    /// A record where reducer `who` inserts row `id` in table `TABLE`.
    fn insert_rec(tx_id: u64, id: u64, who: &str) -> TxRecord {
        TxRecord {
            tx_id,
            timestamp: 1_000 + tx_id as i64,
            shard_id: SHARD,
            mutations: vec![TableMutation {
                table_id: TABLE,
                inserts: vec![vec![LogValue::U64(id)]],
                deletes: vec![],
            }],
            auto_inc: vec![],
            caller: Identity::from_token(who.as_bytes()).as_bytes().to_vec(),
            reducer_name: who.to_owned(),
        }
    }

    fn delete_rec(tx_id: u64, id: u64, who: &str) -> TxRecord {
        TxRecord {
            tx_id,
            timestamp: 1_000 + tx_id as i64,
            shard_id: SHARD,
            mutations: vec![TableMutation {
                table_id: TABLE,
                inserts: vec![],
                deletes: vec![ByteBuf::from(pk_of(id).as_bytes().to_vec())],
            }],
            auto_inc: vec![],
            caller: Identity::from_token(who.as_bytes()).as_bytes().to_vec(),
            reducer_name: who.to_owned(),
        }
    }

    async fn seeded_log(dir: &std::path::Path) -> CommitLog {
        // segment_max_bytes = 1 rotates every append, so the records below
        // span many segments — exercising the cross-segment scan and the
        // per-segment tx prune.
        let opts = CommitLogOptions {
            segment_max_bytes: 1,
            ..CommitLogOptions::default()
        };
        let log = CommitLog::open(dir, SHARD, 1, opts).unwrap();
        // Row 5 is touched by tx 1 (create), 3 (update), 5 (delete); other
        // txs touch other rows so they must be filtered out.
        log.append(insert_rec(1, 5, "create_item")).await.unwrap();
        log.append(insert_rec(2, 9, "create_item")).await.unwrap();
        log.append(insert_rec(3, 5, "rename_item")).await.unwrap();
        log.append(insert_rec(4, 9, "rename_item")).await.unwrap();
        log.append(delete_rec(5, 5, "delete_item")).await.unwrap();
        log.wait_durable(5).await.unwrap();
        log
    }

    #[tokio::test]
    async fn row_history_lists_exactly_the_touching_calls_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(dir.path()).await;

        let mut q = AuditQuery::table(TableId::from_raw(TABLE));
        q.pk = Some(pk_of(5));
        let entries = log.audit(&ITEM, &q).unwrap();

        let names: Vec<&str> = entries.iter().map(|e| e.reducer_name.as_str()).collect();
        assert_eq!(
            names,
            vec!["create_item", "rename_item", "delete_item"],
            "exactly the three calls that touched row 5, in commit order"
        );
        assert_eq!(
            entries.iter().map(|e| e.tx_id).collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert_eq!(entries[0].caller, Identity::from_token(b"create_item"));
        assert!(entries[0].inserted && !entries[0].deleted);
        assert!(entries[2].deleted && !entries[2].inserted);
    }

    #[tokio::test]
    async fn a_tx_window_prunes_the_history() {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(dir.path()).await;

        let mut q = AuditQuery::table(TableId::from_raw(TABLE));
        q.pk = Some(pk_of(5));
        q.tx_from = Some(3);
        let entries = log.audit(&ITEM, &q).unwrap();
        assert_eq!(
            entries.iter().map(|e| e.tx_id).collect::<Vec<_>>(),
            vec![3, 5],
            "tx_from=3 drops the tx-1 create"
        );
    }

    #[tokio::test]
    async fn a_whole_table_trace_covers_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(dir.path()).await;

        let entries = log
            .audit(&ITEM, &AuditQuery::table(TableId::from_raw(TABLE)))
            .unwrap();
        assert_eq!(entries.len(), 5, "every committing call on the table");
    }

    #[tokio::test]
    async fn an_unrelated_table_has_no_history() {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded_log(dir.path()).await;

        let other = AuditQuery::table(TableId::from_raw(0xBEEF));
        assert!(log.audit(&ITEM, &other).unwrap().is_empty());
    }
}
