//! Read-only log replay (STG-030 step 5, STG-031): fold every valid
//! [`TxRecord`] in order, stopping at the first corrupt entry and reporting
//! it — without modifying any file.

use std::path::{Path, PathBuf};

use crate::error::Result;

use super::record::TxRecord;
use super::segment::{ScanOutcome, list_segments, scan_segment};

/// Where replay stopped early (STG-031: everything before it was applied).
#[derive(Debug, Clone)]
pub struct Corruption {
    /// The segment containing the first invalid entry.
    pub segment: PathBuf,
    /// Byte offset of the first invalid entry frame within that segment.
    pub offset: u64,
    /// Human-readable cause (torn write, checksum mismatch, decode failure,
    /// tx-id regression, epoch regression).
    pub reason: String,
}

/// The result of a full replay pass.
#[derive(Debug, Clone)]
pub struct ReplayReport {
    /// Number of records visited.
    pub records: u64,
    /// Last successfully recovered `tx_id` (reported per STG-031).
    pub last_tx_id: Option<u64>,
    /// Set when replay stopped at an invalid entry.
    pub corruption: Option<Corruption>,
}

impl ReplayReport {
    /// The tx id the next committed transaction must receive (STG-015:
    /// `recovered_tx_id + 1`; 1 for an empty log).
    pub fn next_tx_id(&self) -> u64 {
        self.last_tx_id.map_or(1, |tx| tx.saturating_add(1))
    }
}

/// Replay the shard's log in `dir`, invoking `visit(envelope_epoch, record)`
/// for every valid entry in `tx_id` order.
///
/// Replay stops at the first corrupt entry and reports it in the returned
/// [`ReplayReport`]; entries before it are all visited (STG-031). A `visit`
/// error aborts replay and is returned as-is. Read-only: files are never
/// modified — quarantine happens on [`super::CommitLog::open`], not here.
pub fn replay<F>(dir: &Path, shard_id: u32, mut visit: F) -> Result<ReplayReport>
where
    F: FnMut(u64, TxRecord) -> Result<()>,
{
    let segments = list_segments(dir, shard_id)?;
    let mut report = ReplayReport {
        records: 0,
        last_tx_id: None,
        corruption: None,
    };
    let mut prev_tx: Option<u64> = None;
    let mut min_epoch = 0u64;
    for seg in &segments {
        let records = &mut report.records;
        let last_tx = &mut report.last_tx_id;
        let outcome = scan_segment(
            &seg.path,
            shard_id,
            prev_tx,
            min_epoch,
            &mut |epoch, record| {
                *records += 1;
                *last_tx = Some(record.tx_id);
                visit(epoch, record)
            },
        )?;
        match outcome {
            ScanOutcome::HeaderCorrupt(reason) => {
                report.corruption = Some(Corruption {
                    segment: seg.path.clone(),
                    offset: 0,
                    reason,
                });
                break;
            }
            ScanOutcome::Scanned(scan) => {
                prev_tx = scan.last_tx.or(prev_tx);
                min_epoch = scan.max_epoch.max(min_epoch);
                if let Some(fault) = scan.fault {
                    report.corruption = Some(Corruption {
                        segment: seg.path.clone(),
                        offset: fault.offset,
                        reason: fault.reason,
                    });
                    break;
                }
            }
        }
    }
    if let Some(corruption) = &report.corruption {
        tracing::warn!(
            segment = %corruption.segment.display(),
            offset = corruption.offset,
            reason = %corruption.reason,
            last_recovered_tx_id = ?report.last_tx_id,
            "commit-log replay stopped at the first corrupt entry (STG-031)"
        );
    }
    Ok(report)
}
