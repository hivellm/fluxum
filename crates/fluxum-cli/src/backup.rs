//! `fluxum backup create|verify|restore` (SPEC-014 §8/§9, REP-060..REP-072;
//! FR-103/FR-104): hot backups with no writer stall, offline verification,
//! full restore, and point-in-time recovery from archived segments.
//!
//! The heavy lifting lives in [`fluxum_core::backup`]; this module resolves
//! directories (from `--config` or `--data-dir`, defaulting to the same
//! layout the server boots with), parses the PITR target, and renders
//! reports. `--fresh-checkpoint` asks a *running* server for a checkpoint
//! first via `POST /checkpoint` (the admin surface), so the backup's base is
//! as fresh as the moment it started.

use std::path::{Path, PathBuf};

use fluxum_core::backup::store::{S3Config, S3Store};
use fluxum_core::backup::{self, BackupSource, PitrTarget, RestoreDirs};

/// Where the data lives, resolved from `--config`, `--data-dir`, or the
/// server's defaults (`./data` layout).
#[derive(Debug, Clone)]
pub struct DataLayout {
    /// `storage.checkpoint_dir`.
    pub checkpoint_dir: PathBuf,
    /// `storage.commit_log_dir`.
    pub log_dir: PathBuf,
    /// `replication.archive.dir` (the PITR source).
    pub archive_dir: PathBuf,
}

impl DataLayout {
    /// Resolve the layout: an explicit `--config` loads the real config
    /// (env overrides included); `--data-dir` derives the default layout
    /// under it; neither means the built-in defaults (`./data`).
    ///
    /// # Errors
    /// A config file that does not load.
    pub fn resolve(config: Option<&Path>, data_dir: Option<&Path>) -> Result<Self, String> {
        if let Some(dir) = data_dir {
            return Ok(Self {
                checkpoint_dir: dir.join("checkpoints"),
                log_dir: dir.join("log"),
                archive_dir: dir.join("archive"),
            });
        }
        let config = fluxum_core::config::Config::load(config).map_err(|e| e.to_string())?;
        Ok(Self {
            checkpoint_dir: config.storage.checkpoint_dir.clone(),
            log_dir: config.storage.commit_log_dir.clone(),
            archive_dir: config.replication.archive.dir.clone(),
        })
    }
}

/// `fluxum backup create --out <dir>` (REP-060/REP-061).
///
/// # Errors
/// A failed fresh-checkpoint request, or the core create failing.
pub fn create(
    layout: &DataLayout,
    out: &Path,
    fresh_checkpoint: Option<&str>,
) -> Result<String, String> {
    if let Some(server) = fresh_checkpoint {
        // REP-060: trigger a fresh non-blocking checkpoint on the running
        // server so the backup's base covers the current head.
        let body = crate::post_path(server, "/checkpoint", "{}")
            .map_err(|e| format!("--fresh-checkpoint: {e}"))?;
        let fresh: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        let covered = fresh
            .get("payload")
            .and_then(|p| p.get("last_tx_id"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!("fresh checkpoint: covered through tx {covered}");
    }
    let report = backup::create(
        &BackupSource {
            checkpoint_dir: layout.checkpoint_dir.clone(),
            log_dir: layout.log_dir.clone(),
        },
        out,
    )
    .map_err(|e| e.to_string())?;
    Ok(format!(
        "backup {} written to {}\n  shards: {}  segments: {}  head tx: {}",
        report.backup_id,
        out.display(),
        report.shards,
        report.segments,
        report.head_tx_id
    ))
}

/// `fluxum backup verify --from <dir>` (REP-064). `Ok(summary)` when every
/// check passed; `Err(report)` with one line per failing file otherwise.
///
/// # Errors
/// Any failing check (the per-file report), or an unreadable backup.
pub fn verify(from: &Path) -> Result<String, String> {
    let report = backup::verify(from).map_err(|e| e.to_string())?;
    if report.ok() {
        return Ok(format!(
            "backup at {} verifies: {} file(s) checked, all good",
            from.display(),
            report.files.len()
        ));
    }
    let mut out = format!("backup at {} FAILS verification:\n", from.display());
    for check in report.errors() {
        out.push_str(&format!(
            "  {}: {}\n",
            check.file,
            check.error.as_deref().unwrap_or("failed")
        ));
    }
    Err(out)
}

/// `fluxum backup restore --from <dir>` (REP-063), optionally with a PITR
/// target (REP-070/071/072).
///
/// # Errors
/// Verification failures, a non-empty target without `--force`, a chain gap
/// before the PITR target, or I/O.
pub fn restore(
    from: &Path,
    layout: &DataLayout,
    target: Option<PitrTarget>,
    archive_dir: Option<&Path>,
    force: bool,
) -> Result<String, String> {
    let dirs = RestoreDirs {
        checkpoint_dir: layout.checkpoint_dir.clone(),
        log_dir: layout.log_dir.clone(),
    };
    match target {
        None => {
            let report = backup::restore(from, &dirs, force).map_err(|e| e.to_string())?;
            Ok(format!(
                "restored {} shard(s), {} segment(s); state reproduces tx {} on next boot",
                report.shards, report.segments, report.head_tx_id
            ))
        }
        Some(target) => {
            let archive = archive_dir.unwrap_or(&layout.archive_dir);
            let report = backup::pitr(from, &dirs, Some(archive), target, force)
                .map_err(|e| e.to_string())?;
            // REP-071: the boundary is reported — the last applied tx and
            // its timestamp.
            Ok(format!(
                "PITR complete: last applied tx {} at {} µs (fork epoch {}; the restored node \
                 must seed a new replica set, REP-072)",
                report.last_tx_id, report.last_timestamp, report.fork_min_epoch
            ))
        }
    }
}

/// The configured S3-compatible target (SPEC-025 OPS-010): store + prefix
/// from `replication.archive.remote`, requiring it to be enabled.
///
/// # Errors
/// A config that does not load, or remote archival not configured.
pub fn remote_target(config_path: Option<&Path>) -> Result<(S3Store, String), String> {
    let config = fluxum_core::config::Config::load(config_path).map_err(|e| e.to_string())?;
    let remote = &config.replication.archive.remote;
    if !remote.enabled {
        return Err(
            "replication.archive.remote is not enabled in the configuration — set \
             endpoint/bucket/credentials there (SPEC-025 OPS-010)"
                .to_owned(),
        );
    }
    let store = S3Store::new(S3Config {
        endpoint: remote.endpoint.clone(),
        bucket: remote.bucket.clone(),
        region: remote.effective_region().to_owned(),
        access_key: remote.access_key.clone(),
        secret_key: remote
            .secret_key
            .as_ref()
            .map(|s| s.expose_str().to_owned())
            .unwrap_or_default(),
    });
    Ok((store, remote.effective_prefix().to_owned()))
}

/// `fluxum backup create --remote` (OPS-010/011): hot-capture and push to
/// the configured object store, content-addressed and incremental.
///
/// # Errors
/// Configuration, capture, or transport failures.
pub fn create_remote(layout: &DataLayout, config_path: Option<&Path>) -> Result<String, String> {
    let (store, prefix) = remote_target(config_path)?;
    let report = backup::remote::push(
        &BackupSource {
            checkpoint_dir: layout.checkpoint_dir.clone(),
            log_dir: layout.log_dir.clone(),
        },
        &store,
        &prefix,
    )
    .map_err(|e| e.to_string())?;
    Ok(format!(
        "pushed backup {} to the object store\n  manifest: {}\n  uploaded: {} artifact(s) \
         ({} bytes)  already present: {}  head tx: {}",
        report.backup_id,
        report.manifest_key,
        report.uploaded,
        report.bytes_uploaded,
        report.skipped,
        report.head_tx_id
    ))
}

/// `fluxum backup restore --remote` (OPS-010/011): restore — or PITR, with
/// a target — from the newest remote backup; every download re-hashed.
///
/// # Errors
/// Configuration, integrity, chain-gap, or transport failures.
pub fn restore_remote(
    layout: &DataLayout,
    config_path: Option<&Path>,
    target: Option<PitrTarget>,
    force: bool,
) -> Result<String, String> {
    let (store, prefix) = remote_target(config_path)?;
    let dirs = RestoreDirs {
        checkpoint_dir: layout.checkpoint_dir.clone(),
        log_dir: layout.log_dir.clone(),
    };
    match target {
        None => {
            let report = backup::remote::restore(&store, &prefix, &dirs, force)
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "restored {} shard(s), {} segment(s) from the object store; state reproduces \
                 tx {} on next boot",
                report.shards, report.segments, report.head_tx_id
            ))
        }
        Some(target) => {
            let (report, stats) = backup::remote::pitr(&store, &prefix, &dirs, target, force)
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "PITR complete: last applied tx {} at {} µs (fork epoch {}); target segment \
                 fetched via range reads: {} of {} bytes",
                report.last_tx_id,
                report.last_timestamp,
                report.fork_min_epoch,
                stats.target_segment_bytes_fetched,
                stats.target_segment_stored_len
            ))
        }
    }
}

/// Parse `--to-timestamp`: microseconds since the Unix epoch, or an RFC 3339
/// UTC timestamp (`2026-07-24T13:37:09Z`, optional `.fraction`, optional
/// `±HH:MM` offset).
pub fn parse_timestamp(text: &str) -> Result<i64, String> {
    let text = text.trim();
    if let Ok(micros) = text.parse::<i64>() {
        return Ok(micros);
    }
    parse_rfc3339_micros(text)
        .ok_or_else(|| format!("`{text}` is neither µs-since-epoch nor an RFC 3339 timestamp"))
}

/// Minimal RFC 3339 → µs-since-epoch (UTC), no external dependency: the CLI
/// ships in the single static binary. Handles `Z` and `±HH:MM` offsets and
/// fractional seconds to microsecond precision.
fn parse_rfc3339_micros(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let year: i64 = text.get(0..4)?.parse().ok()?;
    let month: i64 = text.get(5..7)?.parse().ok()?;
    let day: i64 = text.get(8..10)?.parse().ok()?;
    let hour: i64 = text.get(11..13)?.parse().ok()?;
    let minute: i64 = text.get(14..16)?.parse().ok()?;
    let second: i64 = text.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut rest = text.get(19..)?;
    let mut micros = 0i64;
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits: String = fraction.chars().take_while(char::is_ascii_digit).collect();
        rest = &fraction[digits.len()..];
        let mut value: i64 = digits.parse().ok()?;
        // Scale to exactly 6 fractional digits.
        for _ in digits.len()..6 {
            value *= 10;
        }
        for _ in 6..digits.len() {
            value /= 10;
        }
        micros = value;
    }
    let offset_seconds = match rest {
        "Z" | "z" | "" => 0i64,
        _ => {
            let (sign, hhmm) = rest.split_at(1);
            let sign = match sign {
                "+" => 1i64,
                "-" => -1i64,
                _ => return None,
            };
            let (hh, mm) = hhmm.split_once(':')?;
            let hh: i64 = hh.parse().ok()?;
            let mm: i64 = mm.parse().ok()?;
            sign * (hh * 3_600 + mm * 60)
        }
    };
    // Howard Hinnant's days-from-civil: exact for all Gregorian dates.
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    Some(seconds * 1_000_000 + micros)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parses_to_micros() {
        // 1970-01-01T00:00:00Z = 0.
        assert_eq!(parse_timestamp("1970-01-01T00:00:00Z").unwrap(), 0);
        // A µs integer passes through.
        assert_eq!(parse_timestamp("1234567").unwrap(), 1_234_567);
        // A known instant: 2026-07-24T13:37:09Z (day 20658 since the epoch).
        let t = parse_timestamp("2026-07-24T13:37:09Z").unwrap();
        assert_eq!(t, 1_784_900_229_000_000);
        // Fractions scale to µs; offsets shift.
        assert_eq!(
            parse_timestamp("2026-07-24T13:37:09.5Z").unwrap(),
            t + 500_000
        );
        assert_eq!(parse_timestamp("2026-07-24T10:37:09-03:00").unwrap(), t);
        assert!(parse_timestamp("yesterday").is_err());
    }
}
