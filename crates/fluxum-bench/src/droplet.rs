//! Small-droplet profile validation (T7.7; SPEC-013 TST-110/111, NFR-12):
//! prove that a memory-constrained deployment holding a dataset **≥ 10× its
//! budget** answers exactly what an unconstrained one does.
//!
//! # Why a reference run
//!
//! Every other tiering assertion is a *bound*: RSS under budget, the pool
//! under capacity, eviction engaging. A bound cannot catch the failure that
//! actually matters here — a row that comes back **wrong** (or not at all)
//! after its page was evicted and faulted back in. TST-110 pins that down by
//! comparing results, not limits: the same dataset is loaded twice, once
//! under pressure and once with room to spare, and the two row sets must be
//! equal. The constrained run is the one under test; the reference run is
//! the oracle.
//!
//! Rows carry `u{user}-r{index}` titles, so a row identifies itself: a
//! missing row, a duplicated one, and a row leaking across the `owner_only`
//! visibility boundary (DM-060) are all visible as a set difference rather
//! than as a count that happens to match.
//!
//! # What is here, and what is not
//!
//! This module is the comparison and its report. The *cgroup-enforced*
//! 1 vCPU / 512 MB host TST-110 calls for is the CI job that invokes it
//! (`.github/workflows/droplet-profile.yml`); running the same command on a
//! roomy developer box exercises the harness but does not validate NFR-12,
//! and the report says so in `cgroup_enforced`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::workload::Side;

/// Knobs for one droplet validation.
#[derive(Debug, Clone)]
pub struct DropletConfig {
    /// Distinct users (each gets its own `owner_only` row set).
    pub users: u32,
    /// Rows per user. `users * rows_per_user` must put the dataset ≥ 10× the
    /// constrained budget for the run to mean anything (TST-110).
    pub rows_per_user: u32,
    /// The constrained deployment's `memory.budget`, bytes.
    pub budget_bytes: u64,
    /// Whether the host really enforces the NFR-12 1 vCPU / 512 MB envelope
    /// (cgroup / container limits), as opposed to merely configuring a small
    /// budget on a big machine.
    pub cgroup_enforced: bool,
}

impl Default for DropletConfig {
    fn default() -> Self {
        Self {
            users: 8,
            rows_per_user: 20_000,
            budget_bytes: 256 * 1024 * 1024,
            cgroup_enforced: false,
        }
    }
}

/// How one user's row set compared between the two runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserDiff {
    /// The user seed.
    pub user: u32,
    /// Rows the constrained run returned.
    pub constrained_rows: usize,
    /// Rows the reference run returned.
    pub reference_rows: usize,
    /// Rows the reference has that the constrained run lost, capped for the
    /// artifact — a tiering bug usually loses a whole page's worth, and the
    /// first few name it as well as all of them would.
    pub missing: Vec<String>,
    /// Rows the constrained run returned that the reference never had.
    pub unexpected: Vec<String>,
}

impl UserDiff {
    /// Whether this user's two row sets matched exactly.
    #[must_use]
    pub fn equal(&self) -> bool {
        self.missing.is_empty()
            && self.unexpected.is_empty()
            && self.constrained_rows == self.reference_rows
    }
}

/// The droplet validation report — the TST-110 artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropletReport {
    /// The bench harness version that produced the run.
    pub harness_version: String,
    /// The run date (`YYYY-MM-DD`).
    pub date: String,
    /// Host facts the run executed on.
    pub hardware: crate::report::Hardware,
    /// Users and rows loaded into each deployment.
    pub users: u32,
    /// Rows per user.
    pub rows_per_user: u32,
    /// The constrained deployment's budget, bytes.
    pub budget_bytes: u64,
    /// Bytes the constrained deployment's cold tier held — the witness that
    /// the dataset really exceeded the budget.
    pub dataset_bytes: u64,
    /// `dataset_bytes / budget_bytes`; TST-110 wants ≥ 10.
    pub dataset_over_budget: f64,
    /// Whether the dataset cleared the 10× bar.
    pub ten_x_dataset: bool,
    /// Whether the host enforced the NFR-12 envelope (cgroup/container), as
    /// opposed to just configuring a small budget on a large machine.
    pub cgroup_enforced: bool,
    /// Per-user row-set comparison.
    pub users_compared: Vec<UserDiff>,
    /// Whether every user's row set matched the reference exactly.
    pub row_sets_equal: bool,
    /// The verdict: row sets equal AND the dataset really was ≥ 10× budget.
    pub pass: bool,
}

impl DropletReport {
    /// Render the human-readable Markdown artifact.
    #[must_use]
    pub fn markdown(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
        let _ = writeln!(out, "# Fluxum small-droplet profile validation\n");
        let _ = writeln!(
            out,
            "- harness `{}` · {} · {} ({} cores, {:.0} GiB RAM)",
            self.harness_version,
            self.date,
            self.hardware.cpu,
            self.hardware.cores,
            self.hardware.ram_gib
        );
        let _ = writeln!(
            out,
            "- **verdict: {}**",
            if self.pass { "PASS ✅" } else { "FAIL ❌" }
        );
        if !self.cgroup_enforced {
            let _ = writeln!(
                out,
                "\n> ⚠️ The host did **not** enforce the NFR-12 1 vCPU / 512 MB envelope. \
                 This run exercises the TST-110 comparison but does not validate NFR-12; \
                 only a cgroup-constrained run does."
            );
        }
        let _ = writeln!(out, "\n## Dataset (TST-110)\n");
        let _ = writeln!(out, "- {} users × {} rows", self.users, self.rows_per_user);
        let _ = writeln!(out, "- budget: {:.0} MiB", mib(self.budget_bytes));
        let _ = writeln!(
            out,
            "- cold-tier dataset: {:.0} MiB",
            mib(self.dataset_bytes)
        );
        let _ = writeln!(
            out,
            "- dataset / budget: {}× ({})",
            format_ratio(self.dataset_over_budget),
            if self.ten_x_dataset {
                "clears the 10× bar"
            } else {
                "**below the 10× bar**"
            }
        );
        let _ = writeln!(out, "\n## Row-set equality vs the reference run\n");
        let _ = writeln!(
            out,
            "- **{}**",
            if self.row_sets_equal {
                "every user's rows matched the unconstrained reference exactly"
            } else {
                "ROW SETS DIVERGED — see the table"
            }
        );
        let _ = writeln!(
            out,
            "\n| user | constrained | reference | missing | unexpected |"
        );
        let _ = writeln!(out, "|---|---|---|---|---|");
        for d in &self.users_compared {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                d.user,
                d.constrained_rows,
                d.reference_rows,
                d.missing.len(),
                d.unexpected.len()
            );
        }
        for d in self.users_compared.iter().filter(|d| !d.equal()) {
            if !d.missing.is_empty() {
                let _ = writeln!(out, "\n- user {} missing: `{:?}`", d.user, d.missing);
            }
            if !d.unexpected.is_empty() {
                let _ = writeln!(out, "- user {} unexpected: `{:?}`", d.user, d.unexpected);
            }
        }
        out
    }

    /// Write the JSON + Markdown artifacts as `{stem}.json` / `{stem}.md`.
    ///
    /// # Errors
    /// Directory creation, serialization, or file I/O failing.
    pub fn write_artifacts(&self, out_dir: &std::path::Path, stem: &str) -> Result<(), String> {
        std::fs::create_dir_all(out_dir)
            .map_err(|e| format!("create {}: {e}", out_dir.display()))?;
        let json = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(out_dir.join(format!("{stem}.json")), json).map_err(|e| e.to_string())?;
        std::fs::write(out_dir.join(format!("{stem}.md")), self.markdown())
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

/// At most this many differing rows are named per user in the artifact.
const DIFF_SAMPLE: usize = 8;

/// Render a dataset/budget ratio. A far-under-the-bar run is the normal
/// outcome of a scaled-down rehearsal, and `0.0×` reads like a broken
/// measurement rather than a small number, so sub-unit ratios keep enough
/// significant digits to be recognisable as real.
#[must_use]
pub fn format_ratio(ratio: f64) -> String {
    if ratio >= 1.0 {
        format!("{ratio:.1}")
    } else if ratio > 0.0 {
        format!("{ratio:.4}")
    } else {
        "0".to_owned()
    }
}

/// Compare one user's two row sets as **multisets** — a duplicated row is a
/// divergence, not a match, so plain set subtraction would be too forgiving.
#[must_use]
pub fn diff_rows(user: u32, constrained: &[String], reference: &[String]) -> UserDiff {
    let mut counts: BTreeMap<&str, i64> = BTreeMap::new();
    for row in reference {
        *counts.entry(row.as_str()).or_default() += 1;
    }
    for row in constrained {
        *counts.entry(row.as_str()).or_default() -= 1;
    }
    let mut missing = Vec::new();
    let mut unexpected = Vec::new();
    for (row, delta) in counts {
        // A positive delta means the reference had copies the constrained
        // run did not return; negative is the reverse.
        for _ in 0..delta.max(0).min(DIFF_SAMPLE as i64) {
            if missing.len() < DIFF_SAMPLE {
                missing.push(row.to_owned());
            }
        }
        for _ in 0..(-delta).max(0).min(DIFF_SAMPLE as i64) {
            if unexpected.len() < DIFF_SAMPLE {
                unexpected.push(row.to_owned());
            }
        }
    }
    UserDiff {
        user,
        constrained_rows: constrained.len(),
        reference_rows: reference.len(),
        missing,
        unexpected,
    }
}

/// Whether the dataset really exceeded the budget by the TST-110 factor.
#[must_use]
pub fn ten_x_dataset(dataset_bytes: u64, budget_bytes: u64) -> bool {
    budget_bytes > 0 && dataset_bytes >= budget_bytes.saturating_mul(10)
}

/// The droplet verdict: every user's rows matched the reference, and the
/// dataset genuinely was ≥ 10× the budget. A comparison over a dataset that
/// fit in memory proves nothing about tiering, so it is not a pass.
#[must_use]
pub fn droplet_pass(diffs: &[UserDiff], ten_x: bool) -> bool {
    ten_x && !diffs.is_empty() && diffs.iter().all(UserDiff::equal)
}

/// Load `cfg.rows_per_user` self-identifying rows for each of `cfg.users`
/// users into `side`, then read every user's rows back.
///
/// Rows are written one user at a time through that user's own client, so
/// the dataset is byte-identical between the constrained and reference runs.
///
/// # Errors
/// Any client operation failing.
pub fn load_and_read(side: &dyn Side, cfg: &DropletConfig) -> Result<Vec<Vec<String>>, String> {
    let mut out = Vec::with_capacity(cfg.users as usize);
    for user in 0..cfg.users {
        let mut client = side.client(u64::from(user))?;
        for row in 0..cfg.rows_per_user {
            client.add_task(&row_title(user, row))?;
        }
        out.push(client.read_all_rows()?);
    }
    Ok(out)
}

/// The self-identifying row body: which user wrote it and which row it is.
#[must_use]
pub fn row_title(user: u32, row: u32) -> String {
    format!("u{user}-r{row}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn rows(user: u32, n: u32) -> Vec<String> {
        let mut v: Vec<String> = (0..n).map(|r| row_title(user, r)).collect();
        v.sort();
        v
    }

    #[test]
    fn identical_row_sets_compare_equal() {
        let d = diff_rows(0, &rows(0, 100), &rows(0, 100));
        assert!(d.equal());
        assert_eq!(d.constrained_rows, 100);
        assert!(d.missing.is_empty() && d.unexpected.is_empty());
    }

    #[test]
    fn a_lost_row_is_reported_as_missing() {
        let reference = rows(0, 10);
        let mut constrained = reference.clone();
        constrained.retain(|r| r != "u0-r4");
        let d = diff_rows(0, &constrained, &reference);
        assert!(!d.equal(), "a lost row must fail the comparison");
        assert_eq!(d.missing, vec!["u0-r4".to_owned()]);
        assert!(d.unexpected.is_empty());
    }

    #[test]
    fn a_corrupted_row_shows_as_both_missing_and_unexpected() {
        // The count matches, so only comparing lengths would pass this.
        let reference = rows(0, 10);
        let mut constrained = reference.clone();
        constrained[3] = "u0-rXX".to_owned();
        let d = diff_rows(0, &constrained, &reference);
        assert!(!d.equal());
        assert_eq!(d.constrained_rows, d.reference_rows);
        assert_eq!(d.missing, vec!["u0-r3".to_owned()]);
        assert_eq!(d.unexpected, vec!["u0-rXX".to_owned()]);
    }

    #[test]
    fn a_duplicated_row_is_a_divergence_not_a_match() {
        // Multiset semantics: plain set subtraction would call this equal.
        let reference = rows(0, 5);
        let mut constrained = reference.clone();
        constrained.push("u0-r2".to_owned());
        let d = diff_rows(0, &constrained, &reference);
        assert!(!d.equal());
        assert_eq!(d.unexpected, vec!["u0-r2".to_owned()]);
    }

    #[test]
    fn a_row_leaking_across_the_visibility_boundary_is_unexpected() {
        // owner_only (DM-060): user 1's row must never appear in user 0's
        // set, however hard the pool churned.
        let reference = rows(0, 4);
        let mut constrained = reference.clone();
        constrained.push(row_title(1, 0));
        let d = diff_rows(0, &constrained, &reference);
        assert!(!d.equal());
        assert_eq!(d.unexpected, vec!["u1-r0".to_owned()]);
    }

    #[test]
    fn the_named_diff_sample_is_capped() {
        let reference = rows(0, 500);
        let d = diff_rows(0, &[], &reference);
        assert_eq!(d.missing.len(), DIFF_SAMPLE, "the artifact stays readable");
        assert_eq!(d.constrained_rows, 0, "the full counts are still exact");
        assert_eq!(d.reference_rows, 500);
    }

    #[test]
    fn a_tiny_ratio_stays_legible_as_a_number() {
        // A scaled-down rehearsal lands far under the bar; rendering that as
        // "0.0" would read as a broken measurement.
        assert_eq!(format_ratio(0.0017), "0.0017");
        assert_eq!(format_ratio(12.34), "12.3");
        assert_eq!(format_ratio(0.0), "0");
    }

    #[test]
    fn ten_x_is_required_for_a_pass() {
        assert!(ten_x_dataset(1000, 100));
        assert!(ten_x_dataset(1001, 100));
        assert!(!ten_x_dataset(999, 100));
        assert!(!ten_x_dataset(1000, 0), "no budget, no ratio");

        let equal = vec![diff_rows(0, &rows(0, 3), &rows(0, 3))];
        assert!(droplet_pass(&equal, true));
        // A dataset that fit in memory proves nothing about tiering.
        assert!(!droplet_pass(&equal, false));
        // Nor does comparing nothing at all.
        assert!(!droplet_pass(&[], true));
        let diverged = vec![diff_rows(0, &rows(0, 2), &rows(0, 3))];
        assert!(!droplet_pass(&diverged, true));
    }

    #[test]
    fn a_report_renders_and_round_trips() {
        let report = DropletReport {
            harness_version: "0.1.0".into(),
            date: "2026-07-27".into(),
            hardware: crate::report::Hardware {
                cpu: "test".into(),
                cores: 1,
                ram_gib: 0.5,
                os: "test".into(),
                disk: "test".into(),
            },
            users: 2,
            rows_per_user: 10,
            budget_bytes: 100,
            dataset_bytes: 2000,
            dataset_over_budget: 20.0,
            ten_x_dataset: true,
            cgroup_enforced: true,
            users_compared: vec![diff_rows(0, &rows(0, 10), &rows(0, 10))],
            row_sets_equal: true,
            pass: true,
        };
        let md = report.markdown();
        assert!(md.contains("PASS"));
        assert!(md.contains("clears the 10× bar"));
        assert!(
            !md.contains("did **not** enforce"),
            "cgroup run: no warning"
        );

        let json = serde_json::to_string_pretty(&report).unwrap();
        let back: DropletReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back.markdown(), md, "JSON is the source of truth");
    }

    #[test]
    fn an_unenforced_host_is_flagged_in_the_artifact() {
        // Running the harness on a roomy dev box must not read as an NFR-12
        // validation, even when the comparison itself passes.
        let mut report = DropletReport {
            harness_version: "0.1.0".into(),
            date: "2026-07-27".into(),
            hardware: crate::report::Hardware {
                cpu: "big".into(),
                cores: 32,
                ram_gib: 127.0,
                os: "test".into(),
                disk: "test".into(),
            },
            users: 1,
            rows_per_user: 1,
            budget_bytes: 100,
            dataset_bytes: 2000,
            dataset_over_budget: 20.0,
            ten_x_dataset: true,
            cgroup_enforced: false,
            users_compared: vec![diff_rows(0, &rows(0, 1), &rows(0, 1))],
            row_sets_equal: true,
            pass: true,
        };
        assert!(report.markdown().contains("did **not** enforce"));
        report.cgroup_enforced = true;
        assert!(!report.markdown().contains("did **not** enforce"));
    }
}
