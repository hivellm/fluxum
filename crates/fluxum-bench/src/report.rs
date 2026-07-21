//! The comparative report (TST-094) and the ratio regression guard
//! (TST-095).
//!
//! A report is a versioned release artifact: hardware, both stacks'
//! versions and durability configuration, every workload's summaries per
//! side, and the four NFR-11 ratios with their targets. The Markdown is
//! rendered FROM the JSON — the machine-readable form is the source of
//! truth the regression guard compares against, so the two cannot drift.

use std::collections::BTreeMap;

use crate::measure::Summary;

/// The four NFR-11 ratios (TST-093). Each is oriented so **bigger is
/// better for Fluxum**, whatever direction the underlying metric runs:
/// throughput is `fluxum / baseline`, latencies are `baseline / fluxum`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Ratios {
    /// Write throughput, `fluxum / baseline`. Target ≥ 10.
    pub write_throughput: f64,
    /// End-to-end change→subscriber p99, `baseline / fluxum`. Target ≥ 10.
    pub e2e_p99: f64,
    /// Hot read p99, `baseline / fluxum`. Target ≥ 50.
    pub hot_p99: f64,
    /// Cold (page-in) load p99, `baseline / fluxum`. Target ≥ 0.5 — i.e.
    /// Fluxum within 2× of the baseline.
    pub cold_p99: f64,
}

/// `(name, value, target, met?)` for each ratio, in report order.
impl Ratios {
    /// The NFR-11 verdicts.
    #[must_use]
    pub fn verdicts(&self) -> Vec<(&'static str, f64, f64, bool)> {
        vec![
            ("write_throughput", self.write_throughput, 10.0, self.write_throughput >= 10.0),
            ("e2e_p99", self.e2e_p99, 10.0, self.e2e_p99 >= 10.0),
            ("hot_p99", self.hot_p99, 50.0, self.hot_p99 >= 50.0),
            ("cold_p99", self.cold_p99, 0.5, self.cold_p99 >= 0.5),
        ]
    }

    /// Compute the ratios from both sides' per-class summaries.
    pub fn from_summaries(
        fluxum: &BTreeMap<String, Summary>,
        baseline: &BTreeMap<String, Summary>,
    ) -> Result<Ratios, String> {
        let get = |map: &BTreeMap<String, Summary>, side: &str, class: &str| {
            map.get(class)
                .cloned()
                .ok_or_else(|| format!("{side} has no {class:?} summary"))
        };
        let ratio = |num: f64, den: f64, what: &str| {
            if den <= 0.0 {
                return Err(format!("{what}: denominator is {den}"));
            }
            Ok(num / den)
        };
        Ok(Ratios {
            write_throughput: ratio(
                get(fluxum, "fluxum", "write")?.throughput_mean,
                get(baseline, "baseline", "write")?.throughput_mean,
                "write_throughput",
            )?,
            e2e_p99: ratio(
                get(baseline, "baseline", "e2e")?.p99_ns_mean,
                get(fluxum, "fluxum", "e2e")?.p99_ns_mean,
                "e2e_p99",
            )?,
            hot_p99: ratio(
                get(baseline, "baseline", "hot")?.p99_ns_mean,
                get(fluxum, "fluxum", "hot")?.p99_ns_mean,
                "hot_p99",
            )?,
            cold_p99: ratio(
                get(baseline, "baseline", "cold")?.p99_ns_mean,
                get(fluxum, "fluxum", "cold")?.p99_ns_mean,
                "cold_p99",
            )?,
        })
    }
}

/// One side's recorded identity and configuration (TST-094).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StackInfo {
    /// "fluxum <version>" / "PostgreSQL <version>" / "SQLite <version>".
    pub version: String,
    /// The durability the measured configuration actually provides.
    pub durability: String,
    /// Free-form configuration notes (pool size, budgets, tuning).
    pub config: String,
}

/// The machine of record (TST-091 equal hardware — both sides ran here).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Hardware {
    /// CPU brand string.
    pub cpu: String,
    /// Logical cores.
    pub cores: usize,
    /// Total RAM, GiB.
    pub ram_gib: f64,
    /// OS name + version.
    pub os: String,
    /// Disk class as stated by the operator (the OS cannot tell honestly).
    pub disk: String,
}

/// The versioned comparative report (TST-094).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Report {
    /// Harness (= workspace) version this report was produced by.
    pub harness_version: String,
    /// ISO date of the run, operator-provided or best-effort.
    pub date: String,
    /// The machine both sides ran on.
    pub hardware: Hardware,
    /// side name → its identity/config.
    pub stacks: BTreeMap<String, StackInfo>,
    /// side name → class → summary (raw measurements, TST-094).
    pub workloads: BTreeMap<String, BTreeMap<String, Summary>>,
    /// The NFR-11 ratios, Fluxum vs the PostgreSQL baseline.
    pub ratios: Ratios,
}

impl Report {
    /// Render the human-readable artifact from the machine-readable one.
    #[must_use]
    pub fn markdown(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let ms = |ns: f64| ns / 1_000_000.0;
        let _ = writeln!(out, "# Fluxum parity report (harness {})", self.harness_version);
        let _ = writeln!(out, "\nDate: {}", self.date);
        let _ = writeln!(out, "\n## Hardware (both sides, same machine — TST-091)\n");
        let _ = writeln!(
            out,
            "- CPU: {} ({} logical cores)\n- RAM: {:.1} GiB\n- OS: {}\n- Disk: {}",
            self.hardware.cpu, self.hardware.cores, self.hardware.ram_gib, self.hardware.os, self.hardware.disk
        );
        let _ = writeln!(out, "\n## Stacks\n");
        for (name, stack) in &self.stacks {
            let _ = writeln!(
                out,
                "- **{name}**: {}\n  - durability: {}\n  - config: {}",
                stack.version, stack.durability, stack.config
            );
        }
        let _ = writeln!(out, "\n## NFR-11 ratios\n");
        let _ = writeln!(out, "| ratio | value | target | met |");
        let _ = writeln!(out, "| --- | --- | --- | --- |");
        for (name, value, target, met) in self.ratios.verdicts() {
            let op = if name == "cold_p99" { "≥ 0.5 (within 2×)" } else { "≥" };
            let target_text = if name == "cold_p99" {
                op.to_owned()
            } else {
                format!("{op} {target}")
            };
            let _ = writeln!(
                out,
                "| {name} | {value:.2} | {target_text} | {} |",
                if met { "✅" } else { "❌" }
            );
        }
        let _ = writeln!(out, "\n## Raw measurements (mean ± stddev across runs — TST-091)\n");
        let _ = writeln!(out, "| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | max ms | ops | runs |");
        let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
        for (side, classes) in &self.workloads {
            for (class, s) in classes {
                let _ = writeln!(
                    out,
                    "| {side} | {class} | {:.0} ±{:.0} | {:.4} | {:.4} | {:.4} | {:.3} | {} | {} |",
                    s.throughput_mean,
                    s.throughput_stddev,
                    ms(s.p50_ns_mean),
                    ms(s.p99_ns_mean),
                    ms(s.p99_ns_stddev),
                    ms(s.max_ns as f64),
                    s.total_ops,
                    s.runs
                );
            }
        }
        let _ = writeln!(
            out,
            "\n*Cold-read honesty note: restarts clear database-level caches (Fluxum buffer \
             pool / PostgreSQL `shared_buffers`) symmetrically; the OS page cache is not \
             dropped on either side, so cold numbers measure database page-in, not platter \
             latency.*"
        );
        out
    }
}

/// TST-095: compare a fresh report's ratios against the published baseline.
/// A ratio may regress by at most `tolerance` (fractional, e.g. 0.2 = 20%);
/// returns the violations, empty = pass. The published baseline advances
/// only when a release commits a new report — never automatically.
#[must_use]
pub fn regressions(current: &Ratios, published: &Ratios, tolerance: f64) -> Vec<String> {
    let floor = |published: f64| published * (1.0 - tolerance);
    let mut violations = Vec::new();
    let mut check = |name: &str, current: f64, published: f64| {
        if current < floor(published) {
            violations.push(format!(
                "{name}: {current:.2} is below {:.2} (published {published:.2} − {:.0}% tolerance)",
                floor(published),
                tolerance * 100.0
            ));
        }
    };
    check("write_throughput", current.write_throughput, published.write_throughput);
    check("e2e_p99", current.e2e_p99, published.e2e_p99);
    check("hot_p99", current.hot_p99, published.hot_p99);
    check("cold_p99", current.cold_p99, published.cold_p99);
    violations
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn summary(throughput: f64, p99_ns: f64) -> Summary {
        Summary {
            runs: 3,
            throughput_mean: throughput,
            throughput_stddev: 0.0,
            p50_ns_mean: p99_ns / 2.0,
            p99_ns_mean: p99_ns,
            p99_ns_stddev: 0.0,
            max_ns: p99_ns as u64,
            total_ops: 100,
        }
    }

    fn sides() -> (BTreeMap<String, Summary>, BTreeMap<String, Summary>) {
        let fluxum: BTreeMap<String, Summary> = [
            ("write".to_owned(), summary(50_000.0, 800_000.0)),
            ("e2e".to_owned(), summary(100.0, 500_000.0)),
            ("hot".to_owned(), summary(1_000_000.0, 1_000.0)),
            ("cold".to_owned(), summary(50.0, 8_000_000.0)),
        ]
        .into();
        let baseline: BTreeMap<String, Summary> = [
            ("write".to_owned(), summary(4_000.0, 3_000_000.0)),
            ("e2e".to_owned(), summary(100.0, 6_000_000.0)),
            ("hot".to_owned(), summary(2_000.0, 400_000.0)),
            ("cold".to_owned(), summary(50.0, 5_000_000.0)),
        ]
        .into();
        (fluxum, baseline)
    }

    #[test]
    fn ratios_orient_every_metric_as_bigger_is_better_for_fluxum() {
        let (fluxum, baseline) = sides();
        let ratios = Ratios::from_summaries(&fluxum, &baseline).unwrap();
        assert!((ratios.write_throughput - 12.5).abs() < 1e-9);
        assert!((ratios.e2e_p99 - 12.0).abs() < 1e-9);
        assert!((ratios.hot_p99 - 400.0).abs() < 1e-9);
        assert!((ratios.cold_p99 - 0.625).abs() < 1e-9);
        let verdicts = ratios.verdicts();
        assert!(verdicts.iter().all(|(_, _, _, met)| *met), "{verdicts:?}");
    }

    #[test]
    fn a_missing_class_names_the_side_and_class() {
        let (fluxum, mut baseline) = sides();
        baseline.remove("hot");
        let err = Ratios::from_summaries(&fluxum, &baseline).unwrap_err();
        assert!(err.contains("baseline") && err.contains("hot"), "{err}");
    }

    #[test]
    fn regression_guard_fires_only_beyond_tolerance() {
        let published = Ratios {
            write_throughput: 12.0,
            e2e_p99: 12.0,
            hot_p99: 400.0,
            cold_p99: 0.8,
        };
        // 10% worse everywhere: inside a 20% tolerance.
        let ok = Ratios {
            write_throughput: 10.8,
            e2e_p99: 10.8,
            hot_p99: 360.0,
            cold_p99: 0.72,
        };
        assert!(regressions(&ok, &published, 0.2).is_empty());
        // Write throughput collapses: exactly one violation, named.
        let bad = Ratios {
            write_throughput: 5.0,
            ..ok
        };
        let violations = regressions(&bad, &published, 0.2);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("write_throughput"), "{violations:?}");
    }

    #[test]
    fn markdown_renders_from_the_json_source_of_truth() {
        let (fluxum, baseline) = sides();
        let ratios = Ratios::from_summaries(&fluxum, &baseline).unwrap();
        let report = Report {
            harness_version: "0.1.0".to_owned(),
            date: "2026-07-21".to_owned(),
            hardware: Hardware {
                cpu: "Test CPU".to_owned(),
                cores: 8,
                ram_gib: 32.0,
                os: "Test OS".to_owned(),
                disk: "NVMe (operator-stated)".to_owned(),
            },
            stacks: [(
                "postgres".to_owned(),
                StackInfo {
                    version: "PostgreSQL 17".to_owned(),
                    durability: "synchronous_commit=on".to_owned(),
                    config: "pool=16, indexed".to_owned(),
                },
            )]
            .into(),
            workloads: [("fluxum".to_owned(), fluxum), ("postgres".to_owned(), baseline)].into(),
            ratios,
        };
        let md = report.markdown();
        assert!(md.contains("# Fluxum parity report"));
        assert!(md.contains("write_throughput"));
        assert!(md.contains("PostgreSQL 17"));
        assert!(md.contains("✅"));
        // The JSON round-trips — the guard reads what the release publishes.
        let json = serde_json::to_string(&report).unwrap();
        let back: Report = serde_json::from_str(&json).unwrap();
        assert_eq!(back.markdown(), md);
    }
}
