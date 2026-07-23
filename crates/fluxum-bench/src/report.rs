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
            (
                "write_throughput",
                self.write_throughput,
                10.0,
                self.write_throughput >= 10.0,
            ),
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

/// The competitive-baseline ratios (TST-097): Fluxum vs SpacetimeDB, one
/// per workload class, oriented so **bigger is better for Fluxum** —
/// throughputs are `fluxum / spacetimedb`, latencies `spacetimedb / fluxum`.
///
/// The target for every class is ≥ 1.0 (parity with the baseline Fluxum
/// must reach). Unlike the NFR-11 ratios these are *informational* until a
/// class first reaches 1.0; from then on [`competitive_regressions`] floors
/// it (a class that reached parity may never silently fall back below).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompetitiveRatios {
    /// Write throughput, `fluxum / spacetimedb`.
    pub write_throughput: f64,
    /// End-to-end change→subscriber p99, `spacetimedb / fluxum`.
    pub e2e_p99: f64,
    /// Hot read p99, `spacetimedb / fluxum`.
    pub hot_p99: f64,
    /// Cold (page-in) load p99, `spacetimedb / fluxum`.
    pub cold_p99: f64,
    /// Write throughput under contention, `fluxum / spacetimedb`.
    pub mixed_write_throughput: f64,
    /// Hot read p99 under contention, `spacetimedb / fluxum`.
    pub mixed_read_p99: f64,
    /// Delivery p99 under contention, `spacetimedb / fluxum`.
    pub mixed_e2e_p99: f64,
}

impl CompetitiveRatios {
    /// `(name, value, reached-parity?)` per class, in report order.
    #[must_use]
    pub fn verdicts(&self) -> Vec<(&'static str, f64, bool)> {
        [
            ("write_throughput", self.write_throughput),
            ("e2e_p99", self.e2e_p99),
            ("hot_p99", self.hot_p99),
            ("cold_p99", self.cold_p99),
            ("mixed_write_throughput", self.mixed_write_throughput),
            ("mixed_read_p99", self.mixed_read_p99),
            ("mixed_e2e_p99", self.mixed_e2e_p99),
        ]
        .into_iter()
        .map(|(name, value)| (name, value, value >= 1.0))
        .collect()
    }

    /// Compute the ratios from both sides' per-class summaries.
    pub fn from_summaries(
        fluxum: &BTreeMap<String, Summary>,
        spacetimedb: &BTreeMap<String, Summary>,
    ) -> Result<CompetitiveRatios, String> {
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
        let throughput = |class: &str, what: &str| {
            ratio(
                get(fluxum, "fluxum", class)?.throughput_mean,
                get(spacetimedb, "spacetimedb", class)?.throughput_mean,
                what,
            )
        };
        let p99 = |class: &str, what: &str| {
            ratio(
                get(spacetimedb, "spacetimedb", class)?.p99_ns_mean,
                get(fluxum, "fluxum", class)?.p99_ns_mean,
                what,
            )
        };
        Ok(CompetitiveRatios {
            write_throughput: throughput("write", "competitive write_throughput")?,
            e2e_p99: p99("e2e", "competitive e2e_p99")?,
            hot_p99: p99("hot", "competitive hot_p99")?,
            cold_p99: p99("cold", "competitive cold_p99")?,
            mixed_write_throughput: throughput(
                "mixed/write",
                "competitive mixed_write_throughput",
            )?,
            mixed_read_p99: p99("mixed/read", "competitive mixed_read_p99")?,
            mixed_e2e_p99: p99("mixed/e2e", "competitive mixed_e2e_p99")?,
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
    /// The TST-097 competitive ratios, Fluxum vs SpacetimeDB. `None` when
    /// the run had no spacetimedb side (kept optional so older published
    /// reports still parse); the release report always carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub competitive: Option<CompetitiveRatios>,
}

impl Report {
    /// Render the human-readable artifact from the machine-readable one.
    #[must_use]
    pub fn markdown(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let ms = |ns: f64| ns / 1_000_000.0;
        let _ = writeln!(
            out,
            "# Fluxum parity report (harness {})",
            self.harness_version
        );
        let _ = writeln!(out, "\nDate: {}", self.date);
        let _ = writeln!(out, "\n## Scope and method\n");
        let _ = writeln!(
            out,
            "The **NFR-11 verdicts** below come from a **PostgreSQL parity harness**: the \
             baseline is tuned PostgreSQL behind an axum+sqlx app server in its own process \
             (pooled prepared statements, covering indexes, LISTEN/NOTIFY fan-out) — the \
             stack a team would replace with Fluxum. They are **not** SpacetimeDB numbers; \
             the competitive SpacetimeDB baseline (TST-097) is measured against a real \
             SpacetimeDB server and reported in its own section, and the two never mix.\n\n\
             Method (TST-091): every class runs on the same idle machine, remote socket \
             transport on every side except where a row is footnoted as an architectural \
             asymmetry. Raw rows report mean ± stddev across runs plus a 95% Student-t \
             confidence half-width on p99, so a verdict is distinguishable from noise. Core \
             pinning (`--pin server=0xMASK,driver=0xMASK`) is a documented methodology knob; \
             the canonical report runs UNPINNED — on the 32-core bench box confining the \
             server to half the cores measurably degrades every heavy phase (recorded \
             2026-07-22, phase0_parity-fanout-latency 1.4) — and the active setting is \
             recorded in each stack's config line."
        );
        let _ = writeln!(out, "\n## Hardware (both sides, same machine — TST-091)\n");
        let _ = writeln!(
            out,
            "- CPU: {} ({} logical cores)\n- RAM: {:.1} GiB\n- OS: {}\n- Disk: {}",
            self.hardware.cpu,
            self.hardware.cores,
            self.hardware.ram_gib,
            self.hardware.os,
            self.hardware.disk
        );
        let _ = writeln!(out, "\n## Stacks\n");
        for (name, stack) in &self.stacks {
            let _ = writeln!(
                out,
                "- **{name}**: {}\n  - durability: {}\n  - config: {}",
                stack.version, stack.durability, stack.config
            );
        }
        let _ = writeln!(out, "\n## NFR-11 ratios (vs the PostgreSQL parity baseline)\n");
        let _ = writeln!(out, "| ratio | value | target | met |");
        let _ = writeln!(out, "| --- | --- | --- | --- |");
        for (name, value, target, met) in self.ratios.verdicts() {
            let op = if name == "cold_p99" {
                "≥ 0.5 (within 2×)"
            } else {
                "≥"
            };
            let target_text = if name == "cold_p99" {
                op.to_owned()
            } else {
                format!("{op} {target}")
            };
            // F-009: the hot ratio is an architecture asymmetry, not a
            // same-transport comparison — footnoted, never a headline.
            let marker = if name == "hot_p99" { "†" } else { "" };
            let _ = writeln!(
                out,
                "| {name}{marker} | {value:.2} | {target_text} | {} |",
                if met { "✅" } else { "❌" }
            );
        }
        let _ = writeln!(
            out,
            "\n† *hot_p99 compares an **in-process cache read** (the Fluxum client reads \
             its subscribed rows from local memory — no socket round-trip) against \
             PostgreSQL's **remote prepared read** over a pooled connection. The asymmetry \
             is the architecture being sold — subscribe once, read locally — but it is not \
             a same-transport ratio, so it must never lead the summary. The same applies \
             to the `hot` and `mixed/read` raw rows below (and to SpacetimeDB's, whose SDK \
             reads its local cache too).*"
        );
        if let Some(competitive) = &self.competitive {
            let _ = writeln!(out, "\n## Competitive baseline vs SpacetimeDB (TST-097)\n");
            let _ = writeln!(
                out,
                "Ratios oriented bigger-is-better-for-Fluxum; ≥ 1.00 = parity with \
                 SpacetimeDB reached for that class. Informational until reached, \
                 floored by the regression guard afterwards.\n"
            );
            let _ = writeln!(out, "| ratio | value | target | reached |");
            let _ = writeln!(out, "| --- | --- | --- | --- |");
            for (name, value, reached) in competitive.verdicts() {
                let _ = writeln!(
                    out,
                    "| {name} | {value:.2} | ≥ 1.0 | {} |",
                    if reached { "✅" } else { "⏳" }
                );
            }
        }
        let _ = writeln!(
            out,
            "\n## Raw measurements (mean ± stddev across runs — TST-091)\n"
        );
        let _ = writeln!(
            out,
            "| side | class | ops/s | p50 ms | p99 ms | p99 σ ms | p99 CI95 ± ms | max ms | ops | runs |"
        );
        let _ = writeln!(
            out,
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
        );
        for (side, classes) in &self.workloads {
            for (class, s) in classes {
                // F-010: e2e classes cap the event rate by design — their
                // ops/s is a workload constant, not a measurement.
                let ops_cell = if class == "e2e" || class.ends_with("/e2e") {
                    "‡ (rate-capped)".to_owned()
                } else {
                    format!("{:.0} ±{:.0}", s.throughput_mean, s.throughput_stddev)
                };
                let ci_cell = match ci95_half_width_ns(s.p99_ns_stddev, s.runs) {
                    Some(half) => format!("{:.4}", ms(half)),
                    None => "—".to_owned(),
                };
                let _ = writeln!(
                    out,
                    "| {side} | {class} | {ops_cell} | {:.4} | {:.4} | {:.4} | {ci_cell} | {:.3} | {} | {} |",
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
            "\n‡ *e2e and mixed/e2e rows are **latency-only**: the workload caps the chat \
             event rate (a fixed messages-per-second sender), so their delivered-updates/s \
             is that cap times the subscriber count on every side — a harness constant, \
             not a throughput result. Only their latency columns are measurements.*"
        );
        if self
            .workloads
            .values()
            .any(|classes| classes.keys().any(|c| c.starts_with("write/pipelined")))
        {
            let _ = writeln!(
                out,
                "\n*write/pipelined(N) is a **fluxum-only NFR-01 evidence row**: the same \
                 acked reducer write with N calls held in flight per connection (Rust SDK \
                 `call_reducer_async`). Its latency columns include the deliberate \
                 client-held window queueing — **throughput is the meaningful column** — \
                 and it feeds no ratio: the incumbent's app-server protocol is strictly \
                 request/response, so its concurrency lever (connection count) is already \
                 the `write` row. The acked-serial `write` row above remains the honest \
                 latency number.*"
            );
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

/// Two-sided 95% Student-t confidence half-width for a mean estimated from
/// `runs` samples (df = runs − 1): `t · σ/√n`. `None` below two runs — no
/// spread exists to bound. Beyond the table the normal 1.96 is close enough
/// (F-011: the report states uncertainty, it does not do inference).
fn ci95_half_width_ns(stddev_ns: f64, runs: usize) -> Option<f64> {
    let t = match runs {
        0 | 1 => return None,
        2 => 12.706,
        3 => 4.303,
        4 => 3.182,
        5 => 2.776,
        6 => 2.571,
        7 => 2.447,
        8 => 2.365,
        9 => 2.306,
        10 => 2.262,
        _ => 1.96,
    };
    #[allow(clippy::cast_precision_loss)]
    Some(t * stddev_ns / (runs as f64).sqrt())
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
    check(
        "write_throughput",
        current.write_throughput,
        published.write_throughput,
    );
    check("e2e_p99", current.e2e_p99, published.e2e_p99);
    check("hot_p99", current.hot_p99, published.hot_p99);
    check("cold_p99", current.cold_p99, published.cold_p99);
    violations
}

/// TST-095, noise-aware (F-011 applied to the guard itself): a ratio has
/// regressed only when BOTH (a) its point estimate dropped more than
/// `tolerance` below the published one — exactly [`regressions`] — AND
/// (b) the two runs' ratio-uncertainty intervals do not overlap. A ratio
/// whose denominator is a sub-µs in-process read sits at timer resolution:
/// its point estimate swings ±50% run to run, and a relative-only guard
/// flaps on pure noise, while a REAL fall (the read becoming a socket
/// round-trip) lands orders of magnitude outside any band. Intervals are
/// 95% Student-t bands on each side's underlying metric (throughput for
/// the write ratio, p99 for the latency ratios), combined by interval
/// arithmetic over positive quantities. When either report lacks the
/// summaries to build a band (foreign artifact), the ratio falls back to
/// the pure relative check.
#[must_use]
pub fn regressions_with_uncertainty(
    current: &Report,
    published: &Report,
    tolerance: f64,
) -> Vec<String> {
    let plain = regressions(&current.ratios, &published.ratios, tolerance);
    if plain.is_empty() {
        return plain;
    }
    plain
        .into_iter()
        .filter(|violation| {
            let name = violation.split(':').next().unwrap_or_default();
            match (ratio_interval(current, name), ratio_interval(published, name)) {
                // Distinguishable from noise only when the current band sits
                // entirely below the published one.
                (Some((_, cur_hi)), Some((pub_lo, _))) => cur_hi < pub_lo,
                // No bands to compare — keep the conservative verdict.
                _ => true,
            }
        })
        .collect()
}

/// The 95% band of one NFR-11 ratio, from the report's own summaries:
/// `[num_lo/den_hi, num_hi/den_lo]`. `None` when the report does not carry
/// the classes (e.g. a hand-built ratios-only artifact).
fn ratio_interval(report: &Report, ratio: &str) -> Option<(f64, f64)> {
    let side = |name: &str, class: &str| report.workloads.get(name)?.get(class).cloned();
    let band = |mean: f64, stddev: f64, runs: usize| -> (f64, f64) {
        let half = ci95_half_width_ns(stddev, runs).unwrap_or(0.0);
        // A positive floor keeps the interval division meaningful even when
        // the band spans zero (a denominator cannot be ≤ 0).
        ((mean - half).max(mean * 1e-3), mean + half)
    };
    let p99_band = |name: &str, class: &str| -> Option<(f64, f64)> {
        let s = side(name, class)?;
        Some(band(s.p99_ns_mean, s.p99_ns_stddev, s.runs))
    };
    let (num, den) = match ratio {
        "write_throughput" => {
            let f = side("fluxum", "write")?;
            let b = side("postgres", "write")?;
            (
                band(f.throughput_mean, f.throughput_stddev, f.runs),
                band(b.throughput_mean, b.throughput_stddev, b.runs),
            )
        }
        "e2e_p99" => (p99_band("postgres", "e2e")?, p99_band("fluxum", "e2e")?),
        "hot_p99" => (p99_band("postgres", "hot")?, p99_band("fluxum", "hot")?),
        "cold_p99" => (p99_band("postgres", "cold")?, p99_band("fluxum", "cold")?),
        _ => return None,
    };
    (den.0 > 0.0).then_some((num.0 / den.1, num.1 / den.0))
}

/// TST-097 guard: the competitive ratios are informational while a class is
/// below parity, but once the published report shows a class at ≥ 1.0 that
/// class is **floored** — reaching SpacetimeDB and silently falling back
/// would un-earn the product claim. The floor honors the same `tolerance`
/// the NFR-11 guard takes: a class sitting AT parity between two
/// noise-dominated measurements (e.g. two sub-µs in-process reads) must not
/// flap the release on a within-noise dip — only a real fall below
/// `1.0 · (1 − tolerance)` fires. Also fires when the published report
/// carries the block and the current run dropped it entirely.
#[must_use]
pub fn competitive_regressions(
    current: Option<&CompetitiveRatios>,
    published: Option<&CompetitiveRatios>,
    tolerance: f64,
) -> Vec<String> {
    let Some(published) = published else {
        return Vec::new(); // nothing earned yet, nothing to floor
    };
    let Some(current) = current else {
        return vec![
            "competitive: published report has the SpacetimeDB block; current run has none"
                .to_owned(),
        ];
    };
    let floor = 1.0 * (1.0 - tolerance);
    let published: BTreeMap<&str, f64> = published
        .verdicts()
        .into_iter()
        .map(|(name, value, _)| (name, value))
        .collect();
    current
        .verdicts()
        .into_iter()
        .filter_map(|(name, value, _)| {
            let earned = published.get(name).copied().unwrap_or(0.0) >= 1.0;
            (earned && value < floor).then(|| {
                format!(
                    "competitive {name}: {value:.2} fell below the parity floor {floor:.2} \
                     (published {:.2} had reached 1.0; tolerance {:.0}%)",
                    published[name],
                    tolerance * 100.0
                )
            })
        })
        .collect()
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

    /// A minimal report over `sides`-shaped workloads with a chosen fluxum
    /// hot p99 (mean, stddev) — the noise-aware guard's test subject.
    fn report_with_hot(fluxum_hot_p99: f64, fluxum_hot_stddev: f64) -> Report {
        let (mut fluxum, baseline) = sides();
        let hot = fluxum.get_mut("hot").unwrap();
        hot.p99_ns_mean = fluxum_hot_p99;
        hot.p99_ns_stddev = fluxum_hot_stddev;
        hot.runs = 5;
        let ratios = Ratios::from_summaries(&fluxum, &baseline).unwrap();
        Report {
            harness_version: "0.1.0".to_owned(),
            date: "2026-07-23".to_owned(),
            hardware: Hardware {
                cpu: "Test CPU".to_owned(),
                cores: 8,
                ram_gib: 32.0,
                os: "Test OS".to_owned(),
                disk: "NVMe".to_owned(),
            },
            stacks: BTreeMap::new(),
            workloads: [
                ("fluxum".to_owned(), fluxum),
                ("postgres".to_owned(), baseline),
            ]
            .into(),
            ratios,
            competitive: None,
        }
    }

    #[test]
    fn the_noise_aware_guard_ignores_within_band_drops_and_catches_real_falls() {
        // Published: fluxum in-process hot p99 120 ns ± 45 (timer-resolution
        // noise) → a huge, noisy ratio. Current: 180 ns ± 45 — a >30% point
        // drop that is pure noise (the 95% bands overlap).
        let published = report_with_hot(120.0, 45.0);
        let noisy = report_with_hot(180.0, 45.0);
        assert!(
            !regressions(&noisy.ratios, &published.ratios, 0.2).is_empty(),
            "the relative-only check DOES flag it (that is the flaw)"
        );
        assert!(
            regressions_with_uncertainty(&noisy, &published, 0.2).is_empty(),
            "the bands overlap — noise, not a regression"
        );
        // A real fall: the in-process read became a socket round trip
        // (100 µs). Far outside any band → fires.
        let real = report_with_hot(100_000.0, 1_000.0);
        let violations = regressions_with_uncertainty(&real, &published, 0.2);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(violations[0].contains("hot_p99"), "{violations:?}");
        // A report without workload summaries (foreign artifact) falls back
        // to the conservative relative check.
        let mut bare = noisy.clone();
        bare.workloads.clear();
        assert!(
            !regressions_with_uncertainty(&bare, &published, 0.2).is_empty(),
            "no bands to compare — keep the relative verdict"
        );
    }

    #[test]
    fn ratio_intervals_cover_every_nfr_ratio_and_reject_unknown_names() {
        let report = report_with_hot(120.0, 45.0);
        for name in ["write_throughput", "e2e_p99", "hot_p99", "cold_p99"] {
            let (lo, hi) = ratio_interval(&report, name).unwrap();
            assert!(lo > 0.0 && hi >= lo, "{name}: [{lo}, {hi}]");
        }
        // The hot band widens around its point estimate (σ 45 over 5 runs);
        // a zero-σ ratio collapses to (almost) a point.
        let (hot_lo, hot_hi) = ratio_interval(&report, "hot_p99").unwrap();
        let point = report.ratios.hot_p99;
        assert!(hot_lo < point && point < hot_hi);
        let (w_lo, w_hi) = ratio_interval(&report, "write_throughput").unwrap();
        assert!((w_hi - w_lo) / w_lo < 0.01, "zero-σ write band is tight");
        assert!(ratio_interval(&report, "nonsense").is_none());
    }

    /// A spacetimedb side whose classes make every competitive ratio land
    /// on an easily-asserted value against [`sides`]'s fluxum numbers.
    fn spacetimedb_classes() -> BTreeMap<String, Summary> {
        [
            ("write".to_owned(), summary(25_000.0, 1_000_000.0)),
            ("e2e".to_owned(), summary(100.0, 1_000_000.0)),
            ("hot".to_owned(), summary(500_000.0, 2_000.0)),
            ("cold".to_owned(), summary(50.0, 4_000_000.0)),
            ("mixed/write".to_owned(), summary(80_000.0, 1_000_000.0)),
            ("mixed/read".to_owned(), summary(500_000.0, 500.0)),
            ("mixed/e2e".to_owned(), summary(100.0, 250_000.0)),
        ]
        .into()
    }

    /// Fluxum mixed classes to pair with [`spacetimedb_classes`].
    fn fluxum_mixed() -> [(String, Summary); 3] {
        [
            ("mixed/write".to_owned(), summary(40_000.0, 900_000.0)),
            ("mixed/read".to_owned(), summary(800_000.0, 1_000.0)),
            ("mixed/e2e".to_owned(), summary(100.0, 500_000.0)),
        ]
    }

    #[test]
    fn competitive_ratios_orient_bigger_as_better_for_fluxum() {
        let (mut fluxum, _) = sides();
        fluxum.extend(fluxum_mixed());
        let competitive =
            CompetitiveRatios::from_summaries(&fluxum, &spacetimedb_classes()).unwrap();
        assert!((competitive.write_throughput - 2.0).abs() < 1e-9); // 50k/25k
        assert!((competitive.e2e_p99 - 2.0).abs() < 1e-9); // 1ms/0.5ms
        assert!((competitive.hot_p99 - 2.0).abs() < 1e-9); // 2µs/1µs
        assert!((competitive.cold_p99 - 0.5).abs() < 1e-9); // 4ms/8ms
        assert!((competitive.mixed_write_throughput - 0.5).abs() < 1e-9); // 40k/80k
        assert!((competitive.mixed_read_p99 - 0.5).abs() < 1e-9); // 0.5µs/1µs
        assert!((competitive.mixed_e2e_p99 - 0.5).abs() < 1e-9); // 0.25ms/0.5ms
        // Parity verdicts follow the 1.0 target per class.
        let verdicts = competitive.verdicts();
        let reached: Vec<bool> = verdicts.iter().map(|(_, _, reached)| *reached).collect();
        assert_eq!(reached, [true, true, true, false, false, false, false]);
    }

    #[test]
    fn competitive_guard_floors_only_classes_that_reached_parity() {
        let (mut fluxum, _) = sides();
        fluxum.extend(fluxum_mixed());
        let published = CompetitiveRatios::from_summaries(&fluxum, &spacetimedb_classes()).unwrap();
        // Identical current: nothing fires (below-parity classes stay
        // informational, reached classes are at their floor).
        assert!(competitive_regressions(Some(&published), Some(&published), 0.2).is_empty());
        // A reached class falling clearly below the floor fires; a
        // never-reached class sinking further does not.
        let mut current = published.clone();
        current.write_throughput = 0.5; // was ≥ 1.0 in published
        current.cold_p99 = 0.1; // was < 1.0 in published
        let violations = competitive_regressions(Some(&current), Some(&published), 0.2);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(violations[0].contains("write_throughput"), "{violations:?}");
        // The floor is tolerance-aware: a within-noise dip at the parity
        // boundary (two noise-dominated measurements) must not flap the
        // release — only a real fall beyond tolerance fires.
        let mut noisy = published.clone();
        noisy.write_throughput = 0.93;
        assert!(competitive_regressions(Some(&noisy), Some(&published), 0.2).is_empty());
        assert_eq!(
            competitive_regressions(Some(&noisy), Some(&published), 0.05).len(),
            1,
            "a tighter tolerance still catches the same dip"
        );
        // Dropping the whole block after publishing it is itself a
        // regression; never having published one is not.
        assert_eq!(competitive_regressions(None, Some(&published), 0.2).len(), 1);
        assert!(competitive_regressions(Some(&current), None, 0.2).is_empty());
        assert!(competitive_regressions(None, None, 0.2).is_empty());
    }

    #[test]
    fn ci95_half_width_follows_student_t_and_needs_two_runs() {
        // No spread to bound below two runs.
        assert!(ci95_half_width_ns(1_000.0, 0).is_none());
        assert!(ci95_half_width_ns(1_000.0, 1).is_none());
        // n = 5 → t = 2.776, half-width = 2.776 · σ/√5.
        let half = ci95_half_width_ns(1_000.0, 5).unwrap();
        assert!((half - 2.776 * 1_000.0 / 5.0_f64.sqrt()).abs() < 1e-9);
        // Beyond the table: the normal approximation.
        let half = ci95_half_width_ns(1_000.0, 30).unwrap();
        assert!((half - 1.96 * 1_000.0 / 30.0_f64.sqrt()).abs() < 1e-9);
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
            workloads: [
                ("fluxum".to_owned(), fluxum),
                ("postgres".to_owned(), baseline),
            ]
            .into(),
            ratios,
            competitive: None,
        };
        let md = report.markdown();
        assert!(md.contains("# Fluxum parity report"));
        assert!(md.contains("write_throughput"));
        assert!(md.contains("PostgreSQL 17"));
        assert!(md.contains("✅"));
        // F-008: the scope statement names the baseline and separates the
        // NFR-11 verdicts from the competitive SpacetimeDB section.
        assert!(md.contains("PostgreSQL parity harness"));
        assert!(md.contains("not** SpacetimeDB numbers"));
        // F-009: hot_p99 carries the asymmetry footnote, never unmarked.
        assert!(md.contains("hot_p99†"));
        assert!(md.contains("in-process cache read"));
        // F-010: e2e rows are latency-only; their ops/s is the cap.
        assert!(md.contains("‡ (rate-capped)"));
        assert!(md.contains("latency-only"));
        assert!(!md.contains("| e2e | 100 ±0 |"), "e2e ops/s must not render");
        // F-011: the p99 CI95 column renders from stddev and runs.
        assert!(md.contains("p99 CI95 ± ms"));
        // F-007: no pipelined class → no pipelined footnote.
        assert!(!md.contains("write/pipelined"));
        // Without a spacetimedb side there is no TST-097 section — and the
        // JSON omits the key entirely, which is also the old-schema shape,
        // so pre-TST-097 published reports keep loading (guard inputs).
        assert!(!md.contains("Competitive baseline"));
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("competitive"));
        let back: Report = serde_json::from_str(&json).unwrap();
        assert!(back.competitive.is_none());
        assert_eq!(back.markdown(), md);

        // With the block: section renders, verdict icons split at 1.0, and
        // the JSON round-trips.
        let mut with_stdb = report;
        let (mut fluxum, _) = sides();
        fluxum.extend(fluxum_mixed());
        with_stdb.competitive =
            Some(CompetitiveRatios::from_summaries(&fluxum, &spacetimedb_classes()).unwrap());
        let md = with_stdb.markdown();
        assert!(md.contains("Competitive baseline vs SpacetimeDB (TST-097)"));
        assert!(md.contains("mixed_e2e_p99"));
        assert!(md.contains("⏳"));
        let json = serde_json::to_string(&with_stdb).unwrap();
        let back: Report = serde_json::from_str(&json).unwrap();
        assert_eq!(back.markdown(), md);

        // F-007: a fluxum-only pipelined-write class renders with its
        // honesty footnote (throughput row, latency includes queueing,
        // feeds no ratio) and never invents a ratio.
        let mut with_pipelined = with_stdb;
        with_pipelined
            .workloads
            .get_mut("fluxum")
            .unwrap()
            .insert("write/pipelined(32)".to_owned(), summary(120_000.0, 2_000_000.0));
        let md = with_pipelined.markdown();
        assert!(md.contains("| fluxum | write/pipelined(32) | 120000"));
        assert!(md.contains("fluxum-only NFR-01 evidence row"));
        assert!(md.contains("feeds no ratio"));
    }
}
