//! Measurement core (TST-091): latency recording, percentiles, and
//! multi-run summaries with variance.
//!
//! The honesty rules make variance a first-class output: every published
//! number is the product of multiple runs, and a result whose runs disagree
//! wildly is reported as exactly that rather than averaged into confidence.
//! Nothing here knows what was measured — workloads produce [`RunResult`]s,
//! this module reduces them.

use std::time::Duration;

/// One run's raw output: how many operations completed, how long the
/// measured window was, and every operation's latency.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Operations completed inside the measured window (warmup excluded).
    pub ops: u64,
    /// The measured window's wall-clock length.
    pub wall: Duration,
    /// Per-operation latencies, nanoseconds. Empty for workloads where the
    /// operation has no meaningful per-op latency (pure throughput drains).
    pub latencies_ns: Vec<u64>,
}

impl RunResult {
    /// Operations per second over the measured window.
    #[must_use]
    pub fn throughput(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.ops as f64 / secs
    }
}

/// A percentile over recorded latencies, nanoseconds.
///
/// Nearest-rank on the sorted sample: deterministic, no interpolation to
/// argue about, and exact for the sample sizes a bench run produces.
#[must_use]
pub fn percentile_ns(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * p).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Mean and sample standard deviation of a series.
#[must_use]
pub fn mean_stddev(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if values.len() < 2 {
        return (mean, 0.0);
    }
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    (mean, var.sqrt())
}

/// The reduced, publishable form of one workload on one side: throughput and
/// latency percentiles as mean ± stddev across runs (TST-091 variance).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    /// Number of runs reduced into this summary.
    pub runs: usize,
    /// Mean operations/second across runs.
    pub throughput_mean: f64,
    /// Sample stddev of the per-run throughput.
    pub throughput_stddev: f64,
    /// Mean p50 latency across runs, nanoseconds (0 when not recorded).
    pub p50_ns_mean: f64,
    /// Mean p99 latency across runs, nanoseconds (0 when not recorded).
    pub p99_ns_mean: f64,
    /// Sample stddev of the per-run p99, nanoseconds.
    pub p99_ns_stddev: f64,
    /// Largest single latency seen in any run, nanoseconds.
    pub max_ns: u64,
    /// Total operations across all runs.
    pub total_ops: u64,
}

impl Summary {
    /// Reduce per-run results into the publishable summary.
    #[must_use]
    pub fn from_runs(runs: &[RunResult]) -> Self {
        let throughputs: Vec<f64> = runs.iter().map(RunResult::throughput).collect();
        let (throughput_mean, throughput_stddev) = mean_stddev(&throughputs);

        let mut p50s = Vec::new();
        let mut p99s = Vec::new();
        let mut max_ns = 0u64;
        for run in runs {
            if run.latencies_ns.is_empty() {
                continue;
            }
            let mut sorted = run.latencies_ns.clone();
            sorted.sort_unstable();
            p50s.push(percentile_ns(&sorted, 0.50) as f64);
            p99s.push(percentile_ns(&sorted, 0.99) as f64);
            max_ns = max_ns.max(*sorted.last().unwrap_or(&0));
        }
        let (p50_ns_mean, _) = mean_stddev(&p50s);
        let (p99_ns_mean, p99_ns_stddev) = mean_stddev(&p99s);

        Summary {
            runs: runs.len(),
            throughput_mean,
            throughput_stddev,
            p50_ns_mean,
            p99_ns_mean,
            p99_ns_stddev,
            max_ns,
            total_ops: runs.iter().map(|r| r.ops).sum(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_nearest_rank_on_the_sorted_sample() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile_ns(&sorted, 0.0), 1);
        assert_eq!(percentile_ns(&sorted, 0.50), 51); // rank round(99*0.5)=50 → value 51
        assert_eq!(percentile_ns(&sorted, 0.99), 99);
        assert_eq!(percentile_ns(&sorted, 1.0), 100);
        assert_eq!(percentile_ns(&[], 0.99), 0);
        assert_eq!(percentile_ns(&[7], 0.99), 7);
    }

    #[test]
    fn mean_and_stddev_match_hand_computation() {
        let (mean, sd) = mean_stddev(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!((mean - 5.0).abs() < 1e-9);
        // Sample stddev of that classic series is ~2.138.
        assert!((sd - 2.138_089_935).abs() < 1e-6, "{sd}");
        assert_eq!(mean_stddev(&[]), (0.0, 0.0));
        assert_eq!(mean_stddev(&[3.0]), (3.0, 0.0));
    }

    #[test]
    fn summary_reduces_runs_with_variance() {
        let run = |ops: u64, ns: Vec<u64>| RunResult {
            ops,
            wall: Duration::from_secs(1),
            latencies_ns: ns,
        };
        let summary = Summary::from_runs(&[
            run(100, vec![10, 20, 30, 40, 1000]),
            run(200, vec![10, 20, 30, 40, 2000]),
        ]);
        assert_eq!(summary.runs, 2);
        assert_eq!(summary.total_ops, 300);
        assert!((summary.throughput_mean - 150.0).abs() < 1e-9);
        assert!(summary.throughput_stddev > 0.0);
        assert_eq!(summary.max_ns, 2000);
        assert!(summary.p99_ns_mean > 0.0);
    }

    #[test]
    fn throughput_handles_a_zero_window() {
        let run = RunResult {
            ops: 5,
            wall: Duration::ZERO,
            latencies_ns: Vec::new(),
        };
        assert_eq!(run.throughput(), 0.0);
    }
}
