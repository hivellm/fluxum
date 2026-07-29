//! `fluxum-bench fanout-burst` — the thin command wrapper over
//! [`fluxum_bench::fanout::run_fanout_burst`]: boot the release server,
//! run the profile, print the summary, persist the JSON report. The driver
//! itself lives in the library so the smoke test covers the same path.

use super::*;

use fluxum_bench::fanout::{FanoutConfig, run_fanout_burst};

pub(super) fn run_fanout_burst_command(opts: &Opts) -> Result<(), String> {
    let cfg = FanoutConfig {
        subscribers: opts.subscribers.max(1),
        writers: opts.clients.max(1),
        rate: opts.rate.max(1),
        duration: Duration::from_secs(opts.duration_secs.max(5)),
    };
    let server = BenchServer::start_with(None)?;
    println!(
        "== fanout: {} commits/s in {}-commit bursts, {} subscribers, {}s ==",
        cfg.rate,
        cfg.writers,
        cfg.subscribers,
        cfg.duration.as_secs()
    );
    let report = run_fanout_burst(&cfg, &server.url, server.http_port)?;

    println!("{}", report.summary_line());
    if let Some(out) = &opts.out {
        let path = report.persist(out)?;
        println!("report at {}", path.display());
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
        );
    }
    if report.deliveries < report.expected {
        return Err(format!(
            "fanout: only {} of {} deliveries arrived within the drain window",
            report.deliveries, report.expected
        ));
    }
    Ok(())
}
