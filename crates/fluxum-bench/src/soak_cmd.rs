//! The `soak` and `droplet` subcommands (T7.7; TST-110/111/112) — split
//! from `main.rs` to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

/// T7.7 (NFR-12/NFR-13; SPEC-013 TST-110/111/112, SPEC-015): the billion-row
/// and small-droplet soak. Boots a sharded, tiered server, loads `--rows`, then
/// sustains writes and live subscriptions for `--duration-secs` while sampling
/// the server's RSS, and writes the soak report artifact. Self-hosts the
/// server (the memory assertion needs the child PID), so `--url` is rejected.
///
/// The launch-defining runs are the operator's: a billion rows for an hour on a
/// large box, and the droplet profile
/// (`--memory-budget 512MiB --shards 1` on a 1 vCPU / 512 MB instance). The
/// driver + report here are validated at small scale by `tests/soak_smoke.rs`.
pub(super) fn run_soak_command(opts: &Opts) -> Result<(), String> {
    use fluxum_bench::soak::{SoakConfig, parse_bytesize, run_soak};

    if opts.url.is_some() {
        return Err(
            "soak self-hosts the server (RSS sampling needs the server child PID); omit --url"
                .to_owned(),
        );
    }
    let budget_str = opts
        .memory_budget
        .clone()
        .unwrap_or_else(|| "512MiB".to_owned());
    let budget_bytes = parse_bytesize(&budget_str)?;

    let server = BenchServer::start_soak(Some(budget_str.clone()), opts.shards)?;
    let side = FluxumSide::new(server.url.clone());
    let pid = server.child.id();

    let duration = Duration::from_secs(opts.duration_secs.max(1));
    let cfg = SoakConfig {
        rows: u64::from(opts.rows),
        duration,
        connections: opts.clients.max(1),
        pipeline: if opts.pipeline > 1 { opts.pipeline } else { 32 },
        subscribers: if opts.subscribers > 0 {
            opts.subscribers
        } else {
            8
        },
        budget_bytes,
        tolerance_bytes: 0,
        // ~60 samples over the window, at least one per second.
        sample_interval: Duration::from_secs((opts.duration_secs / 60).max(1)),
        // TST-111 wants the engine's own buffer-pool accounting sampled
        // beside process RSS; the admin listener is where it is published.
        metrics_addr: Some(format!("127.0.0.1:{}", server.http_port)),
        require_eviction: opts.require_eviction,
        enforce_idle_ceiling: opts.enforce_idle_ceiling,
        shards_requested: opts.shards.max(1),
    };
    println!(
        "== soak: {} rows into {} shard(s), budget {budget_str}, sustain {}s \
         ({} writers, {} subscriptions) ==",
        cfg.rows,
        opts.shards.max(1),
        duration.as_secs(),
        cfg.connections,
        cfg.subscribers
    );

    let sampler = rss_sampler(pid);
    let date = opts.date.clone().unwrap_or_else(default_date);
    let report = run_soak(
        &side,
        &cfg,
        &sampler,
        hardware(opts.disk_note.as_deref()),
        env!("CARGO_PKG_VERSION"),
        &date,
    )?;

    let out = opts
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from("docs/reports"));
    report.write_artifacts(&out, "soak-report")?;
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    println!(
        "soak {}: peak RSS {:.1} MiB vs budget {:.0} MiB (+{:.0} tol) | idle {:.1} MiB | \
         writes {:.0}/s | {} sub deliveries — report at {}/soak-report.json",
        if report.pass { "PASS" } else { "FAIL" },
        mib(report.peak_rss_bytes),
        mib(report.budget_bytes),
        mib(report.tolerance_bytes),
        mib(report.idle_rss_bytes),
        report.write.throughput_mean,
        report.subscription_deliveries,
        out.display()
    );
    // The TST-111/112 witnesses, named individually — a bare FAIL on an
    // hours-long run should not send the operator digging through JSON to
    // find which criterion broke.
    println!(
        "  witnesses: eviction engaged {} ({}) | idle < 100 MB {} ({}) | shards within pool \
         capacity {}/{}",
        yes_no(report.eviction_engaged),
        if report.eviction_required {
            "required"
        } else {
            "informational"
        },
        yes_no(report.idle_rss_ok),
        if report.idle_ceiling_enforced {
            "enforced"
        } else {
            "informational"
        },
        report
            .shard_pools
            .iter()
            .filter(|s| s.within_capacity)
            .count(),
        report.shard_pools.len(),
    );
    if report.shard_pools.is_empty() {
        println!(
            "  note: no buffer-pool samples were collected — the TIER-080 gauges could not be \
             scraped, so the TST-111 witnesses above are not evidence."
        );
    }
    if !report.pass {
        return Err("soak did not pass its budget/liveness criteria (see the report)".to_owned());
    }
    // The server assembles `sharding.shards` hosts and refuses a count it
    // cannot honour, so requested and observed can only disagree when the
    // metrics scrape missed shards — a broken run, not a caveat. (This was
    // a warning while multi-shard hosting did not exist; now that it does,
    // a soak that did not shard must not read as a TST-112 input.)
    if report.shards_observed < report.shards_requested {
        return Err(format!(
            "{} shard(s) requested but only {} reported metrics — the run did not observe \
             the sharded deployment it was asked for (TST-112)",
            report.shards_requested, report.shards_observed
        ));
    }
    Ok(())
}

pub(super) fn yes_no(b: bool) -> &'static str {
    if b { "yes" } else { "NO" }
}

/// SPEC-013 TST-110: load the same dataset into a memory-constrained
/// deployment and an unconstrained one, then prove the constrained run's row
/// sets are identical to the reference's.
///
/// Two servers, run one after the other rather than side by side, so the
/// constrained one is not competing for the page cache with a roommate
/// holding the whole dataset in RAM.
pub(super) fn run_droplet_command(opts: &Opts) -> Result<(), String> {
    use fluxum_bench::droplet::{
        DropletConfig, DropletReport, diff_rows, droplet_pass, load_and_read, ten_x_dataset,
    };
    use fluxum_bench::soak::parse_bytesize;

    if opts.url.is_some() {
        return Err(
            "droplet self-hosts both servers (it needs to set each one's budget); omit --url"
                .to_owned(),
        );
    }
    let budget_str = opts
        .memory_budget
        .clone()
        .unwrap_or_else(|| "256MiB".to_owned());
    let budget_bytes = parse_bytesize(&budget_str)?;
    // The server refuses to boot below its configured floor, and it does so
    // by simply never binding — which surfaces here as an opaque "did not
    // bind" 20 seconds later. Say what is actually wrong instead.
    const MIN_BUDGET: u64 = 128 << 20;
    if budget_bytes < MIN_BUDGET {
        return Err(format!(
            "--memory-budget {budget_str} is below the server's {} MiB floor \
             (config::MIN_MEMORY_BUDGET); it would refuse to start",
            MIN_BUDGET / (1024 * 1024)
        ));
    }
    let cfg = DropletConfig {
        users: opts.users.max(1),
        rows_per_user: opts.rows.max(1),
        budget_bytes,
        cgroup_enforced: opts.cgroup_enforced,
    };
    println!(
        "== droplet validation: {} users x {} rows, constrained budget {budget_str} ==",
        cfg.users, cfg.rows_per_user
    );
    if !cfg.cgroup_enforced {
        println!(
            "   note: --cgroup-enforced not set, so this run exercises TST-110 but does not \
             validate NFR-12 (that needs a cgroup-constrained host)."
        );
    }

    // The run under test: a budget far below the dataset, so pages fault and
    // evict continuously while the reads happen.
    println!("-- constrained run ({budget_str}) --");
    let constrained = {
        let server = BenchServer::start_with(Some(budget_str.clone()))?;
        let side = FluxumSide::new(server.url.clone());
        let rows = load_and_read(&side, &cfg)?;
        // The cold tier on disk is the witness that the dataset really did
        // exceed the budget — a ratio computed from row counts would be a
        // guess.
        let dataset_bytes = dir_bytes(&server.data_dir);
        (rows, dataset_bytes)
    };
    let (constrained_rows, dataset_bytes) = constrained;

    // The oracle: the same dataset with room to spare, so nothing is ever
    // evicted and every read is served from memory.
    println!("-- reference run (unconstrained) --");
    let reference_rows = {
        let server = BenchServer::start_with(None)?;
        let side = FluxumSide::new(server.url.clone());
        load_and_read(&side, &cfg)?
    };

    let diffs: Vec<_> = constrained_rows
        .iter()
        .zip(&reference_rows)
        .enumerate()
        .map(|(user, (c, r))| diff_rows(u32::try_from(user).unwrap_or(u32::MAX), c, r))
        .collect();
    let row_sets_equal = diffs.iter().all(fluxum_bench::droplet::UserDiff::equal);
    let ten_x = ten_x_dataset(dataset_bytes, budget_bytes);
    let report = DropletReport {
        harness_version: env!("CARGO_PKG_VERSION").to_owned(),
        date: opts.date.clone().unwrap_or_else(default_date),
        hardware: hardware(opts.disk_note.as_deref()),
        users: cfg.users,
        rows_per_user: cfg.rows_per_user,
        budget_bytes,
        dataset_bytes,
        dataset_over_budget: if budget_bytes > 0 {
            dataset_bytes as f64 / budget_bytes as f64
        } else {
            0.0
        },
        ten_x_dataset: ten_x,
        cgroup_enforced: cfg.cgroup_enforced,
        users_compared: diffs.clone(),
        row_sets_equal,
        pass: droplet_pass(&diffs, ten_x),
    };

    let out = opts
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from("docs/reports"));
    report.write_artifacts(&out, "droplet-report")?;
    println!(
        "droplet {}: row sets {} | dataset {}x budget ({}) — report at {}/droplet-report.json",
        if report.pass { "PASS" } else { "FAIL" },
        if row_sets_equal {
            "equal to the reference"
        } else {
            "DIVERGED"
        },
        fluxum_bench::droplet::format_ratio(report.dataset_over_budget),
        if ten_x {
            "clears 10x"
        } else {
            "BELOW the 10x bar"
        },
        out.display()
    );
    if !report.pass {
        return Err("droplet validation did not pass (see the report)".to_owned());
    }
    Ok(())
}

/// Total bytes of every file under `dir`, recursively. Unreadable entries
/// are skipped: a partial figure understates the dataset, which can only
/// make the 10× assertion harder to clear, never easier.
pub(super) fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(m) if m.is_dir() => dir_bytes(&entry.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

/// A closure reading the RSS (bytes) of the server child `pid`, cross-platform
/// via `sysinfo` (Linux + Windows). A fresh `System` per sample is fine at the
/// soak's ~minute cadence.
pub(super) fn rss_sampler(pid: u32) -> impl Fn() -> u64 + Send + Sync {
    move || {
        let mut sys = sysinfo::System::new();
        let target = sysinfo::Pid::from_u32(pid);
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[target]), true);
        sys.process(target).map_or(0, sysinfo::Process::memory)
    }
}
