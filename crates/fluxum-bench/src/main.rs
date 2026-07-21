//! `fluxum-bench` CLI — one documented command per side (TST-096).
//!
//! ```text
//! fluxum-bench <write|e2e> --side fluxum   [--url URL] [...]
//! fluxum-bench <write|e2e> --side postgres --database-url postgres://… [...]
//! fluxum-bench <write|e2e> --side sqlite   [--database-url sqlite://…] [...]
//! fluxum-bench baseline-server --database-url URL --port N [--max-connections N]
//! ```
//!
//! Common knobs: `--clients N --warmup-secs N --measure-secs N --runs N`
//! (write), `--subscribers N --rate N --messages N` (e2e), `--json PATH`.
//!
//! Without `--url` the harness boots the side's server itself: for Fluxum
//! the RELEASE `fluxum-server` beside this binary — never a debug fallback,
//! publishing debug numbers is dishonest in the other direction — and for
//! the baseline a `fluxum-bench baseline-server` child process (the
//! incumbent's app server is a separate process; in-process would share the
//! driver's CPU and undercount it). PostgreSQL itself is external; the
//! documented one-command instance is:
//!
//! ```text
//! docker run --rm -d --name fluxum-parity-pg -e POSTGRES_USER=fluxum \
//!   -e POSTGRES_PASSWORD=fluxum -e POSTGRES_DB=parity -p 15432:5432 postgres:17
//! # → --database-url postgres://fluxum:fluxum@127.0.0.1:15432/parity
//! ```

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::baseline::server::serve_blocking;
use fluxum_bench::baseline_side::BaselineSide;
use fluxum_bench::fluxum_side::FluxumSide;
use fluxum_bench::measure::Summary;
use fluxum_bench::workload::{
    ColdReadConfig, E2eConfig, HotReadConfig, MixedConfig, RunConfig, Side, cold_read_workload,
    e2e_workload, hot_read_workload, mixed_workload, write_workload,
};

fn main() {
    if let Err(e) = run(std::env::args().skip(1).collect()) {
        eprintln!("fluxum-bench: {e}");
        std::process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let mut args = args.into_iter();
    let workload = args.next().ok_or_else(usage)?;
    let mut opts = Opts::default();
    let mut rest = args.peekable();
    while let Some(flag) = rest.next() {
        let mut value = |name: &str| -> Result<String, String> {
            rest.next().ok_or_else(|| format!("{name} needs a value"))
        };
        match flag.as_str() {
            "--side" => opts.side = value("--side")?,
            "--url" => opts.url = Some(value("--url")?),
            "--database-url" => opts.database_url = Some(value("--database-url")?),
            "--port" => opts.port = parse(&value("--port")?)?,
            "--max-connections" => opts.max_connections = parse(&value("--max-connections")?)?,
            "--clients" => opts.clients = parse(&value("--clients")?)?,
            "--warmup-secs" => opts.warmup_secs = parse(&value("--warmup-secs")?)?,
            "--measure-secs" => opts.measure_secs = parse(&value("--measure-secs")?)?,
            "--runs" => opts.runs = parse(&value("--runs")?)?,
            "--rows" => opts.rows = parse(&value("--rows")?)?,
            "--users" => opts.users = parse(&value("--users")?)?,
            "--samples" => opts.samples = parse(&value("--samples")?)?,
            "--memory-budget" => opts.memory_budget = Some(value("--memory-budget")?),
            "--cold-restart-cmd" => opts.cold_restart_cmd = Some(value("--cold-restart-cmd")?),
            "--subscribers" => opts.subscribers = parse(&value("--subscribers")?)?,
            "--rate" => opts.rate = parse(&value("--rate")?)?,
            "--messages" => opts.messages = parse(&value("--messages")?)?,
            "--json" => opts.json = Some(PathBuf::from(value("--json")?)),
            "--out" => opts.out = Some(PathBuf::from(value("--out")?)),
            "--date" => opts.date = Some(value("--date")?),
            "--disk-note" => opts.disk_note = Some(value("--disk-note")?),
            "--current" => opts.current = Some(PathBuf::from(value("--current")?)),
            "--published" => opts.published = Some(PathBuf::from(value("--published")?)),
            "--tolerance" => opts.tolerance = parse(&value("--tolerance")?)?,
            other => return Err(format!("unknown flag {other}\n{}", usage())),
        }
    }

    // Not a measurement: serve the baseline app (spawned by the baseline
    // sides below, or run by hand against a database you manage).
    if workload == "baseline-server" {
        let url = opts
            .database_url
            .ok_or("baseline-server needs --database-url")?;
        return serve_blocking(&url, opts.port, opts.max_connections);
    }

    // TST-095: compare a fresh report's ratios against the published one.
    if workload == "regression" {
        let current = load_report(&opts.current.ok_or("regression needs --current PATH")?)?;
        let published =
            load_report(&opts.published.ok_or("regression needs --published PATH")?)?;
        let violations =
            fluxum_bench::report::regressions(&current.ratios, &published.ratios, opts.tolerance);
        if violations.is_empty() {
            println!(
                "no NFR-11 regression beyond {:.0}% tolerance",
                opts.tolerance * 100.0
            );
            return Ok(());
        }
        for violation in &violations {
            eprintln!("REGRESSION: {violation}");
        }
        return Err(format!("{} NFR-11 ratio(s) regressed", violations.len()));
    }

    // TST-094/TST-096: the full matrix, both sides, one command → the
    // versioned report artifact.
    if workload == "report" {
        return run_report(&opts);
    }

    // Cold reads own their server lifecycle (seed → restart → measure), so
    // they take a different construction path from the steady-state
    // workloads below.
    if workload == "cold" {
        let cfg = ColdReadConfig {
            users: opts.users,
            rows_per_user: opts.rows,
            sample_users: opts.samples,
            runs: opts.runs,
        };
        let (name, runs) = match opts.side.as_str() {
            "fluxum" => (
                "fluxum",
                cold_fluxum(opts.memory_budget.clone(), &cfg)?,
            ),
            "postgres" | "sqlite" => {
                let kind: &'static str = if opts.side == "postgres" {
                    "postgres"
                } else {
                    "sqlite"
                };
                let url = match (kind, opts.database_url.clone()) {
                    ("postgres", Some(url)) => url,
                    ("postgres", None) => {
                        return Err("side postgres needs --database-url".to_owned());
                    }
                    (_, Some(url)) => url,
                    (_, None) => format!(
                        "sqlite://{}",
                        std::env::temp_dir()
                            .join(format!("fluxum-parity-cold-{}.sqlite", std::process::id()))
                            .display()
                    ),
                };
                (
                    kind,
                    cold_baseline(
                        kind,
                        &url,
                        opts.max_connections,
                        opts.cold_restart_cmd.as_deref(),
                        &cfg,
                    )?,
                )
            }
            other => return Err(format!("unknown side {other:?} (fluxum|postgres|sqlite)")),
        };
        return emit(
            name,
            &workload,
            &[("cold".to_owned(), Summary::from_runs(&runs))],
            &format!("{cfg:?}"),
            opts.json.as_deref(),
        );
    }

    // The side under measurement.
    let (side, _server): (Box<dyn Side>, Option<Box<dyn std::any::Any>>) = match opts.side.as_str()
    {
        "fluxum" => match &opts.url {
            Some(url) => (Box::new(FluxumSide::new(url.clone())), None),
            None => {
                let server = BenchServer::start()?;
                (
                    Box::new(FluxumSide::new(server.url.clone())),
                    Some(Box::new(server)),
                )
            }
        },
        "postgres" => {
            let url = opts.database_url.clone().ok_or(
                "side postgres needs --database-url (see the docker one-liner in --help)",
            )?;
            let server = BaselineServer::start(&url, opts.max_connections)?;
            (
                Box::new(BaselineSide::new(server.base_url.clone(), "postgres")),
                Some(Box::new(server)),
            )
        }
        "sqlite" => {
            let url = opts.database_url.clone().unwrap_or_else(|| {
                let path = std::env::temp_dir().join(format!(
                    "fluxum-parity-{}.sqlite",
                    std::process::id()
                ));
                format!("sqlite://{}", path.display())
            });
            let server = BaselineServer::start(&url, opts.max_connections)?;
            (
                Box::new(BaselineSide::new(server.base_url.clone(), "sqlite")),
                Some(Box::new(server)),
            )
        }
        other => return Err(format!("unknown side {other:?} (fluxum|postgres|sqlite)")),
    };

    // Every workload reduces to named (class → Summary) pairs; `write`,
    // `e2e` and `hot` have one class, `mixed` has three.
    let (summaries, config_json): (Vec<(String, Summary)>, String) = match workload.as_str() {
        "write" => {
            let cfg = RunConfig {
                clients: opts.clients,
                warmup: Duration::from_secs(opts.warmup_secs),
                measure: Duration::from_secs(opts.measure_secs),
                runs: opts.runs,
            };
            let runs = write_workload(side.as_ref(), &cfg)?;
            (
                vec![("write".to_owned(), Summary::from_runs(&runs))],
                format!("{cfg:?}"),
            )
        }
        "e2e" => {
            let cfg = E2eConfig {
                subscribers: opts.subscribers,
                rate_per_sec: opts.rate,
                messages: opts.messages,
                warmup_messages: opts.messages / 10,
                runs: opts.runs,
            };
            let runs = e2e_workload(side.as_ref(), &cfg)?;
            (
                vec![("e2e".to_owned(), Summary::from_runs(&runs))],
                format!("{cfg:?}"),
            )
        }
        "hot" => {
            let cfg = HotReadConfig {
                clients: opts.clients,
                rows_per_client: opts.rows,
                warmup: Duration::from_secs(opts.warmup_secs),
                measure: Duration::from_secs(opts.measure_secs),
                runs: opts.runs,
            };
            let runs = hot_read_workload(side.as_ref(), &cfg)?;
            (
                vec![("hot".to_owned(), Summary::from_runs(&runs))],
                format!("{cfg:?}"),
            )
        }
        "mixed" => {
            let cfg = MixedConfig {
                writers: opts.clients,
                readers: opts.clients,
                rows_per_reader: opts.rows,
                subscribers: opts.subscribers,
                rate_per_sec: opts.rate,
                warmup: Duration::from_secs(opts.warmup_secs),
                measure: Duration::from_secs(opts.measure_secs),
                runs: opts.runs,
            };
            let runs = mixed_workload(side.as_ref(), &cfg)?;
            let class = |pick: fn(&fluxum_bench::workload::MixedRun) -> &fluxum_bench::measure::RunResult| {
                runs.iter().map(pick).cloned().collect::<Vec<_>>()
            };
            (
                vec![
                    ("mixed/write".to_owned(), Summary::from_runs(&class(|r| &r.write))),
                    ("mixed/read".to_owned(), Summary::from_runs(&class(|r| &r.read))),
                    ("mixed/e2e".to_owned(), Summary::from_runs(&class(|r| &r.e2e))),
                ],
                format!("{cfg:?}"),
            )
        }
        other => return Err(format!("unknown workload {other:?}\n{}", usage())),
    };

    emit(
        side.name(),
        &workload,
        &summaries,
        &config_json,
        opts.json.as_deref(),
    )
}

/// Print the per-class summaries and (optionally) write the JSON artifact
/// the report generator consumes.
fn emit(
    side_name: &str,
    workload: &str,
    summaries: &[(String, Summary)],
    config_json: &str,
    json: Option<&std::path::Path>,
) -> Result<(), String> {
    let ms = |ns: f64| ns / 1_000_000.0;
    for (class, summary) in summaries {
        println!(
            "{side_name} / {class}: {:.0} ops/s (±{:.0}) | p50 {:.4} ms | p99 {:.4} ms (±{:.4}) | max {:.3} ms | {} ops over {} runs",
            summary.throughput_mean,
            summary.throughput_stddev,
            ms(summary.p50_ns_mean),
            ms(summary.p99_ns_mean),
            ms(summary.p99_ns_stddev),
            ms(summary.max_ns as f64),
            summary.total_ops,
            summary.runs,
        );
    }

    if let Some(path) = json {
        let doc = serde_json::json!({
            "harness_version": fluxum_bench::harness_version(),
            "side": side_name,
            "workload": workload,
            "config": config_json,
            "summaries": summaries
                .iter()
                .map(|(class, s)| (class.clone(), s.clone()))
                .collect::<std::collections::BTreeMap<_, _>>(),
        });
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&doc).map_err(|e| e.to_string())?,
        )
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
        println!("wrote {}", path.display());
    }
    Ok(())
}

/// TST-094/TST-096: run the full TST-092 matrix on both sides with one
/// command and write the versioned report artifact (JSON + Markdown).
/// Returns `Err` when an NFR-11 target is unmet, AFTER writing the files —
/// the artifact records reality either way.
fn run_report(opts: &Opts) -> Result<(), String> {
    use fluxum_bench::report::{Ratios, Report, StackInfo};
    use std::collections::BTreeMap;

    let database_url = opts.database_url.clone().ok_or(
        "report needs --database-url for the tuned PostgreSQL \
         (docker run --rm -d --name fluxum-parity-pg -e POSTGRES_USER=fluxum \
         -e POSTGRES_PASSWORD=fluxum -e POSTGRES_DB=parity -p 15432:5432 postgres:17)",
    )?;
    let write_cfg = RunConfig {
        clients: opts.clients,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs),
        runs: opts.runs,
    };
    let e2e_cfg = E2eConfig {
        subscribers: opts.subscribers,
        rate_per_sec: opts.rate,
        messages: opts.messages,
        warmup_messages: opts.messages / 10,
        runs: opts.runs,
    };
    let hot_cfg = HotReadConfig {
        clients: opts.clients,
        rows_per_client: opts.rows,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs.min(5)),
        runs: opts.runs,
    };
    let mixed_cfg = MixedConfig {
        writers: opts.clients,
        readers: opts.clients,
        rows_per_reader: opts.rows,
        subscribers: opts.subscribers,
        rate_per_sec: opts.rate,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs),
        runs: opts.runs,
    };
    let cold_cfg = ColdReadConfig {
        users: opts.users,
        rows_per_user: opts.rows,
        sample_users: opts.samples,
        runs: opts.runs,
    };

    // One side's steady-state classes, into (class → Summary).
    let steady = |side: &dyn Side| -> Result<BTreeMap<String, Summary>, String> {
        let mut classes = BTreeMap::new();
        println!("  write…");
        classes.insert(
            "write".to_owned(),
            Summary::from_runs(&write_workload(side, &write_cfg)?),
        );
        println!("  e2e…");
        classes.insert(
            "e2e".to_owned(),
            Summary::from_runs(&e2e_workload(side, &e2e_cfg)?),
        );
        println!("  hot…");
        classes.insert(
            "hot".to_owned(),
            Summary::from_runs(&hot_read_workload(side, &hot_cfg)?),
        );
        println!("  mixed…");
        let mixed = mixed_workload(side, &mixed_cfg)?;
        let pick = |f: fn(&fluxum_bench::workload::MixedRun) -> &fluxum_bench::measure::RunResult| {
            mixed.iter().map(f).cloned().collect::<Vec<_>>()
        };
        classes.insert("mixed/write".to_owned(), Summary::from_runs(&pick(|r| &r.write)));
        classes.insert("mixed/read".to_owned(), Summary::from_runs(&pick(|r| &r.read)));
        classes.insert("mixed/e2e".to_owned(), Summary::from_runs(&pick(|r| &r.e2e)));
        Ok(classes)
    };

    println!("== fluxum ==");
    let mut fluxum_classes = {
        let server = BenchServer::start()?;
        let side = FluxumSide::new(server.url.clone());
        steady(&side)?
    };
    println!("  cold…");
    fluxum_classes.insert(
        "cold".to_owned(),
        Summary::from_runs(&cold_fluxum(opts.memory_budget.clone(), &cold_cfg)?),
    );

    println!("== postgres ==");
    let mut baseline_classes = {
        let server = BaselineServer::start(&database_url, opts.max_connections)?;
        let side = BaselineSide::new(server.base_url.clone(), "postgres");
        steady(&side)?
    };
    println!("  cold…");
    baseline_classes.insert(
        "cold".to_owned(),
        Summary::from_runs(&cold_baseline(
            "postgres",
            &database_url,
            opts.max_connections,
            opts.cold_restart_cmd.as_deref(),
            &cold_cfg,
        )?),
    );

    let ratios = Ratios::from_summaries(&fluxum_classes, &baseline_classes)?;
    let (pg_version, synchronous_commit) = pg_info(&database_url)?;
    let stacks: BTreeMap<String, StackInfo> = [
        (
            "fluxum".to_owned(),
            StackInfo {
                version: format!("fluxum-server {} (release)", fluxum_bench::harness_version()),
                durability: "TXN-004: ReducerResult acked after the commit-log append reaches \
                             the OS (process-crash safe); fsync is async group commit — \
                             ~50 ms OS-crash window (NFR-08)"
                    .to_owned(),
                config: format!(
                    "development profile, memory budget {}",
                    opts.memory_budget.as_deref().unwrap_or("default (auto)")
                ),
            },
        ),
        (
            "postgres".to_owned(),
            StackInfo {
                version: pg_version,
                durability: format!(
                    "synchronous_commit={synchronous_commit} (WAL fsync before commit ack when on)"
                ),
                config: format!(
                    "axum+sqlx app server (own process), pooled prepared statements \
                     (max_connections={}), covering indexes task(owner) and \
                     chat_message(channel,id), LISTEN/NOTIFY fan-out",
                    opts.max_connections
                ),
            },
        ),
    ]
    .into();

    let report = Report {
        harness_version: fluxum_bench::harness_version().to_owned(),
        date: opts.date.clone().unwrap_or_else(default_date),
        hardware: hardware(opts.disk_note.as_deref()),
        stacks,
        workloads: [
            ("fluxum".to_owned(), fluxum_classes),
            ("postgres".to_owned(), baseline_classes),
        ]
        .into(),
        ratios,
    };

    let out_dir = opts
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from("docs/parity"));
    std::fs::create_dir_all(&out_dir).map_err(|e| e.to_string())?;
    let stem = format!("report-v{}", report.harness_version);
    let json_path = out_dir.join(format!("{stem}.json"));
    std::fs::write(
        &json_path,
        serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("writing {}: {e}", json_path.display()))?;
    let md_path = out_dir.join(format!("{stem}.md"));
    std::fs::write(&md_path, report.markdown())
        .map_err(|e| format!("writing {}: {e}", md_path.display()))?;
    println!("wrote {} and {}", json_path.display(), md_path.display());

    let unmet: Vec<String> = report
        .ratios
        .verdicts()
        .into_iter()
        .filter(|(_, _, _, met)| !met)
        .map(|(name, value, target, _)| format!("{name} = {value:.2} (target {target})"))
        .collect();
    for (name, value, target, met) in report.ratios.verdicts() {
        println!(
            "  {} {name}: {value:.2} (target {target})",
            if met { "OK " } else { "MISS" }
        );
    }
    if unmet.is_empty() {
        Ok(())
    } else {
        Err(format!("NFR-11 targets unmet: {}", unmet.join(", ")))
    }
}

/// The PostgreSQL server's version string and `synchronous_commit` setting,
/// recorded in the report (TST-091 durability documentation).
fn pg_info(database_url: &str) -> Result<(String, String), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
            .map_err(|e| format!("postgres connect for version info: {e}"))?;
        let (version,): (String,) = sqlx::query_as("SELECT version()")
            .fetch_one(&pool)
            .await
            .map_err(|e| e.to_string())?;
        let (sync,): (String,) = sqlx::query_as("SHOW synchronous_commit")
            .fetch_one(&pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok((version, sync))
    })
}

/// The machine of record, per TST-094: CPU, cores, RAM, OS. Disk class is
/// operator-stated (`--disk-note`) — an OS API cannot honestly name it.
fn hardware(disk_note: Option<&str>) -> fluxum_bench::report::Hardware {
    let mut sys = sysinfo::System::new();
    sys.refresh_cpu_list(sysinfo::CpuRefreshKind::nothing());
    sys.refresh_memory();
    fluxum_bench::report::Hardware {
        cpu: sys
            .cpus()
            .first()
            .map(|cpu| cpu.brand().trim().to_owned())
            .unwrap_or_else(|| "unknown".to_owned()),
        cores: sys.cpus().len(),
        ram_gib: sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0),
        os: format!(
            "{} {}",
            sysinfo::System::name().unwrap_or_else(|| std::env::consts::OS.to_owned()),
            sysinfo::System::os_version().unwrap_or_default()
        ),
        disk: disk_note.unwrap_or("unstated (pass --disk-note)").to_owned(),
    }
}

/// Today as `YYYY-MM-DD` (UTC), from the system clock — overridable with
/// `--date`. Civil-from-days per Howard Hinnant's algorithm.
fn default_date() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

/// Load a report JSON (the regression guard's inputs).
fn load_report(path: &std::path::Path) -> Result<fluxum_bench::report::Report, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("parsing {}: {e}", path.display()))
}

/// Cold reads on the Fluxum side: a self-hosted release server (optionally
/// under a small `memory.budget`), crash-and-recovered between runs.
fn cold_fluxum(
    memory_budget: Option<String>,
    cfg: &ColdReadConfig,
) -> Result<Vec<fluxum_bench::measure::RunResult>, String> {
    let server = std::sync::Mutex::new(BenchServer::start_with(memory_budget)?);
    let url = server
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .url
        .clone();
    let side = FluxumSide::new(url);
    let restart = || {
        server
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .restart()
    };
    cold_read_workload(&side, &restart, cfg)
}

/// Cold reads on a baseline side. PostgreSQL's caches live in its own
/// process, so restarting only the app server would measure a warm
/// database — the caller must say how to bounce it (`docker restart …`).
/// The app server restarts too: symmetric with the Fluxum side, and for
/// SQLite it IS the database's page cache.
fn cold_baseline(
    kind: &'static str,
    database_url: &str,
    max_connections: u32,
    cold_restart_cmd: Option<&str>,
    cfg: &ColdReadConfig,
) -> Result<Vec<fluxum_bench::measure::RunResult>, String> {
    if kind == "postgres" && cold_restart_cmd.is_none() {
        return Err(
            "postgres cold reads need --cold-restart-cmd, e.g. \
             --cold-restart-cmd \"docker restart fluxum-parity-pg\""
                .to_owned(),
        );
    }
    let server = std::sync::Mutex::new(BaselineServer::start(database_url, max_connections)?);
    let base_url = server
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .base_url
        .clone();
    let side = BaselineSide::new(base_url, kind);
    let restart = || {
        if let Some(cmd) = cold_restart_cmd {
            run_shell(cmd)?;
        }
        server
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .restart()
    };
    cold_read_workload(&side, &restart, cfg)
}

/// Run a caller-supplied shell command (the PostgreSQL cold-restart hook).
fn run_shell(command: &str) -> Result<(), String> {
    let status = if cfg!(windows) {
        Command::new("cmd").args(["/C", command]).status()
    } else {
        Command::new("sh").args(["-c", command]).status()
    }
    .map_err(|e| format!("{command}: {e}"))?;
    if !status.success() {
        return Err(format!("{command}: exit {status}"));
    }
    Ok(())
}

#[derive(Debug)]
struct Opts {
    side: String,
    url: Option<String>,
    database_url: Option<String>,
    port: u16,
    max_connections: u32,
    clients: usize,
    warmup_secs: u64,
    measure_secs: u64,
    runs: usize,
    rows: u32,
    users: u32,
    samples: u32,
    memory_budget: Option<String>,
    cold_restart_cmd: Option<String>,
    subscribers: usize,
    rate: u32,
    messages: u32,
    json: Option<PathBuf>,
    out: Option<PathBuf>,
    date: Option<String>,
    disk_note: Option<String>,
    current: Option<PathBuf>,
    published: Option<PathBuf>,
    tolerance: f64,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            side: "fluxum".to_owned(),
            url: None,
            database_url: None,
            port: 0,
            max_connections: 16,
            clients: 8,
            warmup_secs: 2,
            measure_secs: 10,
            runs: 3,
            rows: 100,
            users: 64,
            samples: 16,
            memory_budget: None,
            cold_restart_cmd: None,
            subscribers: 50,
            rate: 10,
            messages: 100,
            json: None,
            out: None,
            date: None,
            disk_note: None,
            current: None,
            published: None,
            tolerance: 0.2,
        }
    }
}

fn parse<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("cannot parse {value:?} as a number"))
}

fn usage() -> String {
    "usage: fluxum-bench <write|e2e|hot|cold|mixed> [--side fluxum|postgres|sqlite] [--url URL] \
     [--database-url URL] [--clients N] [--warmup-secs N] [--measure-secs N] [--runs N] \
     [--rows N] [--users N] [--samples N] [--memory-budget SIZE] [--cold-restart-cmd CMD] \
     [--subscribers N] [--rate N] [--messages N] [--max-connections N] [--json PATH]\n\
     \x20      fluxum-bench report --database-url URL --cold-restart-cmd CMD [--out DIR] \
     [--date YYYY-MM-DD] [--disk-note TEXT] [workload knobs]\n\
     \x20      fluxum-bench regression --current PATH --published PATH [--tolerance FRAC]\n\
     \x20      fluxum-bench baseline-server --database-url URL --port N [--max-connections N]"
        .to_owned()
}

// --- Self-hosted baseline app server (postgres/sqlite sides) -----------------

struct BaselineServer {
    base_url: String,
    child: Child,
    database_url: String,
    max_connections: u32,
    port: u16,
}

impl BaselineServer {
    /// Spawn `fluxum-bench baseline-server` (this same binary) as its own
    /// process on a free port — the incumbent's app server is a separate
    /// process, and an in-process one would share the driver's CPU.
    fn start(database_url: &str, max_connections: u32) -> Result<Self, String> {
        let port = free_port()?;
        let child = Self::launch(database_url, max_connections, port)?;
        Ok(BaselineServer {
            base_url: format!("http://127.0.0.1:{port}"),
            child,
            database_url: database_url.to_owned(),
            max_connections,
            port,
        })
    }

    fn launch(database_url: &str, max_connections: u32, port: u16) -> Result<Child, String> {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let child = Command::new(exe)
            .args([
                "baseline-server",
                "--database-url",
                database_url,
                "--port",
                &port.to_string(),
                "--max-connections",
                &max_connections.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn baseline-server: {e}"))?;
        wait_for_port(port, Duration::from_secs(20))?;
        Ok(child)
    }

    /// Kill and relaunch on the same port over the same database.
    fn restart(&mut self) -> Result<(), String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = Self::launch(&self.database_url, self.max_connections, self.port)?;
        Ok(())
    }
}

impl Drop for BaselineServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// --- Self-hosted server (the no-`--url` path) --------------------------------

struct BenchServer {
    url: String,
    child: Child,
    binary: PathBuf,
    http_port: u16,
    tcp_port: u16,
    data_dir: PathBuf,
    memory_budget: Option<String>,
}

impl BenchServer {
    fn start() -> Result<Self, String> {
        Self::start_with(None)
    }

    /// Start with an explicit `memory.budget` (the cold-read knob: a budget
    /// smaller than the seeded dataset forces the cold tier into play).
    fn start_with(memory_budget: Option<String>) -> Result<Self, String> {
        let name = if cfg!(windows) {
            "fluxum-server.exe"
        } else {
            "fluxum-server"
        };
        // target/release relative to this binary (both live in target/*).
        let binary = std::env::current_exe()
            .map_err(|e| e.to_string())?
            .parent()
            .map(|dir| dir.join(name))
            .filter(|p| p.exists())
            .ok_or_else(|| {
                format!(
                    "no {name} beside fluxum-bench — build both with: \
                     cargo build --release -p fluxum-server -p fluxum-bench \
                     (or point --url at a server you started)"
                )
            })?;

        let http_port = free_port()?;
        let tcp_port = free_port()?;
        let data_dir = std::env::temp_dir().join(format!("fluxum-bench-{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let child = launch_fluxum(
            &binary,
            http_port,
            tcp_port,
            &data_dir,
            memory_budget.as_deref(),
        )?;
        Ok(BenchServer {
            url: format!("fluxum://127.0.0.1:{tcp_port}"),
            child,
            binary,
            http_port,
            tcp_port,
            data_dir,
            memory_budget,
        })
    }

    /// Kill and relaunch on the same ports over the same data dir: recovery
    /// replays the seed, and every cache starts empty (the cold restart).
    fn restart(&mut self) -> Result<(), String> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = launch_fluxum(
            &self.binary,
            self.http_port,
            self.tcp_port,
            &self.data_dir,
            self.memory_budget.as_deref(),
        )?;
        Ok(())
    }
}

fn launch_fluxum(
    binary: &std::path::Path,
    http_port: u16,
    tcp_port: u16,
    data_dir: &std::path::Path,
    memory_budget: Option<&str>,
) -> Result<Child, String> {
    let mut command = Command::new(binary);
    command
        .env("FLUXUM_PROFILE", "development")
        .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
        .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
        .env("FLUXUM_STORAGE_DATA_DIR", data_dir)
        .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(budget) = memory_budget {
        command.env("FLUXUM_MEMORY_BUDGET", budget);
    }
    let child = command
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", binary.display()))?;
    wait_for_port(tcp_port, Duration::from_secs(20))?;
    Ok(child)
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> Result<u16, String> {
    Ok(TcpListener::bind("127.0.0.1:0")
        .map_err(|e| e.to_string())?
        .local_addr()
        .map_err(|e| e.to_string())?
        .port())
}

fn wait_for_port(port: u16, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("server did not bind {port} in {timeout:?}"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
