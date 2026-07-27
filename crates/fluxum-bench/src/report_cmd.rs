//! The `report` subcommand (TST-094/096: the full parity matrix → the
//! versioned artifact) — split from `main.rs` to honour the file-size
//! convention.

#[allow(clippy::wildcard_imports)]
use super::*;

/// TST-094/TST-096: run the full TST-092 matrix on both sides with one
/// command and write the versioned report artifact (JSON + Markdown).
/// Returns `Err` when an NFR-11 target is unmet, AFTER writing the files —
/// the artifact records reality either way.
pub(super) fn run_report(opts: &Opts) -> Result<(), String> {
    use fluxum_bench::report::{CompetitiveRatios, Ratios, Report, StackInfo};
    use std::collections::BTreeMap;

    let database_url = opts.database_url.clone().ok_or(
        "report needs --database-url for the tuned PostgreSQL \
         (docker run --rm -d --name fluxum-parity-pg -e POSTGRES_USER=fluxum \
         -e POSTGRES_PASSWORD=fluxum -e POSTGRES_DB=parity -p 15432:5432 postgres:17)",
    )?;
    // F-011 (parity-report-honesty): the versioned artifact never ships on
    // fewer than 5 runs per class — single workloads keep the CLI default,
    // but the REPORT's verdicts must be distinguishable from noise.
    let runs = opts.runs.max(5);
    let write_cfg = RunConfig {
        clients: opts.clients,
        pipeline: 1,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs),
        runs,
    };
    let e2e_cfg = E2eConfig {
        subscribers: opts.subscribers,
        rate_per_sec: opts.rate,
        messages: opts.messages,
        warmup_messages: opts.messages / 10,
        runs,
    };
    let hot_cfg = HotReadConfig {
        clients: opts.clients,
        rows_per_client: opts.rows,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs.min(5)),
        runs,
    };
    let mixed_cfg = MixedConfig {
        writers: opts.clients,
        readers: opts.clients,
        rows_per_reader: opts.rows,
        subscribers: opts.subscribers,
        rate_per_sec: opts.rate,
        warmup: Duration::from_secs(opts.warmup_secs),
        measure: Duration::from_secs(opts.measure_secs),
        runs,
    };
    let cold_cfg = ColdReadConfig {
        users: opts.users,
        rows_per_user: opts.rows,
        sample_users: opts.samples,
        runs,
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
        let pick =
            |f: fn(&fluxum_bench::workload::MixedRun) -> &fluxum_bench::measure::RunResult| {
                mixed.iter().map(f).cloned().collect::<Vec<_>>()
            };
        classes.insert(
            "mixed/write".to_owned(),
            Summary::from_runs(&pick(|r| &r.write)),
        );
        classes.insert(
            "mixed/read".to_owned(),
            Summary::from_runs(&pick(|r| &r.read)),
        );
        classes.insert(
            "mixed/e2e".to_owned(),
            Summary::from_runs(&pick(|r| &r.e2e)),
        );
        Ok(classes)
    };

    // Equal data footing: the docker PostgreSQL persists across runs while
    // every Fluxum server starts on a fresh dir — start both sides empty.
    truncate_baseline(&database_url)?;

    println!("== fluxum ==");
    let mut fluxum_classes = {
        let server = BenchServer::start()?;
        let side = FluxumSide::new(server.url.clone());
        let mut classes = steady(&side)?;
        // F-007 / NFR-01 evidence row, fluxum-only: the same acked write
        // with a window of calls in flight per connection. The incumbent's
        // request/response app-server protocol has no in-connection
        // pipeline — its concurrency lever (connection count) is already
        // the write row — so this class deliberately has no baseline
        // counterpart and feeds NO ratio.
        println!("  write/pipelined…");
        let pipelined_cfg = RunConfig {
            pipeline: REPORT_PIPELINE_WINDOW,
            ..write_cfg.clone()
        };
        classes.insert(
            format!("write/pipelined({REPORT_PIPELINE_WINDOW})"),
            Summary::from_runs(&write_workload(&side, &pipelined_cfg)?),
        );
        classes
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
    // The cold dataset is exactly users × rows_per_user on BOTH sides: the
    // Fluxum cold server is fresh by construction, so the baseline resets
    // too — otherwise it would carry the steady phases' rows into the
    // measurement.
    truncate_baseline(&database_url)?;
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

    // The competitive baseline (TST-097): same machine, same workloads,
    // reset to an empty database before its steady phases and again before
    // cold (equal data footing — the standalone persists in its volume).
    let stdb_classes = match &opts.stdb_url {
        None => {
            println!("== spacetimedb == skipped (no --stdb-url; report will omit TST-097)");
            None
        }
        Some(url) => {
            let reset = opts.stdb_reset_cmd.as_deref().ok_or(
                "report with --stdb-url needs --stdb-reset-cmd (the SpacetimeDB \
                 database persists in its volume; the documented reset republishes \
                 the module with -c always — see docs/parity/spacetimedb-baseline.md)",
            )?;
            println!("== spacetimedb ==");
            run_shell(reset)?;
            stdb_ready(url, &opts.stdb_db)?;
            let side = SpacetimeDbSide::new(url.clone(), opts.stdb_db.clone());
            let mut classes = steady(&side)?;
            println!("  cold…");
            run_shell(reset)?;
            stdb_ready(url, &opts.stdb_db)?;
            classes.insert(
                "cold".to_owned(),
                Summary::from_runs(&cold_spacetimedb(
                    url,
                    &opts.stdb_db,
                    opts.stdb_restart_cmd.as_deref(),
                    &cold_cfg,
                )?),
            );
            Some(classes)
        }
    };

    let ratios = Ratios::from_summaries(&fluxum_classes, &baseline_classes)?;
    let competitive = stdb_classes
        .as_ref()
        .map(|classes| CompetitiveRatios::from_summaries(&fluxum_classes, classes))
        .transpose()?;
    let (pg_version, synchronous_commit) = pg_info(&database_url)?;
    let mut stacks: BTreeMap<String, StackInfo> = [
        (
            "fluxum".to_owned(),
            StackInfo {
                version: format!(
                    "fluxum-server {} (release)",
                    fluxum_bench::harness_version()
                ),
                durability: "TXN-004: ReducerResult acked after the commit-log append reaches \
                             the OS (process-crash safe); fsync is async group commit — \
                             ~50 ms OS-crash window (NFR-08)"
                    .to_owned(),
                config: format!(
                    "development profile, memory budget {}{}",
                    opts.memory_budget.as_deref().unwrap_or("default (auto)"),
                    pin_note(opts)
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
                     chat_message(channel,id), LISTEN/NOTIFY fan-out{}",
                    opts.max_connections,
                    pin_note(opts)
                ),
            },
        ),
    ]
    .into();
    if stdb_classes.is_some() {
        stacks.insert(
            "spacetimedb".to_owned(),
            StackInfo {
                version: opts.stdb_note.clone().unwrap_or_else(|| {
                    "clockworklabs/spacetime:v2.6.1 (standalone, pinned)".to_owned()
                }),
                durability: "reducer acked at in-memory commit, BEFORE the commit-log \
                             append: durability is a background actor batching appends \
                             and fsyncing per batch (group commit) — a process or OS \
                             crash can lose acked transactions since the last sync \
                             (spacetimedb-durability v2.6.1, imp::local). Weaker ack \
                             than Fluxum's TXN-004 (append reaches the OS pre-ack)"
                    .to_owned(),
                config: "demo module 1:1 (spacetimedb-module/, spacetimedb =2.6.1 wasm), \
                         client spacetimedb-sdk =2.6.1 over WebSocket; task visibility \
                         via RLS owner filter (:sender); btree indexes task.owner and \
                         chat_message.channel; send_chat budget table in-module (Fluxum \
                         enforces the same 20/s pre-transaction, RED-050)"
                    .to_owned(),
            },
        );
    }

    let mut workloads: BTreeMap<String, BTreeMap<String, Summary>> = [
        ("fluxum".to_owned(), fluxum_classes),
        ("postgres".to_owned(), baseline_classes),
    ]
    .into();
    if let Some(classes) = stdb_classes {
        workloads.insert("spacetimedb".to_owned(), classes);
    }

    let report = Report {
        harness_version: fluxum_bench::harness_version().to_owned(),
        date: opts.date.clone().unwrap_or_else(default_date),
        hardware: hardware(opts.disk_note.as_deref()),
        stacks,
        workloads,
        ratios,
        competitive,
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
    // TST-097 is informational (the parity target to REACH), never an exit
    // code: the NFR-11 gate and the competitive baseline must not pollute
    // each other.
    if let Some(competitive) = &report.competitive {
        for (name, value, reached) in competitive.verdicts() {
            println!(
                "  {} competitive {name}: {value:.2} (target ≥ 1.0)",
                if reached { "OK  " } else { "GAP " }
            );
        }
    }
    if unmet.is_empty() {
        Ok(())
    } else {
        Err(format!("NFR-11 targets unmet: {}", unmet.join(", ")))
    }
}
