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
    E2eConfig, HotReadConfig, MixedConfig, RunConfig, Side, e2e_workload, hot_read_workload,
    mixed_workload, write_workload,
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
            "--subscribers" => opts.subscribers = parse(&value("--subscribers")?)?,
            "--rate" => opts.rate = parse(&value("--rate")?)?,
            "--messages" => opts.messages = parse(&value("--messages")?)?,
            "--json" => opts.json = Some(PathBuf::from(value("--json")?)),
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

    let ms = |ns: f64| ns / 1_000_000.0;
    for (class, summary) in &summaries {
        println!(
            "{} / {class}: {:.0} ops/s (±{:.0}) | p50 {:.4} ms | p99 {:.4} ms (±{:.4}) | max {:.3} ms | {} ops over {} runs",
            side.name(),
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

    if let Some(path) = &opts.json {
        let doc = serde_json::json!({
            "harness_version": fluxum_bench::harness_version(),
            "side": side.name(),
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
    subscribers: usize,
    rate: u32,
    messages: u32,
    json: Option<PathBuf>,
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
            subscribers: 50,
            rate: 10,
            messages: 100,
            json: None,
        }
    }
}

fn parse<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("cannot parse {value:?} as a number"))
}

fn usage() -> String {
    "usage: fluxum-bench <write|e2e|hot|mixed> [--side fluxum|postgres|sqlite] [--url URL] \
     [--database-url URL] [--clients N] [--warmup-secs N] [--measure-secs N] [--runs N] \
     [--rows N] [--subscribers N] [--rate N] [--messages N] [--max-connections N] [--json PATH]\n\
     \x20      fluxum-bench baseline-server --database-url URL --port N [--max-connections N]"
        .to_owned()
}

// --- Self-hosted baseline app server (postgres/sqlite sides) -----------------

struct BaselineServer {
    base_url: String,
    child: Child,
}

impl BaselineServer {
    /// Spawn `fluxum-bench baseline-server` (this same binary) as its own
    /// process on a free port — the incumbent's app server is a separate
    /// process, and an in-process one would share the driver's CPU.
    fn start(database_url: &str, max_connections: u32) -> Result<Self, String> {
        let port = free_port()?;
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
        Ok(BaselineServer {
            base_url: format!("http://127.0.0.1:{port}"),
            child,
        })
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
}

impl BenchServer {
    fn start() -> Result<Self, String> {
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

        let child = Command::new(&binary)
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", binary.display()))?;
        wait_for_port(tcp_port, Duration::from_secs(20))?;

        Ok(BenchServer {
            url: format!("fluxum://127.0.0.1:{tcp_port}"),
            child,
        })
    }
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
