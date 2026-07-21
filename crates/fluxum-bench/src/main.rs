//! `fluxum-bench` CLI — one documented command per side (TST-096).
//!
//! ```text
//! fluxum-bench write --side fluxum [--url URL] [--clients 8] [--warmup-secs 2]
//!                    [--measure-secs 10] [--runs 3] [--json out.json]
//! fluxum-bench e2e   --side fluxum [--url URL] [--subscribers 50] [--rate 10]
//!                    [--messages 100] [--runs 3] [--json out.json]
//! ```
//!
//! Without `--url` the harness boots its own `fluxum-server` — the RELEASE
//! binary, because publishing numbers from a debug build is dishonest in the
//! other direction — on free ports with the development profile and a fresh
//! temp data dir, and tears it down afterwards. `--url` measures a server
//! you started yourself (any config; the report records it).

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::fluxum_side::FluxumSide;
use fluxum_bench::measure::Summary;
use fluxum_bench::workload::{E2eConfig, RunConfig, Side, e2e_workload, write_workload};

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
            "--clients" => opts.clients = parse(&value("--clients")?)?,
            "--warmup-secs" => opts.warmup_secs = parse(&value("--warmup-secs")?)?,
            "--measure-secs" => opts.measure_secs = parse(&value("--measure-secs")?)?,
            "--runs" => opts.runs = parse(&value("--runs")?)?,
            "--subscribers" => opts.subscribers = parse(&value("--subscribers")?)?,
            "--rate" => opts.rate = parse(&value("--rate")?)?,
            "--messages" => opts.messages = parse(&value("--messages")?)?,
            "--json" => opts.json = Some(PathBuf::from(value("--json")?)),
            other => return Err(format!("unknown flag {other}\n{}", usage())),
        }
    }

    // The side under measurement. The baseline sides land with the next
    // T6.3 items; naming them today keeps the CLI contract stable.
    let (side, _server): (Box<dyn Side>, Option<BenchServer>) = match opts.side.as_str() {
        "fluxum" => match &opts.url {
            Some(url) => (Box::new(FluxumSide::new(url.clone())), None),
            None => {
                let server = BenchServer::start()?;
                (Box::new(FluxumSide::new(server.url.clone())), Some(server))
            }
        },
        "postgres" | "sqlite" => {
            return Err(format!(
                "side {:?} is not wired yet (baseline lands with T6.3 1.2)",
                opts.side
            ));
        }
        other => return Err(format!("unknown side {other:?} (fluxum|postgres|sqlite)")),
    };

    let (runs, config_json) = match workload.as_str() {
        "write" => {
            let cfg = RunConfig {
                clients: opts.clients,
                warmup: Duration::from_secs(opts.warmup_secs),
                measure: Duration::from_secs(opts.measure_secs),
                runs: opts.runs,
            };
            let runs = write_workload(side.as_ref(), &cfg)?;
            (runs, format!("{cfg:?}"))
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
            (runs, format!("{cfg:?}"))
        }
        other => return Err(format!("unknown workload {other:?}\n{}", usage())),
    };

    let summary = Summary::from_runs(&runs);
    let ms = |ns: f64| ns / 1_000_000.0;
    println!(
        "{} / {workload}: {:.0} ops/s (±{:.0}) | p50 {:.3} ms | p99 {:.3} ms (±{:.3}) | max {:.3} ms | {} ops over {} runs",
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

    if let Some(path) = &opts.json {
        let doc = serde_json::json!({
            "harness_version": fluxum_bench::harness_version(),
            "side": side.name(),
            "workload": workload,
            "config": config_json,
            "summary": summary,
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
    clients: usize,
    warmup_secs: u64,
    measure_secs: u64,
    runs: usize,
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
            clients: 8,
            warmup_secs: 2,
            measure_secs: 10,
            runs: 3,
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
    "usage: fluxum-bench <write|e2e> [--side fluxum] [--url URL] [--clients N] \
     [--warmup-secs N] [--measure-secs N] [--runs N] [--subscribers N] [--rate N] \
     [--messages N] [--json PATH]"
        .to_owned()
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
