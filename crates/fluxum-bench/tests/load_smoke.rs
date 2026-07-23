//! T6.6 smoke: the load + fan-out drivers run end-to-end against a real
//! release `fluxum-server` on a SHORT window — this asserts the plumbing
//! (pipelined writers → counter-delta throughput; subscribers → commit→
//! receipt latency), never the published headline numbers, which only the
//! full 60 s / 1,000-subscriber runs produce.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::fluxum_side::FluxumSide;
use fluxum_bench::load::{
    FanoutConfig, LoadConfig, fanout_latency, live_scrape, parse_reducer_counts, run_load,
    scrape_metrics,
};

fn release_server() -> PathBuf {
    let name = if cfg!(windows) {
        "fluxum-server.exe"
    } else {
        "fluxum-server"
    };
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release")
        .join(name)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server {
    child: Child,
    tcp_url: String,
    http_addr: String,
}

impl Server {
    fn start(label: &str) -> Option<Server> {
        let binary = release_server();
        if !binary.exists() {
            eprintln!("skipping: no release server — run: cargo build --release -p fluxum-server");
            return None;
        }
        let (http, tcp) = (free_port(), free_port());
        let dir = std::env::temp_dir().join(format!("fluxum-load-{label}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let child = Command::new(&binary)
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", dir.join("log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", tcp)).is_err()
            || TcpStream::connect(("127.0.0.1", http)).is_err()
        {
            assert!(Instant::now() < deadline, "server did not bind");
            std::thread::sleep(Duration::from_millis(100));
        }
        Some(Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp}"),
            http_addr: format!("127.0.0.1:{http}"),
        })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn load_driver_measures_throughput_by_the_counter_delta() {
    let Some(server) = Server::start("load") else {
        return;
    };
    let side = FluxumSide::new(server.tcp_url.clone());

    // The scrape path is real: /metrics parses to counts.
    let counts = parse_reducer_counts(&scrape_metrics(&server.http_addr).unwrap());
    assert_eq!(counts.err, 0, "a fresh server has no errored calls");

    let cfg = LoadConfig {
        connections: 4,
        pipeline: 8,
        warmup: Duration::from_millis(500),
        measure: Duration::from_secs(2),
    };
    let result = run_load(&side, &cfg, || live_scrape(&server.http_addr)).unwrap();
    assert!(result.delta.ok > 0, "the counter advanced under load");
    assert_eq!(result.delta.err, 0, "no call errored");
    assert!(result.calls_per_sec > 0.0);
    // 4×8 pipelined for 2 s clears far more than a trickle on any machine.
    assert!(
        result.calls_per_sec > 1_000.0,
        "throughput {:.0}/s is implausibly low",
        result.calls_per_sec
    );
}

#[test]
fn fanout_driver_measures_commit_to_receipt_latency() {
    let Some(server) = Server::start("fanout") else {
        return;
    };
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = FanoutConfig {
        subscribers: 20,
        messages: 10,
        rate_per_sec: 50,
    };
    let result = fanout_latency(&side, &cfg).unwrap();
    assert_eq!(
        result.deliveries, 200,
        "every message reaches every subscriber (20 × 10)"
    );
    // Distinct, ordered percentiles — the fraction-vs-percent bug would
    // collapse these onto max.
    assert!(result.p50_ns <= result.p99_ns);
    assert!(result.p99_ns <= result.max_ns);
    assert!(result.p50_ns > 0, "real positive latencies");
}
