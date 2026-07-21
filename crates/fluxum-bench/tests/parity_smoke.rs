//! Harness smoke test: the write and e2e workloads run end-to-end against a
//! real `fluxum-server` (debug binary — this asserts the plumbing, never the
//! published numbers, which only the release harness run produces).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::baseline_side::BaselineSide;
use fluxum_bench::fluxum_side::FluxumSide;
use fluxum_bench::measure::Summary;
use fluxum_bench::workload::{E2eConfig, RunConfig, Side, e2e_workload, write_workload};

fn server_binary() -> PathBuf {
    let name = if cfg!(windows) {
        "fluxum-server.exe"
    } else {
        "fluxum-server"
    };
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug")
        .join(name)
}

struct Server {
    child: Child,
    tcp_url: String,
}

impl Server {
    fn start(label: &str) -> Server {
        let binary = server_binary();
        assert!(
            binary.exists(),
            "no server binary at {} — run: cargo build -p fluxum-server",
            binary.display()
        );
        let free = || {
            TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let (http_port, tcp_port) = (free(), free());
        let data_dir =
            std::env::temp_dir().join(format!("fluxum-bench-{label}-{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).unwrap();

        let child = Command::new(&binary)
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");

        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", tcp_port)).is_err() {
            assert!(Instant::now() < deadline, "server did not bind {tcp_port}");
            std::thread::sleep(Duration::from_millis(100));
        }
        Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp_port}"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn write_workload_measures_acked_small_writes() {
    let server = Server::start("write");
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = RunConfig {
        clients: 4,
        warmup: Duration::from_millis(300),
        measure: Duration::from_secs(2),
        runs: 2,
    };
    let runs = write_workload(&side, &cfg).expect("write workload");
    assert_eq!(runs.len(), 2);
    let summary = Summary::from_runs(&runs);
    assert!(
        summary.throughput_mean > 10.0,
        "4 clients over 2 s should ack far more than 10 writes/s: {}",
        summary.throughput_mean
    );
    // Ack latency is a real, positive duration on every op.
    assert!(summary.p99_ns_mean > 0.0);
    assert!(summary.total_ops > 0);
}

/// A spawned `fluxum-bench baseline-server` over SQLite — the baseline side
/// with no external database to arrange, so the whole client→app-server→SQL
/// →push→client loop is asserted in CI. The PostgreSQL path differs only in
/// the `Db` arm and the NOTIFY hop, exercised by the report runs.
struct SqliteBaseline {
    child: Child,
    base_url: String,
}

impl SqliteBaseline {
    fn start(label: &str) -> SqliteBaseline {
        let free = || {
            TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let port = free();
        let db = std::env::temp_dir().join(format!(
            "fluxum-parity-{label}-{}.sqlite",
            std::process::id()
        ));
        let child = Command::new(env!("CARGO_BIN_EXE_fluxum-bench"))
            .args([
                "baseline-server",
                "--database-url",
                &format!("sqlite://{}", db.display()),
                "--port",
                &port.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn baseline-server");
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "baseline-server did not bind {port}");
            std::thread::sleep(Duration::from_millis(100));
        }
        SqliteBaseline {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
        }
    }
}

impl Drop for SqliteBaseline {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn baseline_sqlite_runs_both_workloads() {
    let server = SqliteBaseline::start("smoke");
    let side = BaselineSide::new(server.base_url.clone(), "sqlite");
    assert_eq!(side.name(), "sqlite");

    let write_cfg = RunConfig {
        clients: 2,
        warmup: Duration::from_millis(200),
        measure: Duration::from_secs(1),
        runs: 1,
    };
    let runs = write_workload(&side, &write_cfg).expect("baseline write workload");
    assert!(runs[0].ops > 0, "no acked baseline writes");

    let e2e_cfg = E2eConfig {
        subscribers: 3,
        rate_per_sec: 20,
        messages: 10,
        warmup_messages: 2,
        runs: 1,
    };
    let runs = e2e_workload(&side, &e2e_cfg).expect("baseline e2e workload");
    assert_eq!(runs[0].ops, 10 * 3, "every message reaches every subscriber");
}

#[test]
fn e2e_workload_measures_fanout_delivery() {
    let server = Server::start("e2e");
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = E2eConfig {
        subscribers: 5,
        rate_per_sec: 15,
        messages: 20,
        warmup_messages: 3,
        runs: 1,
    };
    let runs = e2e_workload(&side, &cfg).expect("e2e workload");
    assert_eq!(runs.len(), 1);
    // Every measured message reached every subscriber.
    assert_eq!(runs[0].ops, 20 * 5);
    let summary = Summary::from_runs(&runs);
    assert!(summary.p99_ns_mean > 0.0, "deliveries carry real latencies");
}
