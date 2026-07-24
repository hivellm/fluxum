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
use fluxum_bench::workload::{
    ColdReadConfig, E2eConfig, HotReadConfig, MixedConfig, RunConfig, Side, cold_read_workload,
    e2e_workload, hot_read_workload, mixed_workload, write_workload,
};

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
    http_port: u16,
    tcp_port: u16,
    data_dir: PathBuf,
}

fn wait_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "server did not bind {port}");
        std::thread::sleep(Duration::from_millis(100));
    }
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

        let child = Self::launch(http_port, tcp_port, &data_dir);
        Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp_port}"),
            http_port,
            tcp_port,
            data_dir,
        }
    }

    fn launch(http_port: u16, tcp_port: u16, data_dir: &Path) -> Child {
        let child = Command::new(server_binary())
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            // Every durable dir must leave the shared checkout: the boot
            // spawns the checkpoint worker (T7.3), and a checkpoint written
            // under a RELATIVE default (./data next to this crate) poisons
            // later runs with cross-binary state.
            .env(
                "FLUXUM_STORAGE_CHECKPOINT_DIR",
                data_dir.join("checkpoints"),
            )
            .env("FLUXUM_STORAGE_PAGE_DIR", data_dir.join("pages"))
            .env("FLUXUM_REPLICATION_ARCHIVE_DIR", data_dir.join("archive"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");
        wait_port(tcp_port);
        child
    }

    /// Crash-and-recover on the same ports and data dir.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = Self::launch(self.http_port, self.tcp_port, &self.data_dir);
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
        pipeline: 1,
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

#[test]
fn pipelined_write_workload_keeps_a_window_in_flight_and_acks_everything() {
    let server = Server::start("write-pipelined");
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = RunConfig {
        clients: 2,
        pipeline: 8,
        warmup: Duration::from_millis(300),
        measure: Duration::from_secs(2),
        runs: 1,
    };
    let runs = write_workload(&side, &cfg).expect("pipelined write workload");
    let summary = Summary::from_runs(&runs);
    assert!(
        summary.throughput_mean > 10.0,
        "pipelined writes should ack continuously: {}",
        summary.throughput_mean
    );
    // Every recorded op resolved to a real ack with a positive latency
    // (which includes the window queueing — the mode's documented caveat).
    assert!(summary.p99_ns_mean > 0.0);
    assert!(summary.total_ops > 0);
}

/// A spawned `fluxum-bench baseline-server` over SQLite — the baseline side
/// with no external database to arrange, so the whole client→app-server→SQL
/// →push→client loop is asserted in CI. The PostgreSQL path differs only in
/// the `Db` arm and the NOTIFY hop, exercised by the report runs.
struct BaselineApp {
    child: Child,
    base_url: String,
    port: u16,
    database_url: String,
}

impl BaselineApp {
    fn start(label: &str) -> BaselineApp {
        let db = std::env::temp_dir().join(format!(
            "fluxum-parity-{label}-{}.sqlite",
            std::process::id()
        ));
        Self::start_on(&format!("sqlite://{}", db.display()))
    }

    /// The same app server over an operator-arranged database URL (the
    /// docker PostgreSQL for the PG-gated smoke).
    fn start_on(database_url: &str) -> BaselineApp {
        let free = || {
            TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let port = free();
        let child = Self::launch(database_url, port);
        BaselineApp {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
            port,
            database_url: database_url.to_owned(),
        }
    }

    fn launch(database_url: &str, port: u16) -> Child {
        let child = Command::new(env!("CARGO_BIN_EXE_fluxum-bench"))
            .args([
                "baseline-server",
                "--database-url",
                database_url,
                "--port",
                &port.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn baseline-server");
        wait_port(port);
        child
    }

    /// Restart the app server over the same database file.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = Self::launch(&self.database_url, self.port);
    }
}

impl Drop for BaselineApp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn baseline_sqlite_runs_all_workloads() {
    let server = BaselineApp::start("smoke");
    let side = BaselineSide::new(server.base_url.clone(), "sqlite");
    assert_eq!(side.name(), "sqlite");

    let write_cfg = RunConfig {
        clients: 2,
        pipeline: 1,
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
    assert_eq!(
        runs[0].ops,
        10 * 3,
        "every message reaches every subscriber"
    );

    let hot_cfg = HotReadConfig {
        clients: 2,
        rows_per_client: 10,
        warmup: Duration::from_millis(200),
        measure: Duration::from_secs(1),
        runs: 1,
    };
    let runs = hot_read_workload(&side, &hot_cfg).expect("baseline hot-read workload");
    assert!(runs[0].ops > 0, "no baseline hot reads measured");
}

/// The PostgreSQL half of the baseline (`Db::Pg` + the real LISTEN/NOTIFY
/// hop) — gated on an operator-arranged docker PG, exactly like the
/// SpacetimeDB smoke: present in the coverage-gate run, skipped where no
/// database exists. Closes the "PG half exercised only by report runs"
/// residual in docs/COVERAGE.md.
#[test]
fn baseline_postgres_runs_all_workloads() {
    let Ok(url) = std::env::var("FLUXUM_BENCH_PG_URL") else {
        eprintln!(
            "skipping: set FLUXUM_BENCH_PG_URL=postgres://fluxum:fluxum@127.0.0.1:15432/parity \
             (docker fluxum-parity-pg) to run the PG baseline smoke"
        );
        return;
    };
    let server = BaselineApp::start_on(&url);
    let side = BaselineSide::new(server.base_url.clone(), "postgres");
    assert_eq!(side.name(), "postgres");

    let write_cfg = RunConfig {
        clients: 2,
        pipeline: 1,
        warmup: Duration::from_millis(200),
        measure: Duration::from_secs(1),
        runs: 1,
    };
    let runs = write_workload(&side, &write_cfg).expect("pg write workload");
    assert!(runs[0].ops > 0, "no acked PG writes");

    // The e2e loop crosses the database: INSERT + pg_notify inside the
    // statement → LISTEN connection → broadcast → WebSocket → callback.
    let e2e_cfg = E2eConfig {
        subscribers: 3,
        rate_per_sec: 20,
        messages: 10,
        warmup_messages: 2,
        runs: 1,
    };
    let runs = e2e_workload(&side, &e2e_cfg).expect("pg e2e workload");
    assert_eq!(
        runs[0].ops,
        10 * 3,
        "every message reaches every subscriber"
    );

    let hot_cfg = HotReadConfig {
        clients: 2,
        rows_per_client: 10,
        warmup: Duration::from_millis(200),
        measure: Duration::from_secs(1),
        runs: 1,
    };
    let runs = hot_read_workload(&side, &hot_cfg).expect("pg hot-read workload");
    assert!(runs[0].ops > 0, "no PG hot reads measured");
}

#[test]
fn hot_read_workload_reads_the_live_view() {
    let server = Server::start("hot");
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = HotReadConfig {
        clients: 2,
        rows_per_client: 10,
        warmup: Duration::from_millis(200),
        measure: Duration::from_secs(1),
        runs: 1,
    };
    let runs = hot_read_workload(&side, &cfg).expect("hot-read workload");
    assert!(runs[0].ops > 0, "no hot reads measured");
    let summary = Summary::from_runs(&runs);
    // In-process lookups: the p99 must sit far under any network round trip.
    assert!(
        summary.p99_ns_mean < 1_000_000.0,
        "in-process hot read p99 {} ns is suspiciously slow",
        summary.p99_ns_mean
    );
}

#[test]
fn cold_read_workload_survives_a_restart_on_both_sides() {
    // Plumbing assertion, not a cold-tier measurement: the dataset is tiny
    // and fits any budget. What must hold: the seed survives the restart
    // (recovery), fresh sessions read ALL their rows, and per-load
    // latencies are recorded. The real page-in numbers come from the
    // release harness run with --memory-budget below the dataset size.
    let cfg = ColdReadConfig {
        users: 4,
        rows_per_user: 5,
        sample_users: 4,
        runs: 2,
    };

    let mut server = Server::start("cold-fluxum");
    let side = FluxumSide::new(server.tcp_url.clone());
    let restart = {
        let server = std::cell::RefCell::new(&mut server);
        move || -> Result<(), String> {
            server.borrow_mut().restart();
            Ok(())
        }
    };
    let runs = cold_read_workload(&side, &restart, &cfg).expect("fluxum cold workload");
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].ops, 4);
    assert_eq!(runs[0].latencies_ns.len(), 4);

    let mut baseline = BaselineApp::start("cold");
    let side = BaselineSide::new(baseline.base_url.clone(), "sqlite");
    let restart = {
        let baseline = std::cell::RefCell::new(&mut baseline);
        move || -> Result<(), String> {
            baseline.borrow_mut().restart();
            Ok(())
        }
    };
    let runs = cold_read_workload(&side, &restart, &cfg).expect("sqlite cold workload");
    assert_eq!(runs[0].ops, 4);
}

#[test]
fn mixed_workload_reports_every_class() {
    let server = Server::start("mixed");
    let side = FluxumSide::new(server.tcp_url.clone());
    let cfg = MixedConfig {
        writers: 2,
        readers: 2,
        rows_per_reader: 10,
        subscribers: 3,
        rate_per_sec: 15,
        warmup: Duration::from_millis(300),
        measure: Duration::from_secs(2),
        runs: 1,
    };
    let runs = mixed_workload(&side, &cfg).expect("mixed workload");
    assert_eq!(runs.len(), 1);
    assert!(runs[0].write.ops > 0, "mixed run acked no writes");
    assert!(runs[0].read.ops > 0, "mixed run measured no reads");
    assert!(runs[0].e2e.ops > 0, "mixed run delivered no messages");
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
