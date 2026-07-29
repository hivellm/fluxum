//! phase9 F-015 smoke: the burst driver runs end-to-end against a real
//! release `fluxum-server` on a SHORT window — it asserts the plumbing
//! (subscribers + writer fleet + rounds + drain + report + OBS-024 scrape),
//! never the attribution numbers, which only the operator's real runs
//! produce. Skips gracefully when the release binary is absent (the
//! `soak_smoke` pattern).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::fanout::{FanoutConfig, run_fanout_burst};

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
    http_port: u16,
}

impl Server {
    fn start() -> Option<Server> {
        let binary = release_server();
        if !binary.exists() {
            eprintln!("skipping: no release server — run: cargo build --release -p fluxum-server");
            return None;
        }
        let (http, tcp) = (free_port(), free_port());
        let data_dir =
            std::env::temp_dir().join(format!("fluxum-fanout-smoke-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&data_dir);
        let child = Command::new(binary)
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", tcp)).is_err() {
            assert!(Instant::now() < deadline, "server did not bind {tcp}");
            std::thread::sleep(Duration::from_millis(100));
        }
        Some(Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp}"),
            http_port: http,
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
fn the_burst_driver_delivers_everything_and_reads_the_batch_factor() {
    let Some(server) = Server::start() else {
        return;
    };
    let cfg = FanoutConfig {
        subscribers: 3,
        writers: 8,
        rate: 80,
        duration: Duration::from_secs(3),
    };
    let report = run_fanout_burst(&cfg, &server.tcp_url, server.http_port).expect("burst run");

    assert!(report.sent > 0, "the window sent commits");
    assert_eq!(
        report.deliveries, report.expected,
        "every commit reached every subscriber (zero loss)"
    );
    assert!(report.e2e_us.p50 > 0, "latency was measured per delivery");
    assert!(report.e2e_us.p99 >= report.e2e_us.p50);
    // The release server carries the OBS-024 counters; the batch factor is
    // at least 1.0 by definition (one frame per write minimum).
    let batch = report
        .frames_per_write
        .expect("OBS-024 counters in the release exposition");
    assert!(batch >= 1.0, "frames/write {batch} < 1.0 is impossible");

    // The command surface: the summary names the essentials and the JSON
    // artifact lands where the operator pointed.
    let line = report.summary_line();
    assert!(line.contains("delivered") && line.contains("frames/write"));
    let out = std::env::temp_dir().join(format!("fluxum-fanout-report-{}", std::process::id()));
    let path = report.persist(&out).expect("persist the artifact");
    let json = std::fs::read_to_string(&path).unwrap();
    assert!(json.contains("\"deliveries\""));
    let _ = std::fs::remove_dir_all(&out);
}

#[test]
fn an_undersized_writer_fleet_is_refused_before_touching_the_server() {
    // 100/s over 2 identities would be 50/s each — past the 20/s admission.
    // The driver refuses up front; no server is needed to prove it.
    let cfg = FanoutConfig {
        subscribers: 1,
        writers: 2,
        rate: 100,
        duration: Duration::from_secs(1),
    };
    let err = run_fanout_burst(&cfg, "fluxum://127.0.0.1:1", 1).unwrap_err();
    assert!(
        err.contains("20/s") && err.contains("raise --clients"),
        "the refusal names the admission ceiling and the fix: {err}"
    );
}
