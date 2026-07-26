//! T7.7 smoke: the soak driver runs end-to-end against a real release
//! `fluxum-server` on a SHORT window with a tiny dataset — it asserts the
//! plumbing (load → sustained writes + live subscriptions → RSS sampling →
//! within-budget check → report artifact), never the launch-defining
//! billion-row / 1 vCPU-512 MB numbers, which only the operator's real runs
//! produce. Skips gracefully when the release binary is absent.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fluxum_bench::fluxum_side::FluxumSide;
use fluxum_bench::report::Hardware;
use fluxum_bench::soak::{SoakConfig, run_soak};

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

/// The server child's RSS (bytes), cross-platform via `sysinfo` — the same
/// reader the `soak` command uses.
fn rss_of(pid: u32) -> u64 {
    let mut sys = sysinfo::System::new();
    let target = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[target]), true);
    sys.process(target).map_or(0, sysinfo::Process::memory)
}

struct Server {
    child: Child,
    tcp_url: String,
}

impl Server {
    fn start() -> Option<Server> {
        let binary = release_server();
        if !binary.exists() {
            eprintln!("skipping: no release server — run: cargo build --release -p fluxum-server");
            return None;
        }
        let (http, tcp) = (free_port(), free_port());
        let dir = std::env::temp_dir().join(format!("fluxum-soak-smoke-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let child = Command::new(&binary)
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", dir.join("log"))
            .env("FLUXUM_STORAGE_CHECKPOINT_DIR", dir.join("checkpoints"))
            .env("FLUXUM_STORAGE_PAGE_DIR", dir.join("pages"))
            .env("FLUXUM_REPLICATION_ARCHIVE_DIR", dir.join("archive"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", tcp)).is_err() {
            assert!(Instant::now() < deadline, "server did not bind");
            std::thread::sleep(Duration::from_millis(100));
        }
        Some(Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp}"),
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
fn soak_driver_loads_sustains_samples_and_reports() {
    let Some(server) = Server::start() else {
        return;
    };
    let pid = server.child.id();
    let side = FluxumSide::new(server.tcp_url.clone());

    let cfg = SoakConfig {
        rows: 2_000,
        duration: Duration::from_secs(2),
        connections: 2,
        pipeline: 16,
        subscribers: 3,
        // Generous ceiling: the smoke asserts the check RAN and passed on a
        // tiny dataset, not a real 10×-RAM budget.
        budget_bytes: 4 * 1024 * 1024 * 1024,
        tolerance_bytes: 0,
        sample_interval: Duration::from_millis(200),
    };
    let hw = Hardware {
        cpu: "smoke".into(),
        cores: 1,
        ram_gib: 1.0,
        os: "smoke".into(),
        disk: "smoke".into(),
    };
    let report = run_soak(&side, &cfg, &move || rss_of(pid), hw, "smoke", "2026-07-26")
        .expect("soak driver runs");

    assert!(
        !report.rss_samples.is_empty(),
        "RSS sampled over the window"
    );
    assert!(
        report.peak_rss_bytes > 0,
        "the live server has a real resident footprint"
    );
    assert!(report.idle_rss_bytes > 0, "idle baseline was sampled");
    assert!(report.within_budget, "well under the generous smoke budget");
    assert!(report.write.total_ops > 0, "sustained writes happened");
    assert!(
        report.subscription_deliveries > 0,
        "the live subscriptions received the writers' messages"
    );
    assert!(report.pass, "a healthy smoke soak passes");
    assert_eq!(report.rows_loaded, 2_000);

    // The report artifact writes as JSON + Markdown.
    let out = std::env::temp_dir().join(format!("fluxum-soak-report-{}", std::process::id()));
    report.write_artifacts(&out, "soak-report").unwrap();
    assert!(out.join("soak-report.json").exists());
    assert!(out.join("soak-report.md").exists());
    let _ = std::fs::remove_dir_all(&out);
}
