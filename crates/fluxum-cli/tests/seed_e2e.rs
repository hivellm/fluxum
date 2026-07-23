//! `fluxum seed` against the real server (SPEC-024 DEV-040): a fixture's
//! reducer calls land through the admin surface, in order, through the full
//! production admission path — and a failing call stops the run instead of
//! seeding a silent half-state.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use fluxum_cli::{CliError, post_path, seed};

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

fn skip() -> bool {
    if server_binary().exists() {
        return false;
    }
    eprintln!("skipping: no server binary — run: cargo build -p fluxum-server");
    true
}

struct Server {
    child: Child,
    admin: String,
}

impl Server {
    fn start(label: &str) -> Self {
        let free = |listener: TcpListener| listener.local_addr().unwrap().port();
        let http = free(TcpListener::bind("127.0.0.1:0").unwrap());
        let tcp = free(TcpListener::bind("127.0.0.1:0").unwrap());
        let dir = std::env::temp_dir().join(format!("fluxum-seed-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let child = Command::new(server_binary())
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", dir.join("log"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", http)).is_err() {
            assert!(Instant::now() < deadline, "server did not bind");
            std::thread::sleep(Duration::from_millis(100));
        }
        Server {
            child,
            admin: format!("127.0.0.1:{http}"),
        }
    }

    /// Rows the admin `/query` returns for `sql`, as the payload JSON.
    fn query(&self, sql: &str) -> serde_json::Value {
        let body = serde_json::json!({ "payload": { "sql": sql } }).to_string();
        let response = post_path(&self.admin, "/query", &body).expect("query");
        serde_json::from_str::<serde_json::Value>(&response)
            .expect("query JSON")
            .get("payload")
            .cloned()
            .expect("query payload")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_fixture(label: &str, text: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("fixture-{label}-{}.json", std::process::id()));
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn a_fixture_seeds_in_order_through_the_admission_path() {
    if skip() {
        return;
    }
    let server = Server::start("apply");
    let fixture = write_fixture(
        "apply",
        r#"{ "calls": [
            { "reducer": "add_task", "args": ["first"] },
            { "reducer": "send_chat", "args": [1, "hello"], "repeat": 3 },
            { "reducer": "add_task", "args": ["second"] }
        ] }"#,
    );

    let report = seed::run_seed(&server.admin, &fixture).expect("seed");
    assert_eq!(report.applied, 5, "1 + 3 (repeat) + 1");

    let chats = server.query("SELECT * FROM ChatMessage");
    assert_eq!(
        chats.get("rows").and_then(|r| r.as_array()).map(Vec::len),
        Some(3),
        "{chats}"
    );
    let tasks = server.query("SELECT * FROM Task");
    assert_eq!(
        tasks.get("rows").and_then(|r| r.as_array()).map(Vec::len),
        Some(2),
        "{tasks}"
    );
    let _ = std::fs::remove_file(fixture);
}

#[test]
fn a_failing_call_stops_the_run_with_the_servers_own_error() {
    if skip() {
        return;
    }
    let server = Server::start("stop");
    let fixture = write_fixture(
        "stop",
        r#"{ "calls": [
            { "reducer": "add_task", "args": ["applies"] },
            { "reducer": "send_chat", "args": [1, ""] },
            { "reducer": "add_task", "args": ["never reached"] }
        ] }"#,
    );

    let err = seed::run_seed(&server.admin, &fixture).expect_err("empty chat must fail");
    let message = err.to_string();
    assert!(message.contains("send_chat"), "{message}");
    assert!(message.contains("after 1 applied"), "{message}");
    assert!(message.contains("empty"), "server's own error: {message}");

    // Ordered semantics: everything before the failure landed, nothing after.
    let tasks = server.query("SELECT * FROM Task");
    assert_eq!(
        tasks.get("rows").and_then(|r| r.as_array()).map(Vec::len),
        Some(1),
        "{tasks}"
    );
    let _ = std::fs::remove_file(fixture);
}

#[test]
fn an_unknown_reducer_is_the_servers_404_not_a_parser_guess() {
    if skip() {
        return;
    }
    let server = Server::start("unknown");
    let fixture = write_fixture(
        "unknown",
        r#"{ "calls": [ { "reducer": "no_such_reducer" } ] }"#,
    );
    let err = seed::run_seed(&server.admin, &fixture).expect_err("unknown reducer");
    assert!(matches!(err, CliError::Response(_)), "{err}");
    let _ = std::fs::remove_file(fixture);
}
