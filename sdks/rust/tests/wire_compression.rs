//! RPC-008 end to end through the SDK, under the `compression` feature: a
//! `WirePreferences { compression: true }` client negotiates the gzip
//! stream against a real spawned server, and live updates decode through
//! the per-connection inflate context — combined with `light_updates`, the
//! full delta stack (light × stream-deflate) in one session.
#![cfg(feature = "compression")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use fluxum_sdk::protocol::FluxValue;
use fluxum_sdk::{Connection, ReconnectPolicy, TableSchema, WirePreferences};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn server_binary() -> PathBuf {
    let name = if cfg!(windows) {
        "fluxum-server.exe"
    } else {
        "fluxum-server"
    };
    repo_root().join("target/debug").join(name)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "server did not bind {port}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct Server {
    child: Child,
    tcp_url: String,
}

impl Server {
    fn start() -> Self {
        let http_port = free_port();
        let tcp_port = free_port();
        let data_dir = std::env::temp_dir().join(format!("fluxum-wiregz-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&data_dir);
        let child = Command::new(server_binary())
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", &data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn fluxum-server (run: cargo build -p fluxum-server)");
        wait_for_port(tcp_port);
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

/// ChatMessage's pk (`id: u64`) is the first FluxBIN field of the row; the
/// delete entry carries the pk field alone. 8 LE bytes either way.
fn chat_schema() -> TableSchema {
    TableSchema {
        name: "ChatMessage".into(),
        pk_of_row: Box::new(|row| row[..8].to_vec()), // leading U64 pk
        pk_of_delete: Box::new(|entry| entry[..8].to_vec()),
    }
}

#[test]
fn a_gzip_light_session_streams_updates_through_the_inflate_context() {
    let server = Server::start();
    let wire = Connection::connect_wire(
        &server.tcp_url,
        b"gzip-side",
        [chat_schema()],
        ReconnectPolicy::default(),
        WirePreferences {
            light_updates: true,
            compression: true,
        },
    )
    .expect("connect with gzip + light negotiated");
    wire.subscribe(&["SELECT * FROM ChatMessage"]).unwrap();

    // A plain writer commits; the compressed session must see every row
    // through its tagged stream. Bodies well over the 64-byte threshold, so
    // the frames really are tag-0x01 chunks of one deflate context.
    let writer = Connection::connect(&server.tcp_url, b"plain-side", [chat_schema()]).unwrap();
    let body = "the quick brown fox jumps over the lazy dog, at length and in detail, \
                so this frame clears the compression threshold comfortably";
    for i in 0..10u32 {
        writer
            .call_reducer(
                "send_chat",
                vec![FluxValue::I64(1), FluxValue::Str(format!("{body} #{i}"))],
            )
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if wire.rows("ChatMessage").len() >= 10 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {} of 10 rows arrived through the gzip stream",
            wire.rows("ChatMessage").len()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
