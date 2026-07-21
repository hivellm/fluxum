//! The `Connection` against the real server (SPEC-011 SDK-050).
//!
//! The corpus runner (conformance.rs) proves protocol-observable behaviour;
//! this pins the client's own surface — typed callbacks, id correlation, the
//! reducer-error mapping — in a couple of focused cases that read at a glance.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fluxum_sdk::protocol::{FluxBinReader, FluxValue};
use fluxum_sdk::{ClientError, Connection, TableSchema};

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
    http_url: String,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

impl Server {
    fn start(label: &str) -> Self {
        let http = free_port();
        let tcp = free_port();
        let dir = std::env::temp_dir().join(format!("fluxum-e2e-{label}-{}", std::process::id()));
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
        while TcpStream::connect(("127.0.0.1", tcp)).is_err()
            || TcpStream::connect(("127.0.0.1", http)).is_err()
        {
            assert!(Instant::now() < deadline, "server did not bind");
            std::thread::sleep(Duration::from_millis(100));
        }
        Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp}"),
            http_url: format!("http://127.0.0.1:{http}"),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// ChatMessage row: (id: U64, sender: Identity, channel: U32, content: Str, sent_at: Timestamp).
fn chat_schema() -> TableSchema {
    TableSchema {
        name: "ChatMessage".into(),
        pk_of_row: Box::new(|row| row[..8].to_vec()), // leading U64 pk
        pk_of_delete: Box::new(|entry| entry[..8].to_vec()),
    }
}

fn skip() -> bool {
    if server_binary().exists() {
        return false;
    }
    eprintln!("skipping: no server binary — run: cargo build -p fluxum-server");
    true
}

#[test]
fn the_client_drives_a_real_session_end_to_end() {
    if skip() {
        return;
    }
    let server = Server::start("drive");
    let db = Connection::connect(&server.tcp_url, b"", [chat_schema()]).expect("connect");
    assert_ne!(db.identity(), [0u8; 32], "the server derived an identity");

    // A typed callback, registered before the rows exist.
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_cb = Arc::clone(&seen);
    db.on(
        "ChatMessage:insert",
        Box::new(move |row, _old| {
            let mut reader = FluxBinReader::new(row);
            reader.read_u64().unwrap(); // id
            reader.read_identity().unwrap(); // sender
            reader.read_u32().unwrap(); // channel
            let content = reader.read_str().unwrap().to_owned();
            seen_cb.lock().unwrap().push(content);
        }),
    );

    db.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");
    assert_eq!(db.cache_size(), 0, "a fresh database starts empty");

    db.call_reducer("send_chat", vec![FluxValue::I64(1), FluxValue::Str("hello".into())])
        .expect("send_chat");

    // The TxUpdate rides the push stream independently of the reducer reply.
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.cache_size() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db.cache_size(), 1, "the row reached the local cache");
    assert_eq!(*seen.lock().unwrap(), vec!["hello".to_owned()], "the callback fired");
}

#[test]
fn the_client_drives_a_real_session_over_streamable_http() {
    // The SAME client surface over `http://` (RPC-004..007): auth via POST,
    // replies in POST response bodies, TxUpdates on the GET push stream.
    if skip() {
        return;
    }
    let server = Server::start("http-drive");
    let db = Connection::connect(&server.http_url, b"", [chat_schema()]).expect("connect");
    assert_ne!(db.identity(), [0u8; 32], "the server derived an identity");

    db.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");
    db.call_reducer("send_chat", vec![FluxValue::I64(1), FluxValue::Str("over http".into())])
        .expect("send_chat");

    let deadline = Instant::now() + Duration::from_secs(5);
    while db.cache_size() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db.cache_size(), 1, "the TxUpdate arrived on the push stream");
}

#[test]
fn an_http_stream_blip_recovers_without_losing_updates() {
    // SPEC-021 CS-021: the push stream dies but the session survives; the
    // client reattaches (resuming from its applied offsets) and a row
    // committed around the outage still reaches the cache.
    if skip() {
        return;
    }
    let server = Server::start("http-blip");
    let alice = Connection::connect(&server.http_url, b"alice", [chat_schema()]).expect("alice");
    alice.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");

    // Kill the read side as a network outage would — the Connection itself
    // stays open and must recover on its own.
    alice.simulate_stream_loss();

    // A second session commits while (or right after) alice's stream is down.
    let bob = Connection::connect(&server.http_url, b"bob", [chat_schema()]).expect("bob");
    bob.call_reducer("send_chat", vec![FluxValue::I64(1), FluxValue::Str("missed?".into())])
        .expect("send_chat");

    let deadline = Instant::now() + Duration::from_secs(10);
    while alice.cache_size() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(alice.cache_size(), 1, "the update survived the stream blip");
}

#[test]
fn a_rejected_reducer_surfaces_as_a_typed_error() {
    if skip() {
        return;
    }
    let server = Server::start("reject");
    let db = Connection::connect(&server.tcp_url, b"", [chat_schema()]).expect("connect");

    // The demo module rejects an empty message with REDUCER_USER_ERROR (5001).
    let err = db
        .call_reducer("send_chat", vec![FluxValue::I64(1), FluxValue::Str(String::new())])
        .unwrap_err();
    match err {
        ClientError::Reducer { code, message, .. } => {
            assert_eq!(code, 5001);
            assert!(message.contains("empty"), "{message}");
        }
        other => panic!("expected a reducer error, got {other:?}"),
    }
}

#[test]
fn resume_offsets_advance_from_real_server_messages() {
    // SPEC-021 CS-020: the client retains the highest applied tx_offset per
    // subscription, fed by the InitialData snapshot and every live TxUpdate.
    // This is the resume bookkeeping wired to the real connection (T6.4 1.3b).
    if skip() {
        return;
    }
    let server = Server::start("resume");
    let db = Connection::connect(&server.tcp_url, b"", [chat_schema()]).expect("connect");

    let ids = db.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");
    let qid = ids[0];
    let snapshot = db.applied_offset(qid).expect("an offset after InitialData");

    db.call_reducer("send_chat", vec![FluxValue::I64(1), FluxValue::Str("one".into())])
        .expect("send_chat");

    // Wait for the TxUpdate to land, then the applied offset must have advanced
    // past the snapshot's.
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.cache_size() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let after = db.applied_offset(qid).expect("an offset after the TxUpdate");
    assert!(
        after > snapshot,
        "the applied offset must advance: {after} !> {snapshot}"
    );
}

#[test]
fn concurrent_reducer_calls_are_correlated_by_id() {
    // RPC-002: with one worker thread per call sharing a connection, each
    // caller must get its OWN outcome, not whoever's reply arrived first.
    if skip() {
        return;
    }
    let server = Server::start("mux");
    let db = Arc::new(Connection::connect(&server.tcp_url, b"", [chat_schema()]).expect("connect"));

    let handles: Vec<_> = ["first", "", "third"]
        .into_iter()
        .map(|content| {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                db.call_reducer(
                    "send_chat",
                    vec![FluxValue::I64(1), FluxValue::Str(content.to_owned())],
                )
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    assert!(results[0].is_ok(), "first");
    assert!(results[1].is_err(), "the empty one, and only it, failed");
    assert!(results[2].is_ok(), "third");
}
