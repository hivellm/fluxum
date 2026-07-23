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

/// Task row: (id: U64, owner: Identity, title: Str, …) — leading U64 pk.
fn task_schema() -> TableSchema {
    TableSchema {
        name: "Task".into(),
        pk_of_row: Box::new(|row| row[..8].to_vec()),
        pk_of_delete: Box::new(|entry| entry[..8].to_vec()),
    }
}

#[test]
fn pipelined_calls_resolve_by_id_and_commit_in_submission_order() {
    // SDK-032 (write pipelining, F-007): a window of un-acked calls shares
    // one connection; each `PendingReducer` resolves exactly its own outcome
    // — a failing call in the middle of the window fails alone — and
    // same-connection commits land in submission order.
    if skip() {
        return;
    }
    let server = Server::start("pipeline");
    let db = Connection::connect(&server.tcp_url, b"", [task_schema()]).expect("connect");

    let titles: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&titles);
    db.on(
        "Task:insert",
        Box::new(move |row, _old| {
            let mut reader = FluxBinReader::new(row);
            reader.read_u64().unwrap(); // id
            reader.read_identity().unwrap(); // owner
            sink.lock().unwrap().push(reader.read_str().unwrap().to_owned());
        }),
    );
    db.subscribe(&["SELECT * FROM Task"]).expect("subscribe");

    // Eight adds in flight with a doomed call planted mid-window.
    let mut pending = Vec::new();
    for i in 0..4 {
        pending.push(
            db.call_reducer_async("add_task", vec![FluxValue::Str(format!("t{i}"))])
                .expect("pipelined send"),
        );
    }
    let doomed = db
        .call_reducer_async("no_such_reducer", vec![])
        .expect("pipelined send");
    for i in 4..8 {
        pending.push(
            db.call_reducer_async("add_task", vec![FluxValue::Str(format!("t{i}"))])
                .expect("pipelined send"),
        );
    }

    for p in pending {
        p.wait().expect("every valid call acks Ok");
    }
    assert!(
        doomed.wait().is_err(),
        "the unknown reducer fails exactly its own handle"
    );

    // Same-connection commits deliver in submission order.
    let expected: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
    let deadline = Instant::now() + Duration::from_secs(5);
    while titles.lock().unwrap().len() < expected.len() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(*titles.lock().unwrap(), expected);
}

/// A plausible optimistic `Task` row: temp id + this client's identity +
/// title. The server will re-mint the id — the optimistic row is a stand-in,
/// swapped for the authoritative one when the commit's `TxUpdate` applies.
fn optimistic_task_row(temp_id: u64, owner: [u8; 32], title: &str) -> Vec<u8> {
    let mut writer = fluxum_sdk::protocol::FluxBinWriter::new();
    writer.write_u64(temp_id);
    writer.write_identity(&owner);
    writer.write_str(title).unwrap();
    writer.into_bytes()
}

/// Decode just the title from a full `Task` row.
fn task_title(row: &[u8]) -> String {
    let mut reader = FluxBinReader::new(row);
    reader.read_u64().unwrap(); // id
    reader.read_identity().unwrap(); // owner
    reader.read_str().unwrap().to_owned()
}

#[test]
fn an_optimistic_call_renders_immediately_and_converges() {
    // SPEC-021 CS-010/CS-011: the updater's row shows before any round-trip;
    // once the commit's TxUpdate applies, the cache holds exactly the
    // authoritative row — no duplicate, no lingering temp row.
    if skip() {
        return;
    }
    let server = Server::start("optimistic");
    let db = Connection::connect(&server.tcp_url, b"", [task_schema()]).expect("connect");
    db.subscribe(&["SELECT * FROM Task"]).expect("subscribe");

    let identity = db.identity();
    let key = db
        .call_optimistic("add_task", vec![FluxValue::Str("optimistic".into())], |s| {
            s.insert("Task", optimistic_task_row(u64::MAX, identity, "optimistic"));
        })
        .expect("call_optimistic");
    assert!(!key.is_empty(), "the submission handle is the minted key");

    // Visible NOW — no server round-trip has completed yet (and even if it
    // had, this assertion only requires the row to be there).
    let rows = db.rows("Task");
    assert_eq!(rows.len(), 1, "the optimistic row renders immediately");
    assert_eq!(task_title(&rows[0]), "optimistic");

    // Converge: the overlay drops when the authoritative update applies.
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.pending_mutations() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db.pending_mutations(), 0, "the call resolved");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let rows = db.rows("Task");
        if rows.len() == 1 && task_title(&rows[0]) == "optimistic" {
            let mut reader = FluxBinReader::new(&rows[0]);
            let id = reader.read_u64().unwrap();
            if id != u64::MAX {
                break; // the authoritative row, server-minted id
            }
        }
        assert!(
            Instant::now() < deadline,
            "cache never converged to the authoritative row: {rows:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn a_rejected_optimistic_call_rolls_back_and_reports() {
    // SPEC-021 CS-011: `Err` removes the optimistic row, the cache matches
    // server state exactly, and the rejected listener hears about it.
    if skip() {
        return;
    }
    let server = Server::start("optimistic-reject");
    let db = Connection::connect(&server.tcp_url, b"", [chat_schema()]).expect("connect");
    db.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");

    let rejections: Arc<Mutex<Vec<(String, String, u16)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&rejections);
    db.on_rejected(Box::new(move |reducer, key, err| {
        sink.lock().unwrap().push((reducer.to_owned(), key.to_owned(), err.code));
    }));

    let identity = db.identity();
    // The demo module rejects an empty message (5001) — but the optimistic
    // updater has already rendered it.
    let key = db
        .call_optimistic(
            "send_chat",
            vec![FluxValue::I64(1), FluxValue::Str(String::new())],
            |s| {
                let mut writer = fluxum_sdk::protocol::FluxBinWriter::new();
                writer.write_u64(u64::MAX);
                writer.write_identity(&identity);
                writer.write_u32(1); // channel
                writer.write_str("").unwrap();
                writer.write_timestamp(0);
                s.insert("ChatMessage", writer.into_bytes());
            },
        )
        .expect("call_optimistic");
    assert_eq!(db.rows("ChatMessage").len(), 1, "rendered optimistically");

    let deadline = Instant::now() + Duration::from_secs(5);
    while db.pending_mutations() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(db.rows("ChatMessage").is_empty(), "rolled back on Err");
    let seen = rejections.lock().unwrap();
    assert_eq!(seen.len(), 1, "the rejection listener fired once");
    assert_eq!(seen[0].0, "send_chat");
    assert_eq!(seen[0].1, key);
    assert_eq!(seen[0].2, 5001, "REDUCER_USER_ERROR");
}

/// A fresh file-backend rooted in a per-test temp directory.
fn file_backend(label: &str) -> std::sync::Arc<fluxum_sdk::FileBackend> {
    let dir = std::env::temp_dir().join(format!("fluxum-sdk-persist-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::sync::Arc::new(fluxum_sdk::FileBackend::new(&dir).expect("backend dir"))
}

fn no_reconnect() -> fluxum_sdk::ReconnectPolicy {
    fluxum_sdk::ReconnectPolicy {
        enabled: false,
        ..fluxum_sdk::ReconnectPolicy::default()
    }
}

#[test]
fn persisted_state_hydrates_and_reconciles_across_a_restart() {
    // SPEC-021 CS-040/CS-041: session 1 subscribes and writes; a "restart"
    // (new Connection, same backend + client_id) hydrates the rows, replays
    // the subscription, and reconciles — a row committed WHILE the client
    // was away is already there when connect returns, no explicit subscribe.
    if skip() {
        return;
    }
    let server = Server::start("persist-hydrate");
    let backend = file_backend("hydrate");

    let db = Connection::connect_persistent(
        &server.tcp_url,
        b"tok",
        [task_schema()],
        fluxum_sdk::ReconnectPolicy::default(),
        backend.clone(),
        "cli-1",
    )
    .expect("session 1");
    db.subscribe(&["SELECT * FROM Task"]).expect("subscribe");
    db.call_reducer("add_task", vec![FluxValue::Str("persisted".into())])
        .expect("add_task");
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.rows("Task").is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db.rows("Task").len(), 1, "session 1 converged");
    drop(db);

    // Someone (same identity) commits while "we" are down.
    let writer =
        Connection::connect(&server.tcp_url, b"tok", [task_schema()]).expect("writer");
    writer
        .call_reducer("add_task", vec![FluxValue::Str("while away".into())])
        .expect("add_task while away");
    drop(writer);

    // The restart: hydration + resubscribe + reconcile happen INSIDE
    // connect, so both rows are present the moment it returns.
    let db2 = Connection::connect_persistent(
        &server.tcp_url,
        b"tok",
        [task_schema()],
        fluxum_sdk::ReconnectPolicy::default(),
        backend,
        "cli-1",
    )
    .expect("session 2");
    let mut titles: Vec<String> = db2.rows("Task").iter().map(|r| task_title(r)).collect();
    titles.sort();
    assert_eq!(
        titles,
        vec!["persisted".to_owned(), "while away".to_owned()],
        "hydrated + net difference, no explicit subscribe"
    );

    // And the replayed subscription is LIVE, not a static snapshot.
    db2.call_reducer("add_task", vec![FluxValue::Str("live".into())])
        .expect("add_task live");
    let deadline = Instant::now() + Duration::from_secs(5);
    while db2.rows("Task").len() < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db2.rows("Task").len(), 3, "updates keep flowing");
}

#[test]
fn a_queued_mutation_survives_a_restart_and_replays_once() {
    // CS-041 scenario: queue offline, crash, restart — the call replays
    // under its ORIGINAL key (CS-032) and applies exactly once.
    if skip() {
        return;
    }
    let server = Server::start("persist-queue");
    let backend = file_backend("queue");

    let db = Connection::connect_persistent(
        &server.tcp_url,
        b"tok",
        [task_schema()],
        no_reconnect(), // stay down once the socket dies: a clean "crash"
        backend.clone(),
        "cli-1",
    )
    .expect("session 1");
    db.subscribe(&["SELECT * FROM Task"]).expect("subscribe");
    let identity = db.identity();

    db.simulate_stream_loss();
    db.call_optimistic("add_task", vec![FluxValue::Str("queued".into())], |s| {
        s.insert("Task", optimistic_task_row(u64::MAX, identity, "queued"));
    })
    .expect("queue offline");
    assert_eq!(db.pending_mutations(), 1, "unacknowledged, persisted");
    drop(db);

    let db2 = Connection::connect_persistent(
        &server.tcp_url,
        b"tok",
        [task_schema()],
        fluxum_sdk::ReconnectPolicy::default(),
        backend,
        "cli-1",
    )
    .expect("session 2");
    let deadline = Instant::now() + Duration::from_secs(10);
    while db2.pending_mutations() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db2.pending_mutations(), 0, "the restored queue drained");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let titles: Vec<String> = db2.rows("Task").iter().map(|r| task_title(r)).collect();
        if titles == vec!["queued".to_owned()] {
            break; // exactly once, and the temp row is gone
        }
        assert!(
            Instant::now() < deadline,
            "replay did not converge exactly-once: {titles:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn a_different_identity_discards_the_hydrated_queue() {
    // CS-040 keys state by identity: if another user starts on the same
    // store, the previous user's queued mutations must NOT replay as them.
    if skip() {
        return;
    }
    let server = Server::start("persist-identity");
    let backend = file_backend("identity");

    let alice = Connection::connect_persistent(
        &server.tcp_url,
        b"alice",
        [chat_schema()],
        no_reconnect(),
        backend.clone(),
        "shared-device",
    )
    .expect("alice");
    alice.subscribe(&["SELECT * FROM ChatMessage"]).expect("subscribe");
    let alice_id = alice.identity();
    alice.simulate_stream_loss();
    alice
        .call_optimistic(
            "send_chat",
            vec![FluxValue::I64(1), FluxValue::Str("alice offline".into())],
            |s| {
                let mut writer = fluxum_sdk::protocol::FluxBinWriter::new();
                writer.write_u64(u64::MAX);
                writer.write_identity(&alice_id);
                writer.write_u32(1);
                writer.write_str("alice offline").unwrap();
                writer.write_timestamp(0);
                s.insert("ChatMessage", writer.into_bytes());
            },
        )
        .expect("alice queues offline");
    assert_eq!(alice.pending_mutations(), 1);
    drop(alice);

    let bob = Connection::connect_persistent(
        &server.tcp_url,
        b"bob",
        [chat_schema()],
        fluxum_sdk::ReconnectPolicy::default(),
        backend,
        "shared-device",
    )
    .expect("bob");
    assert_eq!(
        bob.pending_mutations(),
        0,
        "alice's queue was discarded, not replayed as bob"
    );
    // Negative outcome needs a beat: were the call wrongly replayed, the
    // public ChatMessage row would land on bob's live subscription.
    std::thread::sleep(Duration::from_millis(750));
    assert!(
        bob.rows("ChatMessage").is_empty(),
        "alice's offline message never applied under bob"
    );
}

#[test]
fn offline_optimistic_calls_replay_in_order_exactly_once() {
    // SPEC-021 CS-032: calls made while the connection is down stay queued
    // (and rendered), then replay in submission order on reconnect, each
    // under its stable idempotency key — so nothing double-applies even if a
    // first send actually reached the server.
    if skip() {
        return;
    }
    let server = Server::start("offline-replay");
    let db = Connection::connect(&server.tcp_url, b"", [task_schema()]).expect("connect");
    db.subscribe(&["SELECT * FROM Task"]).expect("subscribe");

    // Kill the socket as an outage would, then submit while (as far as this
    // thread knows) the session is down.
    db.simulate_stream_loss();
    let identity = db.identity();
    for (i, title) in ["offline-a", "offline-b"].into_iter().enumerate() {
        db.call_optimistic("add_task", vec![FluxValue::Str(title.into())], |s| {
            s.insert(
                "Task",
                optimistic_task_row(u64::MAX - i as u64, identity, title),
            );
        })
        .expect("call_optimistic while down");
    }
    // Both render locally regardless of connectivity.
    assert_eq!(db.rows("Task").len(), 2, "queued calls render offline");

    // The reconnect machinery replays the queue; every call applies once.
    let deadline = Instant::now() + Duration::from_secs(15);
    while db.pending_mutations() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(db.pending_mutations(), 0, "the queue drained after replay");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut titles: Vec<String> = db.rows("Task").iter().map(|r| task_title(r)).collect();
        titles.sort();
        if titles == ["offline-a", "offline-b"] {
            break; // exactly once each — no duplicates, no leftovers
        }
        assert!(
            Instant::now() < deadline,
            "replay did not converge exactly-once: {titles:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
