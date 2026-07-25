//! T7.1 exit tests (SPEC-014 §3): a replica converges from COLD against a
//! real primary over the real TCP transport (server-peer auth → hello →
//! stream → apply → ack), the replicated log is byte-identical over the
//! shared range (REP-010), and a stopped replica re-enters via PARTIAL sync
//! from its offset (REP-013) and catches up.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use fluxum_core::config::{AuthProvider, Config, ReplicationRole, ServerPeer};
use fluxum_server::boot;
use fluxum_server::replication::{ReplicaOptions, spawn_replica};

const PEER_TOKEN: &str = "replication-peer-token";

fn base_config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.server.http_port = 0;
    config.server.tcp_port = 0;
    config.auth.provider = AuthProvider::None;
    config.storage.data_dir = dir.into();
    config.storage.commit_log_dir = dir.join("log");
    config.storage.checkpoint_dir = dir.join("checkpoints");
    config.storage.page_dir = dir.join("pages");
    config.storage.checkpoint_interval_tx = 10_000;
    config.replication.archive.dir = dir.join("archive");
    config
}

fn primary_config(dir: &Path) -> Config {
    let mut config = base_config(dir);
    // REP-005: the replica authenticates as this server peer.
    config.auth.server_peers = vec![ServerPeer {
        name: "replica-1".into(),
        token: PEER_TOKEN.into(),
    }];
    // A 1-byte window forces the REP-017 flow-control wait on every batch
    // (each send must be acked before the next), and a tight heartbeat
    // exercises the REP-016 beacons within the test's lifetime — both under
    // real convergence, so neither can deadlock silently.
    config.replication.window_bytes = fluxum_core::config::ByteSize(1);
    config.replication.heartbeat_interval_ms = 25;
    config
}

async fn write_tasks(addr: std::net::SocketAddr, from: u64, to: u64) {
    for i in from..=to {
        let body = format!("[\"task-{i}\"]");
        let request = format!(
            "POST /reducer/add_task HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
            .await
            .unwrap();
        // Read the status line; Content-Length framing is verified by the
        // heavier suites — here only success matters.
        let mut buf = [0u8; 512];
        let n = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
            .await
            .unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(head.starts_with("HTTP/1.1 200"), "write {i}: {head}");
    }
}

fn task_count(ctx: &fluxum_server::ShardContext) -> usize {
    let table = ctx.store().table_id("Task").unwrap();
    ctx.store().snapshot().row_count(table).unwrap()
}

async fn wait_for_count(ctx: &fluxum_server::ShardContext, want: usize, context: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if task_count(ctx) == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{context}: replica stuck at {} of {want}",
            task_count(ctx)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// REP-012: when the primary's retained segments no longer reach back to
/// tx 1 (checkpoint-driven truncation), a cold replica full-syncs — the
/// checkpoint pack streams over, installs, and the tail follows.
#[tokio::test(flavor = "multi_thread")]
async fn a_cold_replica_full_syncs_through_a_checkpoint_transfer() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();

    let primary_dir = root.path().join("primary");
    let mut config = primary_config(&primary_dir);
    // A tight cadence + tiny segments so checkpoint-driven truncation
    // shrinks the live log fast (rotation is the truncation granularity).
    config.storage.checkpoint_interval_tx = 10;
    config.storage.segment_max_bytes = fluxum_core::config::ByteSize(512);
    let primary = boot::serve(config).await.unwrap();
    let primary_http = primary.http.local_addr;
    let primary_tcp = primary.tcp.local_addr;

    write_tasks(primary_http, 1, 30).await;
    // Wait for the worker to checkpoint and truncate the covered prefix
    // (archival keeps the copies; the LIVE log loses them).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let first =
            fluxum_core::commitlog::first_available_tx_id(&primary_dir.join("log"), 0).unwrap();
        if first.is_some_and(|f| f > 1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "compaction never truncated the live log (first={first:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A cold replica now CANNOT partial-sync from tx 1 — full sync it is.
    let replica_dir = root.path().join("replica");
    let mut replica_config = base_config(&replica_dir);
    replica_config.replication.role = ReplicationRole::Replica;
    let replica_ctx = boot::assemble(&replica_config).unwrap();
    let client = spawn_replica(
        std::sync::Arc::clone(&replica_ctx),
        ReplicaOptions {
            primary: primary_tcp.to_string(),
            member_name: "replica-1".into(),
            token: PEER_TOKEN.as_bytes().to_vec(),
            log_dir: replica_config.storage.commit_log_dir.clone(),
            checkpoint_dir: replica_config.storage.checkpoint_dir.clone(),
            ack_interval: Duration::from_millis(50),
        },
    );
    wait_for_count(&replica_ctx, 30, "full sync").await;

    // The transfer really was a checkpoint install, not a from-1 stream:
    // the replica's checkpoint directory holds the transferred manifest.
    assert!(
        std::fs::read_dir(replica_dir.join("checkpoints"))
            .unwrap()
            .any(|e| e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("ckpt-")),
        "the transferred checkpoint must be installed locally (REP-012)"
    );

    // And the tail keeps flowing on the same session.
    write_tasks(primary_http, 31, 35).await;
    wait_for_count(&replica_ctx, 35, "post-full-sync tail").await;
    client.stop();
    primary.shutdown();
}

/// REP-005: a `ReplicaHello` from a connection that did NOT authenticate as
/// a server peer never reaches the replication service — the router answers
/// a plain error and no session registers.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_peer_hello_is_refused_by_the_router() {
    use fluxum_protocol::{ClientMessage, Frame, FrameCodec, ServerMessage};

    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();
    let primary = boot::serve(primary_config(&root.path().join("primary")))
        .await
        .unwrap();

    let codec = FrameCodec::default();
    let mut stream = tokio::net::TcpStream::connect(primary.tcp.local_addr)
        .await
        .unwrap();
    // Authenticate as an ORDINARY client (provider `none` accepts any
    // token; it maps to no server peer).
    let auth = ClientMessage::Authenticate(fluxum_protocol::Authenticate {
        id: 1,
        token: b"just-a-client".to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    });
    let send = |m: &ClientMessage| codec.encode(&m.encode().unwrap()).unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, &send(&auth))
        .await
        .unwrap();
    let hello = ClientMessage::ReplicaHello(fluxum_protocol::ReplicaHello {
        shard_id: 0,
        member_name: "impostor".into(),
        epoch: 1,
        last_applied_tx_id: 0,
    });
    tokio::io::AsyncWriteExt::write_all(&mut stream, &send(&hello))
        .await
        .unwrap();

    // Read frames until the post-auth response to the hello arrives: it
    // must be an Error, and no replication session may exist.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_error = false;
    'read: while tokio::time::Instant::now() < deadline {
        let n = tokio::time::timeout_at(
            deadline,
            tokio::io::AsyncReadExt::read(&mut stream, &mut chunk),
        )
        .await
        .unwrap_or(Ok(0))
        .unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        while let Ok(Some((frame, consumed))) = codec.decode(&buf) {
            if let Frame::Body(body) = frame
                && let Ok(ServerMessage::Error(e)) = ServerMessage::decode(body)
            {
                assert!(e.message.contains("server peers"), "{}", e.message);
                saw_error = true;
                buf.drain(..consumed);
                break 'read;
            }
            buf.drain(..consumed);
        }
    }
    assert!(saw_error, "the non-peer hello must be refused (REP-005)");
    assert!(
        primary
            .ctx
            .replication_primary()
            .unwrap()
            .durable_offsets()
            .is_empty()
    );
    primary.shutdown();
}

/// REP-005: a replica dialing with a WRONG credential is refused at
/// authentication — the client surfaces "auth refused", keeps retrying with
/// backoff, and no session ever registers.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_peer_credential_never_opens_a_session() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();
    let mut config = primary_config(&root.path().join("primary"));
    // Real token auth: a wrong credential actually FAILS (provider `none`
    // would accept anything).
    config.auth.provider = AuthProvider::Token;
    config.auth.secret = Some("root-secret".into());
    let primary = boot::serve(config).await.unwrap();

    let replica_dir = root.path().join("replica");
    let mut replica_config = base_config(&replica_dir);
    replica_config.replication.role = ReplicationRole::Replica;
    let replica_ctx = boot::assemble(&replica_config).unwrap();
    let client = spawn_replica(
        std::sync::Arc::clone(&replica_ctx),
        ReplicaOptions {
            primary: primary.tcp.local_addr.to_string(),
            member_name: "replica-1".into(),
            token: b"wrong-credential".to_vec(),
            log_dir: replica_config.storage.commit_log_dir.clone(),
            checkpoint_dir: replica_config.storage.checkpoint_dir.clone(),
            ack_interval: Duration::from_millis(50),
        },
    );
    // Give it a refusal + one backoff retry, then stop.
    tokio::time::sleep(Duration::from_millis(700)).await;
    client.stop();
    assert!(
        primary
            .ctx
            .replication_primary()
            .unwrap()
            .durable_offsets()
            .is_empty(),
        "a mis-credentialed replica must never register (REP-005)"
    );
    assert_eq!(task_count(&replica_ctx), 0);
    primary.shutdown();
}

/// REP-031 (mechanical half): a member presenting a HIGHER epoch than the
/// primary's fences the primary — the hello is refused with `ReplFence`,
/// the counter moves, and no stream starts. (Election/demote is T7.2.)
#[tokio::test(flavor = "multi_thread")]
async fn a_higher_epoch_hello_fences_the_stale_primary() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();
    let primary_dir = root.path().join("primary");
    let primary = boot::serve(primary_config(&primary_dir)).await.unwrap();

    let replica_dir = root.path().join("replica");
    let mut replica_config = base_config(&replica_dir);
    replica_config.replication.role = ReplicationRole::Replica;
    let replica_ctx = boot::assemble(&replica_config).unwrap();
    // The replica has PERSISTED epoch 7 (a past election this primary,
    // still on epoch 1, never saw).
    fluxum_server::replication::persist_epoch(&replica_config.storage.commit_log_dir, 7).unwrap();

    let client = spawn_replica(
        std::sync::Arc::clone(&replica_ctx),
        ReplicaOptions {
            primary: primary.tcp.local_addr.to_string(),
            member_name: "replica-1".into(),
            token: PEER_TOKEN.as_bytes().to_vec(),
            log_dir: replica_config.storage.commit_log_dir.clone(),
            checkpoint_dir: replica_config.storage.checkpoint_dir.clone(),
            ack_interval: Duration::from_millis(50),
        },
    );
    // The primary counts the fenced hello and never registers the session.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if primary.ctx.metrics().replication_fenced_total() >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the fenced hello was never counted (REP-031)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        primary
            .ctx
            .replication_primary()
            .unwrap()
            .durable_offsets()
            .is_empty(),
        "a fenced hello must not register a session"
    );
    client.stop();
    primary.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_converges_cold_and_resumes_from_its_offset() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();

    // The primary: a full real server.
    let primary_dir = root.path().join("primary");
    let primary = boot::serve(primary_config(&primary_dir)).await.unwrap();
    let primary_http = primary.http.local_addr;
    let primary_tcp = primary.tcp.local_addr;

    // Seed history before the replica exists.
    write_tasks(primary_http, 1, 10).await;
    let primary_tasks = task_count(&primary.ctx);
    assert_eq!(primary_tasks, 10);

    // The replica: assembled without listeners (this test drives the client
    // directly so it can stop and resume it — REP-013).
    let replica_dir = root.path().join("replica");
    let mut replica_config = base_config(&replica_dir);
    replica_config.replication.role = ReplicationRole::Replica;
    let replica_ctx = boot::assemble(&replica_config).unwrap();
    let options = ReplicaOptions {
        primary: primary_tcp.to_string(),
        member_name: "replica-1".into(),
        token: PEER_TOKEN.as_bytes().to_vec(),
        log_dir: replica_config.storage.commit_log_dir.clone(),
        checkpoint_dir: replica_config.storage.checkpoint_dir.clone(),
        ack_interval: Duration::from_millis(50),
    };

    // COLD convergence: empty replica → identical state.
    let client = spawn_replica(std::sync::Arc::clone(&replica_ctx), options.clone());
    wait_for_count(&replica_ctx, 10, "cold sync").await;

    // Live tail: new writes stream through the same session.
    write_tasks(primary_http, 11, 15).await;
    wait_for_count(&replica_ctx, 15, "live tail").await;

    // REP-010: byte-identical logs over the shared range. Wait for the
    // replica's own durability, then compare the segment files.
    let replica_log = replica_ctx.engine.pipeline().log();
    let head = primary
        .ctx
        .engine
        .pipeline()
        .log()
        .durable_tx_id()
        .unwrap()
        .unwrap();
    replica_log.wait_durable(head).await.unwrap();
    let primary_segments = std::fs::read_dir(primary_dir.join("log")).unwrap();
    for entry in primary_segments {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".log") {
            continue;
        }
        let primary_bytes = std::fs::read(entry.path()).unwrap();
        let replica_bytes = std::fs::read(replica_dir.join("log").join(&name)).unwrap();
        assert_eq!(primary_bytes, replica_bytes, "segment {name} (REP-010)");
    }

    // Stop the client; the primary keeps writing while the replica is away.
    client.stop();
    tokio::time::sleep(Duration::from_millis(100)).await;
    write_tasks(primary_http, 16, 25).await;
    assert_eq!(task_count(&replica_ctx), 15, "stopped replica stays put");

    // REP-013: the SAME replica state resumes from its offset — the primary
    // answers Partial (its segments still cover tx 16) and only the gap
    // streams. Convergence to the new head proves the offset path.
    let client = spawn_replica(std::sync::Arc::clone(&replica_ctx), options);
    wait_for_count(&replica_ctx, 25, "offset resync").await;
    client.stop();

    // REP-081: the replica published its offset/lag against the primary.
    let (offset, _lag) = replica_ctx
        .metrics()
        .replication_peer("primary")
        .expect("replica metrics published");
    assert!(offset >= 15, "offset {offset}");

    primary.shutdown();
}
