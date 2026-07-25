//! T7.2 phase A exit test (SPEC-014 §5): automatic failover in a
//! 3-member replica set over the real wire — the primary dies, a replica
//! wins the election (REP-030), promotes (REP-032), accepts writes, and
//! the surviving follower finds the new primary by peer rotation and
//! converges under the new epoch. Writes to a replica are rejected with
//! the retryable `NotPrimary` redirect (REP-042).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::Duration;

use fluxum_core::config::{AuthProvider, Config, ReplicationRole, ServerPeer};
use fluxum_server::boot;

/// A locally free TCP port (bound then released — the race window is
/// acceptable in a test that binds it right back).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn member_config(dir: &Path, name: &str, tcp_port: u16, peers: Vec<String>) -> Config {
    let mut config = Config::default();
    config.server.http_port = 0;
    config.server.tcp_port = tcp_port;
    config.auth.provider = AuthProvider::None;
    // REP-005: every member can authenticate to every other.
    config.auth.server_peers = vec![
        ServerPeer {
            name: "node-a".into(),
            token: "tok-a".into(),
        },
        ServerPeer {
            name: "node-b".into(),
            token: "tok-b".into(),
        },
        ServerPeer {
            name: "node-c".into(),
            token: "tok-c".into(),
        },
    ];
    config.storage.data_dir = dir.into();
    config.storage.commit_log_dir = dir.join("log");
    config.storage.checkpoint_dir = dir.join("checkpoints");
    config.storage.page_dir = dir.join("pages");
    config.storage.checkpoint_interval_tx = 10_000;
    config.replication.archive.dir = dir.join("archive");
    config.replication.member_name = name.into();
    config.replication.peer_token = Some(fluxum_core::secret::Secret::new(format!(
        "tok-{}",
        &name[name.len() - 1..]
    )));
    config.replication.peers = peers;
    config.replication.heartbeat_interval_ms = 25;
    config.replication.ack_interval_ms = 25;
    // Fast elections so the drill completes quickly (jitter is on top),
    // with margin over connection churn so a slow first dial can never
    // spuriously unseat the healthy primary (stickiness covers the rest).
    config.replication.election_timeout_ms = 800;
    config
}

async fn post_task(addr: std::net::SocketAddr, name: &str) -> String {
    let body = format!("[\"{name}\"]");
    let request = format!(
        "POST /reducer/add_task HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let n = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
        .await
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn task_count(ctx: &fluxum_server::ShardContext) -> usize {
    let table = ctx.store().table_id("Task").unwrap();
    ctx.store().snapshot().row_count(table).unwrap()
}

async fn wait_for_count(ctx: &fluxum_server::ShardContext, want: usize, context: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if task_count(ctx) == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{context}: stuck at {} of {want}",
            task_count(ctx)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_wins_the_election_and_serves_writes_when_the_primary_dies() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();

    let (port_a, port_b, port_c) = (free_port(), free_port(), free_port());
    let addr = |p: u16| format!("127.0.0.1:{p}");

    // A is the bootstrap primary; B and C follow (their peer lists start
    // with A so the first dial lands on the live primary).
    let mut config_a = member_config(
        &root.path().join("a"),
        "node-a",
        port_a,
        vec![addr(port_b), addr(port_c)],
    );
    config_a.replication.role = ReplicationRole::Primary;
    let mut config_b = member_config(
        &root.path().join("b"),
        "node-b",
        port_b,
        vec![addr(port_a), addr(port_c)],
    );
    config_b.replication.role = ReplicationRole::Replica;
    let mut config_c = member_config(
        &root.path().join("c"),
        "node-c",
        port_c,
        vec![addr(port_a), addr(port_b)],
    );
    config_c.replication.role = ReplicationRole::Replica;

    let a = boot::serve(config_a).await.unwrap();
    let b = boot::serve(config_b).await.unwrap();
    let c = boot::serve(config_c).await.unwrap();

    // Seed writes on the primary; both replicas converge.
    for i in 1..=5 {
        let head = post_task(a.http.local_addr, &format!("pre-{i}")).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    }
    wait_for_count(&b.ctx, 5, "replica b pre-failover").await;
    wait_for_count(&c.ctx, 5, "replica c pre-failover").await;

    // REP-042: a replica rejects writes with the retryable redirect.
    let refused = post_task(b.http.local_addr, "must-not-land").await;
    assert!(
        !refused.starts_with("HTTP/1.1 200"),
        "a replica must not accept writes: {refused}"
    );
    assert!(refused.contains("primary"), "{refused}");
    assert_eq!(task_count(&b.ctx), 5, "the refused write executed nothing");

    // The primary dies (listener + tasks gone — connection resets for
    // everyone). Heartbeats stop; the election timer fires on B and C.
    a.shutdown();
    drop(a);

    // One of the survivors wins and publishes the primary role.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let (winner, follower) = loop {
        let b_primary = b.ctx.election().unwrap().role().is_primary();
        let c_primary = c.ctx.election().unwrap().role().is_primary();
        if b_primary || c_primary {
            break if b_primary { (&b, &c) } else { (&c, &b) };
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no survivor promoted (REP-030)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let new_epoch = winner.ctx.election().unwrap().role().epoch();
    assert!(
        new_epoch >= 2,
        "promotion increments the epoch: {new_epoch}"
    );
    assert!(
        winner.ctx.metrics().replication_elections_total() >= 1,
        "the election was counted (REP-081)"
    );
    assert!(winner.ctx.metrics().replication_role_primary());

    // Zero pre-failover loss: everything the old primary acknowledged is
    // on the winner (both replicas had converged before the kill).
    assert_eq!(task_count(&winner.ctx), 5);

    // The new primary serves writes; the follower finds it by rotation
    // and converges under the new epoch.
    for i in 1..=3 {
        let head = post_task(winner.http.local_addr, &format!("post-{i}")).await;
        assert!(
            head.starts_with("HTTP/1.1 200"),
            "the new primary must ack writes: {head}"
        );
    }
    wait_for_count(&follower.ctx, 8, "follower under the new primary").await;
    assert_eq!(task_count(&winner.ctx), 8);
    assert!(
        !follower.ctx.election().unwrap().role().is_primary(),
        "exactly one primary after the failover"
    );

    winner.shutdown();
    follower.shutdown();
}
