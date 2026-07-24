//! T7.3 acceptance (SPEC-014 §Acceptance 6; PRD §12.2): backup + restore +
//! PITR against the REAL server assembly under sustained writes — the boot
//! path spawns the checkpoint worker (STG-020) with REP-062 archival,
//! `POST /checkpoint` takes a fresh checkpoint on demand (REP-060
//! `--fresh-checkpoint`), a hot backup taken mid-writes verifies and
//! restores to the exact head, and PITR reproduces the inclusive tx-id
//! prefix on a rebooted server.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::config::{AuthProvider, Config};
use fluxum_server::boot;

/// A server config over a temp data directory laid out the way
/// `fluxum backup --data-dir` expects (`log/`, `checkpoints/`, `archive/`),
/// with OS-assigned ports so parallel tests never collide.
fn config_over(dir: &Path, interval_tx: u64) -> Config {
    let mut config = Config::default();
    config.server.http_port = 0;
    config.server.tcp_port = 0;
    config.auth.provider = AuthProvider::None;
    config.storage.data_dir = dir.into();
    config.storage.commit_log_dir = dir.join("log");
    config.storage.checkpoint_dir = dir.join("checkpoints");
    config.storage.page_dir = dir.join("pages");
    config.storage.checkpoint_interval_tx = interval_tx;
    config.replication.archive.dir = dir.join("archive");
    config
}

struct Resp {
    status: u16,
    body: String,
}

/// One request, reading exactly `Content-Length` body bytes — the server
/// keeps the socket alive, so read-to-EOF would stall out its timeout on
/// every call and throttle the "sustained" writer to a crawl.
async fn req(addr: std::net::SocketAddr, method: &str, path: &str, body: &[u8]) -> Resp {
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (mut header_end, mut content_length) = (None, None);
    loop {
        if let (Some(end), Some(length)) = (header_end, content_length)
            && raw.len() >= end + length
        {
            break;
        }
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .expect("response timed out")
            .unwrap();
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
        if header_end.is_none() {
            let text = String::from_utf8_lossy(&raw);
            if let Some(at) = text.find("\r\n\r\n") {
                header_end = Some(at + 4);
                content_length = text
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .map(String::from)
                    })
                    .and_then(|v| v.parse::<usize>().ok());
            }
        }
    }
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (h, b) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = h
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Resp {
        status,
        body: b.to_owned(),
    }
}

/// `SELECT * FROM Task` row count on a running server.
async fn task_count(addr: std::net::SocketAddr) -> u64 {
    let r = req(
        addr,
        "POST",
        "/query",
        br#"{"sql":"SELECT * FROM Task LIMIT 1000000"}"#,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let v: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    v["payload"]["rows"].as_array().map_or(0, Vec::len) as u64
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_restore_and_pitr_round_trip_under_sustained_writes() {
    fluxum_demo::link();
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    // A tight cadence so the boot-spawned worker checkpoints DURING the run.
    let server = boot::serve(config_over(&data, 20)).await.unwrap();
    let addr = server.http.local_addr;

    // The tx id before any workload write: the system's own commits
    // (migration metadata) land first, so task counts are relative to it.
    let health: serde_json::Value =
        serde_json::from_str(&req(addr, "GET", "/health", &[]).await.body).unwrap();
    let base_tx = health["shards"][0]["tx_id"].as_u64().unwrap();

    // Sustained writes: a background task inserting Tasks until told to stop.
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let writer = {
        let (stop, written) = (Arc::clone(&stop), Arc::clone(&written));
        tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let body = format!("[\"task-{i}\"]");
                let r = req(addr, "POST", "/reducer/add_task", body.as_bytes()).await;
                assert_eq!(r.status, 200, "sustained write {i} failed: {}", r.body);
                written.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    // Let the workload run past the checkpoint cadence.
    while written.load(Ordering::Relaxed) < 30 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        std::fs::read_dir(data.join("checkpoints"))
            .unwrap()
            .any(|e| e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("ckpt-")),
        "the boot-spawned worker must have checkpointed under load (STG-020)"
    );

    // REP-060 --fresh-checkpoint: POST /checkpoint answers with coverage.
    let fresh = req(addr, "POST", "/checkpoint", b"{}").await;
    assert_eq!(fresh.status, 200, "{}", fresh.body);
    let fresh: serde_json::Value = serde_json::from_str(&fresh.body).unwrap();
    let fresh_tx = fresh["payload"]["last_tx_id"].as_u64().unwrap();
    assert!(fresh_tx > base_tx);

    // Let the writer put comfortably more history past the fresh checkpoint
    // so a mid-history PITR target (fresh_tx + 5) exists in the backup.
    let mark = written.load(Ordering::Relaxed);
    while written.load(Ordering::Relaxed) < mark + 10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Hot backup while the writer keeps writing (REP-060: no stall — the
    // writer task asserts every call keeps succeeding throughout).
    let out = root.path().join("backup");
    let source = fluxum_core::backup::BackupSource {
        checkpoint_dir: data.join("checkpoints"),
        log_dir: data.join("log"),
    };
    let report = {
        let (source, out) = (source.clone(), out.clone());
        tokio::task::spawn_blocking(move || fluxum_core::backup::create(&source, &out))
            .await
            .unwrap()
            .unwrap()
    };
    assert!(report.head_tx_id > base_tx, "{report:?}");

    // The CLI wrapper agrees the backup verifies (REP-064).
    let summary = fluxum_cli::backup::verify(&out).unwrap();
    assert!(summary.contains("all good"), "{summary}");

    // OPS-010/011: a remote push while the writer is STILL writing — the
    // same hot capture, content-addressed into an object store (the trait
    // is target-agnostic; FsStore stands in for S3, whose wire client has
    // its own suite). A second push of the same world uploads nothing new
    // beyond what the moving head added.
    let remote_root = root.path().join("object-store");
    let remote_store = fluxum_core::backup::store::FsStore::open(&remote_root).unwrap();
    let push = {
        let source = source.clone();
        tokio::task::spawn_blocking(move || {
            fluxum_core::backup::remote::push(&source, &remote_store, "fluxum")
        })
        .await
        .unwrap()
        .unwrap()
    };
    assert!(push.uploaded > 0, "{push:?}");
    assert!(push.head_tx_id >= report.head_tx_id, "{push:?}");

    // Stop the writes, note the head, and shut the server down.
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    server.shutdown();

    // Remote restore reproduces the push's exact head on a rebooted server.
    let remote_restored = root.path().join("remote-restored");
    let dirs = fluxum_core::backup::RestoreDirs {
        checkpoint_dir: remote_restored.join("checkpoints"),
        log_dir: remote_restored.join("log"),
    };
    let remote_store = fluxum_core::backup::store::FsStore::open(&remote_root).unwrap();
    let restored =
        fluxum_core::backup::remote::restore(&remote_store, "fluxum", &dirs, false).unwrap();
    assert_eq!(restored.head_tx_id, push.head_tx_id);
    let server_r = boot::serve(config_over(&remote_restored, 10_000))
        .await
        .unwrap();
    assert_eq!(
        task_count(server_r.http.local_addr).await,
        push.head_tx_id - base_tx,
        "remote restore reproduces the push head exactly (OPS-010)"
    );
    server_r.shutdown();

    // The backup head is a moving snapshot: tasks captured = head - base.
    let tasks_at_head = report.head_tx_id - base_tx;

    // REP-063: restore into a fresh layout via the CLI and BOOT the real
    // server over it — recovery reproduces the exact captured state.
    let restored = root.path().join("restored");
    let layout = fluxum_cli::backup::DataLayout {
        checkpoint_dir: restored.join("checkpoints"),
        log_dir: restored.join("log"),
        archive_dir: restored.join("archive"),
    };
    fluxum_cli::backup::restore(&out, &layout, None, None, false).unwrap();
    let server2 = boot::serve(config_over(&restored, 10_000)).await.unwrap();
    assert_eq!(
        task_count(server2.http.local_addr).await,
        tasks_at_head,
        "restored state must be exactly the backup head (REP-063)"
    );
    server2.shutdown();

    // REP-070/072: PITR to a mid-history target past the backup's fresh
    // checkpoint — the reboot holds exactly the target prefix and adopts
    // the forked-lineage epoch from the marker.
    let pitr_dir = root.path().join("pitr");
    let layout = fluxum_cli::backup::DataLayout {
        checkpoint_dir: pitr_dir.join("checkpoints"),
        log_dir: pitr_dir.join("log"),
        archive_dir: pitr_dir.join("archive"),
    };
    let target = fresh_tx + 5;
    assert!(report.head_tx_id >= target, "{report:?} vs target {target}");
    let message = fluxum_cli::backup::restore(
        &out,
        &layout,
        Some(fluxum_core::backup::PitrTarget::TxId(target)),
        Some(&data.join("archive")),
        false,
    )
    .unwrap();
    assert!(
        message.contains(&format!("last applied tx {target}")),
        "{message}"
    );
    assert_eq!(
        fluxum_core::backup::pitr_lineage_min_epoch(&layout.log_dir)
            .unwrap()
            .unwrap(),
        2,
        "the restored log's epochs were all 1, so the fork starts at 2 (REP-072)"
    );
    let server3 = boot::serve(config_over(&pitr_dir, 10_000)).await.unwrap();
    assert_eq!(
        task_count(server3.http.local_addr).await,
        target - base_tx,
        "PITR reproduces the inclusive prefix (REP-070/071)"
    );
    server3.shutdown();

    // Roll-forward guard: a target BEFORE the backup's checkpoint cannot be
    // reproduced from this backup and is refused, not silently wrong.
    let err = fluxum_cli::backup::restore(
        &out,
        &fluxum_cli::backup::DataLayout {
            checkpoint_dir: root.path().join("early").join("checkpoints"),
            log_dir: root.path().join("early").join("log"),
            archive_dir: root.path().join("early").join("archive"),
        },
        Some(fluxum_core::backup::PitrTarget::TxId(base_tx + 1)),
        None,
        false,
    )
    .unwrap_err();
    assert!(err.contains("precedes"), "{err}");
}
