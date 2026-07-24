//! Object-storage backup & archive (SPEC-025 OPS-010/011; FR-139): the S3
//! wire client against an in-process S3-compatible fixture, push → restore
//! round-trip to the exact state, content-addressed incremental uploads, a
//! flipped bit failing the download with the artifact named, PITR fetching
//! the cut segment via range reads only, and the checkpoint worker's
//! incremental archiver skipping what is already uploaded.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod crash_support;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use fluxum_core::backup::store::{ArtifactStore, FsStore, S3Config, S3Store};
use fluxum_core::backup::{BackupSource, PitrTarget, RestoreDirs, remote};
use fluxum_core::checkpoint::{CheckpointRepo, DirectoryArchive, compact_covered};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};

use crash_support::{EPOCH, SHARD, StepOptions, commit_step, mem_store, recover_fresh};

const WL: StepOptions = StepOptions {
    heavy: false,
    with_event: true,
};

// --- an in-process S3-compatible wire fixture -------------------------------------

#[derive(Default)]
struct FixtureState {
    objects: HashMap<String, Vec<u8>>,
    puts: u64,
}

/// A minimal S3-compatible HTTP server: PUT/GET(+Range)/HEAD on
/// `/bucket/<key>`, ListObjectsV2 on `/bucket/?list-type=2`. Signature
/// headers are accepted, not validated (the fixture pins the WIRE shape;
/// SigV4 correctness is pinned by its own vector test).
struct S3Fixture {
    endpoint: String,
    state: Arc<Mutex<FixtureState>>,
}

impl S3Fixture {
    fn start() -> S3Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(FixtureState::default()));
        let shared = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let state = Arc::clone(&shared);
                std::thread::spawn(move || serve_conn(stream, &state));
            }
        });
        S3Fixture { endpoint, state }
    }

    fn store(&self) -> S3Store {
        S3Store::new(S3Config {
            endpoint: self.endpoint.clone(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "secret".into(),
        })
    }

    fn flip_byte(&self, key_suffix: &str) -> String {
        let mut state = self.state.lock().unwrap();
        let key = state
            .objects
            .keys()
            .find(|k| k.ends_with(key_suffix) || k.contains(key_suffix))
            .cloned()
            .expect("an object matching the suffix");
        let bytes = state.objects.get_mut(&key).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x01;
        key
    }

    fn puts(&self) -> u64 {
        self.state.lock().unwrap().puts
    }
}

fn serve_conn(mut stream: TcpStream, state: &Arc<Mutex<FixtureState>>) {
    loop {
        // Read one request: headers, then Content-Length body bytes.
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];
        let header_end = loop {
            if let Some(at) = find_subslice(&raw, b"\r\n\r\n") {
                break at + 4;
            }
            match stream.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => raw.extend_from_slice(&buf[..n]),
                Err(_) => return,
            }
        };
        let head = String::from_utf8_lossy(&raw[..header_end]).into_owned();
        let mut lines = head.lines();
        let request_line = lines.next().unwrap_or_default().to_owned();
        let mut content_length = 0usize;
        let mut range: Option<(u64, u64)> = None;
        for line in lines {
            let lower = line.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            if let Some(v) = lower.strip_prefix("range:")
                && let Some(spec) = v.trim().strip_prefix("bytes=")
                && let Some((a, b)) = spec.split_once('-')
                && let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>())
            {
                range = Some((a, b));
            }
        }
        let mut body = raw[header_end..].to_vec();
        while body.len() < content_length {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => body.extend_from_slice(&buf[..n]),
                Err(_) => return,
            }
        }
        body.truncate(content_length);

        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let target = parts.next().unwrap_or_default().to_owned();
        let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
        // Path style: /bucket/<key>.
        let key = path
            .strip_prefix("/test-bucket")
            .map(|k| k.trim_start_matches('/'))
            .unwrap_or_default()
            .to_owned();
        let key = percent_decode(&key);

        let response = {
            let mut state = state.lock().unwrap();
            match method.as_str() {
                "PUT" => {
                    state.objects.insert(key, body);
                    state.puts += 1;
                    http(200, "", b"")
                }
                "HEAD" => match state.objects.get(&key) {
                    Some(bytes) => http_head(200, bytes.len()),
                    None => http_head(404, 0),
                },
                "GET" if query.contains("list-type=2") => {
                    let prefix = query
                        .split('&')
                        .find_map(|p| p.strip_prefix("prefix="))
                        .map(percent_decode)
                        .unwrap_or_default();
                    let mut xml = String::from(
                        "<?xml version=\"1.0\"?><ListBucketResult>\
                         <IsTruncated>false</IsTruncated>",
                    );
                    let mut keys: Vec<&String> = state
                        .objects
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .collect();
                    keys.sort();
                    for k in keys {
                        xml.push_str(&format!("<Contents><Key>{k}</Key></Contents>"));
                    }
                    xml.push_str("</ListBucketResult>");
                    http(200, "application/xml", xml.as_bytes())
                }
                "GET" => match state.objects.get(&key) {
                    Some(bytes) => match range {
                        Some((a, b)) => {
                            let from = usize::try_from(a).unwrap_or(usize::MAX).min(bytes.len());
                            let to = usize::try_from(b.saturating_add(1))
                                .unwrap_or(usize::MAX)
                                .min(bytes.len());
                            http(206, "application/octet-stream", &bytes[from..to])
                        }
                        None => http(200, "application/octet-stream", bytes),
                    },
                    None => http(404, "", b"NoSuchKey"),
                },
                _ => http(405, "", b""),
            }
        };
        if stream.write_all(&response).is_err() {
            return;
        }
    }
}

fn http(code: u16, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {code} X\r\nContent-Length: {}\r\n{}\r\n",
        body.len(),
        if content_type.is_empty() {
            String::new()
        } else {
            format!("Content-Type: {content_type}\r\n")
        }
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

fn http_head(code: u16, len: usize) -> Vec<u8> {
    format!("HTTP/1.1 {code} X\r\nContent-Length: {len}\r\n\r\n").into_bytes()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let (Some(h), Some(l)) = (
                bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16)),
                bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16)),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// --- world builder (the crash-suite oracle workload) ------------------------------

struct World {
    root: tempfile::TempDir,
}

impl World {
    fn log_dir(&self) -> PathBuf {
        self.root.path().join("log")
    }
    fn snap_dir(&self) -> PathBuf {
        self.root.path().join("snapshots")
    }
    fn source(&self) -> BackupSource {
        BackupSource {
            checkpoint_dir: self.snap_dir(),
            log_dir: self.log_dir(),
        }
    }
    fn restore_dirs(&self, name: &str) -> RestoreDirs {
        RestoreDirs {
            checkpoint_dir: self.root.path().join(name).join("snapshots"),
            log_dir: self.root.path().join(name).join("log"),
        }
    }
}

async fn build_with(head: u64, ckpt: u64, segment_max_bytes: u64, options: StepOptions) -> World {
    let world = World {
        root: tempfile::tempdir().unwrap(),
    };
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&world.log_dir(), SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&world.snap_dir()).unwrap();
    for i in 1..=head {
        commit_step(&store, &log, i, options).await;
        if i == ckpt {
            log.wait_durable(i).await.unwrap();
            repo.write(&store.snapshot(), SHARD, i, EPOCH).unwrap();
        }
    }
    log.wait_durable(head).await.unwrap();
    log.close().unwrap();
    world
}

async fn build(head: u64, ckpt: u64, segment_max_bytes: u64) -> World {
    build_with(head, ckpt, segment_max_bytes, WL).await
}

fn assert_restored_state_with(dirs: &RestoreDirs, n: u64, options: StepOptions, context: &str) {
    let (store, outcome) = recover_fresh(&dirs.log_dir, &dirs.checkpoint_dir);
    assert_eq!(outcome.last_tx_id, Some(n), "{context}");
    crash_support::assert_equals_oracle(&store, n, options, context);
}

fn assert_restored_state(dirs: &RestoreDirs, n: u64, context: &str) {
    assert_restored_state_with(dirs, n, WL, context);
}

// --- the S3 wire client against the fixture (OPS-010) -----------------------------

#[test]
fn s3_store_speaks_the_wire_shape() {
    let fixture = S3Fixture::start();
    let store = fixture.store();
    store.put("a/one.bin", b"hello world").unwrap();
    store.put("a/two.bin", b"xy").unwrap();
    assert_eq!(store.get("a/one.bin").unwrap(), b"hello world");
    assert_eq!(store.get_range("a/one.bin", 6, 5).unwrap(), b"world");
    assert_eq!(store.head("a/two.bin").unwrap(), Some(2));
    assert_eq!(store.head("a/absent").unwrap(), None);
    assert_eq!(
        store.list("a/").unwrap(),
        vec!["a/one.bin".to_owned(), "a/two.bin".to_owned()]
    );
    assert!(store.get("a/absent").is_err());
    assert_eq!(store.get_range("a/one.bin", 0, 0).unwrap(), b"");
}

#[test]
fn a_future_remote_manifest_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::open(dir.path()).unwrap();
    let future = remote::RemoteManifest {
        format_version: remote::REMOTE_FORMAT_VERSION + 1,
        backup_id: "x".into(),
        created_at: 0,
        schema_version: 0,
        shards: Vec::new(),
    };
    store
        .put("fluxum/latest", &rmp_serde::to_vec(&future).unwrap())
        .unwrap();
    let err = remote::latest_manifest(&store, "fluxum").unwrap_err();
    assert!(err.to_string().contains("unsupported"), "{err}");
}

// --- push → restore round-trip + incremental (OPS-010/011) ------------------------

#[tokio::test]
async fn push_restores_exactly_and_a_second_push_uploads_nothing() {
    let world = build(12, 6, 256).await;
    let fixture = S3Fixture::start();
    let store = fixture.store();

    let first = remote::push(&world.source(), &store, "fluxum").unwrap();
    assert!(first.uploaded >= 2, "{first:?}");
    assert_eq!(first.skipped, 0, "{first:?}");
    assert_eq!(first.head_tx_id, 12);

    // OPS-011 incremental: an unchanged world re-pushes ZERO artifact bytes
    // — every content-addressed object is already present. The fixture's
    // PUT counter confirms at the wire: only the two manifest keys (the
    // immutable one and `latest`) are written again.
    let puts_after_first = fixture.puts();
    let second = remote::push(&world.source(), &store, "fluxum").unwrap();
    assert_eq!(second.uploaded, 0, "{second:?}");
    assert_eq!(second.skipped, first.uploaded, "{second:?}");
    assert_eq!(fixture.puts(), puts_after_first + 2, "wire-level PUTs");

    // Restore from the object store reproduces the exact head state.
    let dirs = world.restore_dirs("restored");
    let report = remote::restore(&store, "fluxum", &dirs, false).unwrap();
    assert_eq!(report.head_tx_id, 12);
    assert_restored_state(&dirs, 12, "remote restore");
}

// --- OPS-011: a flipped bit fails the download with the artifact named ------------

#[tokio::test]
async fn a_flipped_remote_bit_fails_the_download_precisely() {
    let world = build(12, 6, 256).await;
    let fixture = S3Fixture::start();
    let store = fixture.store();
    remote::push(&world.source(), &store, "fluxum").unwrap();

    let corrupted = fixture.flip_byte("objects/");
    let err = remote::restore(&store, "fluxum", &world.restore_dirs("refused"), false).unwrap_err();
    let message = err.to_string();
    assert!(message.contains("integrity failure"), "{message}");
    assert!(message.contains(&corrupted), "{message}");
}

// --- OPS-010: PITR range-reads only the cut window --------------------------------

#[tokio::test]
async fn remote_pitr_range_reads_only_the_needed_window() {
    // One big segment (no rotation), heavy rows so the raw payload dwarfs
    // the 64 KiB index-tail hint, and an early cut: a whole-artifact fetch
    // would be provably wasteful.
    let heavy = StepOptions {
        heavy: true,
        with_event: true,
    };
    let world = build_with(48, 6, u64::MAX, heavy).await;
    let fixture = S3Fixture::start();
    let store = fixture.store();
    remote::push_with(&world.source(), &store, "fluxum", 16 * 1024).unwrap();

    let dirs = world.restore_dirs("pitr");
    let (report, stats) =
        remote::pitr(&store, "fluxum", &dirs, PitrTarget::TxId(10), false).unwrap();
    assert_eq!(report.last_tx_id, 10);
    assert_restored_state_with(&dirs, 10, heavy, "remote pitr");

    assert!(
        stats.target_segment_stored_len > 0
            && stats.target_segment_bytes_fetched < stats.target_segment_stored_len,
        "the cut segment must be fetched partially: {stats:?}"
    );

    // The timestamp flavor lands on the same boundary (the workload stamps
    // timestamp = tx id in µs), still via range reads.
    let dirs = world.restore_dirs("pitr-ts");
    let (report, ts_stats) = remote::pitr(
        &store,
        "fluxum",
        &dirs,
        PitrTarget::TimestampMicros(10),
        false,
    )
    .unwrap();
    assert_eq!((report.last_tx_id, report.last_timestamp), (10, 10));
    assert_restored_state_with(&dirs, 10, heavy, "remote pitr by timestamp");
    assert!(
        ts_stats.target_segment_bytes_fetched < ts_stats.target_segment_stored_len,
        "{ts_stats:?}"
    );

    // The roll-forward guard holds remotely too.
    let err = remote::pitr(
        &store,
        "fluxum",
        &world.restore_dirs("early"),
        PitrTarget::TxId(3),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("precedes"), "{err}");
}

// --- OPS-011: the worker-driven incremental archiver ------------------------------

#[tokio::test]
async fn the_remote_archiver_uploads_once_and_then_skips() {
    let world = build(16, 8, 256).await;
    // Archive covered segments locally first (the REP-062 flow).
    let archive_dir = world.root.path().join("archive");
    let archive = DirectoryArchive::open(&archive_dir).unwrap();
    compact_covered(&world.log_dir(), SHARD, 8, None, Some(&archive)).unwrap();

    // FsStore: the archiver is target-agnostic behind the trait.
    let store = Arc::new(FsStore::open(world.root.path().join("remote")).unwrap());
    let archiver = remote::RemoteArchiver::new(store, "fluxum");

    let first = archiver
        .sync(&world.snap_dir(), Some(&archive_dir), SHARD)
        .unwrap();
    assert!(first.uploaded > 0, "{first:?}");

    let second = archiver
        .sync(&world.snap_dir(), Some(&archive_dir), SHARD)
        .unwrap();
    assert_eq!(second.uploaded, 0, "{second:?}");
    assert_eq!(second.skipped, first.uploaded, "{second:?}");
}
