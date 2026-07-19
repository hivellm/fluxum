//! SPEC-023 DMX-041 — the `/blob` HTTP endpoints: upload streams the raw
//! body out-of-band of the 16 MB FluxRPC frame and answers the content hash;
//! download serves the bytes back; unknown hashes and disabled stores 404.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{BlobStore, CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, CONTENT_TYPE, HttpOptions};

const SHARD: u32 = 12;

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "avatar",
        ty: FluxType::Blob,
    },
];
static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

async fn start(with_blobs: bool) -> (http::HttpServer, Arc<ShardContext>) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&USER]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("blob-test"),
    );
    let subscriptions = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let authenticator =
        Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subscriptions, authenticator, SHARD, 64);
    if with_blobs {
        let blobs = Arc::new(BlobStore::open(&dir.path().join("blobs")).unwrap());
        ctx.set_blob_store(blobs);
    }
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    (server, ctx)
}

struct Response {
    status: u16,
    body: Vec<u8>,
}

async fn request(addr: std::net::SocketAddr, method: &str, path: &str, body: &[u8]) -> Response {
    request_as(addr, method, path, body, None).await
}

/// A `/blob` request carrying an optional `Fluxum-Session` (F-002 requires an
/// authenticated session on the blob routes).
async fn request_as(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    session: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let session_header = session
        .map(|s| format!("Fluxum-Session: {s}\r\n"))
        .unwrap_or_default();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/octet-stream\r\n\
         {session_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();

    // The server keeps the connection alive between requests: read the head,
    // then exactly Content-Length body bytes (never read-to-EOF).
    let mut raw = Vec::new();
    let headers_end = loop {
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed before a complete response head");
        raw.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&raw[..headers_end]).into_owned();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    let body_start = headers_end + 4;
    while raw.len() < body_start + content_length {
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-body");
        raw.extend_from_slice(&chunk[..n]);
    }
    Response {
        status,
        body: raw[body_start..body_start + content_length].to_vec(),
    }
}

/// Authenticate over `/rpc` (NoneProvider accepts any token) and return the
/// issued `Fluxum-Session`, so blob requests can present it (F-002).
async fn authenticate(addr: std::net::SocketAddr) -> String {
    let body = FrameCodec::default()
        .encode(
            &ClientMessage::Authenticate(Authenticate {
                id: 1,
                token: b"alice".to_vec(),
                compression: None,
                tx_updates: None,
                namespace: None,
            })
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut raw = Vec::new();
    let headers_end = loop {
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "auth response head");
        raw.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&raw[..headers_end]).into_owned();
    // Confirm the handshake actually succeeded before trusting the header.
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())?
        })
        .unwrap_or(0);
    let body_start = headers_end + 4;
    while raw.len() < body_start + content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        raw.extend_from_slice(&chunk[..n]);
    }
    let codec = FrameCodec::default();
    if let Ok(Some((fluxum_protocol::Frame::Body(b), _))) =
        codec.decode(&raw[body_start..body_start + content_length])
    {
        assert!(matches!(
            ServerMessage::decode(b).unwrap(),
            ServerMessage::AuthResult(_)
        ));
    }
    head.lines()
        .find_map(|l| l.strip_prefix("Fluxum-Session: "))
        .expect("session issued")
        .trim()
        .to_owned()
}

/// DMX-041: a 4 MB upload streams out of band, answers the content hash, and
/// the bytes round-trip through the download endpoint.
#[tokio::test(flavor = "multi_thread")]
async fn upload_and_download_round_trip_out_of_band() {
    let (server, ctx) = start(true).await;
    let payload = vec![0xABu8; 4 * 1024 * 1024];
    let session = authenticate(server.local_addr).await;

    // F-002: without a session, the blob upload is refused.
    assert_eq!(
        request(server.local_addr, "POST", "/blob", &payload)
            .await
            .status,
        401,
        "unauthenticated blob upload is closed"
    );

    let up = request_as(server.local_addr, "POST", "/blob", &payload, Some(&session)).await;
    assert_eq!(up.status, 200);
    let body = String::from_utf8(up.body).unwrap();
    let hash = body
        .strip_prefix("{\"hash\":\"")
        .and_then(|s| s.strip_suffix("\"}"))
        .expect("hash JSON shape")
        .to_owned();
    assert_eq!(hash.len(), 64, "64-hex content hash");

    // The staged object is visible to the store (write-time validation).
    let parsed = fluxum_core::commitlog::BlobHash::parse(&hash).unwrap();
    assert!(ctx.blob_store().unwrap().contains(&parsed));

    let down = request_as(
        server.local_addr,
        "GET",
        &format!("/blob/{hash}"),
        &[],
        Some(&session),
    )
    .await;
    assert_eq!(down.status, 200);
    assert_eq!(down.body, payload, "bytes round-trip exactly");

    // F-002: an unauthenticated download is refused too.
    assert_eq!(
        request(server.local_addr, "GET", &format!("/blob/{hash}"), &[])
            .await
            .status,
        401
    );

    server.shutdown();
}

/// DMX-041 edges: empty body 400, malformed hash 400, unknown hash 404, and
/// every `/blob` route 404 when no blob store is installed. All under an
/// authenticated session (F-002 gates the routes before their own edges).
#[tokio::test(flavor = "multi_thread")]
async fn blob_endpoint_edges() {
    let (server, _ctx) = start(true).await;
    let session = authenticate(server.local_addr).await;
    let s = Some(session.as_str());
    assert_eq!(
        request_as(server.local_addr, "POST", "/blob", &[], s)
            .await
            .status,
        400
    );
    assert_eq!(
        request_as(server.local_addr, "GET", "/blob/nothex", &[], s)
            .await
            .status,
        400
    );
    let missing = "0".repeat(64);
    assert_eq!(
        request_as(
            server.local_addr,
            "GET",
            &format!("/blob/{missing}"),
            &[],
            s
        )
        .await
        .status,
        404
    );
    server.shutdown();

    let (server, _ctx) = start(false).await;
    let session = authenticate(server.local_addr).await;
    let s = Some(session.as_str());
    assert_eq!(
        request_as(server.local_addr, "POST", "/blob", b"data", s)
            .await
            .status,
        404
    );
    assert_eq!(
        request_as(
            server.local_addr,
            "GET",
            &format!("/blob/{missing}"),
            &[],
            s
        )
        .await
        .status,
        404
    );
    server.shutdown();
}
