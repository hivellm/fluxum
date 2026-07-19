//! SPEC-026 SEC-035/036/037 — trusted-proxy client-IP resolution over real
//! loopback sockets: a PROXY v2 preamble from a trusted peer re-keys every
//! per-IP defense onto the forwarded client (TCP), `X-Forwarded-For` does
//! the same for HTTP, spoofed metadata from untrusted peers is ignored or
//! refused and counted, and with `trusted_proxies` empty the transports
//! behave exactly as before (the whole `connection_abuse` suite pins that).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::ConnRejectReason;
use fluxum_core::net::IpSet;
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage};
use fluxum_server::ShardContext;
use fluxum_server::clientip::encode_v2_preamble;
use fluxum_server::connguard::{ConnGuard, ConnLimits};
use fluxum_server::http::{self, CONTENT_TYPE, HttpOptions};
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 1;

static CHAT_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
static CHAT: TableSchema = TableSchema {
    name: "Chat",
    columns: CHAT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// A provider that accepts only the token `good` (SEC-031 interplay tests).
#[derive(Debug)]
struct PickyProvider;
impl AuthProvider for PickyProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        if token == b"good" {
            Ok(AuthClaims {
                canonical_token: token.to_vec(),
                display_name: None,
                roles: Vec::new(),
                expires_at: None,
            })
        } else {
            Err("bad token".into())
        }
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        Ok(token.to_vec())
    }
}

fn make_ctx(limits: ConnLimits, trusted: &[&str]) -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT]).unwrap();
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
        Arc::new(ReducerRegistry::from_defs(std::iter::empty()).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("proxy-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(PickyProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_conn_guard(Arc::new(ConnGuard::new(limits)));
    ctx.set_trusted_proxies(
        IpSet::parse(&trusted.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()).unwrap(),
    );
    ctx
}

/// A permissive base with every enforced limit off, to switch one on per
/// test in isolation.
fn permissive() -> ConnLimits {
    ConnLimits {
        max_conns_per_ip: None,
        accept_rate_per_sec: None,
        handshake_timeout: None,
        failed_auth_threshold: None,
        ..ConnLimits::default()
    }
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn auth_bytes(token: &[u8], id: u32) -> Vec<u8> {
    let codec = FrameCodec::default();
    let body = ClientMessage::Authenticate(Authenticate {
        id,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    })
    .encode()
    .unwrap();
    codec.encode(&body).unwrap()
}

/// Whether the peer refused the connection (clean EOF or reset, never data).
async fn is_refused(stream: &mut TcpStream) -> bool {
    let mut chunk = [0u8; 64];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
        Err(_) => false,
    }
}

/// Read one server message from `stream`, or `None` on close/timeout.
async fn recv(stream: &mut TcpStream) -> Option<ServerMessage> {
    let codec = FrameCodec::default();
    let mut buf = Vec::new();
    loop {
        if let Ok(Some((fluxum_protocol::Frame::Body(body), _consumed))) = codec.decode(&buf) {
            return Some(ServerMessage::decode(body).unwrap());
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => return None,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return None,
        }
    }
}

/// Connect, optionally send a PROXY v2 preamble for `client`, then a good
/// `Authenticate`; report whether the server answered with an `AuthResult`.
async fn tcp_auth_via(addr: std::net::SocketAddr, client: Option<IpAddr>) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut bytes = Vec::new();
    if let Some(client) = client {
        bytes.extend_from_slice(&encode_v2_preamble(client, 40000));
    }
    bytes.extend_from_slice(&auth_bytes(b"good", 1));
    stream.write_all(&bytes).await.unwrap();
    let ok = matches!(recv(&mut stream).await, Some(ServerMessage::AuthResult(_)));
    (stream, ok)
}

// --- SEC-036 (TCP): the guard keys on the forwarded client, not the proxy ---------

#[tokio::test(flavor = "multi_thread")]
async fn tcp_per_ip_cap_bites_the_forwarded_client_not_the_proxy() {
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(1);
    let ctx = make_ctx(limits, &["127.0.0.1"]);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Two *different* clients through the same proxy: both admitted, even
    // though the proxy (the socket peer) now holds two connections — the cap
    // is keyed on the resolved client, not the proxy.
    let (_a, ok_a) = tcp_auth_via(addr, Some(ip("203.0.113.1"))).await;
    assert!(ok_a, "client A authenticates through the proxy");
    let (_b, ok_b) = tcp_auth_via(addr, Some(ip("203.0.113.2"))).await;
    assert!(ok_b, "client B is not throttled by client A's slot");

    // The same client again: its own cap of 1 refuses it.
    let mut c = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.1"), 40001);
    bytes.extend_from_slice(&auth_bytes(b"good", 1));
    c.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut c).await,
        "client A's second concurrent connection is capped"
    );
    assert_eq!(ctx.metrics().conn_rejected(ConnRejectReason::ConnCap), 1);

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_failed_auth_backoff_follows_the_forwarded_client() {
    let mut limits = permissive();
    limits.failed_auth_threshold = Some(2);
    limits.failed_auth_backoff_base = Duration::from_secs(5);
    limits.failed_auth_backoff_max = Duration::from_secs(10);
    let ctx = make_ctx(limits, &["127.0.0.1"]);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Client X brute-forces through the proxy: two bad tokens on one
    // connection arm the backoff for X's *resolved* IP.
    let mut attacker = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.66"), 40100);
    bytes.extend_from_slice(&auth_bytes(b"bad", 1));
    attacker.write_all(&bytes).await.unwrap();
    assert!(matches!(
        recv(&mut attacker).await,
        Some(ServerMessage::Error(_))
    ));
    attacker.write_all(&auth_bytes(b"bad", 2)).await.unwrap();
    assert!(matches!(
        recv(&mut attacker).await,
        Some(ServerMessage::Error(_))
    ));

    // X's next connection is refused at admission — even with a good token.
    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.66"), 40101);
    bytes.extend_from_slice(&auth_bytes(b"good", 1));
    blocked.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut blocked).await,
        "the brute-forcing client is backed off"
    );
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::FailedAuth) >= 1);

    // An innocent client through the same proxy sails in: the backoff never
    // attached to the proxy's IP.
    let (_ok, authed) = tcp_auth_via(addr, Some(ip("203.0.113.77"))).await;
    assert!(authed, "another client behind the proxy is unaffected");

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_trusted_peer_without_preamble_is_its_own_client() {
    let ctx = make_ctx(permissive(), &["127.0.0.1"]);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();

    // The proxy host itself (a probe, a local tool) opens with ordinary
    // frames: served under its own IP.
    let (_c, ok) = tcp_auth_via(server.local_addr, None).await;
    assert!(ok, "no preamble from a trusted peer is still a client");
    assert_eq!(
        ctx.metrics().conn_rejected(ConnRejectReason::ProxyPreamble),
        0
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_preamble_from_an_untrusted_peer_is_refused_and_counted() {
    // Proxy awareness is ON, but loopback is not the trusted proxy.
    let ctx = make_ctx(permissive(), &["203.0.113.99"]);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();

    let mut spoofer = TcpStream::connect(server.local_addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("10.0.0.1"), 40200);
    bytes.extend_from_slice(&auth_bytes(b"good", 1));
    spoofer.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut spoofer).await,
        "a preamble from an untrusted peer is a protocol error"
    );
    assert_eq!(
        ctx.metrics().conn_rejected(ConnRejectReason::ProxyPreamble),
        1
    );

    // An ordinary client (no preamble) from the same untrusted IP is served.
    let (_c, ok) = tcp_auth_via(server.local_addr, None).await;
    assert!(ok, "ordinary clients are untouched by detection");

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_malformed_preamble_from_a_trusted_proxy_is_refused_and_counted() {
    let ctx = make_ctx(permissive(), &["127.0.0.1"]);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();

    let mut broken = TcpStream::connect(server.local_addr).await.unwrap();
    // A correct signature followed by an unsupported version nibble.
    let mut bytes = encode_v2_preamble(ip("203.0.113.1"), 40300);
    bytes[12] = 0x11;
    broken.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut broken).await,
        "a malformed preamble from the trusted proxy is dropped"
    );
    assert_eq!(
        ctx.metrics().conn_rejected(ConnRejectReason::ProxyPreamble),
        1
    );
    server.shutdown();
}

// --- SEC-035 (HTTP): X-Forwarded-For under the rightmost-untrusted rule -----------

/// A minimal HTTP response: status and the `Fluxum-Session` header if any.
struct Resp {
    status: u16,
    session: Option<String>,
}

/// Send `POST /rpc` with a good `Authenticate` and optional extra headers on
/// an existing stream; parse the response head.
async fn post_auth(stream: &mut TcpStream, extra_headers: &[(&str, &str)]) -> Resp {
    let body = auth_bytes(b"good", 1);
    let mut req = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    let mut bytes = req.into_bytes();
    bytes.extend_from_slice(&body);
    stream.write_all(&bytes).await.unwrap();

    // Read the response head (+ Content-Length body, drained but unused).
    let mut buf = Vec::new();
    let headers_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break buf.len(),
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => break buf.len(),
        }
    };
    let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut session = None;
    let mut content_length = 0usize;
    for line in head.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim();
            if k == "fluxum-session" {
                session = Some(v.to_owned());
            } else if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
        }
    }
    // Drain the body so the connection is reusable.
    let mut body_got = buf.len().saturating_sub(headers_end + 4);
    while body_got < content_length {
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => body_got += n,
            Ok(Err(_)) => break,
        }
    }
    Resp { status, session }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_per_ip_cap_bites_the_forwarded_client_not_the_proxy() {
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(1);
    let ctx = make_ctx(limits, &["127.0.0.1"]);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Client A authenticates through the proxy and keeps its connection
    // open, holding A's single slot.
    let mut conn_a = TcpStream::connect(addr).await.unwrap();
    let resp = post_auth(&mut conn_a, &[("X-Forwarded-For", "203.0.113.1")]).await;
    assert_eq!(resp.status, 200);
    assert!(resp.session.is_some(), "a session is minted for client A");

    // Client B through the same proxy: its own slot, admitted.
    let mut conn_b = TcpStream::connect(addr).await.unwrap();
    let resp = post_auth(&mut conn_b, &[("X-Forwarded-For", "203.0.113.2")]).await;
    assert_eq!(resp.status, 200, "client B is not throttled by client A");

    // Client A again on a new connection: refused (dropped without bytes).
    let mut conn_a2 = TcpStream::connect(addr).await.unwrap();
    let resp = post_auth(&mut conn_a2, &[("X-Forwarded-For", "203.0.113.1")]).await;
    assert_eq!(resp.status, 0, "client A's second connection is dropped");
    assert_eq!(ctx.metrics().conn_rejected(ConnRejectReason::ConnCap), 1);

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_spoofed_xff_from_an_untrusted_client_is_ignored() {
    // Awareness ON, loopback untrusted: the header must be inert.
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(8);
    let ctx = make_ctx(limits, &["203.0.113.99"]);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    let mut conn = TcpStream::connect(server.local_addr).await.unwrap();
    let resp = post_auth(&mut conn, &[("X-Forwarded-For", "garbage, 10.0.0.1")]).await;
    // Served under the socket peer: even a *malformed* spoofed header is
    // never parsed, so it cannot 400 the request.
    assert_eq!(resp.status, 200, "spoofed XFF is ignored, not honored");
    assert_eq!(
        ctx.metrics().conn_rejected(ConnRejectReason::ProxyHeader),
        0
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_malformed_xff_from_a_trusted_proxy_is_a_400_and_counted() {
    let ctx = make_ctx(permissive(), &["127.0.0.1"]);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    let mut conn = TcpStream::connect(server.local_addr).await.unwrap();
    let resp = post_auth(&mut conn, &[("X-Forwarded-For", "not-an-ip")]).await;
    assert_eq!(
        resp.status, 400,
        "garbage attribution from the proxy is rejected"
    );
    assert_eq!(
        ctx.metrics().conn_rejected(ConnRejectReason::ProxyHeader),
        1
    );
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_rightmost_untrusted_rule_skips_trusted_hops() {
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(1);
    // Loopback and an inner proxy tier are both trusted.
    let ctx = make_ctx(limits, &["127.0.0.1", "10.0.0.0/8"]);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // The chain names a forged hop, the client, then an inner trusted proxy:
    // resolution must land on the client (203.0.113.5), not the forgery.
    let chain = "6.6.6.6, 203.0.113.5, 10.0.0.7";
    let mut conn_a = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        post_auth(&mut conn_a, &[("X-Forwarded-For", chain)])
            .await
            .status,
        200
    );

    // Same client, different forged prefix: same resolved IP → capped.
    let chain2 = "9.9.9.9, 203.0.113.5, 10.0.0.7";
    let mut conn_a2 = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        post_auth(&mut conn_a2, &[("X-Forwarded-For", chain2)])
            .await
            .status,
        0,
        "the resolved client is capped regardless of forged prefixes"
    );
    assert_eq!(ctx.metrics().conn_rejected(ConnRejectReason::ConnCap), 1);

    server.shutdown();
}
