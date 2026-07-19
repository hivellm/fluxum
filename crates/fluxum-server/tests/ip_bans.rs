//! SPEC-026 SEC-033/034 — IP blocklist/allowlist, runtime bans, and the
//! global connection ceiling over real loopback sockets. Distinct client
//! IPs are simulated through the SEC-035/036 trusted-proxy resolution
//! (loopback is the trusted proxy; PROXY v2 preambles and `X-Forwarded-For`
//! name the clients), which doubles as proof the two features compose: bans
//! bite the *resolved* client, never the proxy.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::Config;
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
use fluxum_server::http::{self, HttpOptions};
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

#[derive(Debug)]
struct AnyProvider;
impl AuthProvider for AnyProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        Ok(AuthClaims {
            canonical_token: token.to_vec(),
            display_name: None,
            roles: Vec::new(),
            expires_at: None,
        })
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        Ok(token.to_vec())
    }
}

fn make_ctx() -> Arc<ShardContext> {
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
        fluxum_core::auth::server_identity("bans-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(AnyProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    let limits = ConnLimits {
        accept_rate_per_sec: None,
        max_conns_per_ip: None,
        ..ConnLimits::default()
    };
    ctx.set_conn_guard(Arc::new(ConnGuard::new(limits)));
    // Loopback is the trusted proxy: preambles/XFF simulate distinct clients.
    ctx.set_trusted_proxies(IpSet::parse(&["127.0.0.1".to_owned()]).unwrap());
    ctx
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

fn auth_bytes(id: u32) -> Vec<u8> {
    let codec = FrameCodec::default();
    let body = ClientMessage::Authenticate(Authenticate {
        id,
        token: b"anyone".to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    })
    .encode()
    .unwrap();
    codec.encode(&body).unwrap()
}

async fn is_refused(stream: &mut TcpStream) -> bool {
    let mut chunk = [0u8; 64];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
        Err(_) => false,
    }
}

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

/// Connect as `client` (via preamble) and try to authenticate.
async fn tcp_auth_as(addr: std::net::SocketAddr, client: IpAddr) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(client, 40000);
    bytes.extend_from_slice(&auth_bytes(1));
    stream.write_all(&bytes).await.unwrap();
    let ok = matches!(recv(&mut stream).await, Some(ServerMessage::AuthResult(_)));
    (stream, ok)
}

/// One admin HTTP request; returns (status, body).
async fn admin(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut raw)).await;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, payload)
}

// --- SEC-033: runtime bans over the admin API, biting both transports -------------

#[tokio::test(flavor = "multi_thread")]
async fn a_runtime_ban_refuses_both_transports_and_unban_readmits() {
    let ctx = make_ctx();
    let tcp_server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let http_server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let tcp_addr = tcp_server.local_addr;
    let http_addr = http_server.local_addr;

    // Sanity: the client connects before the ban.
    let (_pre, ok) = tcp_auth_as(tcp_addr, ip("10.9.9.9")).await;
    assert!(ok);

    // Ban via the admin API.
    let (status, body) = admin(http_addr, "POST", "/bans", r#"{"entry": "10.9.9.9"}"#).await;
    assert_eq!(status, 200, "ban accepted: {body}");

    // TCP: the client's next connection is refused before auth.
    let mut blocked = TcpStream::connect(tcp_addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("10.9.9.9"), 40001);
    bytes.extend_from_slice(&auth_bytes(1));
    blocked.write_all(&bytes).await.unwrap();
    assert!(is_refused(&mut blocked).await, "banned on TCP");

    // HTTP: the same client via X-Forwarded-For is dropped pre-auth.
    let mut http_conn = TcpStream::connect(http_addr).await.unwrap();
    let post = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {}\r\n\
         X-Forwarded-For: 10.9.9.9\r\nContent-Length: {}\r\n\r\n",
        http::CONTENT_TYPE,
        auth_bytes(1).len()
    );
    http_conn.write_all(post.as_bytes()).await.unwrap();
    http_conn.write_all(&auth_bytes(1)).await.unwrap();
    assert!(is_refused(&mut http_conn).await, "banned on HTTP");
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::Blocked) >= 2);

    // The listing shows the runtime ban with no TTL.
    let (status, body) = admin(http_addr, "GET", "/bans", "").await;
    assert_eq!(status, 200);
    assert!(body.contains("10.9.9.9"), "ban listed: {body}");

    // Unban readmits immediately; a second delete is a 404.
    let (status, _) = admin(http_addr, "DELETE", "/bans/10.9.9.9", "").await;
    assert_eq!(status, 200);
    let (_post, ok) = tcp_auth_as(tcp_addr, ip("10.9.9.9")).await;
    assert!(ok, "unbanned client is readmitted");
    let (status, _) = admin(http_addr, "DELETE", "/bans/10.9.9.9", "").await;
    assert_eq!(status, 404);

    tcp_server.shutdown();
    http_server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cidr_ban_with_ttl_expires_and_readmits() {
    let ctx = make_ctx();
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // A short-TTL CIDR ban (installed directly; the admin route's unit is
    // seconds and this test should not sleep that long).
    ctx.conn_guard()
        .ban("203.0.113.0/24", Some(Duration::from_millis(200)))
        .unwrap();

    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.7"), 40100);
    bytes.extend_from_slice(&auth_bytes(1));
    blocked.write_all(&bytes).await.unwrap();
    assert!(is_refused(&mut blocked).await, "the CIDR ban bites");

    tokio::time::sleep(Duration::from_millis(300)).await;
    let (_c, ok) = tcp_auth_as(addr, ip("203.0.113.7")).await;
    assert!(ok, "the expired TTL ban readmits by itself");

    server.shutdown();
}

// --- SEC-034: the global ceiling refuses connection N+1 across distinct IPs -------

#[tokio::test(flavor = "multi_thread")]
async fn the_global_ceiling_refuses_the_next_distinct_ip_and_frees_on_disconnect() {
    let ctx = make_ctx();
    ctx.conn_guard().set_max_total_conns(2);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    let (c1, ok1) = tcp_auth_as(addr, ip("198.51.100.1")).await;
    let (_c2, ok2) = tcp_auth_as(addr, ip("198.51.100.2")).await;
    assert!(ok1 && ok2);

    // A third, wholly distinct address: past every per-IP check, but the
    // process-wide ceiling is full.
    let mut third = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("198.51.100.3"), 40200);
    bytes.extend_from_slice(&auth_bytes(1));
    third.write_all(&bytes).await.unwrap();
    assert!(is_refused(&mut third).await, "connection N+1 is refused");
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::GlobalCap) >= 1);

    // A freed slot readmits a newcomer.
    drop(c1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_c4, ok4) = tcp_auth_as(addr, ip("198.51.100.4")).await;
    assert!(ok4, "a freed slot readmits");

    server.shutdown();
}

// --- OPS-040: the static lists hot-apply through the reload publish path ----------

#[tokio::test(flavor = "multi_thread")]
async fn static_lists_hot_apply_via_the_reload_publish_path() {
    let ctx = make_ctx();
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    let (_pre, ok) = tcp_auth_as(addr, ip("203.0.113.5")).await;
    assert!(ok, "admitted before the reload");

    // The operator's new config blocklists the range; publish_reloadable is
    // the exact path `POST /config/reload` and SIGHUP take.
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1".into()];
    config.server.connection_limits.blocklist = vec!["203.0.113.0/24".into()];
    ctx.publish_reloadable(&config, None);

    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.5"), 40300);
    bytes.extend_from_slice(&auth_bytes(1));
    blocked.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut blocked).await,
        "the reloaded blocklist bites"
    );

    // An exclusive allowlist admits its members (and the proxy host) only.
    let mut config = Config::default();
    config.server.trusted_proxies = vec!["127.0.0.1".into()];
    config.server.connection_limits.allowlist = vec!["127.0.0.1".into(), "198.51.100.0/24".into()];
    ctx.publish_reloadable(&config, None);

    let (_in, ok) = tcp_auth_as(addr, ip("198.51.100.9")).await;
    assert!(ok, "an allowlisted client connects");
    let mut outside = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip("203.0.113.5"), 40301);
    bytes.extend_from_slice(&auth_bytes(1));
    outside.write_all(&bytes).await.unwrap();
    assert!(
        is_refused(&mut outside).await,
        "everyone outside the exclusive allowlist is refused"
    );

    server.shutdown();
}
