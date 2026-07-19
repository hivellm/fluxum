//! SPEC-026 SEC-040/041 — overload resilience over real loopback sockets:
//! a many-distinct-IP flood keeps guard memory bounded (pressure eviction),
//! admission control sheds pre-auth work while established sessions keep
//! working, and the moment load drains the server recovers with no
//! cool-down. Distinct client IPs ride the SEC-035/036 trusted-proxy
//! resolution (loopback is the trusted proxy).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::{ConnRejectReason, OverloadState};
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

fn make_ctx(limits: ConnLimits) -> Arc<ShardContext> {
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
        fluxum_core::auth::server_identity("overload-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(AnyProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_conn_guard(Arc::new(ConnGuard::new(limits)));
    ctx.set_trusted_proxies(IpSet::parse(&["127.0.0.1".to_owned()]).unwrap());
    ctx
}

fn permissive() -> ConnLimits {
    ConnLimits {
        max_conns_per_ip: None,
        accept_rate_per_sec: None,
        handshake_timeout: None,
        failed_auth_threshold: None,
        overload_shed: None,
        overload_shed_all: None,
        max_tracked_ips: None,
        ..ConnLimits::default()
    }
}

fn client(n: u8) -> IpAddr {
    IpAddr::from([203, 0, 113, n])
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

async fn tcp_auth_as(addr: std::net::SocketAddr, ip: IpAddr) -> (TcpStream, bool) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut bytes = encode_v2_preamble(ip, 40000);
    bytes.extend_from_slice(&auth_bytes(1));
    stream.write_all(&bytes).await.unwrap();
    let ok = matches!(recv(&mut stream).await, Some(ServerMessage::AuthResult(_)));
    (stream, ok)
}

// --- SEC-040: a many-distinct-IP flood cannot grow guard memory -------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_distinct_ip_flood_keeps_guard_memory_under_the_cap() {
    let mut limits = permissive();
    // A slow bucket makes released entries linger, so only pressure
    // eviction (not the idle GC) can be what bounds the map.
    limits.accept_rate_per_sec = Some(1.0);
    limits.max_tracked_ips = Some(8);
    let ctx = make_ctx(limits);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // 60 distinct clients connect, authenticate, and leave.
    for n in 1..=60u8 {
        let (stream, ok) = tcp_auth_as(addr, client(n)).await;
        assert!(ok, "client {n} authenticates");
        drop(stream);
    }
    let tracked = ctx.conn_guard().tracked_ips();
    assert!(
        tracked <= 8,
        "guard memory stays under the cap: tracked {tracked}"
    );
    assert!(
        ctx.conn_guard().evictions_total() > 0,
        "the flood was absorbed by pressure eviction, not luck"
    );

    server.shutdown();
}

// --- SEC-041: established sessions outlive the shed; recovery is instant ----------

#[tokio::test(flavor = "multi_thread")]
async fn established_tcp_sessions_survive_shed_and_recovery_is_instant() {
    let mut limits = permissive();
    limits.overload_shed = Some(0.2);
    limits.overload_shed_all = Some(0.9);
    let ctx = make_ctx(limits);
    ctx.conn_guard().set_max_total_conns(10);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Two established clients push load to 0.2 → shed-preauth.
    let (mut alice, ok) = tcp_auth_as(addr, client(1)).await;
    assert!(ok);
    let (_bob, ok) = tcp_auth_as(addr, client(2)).await;
    assert!(ok);
    assert_eq!(ctx.overload_state(), OverloadState::ShedPreauth);

    // A newcomer is shed at accept with zero response bytes...
    let mut shed = TcpStream::connect(addr).await.unwrap();
    assert!(is_refused(&mut shed).await, "pre-auth newcomer is shed");
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::Overload) >= 1);

    // ...while the established session keeps getting answers.
    alice.write_all(&auth_bytes(7)).await.unwrap();
    assert!(
        recv(&mut alice).await.is_some(),
        "an established session still gets responses mid-shed"
    );

    // Load drains → the very next connection is admitted, no cool-down.
    drop(alice);
    drop(_bob);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ctx.overload_state(), OverloadState::Normal);
    let (_carol, ok) = tcp_auth_as(addr, client(3)).await;
    assert!(ok, "recovery is immediate once the load drains");

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_sheds_preauth_but_serves_live_sessions_and_admin() {
    let mut limits = permissive();
    // First conn (1/10 = 0.1) stays calm; every later conn's own permit
    // (2/10 = 0.2) crosses the threshold, so the shed decision is made
    // while its request is in hand.
    limits.overload_shed = Some(0.2);
    limits.overload_shed_all = Some(0.99);
    let ctx = make_ctx(limits);
    ctx.conn_guard().set_max_total_conns(10);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Authenticate while calm; hold the connection so its permit keeps the
    // load at 0.1 → shed-preauth.
    let mut session_conn = TcpStream::connect(addr).await.unwrap();
    let auth = auth_bytes(1);
    let post = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n\r\n",
        auth.len()
    );
    session_conn.write_all(post.as_bytes()).await.unwrap();
    session_conn.write_all(&auth).await.unwrap();
    let head = read_head(&mut session_conn).await;
    assert!(head.starts_with("HTTP/1.1 200"), "auth while calm: {head}");
    let token = head
        .lines()
        .find_map(|l| l.strip_prefix("Fluxum-Session: "))
        .expect("session header")
        .trim()
        .to_owned();

    // Pre-auth POST on a fresh connection: dropped without a response.
    let mut preauth = TcpStream::connect(addr).await.unwrap();
    let post = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n\r\n",
        auth.len()
    );
    preauth.write_all(post.as_bytes()).await.unwrap();
    preauth.write_all(&auth).await.unwrap();
    assert!(is_refused(&mut preauth).await, "pre-auth /rpc is shed");
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::Overload) >= 1);

    // A request carrying the live session still works (empty batch → 200).
    let mut live = TcpStream::connect(addr).await.unwrap();
    let post = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\n\
         Fluxum-Session: {token}\r\nContent-Length: 0\r\n\r\n"
    );
    live.write_all(post.as_bytes()).await.unwrap();
    let head = read_head(&mut live).await;
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "an established session is served mid-shed: {head}"
    );

    // The admin surface is never gated: the operator can still see and act.
    let mut admin = TcpStream::connect(addr).await.unwrap();
    admin
        .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let head = read_head(&mut admin).await;
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "/health answers mid-shed: {head}"
    );

    server.shutdown();
}

/// Read one HTTP response head (through the blank line).
async fn read_head(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            return String::from_utf8_lossy(&buf[..pos]).into_owned();
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => return String::from_utf8_lossy(&buf).into_owned(),
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return String::from_utf8_lossy(&buf).into_owned(),
        }
    }
}
