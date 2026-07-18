//! SPEC-026 §4 (SEC-030/031/032) — pre-auth connection-abuse protection over
//! real loopback sockets: the per-IP concurrent-connection cap and the
//! handshake time budget on the TCP transport, the failed-`Authenticate`
//! backoff throttling a brute-force's next connection, the shared guard
//! gating the HTTP transport too, and `fluxum_conn_rejected_total` counting
//! each rejection class.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::ConnRejectReason;
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage};
use fluxum_server::ShardContext;
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

/// A provider that accepts only the token `good` (SEC-031 failed-auth test).
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
        fluxum_core::auth::server_identity("abuse-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(PickyProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_conn_guard(Arc::new(ConnGuard::new(limits)));
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

async fn auth_frame(stream: &mut TcpStream, token: &[u8], id: u32) {
    let codec = FrameCodec::default();
    let body = ClientMessage::Authenticate(Authenticate {
        id,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
    })
    .encode()
    .unwrap();
    stream
        .write_all(&codec.encode(&body).unwrap())
        .await
        .unwrap();
}

/// Whether the peer refused the connection: a guard-dropped socket reads a
/// clean EOF on some platforms and a connection-reset error on others
/// (Windows resets an unread, force-closed socket). A timeout means the
/// connection is still open — not refused.
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
    let deadline = Duration::from_secs(2);
    loop {
        if let Ok(Some((fluxum_protocol::Frame::Body(body), consumed))) = codec.decode(&buf) {
            let msg = ServerMessage::decode(body).unwrap();
            let _ = consumed;
            return Some(msg);
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(deadline, stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => return None,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return None,
        }
    }
}

// --- SEC-030: per-IP concurrent-connection cap (TCP) ------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_third_concurrent_connection_from_one_ip_is_refused() {
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(2);
    let ctx = make_ctx(limits);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Two authenticated connections occupy both slots.
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    auth_frame(&mut c1, b"good", 1).await;
    assert!(matches!(
        recv(&mut c1).await,
        Some(ServerMessage::AuthResult(_))
    ));
    let mut c2 = TcpStream::connect(addr).await.unwrap();
    auth_frame(&mut c2, b"good", 1).await;
    assert!(matches!(
        recv(&mut c2).await,
        Some(ServerMessage::AuthResult(_))
    ));

    // The third is accepted by the OS but dropped by the guard before any
    // session exists: it never gets an AuthResult, and the reject is counted.
    let mut c3 = TcpStream::connect(addr).await.unwrap();
    assert!(
        is_refused(&mut c3).await,
        "a capped connection is closed, not served"
    );
    assert_eq!(ctx.metrics().conn_rejected(ConnRejectReason::ConnCap), 1);

    // Freeing a slot lets a new connection in — the cap tracks live count.
    drop(c1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut c4 = TcpStream::connect(addr).await.unwrap();
    auth_frame(&mut c4, b"good", 1).await;
    assert!(matches!(
        recv(&mut c4).await,
        Some(ServerMessage::AuthResult(_))
    ));

    server.shutdown();
}

// --- SEC-031: handshake time budget (slowloris) -----------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_that_never_authenticates_is_dropped_after_the_budget() {
    let mut limits = permissive();
    limits.handshake_timeout = Some(Duration::from_millis(300));
    let ctx = make_ctx(limits);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();

    // Connect and send nothing: the handshake budget must close it.
    let mut slow = TcpStream::connect(server.local_addr).await.unwrap();
    let start = std::time::Instant::now();
    let mut chunk = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(3), slow.read(&mut chunk))
        .await
        .expect("the slow handshake must be dropped, not left open")
        .unwrap();
    assert_eq!(n, 0, "an unauthenticated connection is dropped");
    assert!(
        start.elapsed() >= Duration::from_millis(300),
        "dropped only after the budget elapsed: {:?}",
        start.elapsed()
    );
    assert_eq!(
        ctx.metrics()
            .conn_rejected(ConnRejectReason::HandshakeBudget),
        1
    );

    // A well-behaved client that authenticates promptly is unaffected.
    let mut ok = TcpStream::connect(server.local_addr).await.unwrap();
    auth_frame(&mut ok, b"good", 1).await;
    assert!(matches!(
        recv(&mut ok).await,
        Some(ServerMessage::AuthResult(_))
    ));

    server.shutdown();
}

// --- SEC-031: failed-auth backoff throttles a brute-force -------------------------

#[tokio::test(flavor = "multi_thread")]
async fn repeated_failed_auth_backs_off_the_next_connection() {
    let mut limits = permissive();
    limits.failed_auth_threshold = Some(3);
    limits.failed_auth_backoff_base = Duration::from_secs(2);
    limits.failed_auth_backoff_max = Duration::from_secs(10);
    let ctx = make_ctx(limits);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Three bad tokens on one connection: each is answered 401 (connection
    // kept open, AUTH-020), and the third crosses the threshold.
    let mut attacker = TcpStream::connect(addr).await.unwrap();
    for id in 1..=3 {
        auth_frame(&mut attacker, b"bad", id).await;
        assert!(
            matches!(recv(&mut attacker).await, Some(ServerMessage::Error(_))),
            "a bad token is a 401"
        );
    }

    // The IP is now in backoff: its next connection is refused at accept,
    // before it can even present a (now-correct) token.
    let mut blocked = TcpStream::connect(addr).await.unwrap();
    assert!(
        is_refused(&mut blocked).await,
        "a brute-forcing IP is refused even with a good token"
    );
    assert!(ctx.metrics().conn_rejected(ConnRejectReason::FailedAuth) >= 1);

    server.shutdown();
}

// --- SEC-032: the rejection metric is exposed with every reason label -------------

#[tokio::test(flavor = "multi_thread")]
async fn conn_rejected_metric_is_exposed_per_reason() {
    let ctx = make_ctx(permissive());
    // Even at zero, every reason label is present so an alert never goes
    // stale-for-lack-of-series.
    let text = ctx.metrics().prometheus(0);
    for reason in ["conn_cap", "accept_rate", "failed_auth", "handshake_budget"] {
        assert!(
            text.contains(&format!(
                "fluxum_conn_rejected_total{{shard=\"1\", reason=\"{reason}\"}} 0"
            )),
            "missing reason {reason} in:\n{text}"
        );
    }
}

// --- SEC-030: the shared guard gates the HTTP transport too ------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_http_transport_enforces_the_same_per_ip_cap() {
    let mut limits = permissive();
    limits.max_conns_per_ip = Some(1);
    let ctx = make_ctx(limits);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    let addr = server.local_addr;

    // Hold one connection open (no request yet): it owns the only slot for
    // its whole life via the permit.
    let _held = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A second connection from the same IP is dropped at accept.
    let mut refused = TcpStream::connect(addr).await.unwrap();
    assert!(
        is_refused(&mut refused).await,
        "the HTTP accept path enforces the shared per-IP cap"
    );
    assert_eq!(ctx.metrics().conn_rejected(ConnRejectReason::ConnCap), 1);

    server.shutdown();
}
