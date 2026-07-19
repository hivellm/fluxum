//! SPEC-026 SEC-050..053 — session-token hijack hardening over real loopback
//! HTTP: CSPRNG tokens unpredictable given a known identity, anti-fixation
//! (an unknown token is never adopted), IP binding, rotation with a grace
//! window, absolute lifetime, and operator revocation. Distinct client IPs
//! ride the SEC-035 trusted-proxy resolution (loopback is the trusted proxy).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::SessionConfig;
use fluxum_core::metrics::SessionRejectReason;
use fluxum_core::net::IpSet;
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, CONTENT_TYPE, HttpOptions};
use fluxum_server::session_sec::SessionPolicy;

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

/// Accepts any non-empty token as its own identity, so distinct tokens →
/// distinct identities (the SEC-050 unpredictability test needs that).
#[derive(Debug)]
struct IdentityProvider;
impl AuthProvider for IdentityProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        if token.is_empty() {
            return Err("empty token".into());
        }
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
        fluxum_core::auth::server_identity("session-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth =
        Authenticator::with_provider(Arc::new(IdentityProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_trusted_proxies(IpSet::parse(&["127.0.0.1".to_owned()]).unwrap());
    ctx
}

async fn start(session: SessionConfig) -> (Arc<ShardContext>, http::HttpServer) {
    let ctx = make_ctx();
    let server = http::serve(
        Arc::clone(&ctx),
        "127.0.0.1:0",
        HttpOptions {
            session: SessionPolicy::from_config(&session),
            ..HttpOptions::default()
        },
    )
    .await
    .unwrap();
    (ctx, server)
}

fn auth_frame(token: &[u8]) -> Vec<u8> {
    let codec = FrameCodec::default();
    let body = ClientMessage::Authenticate(Authenticate {
        id: 1,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    })
    .encode()
    .unwrap();
    codec.encode(&body).unwrap()
}

struct Resp {
    status: u16,
    session: Option<String>,
    body: String,
}

/// One `POST /rpc` with the given body and headers; returns status +
/// Fluxum-Session + textual body.
async fn post(addr: std::net::SocketAddr, body: &[u8], headers: &[(&str, &str)]) -> Resp {
    let mut req = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("Connection: close\r\n\r\n");
    let mut bytes = req.into_bytes();
    bytes.extend_from_slice(body);
    raw_request(addr, &bytes).await
}

/// Any raw HTTP request → parsed status + Fluxum-Session + body text. Reads
/// exactly the header block plus its `Content-Length` body, so it never
/// blocks waiting for the (keep-alive) server to close the socket.
async fn raw_request(addr: std::net::SocketAddr, bytes: &[u8]) -> Resp {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(bytes).await.unwrap();

    let mut raw = Vec::new();
    let headers_end = loop {
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break raw.len(),
            Ok(Ok(n)) => raw.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => break raw.len(),
        }
    };
    let head = String::from_utf8_lossy(&raw[..headers_end]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let session = head.lines().find_map(|l| {
        l.strip_prefix("Fluxum-Session: ")
            .or_else(|| l.strip_prefix("fluxum-session: "))
            .map(|s| s.trim().to_owned())
    });
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().to_owned())
        })
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    // Read the remaining body bytes up to Content-Length.
    let mut body = raw.split_off(headers_end.min(raw.len()));
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => body.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => break,
        }
    }
    Resp {
        status,
        session,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

async fn admin(addr: std::net::SocketAddr, method: &str, path: &str) -> Resp {
    let req = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    raw_request(addr, req.as_bytes()).await
}

/// Authenticate and return the issued token.
async fn authenticate(addr: std::net::SocketAddr, token: &[u8], xff: Option<&str>) -> String {
    let headers: Vec<(&str, &str)> = match xff {
        Some(ip) => vec![("X-Forwarded-For", ip)],
        None => vec![],
    };
    let resp = post(addr, &auth_frame(token), &headers).await;
    assert_eq!(resp.status, 200, "auth ok: {}", resp.body);
    resp.session.expect("session issued")
}

// --- SEC-050: CSPRNG tokens, unpredictable given a known identity -----------------

#[tokio::test(flavor = "multi_thread")]
async fn tokens_are_unpredictable_and_independent_of_identity() {
    let (_ctx, server) = start(SessionConfig::default()).await;
    let addr = server.local_addr;

    // Same identity, two sessions → two unrelated 128-bit tokens.
    let t1 = authenticate(addr, b"alice", None).await;
    let t2 = authenticate(addr, b"alice", None).await;
    assert_eq!(t1.len(), 32, "128-bit hex token");
    assert_ne!(t1, t2, "no counter to walk — each mint is independent");

    // The token is not any hash of the identity: knowing the identity string
    // (here literally the token "alice") tells an attacker nothing.
    use sha2::{Digest, Sha256};
    let id_hash: String = Sha256::digest(b"alice")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_ne!(t1, id_hash);
    assert!(!id_hash.contains(&t1));

    server.shutdown();
}

// --- SEC-050: anti-fixation — an unknown token is never adopted -------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_token_is_never_adopted() {
    let (ctx, server) = start(SessionConfig::default()).await;
    let addr = server.local_addr;

    // A made-up token with no re-auth: stale → 404, counted.
    let resp = post(
        addr,
        &[],
        &[("Fluxum-Session", "deadbeefdeadbeefdeadbeefdeadbeef")],
    )
    .await;
    assert_eq!(resp.status, 404);

    // A made-up token *with* an Authenticate: a fresh session is minted, and
    // the server issues its OWN new token — never the attacker's value.
    let resp = post(
        addr,
        &auth_frame(b"mallory"),
        &[("Fluxum-Session", "deadbeefdeadbeefdeadbeefdeadbeef")],
    )
    .await;
    assert_eq!(resp.status, 200);
    let issued = resp.session.expect("a fresh token is minted");
    assert_ne!(
        issued, "deadbeefdeadbeefdeadbeefdeadbeef",
        "the supplied token is never adopted"
    );
    assert!(
        ctx.metrics()
            .session_rejected(SessionRejectReason::UnknownToken)
            >= 1
    );

    server.shutdown();
}

// --- SEC-051: IP binding ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_bound_token_is_refused_from_another_ip() {
    let (ctx, server) = start(SessionConfig {
        bind_client_ip: true,
        ..SessionConfig::default()
    })
    .await;
    let addr = server.local_addr;

    // Authenticate as client 203.0.113.1 (through the trusted proxy).
    let token = authenticate(addr, b"alice", Some("203.0.113.1")).await;

    // Same client, same token: served.
    let ok = post(
        addr,
        &[],
        &[
            ("Fluxum-Session", &token),
            ("X-Forwarded-For", "203.0.113.1"),
        ],
    )
    .await;
    assert_eq!(ok.status, 200, "the binding IP is honored");

    // The token replayed from a different client IP: refused, counted, and
    // the session is NOT destroyed (the legitimate client keeps working).
    let stolen = post(
        addr,
        &[],
        &[
            ("Fluxum-Session", &token),
            ("X-Forwarded-For", "198.51.100.9"),
        ],
    )
    .await;
    assert_eq!(stolen.status, 403, "a token from another IP is refused");
    assert_eq!(
        ctx.metrics()
            .session_rejected(SessionRejectReason::IpMismatch),
        1
    );

    let still_ok = post(
        addr,
        &[],
        &[
            ("Fluxum-Session", &token),
            ("X-Forwarded-For", "203.0.113.1"),
        ],
    )
    .await;
    assert_eq!(still_ok.status, 200, "the real client is unaffected");

    server.shutdown();
}

// --- SEC-052: rotation with a grace window ----------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_token_rotates_on_interval_and_the_old_one_survives_the_grace_window() {
    let (_ctx, server) = start(SessionConfig {
        rotate_interval_secs: 1,
        rotate_grace_secs: 2,
        ..SessionConfig::default()
    })
    .await;
    let addr = server.local_addr;

    let first = authenticate(addr, b"alice", None).await;

    // Before the interval: no rotation.
    let same = post(addr, &[], &[("Fluxum-Session", &first)]).await;
    assert_eq!(same.status, 200);
    assert!(same.session.is_none(), "no rotation before the interval");

    // Past the interval: the next request rotates and returns a new token.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let rotated = post(addr, &[], &[("Fluxum-Session", &first)]).await;
    assert_eq!(rotated.status, 200);
    let second = rotated.session.expect("a rotated token is issued");
    assert_ne!(second, first);

    // The old token still works inside the grace window (in-flight requests).
    let graced = post(addr, &[], &[("Fluxum-Session", &first)]).await;
    assert_eq!(graced.status, 200, "old token honored during grace");
    // The new token works too.
    let with_new = post(addr, &[], &[("Fluxum-Session", &second)]).await;
    assert_eq!(with_new.status, 200);

    // After the grace window the old token is dead.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let expired = post(addr, &[], &[("Fluxum-Session", &first)]).await;
    assert_eq!(expired.status, 404, "old token dies after grace");

    server.shutdown();
}

// --- SEC-052: absolute lifetime ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_session_past_its_absolute_lifetime_is_expired() {
    let (ctx, server) = start(SessionConfig {
        absolute_lifetime_secs: 1,
        ..SessionConfig::default()
    })
    .await;
    let addr = server.local_addr;

    let token = authenticate(addr, b"alice", None).await;
    assert_eq!(
        post(addr, &[], &[("Fluxum-Session", &token)]).await.status,
        200
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let dead = post(addr, &[], &[("Fluxum-Session", &token)]).await;
    assert_eq!(dead.status, 404, "expired past the absolute lifetime");
    assert!(ctx.metrics().session_rejected(SessionRejectReason::Expired) >= 1);

    server.shutdown();
}

// --- SEC-053: revocation ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_can_list_and_terminate_sessions() {
    let (ctx, server) = start(SessionConfig::default()).await;
    let addr = server.local_addr;

    let token = authenticate(addr, b"alice", None).await;

    // GET /sessions lists the live session — never the token itself.
    let list = admin(addr, "GET", "/sessions").await;
    assert_eq!(list.status, 200);
    assert!(list.body.contains("\"sessions\""));
    assert!(
        !list.body.contains(&token),
        "the listing never exposes token material"
    );

    // Pull the session id out of the listing to terminate it.
    let id = list
        .body
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("a session id")
        .to_owned();

    let del = admin(addr, "DELETE", &format!("/sessions/{id}")).await;
    assert_eq!(del.status, 200);

    // The terminated session's next request is refused.
    let after = post(addr, &[], &[("Fluxum-Session", &token)]).await;
    assert_eq!(after.status, 404, "a revoked session is dead");
    assert!(ctx.metrics().session_rejected(SessionRejectReason::Revoked) >= 1);

    // Terminating an unknown id is a 404.
    assert_eq!(
        admin(addr, "DELETE", "/sessions/deadbeef").await.status,
        404
    );

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn terminating_by_identity_kills_every_session_for_it() {
    let (_ctx, server) = start(SessionConfig::default()).await;
    let addr = server.local_addr;

    // Two sessions for the same identity ("alice").
    let t1 = authenticate(addr, b"alice", None).await;
    let t2 = authenticate(addr, b"alice", None).await;

    // Discover alice's identity hex from the listing.
    let list = admin(addr, "GET", "/sessions").await;
    let identity = list
        .body
        .split("\"identity\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("an identity")
        .to_owned();

    let del = admin(addr, "DELETE", &format!("/sessions?identity={identity}")).await;
    assert_eq!(del.status, 200);
    assert!(
        del.body.contains("\"terminated_count\":2"),
        "both killed: {}",
        del.body
    );

    assert_eq!(
        post(addr, &[], &[("Fluxum-Session", &t1)]).await.status,
        404
    );
    assert_eq!(
        post(addr, &[], &[("Fluxum-Session", &t2)]).await.status,
        404
    );

    server.shutdown();
}
