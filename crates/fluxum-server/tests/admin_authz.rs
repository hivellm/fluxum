//! SPEC-026 SEC-054 — admin-surface access control over real loopback HTTP.
//! The admin API shares `http_port` with `/rpc`, so it must be safe by
//! default: reachable from loopback with no ceremony, refused from any other
//! address unless the operator opts an IP range in AND presents an operator
//! credential. Remote IPs are simulated through the SEC-035 trusted-proxy
//! resolution (loopback is the trusted proxy; `X-Forwarded-For` names the
//! "remote" client), and F-004 gating keeps schedule-only reducers off the
//! admin route.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::{AdminConfig, ServerPeer};
use fluxum_core::metrics::AdminRejectReason;
use fluxum_core::net::IpSet;
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::http::{self, HttpOptions};
use fluxum_server::{AdminPolicy, ShardContext};

const SHARD: u32 = 1;
const OPERATOR_TOKEN: &str = "operator-secret-token";

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

#[derive(Debug, Clone, PartialEq)]
struct ChatRow {
    id: u64,
}
impl Table for ChatRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &CHAT;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id)] => Ok(Self { id: *id }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn add_chat(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(ChatRow { id: 1 })?;
    Ok(())
}
fn no_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("add_chat", args, 0)
}
static ADD_CHAT: ReducerDef = ReducerDef {
    name: "add_chat",
    handler: add_chat,
    check_args: no_args,
    client_callable: true,
    max_rate_per_sec: 0,
};
// A schedule-only reducer: a client (and, per F-004, the admin route) may not
// invoke it.
static TICK_ONLY: ReducerDef = ReducerDef {
    name: "tick_only",
    handler: add_chat,
    check_args: no_args,
    client_callable: false,
    max_rate_per_sec: 0,
};

/// Build a server with an operator peer, proxy-awareness on (so XFF sets the
/// resolved client IP), and the given admin policy.
async fn start(admin: AdminConfig) -> (Arc<ShardContext>, http::HttpServer) {
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
        Arc::new(ReducerRegistry::from_defs([&ADD_CHAT, &TICK_ONLY]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("admin-authz-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    // An operator peer, so a remote request can present a valid credential.
    let peers = ServerPeerRegistry::from_config(&[ServerPeer {
        name: "operator".into(),
        token: OPERATOR_TOKEN.into(),
    }])
    .unwrap();
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), peers);
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_trusted_proxies(IpSet::parse(&["127.0.0.1".to_owned()]).unwrap());
    ctx.set_admin_policy(AdminPolicy::from_config(&admin).unwrap());
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    (ctx, server)
}

struct Resp {
    status: u16,
    body: String,
}

/// An admin request. `xff` sets the resolved client IP (a trusted proxy names
/// the "remote" client); `operator` sets the `Fluxum-Operator` header.
async fn admin(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    xff: Option<&str>,
    operator: Option<&str>,
) -> Resp {
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n",
        body.len()
    );
    if let Some(ip) = xff {
        head.push_str(&format!("X-Forwarded-For: {ip}\r\n"));
    }
    if let Some(token) = operator {
        head.push_str(&format!("Fluxum-Operator: {token}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut raw)).await;
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

// --- SEC-054: loopback is unceremonious; remote is refused by default -------------

#[tokio::test(flavor = "multi_thread")]
async fn loopback_reaches_the_admin_surface_without_a_credential() {
    let (_ctx, server) = start(AdminConfig::default()).await;
    let addr = server.local_addr;
    // No XFF → the resolved client IP is loopback.
    let r = admin(addr, "POST", "/reducer/add_chat", b"[]", None, None).await;
    assert_eq!(r.status, 200, "loopback admin call works: {}", r.body);
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remote_admin_call_is_refused_by_default() {
    let (ctx, server) = start(AdminConfig::default()).await;
    let addr = server.local_addr;
    // A remote client not in admin.trusted: refused before the handler.
    let r = admin(
        addr,
        "POST",
        "/reducer/add_chat",
        b"[]",
        Some("203.0.113.5"),
        None,
    )
    .await;
    assert_eq!(r.status, 403, "remote admin is refused: {}", r.body);
    assert_eq!(
        ctx.metrics().admin_rejected(AdminRejectReason::UntrustedIp),
        1
    );

    // Even a read (`/query`) is refused — no unauthenticated RLS-bypassing read.
    let q = admin(
        addr,
        "POST",
        "/query",
        br#"{"sql":"SELECT * FROM Chat"}"#,
        Some("203.0.113.5"),
        None,
    )
    .await;
    assert_eq!(q.status, 403);
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_remote_needs_an_operator_credential() {
    let (ctx, server) = start(AdminConfig {
        trusted: vec!["203.0.113.0/24".into()],
        ..AdminConfig::default()
    })
    .await;
    let addr = server.local_addr;

    // In the trusted range but no credential: 401.
    let no_cred = admin(
        addr,
        "POST",
        "/reducer/add_chat",
        b"[]",
        Some("203.0.113.5"),
        None,
    )
    .await;
    assert_eq!(
        no_cred.status, 401,
        "trusted remote still needs a credential"
    );
    assert_eq!(
        ctx.metrics()
            .admin_rejected(AdminRejectReason::Unauthenticated),
        1
    );

    // A bad credential is also 401.
    let bad = admin(
        addr,
        "POST",
        "/reducer/add_chat",
        b"[]",
        Some("203.0.113.5"),
        Some("wrong-token"),
    )
    .await;
    assert_eq!(bad.status, 401);

    // The valid operator token gets in.
    let ok = admin(
        addr,
        "POST",
        "/reducer/add_chat",
        b"[]",
        Some("203.0.113.5"),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(
        ok.status, 200,
        "valid operator credential succeeds: {}",
        ok.body
    );

    // Outside the trusted range is still a hard 403, credential or not.
    let elsewhere = admin(
        addr,
        "POST",
        "/reducer/add_chat",
        b"[]",
        Some("198.51.100.9"),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(elsewhere.status, 403);
    server.shutdown();
}

// --- SEC-054: health/metrics stay open (or gate, by config) -----------------------

#[tokio::test(flavor = "multi_thread")]
async fn health_and_metrics_stay_open_for_scrapers_but_can_be_gated() {
    let (_ctx, server) = start(AdminConfig::default()).await;
    let addr = server.local_addr;
    // Remote, no credential: health/metrics answer anyway (default open).
    assert_eq!(
        admin(addr, "GET", "/health", &[], Some("203.0.113.5"), None)
            .await
            .status,
        200
    );
    assert_eq!(
        admin(addr, "GET", "/metrics", &[], Some("203.0.113.5"), None)
            .await
            .status,
        200
    );
    server.shutdown();

    // With open_health_metrics off, they fall behind the gate.
    let (_ctx, server) = start(AdminConfig {
        open_health_metrics: false,
        ..AdminConfig::default()
    })
    .await;
    let addr = server.local_addr;
    assert_eq!(
        admin(addr, "GET", "/health", &[], Some("203.0.113.5"), None)
            .await
            .status,
        403
    );
    // Loopback still reaches them.
    assert_eq!(
        admin(addr, "GET", "/health", &[], None, None).await.status,
        200
    );
    server.shutdown();
}

// --- F-004: schedule-only reducers are not invocable over the admin route ---------

#[tokio::test(flavor = "multi_thread")]
async fn a_schedule_only_reducer_is_refused_on_the_admin_route() {
    let (_ctx, server) = start(AdminConfig::default()).await;
    let addr = server.local_addr;
    // Even from loopback (past the network gate), a non-client-callable
    // reducer is refused — admin cannot call what a client cannot.
    let r = admin(addr, "POST", "/reducer/tick_only", b"[]", None, None).await;
    assert_eq!(r.status, 403, "schedule-only reducer refused: {}", r.body);
    // The client-callable one still works.
    assert_eq!(
        admin(addr, "POST", "/reducer/add_chat", b"[]", None, None)
            .await
            .status,
        200
    );
    server.shutdown();
}

// --- Operator credential via the JSON body token (compat with /audit) -------------

#[tokio::test(flavor = "multi_thread")]
async fn the_operator_credential_may_ride_the_json_body_token() {
    let (_ctx, server) = start(AdminConfig {
        trusted: vec!["203.0.113.0/24".into()],
        ..AdminConfig::default()
    })
    .await;
    let addr = server.local_addr;
    let body = format!("{{\"sql\":\"SELECT * FROM Chat\",\"token\":\"{OPERATOR_TOKEN}\"}}");
    let r = admin(
        addr,
        "POST",
        "/query",
        body.as_bytes(),
        Some("203.0.113.5"),
        None,
    )
    .await;
    assert_eq!(r.status, 200, "body token authenticates: {}", r.body);
    server.shutdown();
}
