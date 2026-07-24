//! SPEC-024 DEV-030/031 — the built-in admin web console over real loopback
//! HTTP: the self-contained shell, the boot-state document, the live diff
//! watch stream (with its table filter), and the DEV-031 gate — anonymous
//! console data access is a `development`-profile affordance only, while the
//! SEC-054 network guard still refuses untrusted remotes outright.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::{AdminConfig, ServerPeer};
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
const OPERATOR_TOKEN: &str = "console-operator-token";

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
static POST_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
static POST: TableSchema = TableSchema {
    name: "Post",
    columns: POST_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

macro_rules! row_table {
    ($row:ident, $schema:ident) => {
        #[derive(Debug, Clone, PartialEq)]
        struct $row {
            id: u64,
        }
        impl Table for $row {
            type Pk = u64;
            const SCHEMA: &'static TableSchema = &$schema;
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
    };
}
row_table!(ChatRow, CHAT);
row_table!(PostRow, POST);

fn add_chat(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(ChatRow { id: 1 })?;
    Ok(())
}
fn add_post(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(PostRow { id: 7 })?;
    Ok(())
}
/// Inserts a fresh PK each call (process-wide counter: tests share statics),
/// for tests that commit repeatedly.
fn add_chat_seq(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(100);
    ctx.tx.insert(ChatRow {
        id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    })?;
    Ok(())
}
fn no_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("reducer", args, 0)
}
static ADD_CHAT: ReducerDef = ReducerDef {
    name: "add_chat",
    handler: add_chat,
    check_args: no_args,
    client_callable: true,
    max_rate_per_sec: 0,
};
static ADD_POST: ReducerDef = ReducerDef {
    name: "add_post",
    handler: add_post,
    check_args: no_args,
    client_callable: true,
    max_rate_per_sec: 0,
};
static ADD_CHAT_SEQ: ReducerDef = ReducerDef {
    name: "add_chat_seq",
    handler: add_chat_seq,
    check_args: no_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

/// Build a server with an operator peer and the DEV-031 gate posture under
/// test; proxy-awareness is on so an `X-Forwarded-For` from loopback names a
/// "remote" client (SEC-035), as in the admin_authz tests.
async fn start(console_open: bool) -> (Arc<ShardContext>, http::HttpServer) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT, &POST]).unwrap();
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
        Arc::new(ReducerRegistry::from_defs([&ADD_CHAT, &ADD_POST, &ADD_CHAT_SEQ]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("console-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let peers = ServerPeerRegistry::from_config(&[ServerPeer {
        name: "operator".into(),
        token: OPERATOR_TOKEN.into(),
    }])
    .unwrap();
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), peers);
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    ctx.set_trusted_proxies(IpSet::parse(&["127.0.0.1".to_owned()]).unwrap());
    let policy = AdminPolicy {
        console_open,
        ..AdminPolicy::from_config(&AdminConfig::default()).unwrap()
    };
    ctx.set_admin_policy(policy);
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();
    (ctx, server)
}

struct Resp {
    status: u16,
    head: String,
    body: String,
}

/// A one-shot request (`Connection: close`); `xff` fakes the resolved client
/// IP through the trusted proxy, `operator` sets `Fluxum-Operator`.
async fn req(
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
        head: h.to_owned(),
        body: b.to_owned(),
    }
}

/// Open a streaming GET and return the socket once the response head has
/// been consumed; `accumulated` holds whatever body bytes rode along.
async fn open_stream(
    addr: std::net::SocketAddr,
    path: &str,
    operator: Option<&str>,
) -> (TcpStream, String) {
    let mut head = format!("GET {path} HTTP/1.1\r\nHost: x\r\n");
    if let Some(token) = operator {
        head.push_str(&format!("Fluxum-Operator: {token}\r\n"));
    }
    head.push_str("\r\n");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    let text = read_until(&mut stream, String::new(), "\r\n\r\n").await;
    let (_, rest) = text.split_once("\r\n\r\n").unwrap();
    (stream, rest.to_owned())
}

/// Read from the socket until `needle` appears in the accumulated text (or
/// panic after 5 s — streams keep the connection open, so EOF never comes).
async fn read_until(stream: &mut TcpStream, mut acc: String, needle: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut buf = [0u8; 4096];
    while !acc.contains(needle) {
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for `{needle}` in:\n{acc}"))
            .unwrap();
        assert!(
            n > 0,
            "stream closed while waiting for `{needle}` in:\n{acc}"
        );
        acc.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    acc
}

// --- DEV-030: the self-contained shell ---------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_console_shell_is_served_with_a_self_contained_csp() {
    let (_ctx, server) = start(true).await;
    let addr = server.local_addr;
    let r = req(addr, "GET", "/console", &[], None, None).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.head.contains("text/html"),
        "html content type: {}",
        r.head
    );
    assert!(
        r.head.contains("Content-Security-Policy"),
        "CSP header pins self-containment: {}",
        r.head
    );
    assert!(r.body.contains("Fluxum"), "the shell is the console page");
    // The trailing-slash spelling serves the same shell; an unknown console
    // subpath is a plain 404.
    assert_eq!(
        req(addr, "GET", "/console/", &[], None, None).await.status,
        200
    );
    assert_eq!(
        req(addr, "GET", "/console/nope", &[], None, None)
            .await
            .status,
        404
    );
    server.shutdown();
}

// --- DEV-031: the boot-state document + the gate -----------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn console_state_reports_the_gate_posture_and_the_auth_verdict() {
    // Production posture: gated, anonymous is not authed.
    let (_ctx, server) = start(false).await;
    let addr = server.local_addr;
    let anon = req(addr, "GET", "/console/state", &[], None, None).await;
    assert_eq!(anon.status, 200);
    let state: serde_json::Value = serde_json::from_str(&anon.body).unwrap();
    assert_eq!(state["payload"]["console_open"], false);
    assert_eq!(state["payload"]["authed"], false);
    // A server-peer credential flips the verdict.
    let authed = req(
        addr,
        "GET",
        "/console/state",
        &[],
        None,
        Some(OPERATOR_TOKEN),
    )
    .await;
    let state: serde_json::Value = serde_json::from_str(&authed.body).unwrap();
    assert_eq!(state["payload"]["authed"], true);
    server.shutdown();

    // Development posture: open.
    let (_ctx, server) = start(true).await;
    let r = req(server.local_addr, "GET", "/console/state", &[], None, None).await;
    let state: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(state["payload"]["console_open"], true);
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_refuses_anonymous_callers_outside_the_development_profile() {
    let (ctx, server) = start(false).await;
    let addr = server.local_addr;
    // Anonymous loopback: 401 — a browser surface can be driven by a lured
    // cross-site request, so production requires the operator credential
    // even locally.
    let anon = req(addr, "GET", "/console/watch", &[], None, None).await;
    assert_eq!(anon.status, 401, "{}", anon.body);
    assert!(anon.body.contains("DEV-031"), "{}", anon.body);
    assert_eq!(
        ctx.metrics()
            .admin_rejected(fluxum_core::metrics::AdminRejectReason::Unauthenticated),
        1
    );
    // A wrong token is still 401.
    let bad = req(addr, "GET", "/console/watch", &[], None, Some("wrong")).await;
    assert_eq!(bad.status, 401);
    // The operator credential opens the stream (hello event arrives).
    let (mut stream, first) = open_stream(addr, "/console/watch", Some(OPERATOR_TOKEN)).await;
    let text = read_until(&mut stream, first, "\"watching\"").await;
    assert!(text.contains("{\"watching\":null}"), "{text}");
    // An untrusted remote stays a hard 403, token or not (SEC-054).
    let remote = req(
        addr,
        "GET",
        "/console/watch",
        &[],
        Some("203.0.113.5"),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(remote.status, 403);
    // The shell too: the network guard covers every console route.
    let remote_shell = req(addr, "GET", "/console", &[], Some("203.0.113.5"), None).await;
    assert_eq!(remote_shell.status, 403);
    server.shutdown();
}

// --- DEV-030: the live diff stream, with its table filter --------------------------

#[tokio::test(flavor = "multi_thread")]
async fn watch_streams_committed_diffs_and_filters_by_table() {
    let (_ctx, server) = start(true).await;
    let addr = server.local_addr;

    // Stream A: all tables. Stream B: only `Post`.
    let (mut all, acc_all) = open_stream(addr, "/console/watch", None).await;
    let acc_all = read_until(&mut all, acc_all, "{\"watching\":null}").await;
    let (mut post_only, acc_post) = open_stream(addr, "/console/watch?table=Post", None).await;
    let acc_post = read_until(&mut post_only, acc_post, "{\"watching\":\"Post\"}").await;

    // Commit to Chat, then to Post (hello already read → subscribed).
    assert_eq!(
        req(addr, "POST", "/reducer/add_chat", b"[]", None, None)
            .await
            .status,
        200
    );
    assert_eq!(
        req(addr, "POST", "/reducer/add_post", b"[]", None, None)
            .await
            .status,
        200
    );

    // The unfiltered stream sees both commits, with full provenance and the
    // same row-JSON currency as `POST /query`.
    let text = read_until(&mut all, acc_all, "\"Post\"").await;
    assert!(text.contains("\"reducer\":\"add_chat\""), "{text}");
    assert!(text.contains("\"table\":\"Chat\""), "{text}");
    assert!(text.contains("\"inserts\":[{\"id\":1}]"), "{text}");
    assert!(text.contains("\"reducer\":\"add_post\""), "{text}");
    assert!(text.contains("\"tx_id\""), "{text}");
    assert!(text.contains("\"caller\""), "{text}");

    // The filtered stream's FIRST data line is the Post commit — the Chat
    // commit (which happened first) was filtered out, not delayed.
    let text = read_until(&mut post_only, acc_post, "\"inserts\"").await;
    let first_data = text
        .lines()
        .find(|l| l.contains("\"tables\""))
        .expect("a data line");
    assert!(first_data.contains("\"table\":\"Post\""), "{first_data}");
    assert!(
        first_data.contains("\"inserts\":[{\"id\":7}]"),
        "{first_data}"
    );
    assert!(!text.contains("\"table\":\"Chat\""), "{text}");
    server.shutdown();
}

// --- DEV-030: the query panel is read-only server-side -----------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_query_surface_rejects_mutating_statements() {
    let (_ctx, server) = start(true).await;
    let addr = server.local_addr;
    for sql in [
        "DELETE FROM Chat",
        "UPDATE Chat SET id = 2",
        "INSERT INTO Chat VALUES (9)",
    ] {
        let body = format!("{{\"sql\":\"{sql}\"}}");
        let r = req(addr, "POST", "/query", body.as_bytes(), None, None).await;
        assert!(
            (400..500).contains(&r.status),
            "`{sql}` must be refused: {} {}",
            r.status,
            r.body
        );
        assert!(r.body.contains("read-only"), "`{sql}`: {}", r.body);
    }
    server.shutdown();
}

// --- DEV-031: no storage locks — /health stays live under an open stream -----------

#[tokio::test(flavor = "multi_thread")]
async fn health_answers_while_a_watch_stream_is_open_and_commits_flow() {
    let (_ctx, server) = start(true).await;
    let addr = server.local_addr;
    let (mut stream, acc) = open_stream(addr, "/console/watch", None).await;
    let _ = read_until(&mut stream, acc, "\"watching\"").await;
    for _ in 0..5 {
        assert_eq!(
            req(addr, "POST", "/reducer/add_chat_seq", b"[]", None, None)
                .await
                .status,
            200
        );
        // Wait for THIS commit's event (each event carries one `tx_id`; the
        // previous event's tail bytes contain none, so a fresh accumulator
        // blocks until the new event arrives).
        let _ = read_until(&mut stream, String::new(), "\"tx_id\"").await;
        // The health path is atomics + a channel gauge (RPC-053): it must
        // answer while the watch stream is live and mid-fan-out. Timed to
        // the response body's arrival (the `req` helper's read_to_end waits
        // out its own timeout on a kept-alive socket, so it can't be timed).
        let started = std::time::Instant::now();
        let mut health = TcpStream::connect(addr).await.unwrap();
        health
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let text = read_until(&mut health, String::new(), "\"status\"").await;
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "health took {:?} with a live watch stream",
            started.elapsed()
        );
    }
    server.shutdown();
}
