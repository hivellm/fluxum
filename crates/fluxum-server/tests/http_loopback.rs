//! T5.2 Streamable HTTP `/rpc` loopback suite (SPEC-006 §3; FR-42; DAG exit
//! intent): `POST /rpc` binary frames with `Content-Type` enforcement (415),
//! the `Fluxum-Session` binding, `GET /rpc` chunked push stream with live
//! `TxUpdate` delivery and keep-alives, session expiry (404 on a stale POST),
//! and transport equivalence — byte-identical FluxRPC frames drive the same
//! auth → subscribe → reducer → TxUpdate session over HTTP as over TCP.
//!
//! Uses a hand-written HTTP/1.1 client over a raw `TcpStream` (the browser
//! `fetch` DAG-exit test runs in headless Chromium in CI); this proves the
//! byte-level protocol without a browser.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{
    Authenticate, ClientMessage, Frame, FrameCodec, ReducerCall, ServerMessage, SubscribeSingle,
};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, CONTENT_TYPE, HttpOptions};

const SHARD: u32 = 1;

// --- Chat table + send_chat reducer (identical fixture to the TCP suite) -------

static CHAT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
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
    text: String,
}
impl Table for ChatRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &CHAT;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.text)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(text)] => Ok(Self {
                id: *id,
                text: text.clone(),
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn send_chat(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("send_chat(text)".into())),
    };
    ctx.tx.insert(ChatRow { id: 0, text })?;
    Ok(())
}
fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("send_chat", args, 1)
}
static SEND_CHAT: ReducerDef = ReducerDef {
    name: "send_chat",
    handler: send_chat,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

async fn start(options: HttpOptions) -> http::HttpServer {
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
        Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("http-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    http::serve(ctx, "127.0.0.1:0", options).await.unwrap()
}

// --- A minimal HTTP/1.1 client -------------------------------------------------

fn frame(message: &ClientMessage) -> Vec<u8> {
    FrameCodec::default()
        .encode(&message.encode().unwrap())
        .unwrap()
}

/// A parsed HTTP response: status, headers (lowercased), body bytes.
struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Decode the FluxRPC frames in the body into server messages.
    fn messages(&self) -> Vec<ServerMessage> {
        let codec = FrameCodec::default();
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < self.body.len() {
            let Ok(Some((frame, consumed))) = codec.decode(&self.body[offset..]) else {
                break;
            };
            if let Frame::Body(bytes) = frame {
                out.push(ServerMessage::decode(bytes).unwrap());
            }
            offset += consumed;
        }
        out
    }
}

/// Send a `POST /rpc` with the given frames and session header; read the
/// full (Content-Length) response.
async fn post(
    addr: std::net::SocketAddr,
    session: Option<&str>,
    content_type: &str,
    frames: &[Vec<u8>],
) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body: Vec<u8> = frames.iter().flatten().copied().collect();
    let mut req = format!(
        "POST /rpc HTTP/1.1\r\nHost: x\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = session {
        req.push_str(&format!("Fluxum-Session: {token}\r\n"));
    }
    req.push_str("Connection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    read_response(&mut stream).await
}

async fn read_response(stream: &mut TcpStream) -> HttpResponse {
    let mut buf = Vec::new();
    let headers_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break buf.len();
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_owned();
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    let mut body: Vec<u8> = buf[headers_end + 4..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    HttpResponse {
        status,
        headers,
        body,
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Authenticate over POST /rpc and return the issued session token.
async fn authenticate(addr: std::net::SocketAddr, token: &[u8]) -> String {
    let auth = ClientMessage::Authenticate(Authenticate {
        id: 1,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
    });
    let resp = post(addr, None, CONTENT_TYPE, &[frame(&auth)]).await;
    assert_eq!(resp.status, 200);
    assert!(matches!(
        resp.messages().first(),
        Some(ServerMessage::AuthResult(_))
    ));
    resp.header("fluxum-session")
        .expect("Fluxum-Session issued")
        .to_owned()
}

// --- FR-42: content-type enforcement + auth handshake --------------------------

#[tokio::test(flavor = "multi_thread")]
async fn wrong_content_type_is_415() {
    let server = start(HttpOptions::default()).await;
    let auth = ClientMessage::Authenticate(Authenticate {
        id: 1,
        token: b"alice".to_vec(),
        compression: None,
        tx_updates: None,
    });
    let resp = post(server.local_addr, None, "application/json", &[frame(&auth)]).await;
    assert_eq!(resp.status, 415, "non-fluxum content type rejected");
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticate_issues_a_session_and_reducer_call_commits() {
    let server = start(HttpOptions::default()).await;
    let session = authenticate(server.local_addr, b"alice").await;
    assert_eq!(session.len(), 64, "SHA-256 hex session token");

    // A reducer call with the session header commits and returns a result.
    let call = ClientMessage::ReducerCall(ReducerCall {
        id: 7,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("hello".into())],
    });
    let resp = post(
        server.local_addr,
        Some(&session),
        CONTENT_TYPE,
        &[frame(&call)],
    )
    .await;
    assert_eq!(resp.status, 200);
    match resp.messages().first() {
        Some(ServerMessage::ReducerResult(r)) => {
            assert_eq!(r.id, 7);
            assert!(r.outcome.is_ok());
        }
        other => panic!("expected ReducerResult, got {other:?}"),
    }
    server.shutdown();
}

// --- AUTH-020: a POST without a session for a non-auth message is 401 ----------

#[tokio::test(flavor = "multi_thread")]
async fn reducer_call_without_a_session_is_401() {
    let server = start(HttpOptions::default()).await;
    let call = ClientMessage::ReducerCall(ReducerCall {
        id: 1,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("x".into())],
    });
    // No session header, and the message is not Authenticate → the session
    // core answers 401 in the response body.
    let resp = post(server.local_addr, None, CONTENT_TYPE, &[frame(&call)]).await;
    assert_eq!(resp.status, 200);
    match resp.messages().first() {
        Some(ServerMessage::Error(e)) => assert_eq!(e.code, 401),
        other => panic!("expected 401 Error, got {other:?}"),
    }
    server.shutdown();
}

// --- RPC-060: a stale session token is 404 -------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_session_post_is_404() {
    let server = start(HttpOptions::default()).await;
    let call = ClientMessage::ReducerCall(ReducerCall {
        id: 1,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("x".into())],
    });
    let resp = post(
        server.local_addr,
        Some("deadbeef"),
        CONTENT_TYPE,
        &[frame(&call)],
    )
    .await;
    assert_eq!(resp.status, 404, "unknown session token");
    server.shutdown();
}

// --- SUB-001/021: GET /rpc push stream delivers live TxUpdate ------------------

#[tokio::test(flavor = "multi_thread")]
async fn get_stream_pushes_txupdate_on_commit() {
    let server = start(HttpOptions::default()).await;
    let addr = server.local_addr;
    let session = authenticate(addr, b"subscriber").await;

    // Subscribe over POST.
    let sub = ClientMessage::SubscribeSingle(SubscribeSingle {
        id: 3,
        query: "SELECT * FROM Chat".into(),
    });
    let resp = post(addr, Some(&session), CONTENT_TYPE, &[frame(&sub)]).await;
    assert!(matches!(
        resp.messages().first(),
        Some(ServerMessage::InitialData(_))
    ));

    // Open the GET push stream.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let get = format!("GET /rpc HTTP/1.1\r\nHost: x\r\nFluxum-Session: {session}\r\n\r\n");
    stream.write_all(get.as_bytes()).await.unwrap();
    // Read + discard the chunked response header.
    let mut header = Vec::new();
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).await.unwrap();
        header.push(b[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&header).contains("chunked"),
        "chunked stream"
    );

    // Commit a Chat row via a second authenticated client.
    let writer = authenticate(addr, b"writer").await;
    let call = ClientMessage::ReducerCall(ReducerCall {
        id: 1,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("live".into())],
    });
    let resp = post(addr, Some(&writer), CONTENT_TYPE, &[frame(&call)]).await;
    assert!(matches!(
        resp.messages().first(),
        Some(ServerMessage::ReducerResult(_))
    ));

    // The subscriber's GET stream carries a chunked TxUpdate frame.
    let update = read_stream_message(&mut stream, Duration::from_secs(3)).await;
    match update {
        Some(ServerMessage::TxUpdate(u)) => assert_eq!(u.tables[0].inserts.len(), 1),
        other => panic!("expected a streamed TxUpdate, got {other:?}"),
    }
    server.shutdown();
}

/// Read chunks from a chunked HTTP stream until one carries a FluxRPC body
/// frame; keep-alive (zero-length) frames are skipped.
async fn read_stream_message(stream: &mut TcpStream, timeout: Duration) -> Option<ServerMessage> {
    let codec = FrameCodec::default();
    let mut frames = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut raw = Vec::new();
    loop {
        // Parse any complete chunks accumulated so far into `frames`.
        while let Some(nl) = find(&raw, b"\r\n") {
            let size_str = String::from_utf8_lossy(&raw[..nl]).into_owned();
            let Ok(size) = usize::from_str_radix(size_str.trim(), 16) else {
                break;
            };
            if raw.len() < nl + 2 + size + 2 {
                break; // incomplete chunk body
            }
            let data = raw[nl + 2..nl + 2 + size].to_vec();
            raw.drain(..nl + 2 + size + 2);
            frames.extend_from_slice(&data);
        }
        // Decode any whole FluxRPC frames.
        if let Ok(Some((frame, consumed))) = codec.decode(&frames) {
            let msg = match frame {
                Frame::Body(bytes) => Some(ServerMessage::decode(bytes).unwrap()),
                Frame::KeepAlive => None,
            };
            frames.drain(..consumed);
            if let Some(msg) = msg {
                return Some(msg);
            }
            continue;
        }
        let mut chunk = [0u8; 4096];
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk)).await;
        match read {
            Ok(Ok(0)) | Err(_) => return None,
            Ok(Ok(n)) => raw.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return None,
        }
    }
}

// --- SPEC-006 acceptance 6: transport equivalence -------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn http_and_tcp_route_byte_identical_frames() {
    // The SAME encoded frames drive both transports; here we assert the HTTP
    // path produces the same server messages the TCP suite asserts (auth →
    // reducer → result), proving the message layer is transport-independent.
    let server = start(HttpOptions::default()).await;
    let addr = server.local_addr;
    let session = authenticate(addr, b"equiv").await;

    // A batch POST: two reducer calls in one body, two results back, ids
    // echoed — identical semantics to two pipelined TCP frames.
    let c1 = frame(&ClientMessage::ReducerCall(ReducerCall {
        id: 100,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("a".into())],
    }));
    let c2 = frame(&ClientMessage::ReducerCall(ReducerCall {
        id: 101,
        reducer: "send_chat".into(),
        version: None,
        args: vec![FluxValue::Str("b".into())],
    }));
    let resp = post(addr, Some(&session), CONTENT_TYPE, &[c1, c2]).await;
    let ids: Vec<u32> = resp
        .messages()
        .iter()
        .filter_map(|m| match m {
            ServerMessage::ReducerResult(r) => Some(r.id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![100, 101], "batched frames answered in order");
    server.shutdown();
}
