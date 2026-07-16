//! Streamable HTTP transport (SPEC-006 §3; FR-42): the binary FluxRPC
//! `/rpc` surface on :15800 that gives browsers — which cannot open raw TCP
//! — the identical message layer as [`crate::tcp`], with no protocol
//! translation, no SSE, no base64, no JSON anywhere on the path.
//!
//! # Shape
//!
//! - `POST /rpc` carries concatenated FluxRPC frames in the request body
//!   (`Content-Type: application/x-fluxum`, else `415`) and returns the
//!   response frames in the body. The first `Authenticate` mints a session
//!   and returns a `Fluxum-Session` header; every later POST must present it
//!   (`404` for a stale/expired session).
//! - `GET /rpc` with the `Fluxum-Session` header opens the binary push
//!   stream (an HTTP/1.1 `chunked` body a browser reads via
//!   `fetch().body.getReader()`): server-initiated `TxUpdate`s plus
//!   zero-length keep-alive frames, and a `408` frame when the session
//!   goes idle.
//!
//! A `Fluxum-Session` binds a run of POSTs and one GET stream to a single
//! logical connection — the same `ConnectionId` + outbound queue the
//! [`crate::tcp`] transport uses — so the fan-out treats HTTP and TCP
//! subscribers identically. The router core ([`crate::session::Session`]) is
//! transport-independent; this module is only HTTP framing over it.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::Instant;

use fluxum_protocol::{ClientMessage, Frame, FrameCodec, ServerMessage, codes};

use crate::session::{Session, SessionState};
use crate::{ConnHandle, OutFrame, ShardContext};

/// The one wire content type for `/rpc` (RPC — binary, never JSON).
pub const CONTENT_TYPE: &str = "application/x-fluxum";
/// The session-binding header issued on `Authenticate`.
pub const SESSION_HEADER: &str = "fluxum-session";

/// Streamable HTTP tuning (RPC-060 idle expiry + keep-alive cadence).
#[derive(Debug, Clone, Copy)]
pub struct HttpOptions {
    /// Idle-session expiry: a session with no POST and an idle GET stream
    /// for this long expires (`408` on the stream, `404` on a stale POST).
    /// `None` disables.
    pub idle_timeout: Option<Duration>,
    /// Keep-alive cadence on an open GET stream (zero-length frames).
    pub keepalive: Duration,
    /// Max inbound frame body size (RPC-061); a larger frame is `413`.
    pub max_frame_bytes: u32,
    /// Per-session outbound queue depth (the SUB-042 send buffer).
    pub send_queue_depth: usize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(60)),
            keepalive: Duration::from_secs(20),
            max_frame_bytes: fluxum_protocol::DEFAULT_MAX_FRAME_BYTES,
            send_queue_depth: 1024,
        }
    }
}

/// A running HTTP transport: the bound address and a shutdown handle.
pub struct HttpServer {
    /// The actually-bound local address.
    pub local_addr: std::net::SocketAddr,
    shutdown: Arc<Notify>,
}

impl HttpServer {
    /// Signal the accept loop and every stream to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// One `Fluxum-Session`: the authenticated router state, its connection id,
/// and the outbound receiver a GET stream drains.
struct HttpSession {
    state: SessionState,
    connection_id: u128,
    shutdown: Arc<Notify>,
    out_rx: Option<mpsc::Receiver<OutFrame>>,
    last_active: Instant,
}

/// HTTP transport state over a shard.
struct HttpState {
    ctx: Arc<ShardContext>,
    options: HttpOptions,
    sessions: Mutex<HashMap<String, HttpSession>>,
    token_counter: AtomicU64,
}

impl HttpState {
    /// Mint an unguessable session token: `SHA-256(identity ++ counter)`
    /// hex. The identity is itself `SHA-256(client token)` (AUTH-001), so a
    /// third party cannot forge it.
    fn mint_token(&self, state: &SessionState) -> String {
        let seq = self.token_counter.fetch_add(1, Ordering::Relaxed);
        let mut hasher = Sha256::new();
        if let SessionState::Authenticated { caller, .. } = state {
            hasher.update(caller.identity.as_bytes());
        }
        hasher.update(seq.to_le_bytes());
        let digest = hasher.finalize();
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Bind `addr` and serve Streamable HTTP `/rpc` over `ctx`.
pub async fn serve(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: HttpOptions,
) -> io::Result<HttpServer> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(Notify::new());
    let state = Arc::new(HttpState {
        ctx: Arc::clone(&ctx),
        options,
        sessions: Mutex::new(HashMap::new()),
        token_counter: AtomicU64::new(1),
    });

    // Shard-wide commit fan-out (SUB-021) — one per shard, shared with TCP.
    crate::spawn_fanout(Arc::clone(&ctx), shutdown.clone());
    // Ephemeral TTL sweeper (DMX-011) — idempotent across transports.
    ctx.start_ephemeral_sweeper();

    let accept_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accept_shutdown.notified() => break,
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else { continue };
                    let conn_state = Arc::clone(&state);
                    let conn_shutdown = accept_shutdown.clone();
                    tokio::spawn(async move {
                        let _ = serve_connection(conn_state, stream, conn_shutdown).await;
                    });
                }
            }
        }
    });

    Ok(HttpServer {
        local_addr,
        shutdown,
    })
}

/// Serve one HTTP/1.1 connection: parse a request, dispatch `/rpc`, and
/// (for POST) keep the connection alive for the next request.
async fn serve_connection(
    state: Arc<HttpState>,
    mut stream: TcpStream,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    loop {
        let request = match read_request(&mut stream, &mut buf).await? {
            Some(request) => request,
            None => return Ok(()), // clean close
        };
        let keep_alive = match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/rpc") => {
                handle_post(&state, &mut stream, &request).await?;
                true
            }
            ("GET", "/rpc") => {
                // The GET stream owns the connection for its lifetime.
                handle_get(&state, stream, &request, server_shutdown).await?;
                return Ok(());
            }
            // Blob upload/download (SPEC-023 DMX-041) — out-of-band of the
            // 16 MB FluxRPC frame; shares this port with the admin surface.
            ("POST", "/blob") => {
                handle_blob_upload(&state.ctx, &mut stream, &request).await?;
                true
            }
            ("GET", path) if path.strip_prefix("/blob/").is_some() => {
                let hash = path.strip_prefix("/blob/").unwrap_or_default().to_owned();
                handle_blob_download(&state.ctx, &mut stream, &hash).await?;
                true
            }
            // The HTTP/JSON admin surface (RPC-050) shares this port.
            (method, path) if crate::admin::is_admin_path(path) => {
                handle_admin(&state.ctx, &mut stream, method, path, &request.body).await?;
                true
            }
            ("GET" | "POST", _) => {
                write_simple(&mut stream, 404, "Not Found").await?;
                true
            }
            _ => {
                write_simple(&mut stream, 405, "Method Not Allowed").await?;
                true
            }
        };
        if !keep_alive {
            return Ok(());
        }
    }
}

/// Dispatch an admin route and write its JSON (or `text/plain` for
/// `/metrics`) response (RPC-050..052).
/// `POST /blob` (DMX-041): stage the raw request body in the shard's blob
/// store and answer `{"hash": "<64-hex>"}`. Staged bytes live under an
/// upload lease until the first row references the hash (or blob GC reclaims
/// the orphan). 404 when no blob store is installed; 413 above the cap.
async fn handle_blob_upload(
    ctx: &Arc<ShardContext>,
    stream: &mut TcpStream,
    request: &Request,
) -> io::Result<()> {
    /// Upload cap: generous (out-of-band of the 16 MB frame), still bounded.
    const MAX_BLOB_BYTES: usize = 256 * 1024 * 1024;
    let Some(blobs) = ctx.blob_store() else {
        return write_simple(stream, 404, "Not Found").await;
    };
    if request.body.len() > MAX_BLOB_BYTES {
        return write_simple(stream, 413, "Payload Too Large").await;
    }
    if request.body.is_empty() {
        return write_simple(stream, 400, "Bad Request").await;
    }
    match blobs.stage(&request.body) {
        Ok(hash) => {
            let body = format!("{{\"hash\":\"{hash}\"}}");
            write_json(stream, 200, "application/json", body.as_bytes()).await
        }
        Err(_) => write_simple(stream, 500, "Internal Server Error").await,
    }
}

/// `GET /blob/:hash` (DMX-041): the raw bytes as `application/octet-stream`;
/// 404 for an unknown hash or when no blob store is installed.
async fn handle_blob_download(
    ctx: &Arc<ShardContext>,
    stream: &mut TcpStream,
    hash: &str,
) -> io::Result<()> {
    let Some(blobs) = ctx.blob_store() else {
        return write_simple(stream, 404, "Not Found").await;
    };
    let Some(hash) = fluxum_core::commitlog::BlobHash::parse(hash) else {
        return write_simple(stream, 400, "Bad Request").await;
    };
    match blobs.get(&hash) {
        Ok(Some(bytes)) => write_json(stream, 200, "application/octet-stream", &bytes).await,
        Ok(None) => write_simple(stream, 404, "Not Found").await,
        Err(_) => write_simple(stream, 500, "Internal Server Error").await,
    }
}

async fn handle_admin(
    ctx: &Arc<ShardContext>,
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    body: &[u8],
) -> io::Result<()> {
    let response = crate::admin::dispatch(ctx, method, path, body).await;
    // `/metrics` returns a JSON string that is really Prometheus text.
    let (content_type, bytes) = match &response.body {
        serde_json::Value::String(text) => ("text/plain; version=0.0.4", text.clone().into_bytes()),
        value => (
            "application/json",
            serde_json::to_vec(value).unwrap_or_default(),
        ),
    };
    write_json(stream, response.status, content_type, &bytes).await
}

/// `POST /rpc`: route the body frames and return the response frames.
async fn handle_post(
    state: &Arc<HttpState>,
    stream: &mut TcpStream,
    request: &Request,
) -> io::Result<()> {
    if !request.header_eq("content-type", CONTENT_TYPE) {
        return write_simple(stream, 415, "Unsupported Media Type").await;
    }
    let codec = FrameCodec::new(state.options.max_frame_bytes);
    let messages = match decode_frames(&codec, &request.body) {
        Ok(messages) => messages,
        Err(code) => {
            // PROTO_FRAME_TOO_LARGE → 413, PROTO_MALFORMED → 400 (SPEC-028 §7).
            return write_simple(stream, http_status(code), "").await;
        }
    };

    let session_token = request.header("fluxum-session");

    // Load or create the session's router state.
    let (mut router_state, connection_id, new_session) = if let Some(token) = &session_token {
        let mut sessions = state.sessions.lock().await;
        match sessions.get_mut(token) {
            Some(sess) => {
                sess.last_active = Instant::now();
                (sess.state.clone(), sess.connection_id, false)
            }
            None => {
                drop(sessions);
                // Stale/expired session token (RPC-060).
                return write_simple(stream, 404, "Not Found").await;
            }
        }
    } else {
        (SessionState::Unauthenticated, 0, true)
    };

    // Route every frame through the transport-independent session core.
    let mut responses: Vec<ServerMessage> = Vec::new();
    let mut commits = Vec::new();
    {
        let mut session = Session::with_state(Arc::clone(&state.ctx), router_state.clone());
        for message in messages {
            let routed = session.handle(message).await;
            responses.extend(routed.responses);
            if let Some(diff) = routed.commit {
                commits.push(diff);
            }
        }
        router_state = session.into_state();
    }

    // On a fresh session that just authenticated, register it + its outbound
    // queue and issue the `Fluxum-Session` header.
    let mut issued_token: Option<String> = None;
    if new_session && matches!(router_state, SessionState::Authenticated { .. }) {
        let connection_id = connection_id_of(&router_state).unwrap_or(0);
        let (out_tx, out_rx) = mpsc::channel::<OutFrame>(state.options.send_queue_depth);
        let shutdown = Arc::new(Notify::new());
        state
            .ctx
            .connections
            .insert(
                connection_id,
                ConnHandle {
                    sink: out_tx,
                    shutdown: Arc::clone(&shutdown),
                },
            )
            .await;
        let token = state.mint_token(&router_state);
        state.sessions.lock().await.insert(
            token.clone(),
            HttpSession {
                state: router_state.clone(),
                connection_id,
                shutdown,
                out_rx: Some(out_rx),
                last_active: Instant::now(),
            },
        );
        issued_token = Some(token);
        // RED-011: run the `on_connect` hooks for the fresh session; their diff
        // is published with the rest of this request's commits below.
        if let SessionState::Authenticated { caller, .. } = &router_state {
            match state
                .ctx
                .engine
                .client_connected(caller.identity, caller.connection_id)
                .await
            {
                Ok(Some(receipt)) => commits.push(receipt.diff),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(target: "fluxum::server", error = %e, "on_connect hook failed");
                }
            }
        }
    } else if let Some(token) = &session_token {
        // Persist any state change (e.g. a re-auth) back to the store.
        if let Some(sess) = state.sessions.lock().await.get_mut(token) {
            sess.state = router_state.clone();
        }
    }
    let _ = connection_id;

    // Publish committed diffs to the fan-out (SUB-021).
    for diff in commits {
        state.ctx.publish_commit(diff);
    }

    // Encode the response frames into the body.
    let mut body = Vec::new();
    for response in &responses {
        if let Ok(frame) = frame_message(&codec, response) {
            body.extend_from_slice(&frame);
        }
    }
    write_response(stream, 200, "OK", issued_token.as_deref(), &body).await
}

/// `GET /rpc`: stream the session's outbound frames (chunked) with
/// keep-alives and idle expiry.
async fn handle_get(
    state: &Arc<HttpState>,
    mut stream: TcpStream,
    request: &Request,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let Some(token) = request.header("fluxum-session") else {
        return write_simple(&mut stream, 404, "Not Found").await;
    };
    // Take the outbound receiver + shutdown for this session.
    let (mut out_rx, shutdown, connection_id) = {
        let mut sessions = state.sessions.lock().await;
        match sessions.get_mut(&token) {
            Some(sess) => match sess.out_rx.take() {
                Some(rx) => (rx, Arc::clone(&sess.shutdown), sess.connection_id),
                None => {
                    drop(sessions);
                    // A stream is already open for this session.
                    return write_simple(&mut stream, 409, "Conflict").await;
                }
            },
            None => {
                drop(sessions);
                return write_simple(&mut stream, 404, "Not Found").await;
            }
        }
    };

    // Chunked streaming response header.
    write_stream_header(&mut stream).await?;

    let keepalive = state.options.keepalive;
    let idle = state.options.idle_timeout;
    let mut keepalive_timer = tokio::time::interval(keepalive);
    keepalive_timer.tick().await; // consume the immediate first tick

    let result = loop {
        let idle_deadline = idle.map(|d| Instant::now() + d);
        tokio::select! {
            _ = server_shutdown.notified() => break Ok(()),
            _ = shutdown.notified() => break Ok(()),
            frame = out_rx.recv() => match frame {
                Some(frame) => {
                    if write_chunk(&mut stream, &frame).await.is_err() {
                        break Ok(());
                    }
                }
                None => break Ok(()), // the connection's sink was dropped
            },
            _ = keepalive_timer.tick() => {
                // Zero-length keep-alive frame (RPC-001/006).
                if write_chunk(&mut stream, &FrameCodec::keepalive()).await.is_err() {
                    break Ok(());
                }
            }
            () = sleep_until(idle_deadline) => {
                // RPC-060: idle expiry — 408 frame then close, drop session.
                let codec = FrameCodec::default();
                let frame = error_frame(&codec, codes::PROTO_IDLE_TIMEOUT, "idle timeout");
                let _ = write_chunk(&mut stream, &frame).await;
                break Ok(());
            }
        }
    };

    // Terminate the chunked body and evict the session.
    let _ = write_last_chunk(&mut stream).await;
    let evicted = state.sessions.lock().await.remove(&token);
    state.ctx.connections.remove(connection_id).await;
    state
        .ctx
        .subscriptions
        .lock()
        .await
        .disconnect(connection_id);
    // RED-012: run the `on_disconnect` hooks and publish their diff to the
    // remaining subscribers (a presence cleanup must reach them).
    if let Some(session) = evicted
        && let SessionState::Authenticated { caller, .. } = &session.state
    {
        match state
            .ctx
            .engine
            .client_disconnected(caller.identity, caller.connection_id)
            .await
        {
            Ok(Some(receipt)) => state.ctx.publish_commit(receipt.diff),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(target: "fluxum::server", error = %e, "on_disconnect hook failed");
            }
        }
    }
    result
}

// --- HTTP/1.1 request parsing --------------------------------------------------

/// A parsed request: method, path, lowercased headers, and the body.
struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    fn header_eq(&self, name: &str, value: &str) -> bool {
        self.header(name).is_some_and(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case(value)
        })
    }
}

/// Read one HTTP/1.1 request; `None` on a clean connection close.
async fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<Option<Request>> {
    // Read until the end of the header block.
    let headers_end = loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
            break pos;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header block too large",
            ));
        }
    };

    let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_owned();
            if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }

    // Consume the header block and read the body.
    let body_start = headers_end + 4;
    buf.drain(..body_start);
    while buf.len() < content_length {
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf.drain(..content_length).collect();

    Ok(Some(Request {
        method,
        path,
        headers,
        body,
    }))
}

// --- HTTP/1.1 response writing -------------------------------------------------

async fn write_response(
    stream: &mut TcpStream,
    code: u16,
    reason: &str,
    session: Option<&str>,
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = session {
        head.push_str(&format!("Fluxum-Session: {token}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// Write a non-FluxRPC response (admin JSON / metrics text) with an explicit
/// content type.
async fn write_json(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
        reason_phrase(code),
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// A minimal reason-phrase table for the admin responses.
fn reason_phrase(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

async fn write_simple(stream: &mut TcpStream, code: u16, reason: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: 0\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

async fn write_stream_header(stream: &mut TcpStream) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {CONTENT_TYPE}\r\nTransfer-Encoding: chunked\r\n\
         Cache-Control: no-cache\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

async fn write_chunk(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

async fn write_last_chunk(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await
}

// --- helpers -------------------------------------------------------------------

fn connection_id_of(state: &SessionState) -> Option<u128> {
    match state {
        SessionState::Authenticated { caller, .. } => Some(caller.connection_id.as_u128()),
        SessionState::Unauthenticated => None,
    }
}

/// Decode every FluxRPC frame in a POST body into client messages. A frame
/// larger than the limit is `413`; a malformed envelope is `400`.
fn decode_frames(codec: &FrameCodec, body: &[u8]) -> Result<Vec<ClientMessage>, u16> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < body.len() {
        match codec.decode(&body[offset..]) {
            Ok(Some((frame, consumed))) => {
                if let Frame::Body(bytes) = frame {
                    let message =
                        ClientMessage::decode(bytes).map_err(|_| codes::PROTO_MALFORMED)?;
                    messages.push(message);
                }
                offset += consumed;
            }
            Ok(None) => break, // trailing partial frame — ignore
            Err(_too_large) => return Err(codes::PROTO_FRAME_TOO_LARGE),
        }
    }
    Ok(messages)
}

fn frame_message(codec: &FrameCodec, message: &ServerMessage) -> Result<Vec<u8>, ()> {
    let body = message.encode().map_err(|_| ())?;
    codec.encode(&body).map_err(|_| ())
}

fn error_frame(codec: &FrameCodec, code: u16, message: &str) -> Vec<u8> {
    let msg = ServerMessage::Error(fluxum_protocol::ErrorMessage::from_catalog(
        None,
        code,
        message,
        Vec::new(),
    ));
    frame_message(codec, &msg).unwrap_or_default()
}

/// The HTTP status a catalog code derives for this transport (SPEC-028 §7).
fn http_status(code: u16) -> u16 {
    codes::entry(code).map_or(500, |entry| entry.http_status)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Sleep until `deadline`, or forever when `None` (idle disabled).
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}
