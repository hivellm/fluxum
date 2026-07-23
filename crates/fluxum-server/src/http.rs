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
//!
//! # Session-token security (SPEC-026 SEC-050..053)
//!
//! The `Fluxum-Session` token is the bearer credential for every post-auth
//! request, so it is a CSPRNG value ([`crate::session_sec`]) stored only as a
//! hash (the sessions map is keyed by `hex(SHA-256(token))`): a disclosure of
//! the map yields no usable token, and a token the server never minted hashes
//! to an absent id and is never adopted (anti-fixation). The transport also
//! enforces the optional [`SessionPolicy`](crate::session_sec::SessionPolicy):
//! client-IP binding, token rotation with a grace window, and an absolute
//! lifetime. Live sessions are mirrored into a [`SessionControl`] directory so
//! the admin API can list and terminate them.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use fluxum_core::metrics::SessionRejectReason;
use fluxum_protocol::{ClientMessage, Frame, FrameCodec, ServerMessage, codes};

use crate::session::{Session, SessionState};
use crate::session_sec::token_id;
use crate::tls::MaybeTls;
use crate::{ConnHandle, OutFrame, ShardContext};

/// The one wire content type for `/rpc` (RPC — binary, never JSON).
pub const CONTENT_TYPE: &str = "application/x-fluxum";
/// The session-binding header issued on `Authenticate`.
pub const SESSION_HEADER: &str = "fluxum-session";

/// Streamable HTTP tuning (RPC-060 idle expiry + keep-alive cadence).
// No longer `Copy`: `static_dir` owns a path. Every use clones it once at
// startup, so the cost is a string per server rather than per request.
#[derive(Debug, Clone)]
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
    /// Directory served for unmatched `GET` paths, or `None` (the default) to
    /// serve nothing.
    ///
    /// Exists because `/rpc` sends no CORS headers: a browser page that talks
    /// to Fluxum must come from the same origin as `/rpc`. Off unless an
    /// operator opts in, so a production server has no file surface at all.
    pub static_dir: Option<std::path::PathBuf>,
    /// Listener/socket hardening knobs (SEC-042); defaults = today's
    /// behavior.
    pub socket: crate::sock::SocketOptions,
    /// Session-token security policy (SEC-050..052); defaults = CSPRNG token,
    /// hashed at rest, no binding/rotation/absolute-lifetime.
    pub session: crate::session_sec::SessionPolicy,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(60)),
            keepalive: Duration::from_secs(20),
            max_frame_bytes: fluxum_protocol::DEFAULT_MAX_FRAME_BYTES,
            send_queue_depth: 1024,
            // Serving files is opt-in: a database with no configured static
            // directory exposes no filesystem at all.
            static_dir: None,
            socket: crate::sock::SocketOptions::default(),
            session: crate::session_sec::SessionPolicy::default(),
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
    /// The database this session bound to on `Authenticate` (SPEC-025
    /// OPS-050); `None` = the default. Persisted here because this transport
    /// rebuilds a `Session` per request, and the binding must survive that.
    namespace: Option<Arc<crate::namespace::Namespace>>,
    /// When the session was first minted (SEC-052 absolute lifetime).
    created: Instant,
    /// When the *current* token was issued (SEC-052 rotation interval).
    issued: Instant,
    /// The client IP the session authenticated from (SEC-051), when binding
    /// is on. A request presenting the token from another IP is refused.
    client_ip: Option<std::net::IpAddr>,
    /// Set when an operator terminates the session (SEC-053): the next
    /// request on it is refused and its stream dropped.
    revoked: Arc<std::sync::atomic::AtomicBool>,
}

/// Why a GET stream ended — it decides the session's fate (SPEC-021
/// CS-021).
#[derive(Debug, Clone, Copy)]
enum GetExit {
    /// The client's socket died mid-stream: a blip. The session, its
    /// subscriptions and their retained delta windows survive so the client
    /// can reattach and `Resume`; the sweeper expires it if it never does.
    Detached,
    /// Idle expiry, server shutdown, or the fan-out dropping this consumer:
    /// the session is over — deregister and run `on_disconnect` (RED-012).
    Expired,
}

/// HTTP transport state over a shard.
struct HttpState {
    ctx: Arc<ShardContext>,
    options: HttpOptions,
    /// Live sessions, keyed by the at-rest id `hex(SHA-256(token))` (SEC-050):
    /// only the hash is stored, never the token, and a presented token the
    /// server never minted hashes to an id that is simply absent.
    sessions: Mutex<HashMap<String, HttpSession>>,
    /// Just-rotated tokens honored for a grace window (SEC-052): old id →
    /// (current id, grace deadline), so an in-flight request carrying the
    /// pre-rotation token still resolves briefly.
    grace: Mutex<HashMap<String, (String, Instant)>>,
    /// Resolved SEC-051/052 policy (binding, rotation, lifetime).
    policy: crate::session_sec::SessionPolicy,
    /// The sync directory the admin API lists and terminates through
    /// (SEC-053); mirrors the sessions map's identifying fields.
    control: Arc<SessionControl>,
}

/// The admin-facing session directory (SPEC-026 SEC-053): a synchronous
/// mirror of the sessions map holding exactly what an operator needs — never
/// token material — plus the per-session shutdown signal and revoked flag,
/// so terminating a session drops its stream and blocks its next request.
#[derive(Default)]
struct SessionControl {
    entries: std::sync::Mutex<HashMap<String, ControlEntry>>,
}

struct ControlEntry {
    identity_hex: String,
    connection_id: u128,
    created: Instant,
    client_ip: Option<std::net::IpAddr>,
    shutdown: Arc<Notify>,
    revoked: Arc<std::sync::atomic::AtomicBool>,
}

impl SessionControl {
    fn insert(&self, id: String, entry: ControlEntry) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, entry);
    }

    fn remove(&self, id: &str) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    /// Re-key an entry when its token rotates (SEC-052), preserving its
    /// shutdown/revoked handles so a revocation mid-rotation still lands.
    fn rekey(&self, old_id: &str, new_id: String) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.remove(old_id) {
            entries.insert(new_id, entry);
        }
    }
}

impl crate::SessionAdmin for SessionControl {
    fn list(&self) -> Vec<crate::SessionInfo> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: Vec<crate::SessionInfo> = entries
            .iter()
            .map(|(id, e)| crate::SessionInfo {
                id: id.clone(),
                identity: e.identity_hex.clone(),
                connection_id: e.connection_id.to_string(),
                age_secs: e.created.elapsed().as_secs(),
                client_ip: e.client_ip.map(|ip| ip.to_string()),
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    fn terminate(&self, id: &str) -> bool {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Mark revoked and fire the stream shutdown before dropping the
        // entry: an open GET stream ends and its handler evicts the session;
        // a session with no stream is refused on its next request by the
        // revoked flag the sessions map still holds.
        if let Some(entry) = entries.remove(id) {
            entry
                .revoked
                .store(true, std::sync::atomic::Ordering::SeqCst);
            entry.shutdown.notify_waiters();
            true
        } else {
            false
        }
    }

    fn terminate_identity(&self, identity_hex: &str) -> usize {
        let ids: Vec<String> = {
            let entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries
                .iter()
                .filter(|(_, e)| e.identity_hex == identity_hex)
                .map(|(id, _)| id.clone())
                .collect()
        };
        ids.iter().filter(|id| self.terminate(id)).count()
    }
}

/// Bind `addr` and serve Streamable HTTP `/rpc` over `ctx`.
pub async fn serve(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: HttpOptions,
) -> io::Result<HttpServer> {
    serve_tls(ctx, addr, options, None).await
}

/// [`serve`] with optional built-in TLS termination (SPEC-026 SEC-059): a
/// directly-accepted connection completes the TLS handshake before the first
/// request; a trusted-proxy connection stays plaintext (the proxy terminated
/// TLS and forwards on a trusted link).
pub async fn serve_tls(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: HttpOptions,
    tls: Option<TlsAcceptor>,
) -> io::Result<HttpServer> {
    let listener = crate::sock::bind(addr, options.socket).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(Notify::new());
    let policy = options.session;
    let control = Arc::new(SessionControl::default());
    // SEC-053: expose the directory to the admin API before serving.
    ctx.set_session_admin(Arc::clone(&control) as Arc<dyn crate::SessionAdmin>);
    let state = Arc::new(HttpState {
        ctx: Arc::clone(&ctx),
        options,
        sessions: Mutex::new(HashMap::new()),
        grace: Mutex::new(HashMap::new()),
        policy,
        control,
    });

    // Shard-wide commit fan-out (SUB-021) — one per shard, shared with TCP.
    crate::spawn_fanout(Arc::clone(&ctx), shutdown.clone());
    // Ephemeral TTL sweeper (DMX-011) — idempotent across transports.
    ctx.start_ephemeral_sweeper();
    ctx.start_ttl_sweeper();
    // RPC-060 expiry for sessions no GET stream is timing (SPEC-021 CS-021).
    spawn_session_sweeper(Arc::clone(&state), shutdown.clone());

    let accept_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accept_shutdown.notified() => break,
                accepted = listener.accept() => {
                    let Ok((stream, peer)) = accepted else { continue };
                    // Realtime wire: push-stream frames are small and must
                    // leave immediately — Nagle would batch them (NFR-04).
                    let _ = stream.set_nodelay(true);
                    // SEC-042: keepalive so dead peers stop holding slots.
                    crate::sock::apply_keepalive(&stream, state.options.socket.tcp_keepalive);
                    // SEC-030/031: gate the pre-auth surface per peer IP, the
                    // same guard the TCP transport uses (shared via ctx).
                    let ip = peer.ip();
                    let trusted = state.ctx.trusted_proxies();
                    if !trusted.is_empty() && trusted.contains(ip.to_canonical()) {
                        // SEC-035: a trusted proxy names the real client in
                        // `X-Forwarded-For`, which lives in the request — so
                        // admission is deferred to the first parsed request,
                        // where the guard can key on the *client*. The proxy
                        // already terminated TLS, so this hop stays plaintext.
                        let conn_state = Arc::clone(&state);
                        let conn_shutdown = accept_shutdown.clone();
                        tokio::spawn(async move {
                            let _ = serve_connection(conn_state, MaybeTls::Plain(stream), ip, Admission::Proxied(trusted), conn_shutdown).await;
                        });
                    } else {
                        match state.ctx.conn_guard().try_accept(ip) {
                            Ok(permit) => {
                                let conn_state = Arc::clone(&state);
                                let conn_shutdown = accept_shutdown.clone();
                                let tls = tls.clone();
                                tokio::spawn(async move {
                                    // SEC-059: terminate TLS (if configured)
                                    // before the first request is read.
                                    let conn = match MaybeTls::accept(stream, tls.as_ref()).await {
                                        Ok(conn) => conn,
                                        Err(e) => {
                                            tracing::debug!(target: "fluxum::http", error = %e, "TLS handshake failed");
                                            return;
                                        }
                                    };
                                    let _ = serve_connection(conn_state, conn, ip, Admission::Direct(permit), conn_shutdown).await;
                                });
                            }
                            Err(reason) => {
                                state.ctx.metrics().note_conn_rejected(reason);
                                fluxum_core::secevent::conn_rejected(ip, reason.as_str());
                                drop(stream);
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(HttpServer {
        local_addr,
        shutdown,
    })
}

/// How a connection was admitted, which decides where its client IP comes
/// from (SEC-035).
enum Admission {
    /// An ordinary peer: admitted at accept, the socket peer is the client.
    /// The permit holds the SEC-030 slot for the connection's whole life.
    Direct(crate::connguard::ConnPermit),
    /// The peer is a trusted proxy: each request's `X-Forwarded-For`
    /// resolves the client, and the guard runs on the first request once
    /// that client is known.
    Proxied(Arc<fluxum_core::net::IpSet>),
}

/// Serve one HTTP/1.1 connection: parse a request, dispatch `/rpc`, and
/// (for POST) keep the connection alive for the next request.
async fn serve_connection(
    state: Arc<HttpState>,
    mut stream: MaybeTls,
    peer_ip: std::net::IpAddr,
    admission: Admission,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    // Holds the SEC-030 concurrent-connection slot for the connection's
    // whole life (a GET stream can be long-lived); released on drop. On a
    // proxied connection it is empty until the first request names the
    // client and the guard admits it.
    let (mut permit, trusted) = match admission {
        Admission::Direct(p) => (Some(p), None),
        Admission::Proxied(t) => (None, Some(t)),
    };
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    loop {
        let request = match read_request(&mut stream, &mut buf).await? {
            Some(request) => request,
            None => return Ok(()), // clean close
        };
        // SEC-035: the IP everything downstream attributes this request to —
        // the socket peer, or the `X-Forwarded-For` client resolved under
        // the rightmost-untrusted rule when the peer is a trusted proxy.
        let ip = match &trusted {
            None => peer_ip,
            Some(trusted) => match crate::clientip::resolve_forwarded_for(
                peer_ip.to_canonical(),
                request.header("x-forwarded-for").as_deref(),
                trusted,
            ) {
                Ok(ip) => ip,
                Err(e) => {
                    // A trusted proxy sending garbage attribution is a
                    // misconfiguration: reject loudly, count it (SEC-035).
                    state
                        .ctx
                        .metrics()
                        .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::ProxyHeader);
                    tracing::warn!(target: "fluxum::http", proxy = %peer_ip, error = %e,
                        "rejected a request: malformed X-Forwarded-For from a trusted proxy");
                    write_simple(&mut stream, 400, "Bad Request").await?;
                    return Ok(());
                }
            },
        };
        // SEC-030/031 on a proxied connection: admit on the resolved client
        // the first time it is known. Keyed on the client, not the proxy —
        // otherwise every per-IP cap would throttle the proxy itself.
        if permit.is_none() && trusted.is_some() {
            match state.ctx.conn_guard().try_accept(ip) {
                Ok(p) => permit = Some(p),
                Err(reason) => {
                    state.ctx.metrics().note_conn_rejected(reason);
                    fluxum_core::secevent::conn_rejected(ip, reason.as_str());
                    return Ok(()); // drop, the cheapest refusal
                }
            }
        }
        // SEC-041 admission control, `/rpc` only — like the drain refusal
        // below, it must NOT gate `/health`, `/metrics` or `/bans`: those
        // are exactly the tools an operator fights an overload with. Under
        // shed-preauth a request carrying a live session keeps working
        // (established clients are the priority); anything pre-auth is
        // dropped with zero response bytes. Under shed-all-new even session
        // reattaches are dropped — but the sockets those sessions already
        // hold keep streaming untouched.
        if request.path == "/rpc" && matches!(request.method.as_str(), "POST" | "GET") {
            use fluxum_core::metrics::OverloadState;
            let shed = match state.ctx.overload_state() {
                OverloadState::Normal => false,
                OverloadState::ShedAllNew => true,
                OverloadState::ShedPreauth => match request.header(SESSION_HEADER) {
                    // A live session (by its resolved id) is established work.
                    Some(token) => resolve_id(&state, &token).await.is_none(),
                    None => true,
                },
            };
            if shed {
                state
                    .ctx
                    .metrics()
                    .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::Overload);
                tracing::debug!(target: "fluxum::http", %ip,
                    "refused an /rpc request: overload shed");
                return Ok(());
            }
        }
        let keep_alive = match (request.method.as_str(), request.path.as_str()) {
            // SPEC-025 OPS-030: while draining, refuse *new* `/rpc` work
            // with a retryable 503 so the client reconnects to the restarted
            // process (OPS-031).
            //
            // The refusal lives here, not in the accept loop: a drained shard
            // must still answer `/health` (so a load balancer sees it leave
            // rotation), `/metrics` (so the drain is observable) and `/drain`
            // itself. Refusing at accept would blind exactly the tooling the
            // drain depends on.
            ("POST" | "GET", "/rpc") if state.ctx.is_draining() => {
                write_simple(&mut stream, 503, "Service Unavailable").await?;
                return Ok(());
            }
            ("POST", "/rpc") => {
                handle_post(&state, &mut stream, ip, &request).await?;
                true
            }
            ("GET", "/rpc") => {
                // The GET stream owns the connection for its lifetime.
                handle_get(&state, stream, &request, server_shutdown).await?;
                return Ok(());
            }
            // Blob upload/download (SPEC-023 DMX-041) — out-of-band of the
            // 16 MB FluxRPC frame; shares this port with the admin surface.
            // F-002: closed to the unauthenticated — a valid `Fluxum-Session`
            // (an authenticated client) is required, so blob storage is not a
            // free anonymous read/write surface on a directly exposed port.
            ("POST", "/blob") if !is_authed_session(&state, &request).await => {
                write_simple(&mut stream, 401, "Unauthorized").await?;
                true
            }
            ("POST", "/blob") => {
                handle_blob_upload(&state.ctx, &mut stream, &request).await?;
                true
            }
            ("GET", path)
                if path.strip_prefix("/blob/").is_some()
                    && !is_authed_session(&state, &request).await =>
            {
                write_simple(&mut stream, 401, "Unauthorized").await?;
                true
            }
            ("GET", path) if path.strip_prefix("/blob/").is_some() => {
                let hash = path.strip_prefix("/blob/").unwrap_or_default().to_owned();
                handle_blob_download(&state.ctx, &mut stream, &hash).await?;
                true
            }
            // SPEC-024 DEV-012: the structured-log stream. Owns the
            // connection while following, like `GET /rpc`.
            ("GET", path) if path == "/logs" || path.starts_with("/logs?") => {
                handle_logs(&state.ctx, stream, &request, ip, &server_shutdown).await?;
                return Ok(());
            }
            // The HTTP/JSON admin surface (RPC-050) shares this port.
            (method, path) if crate::admin::is_admin_path(path) => {
                handle_admin(
                    &state.ctx,
                    &mut stream,
                    method,
                    path,
                    &request.body,
                    ip,
                    &request,
                )
                .await?;
                true
            }
            // Static files, last: every real route above wins, so enabling a
            // static dir can never shadow `/rpc` or the admin surface.
            ("GET", path) if state.options.static_dir.is_some() => {
                handle_static(&state, &mut stream, path).await?;
                // `false`: do not keep this connection alive — see the header
                // comment in `handle_static` for why the socket has to go back
                // to the browser's pool immediately.
                false
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
    stream: &mut MaybeTls,
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
    stream: &mut MaybeTls,
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

/// `GET /logs[?follow=1]` (SPEC-024 DEV-012): the server's recent structured
/// log lines as NDJSON chunks, then — with `follow` — the connection stays
/// open and new lines stream as they are emitted (blank-line keep-alives in
/// the gaps). Rides the SEC-054 admin guard: loopback free, a remote needs
/// trust + credential. Lines are always JSON (the tap's contract) whatever
/// the console format; the emitted set is already governed by the
/// subscriber's global level filter, finer filtering is the client's job
/// (`fluxum logs --level`).
async fn handle_logs(
    ctx: &Arc<ShardContext>,
    mut stream: MaybeTls,
    request: &Request,
    client_ip: std::net::IpAddr,
    server_shutdown: &Arc<Notify>,
) -> io::Result<()> {
    let operator_token = request.header("fluxum-operator");
    let admin_req = crate::admin::AdminRequest {
        method: "GET",
        path: &request.path,
        body: &[],
        client_ip,
        operator_token: operator_token.as_deref(),
    };
    if let Err(deny) = crate::admin::check_access(ctx, &admin_req) {
        let bytes = serde_json::to_vec(&deny.body).unwrap_or_default();
        return write_json(&mut stream, deny.status, "application/json", &bytes).await;
    }
    let Some((ring, mut live)) = crate::logging::LogTap::subscribe() else {
        // An embedded assembly that never called `logging::init` has no tap.
        return write_simple(&mut stream, 503, "Service Unavailable").await;
    };
    let follow = request.path.split_once('?').is_some_and(|(_, query)| {
        query
            .split('&')
            .any(|p| matches!(p, "follow" | "follow=1" | "follow=true"))
    });

    const HEAD: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n\
                        Transfer-Encoding: chunked\r\n\
                        X-Content-Type-Options: nosniff\r\nCache-Control: no-cache\r\n\r\n";
    stream.write_all(HEAD.as_bytes()).await?;
    stream.flush().await?;
    for line in ring {
        if write_chunk(&mut stream, format!("{line}\n").as_bytes())
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    if !follow {
        return write_last_chunk(&mut stream).await;
    }
    let mut keepalive = tokio::time::interval(std::time::Duration::from_secs(15));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await; // arm: the first tick fires immediately
    loop {
        tokio::select! {
            _ = server_shutdown.notified() => break,
            line = live.recv() => match line {
                Ok(line) => {
                    if write_chunk(&mut stream, format!("{line}\n").as_bytes()).await.is_err() {
                        return Ok(()); // follower went away — not an error
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Honest gap marker: the follower was slower than the
                    // log volume; n lines are not in this stream.
                    let marker = format!("{{\"fluxum_logs_dropped\":{n}}}\n");
                    if write_chunk(&mut stream, marker.as_bytes()).await.is_err() {
                        return Ok(());
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
            _ = keepalive.tick() => {
                if write_chunk(&mut stream, b"\n").await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    write_last_chunk(&mut stream).await
}

async fn handle_admin(
    ctx: &Arc<ShardContext>,
    stream: &mut MaybeTls,
    method: &str,
    path: &str,
    body: &[u8],
    client_ip: std::net::IpAddr,
    request: &Request,
) -> io::Result<()> {
    // SEC-054: the operator credential travels in the `Fluxum-Operator`
    // header, or (compat with `/audit`) a JSON `token` field in the body.
    let operator_token = request.header("fluxum-operator").or_else(|| {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("token").and_then(|t| t.as_str()).map(str::to_owned))
    });
    let response = crate::admin::dispatch(
        ctx,
        crate::admin::AdminRequest {
            method,
            path,
            body,
            client_ip,
            operator_token: operator_token.as_deref(),
        },
    )
    .await;
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
    stream: &mut MaybeTls,
    ip: std::net::IpAddr,
    request: &Request,
) -> io::Result<()> {
    if !request.header_eq("content-type", CONTENT_TYPE) {
        return write_simple(stream, 415, "Unsupported Media Type").await;
    }
    let session_token = request.header("fluxum-session");

    // SEC-031: a request without a session token is a pre-auth handshake; cap
    // its body so an oversized handshake is refused before it is parsed.
    if session_token.is_none()
        && let Some(cap) = state.ctx.conn_guard().limits().handshake_max_bytes
        && request.body.len() > cap as usize
    {
        state
            .ctx
            .metrics()
            .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::HandshakeBudget);
        return write_simple(stream, 413, "Payload Too Large").await;
    }

    let codec = FrameCodec::new(state.options.max_frame_bytes);
    let messages = match decode_frames(&codec, &request.body) {
        Ok(messages) => messages,
        Err(code) => {
            // PROTO_FRAME_TOO_LARGE → 413, PROTO_MALFORMED → 400 (SPEC-028 §7).
            return write_simple(stream, http_status(code), "").await;
        }
    };
    // SEC-031: does this request carry an `Authenticate`? Combined with the
    // pre-routing auth state (below), its outcome drives the brute-force
    // throttle.
    let carries_authenticate = messages
        .iter()
        .any(|m| matches!(m, fluxum_protocol::ClientMessage::Authenticate(_)));

    // Load or create the session's router state — including its OPS-050
    // namespace binding, which must survive this transport rebuilding a
    // `Session` per request. `resolved_id` is the session's *current* at-rest
    // id (post-grace-follow) that a later state write / rotation targets.
    let mut resolved_id: Option<String> = None;
    let (mut router_state, connection_id, new_session, mut namespace) =
        if let Some(token) = &session_token {
            // SEC-050: resolve by the token's hash, following a grace mapping
            // if this is a just-rotated token still in its window.
            let id = resolve_id(state, token).await;
            let mut sessions = state.sessions.lock().await;
            match id
                .as_ref()
                .and_then(|id| sessions.get_mut(id).map(|s| (id, s)))
            {
                Some((id, sess)) => {
                    // SEC-053: an operator terminated this session.
                    if sess.revoked.load(std::sync::atomic::Ordering::SeqCst) {
                        let (cid, id) = (sess.connection_id, id.clone());
                        drop(sessions);
                        state
                            .ctx
                            .metrics()
                            .note_session_rejected(SessionRejectReason::Revoked);
                        fluxum_core::secevent::session_rejected(ip, "revoked");
                        evict_session(state, &id, cid).await;
                        return write_simple(stream, 404, "Not Found").await;
                    }
                    // SEC-052: absolute lifetime, on top of RPC-060 idle.
                    if let Some(max) = state.policy.absolute_lifetime
                        && sess.created.elapsed() >= max
                    {
                        let (cid, id) = (sess.connection_id, id.clone());
                        drop(sessions);
                        state
                            .ctx
                            .metrics()
                            .note_session_rejected(SessionRejectReason::Expired);
                        fluxum_core::secevent::session_rejected(ip, "expired");
                        evict_session(state, &id, cid).await;
                        return write_simple(stream, 404, "Not Found").await;
                    }
                    // SEC-051: a token presented from another IP is a
                    // suspected hijack — refused and counted, session intact.
                    if state.policy.bind_client_ip && sess.client_ip != Some(ip) {
                        drop(sessions);
                        state
                            .ctx
                            .metrics()
                            .note_session_rejected(SessionRejectReason::IpMismatch);
                        fluxum_core::secevent::session_rejected(ip, "ip_mismatch");
                        return write_simple(stream, 403, "Forbidden").await;
                    }
                    sess.last_active = Instant::now();
                    resolved_id = Some(id.clone());
                    (
                        sess.state.clone(),
                        sess.connection_id,
                        false,
                        sess.namespace.clone(),
                    )
                }
                None => {
                    drop(sessions);
                    // SEC-050 anti-fixation: a token the server never minted is
                    // NEVER adopted. If the request re-authenticates it starts a
                    // fresh session (new token minted below); otherwise it is a
                    // stale/expired session and gets the RPC-060 404.
                    if carries_authenticate {
                        (SessionState::Unauthenticated, 0, true, None)
                    } else {
                        state
                            .ctx
                            .metrics()
                            .note_session_rejected(SessionRejectReason::UnknownToken);
                        fluxum_core::secevent::session_rejected(ip, "unknown_token");
                        return write_simple(stream, 404, "Not Found").await;
                    }
                }
            }
        } else {
            (SessionState::Unauthenticated, 0, true, None)
        };
    let was_authed = matches!(router_state, SessionState::Authenticated { .. });

    // Route every frame through the transport-independent session core, in
    // the session's database. Committed diffs fan out from the single
    // writer at commit visibility (P0-A 1.3) — nothing to publish here.
    let mut responses: Vec<ServerMessage> = Vec::new();
    {
        let mut session = Session::with_state_in(
            Arc::clone(&state.ctx),
            router_state.clone(),
            namespace.clone(),
        );
        // SEC-047: key the source-side query-admission bucket on the
        // resolved client IP (proxy-aware, SEC-035).
        session.set_source_ip(ip);
        for message in messages {
            let routed = session.handle(message).await;
            responses.extend(routed.responses);
        }
        // An `Authenticate` in this batch may have bound the namespace.
        namespace = session.namespace().cloned();
        router_state = session.into_state();
    }

    // SEC-031: record a pre-auth `Authenticate` outcome so the guard can
    // throttle a brute-force reconnecting per guess. A success clears the
    // peer's streak; a failure advances it toward backoff.
    if !was_authed && carries_authenticate {
        if matches!(router_state, SessionState::Authenticated { .. }) {
            state.ctx.conn_guard().note_auth_success(ip);
            fluxum_core::secevent::auth_success(ip, &identity_hex_of(&router_state));
        } else {
            state.ctx.conn_guard().note_auth_failure(ip);
            fluxum_core::secevent::auth_failure(ip, "bad_credential");
        }
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
        // SEC-050: a CSPRNG token; only its hash id is stored (the map key).
        let minted = crate::session_sec::mint();
        let now = Instant::now();
        let client_ip = state.policy.bind_client_ip.then_some(ip);
        let revoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let identity_hex = identity_hex_of(&router_state);
        state.sessions.lock().await.insert(
            minted.id.clone(),
            HttpSession {
                state: router_state.clone(),
                connection_id,
                shutdown: Arc::clone(&shutdown),
                out_rx: Some(out_rx),
                last_active: now,
                namespace: namespace.clone(),
                created: now,
                issued: now,
                client_ip,
                revoked: Arc::clone(&revoked),
            },
        );
        // SEC-053: mirror into the admin directory.
        state.control.insert(
            minted.id.clone(),
            ControlEntry {
                identity_hex,
                connection_id,
                created: now,
                client_ip,
                shutdown,
                revoked,
            },
        );
        issued_token = Some(minted.raw);
        // RED-011: run the `on_connect` hooks for the fresh session, in its
        // own database; their diff fans out via the commit hook (P0-A 1.3).
        if let SessionState::Authenticated { caller, .. } = &router_state {
            let engine = match &namespace {
                Some(ns) => ns.engine(),
                None => &state.ctx.engine,
            };
            if let Err(e) = engine
                .client_connected(caller.identity, caller.connection_id)
                .await
            {
                tracing::warn!(target: "fluxum::server", error = %e, "on_connect hook failed");
            }
        }
    } else if let Some(id) = &resolved_id {
        // Persist any state change (e.g. a re-auth) back to the store, and
        // decide whether the token rotates (SEC-052): a re-auth always
        // rotates, and so does crossing the rotate interval.
        let reauthed = !was_authed && matches!(router_state, SessionState::Authenticated { .. });
        let mut sessions = state.sessions.lock().await;
        if let Some(sess) = sessions.get_mut(id) {
            sess.state = router_state.clone();
            sess.namespace = namespace.clone();
            let due = state
                .policy
                .rotate_interval
                .is_some_and(|iv| sess.issued.elapsed() >= iv);
            // Re-key the session under a fresh token; the old id lingers in
            // the grace map so an in-flight request with it still lands.
            if (reauthed || due)
                && let Some(mut old) = sessions.remove(id)
            {
                let minted = crate::session_sec::mint();
                old.issued = Instant::now();
                sessions.insert(minted.id.clone(), old);
                drop(sessions);
                state.control.rekey(id, minted.id.clone());
                let deadline = Instant::now() + state.policy.rotate_grace;
                state
                    .grace
                    .lock()
                    .await
                    .insert(id.clone(), (minted.id.clone(), deadline));
                issued_token = Some(minted.raw);
            }
        }
    }
    let _ = connection_id;

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
    mut stream: MaybeTls,
    request: &Request,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let Some(raw_token) = request.header("fluxum-session") else {
        return write_simple(&mut stream, 404, "Not Found").await;
    };
    // SEC-050: resolve the token to its session id (grace-aware).
    let Some(token) = resolve_id(state, &raw_token).await else {
        state
            .ctx
            .metrics()
            .note_session_rejected(SessionRejectReason::UnknownToken);
        return write_simple(&mut stream, 404, "Not Found").await;
    };
    // Take the outbound receiver + shutdown for this session.
    let (mut out_rx, shutdown, connection_id) = {
        let mut sessions = state.sessions.lock().await;
        match sessions.get_mut(&token) {
            Some(sess) => {
                // SEC-053/052/051: a revoked, over-lifetime, or IP-mismatched
                // session must not open a push stream either.
                if sess.revoked.load(std::sync::atomic::Ordering::SeqCst) {
                    drop(sessions);
                    state
                        .ctx
                        .metrics()
                        .note_session_rejected(SessionRejectReason::Revoked);
                    return write_simple(&mut stream, 404, "Not Found").await;
                }
                match sess.out_rx.take() {
                    Some(rx) => (rx, Arc::clone(&sess.shutdown), sess.connection_id),
                    None => {
                        drop(sessions);
                        // A stream is already open for this session.
                        return write_simple(&mut stream, 409, "Conflict").await;
                    }
                }
            }
            None => {
                drop(sessions);
                state
                    .ctx
                    .metrics()
                    .note_session_rejected(SessionRejectReason::UnknownToken);
                return write_simple(&mut stream, 404, "Not Found").await;
            }
        }
    };

    // Chunked streaming response header.
    write_stream_header(&mut stream).await?;

    let keepalive = state.options.keepalive;
    let idle = state.options.idle_timeout;

    // The first keep-alive fires almost immediately; the configured cadence
    // takes over after it.
    //
    // Not cosmetic — without it the browser transport is unusable, and the
    // reason took measuring to find. A browser does not surface this response
    // at all (`fetch()` stays unresolved, headers included) until a chunk
    // written from *this loop* arrives. Writes issued before the loop are
    // invisible to it: a priming frame right after the header changed nothing,
    // and neither did padding that frame to 2 KB.
    //
    // The proof is in the timing. With the 20 s default the browser released
    // the response at 20004 ms; dropping the cadence to 3 s moved it to
    // 3055 ms; adding a 500 ms pre-loop write moved it to 3505 ms — always
    // the first loop tick, never the writes before it. `curl` and Node see
    // the headers in single-digit milliseconds either way, which is exactly
    // why this survived every non-browser test.
    //
    // So the opening tick is what a browser waits on, and the cadence is what
    // an idle connection needs afterwards (RPC-006). Both are keep-alive
    // frames, which receivers ignore (RPC-001).
    const OPENING_TICK: Duration = Duration::from_millis(50);
    let mut keepalive_timer = tokio::time::interval_at(Instant::now() + OPENING_TICK, keepalive);

    let mut exit = GetExit::Expired;
    let result = loop {
        let idle_deadline = idle.map(|d| Instant::now() + d);
        tokio::select! {
            _ = server_shutdown.notified() => break Ok(()),
            _ = shutdown.notified() => break Ok(()),
            frame = out_rx.recv() => match frame {
                Some(frame) => {
                    // OBS-023: same queue_wait stage as the TCP writer.
                    state.ctx.metrics().note_fanout_stage(
                        fluxum_core::metrics::FanoutStage::QueueWait,
                        u64::try_from(frame.enqueued_at.elapsed().as_micros()).unwrap_or(u64::MAX),
                    );
                    if write_chunk(&mut stream, &frame.bytes).await.is_err() {
                        // The client vanished mid-stream: a transport blip,
                        // not a goodbye (SPEC-021 CS-021).
                        exit = GetExit::Detached;
                        break Ok(());
                    }
                }
                None => break Ok(()), // the connection's sink was dropped
            },
            _ = keepalive_timer.tick() => {
                // Zero-length keep-alive frame (RPC-001/006).
                if write_chunk(&mut stream, &FrameCodec::keepalive()).await.is_err() {
                    exit = GetExit::Detached;
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

    // SPEC-021 CS-021: a stream the client dropped only *detaches* — the
    // session, its subscriptions and their retained delta windows stay put
    // so a reconnecting client can `Resume` from its offset instead of
    // re-downloading the snapshot. Give the receiver back so the next GET
    // with this token reattaches and drains what queued meanwhile. The
    // session still dies on idle (the sweeper below owns that once no
    // stream is holding the timer), which is where `on_disconnect` fires.
    if matches!(exit, GetExit::Detached)
        && let Some(session) = state.sessions.lock().await.get_mut(&token)
    {
        session.out_rx = Some(out_rx);
        return result;
    }

    // Terminate the chunked body and evict the session.
    let _ = write_last_chunk(&mut stream).await;
    evict_session(state, &token, connection_id).await;
    result
}

/// Retire a session for good: drop it from the registry, deregister its
/// connection and subscriptions, and run the RED-012 `on_disconnect` hooks,
/// publishing their diff so a presence cleanup reaches the remaining
/// subscribers.
async fn evict_session(state: &Arc<HttpState>, token: &str, connection_id: u128) {
    let evicted = state.sessions.lock().await.remove(token);
    if evicted.is_none() {
        return; // already retired (a racing sweep or stream teardown)
    }
    // Drop the admin-directory mirror and any grace mapping pointing here.
    state.control.remove(token);
    state
        .grace
        .lock()
        .await
        .retain(|old, (current, _)| old != token && current != token);
    state.ctx.metrics().note_disconnect(); // OBS-040
    state.ctx.connections.remove(connection_id).await;
    // Deregister from the session's own database (OPS-050).
    let namespace = evicted.as_ref().and_then(|s| s.namespace.clone());
    match &namespace {
        Some(ns) => ns.subscriptions().lock().await.disconnect(connection_id),
        None => state
            .ctx
            .subscriptions
            .lock()
            .await
            .disconnect(connection_id),
    }
    if let Some(session) = evicted
        && let SessionState::Authenticated { caller, .. } = &session.state
    {
        let engine = match &namespace {
            Some(ns) => ns.engine(),
            None => &state.ctx.engine,
        };
        // The commit hook (P0-A 1.3) fans the hook's diff out.
        if let Err(e) = engine
            .client_disconnected(caller.identity, caller.connection_id)
            .await
        {
            tracing::warn!(target: "fluxum::server", error = %e, "on_disconnect hook failed");
        }
    }
}

/// RPC-060 for sessions with no stream holding the idle timer: a session
/// that is detached (its client blipped away, SPEC-021 CS-021) or was minted
/// but never attached has nobody running the in-stream deadline, so this
/// sweeper retires it once it goes idle. Without it, a client that never
/// comes back would pin its subscriptions — and their retained delta windows
/// — forever, and its `on_disconnect` would never fire.
fn spawn_session_sweeper(state: Arc<HttpState>, shutdown: Arc<Notify>) {
    let Some(idle) = state.options.idle_timeout else {
        return; // idle expiry disabled
    };
    tokio::spawn(async move {
        // Check at a fraction of the deadline so eviction is timely without
        // busy-waiting.
        let tick = (idle / 4).max(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = shutdown.notified() => break,
                () = tokio::time::sleep(tick) => {}
            }
            let stale: Vec<(String, u128)> = {
                let sessions = state.sessions.lock().await;
                sessions
                    .iter()
                    // `out_rx` present = no live GET stream draining it.
                    .filter(|(_, s)| s.out_rx.is_some() && s.last_active.elapsed() >= idle)
                    .map(|(token, s)| (token.clone(), s.connection_id))
                    .collect()
            };
            for (token, connection_id) in stale {
                evict_session(&state, &token, connection_id).await;
            }
        }
    });
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
async fn read_request(stream: &mut MaybeTls, buf: &mut Vec<u8>) -> io::Result<Option<Request>> {
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
    stream: &mut MaybeTls,
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
    stream: &mut MaybeTls,
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

/// Serve a file from `static_dir` (see [`crate::statics`]).
///
/// A path that would escape the root is answered 404 rather than 403: telling
/// a prober which traversals were *rejected* rather than *absent* maps out the
/// filesystem for them.
async fn handle_static(
    state: &Arc<HttpState>,
    stream: &mut MaybeTls,
    path: &str,
) -> io::Result<()> {
    let Some(root) = state.options.static_dir.as_deref() else {
        return write_simple(stream, 404, "Not Found").await;
    };
    let Some(file) = crate::statics::resolve(root, path) else {
        return write_simple(stream, 404, "Not Found").await;
    };
    let Ok(body) = tokio::fs::read(&file).await else {
        return write_simple(stream, 404, "Not Found").await;
    };

    // `Connection: close`, deliberately.
    //
    // A browser allows ~6 concurrent connections per origin on HTTP/1.1, and
    // Fluxum's push stream (RPC-006) holds one of them for the session's whole
    // life. Keeping asset connections alive means the page's own HTML, CSS and
    // JS sit in that pool competing with it — and a page served from the same
    // origin as `/rpc` can exhaust the pool before it ever opens the stream,
    // leaving `GET /rpc` queued in the browser with no request on the wire and
    // no error anywhere. Closing after each file returns the socket at once.
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\
         Cache-Control: no-cache\r\nConnection: close\r\n\r\n",
        crate::statics::content_type(&file),
        body.len(),
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

async fn write_simple(stream: &mut MaybeTls, code: u16, reason: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: 0\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

/// Write the `GET /rpc` streaming response head (RPC-006).
///
/// `X-Content-Type-Options: nosniff` is load-bearing, not hygiene. Browsers
/// MIME-sniff an unrecognised `Content-Type`, and sniffing needs bytes — so
/// Chrome holds the `fetch()` promise unresolved until the first chunk
/// arrives. A push stream is idle by definition until the first commit, which
/// made every browser client hang at connect while Node (which does not sniff)
/// worked fine. `nosniff` tells the browser there is nothing to guess.
async fn write_stream_header(stream: &mut MaybeTls) -> io::Result<()> {
    // `nosniff` because the content type is not one a browser recognises, and
    // sniffing an unrecognised type means waiting for bytes a push stream has
    // no reason to send yet. (It is not what caused the slow-open bug — see
    // `handle_get` — but it is still the right header.)
    const HEAD: &str = concat!(
        "HTTP/1.1 200 OK\r\nContent-Type: ",
        "application/x-fluxum",
        "\r\nTransfer-Encoding: chunked\r\n",
        "X-Content-Type-Options: nosniff\r\nCache-Control: no-cache\r\n\r\n",
    );
    stream.write_all(HEAD.as_bytes()).await?;
    stream.flush().await
}

async fn write_chunk(stream: &mut MaybeTls, data: &[u8]) -> io::Result<()> {
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

async fn write_last_chunk(stream: &mut MaybeTls) -> io::Result<()> {
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

/// The caller identity (hex) of an authenticated session, for the SEC-053
/// admin directory; empty for an unauthenticated state.
fn identity_hex_of(state: &SessionState) -> String {
    match state {
        SessionState::Authenticated { caller, .. } => {
            use std::fmt::Write as _;
            caller.identity.as_bytes().iter().fold(
                String::with_capacity(caller.identity.as_bytes().len() * 2),
                |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                },
            )
        }
        SessionState::Unauthenticated => String::new(),
    }
}

/// Resolve a presented raw token to the at-rest id of a *live* session
/// (SEC-050/052): the token's own hash if it keys a session, else a
/// still-in-window grace mapping from a just-rotated token, else `None`
/// (unknown — never adopted). Expired grace entries are purged here.
/// Whether the request carries a `Fluxum-Session` that resolves to a live,
/// authenticated session (F-002 blob gate). A revoked session does not count.
async fn is_authed_session(state: &Arc<HttpState>, request: &Request) -> bool {
    let Some(raw) = request.header("fluxum-session") else {
        return false;
    };
    let Some(id) = resolve_id(state, &raw).await else {
        return false;
    };
    let sessions = state.sessions.lock().await;
    sessions.get(&id).is_some_and(|s| {
        !s.revoked.load(std::sync::atomic::Ordering::SeqCst)
            && matches!(s.state, SessionState::Authenticated { .. })
    })
}

async fn resolve_id(state: &Arc<HttpState>, raw_token: &str) -> Option<String> {
    let id = token_id(raw_token);
    if state.sessions.lock().await.contains_key(&id) {
        return Some(id);
    }
    let mut grace = state.grace.lock().await;
    let now = Instant::now();
    grace.retain(|_, (_, deadline)| now < *deadline);
    grace.get(&id).map(|(current, _)| current.clone())
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
