//! FluxRPC TCP transport driver (SPEC-006 §3; FR-40/42/45): the tokio
//! listener that binds :15801, the per-connection read/route/write loop, the
//! RPC-060 idle timeout and RPC-061 frame-size limit, and the post-commit
//! `TxUpdate` fan-out.
//!
//! # Concurrency shape
//!
//! - One **reader** future per connection decodes frames and routes each
//!   through the [`Session`], forwarding responses to the writer.
//! - One **writer** task per connection drains a bounded queue to the
//!   socket — the SUB-042 per-client send buffer. Responses and pushed
//!   `TxUpdate`s share this queue, so multiplexing (RPC-002) falls out of
//!   the single ordered writer.
//! - One shard-wide **fan-out** task evaluates every committed diff against
//!   the subscription manager once (SUB-021, lock held only across
//!   evaluation) and pushes the shared encoded `TxUpdate` to each
//!   subscriber's queue; a full queue trips the SUB-042 drop.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio_rustls::TlsAcceptor;

use fluxum_protocol::{ClientMessage, ErrorMessage, Frame, FrameCodec, ServerMessage, codes};

use crate::connguard::ConnPermit;
use crate::session::Session;
use crate::tls::MaybeTls;
use crate::{ConnHandle, OutFrame, ShardContext};

/// TCP transport tuning (RPC-060/061).
#[derive(Debug, Clone, Copy)]
pub struct TcpOptions {
    /// Idle timeout: a connection with no inbound frame for this long is
    /// sent `408` and closed. `None` disables (RPC-060).
    pub idle_timeout: Option<Duration>,
    /// Max inbound frame body size (RPC-061); a larger frame is `413` + close.
    pub max_frame_bytes: u32,
    /// Per-connection outbound queue depth (the SUB-042 send buffer, in
    /// frames). A full queue drops the connection.
    pub send_queue_depth: usize,
    /// Listener/socket hardening knobs (SEC-042); defaults = today's
    /// behavior.
    pub socket: crate::sock::SocketOptions,
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(60)),
            max_frame_bytes: fluxum_protocol::DEFAULT_MAX_FRAME_BYTES,
            send_queue_depth: 1024,
            socket: crate::sock::SocketOptions::default(),
        }
    }
}

/// A running TCP transport: the bound address and a shutdown handle.
pub struct TcpServer {
    /// The actually-bound local address (useful when the config port is 0).
    pub local_addr: std::net::SocketAddr,
    shutdown: Arc<Notify>,
}

impl TcpServer {
    /// Signal every connection and the accept loop to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// Bind `addr` and start serving FluxRPC/TCP over `ctx`. Spawns the accept
/// loop and the fan-out task; returns once the socket is bound (so tests can
/// read `local_addr`). Runs until [`TcpServer::shutdown`].
pub async fn serve(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: TcpOptions,
) -> io::Result<TcpServer> {
    serve_tls(ctx, addr, options, None).await
}

/// [`serve`] with optional built-in TLS termination (SPEC-026 SEC-059): when
/// `tls` is set, a directly-accepted connection completes the TLS handshake
/// before the first FluxRPC frame. A trusted-proxy connection (PROXY v2) is
/// left plaintext — the proxy terminates TLS and forwards on a trusted link.
pub async fn serve_tls(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: TcpOptions,
    tls: Option<TlsAcceptor>,
) -> io::Result<TcpServer> {
    let listener = crate::sock::bind(addr, options.socket).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(Notify::new());

    // Shard-wide commit fan-out (SUB-021).
    crate::spawn_fanout(Arc::clone(&ctx), shutdown.clone());
    // Ephemeral TTL sweeper (DMX-011) — idempotent across transports.
    ctx.start_ephemeral_sweeper();
    ctx.start_ttl_sweeper();

    // Accept loop.
    let accept_ctx = Arc::clone(&ctx);
    let accept_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accept_shutdown.notified() => break,
                accepted = listener.accept() => {
                    match accepted {
                        // SPEC-025 OPS-030: while draining, admit nobody new.
                        // Closing immediately is the retryable signal on a raw
                        // socket — the client reconnects and lands on the
                        // restarted process (OPS-031). Connections already
                        // established keep being serviced.
                        Ok(_) if accept_ctx.is_draining() => {
                            tracing::debug!(target: "fluxum::tcp",
                                "refused a connection: draining");
                        }
                        // SEC-041: under overload, every new TCP connection is
                        // pre-auth work by definition (a session only exists
                        // after `Authenticate` on this very socket) — shed it
                        // at accept, the cheapest possible point. Established
                        // connections are their own long-lived sockets and are
                        // never touched.
                        Ok((stream, peer)) if accept_ctx.overload_state()
                            != fluxum_core::metrics::OverloadState::Normal =>
                        {
                            accept_ctx
                                .metrics()
                                .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::Overload);
                            tracing::debug!(target: "fluxum::tcp", ip = %peer.ip(),
                                "refused a connection: overload shed");
                            drop(stream);
                        }
                        Ok((stream, peer)) => {
                            // Realtime wire: a TxUpdate or ReducerResult is a
                            // small frame that must leave NOW — Nagle would
                            // batch it behind an unacked predecessor and turn
                            // sub-ms fan-out into multi-ms (NFR-04/NFR-05).
                            let _ = stream.set_nodelay(true);
                            // SEC-042: keepalive so dead peers stop holding
                            // connection slots.
                            crate::sock::apply_keepalive(&stream, options.socket.tcp_keepalive);
                            // SEC-030/031: gate the pre-auth surface per peer IP
                            // before a session exists. A refusal is counted and
                            // the socket dropped (closed) — the cheapest signal
                            // to a flooding/throttled client.
                            let ip = peer.ip();
                            let trusted = accept_ctx.trusted_proxies();
                            if !trusted.is_empty() && trusted.contains(ip.to_canonical()) {
                                // SEC-036: a trusted proxy announces the real
                                // client in a PROXY v2 preamble; the guard must
                                // key on *that* IP, so admission happens after
                                // the (bounded) preamble read. The proxy already
                                // terminated TLS, so this hop stays plaintext.
                                let conn_ctx = Arc::clone(&accept_ctx);
                                let conn_shutdown = accept_shutdown.clone();
                                tokio::spawn(async move {
                                    handle_proxied_conn(conn_ctx, stream, ip, options, conn_shutdown).await;
                                });
                            } else {
                                // SEC-036: with proxy awareness on, a preamble
                                // from anyone *not* trusted is a protocol error
                                // — detected in the read loop.
                                let detect_preamble = !trusted.is_empty();
                                match accept_ctx.conn_guard().try_accept(ip) {
                                    Ok(permit) => {
                                        let conn_ctx = Arc::clone(&accept_ctx);
                                        let conn_shutdown = accept_shutdown.clone();
                                        let tls = tls.clone();
                                        tokio::spawn(async move {
                                            // SEC-059: terminate TLS (if configured)
                                            // before the first frame is read.
                                            let conn = match MaybeTls::accept(stream, tls.as_ref()).await {
                                                Ok(conn) => conn,
                                                Err(e) => {
                                                    tracing::debug!(target: "fluxum::tcp", error = %e, "TLS handshake failed");
                                                    return;
                                                }
                                            };
                                            if let Err(e) = drive_connection(conn_ctx, conn, ip, permit, options, Vec::new(), detect_preamble, conn_shutdown).await {
                                                tracing::debug!(target: "fluxum::tcp", error = %e, "connection ended");
                                            }
                                        });
                                    }
                                    Err(reason) => {
                                        accept_ctx.metrics().note_conn_rejected(reason);
                                        fluxum_core::secevent::conn_rejected(ip, reason.as_str());
                                        drop(stream);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(target: "fluxum::tcp", error = %e, "accept failed");
                        }
                    }
                }
            }
        }
    });

    Ok(TcpServer {
        local_addr,
        shutdown,
    })
}

/// Admit a connection from a trusted proxy (SEC-036): read the PROXY v2
/// preamble first — bounded by the SEC-031 handshake time budget — resolve
/// the real client IP from it, and only then run the guard, keyed on that
/// IP. A trusted peer that opens with ordinary frame bytes instead of a
/// preamble is the proxy host itself talking (a probe): it is admitted under
/// its own IP. A malformed preamble is counted and the socket dropped.
async fn handle_proxied_conn(
    ctx: Arc<ShardContext>,
    mut stream: TcpStream,
    peer_ip: std::net::IpAddr,
    options: TcpOptions,
    server_shutdown: Arc<Notify>,
) {
    use crate::clientip::{V2Preamble, is_v2_signature_prefix, parse_v2_preamble};
    use fluxum_core::metrics::ConnRejectReason;

    let limits = *ctx.conn_guard().limits();
    let deadline = limits
        .handshake_timeout
        .map(|budget| tokio::time::Instant::now() + budget);

    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut chunk = [0u8; 1024];
    let resolved = loop {
        // Not a preamble: the trusted proxy host is its own client; the
        // buffered bytes are the start of an ordinary frame stream.
        if !is_v2_signature_prefix(&buf) {
            break peer_ip;
        }
        if buf.len() >= crate::clientip::V2_SIG.len() {
            match parse_v2_preamble(&buf) {
                Ok(V2Preamble::Complete { source, consumed }) => {
                    buf.drain(..consumed);
                    // LOCAL/UNSPEC name no client: the proxy speaks for itself.
                    break source.map_or(peer_ip, |ip| ip.to_canonical());
                }
                Ok(V2Preamble::Incomplete) => {}
                Err(e) => {
                    ctx.metrics()
                        .note_conn_rejected(ConnRejectReason::ProxyPreamble);
                    tracing::debug!(target: "fluxum::tcp", ip = %peer_ip, error = %e,
                        "refused a connection: malformed PROXY v2 preamble from a trusted proxy");
                    return;
                }
            }
        }
        let read = async {
            match deadline {
                Some(d) => tokio::time::timeout_at(d, stream.read(&mut chunk))
                    .await
                    .unwrap_or(Ok(0)),
                None => stream.read(&mut chunk).await,
            }
        };
        let n = tokio::select! {
            _ = server_shutdown.notified() => return,
            n = read => n.unwrap_or(0),
        };
        if n == 0 {
            // EOF or handshake-budget expiry mid-preamble: a preamble that
            // never finishes is a slowloris on the pre-auth surface.
            ctx.metrics()
                .note_conn_rejected(ConnRejectReason::HandshakeBudget);
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    // SEC-030/031: the guard now sees the *client*, not the proxy — this is
    // the entire point: per-IP caps and backoff bite the abusive client
    // while the proxy's other clients keep connecting.
    match ctx.conn_guard().try_accept(resolved) {
        Ok(permit) => {
            // The proxy already terminated TLS; this trusted hop is plaintext.
            if let Err(e) = drive_connection(
                ctx,
                MaybeTls::Plain(stream),
                resolved,
                permit,
                options,
                buf,
                false,
                server_shutdown,
            )
            .await
            {
                tracing::debug!(target: "fluxum::tcp", error = %e, "connection ended");
            }
        }
        Err(reason) => {
            ctx.metrics().note_conn_rejected(reason);
            fluxum_core::secevent::conn_rejected(resolved, reason.as_str());
            drop(stream);
        }
    }
}

/// Drive one connection: read → route → write, with the idle timeout and
/// frame-size limit, until EOF, a fatal frame error, or shutdown.
///
/// `permit` holds the peer's SEC-030 concurrent-connection slot for the
/// connection's whole life (released on drop). While the session is
/// unauthenticated the SEC-031 handshake budget applies: a stricter pre-auth
/// frame-size cap and an absolute time budget to reach a successful
/// `Authenticate`, both to blunt slowloris.
///
/// `initial` seeds the frame buffer with bytes already read off the socket
/// (what followed a trusted proxy's preamble); `detect_preamble` arms the
/// SEC-036 check that refuses a PROXY v2 signature from an untrusted peer.
#[allow(clippy::too_many_arguments)]
async fn drive_connection(
    ctx: Arc<ShardContext>,
    stream: MaybeTls,
    ip: std::net::IpAddr,
    permit: ConnPermit,
    options: TcpOptions,
    initial: Vec<u8>,
    detect_preamble: bool,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let _permit = permit;
    let (mut read_half, write_half) = tokio::io::split(stream);
    let codec = FrameCodec::new(options.max_frame_bytes);
    // SEC-031: a stricter codec while unauthenticated caps pre-auth frames,
    // so an oversized handshake is refused from its 4-byte header before its
    // body is buffered. It is the *tighter* of the handshake cap and the
    // RPC-061 frame limit — the handshake budget only ever narrows, never
    // widens, the frame ceiling.
    let limits = *ctx.conn_guard().limits();
    let handshake_codec = match limits.handshake_max_bytes {
        Some(cap) => FrameCodec::new(cap.min(options.max_frame_bytes)),
        None => codec,
    };
    // SEC-031: absolute deadline to finish authenticating.
    let handshake_deadline = limits
        .handshake_timeout
        .map(|budget| tokio::time::Instant::now() + budget);

    // The per-connection outbound queue (SUB-042 send buffer) + its writer.
    let (out_tx, out_rx) = mpsc::channel::<OutFrame>(options.send_queue_depth);
    let conn_shutdown = Arc::new(Notify::new());
    // Set when this connection's read loop exits, for ancillary tasks that
    // hold an `out_tx` clone (the replication streamer): without it they
    // keep the writer — and the socket — alive after the server "died",
    // and a killed primary would keep heartbeating through it forever.
    let conn_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // RPC-008: armed (with the threshold) after the AuthResult that accepts
    // a gzip negotiation; the writer transforms every later frame.
    let send_wire: Arc<std::sync::OnceLock<usize>> = Arc::new(std::sync::OnceLock::new());
    let writer = tokio::spawn(writer_task(
        write_half,
        out_rx,
        Arc::clone(ctx.metrics()),
        Arc::clone(&send_wire),
    ));

    let mut session = Session::new(Arc::clone(&ctx));
    // SEC-047: key the source-side query-admission bucket on the resolved
    // client IP (proxy-aware, SEC-035).
    session.set_source_ip(ip);
    let mut buf: Vec<u8> = initial;
    let mut read_chunk = [0u8; 8192];
    let mut detect_preamble = detect_preamble;

    // `notify_waiters` wakes only ALREADY-CREATED `Notified` futures — a
    // fresh `.notified()` per select iteration misses a shutdown that
    // lands while a frame is being processed, and the connection then
    // outlives the server forever (a "killed" primary kept streaming
    // heartbeats through it, so no failover election ever fired). One
    // pinned future per source, created before the loop, catches it.
    let server_shutdown_seen = server_shutdown.notified();
    let conn_shutdown_seen = conn_shutdown.notified();
    tokio::pin!(server_shutdown_seen, conn_shutdown_seen);

    let result = 'conn: loop {
        let authed = session.is_authenticated();
        let active_codec = if authed { &codec } else { &handshake_codec };
        // SEC-036: with proxy awareness on, an untrusted peer opening with
        // the PROXY v2 signature is claiming to speak for someone else —
        // refused silently (no response bytes), counted. While the first
        // bytes are still an ambiguous prefix of the signature, hold off the
        // codec (it would misread them as an oversized frame) and read more;
        // the handshake deadline bounds that wait.
        if detect_preamble {
            if authed || !crate::clientip::is_v2_signature_prefix(&buf) {
                detect_preamble = false; // ordinary traffic; stop checking
            } else if buf.len() >= crate::clientip::V2_SIG.len() {
                ctx.metrics()
                    .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::ProxyPreamble);
                tracing::debug!(target: "fluxum::tcp", %ip,
                    "refused a connection: PROXY v2 preamble from an untrusted peer");
                break 'conn Ok(());
            }
        }
        // Drain any whole frames already buffered before reading more. (With
        // the preamble check still ambiguous, hold off the codec entirely —
        // it would misread the signature bytes as an oversized frame.)
        if !detect_preamble {
            loop {
                match active_codec.decode(&buf) {
                    Ok(Some((frame, consumed))) => {
                        let owned: Option<Vec<u8>> = match frame {
                            Frame::KeepAlive => None,
                            Frame::Body(body) => Some(body.to_vec()),
                        };
                        buf.drain(..consumed);
                        if let Some(body) = owned
                            && !route_frame(
                                &ctx,
                                &mut session,
                                ip,
                                &codec,
                                &body,
                                &out_tx,
                                &conn_shutdown,
                                &conn_closed,
                                &send_wire,
                            )
                            .await
                        {
                            break 'conn Ok(());
                        }
                    }
                    Ok(None) => break,
                    Err(too_large) => {
                        // Post-auth, a frame over the limit gets the RPC-061
                        // 413 + close — a real client deserves the diagnosis.
                        // Pre-auth it is a SEC-031 handshake-budget abuse
                        // event: counted and closed with ZERO response bytes
                        // (SEC-043 cheap reject) — an unauthenticated flood
                        // earns no amplification, however small.
                        if !authed {
                            ctx.metrics().note_conn_rejected(
                                fluxum_core::metrics::ConnRejectReason::HandshakeBudget,
                            );
                            break 'conn Ok(());
                        }
                        let msg =
                            error_frame(&codec, None, too_large.code(), too_large.to_string());
                        let _ = out_tx.send(msg).await;
                        break 'conn Ok(());
                    }
                }
            }
        }

        // SEC-031: while unauthenticated, the read waits no longer than the
        // remaining handshake budget; expiry is a slowloris refusal, not the
        // ordinary idle timeout.
        let handshake_timeout = if session.is_authenticated() {
            None
        } else {
            handshake_deadline
        };
        let read = tokio::select! {
            _ = &mut server_shutdown_seen => break Ok(()),
            _ = &mut conn_shutdown_seen => break Ok(()),
            read = read_with_deadline(&mut read_half, &mut read_chunk, options.idle_timeout, handshake_timeout) => read,
        };
        match read {
            ReadOutcome::Data(0) => break Ok(()), // clean EOF
            ReadOutcome::Data(n) => buf.extend_from_slice(&read_chunk[..n]),
            ReadOutcome::HandshakeExpired => {
                // SEC-031: never authenticated in time — drop silently, count.
                ctx.metrics()
                    .note_conn_rejected(fluxum_core::metrics::ConnRejectReason::HandshakeBudget);
                break Ok(());
            }
            ReadOutcome::Idle => {
                // RPC-060: send 408 then close.
                let msg = error_frame(&codec, None, codes::PROTO_IDLE_TIMEOUT, "idle timeout");
                let _ = out_tx.send(msg).await;
                break Ok(());
            }
            ReadOutcome::Err(e) => break Err(e),
        }
    };

    // Cleanup: deregister and stop the writer. The closed flag ends any
    // replication streamer holding an `out_tx` clone (bounded by its
    // heartbeat interval), so `writer.await` cannot hang on it.
    conn_closed.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(conn_id) = session.connection_id() {
        // The session's context, not the listener's: an SHD-011 rebound
        // session registered its fan-out handle on its affinity shard.
        session.ctx().metrics().note_disconnect(); // OBS-040
        session.ctx().connections.remove(conn_id).await;
        session.subscriptions().lock().await.disconnect(conn_id);
        // RED-012: run the `on_disconnect` hooks; their diff reaches the
        // remaining subscribers via the commit hook (P0-A 1.3), like any
        // commit.
        if let Some((identity, cid)) = session.caller().map(|c| (c.identity, c.connection_id))
            && let Err(e) = session.engine().client_disconnected(identity, cid).await
        {
            tracing::warn!(target: "fluxum::server", error = %e, "on_disconnect hook failed");
        }
    }
    drop(out_tx);
    let _ = writer.await;
    result
}

/// Decode one message body, route it, and forward every response frame to
/// the writer. Registers the connection's fan-out handle the moment it
/// authenticates. Returns `false` to close the connection.
#[allow(clippy::too_many_arguments)]
async fn route_frame(
    ctx: &Arc<ShardContext>,
    session: &mut Session,
    ip: std::net::IpAddr,
    codec: &FrameCodec,
    body: &[u8],
    out_tx: &mpsc::Sender<OutFrame>,
    conn_shutdown: &Arc<Notify>,
    conn_closed: &Arc<std::sync::atomic::AtomicBool>,
    send_wire: &Arc<std::sync::OnceLock<usize>>,
) -> bool {
    let message = match ClientMessage::decode(body) {
        Ok(message) => message,
        Err(e) => {
            // RPC-001: a malformed envelope is a 400; keep the connection.
            let frame = error_frame(
                codec,
                None,
                codes::PROTO_MALFORMED,
                format!("malformed message: {e}"),
            );
            return out_tx.send(frame).await.is_ok();
        }
    };

    // SPEC-014 REP-011: replication frames from an authenticated server
    // peer are handed to the replication service instead of the session
    // router — the session's outbound queue becomes the stream channel.
    // From anyone else they fall through to the router's refusal.
    if let Some(peer) = session.server_peer().map(str::to_owned) {
        match &message {
            ClientMessage::ReplicaHello(hello) => {
                // A non-primary member must not serve the stream (its
                // heartbeats would keep the dialer's election timer alive
                // and no failover would ever fire) — close, so the dialer
                // rotates on to the real primary (REP-030/REP-042).
                if let Some(election) = ctx.election()
                    && !election.role().is_primary()
                {
                    tracing::debug!(target: "fluxum::repl", from = %peer,
                        "hello refused: this member is not the primary");
                    return false;
                }
                // SHD-020/REP-014: `ReplicaHello.shard_id` names whose log
                // the replica wants; on a multi-shard deployment each shard
                // has its own log and its own primary service, so the hello
                // routes to that shard's context.
                let target = match ctx.coord() {
                    Some(coord) => match coord.host(hello.shard_id) {
                        Some(host) => Arc::clone(host),
                        None => {
                            tracing::warn!(target: "fluxum::repl", from = %peer,
                                shard = hello.shard_id,
                                "hello refused: unknown shard (SHD-010)");
                            return false;
                        }
                    },
                    None => Arc::clone(ctx),
                };
                if let Some(primary) = target.replication_primary() {
                    return primary.accept(
                        &target,
                        hello,
                        peer,
                        out_tx.clone(),
                        *codec,
                        Arc::clone(conn_closed),
                    );
                }
            }
            ClientMessage::ReplAck(ack) => {
                if let Some(primary) = ctx.replication_primary() {
                    primary.ack(ctx.metrics(), &peer, ack);
                    return true;
                }
            }
            // SPEC-014 REP-030: a peer's election ballot request. Answered
            // inline — the grant is persisted before the response leaves.
            ClientMessage::VoteRequest(request) => {
                if let Some(election) = ctx.election() {
                    let response = election.answer(ctx, request);
                    let message = fluxum_protocol::ServerMessage::VoteResponse(response);
                    if let Ok(body) = message.encode()
                        && let Ok(framed) = codec.encode(&body)
                    {
                        return out_tx
                            .send(OutFrame::now(std::sync::Arc::new(framed)))
                            .await
                            .is_ok();
                    }
                    return false;
                }
            }
            _ => {}
        }
    }

    let was_authed = session.is_authenticated();
    // SEC-031: track the outcome of a pre-auth `Authenticate` so the guard
    // can throttle a brute-force. A success clears the peer's failure streak;
    // a failure advances it toward the backoff threshold.
    let is_auth_attempt = !was_authed && matches!(message, ClientMessage::Authenticate(_));
    let routed = session.handle(message).await;
    if is_auth_attempt {
        if session.is_authenticated() {
            ctx.conn_guard().note_auth_success(ip);
            let id_hex = session
                .caller()
                .map(|c| {
                    use std::fmt::Write as _;
                    c.identity
                        .as_bytes()
                        .iter()
                        .fold(String::new(), |mut a, b| {
                            let _ = write!(a, "{b:02x}");
                            a
                        })
                })
                .unwrap_or_default();
            fluxum_core::secevent::auth_success(ip, &id_hex);
        } else {
            ctx.conn_guard().note_auth_failure(ip);
            fluxum_core::secevent::auth_failure(ip, "bad_credential");
        }
    }

    // On the auth transition, register this connection for the fan-out.
    if !was_authed
        && session.is_authenticated()
        && let Some(conn_id) = session.connection_id()
    {
        session
            .ctx()
            .connections
            .insert(
                conn_id,
                ConnHandle {
                    sink: out_tx.clone(),
                    shutdown: Arc::clone(conn_shutdown),
                    wire: session.wire(),
                },
            )
            .await;
        // RED-011: run the `on_connect` hooks; their diff reaches
        // subscribers via the commit hook (P0-A 1.3), like any commit.
        if let Some((identity, cid)) = session.caller().map(|c| (c.identity, c.connection_id))
            && let Err(e) = session.engine().client_connected(identity, cid).await
        {
            tracing::warn!(target: "fluxum::server", error = %e, "on_connect hook failed");
        }
    }

    for response in routed.responses {
        let Ok(mut frame) = frame_message(codec, &response) else {
            continue;
        };
        // RPC-008: the AuthResult that accepts a gzip negotiation is the
        // last untagged frame — mark it raw, send it, THEN arm the writer's
        // transform so everything after it is tagged.
        let arming = send_wire.get().is_none()
            && matches!(response, ServerMessage::AuthResult(_))
            && session.wire().compression == fluxum_protocol::WireCompression::Gzip;
        frame.raw = arming;
        if out_tx.send(frame).await.is_err() {
            return false;
        }
        if arming {
            let _ = send_wire.set(session.ctx().wire_policy().compression_threshold_bytes);
        }
    }
    true
}

use crate::conn::{WRITE_COALESCE_BYTES, WRITE_COALESCE_FRAMES};

/// The writer task: drain the outbound queue to the socket, coalescing
/// whatever is already queued into one buffered `write_all` per batch
/// (F-001 — one syscall per BATCH, not per frame). Once `send_wire` arms
/// (RPC-008: gzip accepted), every non-`raw` frame is re-framed through
/// this connection's stream context before joining the batch — the shared
/// `Arc`'d bytes stay shared; the transform is strictly per-connection
/// (SUB-024). QueueWait stays per frame (its meaning is the frame's queue
/// time); Flush stays "time inside `write_all`", now per batch.
async fn writer_task(
    mut write_half: WriteHalf<MaybeTls>,
    mut out_rx: mpsc::Receiver<OutFrame>,
    metrics: Arc<fluxum_core::metrics::Metrics>,
    send_wire: Arc<std::sync::OnceLock<usize>>,
) {
    let us = |d: std::time::Duration| u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
    let mut compressor: Option<fluxum_protocol::StreamCompressor> = None;
    let mut frames: Vec<OutFrame> = Vec::with_capacity(WRITE_COALESCE_FRAMES);
    let mut batch: Vec<u8> = Vec::with_capacity(16 * 1024);
    'conn: loop {
        if out_rx.recv_many(&mut frames, WRITE_COALESCE_FRAMES).await == 0 {
            break; // the sink closed
        }
        batch.clear();
        let mut batched = 0u64;
        for frame in frames.drain(..) {
            metrics.note_fanout_stage(
                fluxum_core::metrics::FanoutStage::QueueWait,
                us(frame.enqueued_at.elapsed()),
            );
            let transformed = match (frame.raw, send_wire.get()) {
                (false, Some(&threshold)) => {
                    let ctx = compressor.get_or_insert_with(fluxum_protocol::StreamCompressor::new);
                    match crate::conn::wire_transform(ctx, &frame.bytes, threshold, &metrics) {
                        Ok(bytes) => bytes,
                        // A broken deflate context cannot resynchronize —
                        // connection-fatal by construction.
                        Err(_) => break 'conn,
                    }
                }
                _ => None,
            };
            batch.extend_from_slice(transformed.as_deref().unwrap_or(&frame.bytes));
            batched += 1;
            // The byte cap bounds the assembly buffer, not correctness: an
            // oversized batch flushes early and the loop keeps filling.
            if batch.len() >= WRITE_COALESCE_BYTES {
                if !flush_batch(&mut write_half, &batch, batched, &metrics).await {
                    break 'conn;
                }
                batch.clear();
                batched = 0;
            }
        }
        if batched > 0 && !flush_batch(&mut write_half, &batch, batched, &metrics).await {
            break;
        }
    }
    let _ = write_half.shutdown().await;
}

/// One coalesced socket write + its OBS-023 Flush and OBS-024 records.
async fn flush_batch(
    write_half: &mut WriteHalf<MaybeTls>,
    batch: &[u8],
    frames: u64,
    metrics: &fluxum_core::metrics::Metrics,
) -> bool {
    let began = std::time::Instant::now();
    if write_half.write_all(batch).await.is_err() {
        return false;
    }
    metrics.note_fanout_stage(
        fluxum_core::metrics::FanoutStage::Flush,
        u64::try_from(began.elapsed().as_micros()).unwrap_or(u64::MAX),
    );
    metrics.note_writer_write(
        fluxum_core::metrics::WriterTransport::Tcp,
        frames,
        u64::try_from(batch.len()).unwrap_or(u64::MAX),
    );
    true
}

/// What one bounded read produced.
enum ReadOutcome {
    Data(usize),
    /// The RPC-060 idle timeout elapsed (send 408, close).
    Idle,
    /// The SEC-031 handshake deadline elapsed before auth (drop, count).
    HandshakeExpired,
    Err(io::Error),
}

/// Read bounded by both the per-read idle timeout (RPC-060) and, when set,
/// an absolute handshake deadline (SEC-031). Whichever fires first decides
/// the outcome; the handshake deadline is reported distinctly so the caller
/// can drop silently and meter it rather than send a 408.
async fn read_with_deadline(
    read_half: &mut ReadHalf<MaybeTls>,
    chunk: &mut [u8],
    idle: Option<Duration>,
    handshake_deadline: Option<tokio::time::Instant>,
) -> ReadOutcome {
    let idle_deadline = idle.map(|d| tokio::time::Instant::now() + d);
    // The earliest of the two deadlines governs this read; remember which so
    // an expiry maps to the right outcome.
    let (deadline, is_handshake) = match (idle_deadline, handshake_deadline) {
        (Some(i), Some(h)) if h <= i => (Some(h), true),
        (Some(i), _) => (Some(i), false),
        (None, Some(h)) => (Some(h), true),
        (None, None) => (None, false),
    };
    let read = read_half.read(chunk);
    match deadline {
        Some(deadline) => match tokio::time::timeout_at(deadline, read).await {
            Ok(Ok(n)) => ReadOutcome::Data(n),
            Ok(Err(e)) => ReadOutcome::Err(e),
            Err(_) if is_handshake => ReadOutcome::HandshakeExpired,
            Err(_) => ReadOutcome::Idle,
        },
        None => match read.await {
            Ok(n) => ReadOutcome::Data(n),
            Err(e) => ReadOutcome::Err(e),
        },
    }
}

/// Frame a server message envelope for the socket.
fn frame_message(codec: &FrameCodec, message: &ServerMessage) -> Result<OutFrame, ()> {
    let body = message.encode().map_err(|_| ())?;
    let framed = codec.encode(&body).map_err(|_| ())?;
    Ok(OutFrame::now(Arc::new(framed)))
}

/// A framed `Error` message.
fn error_frame(
    codec: &FrameCodec,
    id: Option<u32>,
    code: u16,
    message: impl Into<String>,
) -> OutFrame {
    let msg = ServerMessage::Error(ErrorMessage::from_catalog(id, code, message, Vec::new()));
    frame_message(codec, &msg).unwrap_or_else(|()| OutFrame::now(Arc::new(Vec::new())))
}
