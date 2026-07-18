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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};

use fluxum_protocol::{ClientMessage, ErrorMessage, Frame, FrameCodec, ServerMessage, codes};

use crate::connguard::ConnPermit;
use crate::session::Session;
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
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(60)),
            max_frame_bytes: fluxum_protocol::DEFAULT_MAX_FRAME_BYTES,
            send_queue_depth: 1024,
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
    let listener = TcpListener::bind(addr).await?;
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
                        Ok((stream, peer)) => {
                            // SEC-030/031: gate the pre-auth surface per peer IP
                            // before a session exists. A refusal is counted and
                            // the socket dropped (closed) — the cheapest signal
                            // to a flooding/throttled client.
                            let ip = peer.ip();
                            match accept_ctx.conn_guard().try_accept(ip) {
                                Ok(permit) => {
                                    let conn_ctx = Arc::clone(&accept_ctx);
                                    let conn_shutdown = accept_shutdown.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = drive_connection(conn_ctx, stream, ip, permit, options, conn_shutdown).await {
                                            tracing::debug!(target: "fluxum::tcp", error = %e, "connection ended");
                                        }
                                    });
                                }
                                Err(reason) => {
                                    accept_ctx.metrics().note_conn_rejected(reason);
                                    tracing::debug!(target: "fluxum::tcp", %ip, reason = reason.as_str(),
                                        "refused a connection: pre-auth abuse limit");
                                    drop(stream);
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

/// Drive one connection: read → route → write, with the idle timeout and
/// frame-size limit, until EOF, a fatal frame error, or shutdown.
///
/// `permit` holds the peer's SEC-030 concurrent-connection slot for the
/// connection's whole life (released on drop). While the session is
/// unauthenticated the SEC-031 handshake budget applies: a stricter pre-auth
/// frame-size cap and an absolute time budget to reach a successful
/// `Authenticate`, both to blunt slowloris.
async fn drive_connection(
    ctx: Arc<ShardContext>,
    stream: TcpStream,
    ip: std::net::IpAddr,
    permit: ConnPermit,
    options: TcpOptions,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let _permit = permit;
    let (mut read_half, write_half) = stream.into_split();
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
    let writer = tokio::spawn(writer_task(write_half, out_rx));

    let mut session = Session::new(Arc::clone(&ctx));
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut read_chunk = [0u8; 8192];

    let result = 'conn: loop {
        let authed = session.is_authenticated();
        let active_codec = if authed { &codec } else { &handshake_codec };
        // Drain any whole frames already buffered before reading more.
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
                        )
                        .await
                    {
                        break 'conn Ok(());
                    }
                }
                Ok(None) => break,
                Err(too_large) => {
                    // A frame over the limit gets the RPC-061 413 + close on
                    // both sides of auth; pre-auth it is *also* a SEC-031
                    // handshake-budget abuse event, so it is counted.
                    if !authed {
                        ctx.metrics().note_conn_rejected(
                            fluxum_core::metrics::ConnRejectReason::HandshakeBudget,
                        );
                    }
                    let msg = error_frame(&codec, None, too_large.code(), too_large.to_string());
                    let _ = out_tx.send(msg).await;
                    break 'conn Ok(());
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
            _ = server_shutdown.notified() => break Ok(()),
            _ = conn_shutdown.notified() => break Ok(()),
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

    // Cleanup: deregister and stop the writer.
    if let Some(conn_id) = session.connection_id() {
        ctx.metrics().note_disconnect(); // OBS-040
        ctx.connections.remove(conn_id).await;
        ctx.subscriptions.lock().await.disconnect(conn_id);
        // RED-012: run the `on_disconnect` hooks and publish their diff to the
        // remaining subscribers (a presence cleanup must reach them).
        if let Some((identity, cid)) = session.caller().map(|c| (c.identity, c.connection_id)) {
            match ctx.engine.client_disconnected(identity, cid).await {
                Ok(Some(receipt)) => ctx.publish_commit(receipt.diff),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(target: "fluxum::server", error = %e, "on_disconnect hook failed");
                }
            }
        }
    }
    drop(out_tx);
    let _ = writer.await;
    result
}

/// Decode one message body, route it, and forward every response frame to
/// the writer. Registers the connection's fan-out handle the moment it
/// authenticates. Returns `false` to close the connection.
async fn route_frame(
    ctx: &Arc<ShardContext>,
    session: &mut Session,
    ip: std::net::IpAddr,
    codec: &FrameCodec,
    body: &[u8],
    out_tx: &mpsc::Sender<OutFrame>,
    conn_shutdown: &Arc<Notify>,
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

    let was_authed = session.is_authenticated();
    // SEC-031: track the outcome of a pre-auth `Authenticate` so the guard
    // can throttle a brute-force. A success clears the peer's failure streak;
    // a failure advances it toward the backoff threshold.
    let is_auth_attempt = !was_authed && matches!(message, ClientMessage::Authenticate(_));
    let routed = session.handle(message).await;
    if is_auth_attempt {
        if session.is_authenticated() {
            ctx.conn_guard().note_auth_success(ip);
        } else {
            ctx.conn_guard().note_auth_failure(ip);
        }
    }

    // On the auth transition, register this connection for the fan-out.
    if !was_authed
        && session.is_authenticated()
        && let Some(conn_id) = session.connection_id()
    {
        ctx.connections
            .insert(
                conn_id,
                ConnHandle {
                    sink: out_tx.clone(),
                    shutdown: Arc::clone(conn_shutdown),
                },
            )
            .await;
        // RED-011: run the `on_connect` hooks and publish their diff to the
        // shard fan-out (a presence insert must reach subscribers).
        if let Some((identity, cid)) = session.caller().map(|c| (c.identity, c.connection_id)) {
            match ctx.engine.client_connected(identity, cid).await {
                Ok(Some(receipt)) => ctx.publish_commit(receipt.diff),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(target: "fluxum::server", error = %e, "on_connect hook failed");
                }
            }
        }
    }

    for response in routed.responses {
        let Ok(frame) = frame_message(codec, &response) else {
            continue;
        };
        if out_tx.send(frame).await.is_err() {
            return false;
        }
    }
    // Publish a committed reducer diff to the shard fan-out (SUB-021).
    if let Some(diff) = routed.commit {
        ctx.publish_commit(diff);
    }
    true
}

/// The writer task: drain the outbound queue to the socket in order.
async fn writer_task(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut out_rx: mpsc::Receiver<OutFrame>,
) {
    while let Some(frame) = out_rx.recv().await {
        if write_half.write_all(&frame).await.is_err() {
            break;
        }
    }
    let _ = write_half.shutdown().await;
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
    read_half: &mut tokio::net::tcp::OwnedReadHalf,
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
    Ok(Arc::new(framed))
}

/// A framed `Error` message.
fn error_frame(
    codec: &FrameCodec,
    id: Option<u32>,
    code: u16,
    message: impl Into<String>,
) -> OutFrame {
    let msg = ServerMessage::Error(ErrorMessage::from_catalog(id, code, message, Vec::new()));
    frame_message(codec, &msg).unwrap_or_else(|()| Arc::new(Vec::new()))
}
