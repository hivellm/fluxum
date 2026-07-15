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
    tokio::spawn(fanout_loop(Arc::clone(&ctx), shutdown.clone()));

    // Accept loop.
    let accept_ctx = Arc::clone(&ctx);
    let accept_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accept_shutdown.notified() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let conn_ctx = Arc::clone(&accept_ctx);
                            let conn_shutdown = accept_shutdown.clone();
                            tokio::spawn(async move {
                                if let Err(e) = drive_connection(conn_ctx, stream, options, conn_shutdown).await {
                                    tracing::debug!(target: "fluxum::tcp", error = %e, "connection ended");
                                }
                            });
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
async fn drive_connection(
    ctx: Arc<ShardContext>,
    stream: TcpStream,
    options: TcpOptions,
    server_shutdown: Arc<Notify>,
) -> io::Result<()> {
    let (mut read_half, write_half) = stream.into_split();
    let codec = FrameCodec::new(options.max_frame_bytes);

    // The per-connection outbound queue (SUB-042 send buffer) + its writer.
    let (out_tx, out_rx) = mpsc::channel::<OutFrame>(options.send_queue_depth);
    let conn_shutdown = Arc::new(Notify::new());
    let writer = tokio::spawn(writer_task(write_half, out_rx));

    let mut session = Session::new(Arc::clone(&ctx));
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut read_chunk = [0u8; 8192];

    let result = 'conn: loop {
        // Drain any whole frames already buffered before reading more.
        loop {
            match codec.decode(&buf) {
                Ok(Some((frame, consumed))) => {
                    let owned: Option<Vec<u8>> = match frame {
                        Frame::KeepAlive => None,
                        Frame::Body(body) => Some(body.to_vec()),
                    };
                    buf.drain(..consumed);
                    if let Some(body) = owned
                        && !route_frame(&ctx, &mut session, &codec, &body, &out_tx, &conn_shutdown)
                            .await
                    {
                        break 'conn Ok(());
                    }
                }
                Ok(None) => break,
                Err(too_large) => {
                    // RPC-061: reply 413 and close the connection.
                    let msg = error_frame(&codec, None, too_large.code(), too_large.to_string());
                    let _ = out_tx.send(msg).await;
                    break 'conn Ok(());
                }
            }
        }

        let read = tokio::select! {
            _ = server_shutdown.notified() => break Ok(()),
            _ = conn_shutdown.notified() => break Ok(()),
            read = read_with_idle(&mut read_half, &mut read_chunk, options.idle_timeout) => read,
        };
        match read {
            ReadOutcome::Data(0) => break Ok(()), // clean EOF
            ReadOutcome::Data(n) => buf.extend_from_slice(&read_chunk[..n]),
            ReadOutcome::Idle => {
                // RPC-060: send 408 then close.
                let msg = error_frame(&codec, None, codes::IDLE_TIMEOUT, "idle timeout");
                let _ = out_tx.send(msg).await;
                break Ok(());
            }
            ReadOutcome::Err(e) => break Err(e),
        }
    };

    // Cleanup: deregister and stop the writer.
    if let Some(conn_id) = session.connection_id() {
        ctx.connections.remove(conn_id).await;
        ctx.subscriptions.lock().await.disconnect(conn_id);
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
                codes::MALFORMED,
                format!("malformed message: {e}"),
            );
            return out_tx.send(frame).await.is_ok();
        }
    };

    let was_authed = session.is_authenticated();
    let routed = session.handle(message).await;

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

/// The shard-wide fan-out (SUB-021/024): evaluate each committed diff against
/// the subscription manager once and push the shared `TxUpdate` to every
/// subscriber's queue. A full queue trips the SUB-042 drop.
async fn fanout_loop(ctx: Arc<ShardContext>, server_shutdown: Arc<Notify>) {
    let mut commits = ctx.subscribe_commits();
    let codec = FrameCodec::default();
    loop {
        let diff = tokio::select! {
            _ = server_shutdown.notified() => break,
            recv = commits.recv() => match recv {
                Ok(diff) => diff,
                // Lagged: the fan-out fell behind; clients recover on
                // reconnect via the tx_id gap (SPEC-006 acceptance 14).
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
        };

        // Evaluate once, holding the mutex only across evaluation (SUB-041).
        let deltas = {
            let manager = ctx.subscriptions.lock().await;
            match manager.on_commit(&diff) {
                Ok(deltas) => deltas,
                Err(e) => {
                    tracing::error!(target: "fluxum::tcp", error = %e, "fan-out evaluation failed");
                    continue;
                }
            }
        };

        for delta in deltas {
            let tx_update =
                fluxum_core::subscription::SubscriptionManager::tx_update(&diff, &delta);
            let Ok(frame) = frame_message(&codec, &ServerMessage::TxUpdate(tx_update)) else {
                continue;
            };
            for (conn_id, handle) in ctx.connections.handles_for(&delta.subscribers).await {
                match handle.sink.try_send(Arc::clone(&frame)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // SUB-042 Full tier: never block the fan-out — drop
                        // the slow consumer.
                        tracing::warn!(target: "fluxum::tcp", connection = conn_id,
                            "subscriber dropped: send buffer full");
                        handle.shutdown.notify_waiters();
                        ctx.connections.remove(conn_id).await;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        ctx.connections.remove(conn_id).await;
                    }
                }
            }
        }
    }
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

/// What one idle-bounded read produced.
enum ReadOutcome {
    Data(usize),
    Idle,
    Err(io::Error),
}

async fn read_with_idle(
    read_half: &mut tokio::net::tcp::OwnedReadHalf,
    chunk: &mut [u8],
    idle: Option<Duration>,
) -> ReadOutcome {
    match idle {
        Some(timeout) => match tokio::time::timeout(timeout, read_half.read(chunk)).await {
            Ok(Ok(n)) => ReadOutcome::Data(n),
            Ok(Err(e)) => ReadOutcome::Err(e),
            Err(_) => ReadOutcome::Idle,
        },
        None => match read_half.read(chunk).await {
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
    let msg = ServerMessage::Error(ErrorMessage {
        id,
        code,
        message: message.into(),
    });
    frame_message(codec, &msg).unwrap_or_else(|()| Arc::new(Vec::new()))
}
