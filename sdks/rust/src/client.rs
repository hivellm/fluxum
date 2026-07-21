//! A blocking FluxRPC client (SPEC-006 §4/§5) over TCP or Streamable HTTP.
//!
//! This is the object an application holds: it authenticates, calls reducers,
//! registers subscriptions with typed row callbacks, and keeps a local
//! [`RowCache`] in step with the server. It is deliberately synchronous and
//! thread-based — no async runtime — because the Rust SDK's first consumers
//! are services and tools that want a plain blocking client, and because it
//! keeps the crate's dependency surface to the vendored wire layer alone.
//!
//! Two transports behind one URL scheme (`Connection::connect` picks by
//! prefix):
//!
//! - `fluxum://host:port` — raw TCP (:15801). One socket; a background thread
//!   owns the read half, decodes frames, routes id-correlated replies
//!   (RPC-002) to the waiting caller, and applies server-initiated
//!   `TxUpdate`s. The write half is shared behind a mutex.
//! - `http://host:port` — Streamable HTTP (:15800, RPC-004..007). Requests go
//!   as `POST /rpc` (the response body carries that request's replies); the
//!   background thread reads the `GET /rpc` chunked push stream. The
//!   `Fluxum-Session` token binds the two.
//!
//! # Automatic reconnect (SPEC-011 SDK-047)
//!
//! When the connection drops, the background thread becomes the reconnect
//! loop, with exponential backoff and jitter between attempts.
//!
//! Over TCP a reconnect is a NEW session whose query ids the server does not
//! recognise, so the sequence is fixed: connect, authenticate, resubscribe,
//! reconcile — in that order (reconciling before resubscribing would compare
//! the cache against an `InitialData` that does not yet cover the registered
//! queries, and dutifully delete every row it could not see). The reconcile
//! is the net-difference pass in [`RowCache::reconcile`].
//!
//! Over HTTP a dropped push stream is first treated as a BLIP (SPEC-021
//! CS-021): the session may have survived, so the client reattaches the GET
//! stream and sends `Resume` from each subscription's highest applied offset
//! ([`ResumeTracker`]) — the server replays only the missed deltas, or
//! answers a `cache_reset` snapshot (CS-022) if it compacted past us. Only
//! when the session is truly gone (404) does the client fall back to the
//! full TCP-style re-establishment.
//!
//! Either way, the ids handed out by [`Connection::subscribe`] are stable
//! application handles; the client re-points them at the server's fresh ids
//! internally.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cache::{RowCache, RowEvent, TableDiff, TableSchema, TableSnapshot};
use crate::http::{ChunkedStream, HttpEndpoint};
use crate::protocol::{
    ClientMessage, ErrorMessage, FluxValue, Frame, FrameCodec, InitialData, ReducerCall, Resume,
    ServerMessage, Subscribe, TableUpdate, TxUpdate, Unsubscribe,
};
use crate::resume::ResumeTracker;

/// A client error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The URL was not `fluxum://host:port` or `http://host:port`.
    #[error("invalid Fluxum URL: {0}")]
    Url(String),
    /// A socket or I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding a message to send failed.
    #[error("encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    /// Framing a message body failed (only if it exceeds the 16 MB cap).
    #[error("frame error: {0}")]
    Frame(#[from] crate::protocol::FrameError),
    /// The server answered a request with an `Error` frame (RPC-034).
    #[error("server error {code} {name}: {message}")]
    Server {
        /// Stable catalog code (SPEC-028).
        code: u16,
        /// Canonical catalog name.
        name: String,
        /// Human-readable message.
        message: String,
    },
    /// A reducer rejected the call (RPC-031).
    #[error("reducer error {code}: {message}")]
    Reducer {
        /// Stable catalog code (5xxx).
        code: u16,
        /// Application-defined code, when the reducer attached one.
        app_code: Option<String>,
        /// Human-readable message.
        message: String,
    },
    /// The HTTP transport got a status it has no better mapping for
    /// (RPC-004..007) — e.g. `415` from something that is not a Fluxum
    /// server, or `409` racing a still-registered push stream.
    #[error("unexpected HTTP status {0}")]
    Http(u16),
    /// The connection closed while a request was in flight.
    #[error("connection closed")]
    Disconnected,
}

impl From<ErrorMessage> for Error {
    fn from(e: ErrorMessage) -> Self {
        Error::Server {
            code: e.code,
            name: e.name,
            message: e.message,
        }
    }
}

/// How the client re-establishes a dropped session (SDK-047): exponential
/// backoff with jitter, on by default.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Reconnect at all. `false` restores the fail-fast client: a drop
    /// disconnects every in-flight and future call.
    pub enabled: bool,
    /// First delay.
    pub initial: Duration,
    /// Ceiling for the delay.
    pub max: Duration,
    /// Growth factor per attempt.
    pub factor: f64,
    /// Random fraction of the delay added or removed. Without jitter, every
    /// client knocked off by the same server restart comes back on the same
    /// schedule and re-creates the load that took it down.
    pub jitter: f64,
    /// Give up after this many consecutive failures. `None` retries forever.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        // The TS SDK's defaults, so the two clients ride out the same outage
        // on the same schedule.
        Self {
            enabled: true,
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            factor: 2.0,
            jitter: 0.2,
            max_attempts: None,
        }
    }
}

/// Delay before attempt `n` (0-based), exponential with jitter and a ceiling.
fn backoff_delay(attempt: u32, policy: &ReconnectPolicy) -> Duration {
    #[allow(clippy::cast_precision_loss)]
    let raw = (policy.initial.as_millis() as f64 * policy.factor.powi(attempt.cast_signed()))
        .min(policy.max.as_millis() as f64);
    let with_jitter = if policy.jitter <= 0.0 {
        raw
    } else {
        (raw + jitter_unit() * raw * policy.jitter).max(0.0)
    };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Duration::from_millis(with_jitter as u64)
}

/// A cheap jitter source in `[-1, 1]` — the system clock's sub-second nanos.
/// Backoff jitter needs decorrelation, not cryptographic quality, and this
/// keeps the crate free of a rand dependency.
fn jitter_unit() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    (f64::from(nanos % 2048) / 1024.0) - 1.0
}

/// A row-event listener: `(row, old)` — `old` is `Some` only for updates.
pub type RowListener = Box<dyn Fn(&[u8], Option<&[u8]>) + Send + Sync>;

/// One reply routed by the reader to a waiting request: a server message, or
/// the error frame that ended the request.
type Routed = Result<ServerMessage, ErrorMessage>;

/// One live subscription: the SQL to replay on reconnect, the stable id the
/// application holds, and the id the CURRENT session's server assigned.
struct SubEntry {
    sql: String,
    app_id: u32,
    server_id: u32,
}

/// Where the parsed URL points.
enum Target {
    Tcp(String),
    Http(String),
}

/// The write half of the current session.
enum WriteHalf {
    /// The TCP socket's write side.
    Tcp(TcpStream),
    /// Streamable HTTP: each send is a `POST /rpc` bound by this session.
    Http {
        /// The `Fluxum-Session` token (RPC-007).
        session: String,
    },
}

/// The read half of the current session, owned by the background thread.
enum ReadHalf {
    Tcp(MessageStream),
    Http(ChunkedStream),
}

impl ReadHalf {
    fn next(&mut self) -> Option<ServerMessage> {
        match self {
            ReadHalf::Tcp(stream) => stream.next(),
            ReadHalf::Http(stream) => stream.next(),
        }
    }

    fn is_http(&self) -> bool {
        matches!(self, ReadHalf::Http(_))
    }
}

struct Shared {
    /// `host:port` of the TCP endpoint, kept for reconnecting. Unused (empty)
    /// on the HTTP transport.
    addr: String,
    /// The `/rpc` endpoint when the transport is Streamable HTTP.
    http: Option<HttpEndpoint>,
    /// The auth token, replayed on every re-authentication (SPEC-009).
    token: Vec<u8>,
    policy: ReconnectPolicy,
    /// Request id → its reply channel (RPC-002 correlation).
    pending: Mutex<HashMap<u32, Sender<Routed>>>,
    /// The row cache plus its per-query bookkeeping, behind one lock.
    cache: Mutex<RowCache>,
    /// `"<Table>:<insert|delete|update>"` → listeners.
    listeners: Mutex<HashMap<String, Vec<RowListener>>>,
    /// The highest applied `tx_offset` per subscription (SPEC-021 CS-020),
    /// fed by every `InitialData`/`TxUpdate` this connection applies. It
    /// drives the HTTP blip `Resume` (CS-021) and is rebuilt on a full
    /// re-establishment: a new session's offsets restart with its snapshot.
    resume: Mutex<ResumeTracker>,
    /// Live subscriptions, in registration order — the reconnect replay set.
    subs: Mutex<Vec<SubEntry>>,
    /// The 32-byte identity the server derived for this session (SPEC-009).
    identity: Mutex<[u8; 32]>,
    /// The write half of the current session. `None` while disconnected, so
    /// sends fail fast instead of writing into a dead session.
    writer: Mutex<Option<WriteHalf>>,
    /// The socket the background thread is currently reading (the TCP socket,
    /// or the HTTP push stream). `Drop` shuts it down to unblock the reader.
    push_socket: Mutex<Option<TcpStream>>,
    /// Monotonic request-id allocator, shared with the reconnect handshake.
    next_id: AtomicU32,
    /// Set by `Drop`; the reconnect loop checks it and stops.
    closed: Mutex<bool>,
    /// Wakes a backoff sleep so `Drop` never waits out a 30 s delay.
    wake: Condvar,
}

impl Shared {
    fn is_closed(&self) -> bool {
        *self
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn alloc_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn set_push_socket(&self, socket: Option<TcpStream>) {
        *self
            .push_socket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = socket;
    }

    fn set_writer(&self, half: Option<WriteHalf>) {
        *self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = half;
    }

    fn authenticate_message(&self) -> (u32, ClientMessage) {
        let id = self.alloc_id();
        let auth = crate::protocol::Authenticate {
            id,
            token: self.token.clone(),
            compression: None,
            tx_updates: None,
            namespace: None,
        };
        (id, ClientMessage::Authenticate(auth))
    }

    /// The active SQL replay set, in registration order.
    fn replay_sqls(&self) -> Vec<String> {
        self.subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|e| e.sql.clone())
            .collect()
    }
}

/// A connected Fluxum client.
pub struct Connection {
    shared: Arc<Shared>,
    reader: Option<JoinHandle<()>>,
}

impl Connection {
    /// Connect, authenticate, and return a live client with the default
    /// [`ReconnectPolicy`].
    ///
    /// `url` picks the transport: `fluxum://host:port` for raw TCP,
    /// `http://host:port` for Streamable HTTP. `token` is the auth token
    /// (empty under the dev `none` provider); `schemas` are the per-table
    /// primary-key projections the cache needs (SDK-040).
    pub fn connect(
        url: &str,
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
    ) -> Result<Self, Error> {
        Self::connect_with(url, token, schemas, ReconnectPolicy::default())
    }

    /// [`Connection::connect`] with an explicit reconnect policy.
    pub fn connect_with(
        url: &str,
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
        policy: ReconnectPolicy,
    ) -> Result<Self, Error> {
        let target = parse_url(url)?;
        let (addr, http) = match &target {
            Target::Tcp(addr) => (addr.clone(), None),
            Target::Http(addr) => (String::new(), Some(HttpEndpoint { addr: addr.clone() })),
        };

        let shared = Arc::new(Shared {
            addr,
            http,
            token: token.to_vec(),
            policy,
            pending: Mutex::new(HashMap::new()),
            cache: Mutex::new(RowCache::new(schemas)),
            listeners: Mutex::new(HashMap::new()),
            resume: Mutex::new(ResumeTracker::new()),
            subs: Mutex::new(Vec::new()),
            identity: Mutex::new([0u8; 32]),
            writer: Mutex::new(None),
            push_socket: Mutex::new(None),
            next_id: AtomicU32::new(1),
            closed: Mutex::new(false),
            wake: Condvar::new(),
        });

        let read_half = match target {
            Target::Tcp(_) => {
                let stream = TcpStream::connect(&shared.addr)?;
                // Reducer calls are small request/response frames; Nagle
                // would hold each one behind the previous frame's ACK.
                let _ = stream.set_nodelay(true);
                shared.set_push_socket(Some(stream.try_clone()?));
                shared.set_writer(Some(WriteHalf::Tcp(stream.try_clone()?)));
                // Authenticate before returning: connecting means "session
                // ready", not "socket open" (RPC-020). The reader thread does
                // not exist yet, so the handshake reads the stream inline.
                let mut messages = MessageStream::new(stream);
                let identity = tcp_authenticate(&shared, &mut messages)?;
                *shared
                    .identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = identity;
                ReadHalf::Tcp(messages)
            }
            // The first HTTP session IS a full establishment with an empty
            // replay set: authenticate, (no queries yet), open the stream.
            Target::Http(_) => try_http_session(&shared)?,
        };

        let reader = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || supervise(read_half, &shared))
        };

        Ok(Connection {
            shared,
            reader: Some(reader),
        })
    }

    /// The 32-byte identity the server derived for this session.
    pub fn identity(&self) -> [u8; 32] {
        *self
            .shared
            .identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register a listener for `"<Table>:<insert|delete|update>"`.
    pub fn on(&self, event: impl Into<String>, listener: RowListener) {
        self.shared
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(event.into())
            .or_default()
            .push(listener);
    }

    /// Snapshot the rows currently cached for `table`, in insertion order.
    pub fn rows(&self, table: &str) -> Vec<Vec<u8>> {
        self.shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rows(table)
    }

    /// Total cached rows across every table.
    pub fn cache_size(&self) -> usize {
        self.shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .size()
    }

    /// Register subscription queries, await every `InitialData`, and return a
    /// stable handle for each (SUB-001) — what [`Connection::unsubscribe`]
    /// and [`Connection::applied_offset`] take. Handles come back in request
    /// order and survive reconnects: the client re-points them at the fresh
    /// server-assigned ids when it resubscribes (SDK-047).
    pub fn subscribe(&self, queries: &[&str]) -> Result<Vec<u32>, Error> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let id = self.shared.alloc_id();
        let sub = Subscribe {
            id,
            queries: queries.iter().map(|q| (*q).to_owned()).collect(),
        };
        let replies = self.request(ClientMessage::Subscribe(sub), id, queries.len())?;

        let mut ids = Vec::new();
        let mut events = Vec::new();
        for reply in replies {
            if let ServerMessage::InitialData(initial) = reply {
                events.extend(apply_initial(&self.shared, &initial));
                for table in &initial.tables {
                    ids.push(table.query_id);
                }
            }
        }
        // Record the SQL for reconnect replay (SDK-047). In the first session
        // the handle IS the server id; a reconnect re-points it.
        {
            let mut subs = self
                .shared
                .subs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (sql, qid) in queries.iter().zip(&ids) {
                subs.push(SubEntry {
                    sql: (*sql).to_owned(),
                    app_id: *qid,
                    server_id: *qid,
                });
            }
        }
        self.dispatch(events);
        Ok(ids)
    }

    /// The highest `tx_offset` this client has applied for the subscription
    /// handle (SPEC-021 CS-020), or `None` if nothing has been applied yet.
    /// How current the subscription is.
    pub fn applied_offset(&self, query_id: u32) -> Option<u64> {
        let server_id = self.server_id(query_id);
        self.shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied_offset(server_id)
    }

    /// Drop subscriptions by the handles [`Connection::subscribe`] returned
    /// (SUB-004). Rows those queries held leave the cache unless another live
    /// subscription still covers them (SDK-044).
    pub fn unsubscribe(&self, query_ids: &[u32]) -> Result<(), Error> {
        if query_ids.is_empty() {
            return Ok(());
        }
        // Resolve handles to the CURRENT session's server ids and drop them
        // from the reconnect replay set.
        let server_ids: Vec<u32> = {
            let mut subs = self
                .shared
                .subs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            query_ids
                .iter()
                .map(|app_id| {
                    match subs.iter().position(|e| e.app_id == *app_id) {
                        Some(pos) => subs.remove(pos).server_id,
                        // Unknown handle — pass it through untranslated, the
                        // pre-reconnect behaviour for raw ids.
                        None => *app_id,
                    }
                })
                .collect()
        };
        // Fire-and-forget: the server sends NO reply to Unsubscribe — delivery
        // simply stops (RPC-024). The message still carries an id for framing
        // symmetry.
        let id = self.shared.alloc_id();
        self.send(ClientMessage::Unsubscribe(Unsubscribe {
            id,
            query_ids: server_ids.clone(),
        }))?;
        let mut events = Vec::new();
        {
            let mut cache = self
                .shared
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for &server_id in &server_ids {
                events.extend(cache.release_query(server_id));
            }
        }
        {
            let mut resume = self
                .shared
                .resume
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for &server_id in &server_ids {
                resume.forget(server_id);
            }
        }
        self.dispatch(events);
        Ok(())
    }

    /// Call a reducer and await its outcome. Resolves when the reducer
    /// committed — the resulting `TxUpdate` may arrive before or after.
    pub fn call_reducer(&self, name: &str, args: Vec<FluxValue>) -> Result<(), Error> {
        let id = self.shared.alloc_id();
        let call = ReducerCall {
            id,
            reducer: name.to_owned(),
            version: None,
            args,
            idempotency_key: None,
        };
        let reply = self.request(ClientMessage::ReducerCall(call), id, 1)?;
        match reply.into_iter().next() {
            Some(ServerMessage::ReducerResult(result)) => match result.outcome {
                Ok(()) => Ok(()),
                Err(e) => Err(Error::Reducer {
                    code: e.code,
                    app_code: e.app_code,
                    message: e.message,
                }),
            },
            _ => Ok(()),
        }
    }

    /// Test hook: kill the socket the background thread is reading, as an
    /// outage would, WITHOUT closing the client — the reconnect machinery
    /// must bring the session back. Hidden because applications have no
    /// business calling it.
    #[doc(hidden)]
    pub fn simulate_stream_loss(&self) {
        if let Some(socket) = self
            .shared
            .push_socket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    }

    // --- Internals -----------------------------------------------------------

    /// The current session's server id behind an application handle.
    fn server_id(&self, app_id: u32) -> u32 {
        self.shared
            .subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|e| e.app_id == app_id)
            .map_or(app_id, |e| e.server_id)
    }

    /// Send one message, register its id, and collect `expected` replies. An
    /// `Error` frame for the id ends the wait early with that error.
    fn request(
        &self,
        message: ClientMessage,
        id: u32,
        expected: usize,
    ) -> Result<Vec<ServerMessage>, Error> {
        let (tx, rx): (Sender<Routed>, Receiver<Routed>) = mpsc::channel();
        self.shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, tx);
        // If sending fails, drop the pending entry so a later disconnect does
        // not try to route to a request that never went out.
        if let Err(e) = self.send(message) {
            self.shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            return Err(e);
        }

        let mut collected = Vec::with_capacity(expected);
        let result = loop {
            match rx.recv() {
                Ok(Ok(msg)) => {
                    collected.push(msg);
                    if collected.len() == expected {
                        break Ok(collected);
                    }
                }
                Ok(Err(err)) => break Err(Error::from(err)),
                Err(_) => break Err(Error::Disconnected),
            }
        };
        self.shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        result
    }

    fn send(&self, message: ClientMessage) -> Result<(), Error> {
        send_message(&self.shared, &message)
    }

    fn dispatch(&self, events: Vec<RowEvent>) {
        dispatch_shared(&self.shared, events);
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        *self
            .shared
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        // Wake a reconnect loop out of its backoff sleep so it can stop.
        self.shared.wake.notify_all();
        // Closing the read-side socket unblocks the background thread.
        self.simulate_stream_loss();
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
    }
}

/// Send one message over whichever write half is live. On HTTP the POST
/// response carries this request's replies — they are routed exactly as the
/// push stream's frames are, into the pending map the caller is waiting on.
fn send_message(shared: &Shared, message: &ClientMessage) -> Result<(), Error> {
    let framed = encode_framed(message)?;
    let session = {
        let mut guard = shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.as_mut() {
            // `None` means the connection dropped and the reconnect loop has
            // not re-established it yet: fail fast, not into the void.
            None => return Err(Error::Disconnected),
            Some(WriteHalf::Tcp(stream)) => {
                stream.write_all(&framed)?;
                stream.flush()?;
                return Ok(());
            }
            Some(WriteHalf::Http { session }) => session.clone(),
        }
        // The lock is released here: an HTTP round-trip must not serialize
        // every other sender behind it.
    };
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;
    let response = endpoint.post(Some(&session), &framed).map_err(Error::Io)?;
    match response.status {
        200 => {
            for message in response.messages {
                route(shared, message);
            }
            Ok(())
        }
        // RPC-007: an unknown/expired session is a 404; the push-stream loop
        // notices the same death and re-establishes.
        404 => Err(Error::Disconnected),
        status => Err(Error::Http(status)),
    }
}

/// Encode a client message into one length-prefixed frame.
fn encode_framed(message: &ClientMessage) -> Result<Vec<u8>, Error> {
    let body = message.encode()?;
    let mut framed = Vec::with_capacity(body.len() + 4);
    // A message body is far under the 16 MB frame cap; a `TooLarge` here
    // would be a client-side bug, surfaced rather than unwrapped.
    FrameCodec::default().encode_into(&body, &mut framed)?;
    Ok(framed)
}

/// Group a `TableUpdate` list into per-`query_id` cache diffs (SUB-001).
fn group_by_query(tables: &[TableUpdate]) -> Vec<(u32, Vec<TableDiff>)> {
    let mut by_query: Vec<(u32, Vec<TableDiff>)> = Vec::new();
    for table in tables {
        let diff = TableDiff {
            table: table.table_name.clone(),
            inserts: table.inserts.iter().map(<[u8]>::to_vec).collect(),
            deletes: table.deletes.iter().map(<[u8]>::to_vec).collect(),
        };
        match by_query.iter_mut().find(|(id, _)| *id == table.query_id) {
            Some((_, diffs)) => diffs.push(diff),
            None => by_query.push((table.query_id, vec![diff])),
        }
    }
    by_query
}

/// Apply an `InitialData` snapshot to the cache, feeding the resume tracker
/// (SPEC-021 CS-020) and honouring a `cache_reset` (CS-022): when the server
/// answered a `Resume` whose offset predated its retained window, the snapshot
/// REPLACES the query's rows rather than merging, so the query's cached rows
/// are cleared before it is applied.
fn apply_initial(shared: &Shared, initial: &InitialData) -> Vec<RowEvent> {
    let reset = shared
        .resume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_initial(initial);

    let mut cache = shared
        .cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut events = Vec::new();
    for (query_id, diffs) in group_by_query(&initial.tables) {
        if reset {
            // CS-022: drop this query's prior rows before the fresh snapshot.
            events.extend(cache.release_query(query_id));
        }
        events.extend(cache.apply_query_diff(query_id, &diffs));
    }
    events
}

/// Apply a server-initiated `TxUpdate` to the cache, attributing rows by their
/// stamped `query_id` (SDK-044) and advancing the resume offsets (CS-020).
fn apply_tx_update(shared: &Shared, update: &TxUpdate) -> Vec<RowEvent> {
    shared
        .resume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_update(update);

    let mut cache = shared
        .cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut events = Vec::new();
    for (query_id, diffs) in group_by_query(&update.tables) {
        events.extend(cache.apply_query_diff(query_id, &diffs));
    }
    events
}

/// Dispatch events to listeners without a `Connection` handle (reader thread).
fn dispatch_shared(shared: &Shared, events: Vec<RowEvent>) {
    if events.is_empty() {
        return;
    }
    let listeners = shared
        .listeners
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for event in events {
        let (table, kind): (&str, &str) = match &event {
            RowEvent::Insert { table, .. } => (table, "insert"),
            RowEvent::Delete { table, .. } => (table, "delete"),
            RowEvent::Update { table, .. } => (table, "update"),
        };
        if let Some(set) = listeners.get(&format!("{table}:{kind}")) {
            for listener in set {
                match &event {
                    RowEvent::Insert { row, .. } | RowEvent::Delete { row, .. } => {
                        listener(row, None)
                    }
                    RowEvent::Update { old, row, .. } => listener(row, Some(old)),
                }
            }
        }
    }
}

// --- The session stream ------------------------------------------------------

/// A blocking, buffered decoder of server messages off one TCP socket. Owned
/// by whichever code is currently reading — the handshake reads it inline,
/// then hands it (buffer and all) to the read loop, so no bytes are lost at
/// the transition.
struct MessageStream {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl MessageStream {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            codec: FrameCodec::default(),
            buf: Vec::new(),
        }
    }

    /// The next decodable server message; `None` on EOF, socket error, or a
    /// framing violation (which desynchronizes the stream — stop reading).
    fn next(&mut self) -> Option<ServerMessage> {
        let mut chunk = [0u8; 8192];
        loop {
            // Drain every complete frame currently buffered before reading.
            loop {
                let (frame_body, consumed) = match self.codec.decode(&self.buf) {
                    Ok(Some((Frame::Body(body), consumed))) => (Some(body.to_vec()), consumed),
                    Ok(Some((Frame::KeepAlive, consumed))) => (None, consumed),
                    Ok(None) => break,
                    Err(_) => return None,
                };
                self.buf.drain(..consumed);
                if let Some(body) = frame_body
                    && let Ok(message) = ServerMessage::decode(&body)
                {
                    return Some(message);
                }
            }
            match self.stream.read(&mut chunk) {
                Ok(0) => return None, // clean EOF
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }
}

/// The background thread: read the session until it drops, then — policy
/// permitting — bring it back and carry on, forever, until the `Connection`
/// is dropped.
fn supervise(mut messages: ReadHalf, shared: &Arc<Shared>) {
    loop {
        while let Some(message) = messages.next() {
            route(shared, message);
        }
        let was_http = messages.is_http();

        // Over TCP the session died with the socket: fail senders fast and
        // unblock in-flight callers. Over HTTP only the PUSH STREAM died —
        // the session may be fine and POSTs keep working, so nothing is
        // failed unless recovery below gives up.
        if !was_http {
            shared.set_writer(None);
            fail_all(shared);
        }

        if shared.is_closed() || !shared.policy.enabled {
            shared.set_writer(None);
            fail_all(shared);
            return;
        }
        let next = if was_http {
            recover_http(shared)
        } else {
            reestablish_tcp(shared)
        };
        match next {
            Some(live) => messages = live,
            None => {
                shared.set_writer(None);
                fail_all(shared);
                return;
            }
        }
    }
}

/// The TCP reconnect loop: connect, authenticate, resubscribe, reconcile —
/// with exponential backoff between attempts (SDK-047). `None` when the
/// client was closed or the policy's attempt budget ran out.
fn reestablish_tcp(shared: &Arc<Shared>) -> Option<ReadHalf> {
    let mut attempt: u32 = 0;
    loop {
        if let Some(max) = shared.policy.max_attempts
            && attempt >= max
        {
            return None;
        }
        let delay = if attempt == 0 {
            Duration::ZERO
        } else {
            backoff_delay(attempt - 1, &shared.policy)
        };
        if !sleep_unless_closed(shared, delay) {
            return None;
        }
        match try_tcp_session(shared) {
            Ok(messages) => return Some(messages),
            Err(_) => attempt += 1,
        }
    }
}

/// The HTTP push-stream recovery loop. Each attempt first tries the BLIP
/// path — reattach the GET stream under the surviving session and `Resume`
/// each subscription from its applied offset (SPEC-021 CS-021) — and falls
/// back to a full re-establishment (new session, resubscribe, reconcile)
/// when the session is gone.
fn recover_http(shared: &Arc<Shared>) -> Option<ReadHalf> {
    let mut attempt: u32 = 0;
    loop {
        if let Some(max) = shared.policy.max_attempts
            && attempt >= max
        {
            return None;
        }
        let delay = if attempt == 0 {
            Duration::ZERO
        } else {
            backoff_delay(attempt - 1, &shared.policy)
        };
        if !sleep_unless_closed(shared, delay) {
            return None;
        }

        let endpoint = shared.http.as_ref()?;
        let session = {
            let guard = shared
                .writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match guard.as_ref() {
                Some(WriteHalf::Http { session }) => Some(session.clone()),
                _ => None,
            }
        };

        if let Some(session) = session {
            match endpoint.open_stream(&session) {
                Ok((200, Some(stream))) => {
                    shared.set_push_socket(stream.socket().ok());
                    if resume_subscriptions(shared, &session).is_ok() {
                        return Some(ReadHalf::Http(stream));
                    }
                    // The session survived but a subscription did not
                    // (SUB unknown query) — rebuild from scratch below.
                }
                // The server still counts the previous stream (409) or is not
                // reachable: back off and retry the blip before giving the
                // session up for dead.
                Ok((409, _)) | Err(_) => {
                    attempt += 1;
                    continue;
                }
                // 404: the session is gone — full re-establishment.
                Ok((_, _)) => {}
            }
        }

        match try_http_session(shared) {
            Ok(messages) => return Some(messages),
            Err(_) => attempt += 1,
        }
    }
}

/// Sleep for `delay`, waking early if the connection is closed. Returns
/// whether the caller should proceed (false = closed).
fn sleep_unless_closed(shared: &Shared, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    let mut closed = shared
        .closed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !*closed {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        closed = shared
            .wake
            .wait_timeout(closed, deadline - now)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
    }
    false
}

/// One TCP reconnect attempt: a full session bring-up. Any failure aborts the
/// attempt; the loop backs off and tries again.
fn try_tcp_session(shared: &Arc<Shared>) -> Result<ReadHalf, Error> {
    let stream = TcpStream::connect(&shared.addr)?;
    let _ = stream.set_nodelay(true);
    // A half-dead handshake must not wedge `Drop`: bound reads until the
    // session is live, then go back to blocking indefinitely.
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut writer = stream.try_clone()?;
    let mut messages = MessageStream::new(stream);

    // 1. Authenticate (the shared writer is still None — send directly).
    let (auth_id, auth) = shared.authenticate_message();
    writer.write_all(&encode_framed(&auth)?)?;
    writer.flush()?;
    let identity = loop {
        match messages.next() {
            None => return Err(Error::Disconnected),
            Some(ServerMessage::AuthResult(result)) if result.id == auth_id => {
                break result.identity;
            }
            Some(ServerMessage::Error(err)) if err.id == Some(auth_id) => {
                return Err(err.into());
            }
            Some(_) => {} // nothing else belongs to a session this young
        }
    };

    // 2. Resubscribe every active query, in registration order — always
    // BEFORE reconcile: InitialData must cover every active query, or
    // reconciliation reads the gap as rows having been deleted.
    let sqls = shared.replay_sqls();
    let mut initials: Vec<InitialData> = Vec::new();
    if !sqls.is_empty() {
        let sub_id = shared.alloc_id();
        let subscribe = Subscribe {
            id: sub_id,
            queries: sqls.clone(),
        };
        writer.write_all(&encode_framed(&ClientMessage::Subscribe(subscribe))?)?;
        writer.flush()?;
        while initials.len() < sqls.len() {
            match messages.next() {
                None => return Err(Error::Disconnected),
                Some(ServerMessage::InitialData(initial)) if initial.id == sub_id => {
                    initials.push(initial);
                }
                Some(ServerMessage::Error(err)) if err.id == Some(sub_id) => {
                    return Err(err.into());
                }
                Some(_) => {}
            }
        }
    }

    // 3. Reconcile under the new session's ids.
    let events = adopt_session(shared, &initials, sqls.len(), identity)?;

    // Session live: back to blocking reads, reopen the shared writer, and only
    // then tell the application what changed while it was away.
    messages.stream.set_read_timeout(None)?;
    shared.set_push_socket(messages.stream.try_clone().ok());
    shared.set_writer(Some(WriteHalf::Tcp(writer)));
    dispatch_shared(shared, events);
    Ok(ReadHalf::Tcp(messages))
}

/// One full HTTP session bring-up: authenticate a fresh session over POST,
/// resubscribe over POST, reconcile, then open the push stream. Also the
/// FIRST session's path, where the replay set is simply empty.
fn try_http_session(shared: &Arc<Shared>) -> Result<ReadHalf, Error> {
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;

    // 1. Authenticate: the response carries the AuthResult and mints the
    // session token (RPC-007).
    let (_, auth) = shared.authenticate_message();
    let response = endpoint.post(None, &encode_framed(&auth)?).map_err(Error::Io)?;
    if response.status != 200 {
        return Err(Error::Http(response.status));
    }
    let session = response.session.clone();
    let mut identity: Option<[u8; 32]> = None;
    for message in response.messages {
        match message {
            ServerMessage::AuthResult(result) => identity = Some(result.identity),
            ServerMessage::Error(err) => return Err(err.into()),
            _ => {}
        }
    }
    let identity = identity.ok_or(Error::Disconnected)?;
    let session = session.ok_or(Error::Disconnected)?;

    // 2. Resubscribe the replay set in one POST; its response body carries
    // every InitialData.
    let sqls = shared.replay_sqls();
    let mut initials: Vec<InitialData> = Vec::new();
    if !sqls.is_empty() {
        let sub_id = shared.alloc_id();
        let subscribe = ClientMessage::Subscribe(Subscribe {
            id: sub_id,
            queries: sqls.clone(),
        });
        let response = endpoint
            .post(Some(&session), &encode_framed(&subscribe)?)
            .map_err(Error::Io)?;
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        for message in response.messages {
            match message {
                ServerMessage::InitialData(initial) if initial.id == sub_id => {
                    initials.push(initial);
                }
                ServerMessage::Error(err) if err.id == Some(sub_id) => {
                    return Err(err.into());
                }
                _ => {}
            }
        }
    }

    // 3. Reconcile under the new session's ids.
    let events = adopt_session(shared, &initials, sqls.len(), identity)?;

    // 4. Open the push stream; anything committed between the subscribe POST
    // and here sits in the session's outbound queue and arrives on attach.
    let (status, stream) = endpoint.open_stream(&session).map_err(Error::Io)?;
    let Some(stream) = stream else {
        return Err(Error::Http(status));
    };
    shared.set_push_socket(stream.socket().ok());
    shared.set_writer(Some(WriteHalf::Http { session }));
    dispatch_shared(shared, events);
    Ok(ReadHalf::Http(stream))
}

/// The HTTP blip path (SPEC-021 CS-021): the session survived a dropped push
/// stream, so ask the server to replay what each subscription missed, from
/// its highest APPLIED offset. Deltas come back as `TxUpdate`s and apply on
/// the normal path; a compacted-away offset comes back as a `cache_reset`
/// snapshot (CS-022) which `apply_initial` already honours. Any error —
/// typically SUB unknown query, a session that did not really survive —
/// tells the caller to rebuild from scratch.
fn resume_subscriptions(shared: &Arc<Shared>, session: &str) -> Result<(), Error> {
    let endpoint = shared.http.as_ref().ok_or(Error::Disconnected)?;
    let targets: Vec<(u32, Option<u64>)> = {
        let subs = shared
            .subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let resume = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        subs.iter()
            .map(|e| (e.server_id, resume.applied_offset(e.server_id)))
            .collect()
    };
    for (server_id, offset) in targets {
        // Nothing applied yet — nothing to resume; the stream reattach alone
        // covers it.
        let Some(from_offset) = offset else { continue };
        let id = shared.alloc_id();
        let resume = ClientMessage::Resume(Resume {
            id,
            query_id: server_id,
            from_offset,
        });
        let response = endpoint
            .post(Some(session), &encode_framed(&resume)?)
            .map_err(Error::Io)?;
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        for message in response.messages {
            match message {
                ServerMessage::InitialData(initial) => {
                    let events = apply_initial(shared, &initial);
                    dispatch_shared(shared, events);
                }
                ServerMessage::Error(_) => return Err(Error::Disconnected),
                other => route(shared, other),
            }
        }
    }
    Ok(())
}

/// Adopt a fresh session's `InitialData` set: rebuild the resume tracker,
/// reconcile the cache to the net difference (SDK-047), re-attribute rows to
/// the NEW query ids, re-point the application handles, and store the
/// re-derived identity. Returns the events to dispatch once the writer is
/// live.
fn adopt_session(
    shared: &Shared,
    initials: &[InitialData],
    expected_queries: usize,
    identity: [u8; 32],
) -> Result<Vec<RowEvent>, Error> {
    // The fresh server-assigned ids, in reply order — one per query, matching
    // the registry's order because the Subscribe listed them in that order.
    let mut new_ids: Vec<u32> = Vec::new();
    let mut per_query: Vec<(u32, Vec<TableSnapshot>)> = Vec::new();
    let mut merged: Vec<TableSnapshot> = Vec::new();
    for initial in initials {
        for table in &initial.tables {
            let rows: Vec<Vec<u8>> = table.inserts.iter().map(<[u8]>::to_vec).collect();
            let snapshot = TableSnapshot {
                table: table.table_name.clone(),
                rows: rows.clone(),
            };
            match per_query.iter_mut().find(|(id, _)| *id == table.query_id) {
                Some((_, snaps)) => snaps.push(snapshot),
                None => {
                    new_ids.push(table.query_id);
                    per_query.push((table.query_id, vec![snapshot]));
                }
            }
            match merged.iter_mut().find(|s| s.table == table.table_name) {
                Some(existing) => existing.rows.extend(rows),
                None => merged.push(TableSnapshot {
                    table: table.table_name.clone(),
                    rows,
                }),
            }
        }
    }
    if new_ids.len() != expected_queries {
        // The reply shape does not match the replay set; treat the attempt as
        // failed rather than mis-binding handles.
        return Err(Error::Disconnected);
    }

    {
        let mut resume = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *resume = ResumeTracker::new();
        for initial in initials {
            let _ = resume.apply_initial(initial);
        }
    }
    let events = {
        let mut cache = shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.reset_queries();
        let events = cache.reconcile(&merged);
        for (query_id, snapshots) in &per_query {
            cache.track_query(*query_id, snapshots);
        }
        events
    };
    {
        let mut subs = shared
            .subs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (entry, new_id) in subs.iter_mut().zip(&new_ids) {
            entry.server_id = *new_id;
        }
    }
    *shared
        .identity
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = identity;
    Ok(events)
}

fn route(shared: &Shared, message: ServerMessage) {
    match message {
        ServerMessage::TxUpdate(update) => {
            let events = apply_tx_update(shared, &update);
            dispatch_shared(shared, events);
        }
        ServerMessage::TxUpdateLight(_) => {}
        ServerMessage::Error(err) => {
            // A null-id error is server-initiated and belongs to nobody.
            if let Some(id) = err.id
                && let Some(tx) = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&id)
            {
                let _ = tx.send(Err(err));
            }
        }
        other => {
            if let Some(id) = reply_id(&other)
                && let Some(tx) = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&id)
            {
                let _ = tx.send(Ok(other));
            }
        }
    }
}

/// The echoed request id of a correlated server reply.
fn reply_id(message: &ServerMessage) -> Option<u32> {
    match message {
        ServerMessage::AuthResult(m) => Some(m.id),
        ServerMessage::ReducerResult(m) => Some(m.id),
        ServerMessage::InitialData(m) => Some(m.id),
        _ => None,
    }
}

/// Authenticate a brand-new first TCP session, reading the stream inline (the
/// reader thread does not exist yet).
fn tcp_authenticate(shared: &Shared, messages: &mut MessageStream) -> Result<[u8; 32], Error> {
    let (auth_id, auth) = shared.authenticate_message();
    send_message(shared, &auth)?;
    loop {
        match messages.next() {
            None => return Err(Error::Disconnected),
            Some(ServerMessage::AuthResult(result)) if result.id == auth_id => {
                return Ok(result.identity);
            }
            Some(ServerMessage::Error(err)) if err.id == Some(auth_id) => {
                return Err(err.into());
            }
            Some(_) => {} // nothing else belongs to a session this young
        }
    }
}

/// Fail every in-flight request when the connection drops, so no caller hangs.
///
/// Clearing the pending map drops each request's `Sender`; the waiting
/// `recv()` then returns `Err`, which [`Connection::request`] maps to
/// [`Error::Disconnected`]. No sentinel message is needed.
fn fail_all(shared: &Shared) {
    shared
        .pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

/// Parse a client URL: `fluxum://host:port` (TCP) or `http://host:port`
/// (Streamable HTTP), both with an explicit port.
fn parse_url(url: &str) -> Result<Target, Error> {
    let (rest, is_http) = if let Some(rest) = url.strip_prefix("fluxum://") {
        (rest, false)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (rest, true)
    } else {
        return Err(Error::Url(format!(
            "expected fluxum://host:port or http://host:port, got {url}"
        )));
    };
    let addr = rest.trim_end_matches('/');
    if !addr.contains(':') {
        return Err(Error::Url(format!("missing port in {url}")));
    }
    Ok(if is_http {
        Target::Http(addr.to_owned())
    } else {
        Target::Tcp(addr.to_owned())
    })
}
