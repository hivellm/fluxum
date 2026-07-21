//! A blocking FluxRPC client over TCP (SPEC-006 §4/§5).
//!
//! This is the object an application holds: it authenticates, calls reducers,
//! registers subscriptions with typed row callbacks, and keeps a local
//! [`RowCache`] in step with the server. It is deliberately synchronous and
//! thread-based — no async runtime — because the Rust SDK's first consumers
//! are services and tools that want a plain blocking client, and because it
//! keeps the crate's dependency surface to the vendored wire layer alone.
//!
//! One background thread owns the read half of the session: it decodes frames,
//! routes id-correlated replies (RPC-002) to the waiting caller, and applies
//! server-initiated `TxUpdate`s to the cache. The write half is shared behind
//! a mutex so any thread can send.
//!
//! # Automatic reconnect (SPEC-011 SDK-047)
//!
//! When the connection drops, the same background thread becomes the
//! reconnect loop: connect, authenticate, resubscribe, reconcile — in that
//! order, with exponential backoff and jitter between attempts. The order
//! matters: reconciling before resubscribing would compare the cache against
//! an `InitialData` that does not yet cover the queries the application
//! registered, and dutifully delete every row it could not see. A TCP
//! reconnect is a NEW session whose query ids the server does not recognise,
//! so the client resubscribes and reconciles (the net-difference pass in
//! [`RowCache::reconcile`]) rather than sending `Resume` — the
//! session-preserving resume (CS-021) belongs to the HTTP stream transport.
//! The ids handed out by [`Connection::subscribe`] are stable application
//! handles; the client re-points them at the server's fresh ids internally.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cache::{RowCache, RowEvent, TableDiff, TableSchema, TableSnapshot};
use crate::protocol::{
    ClientMessage, ErrorMessage, FluxValue, Frame, FrameCodec, InitialData, ReducerCall,
    ServerMessage, Subscribe, TableUpdate, TxUpdate, Unsubscribe,
};
use crate::resume::ResumeTracker;

/// A client error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The URL was not `fluxum://host:port`.
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

struct Shared {
    /// `host:port`, kept for reconnecting.
    addr: String,
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
    /// fed by every `InitialData`/`TxUpdate` this connection applies. Rebuilt
    /// on reconnect: a new session's offsets restart with its snapshot.
    resume: Mutex<ResumeTracker>,
    /// Live subscriptions, in registration order — the reconnect replay set.
    subs: Mutex<Vec<SubEntry>>,
    /// The 32-byte identity the server derived for this session (SPEC-009).
    identity: Mutex<[u8; 32]>,
    /// The write half of the current socket. `None` while disconnected, so
    /// sends fail fast instead of writing into a dead session.
    writer: Mutex<Option<TcpStream>>,
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
}

/// A connected Fluxum client.
pub struct Connection {
    shared: Arc<Shared>,
    reader: Option<JoinHandle<()>>,
}

impl Connection {
    /// Connect over TCP, authenticate, and return a live client with the
    /// default [`ReconnectPolicy`].
    ///
    /// `url` is `fluxum://host:port`; `token` is the auth token (empty under
    /// the dev `none` provider); `schemas` are the per-table primary-key
    /// projections the cache needs (SDK-040).
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
        let addr = parse_fluxum_url(url)?;
        let stream = TcpStream::connect(&addr)?;
        let writer = stream.try_clone()?;

        let shared = Arc::new(Shared {
            addr,
            token: token.to_vec(),
            policy,
            pending: Mutex::new(HashMap::new()),
            cache: Mutex::new(RowCache::new(schemas)),
            listeners: Mutex::new(HashMap::new()),
            resume: Mutex::new(ResumeTracker::new()),
            subs: Mutex::new(Vec::new()),
            identity: Mutex::new([0u8; 32]),
            writer: Mutex::new(Some(writer)),
            next_id: AtomicU32::new(1),
            closed: Mutex::new(false),
            wake: Condvar::new(),
        });

        // Authenticate before returning: connecting means "session ready",
        // not "socket open" (RPC-020). The reader thread does not exist yet,
        // so the handshake reads the stream inline.
        let mut messages = MessageStream::new(stream);
        let identity = authenticate(&shared, &mut messages)?;
        *shared
            .identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = identity;

        let reader = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || supervise(messages, &shared))
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
        let id = self.alloc_id();
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
        let id = self.alloc_id();
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
        let id = self.alloc_id();
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

    // --- Internals -----------------------------------------------------------

    fn alloc_id(&self) -> u32 {
        self.shared.next_id.fetch_add(1, Ordering::Relaxed)
    }

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
        let framed = encode_framed(&message)?;
        let mut guard = self
            .shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // `None` means the connection dropped and the reconnect loop has not
        // re-established it yet: fail fast rather than write into the void.
        let writer = guard.as_mut().ok_or(Error::Disconnected)?;
        writer.write_all(&framed)?;
        writer.flush()?;
        Ok(())
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
        // Closing the socket unblocks the reader thread's `read`.
        if let Some(writer) = self
            .shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            let _ = writer.shutdown(std::net::Shutdown::Both);
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
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

/// A blocking, buffered decoder of server messages off one socket. Owned by
/// whichever code is currently reading — the handshake reads it inline, then
/// hands it (buffer and all) to the read loop, so no bytes are lost at the
/// transition.
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
/// permitting — reconnect and carry on, forever, until the `Connection` is
/// dropped.
fn supervise(mut messages: MessageStream, shared: &Arc<Shared>) {
    loop {
        while let Some(message) = messages.next() {
            route(shared, message);
        }

        // Disconnected: new sends fail fast, in-flight callers unblock.
        *shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        fail_all(shared);

        if shared.is_closed() || !shared.policy.enabled {
            return;
        }
        match reestablish(shared) {
            Some(next) => messages = next,
            None => return,
        }
    }
}

/// The reconnect loop: connect, authenticate, resubscribe, reconcile — with
/// exponential backoff between attempts (SDK-047). `None` when the client was
/// closed or the policy's attempt budget ran out.
fn reestablish(shared: &Arc<Shared>) -> Option<MessageStream> {
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
        match try_session(shared) {
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

/// One reconnect attempt: a full session bring-up. Any failure aborts the
/// attempt; the loop backs off and tries again.
fn try_session(shared: &Arc<Shared>) -> Result<MessageStream, Error> {
    let stream = TcpStream::connect(&shared.addr)?;
    // A half-dead handshake must not wedge `Drop`: bound reads until the
    // session is live, then go back to blocking indefinitely.
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut writer = stream.try_clone()?;
    let mut messages = MessageStream::new(stream);

    // 1. Authenticate (the shared writer is still None — send directly).
    let auth_id = shared.next_id.fetch_add(1, Ordering::Relaxed);
    let auth = crate::protocol::Authenticate {
        id: auth_id,
        token: shared.token.clone(),
        compression: None,
        tx_updates: None,
        namespace: None,
    };
    writer.write_all(&encode_framed(&ClientMessage::Authenticate(auth))?)?;
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
            Some(_) => {} // nothing else belongs to this young session
        }
    };

    // 2. Resubscribe every active query, in registration order — always
    // BEFORE reconcile: InitialData must cover every active query, or
    // reconciliation reads the gap as rows having been deleted.
    let sqls: Vec<String> = shared
        .subs
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|e| e.sql.clone())
        .collect();
    let mut initials: Vec<InitialData> = Vec::new();
    if !sqls.is_empty() {
        let sub_id = shared.next_id.fetch_add(1, Ordering::Relaxed);
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

    // The fresh server-assigned ids, in reply order — one per query, matching
    // the registry's order because the Subscribe listed them in that order.
    let mut new_ids: Vec<u32> = Vec::new();
    let mut per_query: Vec<(u32, Vec<TableSnapshot>)> = Vec::new();
    let mut merged: Vec<TableSnapshot> = Vec::new();
    for initial in &initials {
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
    if new_ids.len() != sqls.len() {
        // The reply shape does not match the replay set; treat the attempt as
        // failed rather than mis-binding handles.
        return Err(Error::Disconnected);
    }

    // 3. Reconcile: net-difference against the fresh snapshot (SDK-047), then
    // re-establish which query holds what under the NEW ids.
    {
        let mut resume = shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *resume = ResumeTracker::new();
        for initial in &initials {
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

    // Session live: back to blocking reads, reopen the shared writer, and only
    // then tell the application what changed while it was away.
    messages.stream.set_read_timeout(None)?;
    *shared
        .writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(writer);
    dispatch_shared(shared, events);
    Ok(messages)
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

/// Authenticate a brand-new first session, reading the stream inline (the
/// reader thread does not exist yet).
fn authenticate(shared: &Shared, messages: &mut MessageStream) -> Result<[u8; 32], Error> {
    let auth_id = shared.next_id.fetch_add(1, Ordering::Relaxed);
    let auth = crate::protocol::Authenticate {
        id: auth_id,
        token: shared.token.clone(),
        compression: None,
        tx_updates: None,
        namespace: None,
    };
    let framed = encode_framed(&ClientMessage::Authenticate(auth))?;
    {
        let mut guard = shared
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let writer = guard.as_mut().ok_or(Error::Disconnected)?;
        writer.write_all(&framed)?;
        writer.flush()?;
    }
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

/// Parse `fluxum://host:port` into a socket address string.
fn parse_fluxum_url(url: &str) -> Result<String, Error> {
    let rest = url
        .strip_prefix("fluxum://")
        .ok_or_else(|| Error::Url(format!("expected fluxum://host:port, got {url}")))?;
    if !rest.contains(':') {
        return Err(Error::Url(format!("missing port in {url}")));
    }
    Ok(rest.trim_end_matches('/').to_owned())
}
