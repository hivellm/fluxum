//! A blocking FluxRPC client over TCP (SPEC-006 §4/§5).
//!
//! This is the object an application holds: it authenticates, calls reducers,
//! registers subscriptions with typed row callbacks, and keeps a local
//! [`RowCache`] in step with the server. It is deliberately synchronous and
//! thread-based — no async runtime — because the Rust SDK's first consumers
//! are services and tools that want a plain blocking client, and because it
//! keeps the crate's dependency surface to the vendored wire layer alone.
//!
//! One background reader thread owns the read half of the socket: it decodes
//! frames, routes id-correlated replies (RPC-002) to the waiting caller, and
//! applies server-initiated `TxUpdate`s to the cache. The write half is shared
//! behind a mutex so any thread can send.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::cache::{RowCache, RowEvent, TableDiff, TableSchema};
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

/// A row-event listener: `(row, old)` — `old` is `Some` only for updates.
pub type RowListener = Box<dyn Fn(&[u8], Option<&[u8]>) + Send + Sync>;

/// One reply routed by the reader to a waiting request: a server message, or
/// the error frame that ended the request.
type Routed = Result<ServerMessage, ErrorMessage>;

struct Shared {
    /// Request id → its reply channel (RPC-002 correlation).
    pending: Mutex<HashMap<u32, Sender<Routed>>>,
    /// The row cache plus its per-query bookkeeping, behind one lock.
    cache: Mutex<RowCache>,
    /// `"<Table>:<insert|delete|update>"` → listeners.
    listeners: Mutex<HashMap<String, Vec<RowListener>>>,
    /// The highest applied `tx_offset` per subscription (SPEC-021 CS-020),
    /// fed by every `InitialData`/`TxUpdate` this connection applies. It is
    /// the bookkeeping a session-preserving resume (CS-021) consumes; on this
    /// TCP client it also lets an application observe how current each
    /// subscription is.
    resume: Mutex<ResumeTracker>,
}

/// A connected Fluxum client.
pub struct Connection {
    shared: Arc<Shared>,
    /// The write half of the socket.
    writer: Mutex<TcpStream>,
    /// Monotonic request-id allocator.
    next_id: Mutex<u32>,
    /// The 32-byte identity the server derived for this session (SPEC-009).
    identity: [u8; 32],
    reader: Option<JoinHandle<()>>,
    stream: TcpStream,
}

impl Connection {
    /// Connect over TCP, authenticate, and return a live client.
    ///
    /// `url` is `fluxum://host:port`; `token` is the auth token (empty under
    /// the dev `none` provider); `schemas` are the per-table primary-key
    /// projections the cache needs (SDK-040).
    pub fn connect(
        url: &str,
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
    ) -> Result<Self, Error> {
        let addr = parse_fluxum_url(url)?;
        let stream = TcpStream::connect(addr)?;
        let reader_stream = stream.try_clone()?;
        let writer = stream.try_clone()?;

        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            cache: Mutex::new(RowCache::new(schemas)),
            listeners: Mutex::new(HashMap::new()),
            resume: Mutex::new(ResumeTracker::new()),
        });

        let reader = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || read_loop(reader_stream, shared))
        };

        let mut conn = Connection {
            shared,
            writer: Mutex::new(writer),
            next_id: Mutex::new(1),
            identity: [0u8; 32],
            reader: Some(reader),
            stream,
        };

        // Authenticate before returning: connecting means "session ready", not
        // "socket open" (RPC-020).
        let auth = crate::protocol::Authenticate {
            id: conn.alloc_id(),
            token: token.to_vec(),
            compression: None,
            tx_updates: None,
            namespace: None,
        };
        let reply = conn.request(ClientMessage::Authenticate(auth.clone()), auth.id, 1)?;
        if let Some(ServerMessage::AuthResult(result)) = reply.into_iter().next() {
            conn.identity = result.identity;
        }
        Ok(conn)
    }

    /// The 32-byte identity the server derived for this session.
    pub fn identity(&self) -> [u8; 32] {
        self.identity
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

    /// Register subscription queries, await every `InitialData`, and return
    /// the server-assigned `query_id` for each (SUB-001) — the handles
    /// [`Connection::unsubscribe`] takes. Ids come back in request order.
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
        self.dispatch(events);
        Ok(ids)
    }

    /// The highest `tx_offset` this client has applied for `query_id`
    /// (SPEC-021 CS-020), or `None` if nothing has been applied yet. How
    /// current the subscription is.
    pub fn applied_offset(&self, query_id: u32) -> Option<u64> {
        self.shared
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied_offset(query_id)
    }

    /// Drop subscriptions by their server-assigned query ids (SUB-004). Rows
    /// those queries held leave the cache unless another live subscription
    /// still covers them (SDK-044).
    pub fn unsubscribe(&self, query_ids: &[u32]) -> Result<(), Error> {
        if query_ids.is_empty() {
            return Ok(());
        }
        // Fire-and-forget: the server sends NO reply to Unsubscribe — delivery
        // simply stops (RPC-024). The message still carries an id for framing
        // symmetry.
        let id = self.alloc_id();
        self.send(ClientMessage::Unsubscribe(Unsubscribe {
            id,
            query_ids: query_ids.to_vec(),
        }))?;
        let mut cache = self
            .shared
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut events = Vec::new();
        for &query_id in query_ids {
            events.extend(cache.release_query(query_id));
        }
        drop(cache);
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
        let mut id = self
            .next_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = *id;
        *id += 1;
        current
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
        let body = message.encode()?;
        let mut framed = Vec::with_capacity(body.len() + 4);
        // A message body is far under the 16 MB frame cap; a `TooLarge` here
        // would be a client-side bug, surfaced rather than unwrapped.
        FrameCodec::default().encode_into(&body, &mut framed)?;
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        // Closing the socket unblocks the reader thread's `read`.
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
    }
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

/// The reader thread: decode frames, route id-correlated replies, and apply
/// server-initiated `TxUpdate`s to the cache.
fn read_loop(mut stream: TcpStream, shared: Arc<Shared>) {
    let codec = FrameCodec::default();
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];

    loop {
        // Drain every complete frame currently buffered before reading more.
        loop {
            let (frame_body, consumed) = match codec.decode(&buf) {
                Ok(Some((Frame::Body(body), consumed))) => (Some(body.to_vec()), consumed),
                Ok(Some((Frame::KeepAlive, consumed))) => (None, consumed),
                Ok(None) => break,
                // A framing violation desynchronizes the stream; stop.
                Err(_) => return fail_all(&shared),
            };
            buf.drain(..consumed);
            if let Some(body) = frame_body
                && let Ok(message) = ServerMessage::decode(&body)
            {
                route(&shared, message);
            }
        }

        match stream.read(&mut chunk) {
            Ok(0) => return fail_all(&shared), // clean EOF
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return fail_all(&shared),
        }
    }
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
