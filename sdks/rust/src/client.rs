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
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cache::{RowEvent, TableDiff, TableSchema, TableSnapshot};
use crate::http::{ChunkedStream, HttpEndpoint};
use crate::idempotency::OfflineQueue;
use crate::optimistic::{OptimisticStore, SyncedCache};
use crate::persist::{ClientStore, PersistedMeta, PersistedQuery, PersistenceBackend};
use crate::protocol::{
    ClientMessage, ErrorMessage, FluxValue, Frame, FrameCodec, InitialData, ReducerCall,
    ReducerError, ReducerResult, Resume, ServerMessage, Subscribe, TableUpdate, TxUpdate,
    Unsubscribe,
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

/// The offline queue's key namespace for this client instance (CS-032).
/// Process id + wall-clock nanos: unique enough that two clients sharing an
/// identity cannot mint colliding keys. A DURABLE queue must reuse the id it
/// persisted instead (SDK offline persistence, CS-040) — that task threads a
/// caller-supplied id through; until then the queue lives and dies with the
/// process, so a fresh namespace per instance is exactly right.
fn mint_client_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{:x}-{nanos:x}", std::process::id())
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

/// A listener for optimistic calls the server REJECTED (SPEC-021 CS-011):
/// `(reducer, idempotency_key, error)`. The overlay has already rolled back
/// when this fires — the callback is how the application tells the user.
pub type RejectedListener = Box<dyn Fn(&str, &str, &ReducerError) + Send + Sync>;

/// The optimistic submission ledger (SPEC-021 CS-010/CS-032): the offline
/// replay queue plus the maps binding a queued call's stable key to its
/// overlay layer and to the request id its current attempt went out under.
struct OptimisticState {
    /// Every unacknowledged optimistic call, oldest first, each carrying the
    /// idempotency key minted at enqueue (CS-032).
    queue: OfflineQueue,
    /// Request id of the in-flight attempt → the call's idempotency key.
    /// Cleared on disconnect: a dead session's ids answer nothing.
    in_flight: HashMap<u32, String>,
    /// Idempotency key → overlay layer id in the [`SyncedCache`].
    layers: HashMap<String, u64>,
}

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
    /// The replica-set endpoints to try on reconnect (SPEC-014 REP-033):
    /// on failover the old primary is dead, so the reconnect loop rotates
    /// through these to find the new one. A single-endpoint connect stores
    /// exactly one entry, preserving the original behavior.
    endpoints: Vec<String>,
    /// The endpoint the reconnect loop is currently trying (index into
    /// [`Shared::endpoints`]); advanced on each failed attempt.
    endpoint_ix: AtomicUsize,
    /// The `/rpc` endpoint when the transport is Streamable HTTP.
    http: Option<HttpEndpoint>,
    /// The auth token, replayed on every re-authentication (SPEC-009).
    token: Vec<u8>,
    policy: ReconnectPolicy,
    /// Request id → its reply channel (RPC-002 correlation).
    pending: Mutex<HashMap<u32, Sender<Routed>>>,
    /// The row cache — authoritative base plus the optimistic overlay
    /// (SPEC-021 CS-010) — and its per-query bookkeeping, behind one lock.
    cache: Mutex<SyncedCache>,
    /// The optimistic queue + ledgers. Lock ORDER: `optimistic` before
    /// `cache`, never the reverse.
    optimistic: Mutex<OptimisticState>,
    /// `"<Table>:<insert|delete|update>"` → listeners.
    listeners: Mutex<HashMap<String, Vec<RowListener>>>,
    /// Listeners for rejected optimistic calls (CS-011 rollback path).
    rejected: Mutex<Vec<RejectedListener>>,
    /// The highest applied `tx_offset` per subscription (SPEC-021 CS-020),
    /// fed by every `InitialData`/`TxUpdate` this connection applies. It
    /// drives the HTTP blip `Resume` (CS-021) and is rebuilt on a full
    /// re-establishment: a new session's offsets restart with its snapshot.
    resume: Mutex<ResumeTracker>,
    /// Live subscriptions, in registration order — the reconnect replay set.
    subs: Mutex<Vec<SubEntry>>,
    /// The durable local store (SPEC-021 CS-040), when persistence was
    /// opted into. `None` — the default — is exactly the old client.
    persist: Option<ClientStore>,
    /// The identity the hydrated state belonged to, consumed by the first
    /// [`replay_offline`]: a mismatch (a different user logged in) discards
    /// the queued mutations instead of replaying them as someone else.
    hydrated_identity: Mutex<Option<[u8; 32]>>,
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

    /// REP-042: the current endpoint answered `NotPrimary` — advance to the
    /// next replica-set member and drop the socket so the reconnect loop
    /// re-establishes against it (which locates the current primary). A
    /// single-endpoint client has nowhere to redirect, so this is a no-op.
    fn redirect_to_next_primary(&self) {
        if self.endpoints.len() <= 1 {
            return;
        }
        self.endpoint_ix.fetch_add(1, Ordering::SeqCst);
        if let Some(socket) = self
            .push_socket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
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
        Self::connect_impl(url, token, schemas, policy, None, &[])
    }

    /// Connect to a **replica set** (SPEC-014 REP-033): the first URL is
    /// tried first, and on reconnect the client rotates through all of
    /// `urls` to find the current primary after a failover. Every URL must
    /// use the same transport (all `tcp://` or all `http://`).
    ///
    /// The reconnect is otherwise identical to [`Connection::connect_with`]:
    /// re-authenticate (same token ⇒ same identity), resubscribe every
    /// active query, and reconcile — the application observes only the net
    /// change across the failover, never a cache wipe (SDK-047).
    ///
    /// # Errors
    /// An empty `urls`, mixed transports, or the initial connect failing.
    pub fn connect_replica_set(
        urls: &[&str],
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
        policy: ReconnectPolicy,
    ) -> Result<Self, Error> {
        let first = urls
            .first()
            .ok_or_else(|| Error::Url("connect_replica_set needs at least one endpoint".into()))?;
        Self::connect_impl(first, token, schemas, policy, None, &urls[1..])
    }

    /// [`Connection::connect_with`] with **durable local persistence**
    /// (SPEC-021 CS-040/CS-041), opt-in: subscribed rows, resume offsets,
    /// and the offline mutation queue are written through to `backend`
    /// under `(url, client_id)`, and a restart hydrates from it.
    ///
    /// On startup the persisted subscriptions are re-registered and the
    /// fresh `InitialData` is reconciled against the hydrated rows, so the
    /// application hears only the NET difference — not a cold re-download's
    /// worth of inserts. Queued mutations replay in submission order under
    /// their ORIGINAL idempotency keys (CS-032): a call queued before the
    /// restart applies exactly once. If the fresh session authenticates as
    /// a DIFFERENT identity than the persisted state's, the queue is
    /// discarded rather than replayed as someone else, and the store is
    /// cleared.
    ///
    /// `client_id` must be stable for this logical client across restarts —
    /// it namespaces both the store and the idempotency keys.
    pub fn connect_persistent(
        url: &str,
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
        policy: ReconnectPolicy,
        backend: std::sync::Arc<dyn PersistenceBackend>,
        client_id: impl Into<String>,
    ) -> Result<Self, Error> {
        let client_id = client_id.into();
        let store = ClientStore::new(backend, url, &client_id);
        Self::connect_impl(url, token, schemas, policy, Some((store, client_id)), &[])
    }

    fn connect_impl(
        url: &str,
        token: &[u8],
        schemas: impl IntoIterator<Item = TableSchema>,
        policy: ReconnectPolicy,
        persistence: Option<(ClientStore, String)>,
        extra_endpoints: &[&str],
    ) -> Result<Self, Error> {
        let target = parse_url(url)?;
        let (addr, http) = match &target {
            Target::Tcp(addr) => (addr.clone(), None),
            Target::Http(addr) => (String::new(), Some(HttpEndpoint { addr: addr.clone() })),
        };
        // REP-033: the replica-set endpoints the reconnect loop rotates
        // through. Each must parse to the same transport as the first.
        let mut endpoints = vec![addr.clone()];
        for extra in extra_endpoints {
            match parse_url(extra)? {
                Target::Tcp(a) if http.is_none() => endpoints.push(a),
                Target::Http(a) if http.is_some() => endpoints.push(a),
                _ => {
                    return Err(Error::Url(
                        "replica-set endpoints must all use the same transport".into(),
                    ));
                }
            }
        }

        // Hydrate the meta blob first (CS-041): the queue must exist —
        // restored, keys intact — before anything can transmit.
        let (persist, queue, hydrated_identity, hydrated_queries) = match persistence {
            None => (None, OfflineQueue::new(mint_client_id()), None, Vec::new()),
            Some((store, client_id)) => {
                let meta = store.load_meta();
                let queries = store.load_queries();
                let (queue, identity) = match meta {
                    Some(meta) => {
                        let identity: Option<[u8; 32]> = meta.identity.as_slice().try_into().ok();
                        (OfflineQueue::restore(meta.queue), identity)
                    }
                    None => (OfflineQueue::new(client_id), None),
                };
                (Some(store), queue, identity, queries)
            }
        };

        let shared = Arc::new(Shared {
            endpoints,
            endpoint_ix: AtomicUsize::new(0),
            http,
            token: token.to_vec(),
            policy,
            pending: Mutex::new(HashMap::new()),
            cache: Mutex::new(SyncedCache::new(schemas)),
            optimistic: Mutex::new(OptimisticState {
                queue,
                in_flight: HashMap::new(),
                layers: HashMap::new(),
            }),
            listeners: Mutex::new(HashMap::new()),
            rejected: Mutex::new(Vec::new()),
            resume: Mutex::new(ResumeTracker::new()),
            subs: Mutex::new(Vec::new()),
            persist,
            hydrated_identity: Mutex::new(hydrated_identity),
            identity: Mutex::new([0u8; 32]),
            writer: Mutex::new(None),
            push_socket: Mutex::new(None),
            next_id: AtomicU32::new(1),
            closed: Mutex::new(false),
            wake: Condvar::new(),
        });

        // Seed the hydrated subscriptions as if a previous session had
        // registered them: rows into the cache, SQL into the replay set.
        // The session establishment below then resubscribes and RECONCILES
        // — the application sees the persisted rows plus the net change,
        // never a cold re-download (CS-041).
        let hydrated = !hydrated_queries.is_empty();
        for query in &hydrated_queries {
            let app_id = shared.alloc_id();
            {
                let mut subs = shared
                    .subs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                subs.push(SubEntry {
                    sql: query.sql.clone(),
                    app_id,
                    server_id: app_id,
                });
            }
            let diffs: Vec<TableDiff> = query
                .snapshots()
                .into_iter()
                .map(|snapshot| TableDiff {
                    table: snapshot.table,
                    inserts: snapshot.rows,
                    deletes: Vec::new(),
                })
                .collect();
            // No listeners exist yet; hydration events go nowhere by design.
            let _ = shared
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .apply_tx(&[(app_id, diffs)], None);
        }

        let read_half = match target {
            // A hydrated TCP client establishes exactly like a reconnect —
            // authenticate, resubscribe the replay set, reconcile, replay
            // the queue. Without hydration the historical inline handshake
            // is kept as-is.
            Target::Tcp(_) if hydrated => try_tcp_session(&shared)?,
            Target::Tcp(_) => {
                // REP-033: try the replica-set endpoints in order — the
                // first may be a stale/known primary that is already down.
                let mut stream = None;
                let mut last_err = None;
                for (ix, endpoint) in shared.endpoints.iter().enumerate() {
                    match TcpStream::connect(endpoint) {
                        Ok(s) => {
                            shared.endpoint_ix.store(ix, Ordering::SeqCst);
                            stream = Some(s);
                            break;
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                let stream = match stream {
                    Some(s) => s,
                    None => {
                        return Err(Error::Io(last_err.unwrap_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::AddrNotAvailable,
                                "no replica-set endpoint reachable",
                            )
                        })));
                    }
                };
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
                // A restored queue may hold calls from before the restart
                // even when no query state was persisted.
                replay_offline(&shared);
                ReadHalf::Tcp(messages)
            }
            // The first HTTP session IS a full establishment: authenticate,
            // resubscribe whatever was hydrated, reconcile, open the stream.
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
        persist_state(&self.shared);
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
        // from the reconnect replay set (and the durable store's).
        let mut dropped_sqls: Vec<String> = Vec::new();
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
                        Some(pos) => {
                            let entry = subs.remove(pos);
                            dropped_sqls.push(entry.sql);
                            entry.server_id
                        }
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
        if let Some(store) = &self.shared.persist {
            for sql in &dropped_sqls {
                store.delete_query(sql);
            }
        }
        persist_state(&self.shared);
        Ok(())
    }

    /// Call a reducer and await its outcome. Resolves when the reducer
    /// committed — the resulting `TxUpdate` may arrive before or after.
    pub fn call_reducer(&self, name: &str, args: Vec<FluxValue>) -> Result<(), Error> {
        self.call_reducer_async(name, args)?.wait()
    }

    /// Start a reducer call WITHOUT waiting for its ack — **write
    /// pipelining** (SDK-032): any number of calls may be in flight on one
    /// connection, each resolved by awaiting its [`PendingReducer`].
    ///
    /// # Concurrency contract
    ///
    /// - **Attribution is exact**: every ack/error frame carries the request
    ///   id (RPC-002); a reply resolves precisely the `PendingReducer` it
    ///   belongs to, never a neighbor — a rejected call among successes
    ///   surfaces on its own handle.
    /// - **Ordering**: same-connection calls are sent in `call_reducer_async`
    ///   invocation order (sends serialize on the write half) and the server
    ///   executes a connection's calls in arrival order, so pipelined writes
    ///   commit in submission order. Ordering ACROSS connections is decided
    ///   by the shard's single-writer queue, as always.
    /// - **In-flight window / backpressure**: the client imposes no cap —
    ///   backpressure is the transport's send buffer plus the server's
    ///   admission control: an overloaded shard answers
    ///   `CLUSTER_SHARD_UNAVAILABLE` ("shard busy", TXN-011), which resolves
    ///   exactly the calls it refused. Callers wanting a bounded window hold
    ///   at most N handles and await the oldest before issuing the next.
    /// - **Disconnects** fail every in-flight handle with
    ///   [`Error::Disconnected`]; delivery of an un-acked call is unknown
    ///   (the classic pipelining trade — use idempotency keys where that
    ///   matters, SPEC-021 CS-030).
    /// - **Transports**: over TCP the calls genuinely share the socket. Over
    ///   Streamable HTTP each send is its own `POST /rpc` whose response
    ///   already carries the ack, so the send itself round-trips —
    ///   pipelining does not overlap requests there; concurrency over HTTP
    ///   comes from concurrent callers instead.
    pub fn call_reducer_async(
        &self,
        name: &str,
        args: Vec<FluxValue>,
    ) -> Result<PendingReducer, Error> {
        let id = self.shared.alloc_id();
        let call = ReducerCall {
            id,
            reducer: name.to_owned(),
            version: None,
            args,
            idempotency_key: None,
        };
        let (tx, rx): (Sender<Routed>, Receiver<Routed>) = mpsc::channel();
        self.shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, tx);
        if let Err(e) = self.send(ClientMessage::ReducerCall(call)) {
            self.shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            return Err(e);
        }
        Ok(PendingReducer {
            shared: Arc::clone(&self.shared),
            rx,
            id,
        })
    }

    /// Call a reducer **optimistically** (SPEC-021 CS-010): `updater` mutates
    /// the local store immediately — before the server confirms — and the
    /// call is queued with a stable `idempotency_key` (CS-032), returned as
    /// the submission handle.
    ///
    /// The lifecycle is fire-and-observe rather than await:
    ///
    /// - the updater's rows show instantly in [`Connection::rows`] and fire
    ///   the usual row listeners;
    /// - on the authoritative confirmation the overlay is swapped for the
    ///   server's rows in one atomic batch — no flicker, no duplicate
    ///   (CS-011);
    /// - on `ReducerResult::Err` the overlay rolls back to the exact
    ///   pre-mutation state and the [`Connection::on_rejected`] listeners
    ///   fire;
    /// - while DISCONNECTED the call simply stays queued and replays in
    ///   submission order when the session comes back (CS-032) — the stable
    ///   key makes the replay exactly-once even when the first send's ack
    ///   was lost.
    ///
    /// The updater runs under the client's internal locks: it must only use
    /// the [`OptimisticStore`] it is handed, never call back into the
    /// `Connection`.
    ///
    /// One caveat inherits from the wire (which this layer does not change):
    /// commits are attributed to their overlay by `(caller identity,
    /// reducer)` in FIFO order, so concurrently mixing `call_optimistic` and
    /// plain [`Connection::call_reducer`] on the SAME reducer — or running
    /// two connections under one identity calling it — can drop an overlay
    /// one update early. The cost is a transient re-render, never divergence.
    pub fn call_optimistic(
        &self,
        reducer: &str,
        args: Vec<FluxValue>,
        updater: impl FnOnce(&mut OptimisticStore<'_>),
    ) -> Result<String, Error> {
        let (key, events, message, id) = {
            let mut optimistic = self
                .shared
                .optimistic
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut cache = self
                .shared
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (layer, events) = cache.apply_optimistic(reducer, updater);
            let key = optimistic.queue.enqueue(reducer, args);
            optimistic.layers.insert(key.clone(), layer);
            let id = self.shared.alloc_id();
            let message = optimistic.queue.attempt(&key, id);
            optimistic.in_flight.insert(id, key.clone());
            (key, events, message, id)
        };
        self.dispatch(events);
        // Transmit if the session is live. ANY send failure leaves the call
        // queued — the reconnect replay resends it under the same key, which
        // is the entire point of minting the key at enqueue time.
        if let Some(message) = message
            && self.send(message).is_err()
        {
            self.shared
                .optimistic
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .in_flight
                .remove(&id);
        }
        // The queue changed: capture it now, so a crash before the ack still
        // replays this call under its key after restart (CS-040/CS-032).
        persist_state(&self.shared);
        Ok(key)
    }

    /// Register a listener for optimistic calls the server rejected. The
    /// rollback has already been applied (and its row events dispatched)
    /// when the listener runs.
    pub fn on_rejected(&self, listener: RejectedListener) {
        self.shared
            .rejected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(listener);
    }

    /// How many optimistic calls are still awaiting acknowledgement —
    /// including everything buffered while disconnected. `0` means every
    /// submitted call has been confirmed or rejected.
    pub fn pending_mutations(&self) -> usize {
        self.shared
            .optimistic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .queue
            .pending()
            .len()
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

/// A reducer call in flight ([`Connection::call_reducer_async`]): resolve it
/// with [`PendingReducer::wait`]. Dropping it without waiting abandons the
/// call — the ack is discarded on arrival (the call itself is NOT cancelled;
/// it was already on the wire).
pub struct PendingReducer {
    shared: Arc<Shared>,
    rx: Receiver<Routed>,
    id: u32,
}

impl PendingReducer {
    /// Await this call's own ack: `Ok` when the reducer committed, the
    /// call's exact error otherwise (RPC-031 reducer rejection, an `Error`
    /// frame, or [`Error::Disconnected`] when the session died with the
    /// call in flight).
    pub fn wait(self) -> Result<(), Error> {
        match self.rx.recv() {
            Ok(Ok(ServerMessage::ReducerResult(result))) => match result.outcome {
                Ok(()) => Ok(()),
                Err(e) => Err(Error::Reducer {
                    code: e.code,
                    app_code: e.app_code,
                    message: e.message,
                }),
            },
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => {
                // REP-042: this member is a replica — redirect to the
                // primary so the (retryable) retry lands on it.
                if err.code == crate::protocol::codes::CLUSTER_NOT_PRIMARY {
                    self.shared.redirect_to_next_primary();
                }
                Err(Error::from(err))
            }
            Err(_) => Err(Error::Disconnected),
        }
    }

    /// The request id this call went out under (RPC-002 correlation).
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }
}

impl Drop for PendingReducer {
    fn drop(&mut self) {
        // Idempotent: `wait` consumed the reply, or the entry is stale.
        self.shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
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

mod runtime;
#[allow(clippy::wildcard_imports)]
use runtime::*;
