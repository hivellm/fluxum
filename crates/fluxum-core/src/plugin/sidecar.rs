//! The generic sidecar proxy (SPEC-020 PLG-031): implements a ReadPath
//! capability trait by issuing a Plugin RPC call to an out-of-process
//! plugin.
//!
//! # Why this is blocking IO
//!
//! The capability traits ([`ScoreReranker`], [`Retriever`], [`Fusion`]) are
//! synchronous, and a ReadPath call sits inside query evaluation. So the
//! proxy dials with `std::net::TcpStream` and bounds every call with a
//! deadline rather than dragging an async runtime into the query path.
//! Blocking the evaluating thread for at most `timeout_ms` *is* the design:
//! the timeout is the query's budget for the plugin, and exceeding it
//! degrades (below) instead of waiting.
//!
//! # Degradation is structural, not implemented here (PLG-031)
//!
//! Nothing in this module falls back to a base result, because it has
//! nothing to fall back to — it only knows how to fail. The call sites in
//! `subscription` already run every ReadPath plugin as
//! `if let Ok(..) = state.guard(..)` and keep the base BM25 list otherwise,
//! so a proxy that returns [`PluginError`] on timeout degrades for free and
//! by the same path an in-process plugin's error does. A sidecar is just
//! another `Arc<dyn ScoreReranker>` to everything above it.
//!
//! # Failure handling
//!
//! - **Timeout**: every call carries a deadline; the socket timeout is
//!   re-armed to the *remaining* budget before each read, so a sidecar that
//!   dribbles bytes cannot extend the call past `timeout_ms`.
//! - **Reconnect**: any transport failure drops the connection; the next
//!   call redials and re-handshakes. A sidecar restart therefore costs one
//!   degraded query, not a permanent outage.
//! - **Circuit breaker**: after [`FAILURE_THRESHOLD`] consecutive failures
//!   the breaker opens for [`BREAKER_COOLDOWN`], and calls fail immediately
//!   (`breaker_open`) rather than each paying the full timeout. One trial
//!   call after the cooldown closes it on success or re-opens it on failure.
//!
//! # Authentication (PLG-031/061)
//!
//! [`SidecarConfig::token`] is a shared secret sent in [`Hello`]; a sidecar
//! refuses a wrong one. It authenticates the *host to the sidecar* — Fluxum
//! dials out, so this is the direction the handshake can prove. Trusting the
//! peer in the other direction is a deployment concern (loopback or mTLS):
//! the operator chose the endpoint.
//!
//! The proxy is granted no identity and bypasses no RLS. It cannot: the wire
//! carries opaque `pk` bytes the host already selected, and a `Retriever`'s
//! externally-sourced key still passes the call site's ordinary filters and
//! `#[visibility]` check before its row is surfaced.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fluxum_protocol::frame::{Frame, FrameCodec};
use fluxum_protocol::plugin_rpc::{
    Candidate, Candidates, FuseRequest, Hello, MatchQuery, PLUGIN_RPC_VERSION, PluginRequest,
    PluginResponse, RerankRequest, RetrieveRequest,
};

use crate::store::PkBytes;

use super::{Capability, FtQuery, Fusion, PluginCtx, PluginError, PluginInstance, Retriever,
            ScoreReranker, Scored};

/// Consecutive failures that open the breaker (PLG-031).
pub const FAILURE_THRESHOLD: u32 = 5;

/// How long the breaker stays open before admitting one trial call.
pub const BREAKER_COOLDOWN: Duration = Duration::from_secs(5);

/// Why a sidecar call failed — the `reason` label of
/// `fluxum_plugin_sidecar_errors_total{plugin, reason}` (PLG-031).
///
/// The split earns its keep by separating signals an operator acts on
/// differently: `Connect` is "the sidecar is down", `Timeout` is "it is too
/// slow for the budget", `Refused` is "it is up, healthy, and said no", and
/// `Protocol` is "we disagree about the wire" — a deployment/version bug.
/// All four degrade identically; only one of them is fixed by restarting the
/// sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidecarErrorReason {
    /// The call exceeded its deadline.
    Timeout,
    /// The endpoint could not be dialed or resolved.
    Connect,
    /// The connection broke mid-call (reset, EOF, short write).
    Transport,
    /// The sidecar answered, but not with something we can use: a bad
    /// version, an undecodable frame, or a reply to another call.
    Protocol,
    /// The sidecar answered `Error` — it is healthy and declined.
    Refused,
    /// The breaker was open; the call never left the process.
    BreakerOpen,
}

impl SidecarErrorReason {
    /// The metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connect => "connect",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::Refused => "refused",
            Self::BreakerOpen => "breaker_open",
        }
    }

    /// Every reason, so the exposition can emit a zero series per label
    /// rather than have one appear only once it first fires.
    pub const ALL: [Self; 6] = [
        Self::Timeout,
        Self::Connect,
        Self::Transport,
        Self::Protocol,
        Self::Refused,
        Self::BreakerOpen,
    ];
}

/// The breaker's state, as reported to `GET /plugins`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Calls flow.
    Closed,
    /// Calls fail fast until the cooldown expires.
    Open,
    /// The cooldown expired; one trial call decides.
    HalfOpen,
}

impl BreakerState {
    /// The introspection name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Open,
            2 => Self::HalfOpen,
            _ => Self::Closed,
        }
    }
}

/// Per-sidecar counters, shared with the registry so `GET /plugins` and
/// `/metrics` can read them without holding the proxy (PLG-031/060).
#[derive(Debug, Default)]
pub struct SidecarStats {
    calls: AtomicU64,
    timeout: AtomicU64,
    connect: AtomicU64,
    transport: AtomicU64,
    protocol: AtomicU64,
    refused: AtomicU64,
    breaker_open: AtomicU64,
    breaker_state: AtomicU8,
    breaker_opened_total: AtomicU64,
}

impl SidecarStats {
    fn counter(&self, reason: SidecarErrorReason) -> &AtomicU64 {
        match reason {
            SidecarErrorReason::Timeout => &self.timeout,
            SidecarErrorReason::Connect => &self.connect,
            SidecarErrorReason::Transport => &self.transport,
            SidecarErrorReason::Protocol => &self.protocol,
            SidecarErrorReason::Refused => &self.refused,
            SidecarErrorReason::BreakerOpen => &self.breaker_open,
        }
    }

    fn note_error(&self, reason: SidecarErrorReason) {
        self.counter(reason).fetch_add(1, Ordering::Relaxed);
    }

    /// Calls attempted (including ones the breaker refused).
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    /// The error count for `reason`.
    pub fn errors(&self, reason: SidecarErrorReason) -> u64 {
        self.counter(reason).load(Ordering::Relaxed)
    }

    /// Every `(label, count)` pair, in [`SidecarErrorReason::ALL`] order —
    /// the `fluxum_plugin_sidecar_errors_total{plugin, reason}` series.
    pub fn by_reason(&self) -> Vec<(&'static str, u64)> {
        SidecarErrorReason::ALL
            .iter()
            .map(|r| (r.as_str(), self.errors(*r)))
            .collect()
    }

    /// The breaker's current state.
    pub fn breaker_state(&self) -> BreakerState {
        BreakerState::from_u8(self.breaker_state.load(Ordering::Relaxed))
    }

    /// How many times the breaker has opened.
    pub fn breaker_opened_total(&self) -> u64 {
        self.breaker_opened_total.load(Ordering::Relaxed)
    }
}

/// What a proxy needs to reach and authenticate to one sidecar.
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    /// The manifest plugin name (metric label, logs, [`Hello::plugin`]).
    pub name: String,
    /// The capability the sidecar is expected to implement.
    pub capability: Capability,
    /// The sidecar's `host:port`.
    pub endpoint: String,
    /// Per-call budget.
    pub timeout: Duration,
    /// The shared secret, when the manifest configures one. Never logged.
    pub token: Option<String>,
    /// How long the breaker stays open before admitting a trial call. The
    /// registry uses [`BREAKER_COOLDOWN`]; it is a field so a deployment can
    /// tune recovery latency and a test need not sleep the default out.
    pub breaker_cooldown: Duration,
}

/// The circuit breaker: consecutive-failure counting with a timed cooldown.
#[derive(Debug)]
struct Breaker {
    consecutive: AtomicU64,
    /// When the breaker opened; `None` while closed. A `Mutex<Option<..>>`
    /// rather than an atomic because `Instant` is not one, and the breaker
    /// is consulted once per call — never in a hot loop.
    opened_at: Mutex<Option<Instant>>,
}

impl Breaker {
    fn new() -> Self {
        Self {
            consecutive: AtomicU64::new(0),
            opened_at: Mutex::new(None),
        }
    }

    fn opened_at(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.opened_at.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether this call may proceed, and in what state. `Open` means fail
    /// fast; `HalfOpen` means this is the trial call.
    fn admit(&self, stats: &SidecarStats, cooldown: Duration) -> BreakerState {
        let mut opened = self.opened_at();
        let state = match *opened {
            None => BreakerState::Closed,
            Some(at) if at.elapsed() >= cooldown => {
                // The cooldown expired: this caller is the trial. Clearing
                // the stamp here (rather than on the trial's result) means a
                // trial that hangs for the full timeout does not park every
                // other caller behind it — they see Closed and try too. That
                // is the intended trade: the breaker bounds a *broken*
                // sidecar's cost, and a recovering one is worth the probe.
                *opened = None;
                BreakerState::HalfOpen
            }
            Some(_) => BreakerState::Open,
        };
        stats
            .breaker_state
            .store(state as u8, Ordering::Relaxed);
        state
    }

    fn note_success(&self, stats: &SidecarStats) {
        self.consecutive.store(0, Ordering::Relaxed);
        *self.opened_at() = None;
        stats
            .breaker_state
            .store(BreakerState::Closed as u8, Ordering::Relaxed);
    }

    fn note_failure(&self, stats: &SidecarStats) {
        let failures = self.consecutive.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= u64::from(FAILURE_THRESHOLD) {
            let mut opened = self.opened_at();
            if opened.is_none() {
                *opened = Some(Instant::now());
                stats.breaker_opened_total.fetch_add(1, Ordering::Relaxed);
            }
            stats
                .breaker_state
                .store(BreakerState::Open as u8, Ordering::Relaxed);
        }
    }
}

/// A live, handshaken connection to the sidecar.
#[derive(Debug)]
struct Conn {
    stream: TcpStream,
    /// Bytes read but not yet consumed as a frame.
    buf: Vec<u8>,
}

/// The generic sidecar proxy (PLG-031). One per manifest binding; shared
/// across query threads, so calls serialize on a single connection — a
/// sidecar is a bounded external resource, and one in-flight call per
/// binding keeps its concurrency the operator's choice (run more sidecars),
/// not a fan-out this process decides on its own.
#[derive(Debug)]
pub struct SidecarProxy {
    config: SidecarConfig,
    codec: FrameCodec,
    conn: Mutex<Option<Conn>>,
    breaker: Breaker,
    stats: Arc<SidecarStats>,
    next_call_id: AtomicU64,
}

impl SidecarProxy {
    /// A proxy for `config`. Nothing is dialed here: a sidecar that is down
    /// at startup must not abort the server (it degrades), and one that is
    /// up must not be held to a connection across an idle night.
    pub fn new(config: SidecarConfig) -> Self {
        Self {
            config,
            codec: FrameCodec::default(),
            conn: Mutex::new(None),
            breaker: Breaker::new(),
            stats: Arc::new(SidecarStats::default()),
            next_call_id: AtomicU64::new(1),
        }
    }

    /// The shared counters, for the registry's introspection/exposition.
    pub fn stats(&self) -> Arc<SidecarStats> {
        Arc::clone(&self.stats)
    }

    /// Build the [`PluginInstance`] a sidecar binding of `capability` needs,
    /// or `None` for a capability with no ReadPath wire (PLG-031 models
    /// rerank/retrieve/fuse; `stream_sink`'s wire is the CDC task's and
    /// `key_provider`'s KMS exception caches keys instead of calling).
    pub fn instance(self: Arc<Self>) -> Option<PluginInstance> {
        Some(match self.config.capability {
            Capability::ScoreReranker => PluginInstance::ScoreReranker(self),
            Capability::Retriever => PluginInstance::Retriever(self),
            Capability::Fusion => PluginInstance::Fusion(self),
            _ => return None,
        })
    }

    fn error(&self, reason: SidecarErrorReason, detail: &str) -> PluginError {
        self.stats.note_error(reason);
        self.breaker.note_failure(&self.stats);
        tracing::debug!(
            plugin = %self.config.name,
            endpoint = %self.config.endpoint,
            reason = reason.as_str(),
            detail,
            "sidecar call failed; degrading to the base result (PLG-031)"
        );
        PluginError(format!(
            "sidecar `{}` ({}): {}: {detail}",
            self.config.name,
            self.config.endpoint,
            reason.as_str()
        ))
    }

    /// Issue one request and await its response, bounded by `timeout`.
    fn call(&self, request: PluginRequest) -> Result<Vec<Candidate>, PluginError> {
        self.stats.calls.fetch_add(1, Ordering::Relaxed);
        if self.breaker.admit(&self.stats, self.config.breaker_cooldown) == BreakerState::Open {
            // Fail fast: the point of the breaker is that a dead sidecar
            // costs one timeout per cooldown, not one per query.
            self.stats.note_error(SidecarErrorReason::BreakerOpen);
            return Err(PluginError(format!(
                "sidecar `{}`: circuit breaker open (PLG-031)",
                self.config.name
            )));
        }
        let deadline = Instant::now() + self.config.timeout;
        let mut guard = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        match self.exchange(&mut guard, &request, deadline) {
            Ok(candidates) => {
                self.breaker.note_success(&self.stats);
                Ok(candidates)
            }
            Err(err) => {
                // Any failed exchange leaves the stream at an unknown offset
                // — a half-written request, or a response we timed out on
                // that will arrive later and desynchronize every subsequent
                // call. Dropping it is the only safe resynchronization.
                *guard = None;
                Err(err)
            }
        }
    }

    fn exchange(
        &self,
        guard: &mut Option<Conn>,
        request: &PluginRequest,
        deadline: Instant,
    ) -> Result<Vec<Candidate>, PluginError> {
        if guard.is_none() {
            *guard = Some(self.connect(deadline)?);
        }
        let Some(conn) = guard.as_mut() else {
            return Err(self.error(SidecarErrorReason::Connect, "no connection"));
        };
        self.send(conn, request, deadline)?;
        let expected = request.call_id();
        let response = self.recv(conn, deadline)?;
        match response {
            PluginResponse::Candidates(Candidates { call_id, candidates })
                if Some(call_id) == expected =>
            {
                Ok(candidates)
            }
            PluginResponse::Error(err) => Err(self.error(SidecarErrorReason::Refused, &err.message)),
            PluginResponse::Candidates(c) => Err(self.error(
                SidecarErrorReason::Protocol,
                &format!(
                    "response for call_id {} but call {expected:?} is in flight",
                    c.call_id
                ),
            )),
            PluginResponse::Ready(_) => Err(self.error(
                SidecarErrorReason::Protocol,
                "unexpected Ready mid-connection",
            )),
        }
    }

    /// Dial and handshake, inside the call's remaining budget.
    fn connect(&self, deadline: Instant) -> Result<Conn, PluginError> {
        let remaining = Self::remaining(deadline)
            .ok_or_else(|| self.error(SidecarErrorReason::Timeout, "budget spent before connect"))?;
        let addr = self
            .config
            .endpoint
            .to_socket_addrs()
            .map_err(|e| self.error(SidecarErrorReason::Connect, &e.to_string()))?
            .next()
            .ok_or_else(|| {
                self.error(
                    SidecarErrorReason::Connect,
                    "endpoint resolved to no address",
                )
            })?;
        let stream = TcpStream::connect_timeout(&addr, remaining)
            .map_err(|e| self.error(SidecarErrorReason::Connect, &e.to_string()))?;
        // Nagle would hold a small request back waiting for more; every
        // frame here is a complete message the sidecar must see now.
        let _ = stream.set_nodelay(true);
        let mut conn = Conn {
            stream,
            buf: Vec::new(),
        };
        let hello = PluginRequest::Hello(Hello {
            version: PLUGIN_RPC_VERSION,
            plugin: self.config.name.clone(),
            capability: self.config.capability.name().to_owned(),
            token: self.config.token.clone(),
        });
        self.send(&mut conn, &hello, deadline)?;
        match self.recv(&mut conn, deadline)? {
            PluginResponse::Ready(ready) if ready.version == PLUGIN_RPC_VERSION => Ok(conn),
            PluginResponse::Ready(ready) => Err(self.error(
                SidecarErrorReason::Protocol,
                &format!(
                    "sidecar speaks Plugin RPC v{} but this build speaks v{PLUGIN_RPC_VERSION}",
                    ready.version
                ),
            )),
            // A rejected handshake is the sidecar declining the token or the
            // capability. It is `Protocol`, not `Refused`: `Refused` means a
            // healthy sidecar declined one call, but this binding cannot make
            // any call at all — a deployment error, not a runtime one.
            PluginResponse::Error(err) => Err(self.error(
                SidecarErrorReason::Protocol,
                &format!("handshake rejected: {}", err.message),
            )),
            PluginResponse::Candidates(_) => Err(self.error(
                SidecarErrorReason::Protocol,
                "sidecar answered the handshake with candidates",
            )),
        }
    }

    fn send(
        &self,
        conn: &mut Conn,
        request: &PluginRequest,
        deadline: Instant,
    ) -> Result<(), PluginError> {
        let body = rmp_serde::to_vec(request)
            .map_err(|e| self.error(SidecarErrorReason::Protocol, &e.to_string()))?;
        let frame = self
            .codec
            .encode(&body)
            .map_err(|e| self.error(SidecarErrorReason::Protocol, &e.to_string()))?;
        self.arm(conn, deadline, /* read */ false)?;
        conn.stream
            .write_all(&frame)
            .and_then(|()| conn.stream.flush())
            .map_err(|e| self.io_error(e, "write"))
    }

    fn recv(&self, conn: &mut Conn, deadline: Instant) -> Result<PluginResponse, PluginError> {
        loop {
            match self.codec.decode(&conn.buf) {
                Ok(Some((Frame::Body(body), consumed))) => {
                    let response = rmp_serde::from_slice(body)
                        .map_err(|e| self.error(SidecarErrorReason::Protocol, &e.to_string()));
                    conn.buf.drain(..consumed);
                    return response;
                }
                // RPC-001: a keep-alive is not a response; keep waiting
                // within the same budget.
                Ok(Some((Frame::KeepAlive, consumed))) => {
                    conn.buf.drain(..consumed);
                    continue;
                }
                Ok(None) => {}
                Err(e) => return Err(self.error(SidecarErrorReason::Protocol, &e.to_string())),
            }
            // Re-arm to the *remaining* budget every read, so a sidecar that
            // dribbles a byte at a time cannot stretch the call past
            // `timeout_ms` by resetting the socket timeout each time.
            self.arm(conn, deadline, /* read */ true)?;
            let mut chunk = [0u8; 4096];
            match conn.stream.read(&mut chunk) {
                Ok(0) => {
                    return Err(self.error(
                        SidecarErrorReason::Transport,
                        "sidecar closed the connection",
                    ));
                }
                Ok(n) => conn.buf.extend_from_slice(&chunk[..n]),
                Err(e) => return Err(self.io_error(e, "read")),
            }
        }
    }

    /// Set the socket's remaining budget, or fail the call if it is spent.
    fn arm(&self, conn: &Conn, deadline: Instant, read: bool) -> Result<(), PluginError> {
        let remaining = Self::remaining(deadline)
            .ok_or_else(|| self.error(SidecarErrorReason::Timeout, "call budget exhausted"))?;
        let set = if read {
            conn.stream.set_read_timeout(Some(remaining))
        } else {
            conn.stream.set_write_timeout(Some(remaining))
        };
        set.map_err(|e| self.error(SidecarErrorReason::Transport, &e.to_string()))
    }

    /// The budget left, or `None` once spent. A zero `Duration` on a socket
    /// timeout means *no timeout* on both Unix and Windows — the one value
    /// that must never reach `set_read_timeout`, since "no budget left"
    /// would become "block forever".
    fn remaining(deadline: Instant) -> Option<Duration> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
    }

    fn io_error(&self, e: std::io::Error, op: &str) -> PluginError {
        let reason = match e.kind() {
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                SidecarErrorReason::Timeout
            }
            _ => SidecarErrorReason::Transport,
        };
        self.error(reason, &format!("{op}: {e}"))
    }

    fn wire_query(query: &FtQuery) -> MatchQuery {
        MatchQuery {
            table: query.table.clone(),
            column: query.column.clone(),
            query: query.query.clone(),
            limit: query.limit as u64,
        }
    }

    fn to_wire(scored: &[Scored]) -> Vec<Candidate> {
        scored
            .iter()
            .map(|s| Candidate {
                pk: s.pk.as_bytes().to_vec(),
                score: s.score,
            })
            .collect()
    }

    fn from_wire(candidates: Vec<Candidate>) -> Vec<Scored> {
        candidates
            .into_iter()
            .map(|c| Scored {
                pk: PkBytes::from_bytes(c.pk),
                score: c.score,
            })
            .collect()
    }

    fn next_call_id(&self) -> u64 {
        self.next_call_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl ScoreReranker for SidecarProxy {
    fn rerank(
        &self,
        query: &FtQuery,
        candidates: Vec<Scored>,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        let request = PluginRequest::Rerank(RerankRequest {
            call_id: self.next_call_id(),
            query: Self::wire_query(query),
            candidates: Self::to_wire(&candidates),
        });
        self.call(request).map(Self::from_wire)
    }
}

impl Retriever for SidecarProxy {
    fn retrieve(
        &self,
        query: &FtQuery,
        k: usize,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        let request = PluginRequest::Retrieve(RetrieveRequest {
            call_id: self.next_call_id(),
            query: Self::wire_query(query),
            k: k as u64,
        });
        self.call(request).map(Self::from_wire)
    }
}

impl Fusion for SidecarProxy {
    fn fuse(&self, lexical: &[Scored], dense: &[Scored], ctx: &PluginCtx) -> Vec<Scored> {
        let request = PluginRequest::Fuse(FuseRequest {
            call_id: self.next_call_id(),
            lexical: Self::to_wire(lexical),
            dense: Self::to_wire(dense),
        });
        // `Fusion::fuse` is infallible by signature, so a failing sidecar
        // degrades right here to the default RRF rather than at the call
        // site. Same rule as every other ReadPath plugin (PLG-031): the base
        // result, never an error.
        self.call(request).map_or_else(
            |_| super::ReciprocalRankFusion::default().fuse(lexical, dense, ctx),
            Self::from_wire,
        )
    }
}
