//! Per-connection session state machine and message routing (SPEC-006 §4;
//! AUTH-020): the socket-independent core the [`crate::tcp`] driver wraps.
//!
//! A session begins [`SessionState::Unauthenticated`]. The only message it
//! accepts there is `Authenticate`; any other message is answered `401
//! "unauthenticated"` with the connection kept open (AUTH-020). A successful
//! `Authenticate` transitions to [`SessionState::Authenticated`] with the
//! derived identity, an allocated `ConnectionId`, and the server-peer /
//! RLS-bypass flag, after which the six client message types route to the
//! reducer engine and subscription manager.
//!
//! Routing is `async` because a reducer call rides the T3.1 pipeline and the
//! subscription manager sits behind the SUB-041 async mutex. Each call
//! returns the [`ServerMessage`]s to send back (every one echoing the
//! request `id`, RPC-002); the driver serializes them onto the socket.

use std::sync::Arc;

use fluxum_core::FluxumError;
use fluxum_core::reducer::{CallOutcome, FluxValue, ReducerCaller};
use fluxum_core::subscription::{Resumed, Subscriber};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_protocol::{
    AuthResult, ClientMessage, ErrorMessage, ReducerResult, ServerMessage, TxUpdate, codes,
};

use crate::ShardContext;

/// The result of routing one message: the responses to send, plus any diff
/// to publish to the commit fan-out (a committed reducer call).
pub struct Routed {
    /// Responses to write back, in order (each echoes the request id).
    pub responses: Vec<ServerMessage>,
    /// A committed transaction to broadcast to subscribers (SUB-021).
    pub commit: Option<fluxum_core::store::TxDiff>,
}

impl Routed {
    fn reply(message: ServerMessage) -> Self {
        Self {
            responses: vec![message],
            commit: None,
        }
    }

    fn none() -> Self {
        Self {
            responses: Vec::new(),
            commit: None,
        }
    }
}

/// A connection's authentication state (SPEC-006 §4; AUTH-020).
#[derive(Debug, Clone)]
pub enum SessionState {
    /// No successful `Authenticate` yet — only `Authenticate` is accepted.
    Unauthenticated,
    /// Authenticated: the derived caller and its RLS posture.
    Authenticated {
        /// Reducer-call caller metadata (identity, connection, shard).
        caller: ReducerCaller,
        /// Subscription viewer (RLS bypass for server peers, SUB-031).
        subscriber: Subscriber,
    },
}

/// One connection's session over a [`ShardContext`].
pub struct Session {
    ctx: Arc<ShardContext>,
    state: SessionState,
    /// The database this connection is bound to (SPEC-025 OPS-050), chosen
    /// on `Authenticate` and fixed for the connection's lifetime. `None` is
    /// the default database — the context's own engine/subscriptions — which
    /// is what every client that names no namespace gets.
    ///
    /// Every read and write below routes through this binding, so a session
    /// simply has no way to name another database's tables: cross-namespace
    /// access is unrepresentable rather than merely rejected.
    namespace: Option<Arc<crate::namespace::Namespace>>,
    /// The resolved client IP (SEC-035), when the transport knows one — the
    /// key of the SEC-047 source bucket that token rotation cannot refill.
    /// `None` (embedded/in-process) falls back to the connection id.
    source_ip: Option<std::net::IpAddr>,
}

impl Session {
    /// A fresh unauthenticated session on the default database.
    pub fn new(ctx: Arc<ShardContext>) -> Self {
        Self {
            ctx,
            state: SessionState::Unauthenticated,
            namespace: None,
            source_ip: None,
        }
    }

    /// Record the transport-resolved client IP (SEC-035) so the SEC-047
    /// source bucket keys on it rather than the connection id.
    pub fn set_source_ip(&mut self, ip: std::net::IpAddr) {
        self.source_ip = Some(ip);
    }

    /// A session resumed from a persisted [`SessionState`] — the Streamable
    /// HTTP transport rebuilds one per request from its `Fluxum-Session`
    /// entry (SPEC-006 §3; the router core is transport-independent).
    pub fn with_state(ctx: Arc<ShardContext>, state: SessionState) -> Self {
        Self {
            ctx,
            state,
            namespace: None,
            source_ip: None,
        }
    }

    /// [`Session::with_state`] rebound to a namespace — the HTTP transport
    /// persists the binding alongside the router state, since it rebuilds a
    /// session per request.
    pub fn with_state_in(
        ctx: Arc<ShardContext>,
        state: SessionState,
        namespace: Option<Arc<crate::namespace::Namespace>>,
    ) -> Self {
        Self {
            ctx,
            state,
            namespace,
            source_ip: None,
        }
    }

    /// The namespace this session is bound to (`None` = the default
    /// database), so the transport can persist/resume the binding and
    /// publish commits to the right fan-out.
    pub fn namespace(&self) -> Option<&Arc<crate::namespace::Namespace>> {
        self.namespace.as_ref()
    }

    /// This session's reducer engine: its namespace's, or the default
    /// database's.
    pub fn engine(&self) -> &fluxum_core::reducer::ReducerEngine {
        match &self.namespace {
            Some(ns) => ns.engine(),
            None => &self.ctx.engine,
        }
    }

    /// This session's subscription registry (namespace-scoped).
    pub fn subscriptions(
        &self,
    ) -> &tokio::sync::Mutex<fluxum_core::subscription::SubscriptionManager> {
        match &self.namespace {
            Some(ns) => ns.subscriptions(),
            None => &self.ctx.subscriptions,
        }
    }

    /// This session's store — the lock-free read surface of its database.
    pub fn store(&self) -> &Arc<fluxum_core::store::MemStore> {
        match &self.namespace {
            Some(ns) => ns.store(),
            None => self.ctx.store(),
        }
    }

    /// Publish a committed diff to *this session's* namespace fan-out, so a
    /// tenant's commit never reaches another tenant's subscribers.
    pub fn publish_commit(&self, diff: fluxum_core::store::TxDiff) {
        match &self.namespace {
            Some(ns) => ns.publish_commit(diff),
            None => self.ctx.publish_commit(diff),
        }
    }

    /// The current session state (to persist across HTTP requests).
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    /// Take the session state out (HTTP request done, persist it back).
    pub fn into_state(self) -> SessionState {
        self.state
    }

    /// Whether the session has authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self.state, SessionState::Authenticated { .. })
    }

    /// The authenticated connection id, if any (for registry cleanup).
    pub fn connection_id(&self) -> Option<u128> {
        match &self.state {
            SessionState::Authenticated { caller, .. } => Some(caller.connection_id.as_u128()),
            SessionState::Unauthenticated => None,
        }
    }

    /// The authenticated caller (identity + connection id), if any — the seam
    /// the transport uses to fire the `on_connect` / `on_disconnect` lifecycle
    /// hooks (RED-011/012).
    pub fn caller(&self) -> Option<&ReducerCaller> {
        match &self.state {
            SessionState::Authenticated { caller, .. } => Some(caller),
            SessionState::Unauthenticated => None,
        }
    }

    /// Route one decoded client message (SPEC-006 §4).
    pub async fn handle(&mut self, message: ClientMessage) -> Routed {
        // Pre-auth gate (AUTH-020): only `Authenticate` is accepted; every
        // other message is a 401 with the connection kept open.
        if !self.is_authenticated() {
            if let ClientMessage::Authenticate(auth) = &message {
                return self.authenticate(auth.id, &auth.token, auth.namespace.as_deref());
            }
            return Routed::reply(error(
                Some(request_id(&message)),
                codes::AUTH_REQUIRED,
                "unauthenticated",
            ));
        }

        // SPEC-025 OPS-030: while draining, refuse *new* work with a
        // retryable signal so the SDK retries it against the restarted
        // process (OPS-031) rather than surfacing a failure. Work already
        // admitted is untouched, and reads/unsubscribe/resume still serve —
        // a drain must not break the clients it is politely shedding.
        if self.ctx.is_draining() && admits_new_work(&message) {
            return Routed::reply(error(
                Some(request_id(&message)),
                codes::CLUSTER_SHARD_UNAVAILABLE,
                "shard draining for restart; retry",
            ));
        }

        match message {
            // A second Authenticate re-derives identity but keeps the
            // connection id (idempotent re-auth).
            ClientMessage::Authenticate(auth) => {
                self.authenticate(auth.id, &auth.token, auth.namespace.as_deref())
            }
            ClientMessage::ReducerCall(call) => {
                self.reducer_call(call.id, call.reducer, call.args, call.idempotency_key)
                    .await
            }
            ClientMessage::Subscribe(sub) => self.subscribe(sub.id, sub.queries).await,
            ClientMessage::SubscribeSingle(sub) => self.subscribe(sub.id, vec![sub.query]).await,
            ClientMessage::Unsubscribe(unsub) => self.unsubscribe(unsub.query_ids).await,
            ClientMessage::OneOffQuery(query) => self.one_off_query(query.id, query.sql).await,
            ClientMessage::Resume(resume) => {
                self.resume(resume.id, resume.query_id, resume.from_offset)
                    .await
            }
        }
    }

    /// AUTH-020/021: validate the token, derive the identity, allocate a
    /// `ConnectionId`, and reply `AuthResult`. A failure is a `401` with the
    /// connection kept open.
    fn authenticate(&mut self, id: u32, token: &[u8], namespace: Option<&str>) -> Routed {
        // OPS-050: resolve the requested database first — an unknown name
        // must not authenticate into *anything*. The binding is fixed for
        // the connection's lifetime, so a re-`Authenticate` naming a
        // different database is refused rather than silently switching the
        // connection's data underneath its live subscriptions.
        let requested = match self.ctx.resolve_namespace(namespace) {
            Ok(ns) => ns,
            Err(e) => {
                self.ctx.metrics().note_auth(false);
                return Routed::reply(from_error(Some(id), &e));
            }
        };
        if self.is_authenticated() {
            let current = self.namespace.as_ref().map(|ns| ns.name());
            let wanted = requested.as_ref().map(|ns| ns.name());
            if current != wanted {
                return Routed::reply(error(
                    Some(id),
                    codes::AUTH_FAILED,
                    "a connection is bound to its database for its lifetime; \
                     reconnect to use another namespace (OPS-050)",
                ));
            }
        }

        let outcome = match self.ctx.authenticator.authenticate(token) {
            Ok(outcome) => outcome,
            Err(e) => {
                // OBS-040: a rejected authentication.
                self.ctx.metrics().note_auth(false);
                return Routed::reply(from_error(Some(id), &e));
            }
        };
        self.namespace = requested;
        // OBS-040: a successful authentication; a first auth is a new
        // connection (re-auth on an existing session keeps its id).
        self.ctx.metrics().note_auth(true);
        // Keep the connection id across a re-auth; allocate on first auth.
        let connection_id = match &self.state {
            SessionState::Authenticated { caller, .. } => caller.connection_id.as_u128(),
            SessionState::Unauthenticated => {
                self.ctx.metrics().note_connect();
                self.ctx.allocate_connection_id()
            }
        };
        let caller = ReducerCaller {
            identity: outcome.identity,
            connection_id: ConnectionId::new(connection_id),
            timestamp: Timestamp::now(),
            shard_id: self.ctx.shard_id,
        };
        let subscriber = Subscriber {
            identity: outcome.identity,
            is_server_peer: outcome.bypass_rls,
            // SPEC-017 CT-040: the auth layer's roles drive column grants.
            roles: outcome.roles.clone().into(),
        };
        self.state = SessionState::Authenticated { caller, subscriber };
        Routed::reply(ServerMessage::AuthResult(AuthResult {
            id,
            identity: *outcome.identity.as_bytes(),
            token: outcome.refreshed_token,
        }))
    }

    /// RPC-021: run a reducer through the engine. A business `Err(String)`
    /// (RED-060) is a `ReducerResult { Err }`, not an `Error` frame; an
    /// admission/query error maps to its wire code; a successful commit is
    /// published to the fan-out.
    async fn reducer_call(
        &self,
        id: u32,
        reducer: String,
        args: Vec<FluxValue>,
        idempotency_key: Option<String>,
    ) -> Routed {
        let (caller, _, _) = self.authed();
        // SPEC-025 OPS-060: the tenant's own ceilings, above the
        // per-(Identity, reducer) limiter. Checked at admission, so a
        // refused call costs no transaction and never touches another
        // tenant's admission or latency.
        if let Some(ns) = &self.namespace {
            if let Err(e) = ns.quotas().admit_reducer_call(ns.name()) {
                return Routed::reply(from_error(Some(id), &e));
            }
            if let Err(e) = ns.quotas().admit_write(
                ns.name(),
                || ns.memory_bytes(),
                || ns.engine().pipeline().log().disk_bytes().ok(),
            ) {
                return Routed::reply(from_error(Some(id), &e));
            }
        }
        let outcome = self
            .engine()
            .call_idempotent(caller, &reducer, args, idempotency_key.as_deref())
            .await;
        match outcome {
            Ok(CallOutcome::Committed(receipt)) => Routed {
                responses: vec![ServerMessage::ReducerResult(ReducerResult {
                    id,
                    outcome: Ok(()),
                })],
                commit: Some(receipt.diff),
            },
            // SPEC-021 CS-030: the key already applied — answer the original
            // result (a committed call's is `Ok`) and publish no diff; the
            // original commit already fanned out.
            Ok(CallOutcome::Deduplicated) => {
                Routed::reply(ServerMessage::ReducerResult(ReducerResult {
                    id,
                    outcome: Ok(()),
                }))
            }
            // RED-060/SPEC-028: a body's own Err is 5001; a panic is 5002 —
            // both travel as the ReducerResult outcome, not an Error frame.
            Err(FluxumError::Reducer(message)) => {
                Routed::reply(ServerMessage::ReducerResult(ReducerResult {
                    id,
                    outcome: Err(fluxum_protocol::ReducerError {
                        code: codes::REDUCER_USER_ERROR,
                        app_code: None,
                        message,
                    }),
                }))
            }
            Err(FluxumError::ReducerPanic(message)) => {
                Routed::reply(ServerMessage::ReducerResult(ReducerResult {
                    id,
                    outcome: Err(fluxum_protocol::ReducerError {
                        code: codes::REDUCER_PANIC,
                        app_code: None,
                        message,
                    }),
                }))
            }
            Err(e) => Routed::reply(from_error(Some(id), &e)),
        }
    }

    /// RPC-022/023: register each query and return its `InitialData`
    /// (SUB-001/002). A batch registers in order; the first failure is
    /// reported and the rest are not attempted (already-registered queries
    /// in the batch stay registered).
    /// SPEC-021 CS-021/CS-022: replay a subscription from the client's
    /// retained offset instead of re-sending its snapshot.
    ///
    /// Inside the retained window the reply is only the deltas after
    /// `from_offset`, as `TxUpdate`s carrying the offset each committed at;
    /// live updates then continue on the normal fan-out path. Outside it,
    /// the reply is a full `InitialData` with `cache_reset` set (CS-022). An
    /// unknown `query_id` — a session that did not outlive the blip — is a
    /// 404 telling the client to `Subscribe` afresh.
    async fn resume(&self, id: u32, query_id: u32, from_offset: u64) -> Routed {
        let (_, _, connection) = self.authed();
        let snapshot = self.store().snapshot();
        let manager = self.subscriptions().lock().await;
        match manager.resume(connection, query_id, from_offset, &snapshot) {
            Ok(Some(Resumed::Deltas(deltas))) => Routed {
                responses: deltas
                    .into_iter()
                    .map(|(offset, update)| {
                        ServerMessage::TxUpdate(TxUpdate {
                            tx_id: offset,
                            timestamp: 0,
                            reducer_name: String::new(),
                            caller: [0u8; 32],
                            duration_us: 0,
                            shard_id: self.ctx.shard_id,
                            tx_offset: offset,
                            tables: vec![(*update).clone()],
                        })
                    })
                    .collect(),
                commit: None,
            },
            Ok(Some(Resumed::Reset(initial))) => {
                let mut initial = *initial;
                initial.id = id;
                Routed::reply(ServerMessage::InitialData(initial))
            }
            Ok(None) => Routed::reply(error(
                Some(id),
                codes::SUB_UNKNOWN_QUERY_ID,
                "unknown query_id: subscribe again (the session did not survive)",
            )),
            Err(e) => Routed::reply(from_error(Some(id), &e)),
        }
    }

    async fn subscribe(&self, id: u32, queries: Vec<String>) -> Routed {
        let (_, subscriber, connection) = self.authed();
        let snapshot = self.store().snapshot();
        let mut manager = self.subscriptions().lock().await;
        let mut responses = Vec::with_capacity(queries.len());
        for sql in queries {
            // SPEC-026 SEC-047: per-identity + per-source admission, one
            // token per registration so a batch charges like a burst.
            if let Err(e) = self.admit_query(&subscriber, connection) {
                responses.push(from_error(Some(id), &e));
                break;
            }
            // SPEC-025 OPS-060: the tenant's subscription ceiling, read from
            // its own manager each time so the count cannot drift as
            // connections come and go — and re-checked per query, so a batch
            // cannot vault over the ceiling in one request.
            if let Some(ns) = &self.namespace {
                let live = u64::try_from(manager.plan_count()).unwrap_or(u64::MAX);
                if let Err(e) = ns.quotas().admit_subscription(ns.name(), live) {
                    responses.push(from_error(Some(id), &e));
                    break;
                }
            }
            match manager.subscribe(connection, subscriber.clone(), &sql, &snapshot) {
                Ok(mut subscribed) => {
                    subscribed.initial.id = id;
                    responses.push(ServerMessage::InitialData(subscribed.initial));
                }
                Err(e) => {
                    responses.push(from_error(Some(id), &e));
                    break;
                }
            }
        }
        Routed {
            responses,
            commit: None,
        }
    }

    /// RPC-024: drop each `query_id`. No response (delivery simply stops).
    async fn unsubscribe(&self, query_ids: Vec<u32>) -> Routed {
        let (_, _, connection) = self.authed();
        let mut manager = self.subscriptions().lock().await;
        for query_id in query_ids {
            manager.unsubscribe(connection, query_id);
        }
        Routed::none()
    }

    /// RPC-025: a one-off read (SUB-025) — the current filtered result,
    /// without registering a subscription. SPEC-022 RV-021: an `AS OF`
    /// clause resolves a historical snapshot from the temporal window;
    /// RLS and masking apply exactly as live (RV-022).
    async fn one_off_query(&self, id: u32, sql: String) -> Routed {
        let (_, subscriber, connection) = self.authed();
        // SPEC-026 SEC-047: one-off reads share the subscription admission
        // buckets — the snapshot evaluator being protected is the same.
        if let Err(e) = self.admit_query(&subscriber, connection) {
            return Routed::reply(from_error(Some(id), &e));
        }
        let snapshot = match fluxum_core::sql::as_of_point(&sql) {
            Ok(Some(point)) => match self.store().snapshot_as_of(point) {
                Ok(snapshot) => snapshot,
                Err(e) => return Routed::reply(from_error(Some(id), &e)),
            },
            Ok(None) => self.store().snapshot(),
            Err(e) => return Routed::reply(from_error(Some(id), &e)),
        };
        let manager = self.subscriptions().lock().await;
        match manager.snapshot_result(subscriber, &sql, &snapshot) {
            Ok(mut initial) => {
                initial.id = id;
                Routed::reply(ServerMessage::InitialData(initial))
            }
            Err(e) => Routed::reply(from_error(Some(id), &e)),
        }
    }

    /// SPEC-026 SEC-047: admit one subscription registration / one-off
    /// query. Server peers are exempt (AUTH-062); everyone else charges the
    /// per-identity bucket and the source-keyed secondary bucket (resolved
    /// client IP where the transport knows one, else the connection id), so
    /// rotating tokens cannot mint fresh budget.
    fn admit_query(
        &self,
        subscriber: &Subscriber,
        connection: u128,
    ) -> Result<(), FluxumError> {
        if subscriber.is_server_peer {
            return Ok(());
        }
        let source = self
            .source_ip
            .map(fluxum_core::reducer::QuerySource::Ip)
            .unwrap_or(fluxum_core::reducer::QuerySource::Connection(connection));
        match self
            .ctx
            .query_limiter()
            .check(&subscriber.identity, source)
        {
            Ok(()) => Ok(()),
            Err(rejected) => {
                self.ctx.metrics().note_query_rate_limited(rejected.bucket);
                Err(rejected.to_error())
            }
        }
    }

    /// The authenticated context (caller, subscriber, connection). Only
    /// called from the authenticated arms of [`Session::handle`], so the
    /// `Unauthenticated` fallback is unreachable in practice — but it is a
    /// benign zero-context rather than a panic, keeping the reducer path
    /// unwind-free.
    fn authed(&self) -> (ReducerCaller, Subscriber, u128) {
        match &self.state {
            SessionState::Authenticated { caller, subscriber } => (
                ReducerCaller {
                    timestamp: Timestamp::now(),
                    ..*caller
                },
                subscriber.clone(),
                caller.connection_id.as_u128(),
            ),
            SessionState::Unauthenticated => {
                let anon = Identity::from_bytes([0u8; 32]);
                let caller = ReducerCaller {
                    identity: anon,
                    connection_id: ConnectionId::new(0),
                    timestamp: Timestamp::now(),
                    shard_id: self.ctx.shard_id,
                };
                (caller, Subscriber::client(anon), 0)
            }
        }
    }
}

/// Whether `message` asks the shard to take on **new** work — the class a
/// drain sheds (SPEC-025 OPS-030).
///
/// `Unsubscribe` and `OneOffQuery` are deliberately absent: dropping a
/// subscription costs nothing and a read touches no writer, so refusing
/// them would only hurt the clients a drain is trying to let down gently.
/// `Resume` likewise continues an *existing* subscription (SPEC-021
/// CS-021) rather than starting one.
fn admits_new_work(message: &ClientMessage) -> bool {
    matches!(
        message,
        ClientMessage::ReducerCall(_)
            | ClientMessage::Subscribe(_)
            | ClientMessage::SubscribeSingle(_)
    )
}

/// The `id` a client message carries (echoed on its response, RPC-002).
fn request_id(message: &ClientMessage) -> u32 {
    match message {
        ClientMessage::Authenticate(m) => m.id,
        ClientMessage::ReducerCall(m) => m.id,
        ClientMessage::Subscribe(m) => m.id,
        ClientMessage::SubscribeSingle(m) => m.id,
        ClientMessage::Unsubscribe(m) => m.id,
        ClientMessage::OneOffQuery(m) => m.id,
        ClientMessage::Resume(m) => m.id,
    }
}

/// An `Error` server message.
fn error(id: Option<u32>, code: u16, message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ErrorMessage::from_catalog(id, code, message, Vec::new()))
}

/// Map a [`FluxumError`] to an `Error` frame: a `Query` error forwards its
/// wire code verbatim (400/403/404/429/503/…); anything else is a 500.
/// Project any [`FluxumError`] onto its SPEC-028 catalog entry — total: the
/// core mapping covers every variant, so no path emits an uncataloged code.
fn from_error(id: Option<u32>, e: &FluxumError) -> ServerMessage {
    let wire = e.to_wire();
    ServerMessage::Error(
        ErrorMessage::from_catalog(id, wire.code, wire.message, wire.details)
            .with_retry_after(wire.retry_after_ms),
    )
}
