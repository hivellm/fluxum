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
use fluxum_core::reducer::{FluxValue, ReducerCaller};
use fluxum_core::subscription::Subscriber;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_protocol::{
    AuthResult, ClientMessage, ErrorMessage, ReducerResult, ServerMessage, codes,
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
}

impl Session {
    /// A fresh unauthenticated session.
    pub fn new(ctx: Arc<ShardContext>) -> Self {
        Self {
            ctx,
            state: SessionState::Unauthenticated,
        }
    }

    /// A session resumed from a persisted [`SessionState`] — the Streamable
    /// HTTP transport rebuilds one per request from its `Fluxum-Session`
    /// entry (SPEC-006 §3; the router core is transport-independent).
    pub fn with_state(ctx: Arc<ShardContext>, state: SessionState) -> Self {
        Self { ctx, state }
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
                return self.authenticate(auth.id, &auth.token);
            }
            return Routed::reply(error(
                Some(request_id(&message)),
                codes::AUTH_REQUIRED,
                "unauthenticated",
            ));
        }

        match message {
            // A second Authenticate re-derives identity but keeps the
            // connection id (idempotent re-auth).
            ClientMessage::Authenticate(auth) => self.authenticate(auth.id, &auth.token),
            ClientMessage::ReducerCall(call) => {
                self.reducer_call(call.id, call.reducer, call.args).await
            }
            ClientMessage::Subscribe(sub) => self.subscribe(sub.id, sub.queries).await,
            ClientMessage::SubscribeSingle(sub) => self.subscribe(sub.id, vec![sub.query]).await,
            ClientMessage::Unsubscribe(unsub) => self.unsubscribe(unsub.query_ids).await,
            ClientMessage::OneOffQuery(query) => self.one_off_query(query.id, query.sql).await,
        }
    }

    /// AUTH-020/021: validate the token, derive the identity, allocate a
    /// `ConnectionId`, and reply `AuthResult`. A failure is a `401` with the
    /// connection kept open.
    fn authenticate(&mut self, id: u32, token: &[u8]) -> Routed {
        let outcome = match self.ctx.authenticator.authenticate(token) {
            Ok(outcome) => outcome,
            Err(e) => {
                // OBS-040: a rejected authentication.
                self.ctx.metrics().note_auth(false);
                return Routed::reply(from_error(Some(id), &e));
            }
        };
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
    async fn reducer_call(&self, id: u32, reducer: String, args: Vec<FluxValue>) -> Routed {
        let (caller, _, _) = self.authed();
        match self.ctx.engine.call(caller, &reducer, args).await {
            Ok(receipt) => Routed {
                responses: vec![ServerMessage::ReducerResult(ReducerResult {
                    id,
                    outcome: Ok(()),
                })],
                commit: Some(receipt.diff),
            },
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
    async fn subscribe(&self, id: u32, queries: Vec<String>) -> Routed {
        let (_, subscriber, connection) = self.authed();
        let snapshot = self.ctx.store().snapshot();
        let mut manager = self.ctx.subscriptions.lock().await;
        let mut responses = Vec::with_capacity(queries.len());
        for sql in queries {
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
        let mut manager = self.ctx.subscriptions.lock().await;
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
        let (_, subscriber, _) = self.authed();
        let snapshot = match fluxum_core::sql::as_of_point(&sql) {
            Ok(Some(point)) => match self.ctx.store().snapshot_as_of(point) {
                Ok(snapshot) => snapshot,
                Err(e) => return Routed::reply(from_error(Some(id), &e)),
            },
            Ok(None) => self.ctx.store().snapshot(),
            Err(e) => return Routed::reply(from_error(Some(id), &e)),
        };
        let manager = self.ctx.subscriptions.lock().await;
        match manager.snapshot_result(subscriber, &sql, &snapshot) {
            Ok(mut initial) => {
                initial.id = id;
                Routed::reply(ServerMessage::InitialData(initial))
            }
            Err(e) => Routed::reply(from_error(Some(id), &e)),
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

/// The `id` a client message carries (echoed on its response, RPC-002).
fn request_id(message: &ClientMessage) -> u32 {
    match message {
        ClientMessage::Authenticate(m) => m.id,
        ClientMessage::ReducerCall(m) => m.id,
        ClientMessage::Subscribe(m) => m.id,
        ClientMessage::SubscribeSingle(m) => m.id,
        ClientMessage::Unsubscribe(m) => m.id,
        ClientMessage::OneOffQuery(m) => m.id,
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
