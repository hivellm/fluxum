//! Session router branch coverage (SPEC-006 §4; AUTH-020/021): the
//! sans-socket [`Session`] core driven directly — accessor surface, the
//! pre-auth 401 gate for every message type, authentication failure, re-auth
//! connection-id stability, and the error mapping for unknown reducers and
//! bad SQL — without any transport in the way.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{
    Authenticate, ClientMessage, OneOffQuery, ReducerCall, ServerMessage, Subscribe,
    SubscribeSingle, Unsubscribe,
};
use fluxum_server::ShardContext;
use fluxum_server::session::{Session, SessionState};

const SHARD: u32 = 1;

static CHAT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static CHAT: TableSchema = TableSchema {
    name: "Chat",
    columns: CHAT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct ChatRow {
    id: u64,
    text: String,
}
impl Table for ChatRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &CHAT;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.text)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(text)] => Ok(Self {
                id: *id,
                text: text.clone(),
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn send_chat(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("send_chat(text)".into())),
    };
    ctx.tx.insert(ChatRow { id: 0, text })?;
    Ok(())
}
static SEND_CHAT: ReducerDef = ReducerDef {
    name: "send_chat",
    handler: send_chat,
    check_args: |_| Ok(()),
    client_callable: true,
    max_rate_per_sec: 0,
};

async fn context() -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&SEND_CHAT]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("session-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 16)
}

fn auth_msg(id: u32, token: &[u8]) -> ClientMessage {
    ClientMessage::Authenticate(Authenticate {
        id,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    })
}

// --- Accessors -------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn accessors_reflect_the_session_lifecycle() {
    let ctx = context().await;
    let mut session = Session::new(Arc::clone(&ctx));
    assert!(matches!(session.state(), SessionState::Unauthenticated));
    assert!(!session.is_authenticated());
    assert!(session.caller().is_none());
    assert!(session.connection_id().is_none());

    let routed = session.handle(auth_msg(1, b"alice")).await;
    assert!(matches!(
        routed.responses.first(),
        Some(ServerMessage::AuthResult(_))
    ));
    assert!(matches!(
        session.state(),
        SessionState::Authenticated { .. }
    ));
    assert!(session.is_authenticated());
    let caller = session.caller().expect("caller after auth");
    assert_eq!(caller.shard_id, SHARD);
    assert_eq!(
        session.connection_id(),
        Some(caller.connection_id.as_u128())
    );

    // A resumed session preserves the state it was built from.
    let state = session.into_state();
    let resumed = Session::with_state(ctx, state);
    assert!(resumed.is_authenticated());
}

// --- AUTH-020: pre-auth 401 gate for every message type ---------------------------

#[tokio::test(flavor = "multi_thread")]
async fn every_pre_auth_message_type_is_401_with_its_id_echoed() {
    let ctx = context().await;
    let messages: Vec<(u32, ClientMessage)> = vec![
        (
            10,
            ClientMessage::ReducerCall(ReducerCall {
                id: 10,
                reducer: "send_chat".into(),
                version: None,
                args: vec![],
                idempotency_key: None,
            }),
        ),
        (
            11,
            ClientMessage::Subscribe(Subscribe {
                id: 11,
                queries: vec!["SELECT * FROM Chat".into()],
            }),
        ),
        (
            12,
            ClientMessage::SubscribeSingle(SubscribeSingle {
                id: 12,
                query: "SELECT * FROM Chat".into(),
            }),
        ),
        (
            13,
            ClientMessage::Unsubscribe(Unsubscribe {
                id: 13,
                query_ids: vec![1],
            }),
        ),
        (
            14,
            ClientMessage::OneOffQuery(OneOffQuery {
                id: 14,
                sql: "SELECT * FROM Chat".into(),
            }),
        ),
    ];
    for (id, message) in messages {
        let mut session = Session::new(Arc::clone(&ctx));
        let routed = session.handle(message).await;
        match routed.responses.first() {
            Some(ServerMessage::Error(e)) => {
                assert_eq!(e.code, fluxum_protocol::codes::AUTH_REQUIRED, "id {id}");
                assert_eq!(e.id, Some(id), "the request id echoes (RPC-002)");
            }
            other => panic!("expected 401 Error for id {id}, got {other:?}"),
        }
        assert!(!session.is_authenticated());
    }
}

// --- AUTH-021/060: authentication failure -----------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_token_in_the_reserved_server_namespace_is_rejected() {
    let ctx = context().await;
    let mut session = Session::new(ctx);
    let routed = session.handle(auth_msg(7, b"SERVER:evil")).await;
    match routed.responses.first() {
        Some(ServerMessage::Error(e)) => {
            assert_eq!(e.id, Some(7));
            // `FluxumError::Auth` is not a Query error: SPEC-006 maps it 500.
            assert_eq!(
                e.code,
                fluxum_protocol::codes::AUTH_FAILED,
                "auth failures map to AUTH_FAILED"
            );
            assert!(e.message.contains("authentication failed"), "{}", e.message);
        }
        other => panic!("expected an auth Error, got {other:?}"),
    }
    // The connection stays open and unauthenticated.
    assert!(!session.is_authenticated());
}

// --- Idempotent re-auth keeps the ConnectionId ------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reauth_rederives_identity_but_keeps_the_connection_id() {
    let ctx = context().await;
    let mut session = Session::new(ctx);
    session.handle(auth_msg(1, b"alice")).await;
    let first_conn = session.connection_id().unwrap();
    let first_caller = *session.caller().unwrap();

    let routed = session.handle(auth_msg(2, b"bob")).await;
    match routed.responses.first() {
        Some(ServerMessage::AuthResult(ar)) => assert_eq!(ar.id, 2),
        other => panic!("expected AuthResult, got {other:?}"),
    }
    assert_eq!(
        session.connection_id(),
        Some(first_conn),
        "the connection id survives a re-auth"
    );
    assert_ne!(
        session.caller().unwrap().identity,
        first_caller.identity,
        "the identity re-derives from the new token"
    );
}

// --- SPEC-006 error mapping --------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_reducer_maps_to_an_error_frame() {
    let ctx = context().await;
    let mut session = Session::new(ctx);
    session.handle(auth_msg(1, b"alice")).await;
    let routed = session
        .handle(ClientMessage::ReducerCall(ReducerCall {
            id: 9,
            reducer: "no_such_reducer".into(),
            version: None,
            args: vec![],
            idempotency_key: None,
        }))
        .await;
    match routed.responses.first() {
        Some(ServerMessage::Error(e)) => {
            assert_eq!(e.id, Some(9));
            assert_eq!(
                e.code,
                fluxum_protocol::codes::REDUCER_UNKNOWN,
                "RED-006 unknown reducer"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
    assert!(routed.commit.is_none(), "nothing committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_query_in_a_subscribe_batch_stops_at_the_failure() {
    let ctx = context().await;
    let mut session = Session::new(ctx);
    session.handle(auth_msg(1, b"alice")).await;
    let routed = session
        .handle(ClientMessage::Subscribe(Subscribe {
            id: 4,
            queries: vec![
                "SELECT * FROM Chat".into(),
                "THIS IS NOT SQL".into(),
                "SELECT * FROM Chat".into(),
            ],
        }))
        .await;
    // First query registered, second reported, third never attempted.
    assert_eq!(routed.responses.len(), 2);
    assert!(matches!(routed.responses[0], ServerMessage::InitialData(_)));
    match &routed.responses[1] {
        ServerMessage::Error(e) => {
            assert_eq!(e.id, Some(4));
            assert_eq!(
                e.code,
                fluxum_protocol::codes::SQL_UNSUPPORTED,
                "SQL parse failure"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_one_off_query_is_an_error_frame() {
    let ctx = context().await;
    let mut session = Session::new(ctx);
    session.handle(auth_msg(1, b"alice")).await;
    let routed = session
        .handle(ClientMessage::OneOffQuery(OneOffQuery {
            id: 6,
            sql: "SELECT * FROM Ghost".into(),
        }))
        .await;
    match routed.responses.first() {
        Some(ServerMessage::Error(e)) => {
            assert_eq!(e.id, Some(6));
            assert_eq!(
                e.code,
                fluxum_protocol::codes::SQL_UNKNOWN_TABLE,
                "unknown table"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}
