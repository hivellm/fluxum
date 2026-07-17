//! SPEC-025 §4 (OPS-030/031) — graceful drain & rolling restart: entering
//! drain refuses new work with a *retryable* signal while in-flight
//! transactions commit, a final checkpoint leaves restart replaying nothing,
//! and the whole thing is bounded by a deadline.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::checkpoint::{CheckpointRepo, SnapshotWorker, WorkerOptions};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::metrics::ShardState;
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerDef, ReducerEngine,
    ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_protocol::{
    ClientMessage, ReducerCall, ServerMessage, Subscribe, SubscribeSingle, Unsubscribe, codes,
};
use fluxum_server::session::Session;
use fluxum_server::{DrainOptions, ShardContext};

const SHARD: u32 = 0;

static NOTE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static NOTE: TableSchema = TableSchema {
    name: "Note",
    columns: NOTE_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct NoteRow {
    id: u64,
    text: String,
}
impl Table for NoteRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &NOTE;
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

fn add_note(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(NoteRow {
        id: 0,
        text: "n".into(),
    })?;
    Ok(())
}
/// Holds the single writer long enough to still be in flight when the drain
/// starts.
fn slow_note(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    std::thread::sleep(Duration::from_millis(200));
    ctx.tx.insert(NoteRow {
        id: 0,
        text: "slow".into(),
    })?;
    Ok(())
}
fn nop_check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static ADD_NOTE: ReducerDef = ReducerDef {
    name: "add_note",
    handler: add_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static SLOW_NOTE: ReducerDef = ReducerDef {
    name: "slow_note",
    handler: slow_note,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};

struct Harness {
    ctx: Arc<ShardContext>,
    store: Arc<MemStore>,
    snap_dir: std::path::PathBuf,
}

fn boot(dir: &std::path::Path) -> Harness {
    let schema = Schema::from_tables([&NOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&ADD_NOTE, &SLOW_NOTE]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("drain-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    Harness {
        ctx: ShardContext::new(engine, subs, auth, SHARD, 64),
        store,
        snap_dir: dir.join("snap"),
    }
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: SHARD,
    }
}

/// An authenticated session, for routing client messages.
async fn authed_session(ctx: &Arc<ShardContext>) -> Session {
    let mut session = Session::new(Arc::clone(ctx));
    session
        .handle(ClientMessage::Authenticate(fluxum_protocol::Authenticate {
            id: 1,
            token: b"client".to_vec(),
            compression: None,
            tx_updates: None,
        }))
        .await;
    session
}

fn error_code(routed: &fluxum_server::session::Routed) -> Option<u16> {
    routed.responses.iter().find_map(|m| match m {
        ServerMessage::Error(e) => Some(e.code),
        _ => None,
    })
}

// --- OPS-030: new work is refused *retryably*; existing work is not --------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_refuses_new_work_with_a_retryable_signal() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());
    let mut session = authed_session(&h.ctx).await;

    // Before drain, a call is served.
    let routed = session
        .handle(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "add_note".into(),
            version: None,
            args: vec![],
            idempotency_key: None,
        }))
        .await;
    assert!(error_code(&routed).is_none(), "served before drain");

    h.ctx.begin_drain();
    assert!(h.ctx.is_draining());

    // A reducer call started mid-drain gets the retryable signal — not a
    // hard drop — so the SDK retries it against the restarted process.
    let routed = session
        .handle(ClientMessage::ReducerCall(ReducerCall {
            id: 3,
            reducer: "add_note".into(),
            version: None,
            args: vec![],
            idempotency_key: None,
        }))
        .await;
    let code = error_code(&routed).expect("refused");
    assert_eq!(code, codes::CLUSTER_SHARD_UNAVAILABLE);
    let entry = codes::entry(code).unwrap();
    assert!(entry.retryable, "OPS-031: the client must know to retry");
    assert_eq!(entry.http_status, 503);

    // New subscriptions are refused too (both forms).
    for message in [
        ClientMessage::Subscribe(Subscribe {
            id: 4,
            queries: vec!["SELECT * FROM Note".into()],
        }),
        ClientMessage::SubscribeSingle(SubscribeSingle {
            id: 5,
            query: "SELECT * FROM Note".into(),
        }),
    ] {
        assert_eq!(
            error_code(&session.handle(message).await),
            Some(codes::CLUSTER_SHARD_UNAVAILABLE)
        );
    }

    // ...but shedding a client must not break it: unsubscribing still works
    // while draining (refusing it would strand the client).
    let routed = session
        .handle(ClientMessage::Unsubscribe(Unsubscribe {
            id: 6,
            query_ids: vec![],
        }))
        .await;
    assert!(error_code(&routed).is_none(), "unsubscribe still served");

    // The shard reports itself shutting down (OBS-050), so /health goes 503
    // and a load balancer takes it out of rotation.
    assert_eq!(h.ctx.metrics().shard_state(), ShardState::ShuttingDown);
}

// --- OPS-030: in-flight transactions commit; the drain quiesces and checkpoints ---

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_flight_call_commits_and_the_drain_checkpoints_it() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    // A call is in flight (the writer is busy for ~200 ms)...
    let engine_ctx = Arc::clone(&h.ctx);
    let in_flight =
        tokio::spawn(async move { engine_ctx.engine.call(caller(), "slow_note", vec![]).await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    // ...when the deploy drains. The drain must not cut it off.
    let repo = Arc::new(CheckpointRepo::open(&h.snap_dir).unwrap());
    let cp = SnapshotWorker::spawn(
        Arc::clone(&h.store),
        Arc::clone(&repo),
        SHARD,
        WorkerOptions::default(),
    )
    .unwrap();

    let receipt_task = tokio::spawn(async move { in_flight.await.unwrap() });
    let report = fluxum_server::drain(&h.ctx, Some(&cp), DrainOptions::default())
        .await
        .unwrap();

    // The in-flight call committed (OPS-030: "that call commits").
    let receipt = receipt_task
        .await
        .unwrap()
        .expect("the in-flight call committed");
    assert!(report.quiesced, "the drain waited for it");
    assert_eq!(
        h.store
            .snapshot()
            .scan(h.store.table_id("Note").unwrap())
            .unwrap()
            .count(),
        1,
        "the write survived the drain"
    );

    // The final checkpoint covers it, so restart replays nothing. The stamp
    // is *past* the call's own tx id because the drain's quiesce barrier is
    // itself the shard's last commit — the checkpoint covers the whole log
    // tail, which is the point.
    let checkpointed = report.checkpoint_tx_id.expect("a checkpoint was taken");
    assert!(
        checkpointed >= receipt.tx_id,
        "checkpoint {checkpointed} must cover the in-flight commit {}",
        receipt.tx_id
    );
    assert_eq!(
        checkpointed, report.last_tx_id,
        "the checkpoint reaches the durable tail, so restart replays nothing"
    );
    cp.close().unwrap();
}

// --- OPS-030: the drain is bounded; a straggler cannot hang the deploy ------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_straggler_past_the_deadline_does_not_hang_the_drain() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());

    // A call that outlives the deadline.
    let engine_ctx = Arc::clone(&h.ctx);
    tokio::spawn(async move {
        let _ = engine_ctx.engine.call(caller(), "slow_note", vec![]).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    // A deadline far shorter than the straggler: the drain reports the
    // failure to quiesce rather than blocking the deploy forever.
    let report = fluxum_server::drain(
        &h.ctx,
        None,
        DrainOptions {
            deadline: Duration::from_millis(20),
        },
    )
    .await
    .unwrap();
    assert!(!report.quiesced, "reported, not hung");
    assert_eq!(report.checkpoint_tx_id, None, "no checkpoint requested");
    // Draining is still in effect — the shard sheds new work regardless.
    assert!(h.ctx.is_draining());
}

// --- Drain with nothing in flight is immediate and idempotent --------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_an_idle_shard_is_immediate_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let h = boot(dir.path());
    h.ctx
        .engine
        .call(caller(), "add_note", vec![])
        .await
        .unwrap();

    let report = fluxum_server::drain(&h.ctx, None, DrainOptions::default())
        .await
        .unwrap();
    assert!(report.quiesced);

    // A second drain (a retried pre-stop hook) is harmless. Its tx id moves
    // on because each drain's quiesce barrier is itself a commit — drain is
    // idempotent in effect, not a no-op on the log.
    let again = fluxum_server::drain(&h.ctx, None, DrainOptions::default())
        .await
        .unwrap();
    assert!(again.quiesced);
    assert!(again.last_tx_id >= report.last_tx_id);
    assert!(h.ctx.is_draining());
}
