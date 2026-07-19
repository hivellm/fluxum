//! SPEC-025 §3 (OPS-020/021) — the admin audit trail over the commit log:
//! a row changed by three reducer calls yields exactly those three entries
//! with the right caller/reducer/tx order, a non-server-peer is refused, and
//! the metadata-only result never carries a column value (so a masked column
//! cannot leak plaintext).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::ServerPeer;
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};
use fluxum_server::ShardContext;
use fluxum_server::admin;
use serde_json::{Value, json};

const SHARD: u32 = 4;
const PEER_TOKEN: &str = "ops-secret";

// --- Order table + reducers -------------------------------------------------------

static ORDER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "note",
        ty: FluxType::Str,
    },
];
static ORDER: TableSchema = TableSchema {
    name: "Order",
    columns: ORDER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Order {
    id: u64,
    note: String,
}

impl Table for Order {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &ORDER;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.note)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(note)] => Ok(Self {
                id: *id,
                note: note.clone(),
            }),
            other => Err(FluxumError::Storage(format!("Order: bad shape {other:?}"))),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn order_args(args: &[FluxValue]) -> Result<(u64, String)> {
    match args {
        [FluxValue::I64(id), FluxValue::Str(note)] => Ok((*id as u64, note.clone())),
        _ => Err(FluxumError::Reducer("place(id: u64, note: String)".into())),
    }
}

fn place_order(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let (id, note) = order_args(args)?;
    ctx.tx.insert(Order { id, note })?;
    Ok(())
}

fn relabel_order(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    // A delete+insert in one transaction: an "update" that touches the row
    // twice, so its audit entry is both inserted and deleted.
    let (id, note) = order_args(args)?;
    ctx.tx.delete::<Order>(id)?;
    ctx.tx.insert(Order { id, note })?;
    Ok(())
}

fn cancel_order(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let id = match args.first() {
        Some(FluxValue::I64(id)) => *id as u64,
        _ => return Err(FluxumError::Reducer("cancel(id: u64)".into())),
    };
    ctx.tx.delete::<Order>(id)?;
    Ok(())
}

fn check_place(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("place", args, 2)
}
fn check_cancel(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("cancel", args, 1)
}

static PLACE: ReducerDef = ReducerDef {
    name: "place_order",
    handler: place_order,
    check_args: check_place,
    client_callable: true,
    max_rate_per_sec: 0,
};
static RELABEL: ReducerDef = ReducerDef {
    name: "relabel_order",
    handler: relabel_order,
    check_args: check_place,
    client_callable: true,
    max_rate_per_sec: 0,
};
static CANCEL: ReducerDef = ReducerDef {
    name: "cancel_order",
    handler: cancel_order,
    check_args: check_cancel,
    client_callable: true,
    max_rate_per_sec: 0,
};

// --- Harness ----------------------------------------------------------------------

struct Harness {
    ctx: Arc<ShardContext>,
    log: Arc<CommitLog>,
}

async fn start() -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&ORDER]).unwrap();
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
    let (pipeline, worker) = TxPipeline::new(
        Arc::clone(&store),
        Arc::clone(&log),
        TxPipelineOptions::default(),
    )
    .unwrap();
    tokio::spawn(worker.run());
    let registry = Arc::new(ReducerRegistry::from_defs([&PLACE, &RELABEL, &CANCEL]).unwrap());
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("audit-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let peers = ServerPeerRegistry::from_config(&[ServerPeer {
        name: "ops".into(),
        token: PEER_TOKEN.into(),
    }])
    .unwrap();
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), peers);
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);
    Harness { ctx, log }
}

fn caller(token: &[u8]) -> fluxum_core::reducer::ReducerCaller {
    fluxum_core::reducer::ReducerCaller {
        identity: Identity::from_token(token),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

async fn audit(ctx: &Arc<ShardContext>, body: Value) -> (u16, Value) {
    let body = serde_json::to_vec(&body).unwrap();
    let resp = admin::dispatch(ctx, admin::AdminRequest::local("POST", "/audit", &body)).await;
    (resp.status, resp.body)
}

// --- 1.7 acceptance: a row's history ----------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_rows_three_changes_are_traced_in_commit_order() {
    let h = start().await;
    let alice = caller(b"alice");
    let bob = caller(b"bob");

    // Three commits touch order 1; a fourth touches order 2 (must be excluded).
    h.ctx
        .engine
        .call(
            alice,
            "place_order",
            vec![FluxValue::I64(1), FluxValue::Str("first".into())],
        )
        .await
        .unwrap();
    h.ctx
        .engine
        .call(
            bob,
            "relabel_order",
            vec![FluxValue::I64(1), FluxValue::Str("TOPSECRET".into())],
        )
        .await
        .unwrap();
    h.ctx
        .engine
        .call(
            alice,
            "place_order",
            vec![FluxValue::I64(2), FluxValue::Str("other".into())],
        )
        .await
        .unwrap();
    let last = h
        .ctx
        .engine
        .call(alice, "cancel_order", vec![FluxValue::I64(1)])
        .await
        .unwrap();
    // Audit reads durable segments — wait for the tail to flush.
    h.log.wait_durable(last.tx_id).await.unwrap();

    let (status, body) = audit(
        &h.ctx,
        json!({ "token": PEER_TOKEN, "table": "Order", "pk": [1] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let entries = body["payload"]["entries"].as_array().unwrap();

    let names: Vec<&str> = entries
        .iter()
        .map(|e| e["reducer_name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["place_order", "relabel_order", "cancel_order"],
        "exactly the three calls that touched order 1, in commit order (the order-2 place is excluded)"
    );
    // tx ids ascend; caller is the committing identity.
    assert_eq!(
        entries[0]["caller"],
        Identity::from_token(b"alice").to_string()
    );
    assert_eq!(
        entries[1]["caller"],
        Identity::from_token(b"bob").to_string()
    );
    // The relabel deleted then re-inserted the row; the cancel only deleted.
    assert_eq!(entries[1]["inserted"], true);
    assert_eq!(entries[1]["deleted"], true);
    assert_eq!(entries[2]["inserted"], false);
    assert_eq!(entries[2]["deleted"], true);

    // OPS-021: the audit is metadata only — a column value (even a would-be
    // masked one) never appears in the result.
    assert!(
        !body.to_string().contains("TOPSECRET"),
        "audit output must not carry column values: {body}"
    );
}

// --- OPS-021 access control -------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_non_server_peer_is_refused() {
    let h = start().await;

    // A plain client token authenticates (NoneProvider accepts it) but is not
    // a server peer → 403.
    let (status, _) = audit(
        &h.ctx,
        json!({ "token": "just-a-client", "table": "Order", "pk": [1] }),
    )
    .await;
    assert_eq!(status, 403, "a client identity cannot audit (OPS-021)");

    // No credential at all → 401.
    let (status, _) = audit(&h.ctx, json!({ "table": "Order" })).await;
    assert_eq!(status, 401);

    // The server-peer credential is accepted.
    let (status, _) = audit(&h.ctx, json!({ "token": PEER_TOKEN, "table": "Order" })).await;
    assert_eq!(status, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_table_is_404() {
    let h = start().await;
    let (status, _) = audit(&h.ctx, json!({ "token": PEER_TOKEN, "table": "Ghost" })).await;
    assert_eq!(status, 404);
}
