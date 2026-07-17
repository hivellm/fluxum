//! SPEC-007 §3/§4/§5 (T5.4 exit) — ShardCoord + ShardHost: multi-shard
//! boot, global-table replication (write gate + sync visibility + no
//! replica log entries), shard independence under panic and saturation,
//! caller affinity, and the drain barrier.
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
use fluxum_core::shard::{ShardId, ShardRouter};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_server::ShardContext;
use fluxum_server::shard::{ShardCoord, ShardHost};

// --- Schema: one global table + one ordinary table -------------------------------

static CFG_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "key",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::U64,
    },
];
static CFG: TableSchema = TableSchema {
    name: "ServerCfg",
    columns: CFG_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Global,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

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
struct Cfg {
    key: String,
    value: u64,
}
impl Table for Cfg {
    type Pk = String;
    const SCHEMA: &'static TableSchema = &CFG;
    fn primary_key(&self) -> String {
        self.key.clone()
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::Str(self.key), RowValue::U64(self.value)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::Str(key), RowValue::U64(value)] => Ok(Self {
                key: key.clone(),
                value: *value,
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &String) -> Vec<RowValue> {
        vec![RowValue::Str(pk.clone())]
    }
}

// --- Reducers ---------------------------------------------------------------------

fn set_cfg(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let (Some(FluxValue::Str(key)), Some(FluxValue::I64(value))) = (args.first(), args.get(1))
    else {
        return Err(fluxum_core::FluxumError::Reducer(
            "set_cfg(key, value)".into(),
        ));
    };
    ctx.tx.upsert(Cfg {
        key: key.clone(),
        value: u64::try_from(*value).unwrap_or(0),
    })?;
    Ok(())
}
fn boom(_ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    panic!("shard-0 reducer exploded");
}
fn nop_check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static SET_CFG: ReducerDef = ReducerDef {
    name: "set_cfg",
    handler: set_cfg,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static BOOM: ReducerDef = ReducerDef {
    name: "boom",
    handler: boom,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn caller(shard: ShardId) -> fluxum_core::reducer::ReducerCaller {
    fluxum_core::reducer::ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: shard,
    }
}

/// Boot one fully-independent shard host (SHD-020).
fn boot_shard(dir: &std::path::Path, shard_id: ShardId) -> ShardHost {
    let schema = Schema::from_tables([&CFG, &NOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.join(format!("shard-{shard_id}")),
            shard_id,
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
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&SET_CFG, &BOOM]).unwrap()),
        LifecycleHooks::none(),
        shard_id,
        fluxum_core::auth::server_identity("shard-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardHost {
        shard_id,
        ctx: ShardContext::new(engine, subs, auth, shard_id, 64),
    }
}

fn coord(dir: &std::path::Path, shards: u32) -> ShardCoord {
    let schema = Arc::new(Schema::from_tables([&CFG, &NOTE]).unwrap());
    let router = ShardRouter::from_schema(&schema, shards);
    let hosts: Vec<ShardHost> = (0..shards).map(|id| boot_shard(dir, id)).collect();
    ShardCoord::new(schema, router, hosts).unwrap()
}

#[tokio::test]
async fn single_and_multi_shard_boot_with_global_replication() {
    let dir = tempfile::tempdir().unwrap();
    // Single-shard boot works (the degenerate deployment).
    let single = coord(dir.path(), 1);
    single
        .call(
            0,
            caller(0),
            "set_cfg",
            &[FluxValue::Str("a".into()), FluxValue::I64(1)],
        )
        .await
        .unwrap();

    // Two shards: a global write on the authoritative shard is readable on
    // the replica BEFORE the call returns (SHD-030).
    let dir2 = tempfile::tempdir().unwrap();
    let multi = coord(dir2.path(), 2);
    assert_eq!(multi.shard_ids().collect::<Vec<_>>(), vec![0, 1]);
    multi
        .call(
            0,
            caller(0),
            "set_cfg",
            &[FluxValue::Str("mode".into()), FluxValue::I64(7)],
        )
        .await
        .unwrap();
    let replica = multi.host(1).unwrap();
    let row = replica
        .store()
        .snapshot()
        .query_pk(
            replica.store().table_id("ServerCfg").unwrap(),
            &[RowValue::Str("mode".into())],
        )
        .unwrap()
        .expect("replicated row visible on shard 1");
    assert_eq!(row.values()[1], RowValue::U64(7));

    // The replica consumed NO tx id (its log is untouched by replication).
    let replica_tx = replica.store().begin();
    assert_eq!(replica_tx.tx_id(), 1, "replica never committed anything");
    drop(replica_tx);

    // SHD-031: a global write attempted on the replica errors.
    let err = multi
        .call(
            1,
            caller(1),
            "set_cfg",
            &[FluxValue::Str("x".into()), FluxValue::I64(1)],
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("authoritative shard"), "{err}");
    // ...and the authoritative copy is unchanged everywhere.
    let auth_row = multi
        .host(0)
        .unwrap()
        .store()
        .snapshot()
        .query_pk(
            multi
                .host(0)
                .unwrap()
                .store()
                .table_id("ServerCfg")
                .unwrap(),
            &[RowValue::Str("mode".into())],
        )
        .unwrap()
        .unwrap();
    assert_eq!(auth_row.values()[1], RowValue::U64(7));
}

#[tokio::test]
async fn shards_are_independent_under_panic_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2);

    // A panicking reducer on shard 0 rolls back there (RED-004/SHD-020)…
    let err = multi.call(0, caller(0), "boom", &[]).await.unwrap_err();
    assert!(err.to_string().contains("exploded"), "{err}");

    // …and shard 1 keeps serving global reads and its own work unaffected.
    multi
        .call(
            0,
            caller(0),
            "set_cfg",
            &[FluxValue::Str("after".into()), FluxValue::I64(1)],
        )
        .await
        .unwrap();
    let replica = multi.host(1).unwrap();
    assert!(
        replica
            .store()
            .snapshot()
            .query_pk(
                replica.store().table_id("ServerCfg").unwrap(),
                &[RowValue::Str("after".into())],
            )
            .unwrap()
            .is_some(),
        "shard 1 unaffected by shard 0's panic"
    );

    // Affinity is deterministic and lands on a registered shard (SHD-011).
    let a = multi.affinity_of(&Identity::from_bytes([1; 32]));
    assert_eq!(a, multi.affinity_of(&Identity::from_bytes([1; 32])));
    assert!(multi.shard_ids().any(|s| s == a));

    // The drain barrier completes: every prior in-flight reducer finished
    // (SHD-061 steps 1–2).
    multi.drain().await.unwrap();
}

#[test]
fn router_places_rows_per_strategy_and_globals_on_the_authority() {
    let schema = Arc::new(Schema::from_tables([&CFG, &NOTE]).unwrap());
    let router = ShardRouter::from_schema(&schema, 4);
    // Unpartitioned table → shard 0 (SHD-004, OQ-6 normative default).
    let note = fluxum_core::store::TableId::of("Note");
    assert_eq!(
        router
            .shard_of_row(
                &schema,
                note,
                &[RowValue::U64(9), RowValue::Str("x".into())]
            )
            .unwrap(),
        0
    );
    // Global table → the authoritative shard regardless of values (SHD-030).
    let cfg = fluxum_core::store::TableId::of("ServerCfg");
    assert_eq!(
        router
            .shard_of_row(&schema, cfg, &[RowValue::Str("k".into()), RowValue::U64(1)])
            .unwrap(),
        router.authoritative_global()
    );
}
