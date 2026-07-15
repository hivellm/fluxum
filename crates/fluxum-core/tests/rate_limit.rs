//! T3.5 rate-limit conformance suite (SPEC-004 RED-050..RED-052 acceptance
//! 7/8; FR-24; DAG exit test): a 10-call burst against `max_rate = "5/s"`
//! yields 5 accepted calls and 5 rejections with code 429 and zero
//! `TxState`/commit-log cost; buckets are independent per `(Identity,
//! reducer)`; refill restores capacity; server identities are never
//! limited; load above `shard_max_reducers_per_sec` answers 503 on the
//! excess calls only.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions, replay};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, RateLimiter, RateLimiterOptions, ReducerCaller, ReducerContext,
    ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};

const SHARD: u32 = 23;

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
    name: "ChatMessage",
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
struct ChatMessage {
    id: u64,
    text: String,
}

impl Table for ChatMessage {
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
            other => Err(FluxumError::Storage(format!(
                "ChatMessage: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn send_chat(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(ChatMessage {
        id: 0,
        text: "hi".into(),
    })?;
    Ok(())
}

fn check_none(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static SEND_CHAT: ReducerDef = ReducerDef {
    name: "send_chat",
    handler: send_chat,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 5, // the RED-050 conformance rate
};
static RENAME_USER: ReducerDef = ReducerDef {
    name: "rename_user",
    handler: send_chat,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 5,
};
static UNLIMITED: ReducerDef = ReducerDef {
    name: "unlimited",
    handler: send_chat,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn engine(dir: &Path, limiter: Option<RateLimiter>) -> ReducerEngine {
    let schema = Schema::from_tables([&CHAT]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) = TxPipeline::new(store, log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let registry =
        Arc::new(ReducerRegistry::from_defs([&SEND_CHAT, &RENAME_USER, &UNLIMITED]).unwrap());
    let engine = ReducerEngine::new(
        pipeline,
        registry,
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("rate-test"),
    );
    match limiter {
        Some(limiter) => engine.with_rate_limiter(limiter),
        None => engine,
    }
}

fn caller(seed: u8) -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([seed; 32]),
        connection_id: ConnectionId::new(u128::from(seed)),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

fn logged_records(dir: &Path) -> usize {
    let mut count = 0usize;
    let report = replay(&dir.join("log"), SHARD, |_, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert!(report.corruption.is_none());
    count
}

// --- Acceptance 7 (DAG exit): 10-call burst vs "5/s" --------------------------

#[tokio::test(flavor = "multi_thread")]
async fn burst_of_ten_against_five_per_second_accepts_five_rejects_five() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), None);
    let ana = caller(1);

    let mut accepted = 0;
    let mut rejected = 0;
    let mut last_tx = 0;
    for _ in 0..10 {
        match engine.call(ana, "send_chat", vec![]).await {
            Ok(receipt) => {
                accepted += 1;
                last_tx = receipt.tx_id;
            }
            Err(e) => {
                assert_eq!(e.query_code(), Some(429), "{e}");
                assert!(e.to_string().contains("rate limit"), "{e}");
                rejected += 1;
            }
        }
    }
    assert_eq!((accepted, rejected), (5, 5), "RED-050 conformance");

    // Zero TxState cost on the reject path: exactly the 5 accepted commits
    // reached the log, and the tx-id sequence is gap-free.
    engine.pipeline().log().wait_durable(last_tx).await.unwrap();
    assert_eq!(logged_records(dir.path()), 5);
    assert_eq!(last_tx, 5);
}

// --- Acceptance 7 tail: independence, refill, server exemption -----------------

#[tokio::test(flavor = "multi_thread")]
async fn buckets_are_independent_and_refill_restores_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), None);
    let ana = caller(1);
    let bo = caller(2);

    for _ in 0..5 {
        engine.call(ana, "send_chat", vec![]).await.unwrap();
    }
    let err = engine.call(ana, "send_chat", vec![]).await.unwrap_err();
    assert_eq!(err.query_code(), Some(429), "{err}");

    // Same identity, different reducer: independent bucket.
    engine.call(ana, "rename_user", vec![]).await.unwrap();
    // Different identity, same reducer: independent bucket.
    engine.call(bo, "send_chat", vec![]).await.unwrap();
    // Undeclared rate: never limited.
    for _ in 0..20 {
        engine.call(ana, "unlimited", vec![]).await.unwrap();
    }

    // Refill: 5/s = one token per 200 ms.
    tokio::time::sleep(Duration::from_millis(450)).await;
    engine
        .call(ana, "send_chat", vec![])
        .await
        .expect("refill restores capacity (RED-051)");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_identities_are_never_rate_limited() {
    let dir = tempfile::tempdir().unwrap();
    let peer = fluxum_core::auth::server_identity("backend-service");
    let engine = engine(
        dir.path(),
        Some(RateLimiter::new(
            RateLimiterOptions::default(),
            [peer], // AUTH-062: registered server peer
        )),
    );
    let server_caller = ReducerCaller {
        identity: peer,
        connection_id: ConnectionId::new(9),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    };
    for _ in 0..25 {
        engine
            .call(server_caller, "send_chat", vec![])
            .await
            .unwrap();
    }
}

// --- Acceptance 8: shard overload guard (RED-052) -------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn load_above_the_shard_cap_answers_503_on_the_excess_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(
        dir.path(),
        Some(RateLimiter::new(
            RateLimiterOptions {
                shard_max_reducers_per_sec: 4,
            },
            [],
        )),
    );

    let mut ok = 0;
    let mut overloaded = 0;
    let mut last_tx = 0;
    for seed in 0..8u8 {
        // Distinct identities on the unlimited reducer: only the global
        // guard is in play.
        match engine.call(caller(seed), "unlimited", vec![]).await {
            Ok(receipt) => {
                ok += 1;
                last_tx = receipt.tx_id;
            }
            Err(e) => {
                assert_eq!(e.query_code(), Some(503), "{e}");
                assert!(e.to_string().contains("shard overloaded"), "{e}");
                overloaded += 1;
            }
        }
    }
    assert_eq!((ok, overloaded), (4, 4), "excess calls only (RED-052)");
    engine.pipeline().log().wait_durable(last_tx).await.unwrap();
    assert_eq!(logged_records(dir.path()), 4, "zero cost for the 503s");
}
