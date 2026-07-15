//! T3.3 macro end-to-end suite (SPEC-004 RED-001/006/010..013/030/060):
//! `#[fluxum::reducer]`, the lifecycle hooks, and `#[fluxum::view]` declared
//! with the real macros register through the link-time registries and run
//! against a real store + pipeline + engine — argument decode glue, the
//! RED-001 pre-transaction argument check, `Err(String)` mapping, presence
//! end to end, and JSON view results.
#![allow(dead_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerEngine, ReducerRegistry,
    ViewContext, ViewRegistry,
};
use fluxum_core::schema::Schema;
use fluxum_core::store::MemStore;
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

const SHARD: u32 = 9;

// --- Application module (what a real Fluxum module looks like) ---------------

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub sent_at: Timestamp,
}

#[fluxum::table(private)]
#[derive(Debug, Clone, PartialEq)]
pub struct OnlineUser {
    #[primary_key]
    pub identity: Identity,
    pub connected_at: Timestamp,
}

#[fluxum::table(private)]
#[derive(Debug, Clone, PartialEq)]
pub struct AppConfig {
    #[primary_key]
    pub id: u32,
    pub motd: String,
}

#[fluxum::reducer]
fn send_message(ctx: &ReducerContext, channel: u32, content: String) -> Result<(), String> {
    if content.is_empty() {
        return Err("empty message".to_string());
    }
    ctx.tx
        .insert(ChatMessage {
            id: 0,
            sender: ctx.identity,
            channel,
            content,
            sent_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::on_init]
fn seed_config(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .insert(AppConfig {
            id: 0,
            motd: "welcome".to_string(),
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::on_shard_start]
fn log_boot(_ctx: &ReducerContext) -> Result<(), String> {
    Ok(())
}

#[fluxum::on_connect]
fn presence_up(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .upsert(OnlineUser {
            identity: ctx.identity,
            connected_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::on_disconnect]
fn presence_down(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .delete::<OnlineUser>(ctx.identity)
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[derive(serde::Serialize)]
pub struct ChannelStats {
    pub channel: u32,
    pub messages: u64,
}

#[fluxum::view]
fn channel_stats(ctx: &ViewContext, channel: u32) -> ChannelStats {
    let messages = ctx
        .tx
        .scan_where::<ChatMessage>(|m| m.channel == channel)
        .map(|rows| rows.len() as u64)
        .unwrap_or(0);
    ChannelStats { channel, messages }
}

// --- Harness -------------------------------------------------------------------

fn engine(dir: &std::path::Path, store: &Arc<MemStore>) -> ReducerEngine {
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_registered().unwrap()),
        LifecycleHooks::from_registered(),
        SHARD,
        fluxum_core::auth::server_identity("macro-e2e"),
    )
}

fn caller(seed: u8) -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([seed; 32]),
        connection_id: ConnectionId::new(u128::from(seed)),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn macro_declared_module_runs_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::assemble().unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let engine = engine(dir.path(), &store);

    // The link-time registries carry every macro-declared item.
    assert!(engine.registry().contains("send_message"));
    let views = ViewRegistry::from_registered().unwrap();
    assert!(views.contains("channel_stats"));

    // Lifecycle: fresh boot seeds config (on_init) and runs on_shard_start.
    let report = engine.start(true).await.unwrap();
    assert_eq!(report.ran_on_init, ["seed_config"]);
    assert_eq!(report.ran_on_shard_start, ["log_boot"]);

    // Presence end to end (RED-011/012, UC-1).
    let ana = caller(0xA1);
    engine
        .client_connected(ana.identity, ana.connection_id)
        .await
        .unwrap();

    // Typed dispatch through the generated decode glue (RED-001).
    engine
        .call(
            ana,
            "send_message",
            vec![FluxValue::I64(7), FluxValue::Str("hello".into())],
        )
        .await
        .unwrap();
    engine
        .call(
            ana,
            "send_message",
            vec![FluxValue::I64(7), FluxValue::Str("again".into())],
        )
        .await
        .unwrap();
    engine
        .call(
            ana,
            "send_message",
            vec![FluxValue::I64(8), FluxValue::Str("other".into())],
        )
        .await
        .unwrap();

    // Err(String) maps verbatim (RED-060) and rolls back.
    let err = engine
        .call(
            ana,
            "send_message",
            vec![FluxValue::I64(7), FluxValue::Str(String::new())],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, fluxum_core::FluxumError::Reducer(m) if m == "empty message"),
        "{err:?}"
    );

    // The generated check rejects arity and type mismatches with no
    // transaction (RED-001).
    let err = engine
        .call(ana, "send_message", vec![FluxValue::I64(7)])
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");
    let err = engine
        .call(
            ana,
            "send_message",
            vec![
                FluxValue::Str("not a channel".into()),
                FluxValue::Str("x".into()),
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");
    assert!(err.to_string().contains("`channel`"), "{err}");

    // The view computes over committed state and serializes to JSON.
    let snapshot = store.snapshot();
    let stats = views
        .dispatch("channel_stats", &snapshot, SHARD, &[FluxValue::I64(7)])
        .unwrap();
    assert_eq!(stats, serde_json::json!({ "channel": 7, "messages": 2 }));

    // Disconnect clears presence.
    engine
        .client_disconnected(ana.identity, ana.connection_id)
        .await
        .unwrap();
    let online = store.table_id("OnlineUser").unwrap();
    assert_eq!(store.snapshot().scan(online).unwrap().count(), 0);
}
