//! T3.4 macro end-to-end suite (SPEC-004 RED-020/RED-021/RED-025):
//! `#[fluxum::tick]` and `#[fluxum::schedule]` declared with the real macros
//! register through the link-time registries (as schedule-only reducers +
//! `TickDef`/`ScheduleDef`) and run against a real store + pipeline +
//! scheduler — periodic firing, boot-time one-shot, recurring entry, and
//! the 403 / `client_callable` admission split.
#![allow(dead_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerEngine, ReducerRegistry,
};
use fluxum_core::scheduler::{SCHEDULE_TABLE, Scheduler, SchedulerOptions};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::MemStore;
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

const SHARD: u32 = 21;

#[fluxum::table(private)]
#[derive(Debug, Clone, PartialEq)]
pub struct CycleLog {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub label: String,
}

#[fluxum::tick(rate = 50)]
fn heartbeat(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .insert(CycleLog {
            id: 0,
            label: "beat".to_string(),
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::schedule(delay_ms = 30)]
fn boot_probe(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .insert(CycleLog {
            id: 0,
            label: "boot".to_string(),
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::schedule(delay_ms = 40, every_ms = 80, client_callable = true)]
fn sweeper(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .insert(CycleLog {
            id: 0,
            label: "sweep".to_string(),
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn client() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_bytes([0xE7; 32]),
        connection_id: ConnectionId::new(3),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn macro_declared_ticks_and_schedules_run_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_tables([CycleLog::SCHEMA, &SCHEDULE_TABLE]).unwrap();
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

    let registry = Arc::new(ReducerRegistry::from_registered().unwrap());
    assert!(registry.contains("heartbeat"));
    assert!(registry.contains("boot_probe"));
    assert!(registry.contains("sweeper"));

    // RED-025 admission: ticks and schedules are schedule-only unless
    // opted in with client_callable = true.
    let engine = ReducerEngine::new(
        pipeline.clone(),
        Arc::clone(&registry),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("macro-sched"),
    );
    let err = engine
        .call(client(), "heartbeat", vec![])
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_SCHEDULE_ONLY),
        "{err}"
    );
    let err = engine
        .call(client(), "boot_probe", vec![])
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_SCHEDULE_ONLY),
        "{err}"
    );
    engine
        .call(client(), "sweeper", vec![])
        .await
        .expect("client_callable = true admits clients");
    // And the generated zero-arg check still applies to admitted calls.
    let err = engine
        .call(client(), "sweeper", vec![FluxValue::I64(1)])
        .await
        .unwrap_err();
    assert_eq!(
        err.query_code(),
        Some(fluxum_protocol::codes::REDUCER_BAD_ARGS),
        "{err}"
    );

    // Run the link-time scheduler: tick at 50 Hz plus both static defs.
    let scheduler = Scheduler::from_registered(
        pipeline,
        Arc::clone(&registry),
        SHARD,
        fluxum_core::auth::server_identity("macro-sched"),
        SchedulerOptions::default(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(450)).await;
    let stats = scheduler.tick_stats("heartbeat").unwrap();
    let beats_fired = stats.executions.load(std::sync::atomic::Ordering::SeqCst);
    scheduler.stop().await;

    let table = store.table_id("CycleLog").unwrap();
    let snapshot = store.snapshot();
    let labels: Vec<String> = snapshot
        .scan(table)
        .unwrap()
        .map(|row| CycleLog::from_values(row.values()).unwrap().label)
        .collect();

    let beats = labels.iter().filter(|l| *l == "beat").count();
    let boots = labels.iter().filter(|l| *l == "boot").count();
    let sweeps = labels.iter().filter(|l| *l == "sweep").count();
    assert!(
        beats >= 10,
        "50 Hz over ~450 ms fires >=10 times, got {beats}"
    );
    assert!(beats_fired >= 10, "tick stats track firings: {beats_fired}");
    assert_eq!(boots, 1, "one-shot #[fluxum::schedule] fires exactly once");
    assert!(
        (2..=8).contains(&sweeps),
        "recurring 80 ms schedule keeps firing without bursting, got {sweeps}"
    );

    // The recurring entry stays pending; the one-shot's row is gone.
    let sched = store.table_id("__schedule__").unwrap();
    let snapshot = store.snapshot();
    let pending: Vec<String> = snapshot
        .scan(sched)
        .unwrap()
        .map(|row| {
            fluxum_core::scheduler::ScheduleEntry::from_values(row.values())
                .unwrap()
                .reducer_name
        })
        .collect();
    assert_eq!(pending, ["sweeper"], "{pending:?}");
}
