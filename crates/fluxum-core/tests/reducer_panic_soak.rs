//! T3.3 panic-isolation soak (SPEC-004 RED-061 acceptance 4; FR-25): a
//! deliberately panicking reducer called 10,000 times returns an
//! internal-error result on every call while interleaved healthy calls keep
//! succeeding; the process never exits and memory stays stable.
//!
//! Own test binary: the soak silences the global panic hook (10,000 caught
//! panics would otherwise spam stderr), which would eat other tests'
//! assertion messages if they shared the process.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerCaller, ReducerContext, ReducerDef, ReducerEngine,
    ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_core::{FluxumError, Result};

const SHARD: u32 = 11;
const PANICS: u64 = 10_000;
const HEALTHY_EVERY: u64 = 50;

static COUNTER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::U64,
    },
];

static COUNTER: TableSchema = TableSchema {
    name: "Counter",
    columns: COUNTER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Counter {
    id: u64,
    value: u64,
}

impl Table for Counter {
    type Pk = u64;

    const SCHEMA: &'static TableSchema = &COUNTER;

    fn primary_key(&self) -> u64 {
        self.id
    }

    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::U64(self.value)]
    }

    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::U64(value)] => Ok(Self {
                id: *id,
                value: *value,
            }),
            other => Err(FluxumError::Storage(format!(
                "Counter: unexpected row shape {other:?}"
            ))),
        }
    }

    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn buggy(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    // Buffer a write, then hit the module author's bug: an out-of-bounds
    // index — a representative unwinding panic (RED-061). black_box keeps
    // rustc's unconditional-panic lint from rejecting the deliberate bug.
    ctx.tx.insert(Counter { id: 1, value: 1 })?;
    let empty: [u64; 0] = [];
    let _ = empty[std::hint::black_box(0usize)];
    Ok(())
}

fn bump(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    let current = ctx
        .tx
        .query_pk::<Counter>(0)?
        .map_or(0, |counter| counter.value);
    ctx.tx.upsert(Counter {
        id: 0,
        value: current + 1,
    })?;
    Ok(())
}

fn check_none(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static BUGGY: ReducerDef = ReducerDef {
    name: "buggy",
    handler: buggy,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};
static BUMP: ReducerDef = ReducerDef {
    name: "bump",
    handler: bump,
    check_args: check_none,
    client_callable: true,
    max_rate_per_sec: 0,
};

/// Resident set size of this process in bytes.
fn rss_bytes() -> u64 {
    let pid = sysinfo::get_current_pid().unwrap();
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map_or(0, sysinfo::Process::memory)
}

#[tokio::test(flavor = "multi_thread")]
async fn ten_thousand_panics_leave_the_shard_serving_and_memory_stable() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_tables([&COUNTER]).unwrap();
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
        Arc::new(ReducerRegistry::from_defs([&BUGGY, &BUMP]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("soak"),
    );
    let caller = ReducerCaller {
        identity: Identity::from_bytes([7; 32]),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::now(),
        shard_id: SHARD,
    };

    // 10,000 caught panics print nothing (they are expected); restored at
    // the end so a failing assertion in THIS test still reports.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let mut healthy = 0u64;
    let mut rss_after_warmup = 0u64;
    for i in 0..PANICS {
        let err = engine
            .call(caller, "buggy", vec![])
            .await
            .expect_err("the buggy reducer must fail every time");
        assert_eq!(err.query_code(), Some(500), "internal error result: {err}");

        if i % HEALTHY_EVERY == 0 {
            engine
                .call(caller, "bump", vec![])
                .await
                .expect("interleaved healthy calls keep succeeding (RED-061)");
            healthy += 1;
        }
        if i == 999 {
            rss_after_warmup = rss_bytes();
        }
    }
    std::panic::set_hook(default_hook);

    // Every healthy call committed; the panicking ones left no trace.
    let snapshot = store.snapshot();
    let table = store.table_id("Counter").unwrap();
    let row = snapshot
        .query_pk(table, &[RowValue::U64(0)])
        .unwrap()
        .expect("healthy counter row exists");
    assert_eq!(row.value(1), Some(&RowValue::U64(healthy)));
    assert!(
        snapshot
            .query_pk(table, &[RowValue::U64(1)])
            .unwrap()
            .is_none(),
        "the buggy reducer's buffered insert never became visible"
    );

    // Memory stability: RSS growth over the last 9,000 panic/rollback
    // cycles stays bounded (leaked TxStates or panic payloads would grow
    // linearly — ~9k × row buffers would blow well past this).
    let rss_at_end = rss_bytes();
    let growth = rss_at_end.saturating_sub(rss_after_warmup);
    assert!(
        growth < 64 * 1024 * 1024,
        "RSS grew {growth} bytes across 9k panic cycles (warmup {rss_after_warmup}, \
         end {rss_at_end})"
    );
}
