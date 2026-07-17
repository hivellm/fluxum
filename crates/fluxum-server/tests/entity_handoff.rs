//! SPEC-007 §6 (T5.5 exit) — entity handoff: the 11-step atomic protocol
//! (SHD-041), fault injection at each step with retry-or-abort (SHD-042),
//! exactly-once queued calls during migration (SHD-044), and the two-shard
//! zero-loss move with a byte-identical FluxBIN row set (acceptance 1.5).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::shard::{
    HANDOFF_TABLE, PartitionStrategy, ShardId, ShardRouter, encode_entity_key,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_server::ShardContext;
use fluxum_server::shard::{HandoffOptions, ShardCoord, ShardHost};

// --- Schema: a three-table partition domain keyed by `owner` ----------------------

static ACCOUNT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "owner",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "balance",
        ty: FluxType::U64,
    },
];
static ACCOUNT: TableSchema = TableSchema {
    name: "Account",
    columns: ACCOUNT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: Some(0),
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "label",
        ty: FluxType::Str,
    },
];
static ITEM: TableSchema = TableSchema {
    name: "Item",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: Some(1),
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static LEDGER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::I64,
    },
];
static LEDGER: TableSchema = TableSchema {
    name: "Ledger",
    columns: LEDGER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: Some(1),
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct Account {
    owner: i64,
    balance: u64,
}
impl Table for Account {
    type Pk = i64;
    const SCHEMA: &'static TableSchema = &ACCOUNT;
    fn primary_key(&self) -> i64 {
        self.owner
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::I64(self.owner), RowValue::U64(self.balance)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::I64(owner), RowValue::U64(balance)] => Ok(Self {
                owner: *owner,
                balance: *balance,
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &i64) -> Vec<RowValue> {
        vec![RowValue::I64(*pk)]
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Item {
    id: u64,
    owner: i64,
    label: String,
}
impl Table for Item {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &ITEM;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![
            RowValue::U64(self.id),
            RowValue::I64(self.owner),
            RowValue::Str(self.label),
        ]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [
                RowValue::U64(id),
                RowValue::I64(owner),
                RowValue::Str(label),
            ] => Ok(Self {
                id: *id,
                owner: *owner,
                label: label.clone(),
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Ledger {
    id: u64,
    owner: i64,
}
impl Table for Ledger {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &LEDGER;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::I64(self.owner)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::I64(owner)] => Ok(Self {
                id: *id,
                owner: *owner,
            }),
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

// --- Reducers ----------------------------------------------------------------------

/// Seed one entity's whole row set (1 account + 2 items).
fn seed(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let Some(FluxValue::I64(owner)) = args.first() else {
        return Err(fluxum_core::FluxumError::Reducer("seed(owner)".into()));
    };
    ctx.tx.upsert(Account {
        owner: *owner,
        balance: 700,
    })?;
    ctx.tx.upsert(Item {
        id: u64::try_from(*owner).unwrap_or(0) * 10 + 1,
        owner: *owner,
        label: "sword".into(),
    })?;
    ctx.tx.upsert(Item {
        id: u64::try_from(*owner).unwrap_or(0) * 10 + 2,
        owner: *owner,
        label: "shield".into(),
    })?;
    Ok(())
}

/// Record one ledger entry — the exactly-once probe (SHD-044).
fn credit(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let (Some(FluxValue::I64(owner)), Some(FluxValue::I64(id))) = (args.first(), args.get(1))
    else {
        return Err(fluxum_core::FluxumError::Reducer(
            "credit(owner, id)".into(),
        ));
    };
    ctx.tx.insert(Ledger {
        id: u64::try_from(*id).unwrap_or(0),
        owner: *owner,
    })?;
    Ok(())
}

fn nop_check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}

static SEED: ReducerDef = ReducerDef {
    name: "seed",
    handler: seed,
    check_args: nop_check,
    client_callable: true,
    max_rate_per_sec: 0,
};
static CREDIT: ReducerDef = ReducerDef {
    name: "credit",
    handler: credit,
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

fn test_schema() -> Schema {
    Schema::from_tables([&ACCOUNT, &ITEM, &LEDGER, &HANDOFF_TABLE]).unwrap()
}

fn boot_shard(dir: &std::path::Path, shard_id: ShardId) -> ShardHost {
    let schema = test_schema();
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
        Arc::new(ReducerRegistry::from_defs([&SEED, &CREDIT]).unwrap()),
        LifecycleHooks::none(),
        shard_id,
        fluxum_core::auth::server_identity("handoff-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardHost {
        shard_id,
        ctx: ShardContext::new(engine, subs, auth, shard_id, 64),
    }
}

/// Range partitioning: owner < 100 → shard 0, owner ≥ 100 → shard 1.
fn coord(dir: &std::path::Path, shards: u32) -> ShardCoord {
    let schema = Arc::new(test_schema());
    let mut router = ShardRouter::from_schema(&schema, shards);
    let boundaries = vec![(0, 0), (100, 1)];
    router.set_strategy(
        TableId::of("Account"),
        PartitionStrategy::Range {
            boundaries: boundaries.clone(),
        },
        vec![0],
    );
    router.set_strategy(
        TableId::of("Item"),
        PartitionStrategy::Range {
            boundaries: boundaries.clone(),
        },
        vec![1],
    );
    router.set_strategy(
        TableId::of("Ledger"),
        PartitionStrategy::Range { boundaries },
        vec![1],
    );
    let hosts: Vec<ShardHost> = (0..shards).map(|id| boot_shard(dir, id)).collect();
    ShardCoord::new(schema, router, hosts).unwrap()
}

/// The number of committed rows of `table` on `shard`.
fn rows_on(coord: &ShardCoord, shard: ShardId, table: &str) -> usize {
    let store = coord.host(shard).unwrap().store();
    let tid = store.table_id(table).unwrap();
    store.snapshot().scan(tid).unwrap().count()
}

/// Export the entity's FluxBIN row-set buffer straight from a shard's
/// committed state (the acceptance 1.5 byte-identity probe).
async fn export_buffer(coord: &ShardCoord, shard: ShardId, owner: i64) -> Vec<u8> {
    let slot: Arc<std::sync::Mutex<Option<Vec<u8>>>> = Arc::default();
    let domain: Vec<(TableId, Vec<u16>)> = vec![
        (TableId::of("Account"), vec![0]),
        (TableId::of("Item"), vec![1]),
        (TableId::of("Ledger"), vec![1]),
    ];
    let out = Arc::clone(&slot);
    coord
        .host(shard)
        .unwrap()
        .engine
        .pipeline()
        .call(Box::new(move |tx| {
            *out.lock().unwrap() = Some(tx.handoff_export(&domain, &[RowValue::I64(owner)])?);
            Ok(())
        }))
        .await
        .unwrap();
    let taken = slot.lock().unwrap().take();
    taken.unwrap()
}

fn marker_present(coord: &ShardCoord, shard: ShardId, owner: i64) -> bool {
    let key_bytes = encode_entity_key(&[RowValue::I64(owner)]).unwrap();
    let key_hex: String = key_bytes.iter().map(|b| format!("{b:02x}")).collect();
    let store = coord.host(shard).unwrap().store();
    let tid = store.table_id("__handoff__").unwrap();
    store
        .snapshot()
        .query_pk(tid, &[RowValue::Str(key_hex)])
        .unwrap()
        .is_some()
}

// --- 1.1 + 1.5: the two-shard zero-loss move --------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff_moves_the_whole_row_set_with_byte_identical_fluxbin() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2);

    // Seeding owner=150 ON shard 0 misplaces the entity (range says shard
    // 1) — SHD-040 detects the move and the handoff runs before `call`
    // returns.
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    assert_eq!(multi.handoffs_completed(), 1);
    assert_eq!(multi.handoffs_aborted(), 0);

    // Shard 0 lost the entire row set + its marker; shard 1 owns all 3 rows.
    assert_eq!(rows_on(&multi, 0, "Account"), 0);
    assert_eq!(rows_on(&multi, 0, "Item"), 0);
    assert_eq!(rows_on(&multi, 1, "Account"), 1);
    assert_eq!(rows_on(&multi, 1, "Item"), 2);
    assert!(!marker_present(&multi, 0, 150), "marker cleared (step 10)");

    // Byte-identity (acceptance 1.5): the FluxBIN row set on shard 1 equals
    // the row set the same reducer produces on an untouched control shard.
    let control_dir = tempfile::tempdir().unwrap();
    let control = coord(control_dir.path(), 1);
    control
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    assert_eq!(
        control.handoffs_completed(),
        0,
        "single-shard control never migrates"
    );
    let moved = export_buffer(&multi, 1, 150).await;
    let reference = export_buffer(&control, 0, 150).await;
    assert_eq!(moved, reference, "row set is byte-identical FluxBIN");

    // Entities already on their right shard never trigger a handoff.
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(5)])
        .await
        .unwrap();
    assert_eq!(multi.handoffs_completed(), 1);
    assert_eq!(rows_on(&multi, 0, "Account"), 1);
}

// --- 1.2: fault injection at each protocol step (SHD-042) -------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_at_step_5_aborts_with_the_entity_whole_on_the_origin() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2);
    multi.fail_once_at(5);
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    assert_eq!(multi.handoffs_aborted(), 1);
    // The export+marker commit failed atomically: no marker, nothing moved.
    assert_eq!(rows_on(&multi, 0, "Account"), 1);
    assert_eq!(rows_on(&multi, 0, "Item"), 2);
    assert_eq!(rows_on(&multi, 1, "Account"), 0);
    assert!(!marker_present(&multi, 0, 150));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_at_step_7_retries_and_completes() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2);
    multi.fail_once_at(7);
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    // One import attempt failed; the retry (budget 3) landed the entity.
    assert_eq!(multi.handoffs_completed(), 1);
    assert_eq!(multi.handoffs_aborted(), 0);
    assert_eq!(rows_on(&multi, 0, "Account"), 0);
    assert_eq!(rows_on(&multi, 1, "Account"), 1);
    assert_eq!(rows_on(&multi, 1, "Item"), 2);
    assert!(!marker_present(&multi, 0, 150));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn import_budget_exhaustion_aborts_and_keeps_the_entity_on_the_origin() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2).with_handoff_options(HandoffOptions {
        attempts: 1,
        fail_once_at: std::sync::atomic::AtomicI64::new(-1),
    });
    multi.fail_once_at(7);
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    assert_eq!(multi.handoffs_aborted(), 1);
    // SHD-042: the entity is on exactly one shard (the origin), the marker
    // is rolled back, and the target holds nothing.
    assert_eq!(rows_on(&multi, 0, "Account"), 1);
    assert_eq!(rows_on(&multi, 0, "Item"), 2);
    assert_eq!(rows_on(&multi, 1, "Account"), 0);
    assert_eq!(rows_on(&multi, 1, "Item"), 0);
    assert!(!marker_present(&multi, 0, 150));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_at_step_10_retries_the_cleanup_and_completes() {
    let dir = tempfile::tempdir().unwrap();
    let multi = coord(dir.path(), 2);
    multi.fail_once_at(10);
    multi
        .call(0, caller(0), "seed", &[FluxValue::I64(150)])
        .await
        .unwrap();
    assert_eq!(multi.handoffs_completed(), 1);
    // Cleanup eventually succeeded: no duplicate copy, no stale marker.
    assert_eq!(rows_on(&multi, 0, "Account"), 0);
    assert_eq!(rows_on(&multi, 0, "Item"), 0);
    assert_eq!(rows_on(&multi, 1, "Account"), 1);
    assert!(!marker_present(&multi, 0, 150));
}

// --- 1.3: client continuity — queued calls, exactly once (SHD-044) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_arriving_mid_handoff_queue_and_run_exactly_once_on_the_new_owner() {
    let dir = tempfile::tempdir().unwrap();
    let multi = Arc::new(coord(dir.path(), 2));

    // Widen the migration window: stall shard 1's single-writer queue so
    // the import (steps 6–8) waits behind this barrier.
    let stall = {
        let target = Arc::clone(multi.host(1).unwrap());
        tokio::spawn(async move {
            target
                .engine
                .pipeline()
                .call(Box::new(|_tx| {
                    std::thread::sleep(Duration::from_millis(500));
                    Ok(())
                }))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The misplaced seed commits on shard 0, then blocks in the import.
    let seeding = {
        let multi = Arc::clone(&multi);
        tokio::spawn(async move {
            multi
                .call(0, caller(0), "seed", &[FluxValue::I64(150)])
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Mid-handoff: an entity-keyed call must queue, then execute exactly
    // once on the post-handoff owner.
    let receipt = multi
        .call_entity(
            &[RowValue::I64(150)],
            caller(0),
            "credit",
            &[FluxValue::I64(150), FluxValue::I64(9)],
        )
        .await
        .unwrap();
    assert!(receipt.diff.tables.iter().any(|t| !t.inserts.is_empty()));

    seeding.await.unwrap().unwrap();
    stall.await.unwrap().unwrap();
    assert_eq!(multi.handoffs_completed(), 1);

    // Exactly once, on shard 1 (SHD-042/044: no absence, no duplicates).
    assert_eq!(rows_on(&multi, 1, "Ledger"), 1);
    assert_eq!(rows_on(&multi, 0, "Ledger"), 0);
    assert_eq!(rows_on(&multi, 1, "Account"), 1);
    assert_eq!(rows_on(&multi, 0, "Account"), 0);

    // Post-handoff, entity-keyed routing goes straight to the new owner.
    multi
        .call_entity(
            &[RowValue::I64(150)],
            caller(0),
            "credit",
            &[FluxValue::I64(150), FluxValue::I64(10)],
        )
        .await
        .unwrap();
    assert_eq!(rows_on(&multi, 1, "Ledger"), 2);
    assert_eq!(rows_on(&multi, 0, "Ledger"), 0);
}
