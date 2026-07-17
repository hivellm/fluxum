//! SPEC-026 SEC-020/021 — the determinism-preserving reducer stdlib: the
//! transaction RNG (`ctx.rng()`) reproduces its sequence for the same
//! `(tx_id, shard_id)`, one stream is shared across a nested reducer call, and
//! the logical-time helpers bucket `ctx.timestamp` without the wall clock.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fluxum_core::ReducerContext;
use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, handler, with_context};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};

static COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
static T: TableSchema = TableSchema {
    name: "T",
    columns: COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn new_store() -> MemStore {
    MemStore::new(&Schema::from_tables([&T]).unwrap()).unwrap()
}

fn caller(shard: u32, ts_micros: i64) -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("tester"),
        connection_id: ConnectionId::new(7),
        timestamp: Timestamp::from_micros(ts_micros),
        shard_id: shard,
    }
}

/// Draw `n` u64s from a fresh transaction's RNG on a fresh store (so the tx id
/// is 1) at shard `shard`.
fn draw(shard: u32, n: usize) -> Vec<u64> {
    let store = new_store();
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    let out = with_context(&registry, caller(shard, 0), &mut tx, |ctx| {
        Ok((0..n).map(|_| ctx.rng().next_u64()).collect::<Vec<_>>())
    })
    .unwrap();
    tx.commit().unwrap();
    out
}

/// SEC-020: the same `(tx_id, shard_id)` reproduces the identical sequence,
/// and a different shard diverges.
#[test]
fn rng_is_reproducible_per_transaction() {
    assert_eq!(draw(5, 32), draw(5, 32), "same tx_id + shard ⇒ same stream");
    assert_ne!(
        draw(5, 32),
        draw(6, 32),
        "different shard ⇒ different stream"
    );
}

/// SEC-020: successive transactions on the same store draw different streams
/// (the tx id advances), but each is reproducible on replay.
#[test]
fn successive_transactions_advance_the_seed() {
    let store = new_store();
    let registry = ReducerRegistry::new();
    let run = |store: &MemStore| -> Vec<u64> {
        let mut tx = store.begin();
        let out = with_context(&registry, caller(5, 0), &mut tx, |ctx| {
            Ok((0..8).map(|_| ctx.rng().next_u64()).collect::<Vec<_>>())
        })
        .unwrap();
        tx.commit().unwrap();
        out
    };
    let first = run(&store);
    let second = run(&store);
    assert_ne!(first, second, "tx 1 vs tx 2 differ");

    // Replaying tx 1 and tx 2 on a fresh store reproduces both exactly.
    let replay = new_store();
    assert_eq!(run(&replay), first);
    assert_eq!(run(&replay), second);
}

/// SEC-020: a nested reducer call draws from the **same** transaction RNG
/// stream as its parent — the child's value is exactly the parent's next
/// draw, and the whole thing is reproducible through the nested call.
#[test]
fn nested_calls_share_one_reproducible_stream() {
    static CHILD_DRAW: AtomicU64 = AtomicU64::new(0);
    let mut registry = ReducerRegistry::new();
    registry
        .register(
            "child",
            handler(|ctx, _args| {
                CHILD_DRAW.store(ctx.rng().next_u64(), Ordering::SeqCst);
                Ok(())
            }),
        )
        .unwrap();

    // Parent: draw v0, call the child (which draws from the same stream),
    // then draw v2.
    let store = new_store();
    let mut tx = store.begin();
    let (v0, v2) = with_context(&registry, caller(9, 0), &mut tx, |ctx| {
        let v0 = ctx.rng().next_u64();
        ctx.tx.call("child", &[])?;
        let v2 = ctx.rng().next_u64();
        Ok((v0, v2))
    })
    .unwrap();
    tx.commit().unwrap();
    let v1 = CHILD_DRAW.load(Ordering::SeqCst);

    // The three draws (parent, child, parent) are the first three values of
    // one shared stream on a fresh tx — proven by an uninterrupted 3-draw run.
    let plain = draw(9, 3);
    assert_eq!(
        vec![v0, v1, v2],
        plain,
        "the nested child draws the middle value of the shared stream"
    );
    assert!(v0 != v1 && v1 != v2, "each draw advances the stream");
}

/// SEC-020: logical-time helpers bucket `ctx.timestamp` deterministically and
/// never read the wall clock; negative (pre-epoch) timestamps floor correctly.
#[test]
fn logical_time_buckets_are_deterministic() {
    let store = new_store();
    let registry = ReducerRegistry::new();
    let minute = Duration::from_secs(60);
    // 09:30:45 past a minute boundary → floors to 09:30:00.
    let ts = 1_000 * 60 * 1_000_000 + 45 * 1_000_000; // 1000 min + 45 s, in µs
    let mut tx = store.begin();
    with_context(&registry, caller(5, ts), &mut tx, |ctx: &ReducerContext| {
        assert_eq!(ctx.time_bucket(minute), 1_000 * 60 * 1_000_000);
        assert_eq!(ctx.bucket_index(minute), 1_000);
        // Zero interval is a no-op / zero.
        assert_eq!(ctx.time_bucket(Duration::ZERO), ts);
        assert_eq!(ctx.bucket_index(Duration::ZERO), 0);
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // Pre-epoch timestamp floors toward negative infinity (not toward zero).
    let mut tx = store.begin();
    let neg = -90 * 1_000_000i64; // -90 s
    with_context(
        &registry,
        caller(5, neg),
        &mut tx,
        |ctx: &ReducerContext| {
            assert_eq!(ctx.time_bucket(minute), -120 * 1_000_000, "floors down");
            assert_eq!(ctx.bucket_index(minute), -2);
            Ok(())
        },
    )
    .unwrap();
    tx.commit().unwrap();
}
