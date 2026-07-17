//! SPEC-021 §3 (T5.x exit) — resumable subscriptions: the monotonic
//! `tx_offset` on `InitialData`/`TxUpdate` (CS-020), delta replay from a
//! retained offset (CS-021), and the compacted-window fallback to a full
//! snapshot with `cache_reset` (CS-022).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId, Tx};
use fluxum_core::subscription::{Resumed, Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::I64,
    },
];
static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&SENSOR]).unwrap())
}
fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([&SENSOR]).unwrap()).unwrap()
}
fn manager_with(window: usize) -> SubscriptionManager {
    SubscriptionManager::new(
        schema(),
        SubscriptionLimits {
            resume_window_deltas: window,
            ..SubscriptionLimits::default()
        },
    )
}
fn commit(store: &MemStore, write: impl FnOnce(&mut Tx<'_>)) -> fluxum_core::store::TxDiff {
    let mut tx = store.begin();
    write(&mut tx);
    tx.commit().unwrap()
}
fn sensor(id: u64, reading: i64) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::I64(reading)]
}
fn subscriber() -> Subscriber {
    Subscriber::client(Identity::from_bytes([7u8; 32]))
}

const CONN: u128 = 1;

/// Subscribe `CONN` to every Sensor row and return its `query_id`.
fn subscribe_all(mgr: &mut SubscriptionManager, store: &MemStore) -> (u32, u64) {
    let snapshot = store.snapshot();
    let subscribed = mgr
        .subscribe(CONN, subscriber(), "SELECT * FROM Sensor", &snapshot)
        .unwrap();
    (subscribed.query_id, subscribed.initial.tx_offset)
}

// --- CS-020: the offset is exposed and advances with commits ---------------------

#[test]
fn initial_data_and_tx_updates_carry_a_monotonic_offset() {
    let store = store();
    let mut mgr = manager_with(256);
    let sensor_id = TableId::of("Sensor");

    // A snapshot taken before any commit sits at offset 0.
    let (_, offset0) = subscribe_all(&mut mgr, &store);
    assert_eq!(offset0, 0, "no commits yet");

    // Each commit advances the shard offset, and the TxUpdate carries it.
    let diff = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(1, 10)).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    let update = SubscriptionManager::tx_update(&diff, &deltas[0]);
    assert_eq!(update.tx_offset, diff.tx_id, "CS-020: mirrors tx_id");
    assert_eq!(mgr.current_offset(), diff.tx_id);

    let diff2 = commit(&store, |tx| {
        tx.insert(sensor_id, sensor(2, 20)).unwrap();
    });
    mgr.on_commit(&diff2).unwrap();
    assert!(
        mgr.current_offset() > diff.tx_id,
        "the offset is monotonic across commits"
    );

    // A snapshot taken now reports the offset it reflects, so a client can
    // resume from it without a gap.
    let snapshot = store.snapshot();
    let fresh = mgr
        .subscribe(2, subscriber(), "SELECT * FROM Sensor", &snapshot)
        .unwrap();
    assert_eq!(fresh.initial.tx_offset, mgr.current_offset());
    assert!(!fresh.initial.cache_reset, "a plain subscribe is no reset");
}

// --- CS-021: replay only what changed while the client was away ------------------

#[test]
fn resume_inside_the_window_replays_only_the_missed_deltas() {
    let store = store();
    let mut mgr = manager_with(256);
    let sensor_id = TableId::of("Sensor");

    let (query_id, start) = subscribe_all(&mut mgr, &store);

    // Three commits land while the client is "offline".
    let mut offsets = Vec::new();
    for id in 1..=3u64 {
        let diff = commit(&store, |tx| {
            tx.insert(sensor_id, sensor(id, id as i64 * 10)).unwrap();
        });
        mgr.on_commit(&diff).unwrap();
        offsets.push(diff.tx_id);
    }

    // Resuming from the pre-commit offset replays exactly those three, in
    // ascending offset order — no snapshot.
    let resumed = mgr
        .resume(CONN, query_id, start, &store.snapshot())
        .unwrap()
        .expect("the session still holds the query");
    let Resumed::Deltas(deltas) = resumed else {
        panic!("expected a delta replay, not a reset");
    };
    assert_eq!(deltas.len(), 3, "one delta per missed commit");
    assert_eq!(
        deltas.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        offsets,
        "ascending by offset"
    );
    assert_eq!(deltas[0].1.inserts.len(), 1);

    // Resuming from the newest offset yields nothing: already up to date.
    let resumed = mgr
        .resume(CONN, query_id, *offsets.last().unwrap(), &store.snapshot())
        .unwrap()
        .unwrap();
    let Resumed::Deltas(deltas) = resumed else {
        panic!("expected an empty delta replay");
    };
    assert!(deltas.is_empty(), "caught up → nothing to replay");

    // Resuming from the middle replays only what follows it.
    let resumed = mgr
        .resume(CONN, query_id, offsets[0], &store.snapshot())
        .unwrap()
        .unwrap();
    let Resumed::Deltas(deltas) = resumed else {
        panic!("expected a delta replay");
    };
    assert_eq!(deltas.len(), 2, "only the commits after offsets[0]");
}

// --- CS-022: a compacted window falls back to a snapshot + cache reset ------------

#[test]
fn resume_outside_the_window_falls_back_to_a_snapshot_with_cache_reset() {
    let store = store();
    // A 2-delta window: the third commit evicts the first.
    let mut mgr = manager_with(2);
    let sensor_id = TableId::of("Sensor");

    let (query_id, start) = subscribe_all(&mut mgr, &store);
    for id in 1..=4u64 {
        let diff = commit(&store, |tx| {
            tx.insert(sensor_id, sensor(id, id as i64)).unwrap();
        });
        mgr.on_commit(&diff).unwrap();
    }

    // `start` predates everything still retained → CS-022 fallback.
    let resumed = mgr
        .resume(CONN, query_id, start, &store.snapshot())
        .unwrap()
        .unwrap();
    let Resumed::Reset(initial) = resumed else {
        panic!("a compacted offset must reset, not silently under-replay");
    };
    assert!(initial.cache_reset, "CS-022: the SDK must clear first");
    assert_eq!(initial.tx_offset, mgr.current_offset());
    assert_eq!(
        initial.tables[0].query_id, query_id,
        "addressed to the query"
    );
    assert_eq!(
        initial.tables[0].inserts.len(),
        4,
        "the full current row set, not a delta"
    );

    // An offset still inside the window resumes normally.
    let recent = mgr.current_offset();
    let resumed = mgr
        .resume(CONN, query_id, recent, &store.snapshot())
        .unwrap()
        .unwrap();
    assert!(
        matches!(resumed, Resumed::Deltas(d) if d.is_empty()),
        "the newest offset is inside the window"
    );
}

// --- An unknown query_id tells the client to subscribe afresh --------------------

#[test]
fn resume_of_an_unknown_query_is_not_serviceable() {
    let store = store();
    let mut mgr = manager_with(256);
    let (query_id, _) = subscribe_all(&mut mgr, &store);

    // A query_id this connection never held.
    assert!(
        mgr.resume(CONN, query_id + 99, 0, &store.snapshot())
            .unwrap()
            .is_none()
    );
    // A connection that holds nothing.
    assert!(
        mgr.resume(9999, query_id, 0, &store.snapshot())
            .unwrap()
            .is_none()
    );

    // After a disconnect the subscription is gone: resume is not
    // serviceable and the client must subscribe again.
    mgr.disconnect(CONN);
    assert!(
        mgr.resume(CONN, query_id, 0, &store.snapshot())
            .unwrap()
            .is_none()
    );
}

// --- The window is bounded and freed with its bucket -----------------------------

#[test]
fn the_retained_window_is_bounded_and_released_on_disconnect() {
    let store = store();
    let mut mgr = manager_with(2);
    let sensor_id = TableId::of("Sensor");
    let (query_id, _) = subscribe_all(&mut mgr, &store);

    // Far more commits than the window holds: retention stays bounded, and
    // the oldest offsets stop being resumable rather than growing forever.
    let mut offsets = Vec::new();
    for id in 1..=20u64 {
        let diff = commit(&store, |tx| {
            tx.insert(sensor_id, sensor(id, id as i64)).unwrap();
        });
        mgr.on_commit(&diff).unwrap();
        offsets.push(diff.tx_id);
    }
    // The most recent retained offsets replay; an old one resets.
    let resumed = mgr
        .resume(CONN, query_id, offsets[17], &store.snapshot())
        .unwrap()
        .unwrap();
    assert!(
        matches!(resumed, Resumed::Deltas(d) if d.len() == 2),
        "only the 2-delta window is retained"
    );
    assert!(
        matches!(
            mgr.resume(CONN, query_id, offsets[0], &store.snapshot())
                .unwrap()
                .unwrap(),
            Resumed::Reset(_)
        ),
        "evicted offsets reset"
    );

    // Dropping the last subscriber frees the bucket and its window; a fresh
    // subscribe starts a new one rather than inheriting stale deltas.
    mgr.disconnect(CONN);
    let (new_id, _) = subscribe_all(&mut mgr, &store);
    let resumed = mgr
        .resume(CONN, new_id, mgr.current_offset(), &store.snapshot())
        .unwrap()
        .unwrap();
    assert!(
        matches!(resumed, Resumed::Deltas(d) if d.is_empty()),
        "a rebuilt bucket starts with an empty window"
    );
}
