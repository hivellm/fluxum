//! SPEC-022 RV-010/011/012/013 — reactive materialized views end-to-end
//! through `#[fluxum::view(materialized, …)]`: incremental aggregate
//! maintenance from commit deltas (O(affected groups)), subscribable pushed
//! view rows, the live top-N leaderboard window with bounded rank deltas,
//! and crash-consistency (incremental state ≡ fresh rebuild).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::{MemStore, RowValue, TxDiff};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_macros as fluxum;
use fluxum_protocol::FluxBinReader;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Score {
    #[primary_key]
    pub id: u64,
    pub region: String,
    pub points: u64,
}

/// Sum of points per region (RV-010: aggregate + GROUP BY).
#[fluxum::view(materialized, table = Score, aggregate = sum(points), group_by = region)]
pub struct PointsByRegion;

/// Global average (RV-010: aggregate, one global group).
#[fluxum::view(materialized, table = Score, aggregate = avg(points))]
pub struct AvgPoints;

/// The live leaderboard (RV-012: top-N window).
#[fluxum::view(materialized, table = Score, order_by = points, desc, limit = 3)]
pub struct TopScores;

fn setup() -> (MemStore, SubscriptionManager) {
    let schema = Arc::new(Schema::from_tables([Score::SCHEMA]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let mut manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    manager.init_views(&store.snapshot()).unwrap();
    (store, manager)
}

fn commit_scores(store: &MemStore, rows: &[(u64, &str, u64)]) -> TxDiff {
    let table = store.table_id("Score").unwrap();
    let mut tx = store.begin();
    for (id, region, points) in rows {
        tx.upsert(
            table,
            vec![
                RowValue::U64(*id),
                RowValue::Str((*region).into()),
                RowValue::U64(*points),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap()
}

/// Decode an aggregate view row `[group, value]`.
fn agg_row(bytes: &[u8]) -> (String, i64) {
    let mut r = FluxBinReader::new(bytes);
    let group = r.read_str().unwrap().to_owned();
    (group, r.read_i64().unwrap())
}

/// Decode a top-N view row `[rank, value, pk]` → `(rank, points)`.
fn top_row(bytes: &[u8]) -> (u32, u64) {
    let mut r = FluxBinReader::new(bytes);
    let rank = r.read_u32().unwrap();
    (rank, r.read_u64().unwrap())
}

#[test]
fn aggregates_maintain_incrementally_with_group_scoped_deltas() {
    let (store, mut manager) = setup();
    manager
        .on_commit(&commit_scores(
            &store,
            &[(1, "eu", 10), (2, "eu", 20), (3, "us", 5)],
        ))
        .unwrap();

    // Subscribe AFTER some commits: InitialData is the current view.
    let initial = manager.subscribe_view(7, "PointsByRegion").unwrap();
    let mut groups: Vec<(String, i64)> = initial.tables[0].inserts.iter().map(agg_row).collect();
    groups.sort();
    assert_eq!(groups, vec![("eu".into(), 30), ("us".into(), 5)]);

    // One commit touching only `eu`: the delta carries ONE group (RV-011).
    let deltas = manager
        .on_commit(&commit_scores(&store, &[(4, "eu", 7)]))
        .unwrap();
    assert_eq!(deltas.len(), 1);
    let update = deltas[0].update.as_ref();
    assert_eq!(update.table_name, "PointsByRegion");
    assert_eq!(update.inserts.len(), 1, "only the affected group (RV-011)");
    assert_eq!(
        agg_row(update.inserts.iter().next().unwrap()),
        ("eu".into(), 37)
    );
    assert_eq!(update.deletes.len(), 1, "the group's previous row retires");
    assert_eq!(deltas[0].connections(), vec![7]);

    // Deleting the last `us` row retires the group entirely.
    let table = store.table_id("Score").unwrap();
    let mut tx = store.begin();
    tx.delete(table, &[RowValue::U64(3)]).unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    let update = deltas[0].update.as_ref();
    assert_eq!(update.inserts.len(), 0);
    assert_eq!(update.deletes.len(), 1, "vanished group = delete only");

    // RV-013: the incremental state equals a fresh rebuild.
    manager.validate_views(&store.snapshot()).unwrap();
}

#[test]
fn global_average_and_unknown_view_admission() {
    let (store, mut manager) = setup();
    manager
        .on_commit(&commit_scores(&store, &[(1, "eu", 10), (2, "us", 30)]))
        .unwrap();
    let initial = manager.subscribe_view(1, "AvgPoints").unwrap();
    let row = initial.tables[0].inserts.iter().next().unwrap();
    let mut r = FluxBinReader::new(row);
    assert_eq!(r.read_str().unwrap(), "*", "global group");
    let avg = r.read_f64().unwrap();
    assert!((avg - 20.0).abs() < 1e-9, "avg of 10 and 30: {avg}");

    assert!(manager.subscribe_view(1, "Ghost").is_err(), "unknown view");
}

#[test]
fn leaderboard_window_emits_bounded_rank_deltas() {
    let (store, mut manager) = setup();
    manager
        .on_commit(&commit_scores(
            &store,
            &[
                (1, "eu", 100),
                (2, "eu", 90),
                (3, "eu", 80),
                (4, "eu", 70),
                (5, "eu", 60),
            ],
        ))
        .unwrap();

    let initial = manager.subscribe_view(9, "TopScores").unwrap();
    let window: Vec<(u32, u64)> = initial.tables[0].inserts.iter().map(top_row).collect();
    assert_eq!(window, vec![(1, 100), (2, 90), (3, 80)], "the live window");

    // Player 5 (rank 5, outside) jumps to 95: enters at rank 2; rank 3's
    // occupant changes; player at old rank 3 leaves. Deltas bounded by the
    // window — the score table is never re-evaluated (RV-012).
    let deltas = manager
        .on_commit(&commit_scores(&store, &[(5, "eu", 95)]))
        .unwrap();
    assert_eq!(deltas.len(), 1);
    let update = deltas[0].update.as_ref();
    assert_eq!(update.table_name, "TopScores");
    let mut inserted: Vec<(u32, u64)> = update.inserts.iter().map(top_row).collect();
    inserted.sort_unstable();
    assert_eq!(
        inserted,
        vec![(2, 95), (3, 90)],
        "only ranks 2 and 3 changed; rank 1 untouched"
    );
    assert!(update.deletes.len() <= 2, "bounded rank retirements");

    // A change entirely below the window emits nothing.
    let deltas = manager
        .on_commit(&commit_scores(&store, &[(6, "eu", 1)]))
        .unwrap();
    assert!(deltas.is_empty(), "below-window change = no view delta");

    // Deleting the leader shifts everyone up — still bounded by the window.
    let table = store.table_id("Score").unwrap();
    let mut tx = store.begin();
    tx.delete(table, &[RowValue::U64(1)]).unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    let update = deltas[0].update.as_ref();
    let mut inserted: Vec<(u32, u64)> = update.inserts.iter().map(top_row).collect();
    inserted.sort_unstable();
    assert_eq!(
        inserted,
        vec![(1, 95), (2, 90), (3, 80)],
        "window shifted up"
    );

    manager.validate_views(&store.snapshot()).unwrap();
}

#[test]
fn view_subscriptions_die_with_the_connection() {
    let (store, mut manager) = setup();
    manager
        .on_commit(&commit_scores(&store, &[(1, "eu", 10)]))
        .unwrap();
    manager.subscribe_view(4, "PointsByRegion").unwrap();
    manager.disconnect(4);
    let deltas = manager
        .on_commit(&commit_scores(&store, &[(2, "eu", 5)]))
        .unwrap();
    assert!(deltas.is_empty(), "no subscribers → no view fan-out");

    // Unsubscribe (not disconnect) works too.
    manager.subscribe_view(5, "PointsByRegion").unwrap();
    assert!(manager.unsubscribe_view(5, "PointsByRegion"));
    assert!(!manager.unsubscribe_view(5, "PointsByRegion"), "idempotent");
    let deltas = manager
        .on_commit(&commit_scores(&store, &[(3, "eu", 5)]))
        .unwrap();
    assert!(deltas.is_empty());
}
