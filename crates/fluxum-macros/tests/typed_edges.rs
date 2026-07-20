//! SPEC-023 §6 (DMX-050/051) — typed edges: `#[fluxum::edge]` expands to a
//! composite-PK edge table with a `btree(from)` neighbor index, traversal
//! is a point prefix scan (never a JOIN), and neighbor sets subscribe like
//! tables with live diffs as edges are added and removed.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table, registered_edges};
use fluxum_core::sql::{AccessPath, compile};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Player {
    #[primary_key]
    pub id: u64,
    pub name: String,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    #[primary_key]
    pub id: u64,
    pub label: String,
}

/// The player→item ownership relation (DMX-050), with a property column.
#[fluxum::edge(from = Player, to = Item)]
#[derive(Debug, Clone, PartialEq)]
pub struct Owns {
    pub from: u64,
    pub to: u64,
    pub qty: u32,
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

fn setup() -> (MemStore, SubscriptionManager) {
    let schema =
        Arc::new(Schema::from_tables([Player::SCHEMA, Item::SCHEMA, Owns::SCHEMA]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    (store, manager)
}

fn add_edge(store: &MemStore, from: u64, to: u64, qty: u32) -> fluxum_core::store::TxDiff {
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.insert(Owns { from, to, qty })
    })
    .unwrap();
    tx.commit().unwrap()
}

#[test]
fn edge_expands_to_composite_pk_table_with_neighbor_index() {
    // (from, to) composite primary key.
    assert_eq!(Owns::SCHEMA.primary_key, &[0, 1]);
    // The btree(from) neighbor index exists.
    assert!(Owns::SCHEMA.indexes.iter().any(|index| {
        matches!(index, fluxum_core::schema::IndexSchema::BTree { columns } if **columns == [0])
    }));
    // The EdgeDef names its endpoints (DMX-050 descriptor).
    let def = registered_edges().find(|d| d.name == "Owns").unwrap();
    assert_eq!((def.from_table, def.to_table), ("Player", "Item"));

    // Endpoint validation: assembling without Item aborts descriptively.
    let err = Schema::from_tables([Player::SCHEMA, Owns::SCHEMA]).unwrap_err();
    assert!(err.to_string().contains("DMX-050"), "{err}");

    // Duplicate (from, to) is a PK conflict — the relation is a set.
    let (store, _) = setup();
    add_edge(&store, 1, 10, 1);
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    let dup = with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.insert(Owns {
            from: 1,
            to: 10,
            qty: 5,
        })
    });
    assert!(dup.is_err(), "duplicate edge rejected by the composite PK");
}

#[test]
fn traversal_is_an_index_scan_and_subscriptions_deliver_live_diffs() {
    let (store, mut manager) = setup();
    // Player 1 owns items 10, 11; player 2 owns item 20.
    add_edge(&store, 1, 10, 1);
    add_edge(&store, 1, 11, 3);
    add_edge(&store, 2, 20, 1);

    // Reducer-side traversal: player 1's outgoing edges (DMX-050).
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let owned = ctx.tx.traverse::<Owns>(RowValue::U64(1))?;
        let mut items: Vec<(u64, u32)> = owned.iter().map(|e| (e.to, e.qty)).collect();
        items.sort_unstable();
        assert_eq!(items, vec![(10, 1), (11, 3)]);
        assert!(ctx.tx.traverse::<Owns>(RowValue::U64(99))?.is_empty());
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // Snapshot-side traversal helper.
    let owns_table = store.table_id("Owns").unwrap();
    let neighbors = store
        .snapshot()
        .edge_neighbors(owns_table, &RowValue::U64(1))
        .unwrap();
    assert_eq!(neighbors.len(), 2);

    // The neighbor subscription compiles to an IndexScan over btree(from) —
    // a prefix scan, not a table scan (DMX-050).
    let schema = Schema::from_tables([Player::SCHEMA, Item::SCHEMA, Owns::SCHEMA]).unwrap();
    let plan = compile(&schema, "SELECT * FROM Owns WHERE from = 1").unwrap();
    assert!(
        matches!(plan.access, AccessPath::IndexScan(_)),
        "traversal query rides the neighbor index: {:?}",
        plan.access
    );

    // DMX-051: subscribe to player 1's neighbors; edges added/removed fan
    // out as live diffs; player 2's edges never reach this subscriber.
    let subscribed = manager
        .subscribe(
            7,
            Subscriber::client(Identity::from_bytes([1; 32])),
            "SELECT * FROM Owns WHERE from = 1",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(
        subscribed.initial.tables[0].inserts.len(),
        2,
        "current neighbors"
    );

    let deltas = manager.on_commit(&add_edge(&store, 1, 12, 7)).unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(
        deltas[0].connections(),
        vec![7],
        "edge add reaches the subscriber"
    );

    let deltas = manager.on_commit(&add_edge(&store, 2, 21, 1)).unwrap();
    assert!(deltas.is_empty(), "another node's edges never fan out here");

    // Edge removal delivers a delete diff.
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.delete::<Owns>((1, 10))
    })
    .unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(
        deltas[0].update.deletes.len(),
        1,
        "edge removal = delete diff"
    );
}
