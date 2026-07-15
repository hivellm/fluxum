//! T4.3 row-level security matrix (SPEC-005 SUB-030/031; SPEC-001 acceptance
//! 9; FR-32/72; DAG exit test): `#[visibility(owner_only(field))]` enforced
//! per subscriber on InitialData AND every TxUpdate diff — {owner, other
//! user, server peer} × {InitialData, TxUpdate}: the owner sees only their
//! rows, another user sees nothing of the owner's rows, a server peer sees
//! all; a private table can never be subscribed; two-client Task scenario
//! (SPEC-001).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, Tx};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

// --- owner_only Task table (owner column ordinal 1) ----------------------------

static TASK_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
];

static TASK: TableSchema = TableSchema {
    name: "Task",
    columns: TASK_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::OwnerOnly { owner: 1 },
};

// --- a Private table (never subscribable) --------------------------------------

static SECRET_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];

static SECRET: TableSchema = TableSchema {
    name: "Secret",
    columns: SECRET_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

const ALICE: [u8; 32] = [0xA1; 32];
const BOB: [u8; 32] = [0xB0; 32];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&TASK, &SECRET]).unwrap())
}

fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([&TASK, &SECRET]).unwrap()).unwrap()
}

fn manager() -> SubscriptionManager {
    SubscriptionManager::new(schema(), SubscriptionLimits::default())
}

fn task(id: u64, owner: [u8; 32], title: &str) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Identity(Identity::from_bytes(owner)),
        RowValue::Str(title.into()),
    ]
}

fn commit(store: &MemStore, write: impl FnOnce(&mut Tx<'_>)) -> fluxum_core::store::TxDiff {
    let mut tx = store.begin();
    write(&mut tx);
    tx.commit().unwrap()
}

fn initial_len(sub: &fluxum_core::subscription::Subscribed) -> usize {
    sub.initial.tables[0].inserts.len()
}

// --- SUB-030: owner_only on InitialData ----------------------------------------

#[test]
fn owner_only_filters_initialdata_per_subscriber() {
    let store = store();
    let task_id = store.table_id("Task").unwrap();
    commit(&store, |tx| {
        tx.insert(task_id, task(1, ALICE, "alice-1")).unwrap();
        tx.insert(task_id, task(2, ALICE, "alice-2")).unwrap();
        tx.insert(task_id, task(3, BOB, "bob-1")).unwrap();
    });
    let mut mgr = manager();

    // Alice sees only her two rows.
    let alice = mgr
        .subscribe(
            1,
            Subscriber::client(Identity::from_bytes(ALICE)),
            "SELECT * FROM Task",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(initial_len(&alice), 2, "owner sees own rows only (SUB-030)");

    // Bob sees only his one row.
    let bob = mgr
        .subscribe(
            2,
            Subscriber::client(Identity::from_bytes(BOB)),
            "SELECT * FROM Task",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(initial_len(&bob), 1);

    // A third user (no rows) sees nothing.
    let carol = mgr
        .subscribe(
            3,
            Subscriber::client(Identity::from_bytes([0xCC; 32])),
            "SELECT * FROM Task",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(initial_len(&carol), 0, "another user sees nothing");

    // A server peer bypasses RLS and sees all three rows (SUB-031).
    let server = mgr
        .subscribe(
            4,
            Subscriber::server_peer(fluxum_core::auth::server_identity("svc")),
            "SELECT * FROM Task",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(initial_len(&server), 3, "server peer sees all (SUB-031)");

    // Alice and Bob are DISTINCT plan buckets (caller-parameterized), and
    // so is the server bypass: 4 subscribers, but 4 distinct viewers →
    // no accidental sharing of a filtered encoding.
    assert_eq!(mgr.plan_count(), 4, "one bucket per distinct viewer/bypass");
}

// --- SUB-030: owner_only on every TxUpdate diff --------------------------------

#[test]
fn owner_only_filters_txupdate_diffs_per_subscriber() {
    let store = store();
    let task_id = store.table_id("Task").unwrap();
    let mut mgr = manager();

    mgr.subscribe(
        1,
        Subscriber::client(Identity::from_bytes(ALICE)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();
    mgr.subscribe(
        2,
        Subscriber::client(Identity::from_bytes(BOB)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();
    mgr.subscribe(
        3,
        Subscriber::server_peer(fluxum_core::auth::server_identity("svc")),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();

    // Commit an Alice row and a Bob row in one transaction.
    let diff = commit(&store, |tx| {
        tx.insert(task_id, task(1, ALICE, "alice-1")).unwrap();
        tx.insert(task_id, task(2, BOB, "bob-1")).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();

    // Three buckets matched. Alice's delta carries only her row, Bob's only
    // his, the server's both.
    let by_sub = |conn: u128| {
        deltas
            .iter()
            .find(|d| d.subscribers == vec![conn])
            .unwrap_or_else(|| panic!("no delta for connection {conn}"))
    };
    assert_eq!(by_sub(1).update.inserts.len(), 1, "Alice sees only her row");
    assert_eq!(by_sub(2).update.inserts.len(), 1, "Bob sees only his row");
    assert_eq!(by_sub(3).update.inserts.len(), 2, "server peer sees both");

    // A commit of ONLY a Bob row produces no delta for Alice (she never
    // sees a change she has no visibility into).
    let diff = commit(&store, |tx| {
        tx.insert(task_id, task(3, BOB, "bob-2")).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert!(
        deltas.iter().all(|d| d.subscribers != vec![1]),
        "Alice gets no delta for a Bob-only commit"
    );
    // Bob and the server both do.
    assert!(deltas.iter().any(|d| d.subscribers == vec![2]));
    assert!(deltas.iter().any(|d| d.subscribers == vec![3]));

    // Deleting an Alice row is delivered to Alice (and server), not to Bob.
    let diff = commit(&store, |tx| {
        assert!(tx.delete(task_id, &[RowValue::U64(1)]).unwrap());
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    let alice_delta = deltas.iter().find(|d| d.subscribers == vec![1]).unwrap();
    assert_eq!(alice_delta.update.deletes.len(), 1, "Alice sees her delete");
    assert!(
        deltas.iter().all(|d| d.subscribers != vec![2]),
        "Bob never saw Alice's row, so no delete for him"
    );
}

// --- SPEC-001 acceptance 9: private tables are never subscribable ---------------

#[test]
fn private_tables_cannot_be_subscribed() {
    let store = store();
    let mut mgr = manager();
    let err = mgr
        .subscribe(
            1,
            Subscriber::client(Identity::from_bytes(ALICE)),
            "SELECT * FROM Secret",
            &store.snapshot(),
        )
        .unwrap_err();
    assert_eq!(err.query_code(), Some(403), "{err}");
    assert!(err.to_string().contains("not public"), "{err}");
    // Even a server peer cannot subscribe to a private table.
    let err = mgr
        .subscribe(
            2,
            Subscriber::server_peer(fluxum_core::auth::server_identity("svc")),
            "SELECT * FROM Secret",
            &store.snapshot(),
        )
        .unwrap_err();
    assert_eq!(err.query_code(), Some(403), "{err}");
}

// --- SPEC-001 two-client Task scenario -----------------------------------------

#[test]
fn two_client_task_scenario_isolates_owners() {
    let store = store();
    let task_id = store.table_id("Task").unwrap();
    let mut mgr = manager();

    // Alice subscribes to all Tasks; she is the only subscriber so far.
    mgr.subscribe(
        1,
        Subscriber::client(Identity::from_bytes(ALICE)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();

    // A reducer inserts a Task owned by Bob → Alice's subscription does NOT
    // receive it.
    let diff = commit(&store, |tx| {
        tx.insert(task_id, task(1, BOB, "write report")).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    assert!(
        deltas.iter().all(|d| d.subscribers != vec![1]),
        "Alice does not receive Bob's row (SPEC-001 acceptance)"
    );

    // A reducer inserts a Task owned by Alice → she DOES receive it.
    let diff = commit(&store, |tx| {
        tx.insert(task_id, task(2, ALICE, "review PR")).unwrap();
    });
    let deltas = mgr.on_commit(&diff).unwrap();
    let alice_delta = deltas.iter().find(|d| d.subscribers == vec![1]).unwrap();
    assert_eq!(
        alice_delta.update.inserts.len(),
        1,
        "Alice receives her row"
    );
}

// --- Same identity still dedups (dedup survives caller-parameterization) --------

#[test]
fn identical_viewer_and_query_still_share_one_plan() {
    let store = store();
    let mut mgr = manager();
    // Two connections, same identity, same query on an owner_only table:
    // one shared bucket (the identity folds into the hash identically).
    mgr.subscribe(
        1,
        Subscriber::client(Identity::from_bytes(ALICE)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();
    mgr.subscribe(
        2,
        Subscriber::client(Identity::from_bytes(ALICE)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();
    assert_eq!(
        mgr.plan_count(),
        1,
        "same viewer + query dedups to one bucket"
    );

    // A different identity on the same query text is a distinct bucket.
    mgr.subscribe(
        3,
        Subscriber::client(Identity::from_bytes(BOB)),
        "SELECT * FROM Task",
        &store.snapshot(),
    )
    .unwrap();
    assert_eq!(mgr.plan_count(), 2);
}
