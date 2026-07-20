//! SPEC-022 RV-040/041 — relational row visibility: a `Document` row is
//! visible iff a matching `ProjectMember` row exists for the viewer,
//! evaluated for initial data AND diffs against the manager's membership
//! index (sub-linear probes), with membership changes flipping visibility
//! on later commits.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::{MemStore, RowValue, TxDiff};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectMember {
    #[primary_key]
    pub id: u64,
    pub project_id: u64,
    pub member: Identity,
}

#[fluxum::table(public)]
#[visibility(member_of(ProjectMember, project_id))]
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    #[primary_key]
    pub id: u64,
    pub project_id: u64,
    pub title: String,
}

fn alice() -> Identity {
    Identity::from_bytes([1; 32])
}
fn bob() -> Identity {
    Identity::from_bytes([2; 32])
}

fn setup() -> (MemStore, SubscriptionManager) {
    let schema = Arc::new(Schema::from_tables([ProjectMember::SCHEMA, Document::SCHEMA]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    (store, manager)
}

fn add_member(store: &MemStore, id: u64, project: u64, who: Identity) -> TxDiff {
    let table = store.table_id("ProjectMember").unwrap();
    let mut tx = store.begin();
    tx.insert(
        table,
        vec![
            RowValue::U64(id),
            RowValue::U64(project),
            RowValue::Identity(who),
        ],
    )
    .unwrap();
    tx.commit().unwrap()
}

fn add_doc(store: &MemStore, id: u64, project: u64) -> TxDiff {
    let table = store.table_id("Document").unwrap();
    let mut tx = store.begin();
    tx.insert(
        table,
        vec![
            RowValue::U64(id),
            RowValue::U64(project),
            RowValue::Str(format!("doc-{id}")),
        ],
    )
    .unwrap();
    tx.commit().unwrap()
}

fn doc_ids(manager: &SubscriptionManager, store: &MemStore, viewer: Identity) -> Vec<u64> {
    let result = manager
        .query_json(
            Subscriber::client(viewer),
            "SELECT * FROM Document",
            &store.snapshot(),
        )
        .unwrap();
    let mut ids: Vec<u64> = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_u64().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn initial_data_is_scoped_by_membership_and_server_peers_bypass() {
    let (store, manager) = setup();
    manager
        .on_commit(&add_member(&store, 1, 10, alice()))
        .unwrap();
    manager.on_commit(&add_doc(&store, 100, 10)).unwrap();
    manager.on_commit(&add_doc(&store, 200, 20)).unwrap();
    // (init_views also works for pre-existing state — exercised below.)

    assert_eq!(
        doc_ids(&manager, &store, alice()),
        vec![100],
        "member scope"
    );
    assert_eq!(
        doc_ids(&manager, &store, bob()),
        Vec::<u64>::new(),
        "non-member sees nothing"
    );

    // Server peers bypass relational RLS like owner_only (SUB-031).
    let result = manager
        .query_json(
            Subscriber::server_peer(Identity::from_bytes([9; 32])),
            "SELECT * FROM Document",
            &store.snapshot(),
        )
        .unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
}

#[test]
fn membership_index_rebuilds_from_preexisting_rows() {
    let (store, mut manager) = setup();
    // Rows committed BEFORE the manager sees any commit (recovery shape).
    add_member(&store, 1, 10, alice());
    add_doc(&store, 100, 10);
    manager.init_views(&store.snapshot()).unwrap();
    assert_eq!(
        doc_ids(&manager, &store, alice()),
        vec![100],
        "rebuilt (RV-041)"
    );
    assert!(doc_ids(&manager, &store, bob()).is_empty());
}

#[test]
fn diffs_are_scoped_and_membership_changes_flip_later_commits() {
    let (store, mut manager) = setup();
    manager
        .on_commit(&add_member(&store, 1, 10, alice()))
        .unwrap();

    // Two subscribers on the SAME SQL: caller-scoped buckets (RV-040).
    manager
        .subscribe(
            1,
            Subscriber::client(alice()),
            "SELECT * FROM Document",
            &store.snapshot(),
        )
        .unwrap();
    manager
        .subscribe(
            2,
            Subscriber::client(bob()),
            "SELECT * FROM Document",
            &store.snapshot(),
        )
        .unwrap();

    // A project-10 doc reaches Alice only.
    let deltas = manager.on_commit(&add_doc(&store, 100, 10)).unwrap();
    assert_eq!(deltas.len(), 1, "one caller-scoped bucket matched");
    assert_eq!(
        deltas[0].connections(),
        vec![1],
        "only the member (RV-040 diffs)"
    );

    // Bob joins project 10: visibility flips for LATER commits.
    let deltas = manager
        .on_commit(&add_member(&store, 2, 10, bob()))
        .unwrap();
    assert!(
        deltas.is_empty(),
        "the membership row itself is not a Document delta"
    );
    let deltas = manager.on_commit(&add_doc(&store, 101, 10)).unwrap();
    let mut reached: Vec<u128> = deltas.iter().flat_map(|d| d.connections()).collect();
    reached.sort_unstable();
    assert_eq!(
        reached,
        vec![1, 2],
        "both members now receive project-10 docs"
    );

    // Bob leaves: later commits stop reaching him.
    {
        let table = store.table_id("ProjectMember").unwrap();
        let mut tx = store.begin();
        tx.delete(table, &[RowValue::U64(2)]).unwrap();
        manager.on_commit(&tx.commit().unwrap()).unwrap();
    }
    let deltas = manager.on_commit(&add_doc(&store, 102, 10)).unwrap();
    let reached: Vec<u128> = deltas.iter().flat_map(|d| d.connections()).collect();
    assert_eq!(reached, vec![1], "departed member no longer receives docs");

    // And his one-off reads shrink accordingly (initial-data parity).
    assert!(doc_ids(&manager, &store, bob()).is_empty());
    assert_eq!(doc_ids(&manager, &store, alice()), vec![100, 101, 102]);
}

#[test]
fn schema_assembly_validates_member_of_ends() {
    // Key column missing from the protected table's schema is caught by the
    // MACRO; the cross-table ends are validated at assembly. Here: a schema
    // missing the membership table.
    let err = Schema::from_tables([Document::SCHEMA]).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ProjectMember") && msg.contains("RV-040"),
        "{msg}"
    );
}
