//! SPEC-023 §7 (DMX-060/061) — the CRDT text column: deterministic
//! convergence of concurrent character-level edits within the single-writer
//! shard, idempotent/commutative op application, compact tagged op-diff
//! encoding, and the subscription fan-out that sends patches instead of
//! full-document rewrites.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::crdt::{CrdtText, TAG_PATCH, TAG_STATE, decode_patch, encode_ops};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;
use fluxum_protocol::FluxBinReader;

const ALICE: u64 = 1;
const BOB: u64 = 2;

/// A doc seeded with `text` by a setup actor.
fn seeded(text: &str) -> CrdtText {
    let mut doc = CrdtText::new();
    doc.local_insert(0, text, 99).unwrap();
    doc
}

// --- DMX-060: deterministic convergence ------------------------------------------

#[test]
fn concurrent_inserts_at_one_position_converge_in_either_order() {
    let base = seeded("AB");

    // Two editors compose against the SAME stale snapshot.
    let mut alice = base.clone();
    let ops_a = alice.local_insert(1, "x", ALICE).unwrap();
    let mut bob = base.clone();
    let ops_b = bob.local_insert(1, "yz", BOB).unwrap();

    // The single writer applies them back-to-back — in either order.
    let mut first = base.clone();
    first.apply_ops(&ops_a).unwrap();
    first.apply_ops(&ops_b).unwrap();
    let mut second = base.clone();
    second.apply_ops(&ops_b).unwrap();
    second.apply_ops(&ops_a).unwrap();

    assert_eq!(first, second, "structural convergence (DMX-060)");
    assert_eq!(first.text(), second.text());
    // Both editors' insertions survive, contiguous, between A and B.
    let text = first.text();
    assert!(text.starts_with('A') && text.ends_with('B'), "{text}");
    assert!(text.contains('x') && text.contains("yz"), "{text}");
    assert_eq!(text.len(), 5, "{text}");
}

#[test]
fn ops_are_idempotent_and_duplicate_deletes_converge() {
    let base = seeded("AB");

    let mut alice = base.clone();
    let del_a = alice.local_delete(0, 1, ALICE).unwrap();
    let mut bob = base.clone();
    let del_b = bob.local_delete(0, 1, BOB).unwrap();

    // Both editors deleted 'A'; both orders converge to identical state.
    let mut first = base.clone();
    first.apply_ops(&del_a).unwrap();
    first.apply_ops(&del_b).unwrap();
    first.apply_ops(&del_a).unwrap(); // replay: idempotent
    let mut second = base.clone();
    second.apply_ops(&del_b).unwrap();
    second.apply_ops(&del_a).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.text(), "B");
}

#[test]
fn insert_after_a_deleted_character_still_positions_correctly() {
    let base = seeded("AB");
    // Bob types after 'A' while Alice concurrently deletes 'A': the
    // tombstone keeps the position identifier meaningful.
    let mut alice = base.clone();
    let deletes = alice.local_delete(0, 1, ALICE).unwrap();
    let mut bob = base.clone();
    let inserts = bob.local_insert(1, "x", BOB).unwrap();

    let mut merged = base.clone();
    merged.apply_ops(&deletes).unwrap();
    merged.apply_ops(&inserts).unwrap();
    assert_eq!(merged.text(), "xB");
}

#[test]
fn local_edit_positions_are_bounds_checked() {
    let mut doc = seeded("AB");
    assert!(doc.local_insert(3, "x", ALICE).is_err(), "beyond end");
    assert!(doc.local_delete(1, 2, ALICE).is_err(), "range beyond end");
}

// --- DMX-061: compact tagged encodings ---------------------------------------

#[test]
fn patch_bytes_replay_a_delta_onto_the_older_state() {
    // A realistically sized paragraph: the patch's compactness claim is
    // relative to the document, so the base must dominate the edit.
    let paragraph = "hello".repeat(40);
    let old = seeded(&paragraph);
    let mut new = old.clone();
    new.local_insert(5, " world", ALICE).unwrap();
    new.local_delete(0, 1, BOB).unwrap();

    let ops = new.ops_since(&old);
    assert_eq!(ops.len(), 7, "6 inserts + 1 delete — the delta only");
    let patch = encode_ops(&ops);
    assert_eq!(patch[0], TAG_PATCH);
    let state = new.to_bytes();
    assert_eq!(state[0], TAG_STATE);
    assert!(
        patch.len() < state.len(),
        "the patch is compact: {} < {} bytes",
        patch.len(),
        state.len()
    );

    let mut replayed = old.clone();
    replayed.apply_patch_bytes(&patch).unwrap();
    assert_eq!(replayed, new, "old + patch = new");
    assert!(
        replayed.text().starts_with("ello world"),
        "{}",
        replayed.text()
    );

    // State/patch tags are mutually exclusive at every decode boundary.
    assert!(CrdtText::from_bytes(&patch).is_err());
    assert!(decode_patch(&state).is_err());
    assert!(CrdtText::from_bytes(&[]).is_err());
}

#[test]
fn state_roundtrip_and_actor_derivation() {
    let doc = seeded("état 🚀"); // non-ASCII chars round-trip
    let decoded = CrdtText::from_bytes(&doc.to_bytes()).unwrap();
    assert_eq!(decoded, doc);
    assert_eq!(decoded.text(), "état 🚀");

    let actor = CrdtText::actor_of(&Identity::from_bytes([7; 32]));
    assert_eq!(
        actor,
        u64::from_le_bytes([7; 8]),
        "stable, identity-derived"
    );
}

// --- DMX-061: subscription fan-out sends patches, not rewrites -------------------

static DOC_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "body",
        ty: FluxType::CrdtText,
    },
];
static DOC: TableSchema = TableSchema {
    name: "Doc",
    columns: DOC_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// Decode a fan-out row of `Doc`: `(id, body bytes)`.
fn decode_doc_row(bytes: &[u8]) -> (u64, Vec<u8>) {
    let mut r = FluxBinReader::new(bytes);
    let id = r.read_u64().unwrap();
    let body = r.read_bytes().unwrap().to_vec();
    (id, body)
}

#[test]
fn tx_updates_carry_op_diffs_and_fresh_inserts_carry_state() {
    let schema = Arc::new(Schema::from_tables([&DOC]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let doc_table = store.table_id("Doc").unwrap();
    let mut manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    manager
        .subscribe(
            1,
            Subscriber::client(Identity::from_bytes([1; 32])),
            "SELECT * FROM Doc",
            &store.snapshot(),
        )
        .unwrap();

    // Fresh insert: the fan-out carries the full tagged state.
    let v1 = seeded("hello");
    let mut tx = store.begin();
    tx.insert(
        doc_table,
        vec![RowValue::U64(7), RowValue::Bytes(v1.to_bytes())],
    )
    .unwrap();
    let diff = tx.commit().unwrap();
    let deltas = manager.on_commit(&diff).unwrap();
    assert_eq!(deltas.len(), 1);
    let update = SubscriptionManager::tx_update(&diff, &deltas[0]);
    let row = update.tables[0].inserts.iter().next().unwrap();
    let (id, body) = decode_doc_row(row);
    assert_eq!(id, 7);
    assert_eq!(body[0], TAG_STATE, "fresh insert = full state");
    assert_eq!(CrdtText::from_bytes(&body).unwrap().text(), "hello");

    // In-place update: the fan-out carries the compact op diff (DMX-061).
    let mut v2 = v1.clone();
    v2.local_insert(5, " world", ALICE).unwrap();
    let mut tx = store.begin();
    tx.upsert(
        doc_table,
        vec![RowValue::U64(7), RowValue::Bytes(v2.to_bytes())],
    )
    .unwrap();
    let diff = tx.commit().unwrap();
    let deltas = manager.on_commit(&diff).unwrap();
    assert_eq!(deltas.len(), 1);
    let update = SubscriptionManager::tx_update(&diff, &deltas[0]);
    let row = update.tables[0].inserts.iter().next().unwrap();
    let (_, body) = decode_doc_row(row);
    assert_eq!(body[0], TAG_PATCH, "update = op diff, not a rewrite");
    assert!(
        body.len() < v2.to_bytes().len(),
        "compact: {} < {}",
        body.len(),
        v2.to_bytes().len()
    );

    // The subscriber replays the patch onto its held state and converges.
    let mut held = v1.clone();
    held.apply_patch_bytes(&body).unwrap();
    assert_eq!(held, v2);
    assert_eq!(held.text(), "hello world");
}
