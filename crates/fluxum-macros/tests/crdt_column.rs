//! SPEC-023 DMX-060/061 — a `CrdtText` column through the macro + reducer
//! path: `Doc.body: CrdtText` compiles as a table column, edit ops travel
//! as reducer-call bytes and ride the single-writer serialization, and
//! overlapping editors converge to the same text in either apply order.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::crdt::{CrdtText, decode_ops, encode_ops};
use fluxum_core::reducer::{ReducerCaller, ReducerContext, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::MemStore;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    #[primary_key]
    pub id: u64,
    /// The collaborative body (DMX-060).
    pub body: CrdtText,
}

fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([Doc::SCHEMA]).unwrap()).unwrap()
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

fn commit<R>(
    store: &MemStore,
    registry: &ReducerRegistry,
    body: impl FnOnce(&ReducerContext) -> fluxum_core::Result<R>,
) -> R {
    let mut tx = store.begin();
    let out = with_context(registry, caller(), &mut tx, body).unwrap();
    tx.commit().unwrap();
    out
}

/// The DMX-061 reducer shape: ops arrive as encoded bytes, are applied to
/// the authoritative doc inside the transaction, and the row is upserted —
/// riding the existing single-writer serialization.
fn apply_edit(ctx: &ReducerContext, doc_id: u64, op_bytes: &[u8]) -> fluxum_core::Result<()> {
    let mut doc = ctx.tx.query_pk::<Doc>(doc_id)?.expect("doc exists");
    let ops = decode_ops(op_bytes)?;
    doc.body.apply_ops(&ops)?;
    ctx.tx.upsert(doc)?;
    Ok(())
}

#[test]
fn concurrent_editors_converge_through_reducer_calls_in_either_order() {
    let registry = ReducerRegistry::new();

    // Seed the same document into two independent shards.
    let seed = {
        let mut body = CrdtText::new();
        body.local_insert(0, "AB", 99).unwrap();
        body
    };
    let store_one = store();
    let store_two = store();
    for store in [&store_one, &store_two] {
        let body = seed.clone();
        commit(store, &registry, move |ctx| {
            ctx.tx.insert(Doc { id: 1, body })
        });
    }

    // Two editors compose ops against the SAME committed snapshot, sending
    // them as reducer-call bytes (DMX-061).
    let alice_actor = CrdtText::actor_of(&Identity::from_bytes([1; 32]));
    let bob_actor = CrdtText::actor_of(&Identity::from_bytes([2; 32]));
    let mut alice_view = seed.clone();
    let alice_bytes = encode_ops(&alice_view.local_insert(1, "x", alice_actor).unwrap());
    let mut bob_view = seed.clone();
    let bob_bytes = encode_ops(&bob_view.local_insert(1, "yz", bob_actor).unwrap());

    // Shard one applies Alice then Bob; shard two applies Bob then Alice.
    commit(&store_one, &registry, |ctx| {
        apply_edit(ctx, 1, &alice_bytes)
    });
    commit(&store_one, &registry, |ctx| apply_edit(ctx, 1, &bob_bytes));
    commit(&store_two, &registry, |ctx| apply_edit(ctx, 1, &bob_bytes));
    commit(&store_two, &registry, |ctx| {
        apply_edit(ctx, 1, &alice_bytes)
    });

    let one = commit(&store_one, &registry, |ctx| {
        Ok(ctx.tx.query_pk::<Doc>(1)?.unwrap())
    });
    let two = commit(&store_two, &registry, |ctx| {
        Ok(ctx.tx.query_pk::<Doc>(1)?.unwrap())
    });
    assert_eq!(one.body, two.body, "deterministic merge (DMX-060)");
    assert_eq!(one.body.text(), two.body.text());
    let text = one.body.text();
    assert!(text.starts_with('A') && text.ends_with('B'), "{text}");
    assert_eq!(text.len(), 5, "both editors' inserts survive: {text}");
}

#[test]
fn crdt_text_round_trips_through_the_typed_row_surface() {
    let registry = ReducerRegistry::new();
    let store = store();
    let mut body = CrdtText::new();
    body.local_insert(0, "draft", 7).unwrap();
    let expected = body.clone();
    commit(&store, &registry, move |ctx| {
        ctx.tx.insert(Doc { id: 2, body })
    });
    let read = commit(&store, &registry, |ctx| {
        Ok(ctx.tx.query_pk::<Doc>(2)?.unwrap())
    });
    assert_eq!(read.body, expected);
    assert_eq!(read.body.text(), "draft");
}
