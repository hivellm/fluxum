//! SPEC-022 RV-030/031/032 — declarative constraints and triggers:
//! `#[check]` / `#[not_null]` / `#[references]` validated on write before
//! merge (typed aborts), `#[fluxum::on_insert/on_update/on_delete]` hooks
//! running inside the triggering transaction, and `on_delete` referential
//! actions (restrict / cascade / set_null) applied atomically.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::reducer::{ReducerCaller, ReducerContext, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::MemStore;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Customer {
    #[primary_key]
    pub id: u64,
    pub name: String,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Order {
    #[primary_key]
    pub id: u64,
    /// Deleting the customer deletes their orders (RV-032 cascade).
    #[references(Customer(id), on_delete = cascade)]
    pub customer_id: u64,
    /// RV-030: quantity must be positive and bounded.
    #[check(qty > 0 && qty <= 1_000)]
    pub qty: u64,
    /// RV-030: nullable on the wire, required at commit.
    #[not_null]
    pub note: Option<String>,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Shipment {
    #[primary_key]
    pub id: u64,
    /// An order with shipments cannot be deleted (RV-032 restrict default).
    #[references(Order(id))]
    pub order_id: u64,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
    #[primary_key]
    pub id: u64,
    /// Deleting the customer unlinks the profile (RV-032 set_null).
    #[references(Customer(id), on_delete = set_null)]
    pub customer_id: Option<u64>,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct AuditLog {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub action: String,
    pub order_id: u64,
}

// --- RV-031 hooks on Order -------------------------------------------------

#[fluxum::on_insert(Order)]
fn order_inserted(ctx: &ReducerContext, row: &Order) -> Result<(), String> {
    ctx.tx
        .insert(AuditLog {
            id: 0,
            action: "insert".into(),
            order_id: row.id,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::on_update(Order)]
fn order_updated(ctx: &ReducerContext, old: &Order, new: &Order) -> Result<(), String> {
    ctx.tx
        .insert(AuditLog {
            id: 0,
            action: format!("update:{}->{}", old.qty, new.qty),
            order_id: new.id,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[fluxum::on_delete(Order)]
fn order_deleted(ctx: &ReducerContext, row: &Order) -> Result<(), String> {
    ctx.tx
        .insert(AuditLog {
            id: 0,
            action: "delete".into(),
            order_id: row.id,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

// --- Harness ----------------------------------------------------------------

fn store() -> MemStore {
    MemStore::new(
        &Schema::from_tables([
            Customer::SCHEMA,
            Order::SCHEMA,
            Shipment::SCHEMA,
            Profile::SCHEMA,
            AuditLog::SCHEMA,
        ])
        .unwrap(),
    )
    .unwrap()
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

/// Run `body` as one committed transaction.
fn commit<R>(
    store: &MemStore,
    registry: &ReducerRegistry,
    body: impl FnOnce(&ReducerContext) -> fluxum_core::Result<R>,
) -> fluxum_core::Result<R> {
    let mut tx = store.begin();
    let out = with_context(registry, caller(), &mut tx, body)?;
    tx.commit()?;
    Ok(out)
}

fn order(id: u64, customer_id: u64, qty: u64) -> Order {
    Order {
        id,
        customer_id,
        qty,
        note: Some("ok".into()),
    }
}

fn seed_customer(store: &MemStore, registry: &ReducerRegistry, id: u64) {
    commit(store, registry, |ctx| {
        ctx.tx.insert(Customer {
            id,
            name: format!("c{id}"),
        })
    })
    .unwrap();
}

// --- Store assembly validates FK ends (RV-030) --------------------------------

#[test]
fn store_assembly_rejects_missing_parent_table() {
    let schema = Schema::from_tables([Order::SCHEMA, AuditLog::SCHEMA]).unwrap();
    let err = MemStore::new(&schema).expect_err("Order references Customer, absent here");
    let msg = err.to_string();
    assert!(
        msg.contains("Customer") && msg.contains("not in the assembled schema"),
        "{msg}"
    );
}

// --- RV-030: write-time constraint validation --------------------------------

#[test]
fn check_violation_aborts_with_typed_error() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);

    let err = commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 1, 0)))
        .expect_err("qty 0 violates #[check]");
    let msg = err.to_string();
    assert!(msg.contains("#[check(") && msg.contains("RV-030"), "{msg}");

    // The upper bound too.
    let err = commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 1, 1_001)))
        .expect_err("qty 1001 violates #[check]");
    assert!(err.to_string().contains("#[check("), "{err}");

    // A valid row passes.
    commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 1, 5))).unwrap();
}

#[test]
fn not_null_rejects_none_on_write() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);

    let err = commit(&store, &registry, |ctx| {
        ctx.tx.insert(Order {
            id: 1,
            customer_id: 1,
            qty: 1,
            note: None,
        })
    })
    .expect_err("None violates #[not_null]");
    let msg = err.to_string();
    assert!(msg.contains("#[not_null]") && msg.contains("note"), "{msg}");
}

#[test]
fn foreign_key_write_requires_visible_parent() {
    let store = store();
    let registry = ReducerRegistry::new();

    // No Customer 7 anywhere → typed FK violation.
    let err =
        commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 7, 1))).expect_err("missing parent");
    let msg = err.to_string();
    assert!(
        msg.contains("#[references]") && msg.contains("Customer"),
        "{msg}"
    );

    // A parent inserted earlier in the SAME transaction satisfies the
    // reference (overlay-aware, RV-030).
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(Customer {
            id: 7,
            name: "same-tx".into(),
        })?;
        ctx.tx.insert(order(1, 7, 1))
    })
    .unwrap();

    // An Option-typed reference validates Some(...) and allows None.
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(Profile {
            id: 1,
            customer_id: None,
        })?;
        ctx.tx.insert(Profile {
            id: 2,
            customer_id: Some(7),
        })
    })
    .unwrap();
    let err = commit(&store, &registry, |ctx| {
        ctx.tx.insert(Profile {
            id: 3,
            customer_id: Some(999),
        })
    })
    .expect_err("missing optional parent");
    assert!(err.to_string().contains("#[references]"), "{err}");
}

// --- RV-032: referential actions on delete ------------------------------------

#[test]
fn restrict_blocks_delete_while_children_exist() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(order(1, 1, 1))?;
        ctx.tx.insert(Shipment { id: 1, order_id: 1 })
    })
    .unwrap();

    let err = commit(&store, &registry, |ctx| ctx.tx.delete::<Order>(1))
        .expect_err("shipment restricts the order delete");
    let msg = err.to_string();
    assert!(
        msg.contains("restrict") && msg.contains("Shipment"),
        "{msg}"
    );

    // Removing the child first unblocks the delete — in the same tx.
    commit(&store, &registry, |ctx| {
        ctx.tx.delete::<Shipment>(1)?;
        ctx.tx.delete::<Order>(1)
    })
    .unwrap();
    let remaining = commit(&store, &registry, |ctx| ctx.tx.scan::<Order>()).unwrap();
    assert!(remaining.is_empty());
}

#[test]
fn cascade_deletes_children_in_the_same_transaction() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);
    seed_customer(&store, &registry, 2);
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(order(1, 1, 1))?;
        ctx.tx.insert(order(2, 1, 2))?;
        ctx.tx.insert(order(3, 2, 3))
    })
    .unwrap();

    commit(&store, &registry, |ctx| ctx.tx.delete::<Customer>(1)).unwrap();

    let orders = commit(&store, &registry, |ctx| ctx.tx.scan::<Order>()).unwrap();
    assert_eq!(
        orders.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![3],
        "customer 1's orders cascaded; customer 2's survive"
    );
    // The cascade fired the Order delete triggers too (RV-031).
    let audits = commit(&store, &registry, |ctx| {
        ctx.tx.scan_where::<AuditLog>(|a| a.action == "delete")
    })
    .unwrap();
    let mut deleted: Vec<u64> = audits.iter().map(|a| a.order_id).collect();
    deleted.sort_unstable();
    assert_eq!(deleted, vec![1, 2], "cascade deletes fired on_delete hooks");
}

#[test]
fn cascade_delete_still_restricted_by_grandchildren() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(order(1, 1, 1))?;
        ctx.tx.insert(Shipment { id: 1, order_id: 1 })
    })
    .unwrap();

    // Customer → (cascade) Order → (restrict) Shipment: the whole delete
    // aborts atomically; nothing is half-applied.
    let err = commit(&store, &registry, |ctx| ctx.tx.delete::<Customer>(1))
        .expect_err("grandchild shipment restricts the cascade");
    assert!(err.to_string().contains("restrict"), "{err}");
    let customers = commit(&store, &registry, |ctx| ctx.tx.scan::<Customer>()).unwrap();
    let orders = commit(&store, &registry, |ctx| ctx.tx.scan::<Order>()).unwrap();
    assert_eq!((customers.len(), orders.len()), (1, 1), "rolled back whole");
}

#[test]
fn set_null_unlinks_children_on_parent_delete() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);
    commit(&store, &registry, |ctx| {
        ctx.tx.insert(Profile {
            id: 1,
            customer_id: Some(1),
        })
    })
    .unwrap();

    commit(&store, &registry, |ctx| ctx.tx.delete::<Customer>(1)).unwrap();

    let profile = commit(&store, &registry, |ctx| ctx.tx.query_pk::<Profile>(1))
        .unwrap()
        .expect("profile survives");
    assert_eq!(profile.customer_id, None, "reference cleared, row kept");
}

/// RV-030/032 (§1.7): the parent delete, its cascaded child deletes, and the
/// trigger's own writes all ride ONE committed diff — subscribers see a
/// single atomic `TxUpdate`, never a torn sequence.
#[test]
fn cascade_and_trigger_writes_share_one_commit_diff() {
    use fluxum_core::store::TableId;

    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);
    commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 1, 5))).unwrap();

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.delete::<Customer>(1)
    })
    .unwrap();
    let diff = tx.commit().unwrap();

    let of = |name: &str| diff.tables.iter().find(|t| t.table_id == TableId::of(name));
    let customer = of("Customer").expect("customer delete in diff");
    let orders = of("Order").expect("cascaded order delete in diff");
    let audit = of("AuditLog").expect("trigger's audit insert in diff");
    assert_eq!(customer.deletes.len(), 1);
    assert_eq!(orders.deletes.len(), 1, "cascade in the same diff");
    assert_eq!(audit.inserts.len(), 1, "the cascade's on_delete hook wrote");
}

// --- RV-031: triggers run inside the triggering transaction -------------------

#[test]
fn triggers_fire_on_insert_update_delete() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);

    // Insert + trigger commit atomically: the audit row is written by the
    // hook inside the same transaction.
    commit(&store, &registry, |ctx| ctx.tx.insert(order(1, 1, 5))).unwrap();
    // Upsert over the committed row fires on_update with old and new.
    commit(&store, &registry, |ctx| ctx.tx.upsert(order(1, 1, 9))).unwrap();
    // Delete fires on_delete.
    commit(&store, &registry, |ctx| ctx.tx.delete::<Order>(1)).unwrap();

    let audits = commit(&store, &registry, |ctx| ctx.tx.scan::<AuditLog>()).unwrap();
    let actions: Vec<&str> = audits.iter().map(|a| a.action.as_str()).collect();
    assert_eq!(
        actions,
        vec!["insert", "update:5->9", "delete"],
        "one hook per mutation, in order, with old/new visible to on_update"
    );
    assert!(audits.iter().all(|a| a.order_id == 1));
}

#[test]
fn trigger_writes_roll_back_with_the_transaction() {
    let store = store();
    let registry = ReducerRegistry::new();
    seed_customer(&store, &registry, 1);

    // The insert fires the hook (audit row buffered), then the reducer
    // fails — everything rolls back together (RV-031: same transaction).
    let err = commit(&store, &registry, |ctx| {
        ctx.tx.insert(order(1, 1, 5))?;
        Err::<(), _>(fluxum_core::FluxumError::Reducer("abort".into()))
    })
    .expect_err("reducer aborts");
    assert!(err.to_string().contains("abort"), "{err}");

    let audits = commit(&store, &registry, |ctx| ctx.tx.scan::<AuditLog>()).unwrap();
    let orders = commit(&store, &registry, |ctx| ctx.tx.scan::<Order>()).unwrap();
    assert!(audits.is_empty() && orders.is_empty(), "no partial state");
}
