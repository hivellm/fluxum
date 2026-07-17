//! SPEC-022 RV-050/051 — `#[computed(expr)]` generated columns: the value is
//! derived from sibling columns on write (read-only to reducers), stored,
//! and usable in filters/indexes like any column.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::MemStore;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[index(btree(total))]
#[derive(Debug, Clone, PartialEq)]
pub struct OrderLine {
    #[primary_key]
    pub id: u64,
    pub qty: u64,
    pub unit_price: u64,
    /// Derived: quantity × unit price.
    #[computed(qty * unit_price)]
    pub total: u64,
    /// Derived over another computed column (`total`) + a discount.
    #[computed(total.saturating_sub(discount))]
    pub net: u64,
    pub discount: u64,
    /// Derived string over a sibling (reference columns as real idents, not
    /// inline `{id}` format captures).
    #[computed(format!("line-{}", id))]
    pub label: String,
}

fn store() -> MemStore {
    MemStore::new(&Schema::from_tables([OrderLine::SCHEMA]).unwrap()).unwrap()
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

/// RV-050: computed columns are derived on write from siblings — including an
/// earlier computed column — and whatever the reducer put in them is ignored.
#[test]
fn computed_columns_are_derived_on_write_and_read_only() {
    let store = store();
    let registry = ReducerRegistry::new();

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        // The reducer sets bogus values for the computed columns; they are
        // overwritten by the derivation.
        let stored = ctx.tx.insert(OrderLine {
            id: 7,
            qty: 3,
            unit_price: 10,
            total: 999, // ignored → 3 * 10 = 30
            net: 999,   // ignored → 30 - 5 = 25
            discount: 5,
            label: "bogus".into(), // ignored → "line-7"
        })?;
        assert_eq!(stored.total, 30, "total = qty * unit_price");
        assert_eq!(
            stored.net, 25,
            "net = total - discount (computed over computed)"
        );
        assert_eq!(stored.label, "line-7", "string derivation");
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // The stored/queried row carries the derived values.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let got = ctx.tx.query_pk::<OrderLine>(7)?.unwrap();
        assert_eq!((got.total, got.net, got.label.as_str()), (30, 25, "line-7"));
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();
}

/// RV-051: a computed column is a stored column like any other — it can be
/// indexed and filtered/sorted on server-side.
#[test]
fn computed_column_is_indexable_and_filterable() {
    let store = store();
    let registry = ReducerRegistry::new();

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        for (id, qty, price) in [(1u64, 2u64, 5u64), (2, 4, 5), (3, 1, 5)] {
            ctx.tx.insert(OrderLine {
                id,
                qty,
                unit_price: price,
                total: 0,
                net: 0,
                discount: 0,
                label: String::new(),
            })?;
        }
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // Filter on the computed `total` (10, 20, 5) via a typed scan.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let big = ctx.tx.scan_where::<OrderLine>(|o| o.total >= 10)?;
        let mut totals: Vec<u64> = big.iter().map(|o| o.total).collect();
        totals.sort_unstable();
        assert_eq!(totals, vec![10, 20], "filtered on the derived column");
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // The B-tree index over `total` was maintained from the derived values —
    // a fresh rebuild agrees (STG-007).
    store
        .snapshot()
        .verify_index_integrity(store.table_id("OrderLine").unwrap())
        .expect("computed-column index is rebuild-identical");
}

/// RV-051: a computed column resolves in subscription SQL — WHERE filters on
/// the derived value and ORDER BY targets its ordinal like any column.
#[test]
fn computed_column_usable_in_sql_where_and_order_by() {
    let store = store();
    let registry = ReducerRegistry::new();

    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        for (id, qty, price) in [(1u64, 2u64, 5u64), (2, 4, 5), (3, 1, 5)] {
            ctx.tx.insert(OrderLine {
                id,
                qty,
                unit_price: price,
                total: 0,
                net: 0,
                discount: 0,
                label: String::new(),
            })?;
        }
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    let schema = Schema::from_tables([OrderLine::SCHEMA]).unwrap();
    let plan = fluxum_core::sql::compile(
        &schema,
        "SELECT * FROM OrderLine WHERE total BETWEEN 10 AND 100 ORDER BY total DESC",
    )
    .expect("computed column compiles in WHERE and ORDER BY");

    // ORDER BY resolved to the computed column's ordinal (`total` is column 3).
    let order = plan.order_by.expect("ORDER BY was compiled");
    assert_eq!((order.column, order.descending), (3, true));

    // The predicate evaluates over the stored (derived) values: totals are
    // 10, 20, 5 — only the first two match.
    let snapshot = store.snapshot();
    let table = store.table_id("OrderLine").unwrap();
    let matched: usize = snapshot
        .scan(table)
        .unwrap()
        .filter(|row| plan.matches(row))
        .count();
    assert_eq!(matched, 2, "WHERE filtered on the derived column");
}
