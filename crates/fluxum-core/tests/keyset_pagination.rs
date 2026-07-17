//! SPEC-018 §5/§6 (QP-030/031/032/040/041/042) — range operators and keyset
//! pagination: `<`/`>`/`<=`/`>=` compile and fold into index bounds,
//! `AFTER (value, pk)` pages a stable snapshot with no gaps/overlaps in
//! either direction, ties resolve via the PK tiebreak, and the rejected
//! constructs (`OFFSET`, `!=`, `OR`, mis-typed comparisons) stay 400s.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::sql::{AccessPath, compile, explain};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;
use fluxum_protocol::FluxBinReader;

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "category",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "price",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "flag",
        ty: FluxType::Bool,
    },
];
static ITEM: TableSchema = TableSchema {
    name: "Item",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::BTree { columns: &[1, 2] }],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&ITEM]).unwrap())
}

/// 500 rows in one category: price = (id % 50) * 10 — every price value is
/// shared by 10 rows, so the PK tiebreak is genuinely exercised.
fn seeded(schema: &Schema) -> MemStore {
    let store = MemStore::new(schema).unwrap();
    let item = store.table_id("Item").unwrap();
    let mut tx = store.begin();
    for id in 1..=500u64 {
        tx.insert(
            item,
            vec![
                RowValue::U64(id),
                RowValue::Str("c".into()),
                RowValue::U64((id % 50) * 10),
                RowValue::Bool(id % 2 == 0),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    store
}

fn admin() -> Subscriber {
    Subscriber::server_peer(Identity::from_bytes([9; 32]))
}

/// Decode fan-out rows to `(id, price)` pairs.
fn page_rows(manager: &SubscriptionManager, store: &MemStore, sql: &str) -> Vec<(u64, u64)> {
    let initial = manager
        .snapshot_result(admin(), sql, &store.snapshot())
        .unwrap();
    initial.tables[0]
        .inserts
        .iter()
        .map(|bytes| {
            let mut r = FluxBinReader::new(bytes);
            let id = r.read_u64().unwrap();
            let _category = r.read_str().unwrap();
            let price = r.read_u64().unwrap();
            (id, price)
        })
        .collect()
}

// --- QP-030/032: range operators ---------------------------------------------

#[test]
fn comparison_operators_compile_fold_and_filter_correctly() {
    let schema = schema();
    // A same-column pair folds into one interval, exactly like BETWEEN.
    let folded = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c' AND price >= 100 AND price <= 300",
    )
    .unwrap();
    let AccessPath::IndexScan(scan) = &folded.access else {
        panic!("expected IndexScan: {:?}", folded.access);
    };
    assert_eq!(
        (scan.lower.clone(), scan.upper.clone()),
        (
            Bound::Included(RowValue::U64(100)),
            Bound::Included(RowValue::U64(300)),
        ),
        "pair folded to a closed interval (QP-030)"
    );
    assert!(folded.residual.is_none(), "both comparisons pushed down");

    // Strict bounds stay strict; the tighter of overlapping bounds wins.
    let strict = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c' AND price > 100 AND price >= 90 AND price < 300",
    )
    .unwrap();
    let AccessPath::IndexScan(scan) = &strict.access else {
        panic!("expected IndexScan");
    };
    assert_eq!(scan.lower, Bound::Excluded(RowValue::U64(100)));
    assert_eq!(scan.upper, Bound::Excluded(RowValue::U64(300)));

    // Executed result matches the arithmetic.
    let store = seeded(&schema);
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    let rows = page_rows(
        &manager,
        &store,
        "SELECT * FROM Item WHERE category = 'c' AND price > 100 AND price < 140",
    );
    assert_eq!(rows.len(), 30, "prices 110/120/130 → 3 values × 10 rows");
    assert!(rows.iter().all(|(_, p)| *p > 100 && *p < 140));

    // QP-032: no order over Bool; rejected constructs stay rejected.
    let err = compile(&schema, "SELECT * FROM Item WHERE flag > TRUE").unwrap_err();
    assert!(err.to_string().contains("QP-032"), "{err}");
    let err = compile(&schema, "SELECT * FROM Item WHERE price != 3").unwrap_err();
    assert!(err.to_string().contains("!="), "{err}");
    let err =
        compile(&schema, "SELECT * FROM Item WHERE price = 1 OR price = 2").unwrap_err();
    assert!(err.to_string().contains("OR"), "{err}");
}

// --- QP-040/041/042: keyset pagination -----------------------------------------

/// Page through the whole category with `AFTER`, asserting exactly-once
/// coverage with no gaps or overlaps under a stable snapshot.
fn page_through(descending: bool) {
    let schema = schema();
    let store = seeded(&schema);
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    let direction = if descending { "DESC" } else { "ASC" };

    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let mut ordered: Vec<(u64, u64)> = Vec::new();
    let mut cursor: Option<(u64, u64)> = None; // (price, id)
    let mut pages = 0;
    loop {
        let after = cursor
            .map(|(price, id)| format!(" AFTER ({price}, {id})"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM Item WHERE category = 'c' ORDER BY price {direction} LIMIT 37{after}"
        );
        let rows = page_rows(&manager, &store, &sql);
        if rows.is_empty() {
            break;
        }
        pages += 1;
        assert!(pages <= 15, "runaway paging loop");
        for (id, price) in &rows {
            assert!(seen.insert(*id), "row {id} delivered twice (overlap)");
            ordered.push((*id, *price));
        }
        let (last_id, last_price) = {
            let (id, price) = rows.last().copied().unwrap();
            (id, price)
        };
        cursor = Some((last_price, last_id));
        if rows.len() < 37 {
            break;
        }
    }
    assert_eq!(seen.len(), 500, "every row exactly once (no gaps)");
    assert_eq!(pages, 14, "500 rows / 37 per page");
    // The concatenated pages are monotone in the ORDER BY column; within
    // equal values the total order is the PK's index byte order (QP-041) —
    // deterministic and cursor-unambiguous, exactly-once proves it holds
    // across every page boundary.
    for window in ordered.windows(2) {
        let (_, price_a) = window[0];
        let (_, price_b) = window[1];
        if descending {
            assert!(price_a >= price_b, "order violated: {price_a} → {price_b}");
        } else {
            assert!(price_a <= price_b, "order violated: {price_a} → {price_b}");
        }
    }
}

#[test]
fn keyset_pages_cover_every_row_exactly_once_ascending() {
    page_through(false);
}

#[test]
fn keyset_pages_cover_every_row_exactly_once_descending() {
    page_through(true);
}

#[test]
fn cursor_requires_index_served_order_and_offset_stays_rejected() {
    let schema = schema();
    // AFTER without ORDER BY.
    let err = compile(&schema, "SELECT * FROM Item LIMIT 5 AFTER (10, 20)").unwrap_err();
    assert!(err.to_string().contains("AFTER requires ORDER BY"), "{err}");
    // AFTER with a non-index-served order.
    let err = compile(
        &schema,
        "SELECT * FROM Item ORDER BY id ASC LIMIT 5 AFTER (10, 20)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("indexed column"), "{err}");
    // OFFSET is deliberately absent, with a pointer to keyset.
    let err = compile(
        &schema,
        "SELECT * FROM Item ORDER BY price ASC LIMIT 5 OFFSET 100",
    )
    .unwrap_err();
    assert!(err.to_string().contains("OFFSET") && err.to_string().contains("keyset"), "{err}");

    // QP-041: the explicit tiebreak must be the PK, same direction.
    compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c' ORDER BY price ASC, id ASC LIMIT 5",
    )
    .unwrap();
    let err = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c' ORDER BY price ASC, price ASC",
    )
    .unwrap_err();
    assert!(err.to_string().contains("primary key"), "{err}");
    let err = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c' ORDER BY price ASC, id DESC",
    )
    .unwrap_err();
    assert!(err.to_string().contains("direction"), "{err}");
}

#[test]
fn cursor_pages_have_distinct_query_hashes_and_explain_shows_the_cursor() {
    let schema = schema();
    let base = "SELECT * FROM Item WHERE category = 'c' ORDER BY price ASC LIMIT 50";
    let page1 = compile(&schema, base).unwrap();
    let page2 = compile(&schema, &format!("{base} AFTER (250, 123)")).unwrap();
    assert_ne!(
        page1.query_hash, page2.query_hash,
        "a cursor page is its own normalized query (SUB-020)"
    );
    assert!(page2.cursor.is_some());

    let report = explain(&schema, &format!("{base} AFTER (250, 123)")).unwrap();
    assert_eq!(report["access"]["kind"], "index_scan");
    assert!(report["cursor"]["order_value"].as_str().unwrap().contains("250"));
    assert!(
        report["access"]["lower"].as_str().unwrap().contains("250"),
        "the cursor tightened the scan's lower bound: {report}"
    );
}
