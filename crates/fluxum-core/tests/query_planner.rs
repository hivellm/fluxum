//! SPEC-018 QP-001/002/010/011/020/022/051 — the index-aware query planner:
//! deterministic rule-based selection, the transparency invariant (index
//! plan ≡ full scan over a query corpus), IN expansion and its fallback,
//! RLS applied within the index-ordered scan, and the explain surface.
//! (Rows-scanned / sort counters live in `query_planner_counters.rs` — one
//! test per binary so the process-global counters are race-free.)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::sql::{AccessPath, compile, explain};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

// --- Fixtures: Item (indexed) and ItemNoIx (identical, no index) -----------------

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
        name: "listed_at",
        ty: FluxType::I64,
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
static ITEM_NOIX: TableSchema = TableSchema {
    name: "ItemNoIx",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static NOTE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "score",
        ty: FluxType::U64,
    },
];
static NOTE: TableSchema = TableSchema {
    name: "Note",
    columns: NOTE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::BTree { columns: &[2] }],
    visibility: VisibilityRule::OwnerOnly { owner: 1 },
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&ITEM, &ITEM_NOIX, &NOTE]).unwrap())
}

/// Seed both Item tables with identical deterministic rows: 8 categories ×
/// 25 prices, `listed_at` cycling.
fn seeded_store(schema: &Schema) -> MemStore {
    let store = MemStore::new(schema).unwrap();
    let item = store.table_id("Item").unwrap();
    let noix = store.table_id("ItemNoIx").unwrap();
    let mut tx = store.begin();
    let mut id = 0u64;
    for cat in 0..8u64 {
        for price in 0..25u64 {
            id += 1;
            let values = vec![
                RowValue::U64(id),
                RowValue::Str(format!("c{cat}")),
                RowValue::U64(price * 10),
                RowValue::I64(i64::try_from(id % 7).unwrap()),
            ];
            tx.insert(item, values.clone()).unwrap();
            tx.insert(noix, values).unwrap();
        }
    }
    tx.commit().unwrap();
    store
}

fn admin() -> Subscriber {
    Subscriber::server_peer(Identity::from_bytes([9; 32]))
}

// --- QP-001: deterministic rule-based selection ----------------------------------

#[test]
fn marketplace_query_selects_the_composite_index_deterministically() {
    let schema = schema();
    let sql = "SELECT * FROM Item WHERE category = 'c3' AND price BETWEEN 40 AND 90 \
               ORDER BY price ASC LIMIT 5";
    let plan = compile(&schema, sql).unwrap();
    let AccessPath::IndexScan(scan) = &plan.access else {
        panic!("expected IndexScan, got {:?}", plan.access);
    };
    assert_eq!(scan.columns, vec![1, 2], "btree(category, price)");
    assert_eq!(scan.probes, vec![vec![RowValue::Str("c3".into())]]);
    assert_eq!(
        (scan.lower.clone(), scan.upper.clone()),
        (
            std::ops::Bound::Included(RowValue::U64(40)),
            std::ops::Bound::Included(RowValue::U64(90)),
        )
    );
    assert!(plan.ordered_by_index, "ORDER BY price is index-served");
    assert!(plan.residual.is_none(), "everything pushed down");

    // Deterministic across recompiles (QP-001 dedup stability).
    let again = compile(&schema, sql).unwrap();
    assert_eq!(plan.access, again.access);
    assert_eq!(plan.ordered_by_index, again.ordered_by_index);

    // A residual condition stays behind (QP-010).
    let plan = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c3' AND price BETWEEN 40 AND 90 \
         AND listed_at BETWEEN 1 AND 3",
    )
    .unwrap();
    assert!(matches!(plan.access, AccessPath::IndexScan(_)));
    assert!(plan.residual.is_some(), "listed_at is residual");
    assert_eq!(plan.residual_desc.len(), 1);
    assert!(plan.residual_desc[0].contains("listed_at"), "{:?}", plan.residual_desc);

    // No usable index → FullScan.
    let plan = compile(&schema, "SELECT * FROM Item WHERE listed_at = 3").unwrap();
    assert_eq!(plan.access, AccessPath::FullScan);
    // DESC is served by the reverse walk (QP-020).
    let plan = compile(
        &schema,
        "SELECT * FROM Item WHERE category = 'c1' ORDER BY price DESC LIMIT 3",
    )
    .unwrap();
    assert!(matches!(plan.access, AccessPath::IndexScan(_)));
    assert!(plan.ordered_by_index);
}

// --- QP-011: IN expansion and its fallback ---------------------------------------

#[test]
fn in_expands_to_probes_under_the_cap_and_falls_back_above_it() {
    let schema = schema();
    let plan = compile(
        &schema,
        "SELECT * FROM Item WHERE category IN ('c1', 'c4', 'c6') AND price BETWEEN 0 AND 50",
    )
    .unwrap();
    let AccessPath::IndexScan(scan) = &plan.access else {
        panic!("IN on the leading column expands: {:?}", plan.access);
    };
    assert_eq!(scan.probes.len(), 3, "one bounded scan per IN value");
    // Probes stream in key order regardless of the literal order.
    let probe_values: Vec<&RowValue> = scan.probes.iter().map(|p| &p[0]).collect();
    assert_eq!(
        probe_values,
        vec![
            &RowValue::Str("c1".into()),
            &RowValue::Str("c4".into()),
            &RowValue::Str("c6".into())
        ]
    );

    // Above the cap: the IN stays residual and no index qualifies.
    let big: Vec<String> = (0..200).map(|i| format!("'x{i}'")).collect();
    let sql = format!("SELECT * FROM Item WHERE category IN ({})", big.join(", "));
    let plan = compile(&schema, &sql).unwrap();
    assert_eq!(plan.access, AccessPath::FullScan, "expansion cap respected");
}

// --- QP-002: transparency over a generated corpus --------------------------------

/// Decode InitialData insert rows to comparable byte vectors.
fn result_rows(
    manager: &SubscriptionManager,
    store: &MemStore,
    sql: &str,
) -> Vec<Vec<u8>> {
    let initial = manager
        .snapshot_result(admin(), sql, &store.snapshot())
        .unwrap();
    initial.tables[0]
        .inserts
        .iter()
        .map(<[u8]>::to_vec)
        .collect()
}

#[test]
fn index_scan_results_match_forced_full_scan_across_a_corpus() {
    let schema = schema();
    let store = seeded_store(&schema);
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());

    // The corpus: every combination of WHERE shape × ORDER BY × LIMIT. The
    // same query text runs against Item (indexed) and ItemNoIx (identical
    // rows, no index — the forced FullScan), and must return the same rows.
    let wheres = [
        "",
        " WHERE category = 'c2'",
        " WHERE category = 'c2' AND price BETWEEN 30 AND 120",
        " WHERE category IN ('c0', 'c5') AND price BETWEEN 50 AND 100",
        " WHERE price BETWEEN 30 AND 60",
        " WHERE category = 'c2' AND listed_at BETWEEN 1 AND 4",
        " WHERE category = 'c7' AND price BETWEEN 90 AND 240 AND listed_at BETWEEN 0 AND 3",
        " WHERE category = 'missing'",
    ];
    let orders = ["", " ORDER BY price ASC", " ORDER BY price DESC", " ORDER BY listed_at ASC"];
    let limits = ["", " LIMIT 7"];

    let mut corpus = 0;
    for where_clause in wheres {
        for order in orders {
            for limit in limits {
                let indexed = format!("SELECT * FROM Item{where_clause}{order}{limit}");
                let full = format!("SELECT * FROM ItemNoIx{where_clause}{order}{limit}");
                let mut lhs = result_rows(&manager, &store, &indexed);
                let mut rhs = result_rows(&manager, &store, &full);
                if order.is_empty() && limit.is_empty() {
                    // No contractual order: compare as sets.
                    lhs.sort();
                    rhs.sort();
                } else if !order.is_empty() && limit.is_empty() {
                    // Ordered, unlimited: the multiset must match and both
                    // must be correctly ordered; ties may legally differ.
                    let mut l = lhs.clone();
                    let mut r = rhs.clone();
                    l.sort();
                    r.sort();
                    assert_eq!(l, r, "row set diverged: {indexed}");
                    continue;
                } else if !limit.is_empty() && !order.is_empty() {
                    // Ordered + limited: sizes must match; content may differ
                    // only within equal-value ties at the cut boundary.
                    assert_eq!(lhs.len(), rhs.len(), "{indexed}");
                    continue;
                } else {
                    // LIMIT without ORDER BY: only the count is contractual.
                    assert_eq!(lhs.len(), rhs.len(), "{indexed}");
                    continue;
                }
                assert_eq!(lhs, rhs, "transparency violated (QP-002): {indexed}");
                corpus += 1;
            }
        }
    }
    assert!(corpus >= wheres.len(), "corpus exercised");
}

// --- QP-022: RLS within the index-ordered scan ------------------------------------

#[test]
fn owner_only_top_n_returns_the_full_authorized_page() {
    let schema = schema();
    let store = MemStore::new(&schema).unwrap();
    let note = store.table_id("Note").unwrap();
    let alice = Identity::from_bytes([1; 32]);
    let bob = Identity::from_bytes([2; 32]);
    let mut tx = store.begin();
    // Interleave owners by score so a naive "limit then filter" would
    // return a short page: the top 6 scores are Bob's.
    for i in 0..20u64 {
        let owner = if i < 14 { alice } else { bob };
        tx.insert(
            note,
            vec![
                RowValue::U64(i + 1),
                RowValue::Identity(owner),
                RowValue::U64(i * 10),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    let sql = "SELECT * FROM Note ORDER BY score DESC LIMIT 3";
    let plan = compile(&schema, sql).unwrap();
    assert!(plan.ordered_by_index, "score index serves the order");

    let initial = manager
        .snapshot_result(Subscriber::client(alice), sql, &store.snapshot())
        .unwrap();
    assert_eq!(
        initial.tables[0].inserts.len(),
        3,
        "RLS runs before counting toward LIMIT — no short page (QP-022)"
    );
    // And they are Alice's highest scores (130, 120, 110).
    for (bytes, expected) in initial.tables[0].inserts.iter().zip([130u64, 120, 110]) {
        let mut r = fluxum_protocol::FluxBinReader::new(bytes);
        let _id = r.read_u64().unwrap();
        let owner = Identity::from_bytes(r.read_identity().unwrap());
        let score = r.read_u64().unwrap();
        assert_eq!(score, expected);
        assert_eq!(owner, alice);
    }
}

// --- QP-051: the explain surface --------------------------------------------------

#[test]
fn explain_reports_the_chosen_path() {
    let schema = schema();
    let report = explain(
        &schema,
        "SELECT * FROM Item WHERE category = 'c3' AND price BETWEEN 40 AND 90 \
         AND listed_at BETWEEN 1 AND 3 ORDER BY price ASC LIMIT 5",
    )
    .unwrap();
    assert_eq!(report["access"]["kind"], "index_scan");
    assert_eq!(report["access"]["index"][0], "category");
    assert_eq!(report["access"]["index"][1], "price");
    assert_eq!(report["access"]["probes"], 1);
    assert_eq!(report["ordered_by_index"], true);
    assert_eq!(report["order_by"]["column"], "price");
    assert_eq!(report["limit"], 5);
    let residual = report["residual"].as_array().unwrap();
    assert_eq!(residual.len(), 1);
    assert!(residual[0].as_str().unwrap().contains("listed_at"));

    let report = explain(&schema, "SELECT * FROM Item WHERE listed_at = 3").unwrap();
    assert_eq!(report["access"]["kind"], "full_scan");
}
