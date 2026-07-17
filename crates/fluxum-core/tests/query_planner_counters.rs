//! SPEC-018 acceptance 2/3 (QP-010/020/021) — the rows-scanned and
//! sort-invoked counters prove range pushdown and the no-sort index-ordered
//! top-N. One test in this binary: the counters are process-global, so a
//! dedicated process keeps the deltas exact.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{
    QUERY_ROWS_SCANNED, QUERY_SORTS, Subscriber, SubscriptionLimits, SubscriptionManager,
};
use fluxum_core::types::Identity;

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

#[test]
fn pushdown_scans_the_bounded_range_and_top_n_never_sorts() {
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let item = store.table_id("Item").unwrap();
    // 10 categories × 100 prices = 1,000 rows.
    let mut tx = store.begin();
    let mut id = 0u64;
    for cat in 0..10u64 {
        for price in 0..100u64 {
            id += 1;
            tx.insert(
                item,
                vec![
                    RowValue::U64(id),
                    RowValue::Str(format!("c{cat}")),
                    RowValue::U64(price),
                    RowValue::I64(i64::try_from(id % 5).unwrap()),
                ],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    let admin = || Subscriber::server_peer(Identity::from_bytes([9; 32]));
    let run = |sql: &str| -> (usize, u64, u64) {
        let scanned0 = QUERY_ROWS_SCANNED.load(Ordering::Relaxed);
        let sorts0 = QUERY_SORTS.load(Ordering::Relaxed);
        let initial = manager
            .snapshot_result(admin(), sql, &store.snapshot())
            .unwrap();
        (
            initial.tables[0].inserts.len(),
            QUERY_ROWS_SCANNED.load(Ordering::Relaxed) - scanned0,
            QUERY_SORTS.load(Ordering::Relaxed) - sorts0,
        )
    };

    // Acceptance 2: the range pushdown touches only the (c5, [20,40]) key
    // range — 21 rows of 1,000 — with the residual applied to those alone.
    let (rows, scanned, _) = run(
        "SELECT * FROM Item WHERE category = 'c5' AND price BETWEEN 20 AND 40 \
         AND listed_at BETWEEN 0 AND 4",
    );
    assert_eq!(rows, 21);
    assert_eq!(scanned, 21, "O(bounded range), not O(1,000)");

    // Acceptance 3: the index-ordered top-N stops after LIMIT rows and
    // never invokes the in-RAM sort.
    let (rows, scanned, sorts) = run(
        "SELECT * FROM Item WHERE category = 'c5' AND price BETWEEN 20 AND 90 \
         ORDER BY price ASC LIMIT 5",
    );
    assert_eq!(rows, 5);
    assert_eq!(sorts, 0, "index-served order skips the sort (QP-020)");
    assert_eq!(scanned, 5, "early stop after n rows (QP-021)");

    // DESC via the reverse walk: bounded range read, still no sort.
    let (rows, scanned, sorts) = run(
        "SELECT * FROM Item WHERE category = 'c5' AND price BETWEEN 20 AND 90 \
         ORDER BY price DESC LIMIT 5",
    );
    assert_eq!(rows, 5);
    assert_eq!(sorts, 0, "reverse walk serves DESC");
    assert!(scanned <= 71, "bounded by the key range, not the table");

    // A non-index-served order still materializes + sorts (unchanged path).
    let (rows, _, sorts) = run(
        "SELECT * FROM Item WHERE category = 'c5' AND price BETWEEN 20 AND 40 \
         ORDER BY listed_at ASC LIMIT 5",
    );
    assert_eq!(rows, 5);
    assert_eq!(sorts, 1, "non-served order sorts as before");

    // A full scan touches the whole table (the baseline the index beats).
    let (_, scanned, _) = run("SELECT * FROM Item WHERE listed_at = 3");
    assert_eq!(scanned, 1_000, "FullScan baseline");

    // SPEC-018 QP-040 acceptance 6: page N+1 through an AFTER cursor is a
    // bounded index seek — rows-scanned ≈ page size, independent of N.
    // Page deep into the category (page 8 of 10) and compare with page 1.
    let (rows, first_page_scanned, sorts) =
        run("SELECT * FROM Item WHERE category = 'c7' ORDER BY price ASC LIMIT 10");
    assert_eq!((rows, sorts), (10, 0));
    let (rows, deep_page_scanned, sorts) = run(
        "SELECT * FROM Item WHERE category = 'c7' ORDER BY price ASC LIMIT 10 AFTER (69, 770)",
    );
    assert_eq!((rows, sorts), (10, 0));
    assert!(
        deep_page_scanned <= first_page_scanned + 1,
        "deep page cost ({deep_page_scanned}) ≈ page size, not O(N · page) \
         (first page: {first_page_scanned})"
    );
}
