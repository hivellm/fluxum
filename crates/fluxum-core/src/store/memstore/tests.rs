use super::*;
use crate::schema::{ColumnSchema, FluxType, TableAccess, VisibilityRule};

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "note",
        ty: FluxType::Str,
    },
];

/// A minimal table schema literal (const so statics can be built from it).
const fn table(
    name: &'static str,
    columns: &'static [ColumnSchema],
    auto_inc: Option<u16>,
    indexes: &'static [IndexSchema],
) -> TableSchema {
    TableSchema {
        name,
        columns,
        primary_key: &[0],
        auto_inc,
        access: TableAccess::Private,
        partition_by: None,
        unique: &[],
        indexes,
        visibility: VisibilityRule::PublicAll,
    }
}

static ITEM: TableSchema = table("CovItem", ITEM_COLS, None, &[]);

fn item_store() -> (MemStore, TableId) {
    let schema = Schema::from_tables([&ITEM]).unwrap_or_else(|e| panic!("{e}"));
    let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
    let table = TableId::of("CovItem");
    (store, table)
}

fn item_values(id: u64, note: &str) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::Str(note.into())]
}

fn item_pk(id: u64) -> PkBytes {
    encode_pk_values(&ITEM, &[RowValue::U64(id)]).unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn a_zero_allocation_step_is_rejected() {
    let schema = Schema::from_tables([&ITEM]).unwrap_or_else(|e| panic!("{e}"));
    let err = match MemStore::with_options(
        &schema,
        StoreOptions {
            auto_inc_allocation_step: 0,
            ..StoreOptions::default()
        },
    ) {
        Ok(_) => panic!("step 0 accepted"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("must be >= 1 (STG-040)"), "{err}");
}

#[test]
fn colliding_table_ids_are_rejected_with_both_names() {
    // "plumless" and "buckeroo" are the classic IEEE CRC32 collision.
    static COLS: &[ColumnSchema] = &[ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    }];
    static PLUMLESS: TableSchema = TableSchema {
        name: "plumless",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Private,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };
    static BUCKEROO: TableSchema = TableSchema {
        name: "buckeroo",
        ..PLUMLESS
    };
    assert_eq!(
        TableId::of("plumless"),
        TableId::of("buckeroo"),
        "collision precondition"
    );
    let schema = Schema::from_tables([&PLUMLESS, &BUCKEROO]).unwrap_or_else(|e| panic!("{e}"));
    let err = match MemStore::new(&schema) {
        Ok(_) => panic!("colliding table ids accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("TableId collision"), "{err}");
    assert!(
        err.contains("plumless") && err.contains("buckeroo"),
        "{err}"
    );
    assert!(err.contains("STG-050"), "{err}");
}

#[test]
fn install_recovered_rejects_tx_id_zero_and_wrong_coverage() {
    let (store, _) = item_store();
    let good = (*store.snapshot().state).clone();
    let err = match store.install_recovered(good, 0) {
        Ok(()) => panic!("tx id 0 accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("next_tx_id must be >= 1"), "{err}");

    let empty = CommittedState::detached(HashMap::new());
    let err = match store.install_recovered(empty, 5) {
        Ok(()) => panic!("empty recovered state accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("does not cover exactly"), "{err}");
}

#[test]
fn a_delete_op_without_a_committed_row_is_an_invariant_breach() {
    let (store, table) = item_store();
    let mut tx = store.begin();
    // Corrupt TxState directly: a Delete op for a pk that was never
    // committed (the public delete() path cannot produce this).
    tx.state
        .tables
        .entry(table)
        .or_default()
        .insert(item_pk(1), PendingOp::Delete);
    let err = match tx.insert(table, item_values(1, "x")) {
        Ok(_) => panic!("insert over a phantom Delete op succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("Delete op for pk absent from CommittedState"),
        "{err}"
    );
}

#[test]
fn commit_reports_ops_for_a_table_missing_from_committed_state() {
    let (store, _) = item_store();
    let mut tx = store.begin();
    tx.state
        .tables
        .entry(TableId::from_raw(0xDEAD_BEEF))
        .or_default()
        .insert(item_pk(1), PendingOp::Delete);
    let err = match tx.commit() {
        Ok(_) => panic!("commit over an unknown table succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("touched table") && err.contains("missing from"),
        "{err}"
    );
}

#[test]
fn commit_reports_a_delete_op_for_a_missing_committed_row() {
    let (store, table) = item_store();
    let mut tx = store.begin();
    tx.state
        .tables
        .entry(table)
        .or_default()
        .insert(item_pk(7), PendingOp::Delete);
    let err = match tx.commit() {
        Ok(_) => panic!("commit of a phantom Delete succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("Delete/Update op for pk absent from CommittedState"),
        "{err}"
    );
}

#[test]
fn commit_reports_a_dirty_auto_inc_table_missing_from_committed_state() {
    let (store, _) = item_store();
    let mut tx = store.begin();
    tx.writer.high_water_dirty.insert(TableId::from_raw(0xBEEF));
    let err = match tx.commit() {
        Ok(_) => panic!("commit with a bogus dirty auto-inc table succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("auto-inc table") && err.contains("missing from"),
        "{err}"
    );
}

#[test]
fn assign_auto_inc_rejects_a_non_u64_auto_inc_column() {
    // A schema whose #[auto_inc] ordinal points at a Str column — the
    // registry rejects this (DM-004); the writer still guards it.
    static BAD: TableSchema = TableSchema {
        name: "CovBadAutoInc",
        columns: ITEM_COLS,
        primary_key: &[0],
        auto_inc: Some(1),
        access: TableAccess::Private,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };
    let (store, table) = item_store();
    let mut tx = store.begin();
    let mut values = item_values(1, "not-a-counter");
    let err = match tx.assign_auto_inc(table, &BAD, &mut values) {
        Ok(()) => panic!("Str auto-inc column accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("#[auto_inc] column must be u64"), "{err}");
    assert!(err.contains("Str"), "{err}");
}

#[test]
fn migrate_rows_sees_replacements_and_skips_deletes_and_inserts() {
    let (store, table) = item_store();
    let mut tx = store.begin();
    for id in 1..=3u64 {
        tx.insert(table, item_values(id, "old"))
            .unwrap_or_else(|e| panic!("{e}"));
    }
    tx.commit().unwrap_or_else(|e| panic!("{e}"));

    let mut tx = store.begin();
    tx.migrate_replace(table, item_pk(2), item_values(2, "rewritten"))
        .unwrap_or_else(|e| panic!("{e}"));
    tx.delete(table, &[RowValue::U64(3)])
        .unwrap_or_else(|e| panic!("{e}"));
    tx.insert(table, item_values(4, "fresh"))
        .unwrap_or_else(|e| panic!("{e}"));

    let rows = tx.migrate_rows(table).unwrap_or_else(|e| panic!("{e}"));
    let ids: Vec<(PkBytes, RowValue)> = rows
        .iter()
        .map(|(pk, row)| (pk.clone(), row.values()[1].clone()))
        .collect();
    assert_eq!(
        ids,
        vec![
            (item_pk(1), RowValue::Str("old".into())),
            (item_pk(2), RowValue::Str("rewritten".into())),
        ],
        "replacements compose; deletes and fresh inserts are excluded"
    );
}

#[test]
fn migrate_replace_requires_a_committed_row() {
    let (store, table) = item_store();
    let mut tx = store.begin();
    let err = match tx.migrate_replace(table, item_pk(99), item_values(99, "x")) {
        Ok(()) => panic!("migrate_replace of an absent pk succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("has no committed row at pk"), "{err}");
    assert!(err.contains("SPEC-010"), "{err}");
}

// --- spatial surface -------------------------------------------------

static SPOT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F64,
    },
];

static SPOT: TableSchema = TableSchema {
    name: "CovSpot",
    columns: SPOT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[1, 2],
    }],
    visibility: VisibilityRule::PublicAll,
};

#[test]
fn index_id_ignores_spatial_declarations() {
    let schema = Schema::from_tables([&SPOT]).unwrap_or_else(|e| panic!("{e}"));
    let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
    // The spatial declaration over (x, y) is not a B-tree index.
    assert_eq!(store.index_id("CovSpot", &["x", "y"]), None);
    assert_eq!(store.index_id("CovSpot", &["x"]), None);
}

#[test]
fn tx_spatial_queries_read_the_committed_snapshot() {
    let schema = Schema::from_tables([&SPOT]).unwrap_or_else(|e| panic!("{e}"));
    let store = MemStore::new(&schema).unwrap_or_else(|e| panic!("{e}"));
    let table = TableId::of("CovSpot");
    let mut tx = store.begin();
    tx.insert(
        table,
        vec![RowValue::U64(1), RowValue::F64(1.0), RowValue::F64(2.0)],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    tx.insert(
        table,
        vec![RowValue::U64(2), RowValue::F64(50.0), RowValue::F64(50.0)],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    tx.commit().unwrap_or_else(|e| panic!("{e}"));

    let tx = store.begin();
    let near = tx
        .spatial_region(table, Rect::new(0.0, 0.0, 10.0, 10.0))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(near.len(), 1);
    assert_eq!(near[0].values()[0], RowValue::U64(1));
    let far = tx
        .spatial_radius(table, 50.0, 50.0, 1.0)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(far.len(), 1);
    assert_eq!(far[0].values()[0], RowValue::U64(2));
}

// --- schema-shape guards over the index builders ----------------------

#[test]
fn a_second_spatial_declaration_is_rejected() {
    static DOUBLE: TableSchema = TableSchema {
        indexes: &[
            IndexSchema::Spatial {
                kind: SpatialKind::QuadTree,
                columns: &[1, 2],
            },
            IndexSchema::Spatial {
                kind: SpatialKind::RTree,
                columns: &[1, 2, 1, 2],
            },
        ],
        ..SPOT
    };
    let dir = tempfile::Builder::new()
        .prefix("fluxum-spx-build-")
        .tempdir()
        .unwrap_or_else(|e| panic!("{e}"));
    let pager = Pager::open(
        dir.path(),
        PagerOptions {
            shard_id: 0,
            page_size: 4096,
            pool_capacity_bytes: 64 * 4096,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression: crate::config::PageCompression::None,
            compression_min_bytes: 1024,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let err = match build_spatial_index(
        &DOUBLE,
        &StoreOptions::default(),
        &pager,
        TableId::of("Spot"),
    ) {
        Ok(_) => panic!("two spatial declarations accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("multiple #[spatial(...)]"), "{err}");
    assert!(err.contains("SPEC-008"), "{err}");
}

#[test]
fn btree_index_builders_guard_ordinals_and_id_collisions() {
    let dir = tempfile::Builder::new()
        .prefix("fluxum-idx-build-")
        .tempdir()
        .unwrap_or_else(|e| panic!("{e}"));
    let pager = Pager::open(
        dir.path(),
        PagerOptions {
            shard_id: 0,
            page_size: 4096,
            pool_capacity_bytes: 64 * 4096,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression: crate::config::PageCompression::None,
            compression_min_bytes: 1024,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let table_id = TableId::of("Item");

    static OUT_OF_RANGE: TableSchema = TableSchema {
        indexes: &[IndexSchema::BTree { columns: &[9] }],
        ..ITEM
    };
    let err = match build_btree_indexes(&OUT_OF_RANGE, &pager, table_id) {
        Ok(_) => panic!("out-of-range index ordinal accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("ordinal 9 out of range"), "{err}");

    static DUPLICATED: TableSchema = TableSchema {
        indexes: &[
            IndexSchema::BTree { columns: &[1] },
            IndexSchema::BTree { columns: &[1] },
        ],
        ..ITEM
    };
    let err = match build_btree_indexes(&DUPLICATED, &pager, table_id) {
        Ok(_) => panic!("duplicate index declarations accepted"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("IndexId collision"), "{err}");
    assert!(err.contains("STG-051"), "{err}");
}
