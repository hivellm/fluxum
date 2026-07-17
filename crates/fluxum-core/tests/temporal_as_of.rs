//! SPEC-022 §3 (RV-020/021/022) — temporal AS OF reads: superseded row
//! versions stay reachable inside the bounded commit window, `AS OF TX` /
//! `AS OF TIMESTAMP` resolve the committed state at that point, requests
//! past the window are typed 3020s, and RLS applies to historical reads
//! exactly as to live ones.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::sql::as_of_point;
use fluxum_core::store::{AsOfPoint, MemStore, RowValue, StoreOptions};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

static DOC_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "body",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Identity,
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
    visibility: VisibilityRule::OwnerOnly { owner: 2 },
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&DOC]).unwrap())
}

fn store_with_window(window: usize) -> MemStore {
    MemStore::with_options(
        &schema(),
        StoreOptions {
            temporal_window: window,
            ..StoreOptions::default()
        },
    )
    .unwrap()
}

fn commit_body(store: &MemStore, owner: Identity, body: &str) -> u64 {
    let table = store.table_id("Doc").unwrap();
    let mut tx = store.begin();
    tx.upsert(
        table,
        vec![
            RowValue::U64(1),
            RowValue::Str(body.into()),
            RowValue::Identity(owner),
        ],
    )
    .unwrap();
    tx.commit().unwrap().tx_id
}

fn body_as_of(
    manager: &SubscriptionManager,
    store: &MemStore,
    subscriber: Subscriber,
    sql: &str,
) -> Option<String> {
    // The transport's resolution flow: extract the point, resolve the
    // snapshot, evaluate — RLS/masking ride the ordinary path (RV-022).
    let snapshot = match as_of_point(sql).unwrap() {
        Some(point) => store.snapshot_as_of(point).unwrap(),
        None => store.snapshot(),
    };
    let result = manager.query_json(subscriber, sql, &snapshot).unwrap();
    result["rows"]
        .as_array()
        .unwrap()
        .first()
        .map(|row| row["body"].as_str().unwrap().to_owned())
}

#[test]
fn as_of_tx_reads_each_retained_version_and_bounds_the_window() {
    let store = store_with_window(2);
    let manager = SubscriptionManager::new(schema(), SubscriptionLimits::default());
    let owner = Identity::from_bytes([1; 32]);
    let sub = || Subscriber::server_peer(Identity::from_bytes([9; 32]));

    let t1 = commit_body(&store, owner, "v1");
    let t2 = commit_body(&store, owner, "v2");
    let t3 = commit_body(&store, owner, "v3");

    // Window = 2: t2 and t3 retained; t1 pruned (RV-020).
    let read =
        |sql: &str| body_as_of(&manager, &store, sub(), sql);
    assert_eq!(read(&format!("SELECT * FROM Doc AS OF TX {t2}")), Some("v2".into()));
    assert_eq!(read(&format!("SELECT * FROM Doc AS OF TX {t3}")), Some("v3".into()));
    // Newer than every commit = the live state.
    assert_eq!(
        read(&format!("SELECT * FROM Doc AS OF TX {}", t3 + 100)),
        Some("v3".into())
    );
    // Older than the window: typed 3020, never a silent approximation.
    let err = store.snapshot_as_of(AsOfPoint::Tx(t1)).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("temporal window") || msg.contains("retained"), "{msg}");
    assert_eq!(err.query_code(), Some(3020), "{msg}");

    // Timestamp form: far future = live; epoch 0 = out of window.
    assert_eq!(
        read("SELECT * FROM Doc AS OF TIMESTAMP 9999999999999999"),
        Some("v3".into())
    );
    let err = store.snapshot_as_of(AsOfPoint::Timestamp(0)).unwrap_err();
    assert_eq!(err.query_code(), Some(3020));

    // Window 0 disables temporal reads entirely.
    let disabled = store_with_window(0);
    commit_body(&disabled, owner, "x");
    assert_eq!(
        disabled
            .snapshot_as_of(AsOfPoint::Tx(1))
            .unwrap_err()
            .query_code(),
        Some(3020)
    );
}

#[test]
fn as_of_reads_honor_rls_exactly_like_live_reads() {
    let store = store_with_window(8);
    let manager = SubscriptionManager::new(schema(), SubscriptionLimits::default());
    let owner = Identity::from_bytes([1; 32]);
    let stranger = Identity::from_bytes([2; 32]);

    let t1 = commit_body(&store, owner, "draft");
    commit_body(&store, owner, "final");

    let sql = format!("SELECT * FROM Doc AS OF TX {t1}");
    // The owner reads the historical version.
    assert_eq!(
        body_as_of(&manager, &store, Subscriber::client(owner), &sql),
        Some("draft".into())
    );
    // A stranger sees nothing — historically or live (RV-022).
    assert_eq!(
        body_as_of(&manager, &store, Subscriber::client(stranger), &sql),
        None
    );
    assert_eq!(
        body_as_of(&manager, &store, Subscriber::client(stranger), "SELECT * FROM Doc"),
        None
    );

    // Distinct normalized queries → distinct hashes (each point is its own
    // cacheable plan).
    let live = fluxum_core::sql::compile(&schema(), "SELECT * FROM Doc").unwrap();
    let historical = fluxum_core::sql::compile(&schema(), &sql).unwrap();
    assert_ne!(live.query_hash, historical.query_hash);
    assert_eq!(historical.as_of, Some(AsOfPoint::Tx(t1)));
}
