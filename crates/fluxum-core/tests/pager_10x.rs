//! T2.8 paged cold tier + buffer pool acceptance suite (SPEC-015 §12,
//! checklist 1.4/1.5/1.7/1.8/1.9 — the Gate G2 10x-dataset input):
//!
//! - a dataset ≥ 10× the pool capacity is served correctly — point reads,
//!   range scans, secondary-index and spatial-declaration queries, writes —
//!   while pages fault and evict continuously (TIER-070);
//! - the budget is never exceeded: `fluxum_bufferpool_bytes` stays ≤
//!   `fluxum_bufferpool_capacity_bytes` throughout (TIER-003/004 — the RSS
//!   bound itself is asserted by the droplet-profile CI job, NFR-12; here
//!   the enforced accounting invariant is the witness);
//! - an index-dominated workload whose index pages alone exceed the budget
//!   faults and evicts index pages (witnessed by `fluxum_page_reads_total`
//!   with the index flag) while every query stays correct (TIER-050);
//! - every fault-in verifies the page CRC32C — a tampered page is never
//!   served, always `PageCorrupt` (TIER-021/032/062);
//! - content hashes round-trip through evict/fault cycles unchanged
//!   (TIER-063);
//! - with the working set resident, the read path does zero disk I/O
//!   (`fluxum_page_reads_total` constant, TIER-014/NFR-07) and a pool-hit
//!   point lookup stays sub-microsecond (NFR-02, asserted in release
//!   builds).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use fluxum_core::FluxumError;
use fluxum_core::config::PageCompression;
use fluxum_core::index::IndexId;
use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::pager::{ColdTable, Pager, PagerOptions};
use fluxum_core::store::{MemStore, Row, RowValue, TableId};

// --- Hand-built static schema (macro output stand-in, like store_acid.rs) ---

static READING_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "device",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "seq",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "payload",
        ty: FluxType::Bytes,
    },
];

static READING: TableSchema = TableSchema {
    name: "Reading",
    columns: READING_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[
        // Composite secondary index (TIER-050).
        IndexSchema::BTree { columns: &[1, 2] },
        // Single-column range index.
        IndexSchema::BTree { columns: &[2] },
        // Spatial declaration: pages through the same linear-key tree
        // (TIER-051).
        IndexSchema::Spatial {
            kind: SpatialKind::QuadTree,
            columns: &[3, 4],
        },
    ],
    visibility: VisibilityRule::PublicAll,
};

/// Tiny deterministic PRNG (splitmix64) — no rand dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn reading(id: u64, rng: &mut Rng, payload_len: usize) -> Vec<RowValue> {
    let device = format!("device-{:03}", id % 40);
    let mut payload = Vec::with_capacity(payload_len);
    while payload.len() < payload_len {
        payload.extend_from_slice(&rng.next().to_le_bytes());
    }
    payload.truncate(payload_len);
    vec![
        RowValue::U64(id),
        RowValue::Str(device),
        RowValue::I64((id as i64) * 7 - 500),
        RowValue::F64((rng.next() % 2_000_000) as f64 - 1_000_000.0),
        RowValue::F64((rng.next() % 2_000_000) as f64 - 1_000_000.0),
        RowValue::Bytes(payload),
    ]
}

/// A pager with a deliberately tiny pool: 64 frames × 4 KiB = 256 KiB.
/// Compression stays off so this suite measures the T2.8 tiering invariants
/// (e.g. the raw 10× on-disk footprint) unchanged; the T2.9 compression
/// suite lives in `page_compression.rs`.
fn tiny_pager(dir: &std::path::Path) -> Arc<Pager> {
    Pager::open(
        dir,
        PagerOptions {
            shard_id: 0,
            page_size: 4096,
            pool_capacity_bytes: 64 * 4096,
            high_watermark: 0.95,
            low_watermark: 0.90,
            compression: PageCompression::None,
            compression_min_bytes: 1024,
        },
    )
    .expect("pager opens")
}

/// Build a committed MemStore with `n` Reading rows.
fn populated_store(n: u64, payload_len: usize) -> (MemStore, TableId) {
    let schema = Schema::from_tables([&READING]).expect("schema assembles");
    let store = MemStore::new(&schema).expect("store builds");
    let table = store.table_id("Reading").expect("table registered");
    let mut rng = Rng(42);
    let mut inserted = 0;
    while inserted < n {
        let mut tx = store.begin();
        for _ in 0..1_000.min(n - inserted) {
            tx.insert(table, reading(inserted, &mut rng, payload_len))
                .expect("insert");
            inserted += 1;
        }
        tx.commit().expect("commit");
    }
    (store, table)
}

fn assert_budget_held(pager: &Pager) {
    let snap = pager.metrics().snapshot();
    assert!(
        snap.bufferpool_bytes <= snap.bufferpool_capacity_bytes,
        "pool accounting exceeded capacity: {snap:?}"
    );
    assert!(
        pager.pool().occupied_frames() <= pager.pool().capacity_frames(),
        "pool frames exceeded capacity"
    );
}

// --- checklist 1.8 / 1.9: the 10x-dataset correctness suite -----------------

#[test]
fn ten_x_dataset_is_served_correctly_under_a_tiny_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (store, table) = populated_store(12_000, 200);
    let snap = store.snapshot();

    let pager = tiny_pager(dir.path());
    let cold = ColdTable::spill_snapshot(&pager, &snap, table).expect("spill");
    pager.flush().expect("flush");
    assert_budget_held(&pager);

    // The on-disk dataset really is ≥ 10× the pool capacity (TIER-070).
    let capacity = pager.metrics().snapshot().bufferpool_capacity_bytes;
    let on_disk = pager.coldtier_bytes(table).expect("coldtier bytes");
    assert!(
        on_disk >= 10 * capacity,
        "dataset {on_disk} bytes is not 10x the {capacity}-byte budget"
    );

    // Every committed row is served back correctly through fault/evict
    // cycles (uniform coverage: every pk, not a sample).
    for id in 0..12_000u64 {
        let expected = snap
            .query_pk(table, &[RowValue::U64(id)])
            .expect("hot read")
            .expect("row exists hot");
        let got = cold
            .get(&[RowValue::U64(id)])
            .expect("cold read")
            .unwrap_or_else(|| panic!("row {id} missing from the cold tier"));
        assert_eq!(got, expected, "row {id} diverged");
    }
    assert_budget_held(&pager);

    // Full scan matches the snapshot scan exactly (same pk-byte order).
    let hot: Vec<Row> = snap.scan(table).expect("hot scan").cloned().collect();
    let cold_rows = cold.scan_all().expect("cold scan");
    assert_eq!(cold_rows.len(), hot.len());
    assert_eq!(cold_rows, hot, "full scan diverged");
    assert_budget_held(&pager);

    // Secondary-index queries: equality and composite prefix + range
    // (TIER-050), against the hot-tier oracle.
    let by_device_seq = store
        .index_id("Reading", &["device", "seq"])
        .expect("composite index");
    let by_seq = store.index_id("Reading", &["seq"]).expect("seq index");
    for device in ["device-000", "device-017", "device-039", "device-999"] {
        let hot: Vec<Row> = snap
            .index_eq(table, by_device_seq, &[RowValue::Str(device.into())])
            .expect("hot index scan")
            .cloned()
            .collect();
        let cold_hits = cold
            .index_eq(by_device_seq, &[RowValue::Str(device.into())])
            .expect("cold index scan");
        assert_eq!(cold_hits, hot, "index_eq({device}) diverged");
    }
    let hot_range: Vec<Row> = snap
        .index_scan(
            table,
            by_seq,
            &[],
            Bound::Included(&RowValue::I64(1_000)),
            Bound::Excluded(&RowValue::I64(9_000)),
        )
        .expect("hot range")
        .cloned()
        .collect();
    let cold_range = cold
        .index_scan(
            by_seq,
            &[],
            Bound::Included(&RowValue::I64(1_000)),
            Bound::Excluded(&RowValue::I64(9_000)),
        )
        .expect("cold range");
    assert_eq!(cold_range, hot_range, "seq range scan diverged");
    assert!(!cold_range.is_empty(), "range scan should hit rows");

    // The spatial declaration pages through the same tree (TIER-051): an
    // x-ordered range over the quadtree's linear key returns exactly the
    // rows whose x falls in the window.
    let spatial = cold
        .index_scan(
            IndexId::of("Reading", &["x", "y"]),
            &[],
            Bound::Included(&RowValue::F64(-100_000.0)),
            Bound::Excluded(&RowValue::F64(100_000.0)),
        )
        .expect("spatial linear scan");
    let oracle = hot
        .iter()
        .filter(|row| {
            matches!(row.value(3), Some(RowValue::F64(x))
                if (-100_000.0..100_000.0).contains(x))
        })
        .count();
    assert_eq!(spatial.len(), oracle, "spatial window diverged");

    // Pages faulted and evicted continuously.
    let m = pager.metrics().snapshot();
    assert!(m.page_reads_total() > 0, "no faults in a 10x run: {m:?}");
    assert!(m.evictions_total() > 0, "no evictions in a 10x run: {m:?}");
    assert_budget_held(&pager);
}

// --- checklist 1.4 / 1.8: index-dominated workload ---------------------------

#[test]
fn index_pages_alone_exceeding_the_budget_fault_and_stay_correct() {
    let dir = tempfile::tempdir().unwrap();
    // Long indexed strings: the (device, seq) index tree alone outweighs
    // the 256 KiB pool while row payloads stay small.
    let (store, table) = {
        let schema = Schema::from_tables([&READING]).expect("schema assembles");
        let store = MemStore::new(&schema).expect("store builds");
        let table = store.table_id("Reading").expect("table registered");
        let mut rng = Rng(7);
        let mut tx = store.begin();
        for id in 0..6_000u64 {
            let mut row = reading(id, &mut rng, 8);
            row[1] = RowValue::Str(format!("very-long-device-name-{:0100}", id % 500));
            tx.insert(table, row).expect("insert");
        }
        tx.commit().expect("commit");
        (store, table)
    };
    let snap = store.snapshot();

    let pager = tiny_pager(dir.path());
    let cold = ColdTable::spill_snapshot(&pager, &snap, table).expect("spill");
    pager.flush().expect("flush");
    pager.evict_all().expect("start cold");

    let capacity = pager.metrics().snapshot().bufferpool_capacity_bytes;
    let by_device_seq = store
        .index_id("Reading", &["device", "seq"])
        .expect("composite index");

    let before = pager.metrics().snapshot();
    let mut total_hits = 0usize;
    for bucket in 0..500u64 {
        let device = format!("very-long-device-name-{bucket:0100}");
        let hot: Vec<Row> = snap
            .index_eq(table, by_device_seq, &[RowValue::Str(device.clone())])
            .expect("hot index scan")
            .cloned()
            .collect();
        let cold_hits = cold
            .index_eq(by_device_seq, &[RowValue::Str(device)])
            .expect("cold index scan");
        assert_eq!(cold_hits, hot, "bucket {bucket} diverged");
        total_hits += cold_hits.len();
        assert_budget_held(&pager);
    }
    assert_eq!(total_hits, 6_000, "every row reached through the index");

    let after = pager.metrics().snapshot();
    let index_reads = after.page_reads_index - before.page_reads_index;
    assert!(
        index_reads > 0,
        "index pages must fault under pressure (TIER-050 witness): {after:?}"
    );
    // The index tree alone is bigger than the whole pool: more index pages
    // were read than the pool can hold at once, proving eviction of index
    // pages (not just data pages).
    assert!(
        index_reads * 4096 > capacity,
        "index fault volume ({index_reads} pages) should exceed the pool"
    );
}

// --- writes through the cold tier, incl. overflow rows ----------------------

#[test]
fn cold_writes_updates_deletes_and_overflow_rows_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let (store, table) = populated_store(3_000, 120);
    let snap = store.snapshot();
    let pager = tiny_pager(dir.path());
    let mut cold = ColdTable::spill_snapshot(&pager, &snap, table).expect("spill");

    // Oracle: id → row values.
    let mut oracle: BTreeMap<u64, Vec<RowValue>> = BTreeMap::new();
    for row in snap.scan(table).expect("scan") {
        let RowValue::U64(id) = row.value(0).unwrap() else {
            panic!("bad pk");
        };
        oracle.insert(*id, row.values().to_vec());
    }

    let mut rng = Rng(99);
    // Inserts beyond the snapshot, including a row whose payload exceeds
    // the leaf inline cap → overflow chain (TIER-026).
    for id in 3_000..3_200u64 {
        let mut row = reading(id, &mut rng, 64);
        if id == 3_100 {
            row[5] = RowValue::Bytes(vec![0xEE; 10_000]); // multi-page chain
        }
        cold.insert(row.clone()).expect("cold insert");
        oracle.insert(id, row);
    }
    // Updates (replacing index entries), one growing into overflow.
    for id in (0..3_000u64).step_by(97) {
        let mut row = oracle.get(&id).unwrap().clone();
        row[1] = RowValue::Str("rewritten-device".into());
        row[5] = RowValue::Bytes(vec![id as u8; if id % 194 == 0 { 5_000 } else { 40 }]);
        cold.insert(row.clone()).expect("cold update");
        oracle.insert(id, row);
    }
    // Deletes.
    for id in (1..3_000u64).step_by(131) {
        assert!(cold.delete(&[RowValue::U64(id)]).expect("cold delete"));
        oracle.remove(&id);
    }
    assert!(!cold.delete(&[RowValue::U64(999_999)]).expect("absent"));

    // Everything faults back correctly after a full eviction.
    pager.flush().expect("flush");
    pager.evict_all().expect("evict all");
    for (id, expected) in &oracle {
        let got = cold
            .get(&[RowValue::U64(*id)])
            .expect("cold read")
            .unwrap_or_else(|| panic!("row {id} missing"));
        assert_eq!(got.values(), &expected[..], "row {id} diverged");
    }
    // Deleted rows are gone.
    assert!(cold.get(&[RowValue::U64(1)]).expect("read").is_none());
    // Updated rows are reachable under the new index key, not the old one.
    let by_device_seq = store
        .index_id("Reading", &["device", "seq"])
        .expect("composite index");
    let rewritten = cold
        .index_eq(by_device_seq, &[RowValue::Str("rewritten-device".into())])
        .expect("index scan");
    let expected_rewritten = oracle
        .values()
        .filter(|row| matches!(&row[1], RowValue::Str(s) if s == "rewritten-device"))
        .count();
    assert_eq!(rewritten.len(), expected_rewritten);
    assert_budget_held(&pager);
}

// --- checklist 1.7: hot-path zero-disk-I/O + latency -------------------------

#[test]
fn resident_working_set_reads_do_zero_disk_io() {
    let dir = tempfile::tempdir().unwrap();
    // Small working set: fits comfortably in the 64-frame pool.
    let (cold, pager) = {
        let (store, table) = populated_store(300, 40);
        let snap = store.snapshot();
        let pager = tiny_pager(dir.path());
        let cold = ColdTable::spill_snapshot(&pager, &snap, table).expect("spill");
        (cold, pager)
    };

    // Warm every page on the read path.
    for id in 0..300u64 {
        assert!(cold.get(&[RowValue::U64(id)]).expect("warm").is_some());
    }

    // TIER-014: with the working set resident, fluxum_page_reads_total is
    // constant across the whole read run — the observable witness that no
    // I/O syscall is issued on the read path (SPEC-015 §10).
    let before = pager.metrics().snapshot();
    let lookups = 50_000u64;
    for i in 0..lookups {
        let id = i % 300;
        let row = cold
            .get(&[RowValue::U64(id)])
            .expect("hot-path read")
            .expect("resident row");
        assert!(matches!(row.value(0), Some(RowValue::U64(v)) if *v == id));
    }

    // NFR-02: a committed-state point lookup that hits the pool completes
    // in < 1 µs. TIER-014 defines the measured path as "frame-table lookup
    // → set referenced bit → read row bytes": time the byte-level lookup
    // through the paged primary tree (a FluxBIN u64 PK is its 8 LE bytes).
    let pks: Vec<[u8; 8]> = (0..300u64).map(|id| id.to_le_bytes()).collect();
    let started = std::time::Instant::now();
    for i in 0..lookups {
        let pk = &pks[(i % 300) as usize];
        let bytes = cold
            .primary_tree()
            .get(pk)
            .expect("hot-path read")
            .expect("resident row");
        assert!(!bytes.is_empty());
    }
    let elapsed = started.elapsed();

    let after = pager.metrics().snapshot();
    assert_eq!(
        after.page_reads_total(),
        before.page_reads_total(),
        "the resident read path issued disk I/O (TIER-014)"
    );
    assert!(after.hits > before.hits, "reads should be pool hits");

    let per_lookup = elapsed / u32::try_from(lookups).unwrap();
    eprintln!("pool-hit point lookup: {per_lookup:?} avg over {lookups} lookups");
    // Asserted in optimized builds only — debug builds carry no performance
    // contract (CI runs the debug suite; the perf gate is the release run).
    #[cfg(not(debug_assertions))]
    assert!(
        per_lookup < std::time::Duration::from_micros(1),
        "pool-hit lookup took {per_lookup:?} (NFR-02 budget is 1 µs)"
    );
}

// --- checklist 1.5: CRC on every fault-in + content-hash round-trips --------

#[test]
fn tampered_pages_are_never_served_and_content_hashes_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let (store, table) = populated_store(2_000, 100);
    let snap = store.snapshot();
    let pager = tiny_pager(dir.path());
    let cold = ColdTable::spill_snapshot(&pager, &snap, table).expect("spill");
    let _ = &store;
    pager.flush().expect("flush");

    // TIER-063: content hashes are stable across evict/fault cycles.
    let root = cold.primary_tree().root_page_id();
    let hash_before = pager.content_hash(table, root).expect("hash");
    pager.evict_all().expect("evict");
    let hash_after = pager.content_hash(table, root).expect("hash after fault");
    assert_eq!(hash_before, hash_after, "evict/fault changed page content");

    // A mutation invalidates the hash (recomputed on demand → new value):
    // use a small table whose root is a leaf, so an insert provably
    // rewrites the hashed page in place.
    {
        let (small_store, small_table) = populated_store(10, 20);
        let small_snap = small_store.snapshot();
        let mut small_cold =
            ColdTable::spill_snapshot(&pager, &small_snap, small_table).expect("spill small");
        let leaf_root = small_cold.primary_tree().root_page_id();
        let h1 = pager.content_hash(small_table, leaf_root).expect("hash");
        small_cold
            .insert(reading(10, &mut Rng(1), 20))
            .expect("insert");
        assert_eq!(
            small_cold.primary_tree().root_page_id(),
            leaf_root,
            "ten rows plus one must still fit the leaf root"
        );
        let h2 = pager.content_hash(small_table, leaf_root).expect("hash");
        assert_ne!(h1, h2, "mutation must change the content hash");
    }

    // TIER-021/032/062: flip one bit in a live extent — the page must fail
    // CRC on fault-in and never be served.
    let (offset, len) = pager
        .page_extent(table, root)
        .expect("extent lookup")
        .expect("root spilled");
    pager.evict_all().expect("evict before tamper");
    {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        let path = dir
            .path()
            .join("shard-0")
            .join(format!("table-{}.pages", table.as_u32()));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("page file opens");
        let target = offset + len / 2;
        file.seek(SeekFrom::Start(target)).expect("seek");
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).expect("read byte");
        byte[0] ^= 0x10;
        file.seek(SeekFrom::Start(target)).expect("seek back");
        file.write_all(&byte).expect("flip bit");
        file.sync_data().expect("sync");
    }
    let err = match pager.fault(table, root) {
        Ok(_) => panic!("tampered page was served"),
        Err(e) => e,
    };
    match err {
        FluxumError::PageCorrupt {
            shard_id,
            table_id,
            page_id,
        } => {
            assert_eq!(shard_id, 0);
            assert_eq!(table_id, table.as_u32());
            assert_eq!(page_id, root);
        }
        other => panic!("expected PageCorrupt, got {other}"),
    }
    // Reads through the table fail the same way — nothing is served off a
    // corrupt page (the caller's transaction rolls back per TIER-062).
    assert!(matches!(
        cold.get(&[RowValue::U64(0)]),
        Err(FluxumError::PageCorrupt { .. })
    ));
}
