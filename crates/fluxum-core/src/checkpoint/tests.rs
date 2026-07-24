//! T2.3 verification suite (DAG exit tests; SPEC-002 acceptance 4):
//! checkpoint + replay equivalence vs full-log replay, incremental
//! content-addressed writes, manifest/object corruption fallback,
//! non-blocking checkpoints under sustained write load, and recovery after
//! archival-hooked log compaction.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::commitlog::{CommitLog, CommitLogOptions};
use crate::hw::HardwareProfile;
use crate::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, TableAccess, TableSchema, VisibilityRule,
};
use crate::store::{MemStore, Row, RowValue, TableId};
use crate::types::Timestamp;

use super::manifest::{Manifest, decode_manifest, encode_manifest};
use super::{
    CheckpointRepo, DirectoryArchive, LogCompaction, SnapshotWorker, WorkerOptions,
    adaptive_interval_tx, compact_covered, recover,
};

const SHARD: u32 = 3;
const EPOCH: u64 = 1;

// --- fixtures ----------------------------------------------------------------

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
];

static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::BTree { columns: &[1] }],
    visibility: VisibilityRule::PublicAll,
};

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F64,
    },
];

static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0, 1],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn mem_store() -> MemStore {
    MemStore::new(&Schema::from_tables([&USER, &SENSOR]).unwrap()).unwrap()
}

fn user(store: &MemStore) -> TableId {
    store.table_id("User").unwrap()
}

fn sensor(store: &MemStore) -> TableId {
    store.table_id("Sensor").unwrap()
}

/// Commit one mixed transaction and append it to the log. `i` seeds
/// deterministic inserts / deletes / in-place updates across both tables.
async fn commit_step(store: &MemStore, log: &CommitLog, i: i32) -> u64 {
    let u = user(store);
    let s = sensor(store);
    let mut tx = store.begin();
    tx.insert(
        u,
        vec![RowValue::U64(0), RowValue::Str(format!("user-{i}"))],
    )
    .unwrap();
    tx.insert(
        s,
        vec![
            RowValue::I32(i),
            RowValue::I32(-i),
            RowValue::F64(f64::from(i) * 0.5),
        ],
    )
    .unwrap();
    if i > 2 && i % 3 == 0 {
        // Delete an older user row (auto-inc ids are 1-based and dense here).
        tx.delete(u, &[RowValue::U64(u64::try_from(i).unwrap() - 2)])
            .unwrap();
    }
    if i > 1 && i % 4 == 0 {
        // In-place sensor update (delete + reinsert with different content).
        let prev = i - 1;
        tx.delete(s, &[RowValue::I32(prev), RowValue::I32(-prev)])
            .unwrap();
        tx.insert(
            s,
            vec![
                RowValue::I32(prev),
                RowValue::I32(-prev),
                RowValue::F64(999.0),
            ],
        )
        .unwrap();
    }
    let diff = tx.commit().unwrap();
    let tx_id = diff.tx_id;
    log.append_diff(&diff, Timestamp::from_micros(i64::from(i)))
        .await
        .unwrap();
    tx_id
}

/// Logical state fingerprint: rows and auto-inc high-water per table.
fn fingerprint(store: &MemStore) -> Vec<(u32, Vec<Row>, u64)> {
    let snapshot = store.snapshot();
    [user(store), sensor(store)]
        .into_iter()
        .map(|id| {
            (
                id.as_u32(),
                snapshot.scan(id).unwrap().cloned().collect(),
                snapshot.auto_inc_high_water(id).unwrap(),
            )
        })
        .collect()
}

fn object_names(repo_dir: &Path) -> BTreeSet<String> {
    fs::read_dir(repo_dir.join("objects"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

fn flip_byte(path: &Path, offset_from_end: usize) {
    let mut bytes = fs::read(path).unwrap();
    let len = bytes.len();
    bytes[len - 1 - offset_from_end] ^= 0xFF;
    fs::write(path, bytes).unwrap();
}

// --- manifest integrity (task 1.1) -------------------------------------------

#[test]
fn manifest_roundtrips_and_rejects_any_single_byte_corruption() {
    let manifest = Manifest {
        format_version: super::manifest::MANIFEST_VERSION,
        shard_id: SHARD,
        last_tx_id: 42,
        epoch: 7,
        timestamp: 123_456,
        tables: vec![super::TableManifest {
            table_id: TableId::of("User").as_u32(),
            table_name: "User".into(),
            auto_inc_high_water: 4096,
            row_count: 2,
            chunks: vec![serde_bytes::ByteBuf::from(vec![0xAB; 32])],
        }],
    };
    let bytes = encode_manifest(&manifest).unwrap();
    assert_eq!(decode_manifest(&bytes, None).unwrap(), manifest);

    // Any single corrupted byte — magic, body, or the trailing hash — is
    // detected (STG-021 integrity hash).
    for pos in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[pos] ^= 0x01;
        assert!(decode_manifest(&bad, None).is_err(), "byte {pos}");
    }
    assert!(decode_manifest(&bytes[..bytes.len() - 1], None).is_err());
    assert!(decode_manifest(b"short", None).is_err());
}

#[test]
fn manifest_field_validation_and_hash_rendering() {
    use super::manifest::ObjectHash;

    let hash = ObjectHash::of(b"chunk");
    assert_eq!(format!("{hash:?}"), format!("ObjectHash({hash})"));
    assert_eq!(hash.to_string().len(), 64);
    assert_eq!(ObjectHash::from_bytes(*hash.as_bytes()), hash);

    // A chunk hash of the wrong width is a typed error naming the table.
    let bad_chunk = super::TableManifest {
        table_id: 1,
        table_name: "User".into(),
        auto_inc_high_water: 0,
        row_count: 0,
        chunks: vec![serde_bytes::ByteBuf::from(vec![0xAB; 31])],
    };
    let err = bad_chunk.chunk_hashes().unwrap_err();
    assert!(
        err.to_string().contains("must be 32 bytes, got 31"),
        "{err}"
    );

    // An unknown format version is rejected after integrity verification.
    let future = Manifest {
        format_version: super::manifest::MANIFEST_VERSION + 1,
        shard_id: SHARD,
        last_tx_id: 1,
        epoch: 1,
        timestamp: 0,
        tables: vec![],
    };
    let err = decode_manifest(&encode_manifest(&future).unwrap(), None).unwrap_err();
    assert!(
        err.to_string().contains("unsupported format version"),
        "{err}"
    );
}

// --- repository verification failures (STG-021) --------------------------------

mod repo_verification {
    use super::*;
    use crate::commitlog::record::LogValue;
    use crate::store::pager::codec::compress_artifact;

    fn manifest_name(shard: u32, tx: u64) -> String {
        format!("ckpt-{shard:010}-{tx:020}.manifest")
    }

    /// Write a hand-crafted manifest (and optional raw objects) into a repo.
    fn plant(
        dir: &Path,
        tx: u64,
        tables: Vec<super::super::TableManifest>,
        objects: &[Vec<u8>],
    ) -> CheckpointRepo {
        let repo = CheckpointRepo::open(dir).unwrap();
        for bytes in objects {
            let hash = super::super::manifest::ObjectHash::of(bytes);
            fs::write(dir.join("objects").join(hash.to_string()), bytes).unwrap();
        }
        let manifest = Manifest {
            format_version: super::super::manifest::MANIFEST_VERSION,
            shard_id: SHARD,
            last_tx_id: tx,
            epoch: EPOCH,
            timestamp: 0,
            tables,
        };
        fs::write(
            dir.join(manifest_name(SHARD, tx)),
            encode_manifest(&manifest).unwrap(),
        )
        .unwrap();
        repo
    }

    fn table_entry(
        name: &str,
        table_id: u32,
        row_count: u64,
        chunk_bytes: &[Vec<u8>],
    ) -> super::super::TableManifest {
        super::super::TableManifest {
            table_id,
            table_name: name.into(),
            auto_inc_high_water: 0,
            row_count,
            chunks: chunk_bytes
                .iter()
                .map(|bytes| {
                    serde_bytes::ByteBuf::from(
                        super::super::manifest::ObjectHash::of(bytes)
                            .as_bytes()
                            .to_vec(),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn compression_level_is_configurable_and_reads_stay_level_agnostic() {
        let dir = tempfile::tempdir().unwrap();
        let store = mem_store();
        let mut tx = store.begin();
        tx.insert(
            user(&store),
            vec![RowValue::U64(0), RowValue::Str("compressed".into())],
        )
        .unwrap();
        tx.commit().unwrap();

        let repo = CheckpointRepo::open(dir.path())
            .unwrap()
            .with_compression_level(9);
        repo.write(&store.snapshot(), SHARD, 1, EPOCH).unwrap();
        let loaded = repo.load(&repo.list(SHARD).unwrap()[0]).unwrap();
        let users = loaded
            .tables
            .iter()
            .find(|t| t.table_name == "User")
            .unwrap();
        assert_eq!(users.rows.len(), 1);
    }

    #[test]
    fn manifest_body_and_file_name_must_agree() {
        let dir = tempfile::tempdir().unwrap();
        let repo = plant(dir.path(), 5, vec![], &[]);
        // Rename the manifest so its file name claims tx 6.
        fs::rename(
            dir.path().join(manifest_name(SHARD, 5)),
            dir.path().join(manifest_name(SHARD, 6)),
        )
        .unwrap();
        let refs = repo.list(SHARD).unwrap();
        assert_eq!(refs[0].last_tx_id, 6);
        let err = repo.load(&refs[0]).unwrap_err();
        assert!(
            err.to_string().contains("disagrees with the file name"),
            "{err}"
        );
    }

    #[test]
    fn undecodable_chunk_objects_fail_the_load() {
        let dir = tempfile::tempdir().unwrap();
        // The object's stored bytes hash correctly but are not MessagePack.
        let garbage = b"not messagepack rows".to_vec();
        let repo = plant(
            dir.path(),
            1,
            vec![table_entry(
                "User",
                TableId::of("User").as_u32(),
                1,
                std::slice::from_ref(&garbage),
            )],
            &[garbage],
        );
        let err = repo.load(&repo.list(SHARD).unwrap()[0]).unwrap_err();
        assert!(err.to_string().contains("decode failed"), "{err}");
    }

    #[test]
    fn row_count_disagreement_fails_the_load() {
        let dir = tempfile::tempdir().unwrap();
        // One real row chunk, but the manifest declares 2 rows.
        let rows: Vec<Vec<LogValue>> = vec![vec![LogValue::U64(1), LogValue::Str("ana".into())]];
        let chunk = compress_artifact(&rmp_serde::to_vec(&rows).unwrap(), 3, None).unwrap();
        let repo = plant(
            dir.path(),
            1,
            vec![table_entry(
                "User",
                TableId::of("User").as_u32(),
                2,
                std::slice::from_ref(&chunk),
            )],
            &[chunk],
        );
        let err = repo.load(&repo.list(SHARD).unwrap()[0]).unwrap_err();
        assert!(
            err.to_string()
                .contains("1 rows restored but the manifest declares 2"),
            "{err}"
        );
    }

    #[test]
    fn latest_verified_tx_skips_unreadable_manifests_and_bad_names() {
        let dir = tempfile::tempdir().unwrap();
        let repo = plant(dir.path(), 3, vec![], &[]);
        // A newer manifest full of junk is skipped, not adopted.
        fs::write(dir.path().join(manifest_name(SHARD, 9)), b"junk").unwrap();
        assert_eq!(repo.latest_verified_tx(SHARD).unwrap(), Some(3));

        // Malformed manifest names are not checkpoints at all.
        fs::write(dir.path().join("ckpt-0000000003-123.manifest"), b"x").unwrap();
        fs::write(dir.path().join("ckpt-junk"), b"x").unwrap();
        let listed: Vec<u64> = repo
            .list(SHARD)
            .unwrap()
            .iter()
            .map(|r| r.last_tx_id)
            .collect();
        assert_eq!(listed, vec![3, 9]);
    }
}

// --- archival hook edge case (FR-104) ------------------------------------------

#[test]
fn directory_archive_rejects_paths_without_a_file_name() {
    let dir = tempfile::tempdir().unwrap();
    let archive = DirectoryArchive::open(&dir.path().join("archive")).unwrap();
    let err = super::compact::SegmentArchive::archive(&archive, Path::new("/")).unwrap_err();
    assert!(err.to_string().contains("has no file name"), "{err}");
}

// --- worker failure accounting (STG-020/023) ------------------------------------

#[tokio::test]
async fn checkpoint_now_surfaces_write_failures_and_counts_them() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(mem_store());
    let repo = Arc::new(CheckpointRepo::open(dir.path()).unwrap());

    let mut tx = store.begin();
    tx.insert(
        user(&store),
        vec![RowValue::U64(0), RowValue::Str("w".into())],
    )
    .unwrap();
    tx.commit().unwrap();

    let worker = SnapshotWorker::spawn(
        Arc::clone(&store),
        Arc::clone(&repo),
        SHARD,
        WorkerOptions {
            interval_tx: 1_000_000, // cadence never fires on its own
            compaction: Some(LogCompaction {
                // A log dir that does not exist: compaction after a
                // successful write fails and is counted, never fatal.
                log_dir: dir.path().join("no-such-log-dir"),
                archive_dir: None,
                archive_retention: None,
                remote: None,
            }),
            ..WorkerOptions::default()
        },
    )
    .unwrap();
    worker.observe_commit(1);

    // First checkpoint succeeds (stamp 1); its post-write compaction fails.
    worker.checkpoint_now().unwrap();

    // A competing writer moves the repo ahead; the worker's next stamp (still
    // 1-based) no longer strictly increases, so the write itself fails.
    let side = CheckpointRepo::open(dir.path()).unwrap();
    let mut tx = store.begin();
    tx.insert(
        user(&store),
        vec![RowValue::U64(0), RowValue::Str("w2".into())],
    )
    .unwrap();
    tx.commit().unwrap();
    side.write(&store.snapshot(), SHARD, 50, EPOCH).unwrap();

    worker.observe_commit(2);
    let err = worker.checkpoint_now().unwrap_err();
    assert!(err.to_string().contains("checkpoint write failed"), "{err}");

    let report = worker.close().unwrap();
    assert_eq!(report.checkpoints, 1);
    assert!(
        report.failures >= 2,
        "one compaction failure + one write failure, got {}",
        report.failures
    );
    assert_eq!(report.last_checkpoint_tx, 1);
}

#[tokio::test]
async fn dropping_a_worker_without_close_shuts_it_down() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(mem_store());
    let repo = Arc::new(CheckpointRepo::open(dir.path()).unwrap());
    let worker = SnapshotWorker::spawn(store, repo, SHARD, WorkerOptions::default()).unwrap();
    worker.observe_commit(1);
    drop(worker); // Drop joins the actor thread; no panic, no leak.
}

// --- recovery schema-mismatch guards (STG-030/STG-050) ---------------------------

mod recovery_guards {
    use super::*;

    fn plant_manifest(dir: &Path, tables: Vec<super::super::TableManifest>) -> CheckpointRepo {
        let repo = CheckpointRepo::open(dir).unwrap();
        let manifest = Manifest {
            format_version: super::super::manifest::MANIFEST_VERSION,
            shard_id: SHARD,
            last_tx_id: 1,
            epoch: EPOCH,
            timestamp: 0,
            tables,
        };
        fs::write(
            dir.join(format!("ckpt-{SHARD:010}-{:020}.manifest", 1)),
            encode_manifest(&manifest).unwrap(),
        )
        .unwrap();
        repo
    }

    fn entry(name: &str, table_id: u32) -> super::super::TableManifest {
        super::super::TableManifest {
            table_id,
            table_name: name.into(),
            auto_inc_high_water: 0,
            row_count: 0,
            chunks: vec![],
        }
    }

    #[test]
    fn recorded_table_id_must_equal_crc32_of_the_name() {
        let dir = tempfile::tempdir().unwrap();
        let repo = plant_manifest(&dir.path().join("snap"), vec![entry("User", 999)]);
        let store = mem_store();
        let err = recover(&store, &repo, &dir.path().join("log"), SHARD).unwrap_err();
        assert!(
            err.to_string().contains("disagrees with crc32(name)"),
            "{err}"
        );
    }

    #[test]
    fn checkpoint_tables_must_exist_in_the_assembled_schema() {
        let dir = tempfile::tempdir().unwrap();
        let repo = plant_manifest(
            &dir.path().join("snap"),
            vec![entry("Ghost", TableId::of("Ghost").as_u32())],
        );
        let store = mem_store();
        let err = recover(&store, &repo, &dir.path().join("log"), SHARD).unwrap_err();
        assert!(
            err.to_string().contains("is not in the assembled schema"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn log_records_for_unknown_tables_abort_recovery() {
        use crate::commitlog::record::{TableMutation, TxRecord};

        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
        log.append(TxRecord {
            tx_id: 1,
            timestamp: 0,
            shard_id: SHARD,
            mutations: vec![TableMutation {
                table_id: 0xDEAD_BEEF, // no such table in the schema
                inserts: vec![],
                deletes: vec![],
            }],
            auto_inc: vec![],
            caller: Vec::new(),
            reducer_name: String::new(),
        })
        .await
        .unwrap();
        log.wait_durable(1).await.unwrap();
        log.close().unwrap();

        let repo = CheckpointRepo::open(&dir.path().join("snap")).unwrap();
        let store = mem_store();
        let err = recover(&store, &repo, &log_dir, SHARD).unwrap_err();
        assert!(
            err.to_string()
                .contains("references table 0xdeadbeef which is not in the assembled schema"),
            "{err}"
        );
    }
}

// --- recovery rebuilds spatial + unique structures (STG-030 step 5) --------------

mod recovery_rebuilds {
    use super::*;
    use crate::schema::SpatialKind;

    static POI_COLS: &[ColumnSchema] = &[
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
        ColumnSchema {
            name: "tag",
            ty: FluxType::Str,
        },
    ];

    static POI: TableSchema = TableSchema {
        name: "Poi",
        columns: POI_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[&[3]],
        indexes: &[IndexSchema::Spatial {
            kind: SpatialKind::QuadTree,
            columns: &[1, 2],
        }],
        visibility: VisibilityRule::PublicAll,
    };

    fn poi_store() -> MemStore {
        MemStore::new(&Schema::from_tables([&POI]).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn spatial_and_unique_indexes_are_rebuilt_from_recovered_rows() {
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("log");
        let store = poi_store();
        let poi = store.table_id("Poi").unwrap();
        let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
        let mut tx = store.begin();
        tx.insert(
            poi,
            vec![
                RowValue::U64(1),
                RowValue::F64(1.0),
                RowValue::F64(2.0),
                RowValue::Str("cafe".into()),
            ],
        )
        .unwrap();
        tx.insert(
            poi,
            vec![
                RowValue::U64(2),
                RowValue::F64(50.0),
                RowValue::F64(60.0),
                RowValue::Str("park".into()),
            ],
        )
        .unwrap();
        let diff = tx.commit().unwrap();
        log.append_diff(&diff, Timestamp::from_micros(1))
            .await
            .unwrap();
        log.wait_durable(diff.tx_id).await.unwrap();
        log.close().unwrap();

        let recovered = poi_store();
        let repo = CheckpointRepo::open(&dir.path().join("snap")).unwrap();
        recover(&recovered, &repo, &log_dir, SHARD).unwrap();
        let poi = recovered.table_id("Poi").unwrap();
        assert_eq!(recovered.snapshot().row_count(poi).unwrap(), 2);

        // The rebuilt unique constraint still rejects duplicates…
        let mut tx = recovered.begin();
        let err = tx
            .insert(
                poi,
                vec![
                    RowValue::U64(3),
                    RowValue::F64(9.0),
                    RowValue::F64(9.0),
                    RowValue::Str("cafe".into()),
                ],
            )
            .unwrap_err();
        assert!(err.to_string().contains("unique"), "{err}");
        tx.rollback();

        // …and the rebuilt spatial index answers region queries.
        let rows = recovered
            .snapshot()
            .spatial_region(poi, crate::index::Rect::new(0.0, 0.0, 10.0, 10.0))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value(0), Some(&RowValue::U64(1)));
    }
}

// --- equivalence: checkpoint + replay == full-log replay (task 1.6) ----------

#[tokio::test]
async fn checkpoint_plus_replay_equals_full_log_replay() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let snap_dir = dir.path().join("snapshots");
    let store = mem_store();
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();

    let mut mid = 0;
    for i in 1..=8 {
        mid = commit_step(&store, &log, i).await;
    }
    let repo = CheckpointRepo::open(&snap_dir).unwrap();
    let stats = repo.write(&store.snapshot(), SHARD, mid, EPOCH).unwrap();
    assert_eq!(stats.last_tx_id, mid);
    assert_eq!(stats.objects_shared, 0, "first checkpoint shares nothing");

    let mut last = mid;
    for i in 9..=15 {
        last = commit_step(&store, &log, i).await;
    }
    log.wait_durable(last).await.unwrap();
    log.close().unwrap();

    // Checkpoint + replay.
    let via_checkpoint = mem_store();
    let outcome = recover(&via_checkpoint, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(mid));
    assert!(outcome.rejected.is_empty());
    assert!(outcome.replay.corruption.is_none());
    assert_eq!(outcome.last_tx_id, Some(last));
    assert_eq!(outcome.next_tx_id, last + 1);
    assert!(
        outcome.applied_records < outcome.replay.records,
        "records covered by the checkpoint must be skipped"
    );

    // Full-log replay (no checkpoint present).
    let empty_repo = CheckpointRepo::open(&dir.path().join("empty")).unwrap();
    let via_log = mem_store();
    let full = recover(&via_log, &empty_repo, &log_dir, SHARD).unwrap();
    assert_eq!(full.checkpoint_tx_id, None);
    assert_eq!(full.applied_records, full.replay.records);
    assert_eq!(full.next_tx_id, last + 1);

    // Identical to each other and to the pre-crash committed state.
    assert_eq!(fingerprint(&via_checkpoint), fingerprint(&store));
    assert_eq!(fingerprint(&via_log), fingerprint(&store));

    // Secondary indexes are bit-identical to a fresh rebuild (STG-007).
    via_checkpoint
        .snapshot()
        .verify_index_integrity(user(&via_checkpoint))
        .unwrap();

    // tx ids resume at last + 1 (STG-015) and auto-inc never reuses ids
    // (STG-040): the next insert gets a fresh id above the high-water mark.
    let hw = store.snapshot().auto_inc_high_water(user(&store)).unwrap();
    let mut tx = via_checkpoint.begin();
    assert_eq!(tx.tx_id(), last + 1);
    let row = tx
        .insert(
            user(&via_checkpoint),
            vec![RowValue::U64(0), RowValue::Str("post-recovery".into())],
        )
        .unwrap();
    let RowValue::U64(id) = row.values()[0] else {
        panic!("auto-inc id expected")
    };
    assert!(id > hw, "id {id} must exceed the recovered high-water {hw}");
    tx.commit().unwrap();
}

#[test]
fn recovery_of_empty_dirs_yields_a_fresh_store() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    fs::create_dir_all(&log_dir).unwrap();
    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();
    let store = mem_store();
    let outcome = recover(&store, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, None);
    assert_eq!(outcome.last_tx_id, None);
    assert_eq!(outcome.next_tx_id, 1);
    let mut tx = store.begin();
    assert_eq!(tx.tx_id(), 1);
    tx.insert(
        user(&store),
        vec![RowValue::U64(0), RowValue::Str("a".into())],
    )
    .unwrap();
    tx.commit().unwrap();
}

#[test]
fn recovery_requires_a_fresh_store() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    fs::create_dir_all(&log_dir).unwrap();
    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();

    // A store that already committed refuses recovery (STG-030).
    let store = mem_store();
    let mut tx = store.begin();
    tx.insert(
        user(&store),
        vec![RowValue::U64(0), RowValue::Str("x".into())],
    )
    .unwrap();
    tx.commit().unwrap();
    let err = recover(&store, &repo, &log_dir, SHARD).unwrap_err();
    assert!(
        err.to_string().contains("before the first transaction"),
        "{err}"
    );
}

// --- incrementality: no full-dump scaling cliff (tasks 1.1, 1.6) -------------

#[tokio::test]
async fn incremental_checkpoint_writes_only_changed_objects() {
    let dir = tempfile::tempdir().unwrap();
    let store = mem_store();
    let u = user(&store);

    // A large committed dataset in one transaction.
    let mut tx = store.begin();
    for i in 0..2_000u64 {
        tx.insert(
            u,
            vec![RowValue::U64(0), RowValue::Str(format!("bulk-{i}"))],
        )
        .unwrap();
    }
    let first_tx = tx.commit().unwrap().tx_id;

    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();
    let full = repo
        .write(&store.snapshot(), SHARD, first_tx, EPOCH)
        .unwrap();
    assert!(full.objects_total > 10, "chunking must split a large table");
    assert_eq!(full.objects_written, full.objects_total);

    // Mutate a tiny fraction: one in-place row update.
    let mut tx = store.begin();
    tx.delete(u, &[RowValue::U64(1_000)]).unwrap();
    tx.insert(
        u,
        vec![RowValue::U64(1_000), RowValue::Str("changed".into())],
    )
    .unwrap();
    let second_tx = tx.commit().unwrap().tx_id;

    let incr = repo
        .write(&store.snapshot(), SHARD, second_tx, EPOCH)
        .unwrap();
    assert_eq!(
        incr.objects_total,
        incr.objects_written + incr.objects_shared
    );
    assert!(
        incr.objects_written <= 3,
        "one mutated row must re-write only its chunk neighborhood, wrote {}",
        incr.objects_written
    );
    assert!(
        incr.objects_shared >= incr.objects_total - 3,
        "unchanged objects must be shared with the previous checkpoint: {incr:?}"
    );
    assert!(incr.bytes_written < full.bytes_written / 10);

    // Both checkpoints stay fully loadable (shared objects intact).
    for r in repo.list(SHARD).unwrap() {
        repo.load(&r).unwrap();
    }
}

// --- corruption fallback (task 1.2) -------------------------------------------

#[tokio::test]
async fn corrupted_manifest_or_object_falls_back_to_an_older_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let store = mem_store();
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();

    let mut ckpt_a = 0;
    for i in 1..=6 {
        ckpt_a = commit_step(&store, &log, i).await;
    }
    repo.write(&store.snapshot(), SHARD, ckpt_a, EPOCH).unwrap();
    let objects_a = object_names(repo.dir());

    let mut ckpt_b = 0;
    for i in 7..=12 {
        ckpt_b = commit_step(&store, &log, i).await;
    }
    let stats_b = repo.write(&store.snapshot(), SHARD, ckpt_b, EPOCH).unwrap();
    log.wait_durable(ckpt_b).await.unwrap();
    log.close().unwrap();

    // 1. Corrupt B's manifest: recovery falls back to A and still lands on
    //    the exact pre-crash state via replay.
    let manifest_b = stats_b.manifest.clone();
    let pristine_b = fs::read(&manifest_b).unwrap();
    flip_byte(&manifest_b, 40);
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(ckpt_a));
    assert_eq!(outcome.rejected.len(), 1);
    assert_eq!(outcome.rejected[0].path, manifest_b);
    assert_eq!(fingerprint(&recovered), fingerprint(&store));

    // 2. Corrupt an object referenced only by B: same fallback.
    fs::write(&manifest_b, &pristine_b).unwrap();
    let only_b: Vec<String> = object_names(repo.dir())
        .difference(&objects_a)
        .cloned()
        .collect();
    assert!(
        !only_b.is_empty(),
        "checkpoint B must have written new objects"
    );
    flip_byte(&repo.dir().join("objects").join(&only_b[0]), 0);
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(ckpt_a));
    assert!(
        outcome.rejected[0].reason.contains("hash mismatch")
            || outcome.rejected[0].reason.contains("decode failed"),
        "{:?}",
        outcome.rejected
    );
    assert_eq!(fingerprint(&recovered), fingerprint(&store));

    // 3. Every checkpoint corrupt: recovery starts empty and full-log replay
    //    still reconstructs the exact state.
    for r in repo.list(SHARD).unwrap() {
        flip_byte(&r.path, 40);
    }
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, None);
    assert_eq!(outcome.rejected.len(), 2);
    assert_eq!(fingerprint(&recovered), fingerprint(&store));
}

// --- non-blocking checkpoint under sustained write load (task 1.3) -----------

#[test]
fn checkpoint_write_never_blocks_commits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(mem_store());
    let u = user(&store);

    // A dataset large enough that the checkpoint write takes real time
    // (hundreds of fsynced objects).
    let mut tx = store.begin();
    for i in 0..10_000u64 {
        tx.insert(
            u,
            vec![RowValue::U64(0), RowValue::Str(format!("load-{i}"))],
        )
        .unwrap();
    }
    let last_tx = tx.commit().unwrap().tx_id;
    let rows_at_checkpoint = store.snapshot().row_count(u).unwrap();

    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        // STG-022: the checkpoint consumes only a wait-free snapshot — no
        // store lock is held while objects hit disk.
        let snapshot = store.snapshot();
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let stats = repo.write(&snapshot, SHARD, last_tx, EPOCH);
            done.store(true, Ordering::SeqCst);
            (repo, stats)
        })
    };

    // Sustained write load while the checkpoint is in flight.
    let mut commits_during = 0u64;
    while !done.load(Ordering::SeqCst) {
        let mut tx = store.begin();
        tx.insert(
            u,
            vec![
                RowValue::U64(0),
                RowValue::Str(format!("during-{commits_during}")),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        commits_during += 1;
    }
    let (repo, stats) = writer.join().unwrap();
    let stats = stats.unwrap();
    assert!(
        commits_during > 0,
        "commits must proceed while the checkpoint writes (STG-022)"
    );

    // The checkpoint is the pre-write snapshot: concurrent commits are
    // invisible to it, and it loads back with exactly that row count.
    let refs = repo.list(SHARD).unwrap();
    assert_eq!(refs.len(), 1);
    let loaded = repo.load(&refs[0]).unwrap();
    let loaded_user = loaded
        .tables
        .iter()
        .find(|t| t.table_name == "User")
        .unwrap();
    assert_eq!(loaded_user.rows.len(), rows_at_checkpoint);
    assert!(stats.objects_written > 30, "{stats:?}");
}

// --- compaction + archival hook (task 1.4) ------------------------------------

#[tokio::test]
async fn recovery_succeeds_after_compaction_and_fallback_survives_it() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 192, // force rotation every couple of entries
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let repo = CheckpointRepo::open(&dir.path().join("snapshots")).unwrap();

    let mut ckpt_a = 0;
    for i in 1..=10 {
        ckpt_a = commit_step(&store, &log, i).await;
    }
    log.wait_durable(ckpt_a).await.unwrap();
    repo.write(&store.snapshot(), SHARD, ckpt_a, EPOCH).unwrap();

    let mut ckpt_b = 0;
    for i in 11..=20 {
        ckpt_b = commit_step(&store, &log, i).await;
    }
    log.wait_durable(ckpt_b).await.unwrap();
    let stats_b = repo.write(&store.snapshot(), SHARD, ckpt_b, EPOCH).unwrap();
    log.close().unwrap();

    // Compact to the OLDEST retained checkpoint (the worker's rule): both
    // checkpoints keep the suffix they need.
    let removed = compact_covered(&log_dir, SHARD, ckpt_a, None, None).unwrap();
    assert!(
        !removed.is_empty(),
        "expected covered segments to be removed"
    );
    for path in &removed {
        assert!(!path.exists());
    }

    // Recovery via the newest checkpoint still succeeds after compaction.
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(ckpt_b));
    assert!(outcome.replay.corruption.is_none());
    assert_eq!(fingerprint(&recovered), fingerprint(&store));

    // The STG-021 fallback also survives compaction: with B corrupt, A plus
    // the retained log suffix reconstructs the same state.
    flip_byte(&stats_b.manifest, 40);
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert_eq!(outcome.checkpoint_tx_id, Some(ckpt_a));
    assert_eq!(fingerprint(&recovered), fingerprint(&store));
}

#[tokio::test]
async fn archival_hook_preserves_segments_byte_identically() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let archive_dir = dir.path().join("archive");
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 192,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let mut last = 0;
    for i in 1..=12 {
        last = commit_step(&store, &log, i).await;
    }
    log.wait_durable(last).await.unwrap();
    log.close().unwrap();

    // Snapshot the covered segments' bytes before compaction.
    let originals: Vec<(std::path::PathBuf, Vec<u8>)> = fs::read_dir(&log_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .map(|p| (p.clone(), fs::read(&p).unwrap()))
        .collect();

    // Archival enabled: segments are archived (not deleted), byte-identical.
    let archive = DirectoryArchive::open(&archive_dir).unwrap();
    let removed = compact_covered(&log_dir, SHARD, last, None, Some(&archive)).unwrap();
    assert!(!removed.is_empty());
    for path in &removed {
        let name = path.file_name().unwrap();
        let archived = archive_dir.join(name);
        let original = originals
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, bytes)| bytes)
            .unwrap();
        assert_eq!(&fs::read(&archived).unwrap(), original, "{archived:?}");
        assert!(!path.exists(), "the live segment must be gone");
    }

    // The tail always survives, so the log still opens and appends.
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    assert_eq!(log.recovery().last_tx_id, Some(last));
    log.close().unwrap();
}

#[tokio::test]
async fn compaction_respects_retention_holds_and_the_active_tail() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let store = mem_store();
    let opts = CommitLogOptions {
        segment_max_bytes: 192,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let mut last = 0;
    for i in 1..=12 {
        last = commit_step(&store, &log, i).await;
    }
    log.wait_durable(last).await.unwrap();
    log.close().unwrap();

    // A replication retention hold at tx 1 pins every segment (STG-013).
    assert!(
        compact_covered(&log_dir, SHARD, last, Some(1), None)
            .unwrap()
            .is_empty()
    );

    // Nothing covered: nothing removed.
    assert!(
        compact_covered(&log_dir, SHARD, 0, None, None)
            .unwrap()
            .is_empty()
    );

    // Full coverage removes everything except the active tail.
    let before = fs::read_dir(&log_dir).unwrap().count();
    assert!(before > 1);
    let removed = compact_covered(&log_dir, SHARD, last, None, None).unwrap();
    assert_eq!(removed.len(), before - 1);
}

// --- repo invariants (tasks 1.1, STG-023) -------------------------------------

#[test]
fn repo_rejects_non_monotone_and_tx_zero_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = mem_store();
    let repo = CheckpointRepo::open(dir.path()).unwrap();
    let snapshot = store.snapshot();
    assert!(repo.write(&snapshot, SHARD, 0, EPOCH).is_err());
    repo.write(&snapshot, SHARD, 5, EPOCH).unwrap();
    assert!(repo.write(&snapshot, SHARD, 5, EPOCH).is_err());
    assert!(repo.write(&snapshot, SHARD, 4, EPOCH).is_err());
    repo.write(&snapshot, SHARD, 6, EPOCH).unwrap();
}

#[test]
fn prune_honors_retention_minimum_pins_and_shared_objects() {
    let dir = tempfile::tempdir().unwrap();
    let store = mem_store();
    let u = user(&store);
    let repo = CheckpointRepo::open(dir.path()).unwrap();

    // ckpt 1: 200 rows. ckpt 2: 100 of them deleted (the deleted range's
    // chunks become exclusive to ckpt 1). ckpt 3: small change.
    let mut tx = store.begin();
    for i in 0..200u64 {
        tx.insert(u, vec![RowValue::U64(0), RowValue::Str(format!("r{i}"))])
            .unwrap();
    }
    let t1 = tx.commit().unwrap().tx_id;
    repo.write(&store.snapshot(), SHARD, t1, EPOCH).unwrap();

    let mut tx = store.begin();
    for id in 100..200u64 {
        tx.delete(u, &[RowValue::U64(id)]).unwrap();
    }
    let t2 = tx.commit().unwrap().tx_id;
    repo.write(&store.snapshot(), SHARD, t2, EPOCH).unwrap();

    let mut tx = store.begin();
    tx.insert(u, vec![RowValue::U64(0), RowValue::Str("tail".into())])
        .unwrap();
    let t3 = tx.commit().unwrap().tx_id;
    repo.write(&store.snapshot(), SHARD, t3, EPOCH).unwrap();

    // Retention below the STG-023 minimum is rejected.
    assert!(repo.prune(SHARD, 1).is_err());

    // A pinned checkpoint survives pruning (replica transfer, STG-023).
    repo.pin(t1);
    assert!(repo.prune(SHARD, 2).unwrap().is_empty());
    assert_eq!(repo.list(SHARD).unwrap().len(), 3);

    // Unpinned, the oldest goes; objects shared with retained checkpoints
    // stay, exclusive ones are garbage-collected.
    repo.unpin(t1);
    let objects_before = object_names(dir.path()).len();
    let removed = repo.prune(SHARD, 2).unwrap();
    assert_eq!(removed.len(), 1);
    let refs = repo.list(SHARD).unwrap();
    assert_eq!(
        refs.iter().map(|r| r.last_tx_id).collect::<Vec<_>>(),
        vec![t2, t3]
    );
    assert!(object_names(dir.path()).len() < objects_before);
    for r in refs {
        repo.load(&r).unwrap(); // every retained checkpoint still verifies
    }
}

// --- worker: cadence, retention, compaction (tasks 1.3, 1.5) -------------------

#[tokio::test]
async fn worker_checkpoints_on_cadence_and_recovery_matches() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let store = Arc::new(mem_store());
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, CommitLogOptions::default()).unwrap();
    let repo = Arc::new(CheckpointRepo::open(&dir.path().join("snapshots")).unwrap());

    let worker = SnapshotWorker::spawn(
        Arc::clone(&store),
        Arc::clone(&repo),
        SHARD,
        WorkerOptions {
            interval_tx: 10,
            retention: 2,
            epoch: EPOCH,
            compaction: None,
            metrics: None,
        },
    )
    .unwrap();

    let mut last = 0;
    for i in 1..=30 {
        last = commit_step(&store, &log, i).await;
        worker.observe_commit(last);
    }
    let report = worker.close().unwrap();
    assert!(report.checkpoints >= 1, "{report:?}");
    assert_eq!(report.failures, 0, "{report:?}");
    assert!(report.last_checkpoint_tx >= 10);

    log.wait_durable(last).await.unwrap();
    log.close().unwrap();

    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert!(outcome.checkpoint_tx_id.is_some());
    assert_eq!(fingerprint(&recovered), fingerprint(&store));
}

#[tokio::test]
async fn worker_prunes_retention_and_compacts_through_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let archive_dir = dir.path().join("archive");
    let store = Arc::new(mem_store());
    let opts = CommitLogOptions {
        segment_max_bytes: 192,
        ..CommitLogOptions::default()
    };
    let log = CommitLog::open(&log_dir, SHARD, EPOCH, opts).unwrap();
    let repo = Arc::new(CheckpointRepo::open(&dir.path().join("snapshots")).unwrap());

    let worker = SnapshotWorker::spawn(
        Arc::clone(&store),
        Arc::clone(&repo),
        SHARD,
        WorkerOptions {
            interval_tx: 1_000_000, // cadence never fires; checkpoint_now drives
            retention: 2,
            epoch: EPOCH,
            compaction: Some(LogCompaction {
                log_dir: log_dir.clone(),
                archive_dir: Some(archive_dir.clone()),
                archive_retention: None,
                remote: None,
            }),
            metrics: None,
        },
    )
    .unwrap();

    let mut last = 0;
    for round in 0..3 {
        for i in 1..=8 {
            last = commit_step(&store, &log, round * 8 + i).await;
            worker.observe_commit(last);
        }
        log.wait_durable(last).await.unwrap();
        worker.checkpoint_now().unwrap();
    }
    let report = worker.close().unwrap();
    assert_eq!(report.checkpoints, 3);
    assert_eq!(report.failures, 0, "{report:?}");

    // Retention (STG-023): only the newest 2 checkpoints remain.
    assert_eq!(repo.list(SHARD).unwrap().len(), 2);

    // Compaction ran up to the oldest retained checkpoint, through the
    // archival hook: archived segments exist, and the live log lost them.
    let archived: Vec<_> = fs::read_dir(&archive_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert!(!archived.is_empty(), "expected archived segments");
    for path in &archived {
        assert!(!log_dir.join(path.file_name().unwrap()).exists());
    }

    // The compacted log + retained checkpoints still recover exactly.
    log.close().unwrap();
    let recovered = mem_store();
    let outcome = recover(&recovered, &repo, &log_dir, SHARD).unwrap();
    assert!(outcome.checkpoint_tx_id.is_some());
    assert_eq!(fingerprint(&recovered), fingerprint(&store));

    // checkpoint_now with nothing new to cover is an explicit error.
    let worker = SnapshotWorker::spawn(
        Arc::clone(&store),
        Arc::clone(&repo),
        SHARD,
        WorkerOptions {
            interval_tx: 10,
            retention: 2,
            epoch: EPOCH,
            compaction: None,
            metrics: None,
        },
    )
    .unwrap();
    assert!(worker.checkpoint_now().is_err());
    worker.close().unwrap();
}

// --- adaptive cadence (task 1.5, FR-113) ---------------------------------------

#[test]
fn adaptive_interval_scales_with_effective_memory_and_clamps() {
    let profile = |total: u64, limit: Option<u64>| HardwareProfile {
        logical_cores: 8,
        physical_cores: 4,
        total_ram_bytes: total,
        available_ram_bytes: total,
        cgroup_cpu_quota: None,
        cgroup_memory_limit_bytes: limit,
    };

    // The STG-020 default at the 512 MiB reference profile.
    assert_eq!(adaptive_interval_tx(&profile(512 << 20, None)), 10_000);
    // Scales linearly with memory.
    assert_eq!(adaptive_interval_tx(&profile(1 << 30, None)), 20_000);
    // Clamped at both ends.
    assert_eq!(adaptive_interval_tx(&profile(16 << 20, None)), 1_000);
    assert_eq!(adaptive_interval_tx(&profile(128 << 30, None)), 200_000);
    // Container-aware: the cgroup limit wins over host totals (HWA-002).
    assert_eq!(
        adaptive_interval_tx(&profile(128 << 30, Some(512 << 20))),
        10_000
    );

    // Cadence and retention are validated at spawn.
    let store = Arc::new(mem_store());
    let dir = tempfile::tempdir().unwrap();
    let repo = Arc::new(CheckpointRepo::open(dir.path()).unwrap());
    for bad in [
        WorkerOptions {
            interval_tx: 0,
            ..WorkerOptions::default()
        },
        WorkerOptions {
            retention: 1,
            ..WorkerOptions::default()
        },
    ] {
        assert!(SnapshotWorker::spawn(Arc::clone(&store), Arc::clone(&repo), SHARD, bad).is_err());
    }
    let adaptive = WorkerOptions::adaptive(&profile(512 << 20, None));
    assert_eq!(adaptive.interval_tx, 10_000);
    assert_eq!(adaptive.retention, 3);
}
