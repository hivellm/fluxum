//! SPEC-023 DMX-040/041 — the blob / large-object store: `Blob` columns hold
//! a content-hash reference; the bytes live in the per-shard content-addressed
//! [`BlobStore`], reference-counted by row references and reclaimed when the
//! last one goes (upload leases protect staged-but-unreferenced bytes).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use fluxum_core::commitlog::{BlobHash, BlobStore, CommitLog, CommitLogOptions};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_core::types::BlobRef;

const SHARD: u32 = 11;

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "avatar",
        ty: FluxType::Blob,
    },
];
static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn user(id: u64, avatar: &BlobHash) -> Vec<RowValue> {
    vec![
        RowValue::U64(id),
        RowValue::Blob(BlobRef::from_bytes(*avatar.as_bytes())),
    ]
}

struct Harness {
    store: Arc<MemStore>,
    blobs: Arc<BlobStore>,
    pipeline: TxPipeline,
    _worker: tokio::task::JoinHandle<()>,
}

fn harness(dir: &std::path::Path) -> Harness {
    let schema = Schema::from_tables([&USER]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let blobs = Arc::new(BlobStore::open(&dir.join("blobs")).unwrap());
    store.attach_blob_store(Arc::clone(&blobs));
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    Harness {
        store,
        blobs,
        pipeline,
        _worker: tokio::spawn(worker.run()),
    }
}

/// DMX-040: row references drive the refcount — shared blob counted per
/// referencing row; the update path swaps references; the last delete drops
/// the count to zero and `reclaim` removes the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn row_references_drive_refcount_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let uid = h.store.table_id("User").unwrap();

    // Stage an upload: bytes stored at refcount 0 under an upload lease.
    let payload = vec![7u8; 4 * 1024 * 1024]; // the 4 MB avatar of the scenario
    let hash = h.blobs.stage(&payload).unwrap();
    assert_eq!(h.blobs.refcount(&hash), Some(0));

    // Two rows reference it: count 2; the first reference released the lease.
    for id in [1u64, 2] {
        h.pipeline
            .call(Box::new(move |tx| {
                tx.insert(uid, user(id, &hash))?;
                Ok(())
            }))
            .await
            .unwrap();
    }
    assert_eq!(h.blobs.refcount(&hash), Some(2));

    // An update to a different blob swaps the references.
    let other = h.blobs.stage(b"replacement avatar").unwrap();
    {
        h.pipeline
            .call(Box::new(move |tx| {
                tx.upsert(uid, user(2, &other))?;
                Ok(())
            }))
            .await
            .unwrap();
    }
    assert_eq!(h.blobs.refcount(&hash), Some(1));
    assert_eq!(h.blobs.refcount(&other), Some(1));

    // Deleting the remaining rows drops both to zero; reclaim removes bytes.
    h.pipeline
        .call(Box::new(move |tx| {
            tx.delete(uid, &[RowValue::U64(1)])?;
            tx.delete(uid, &[RowValue::U64(2)])?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(h.blobs.refcount(&hash), Some(0));
    let mut reclaimed = h.blobs.reclaim().unwrap();
    reclaimed.sort();
    let mut expected = vec![hash, other];
    expected.sort();
    assert_eq!(reclaimed, expected);
    assert_eq!(h.blobs.get(&hash).unwrap(), None, "bytes reclaimed");
}

/// DMX-040 validation: a `Blob` write must reference a stored object, and a
/// store must be attached at all.
#[tokio::test(flavor = "multi_thread")]
async fn blob_writes_are_validated_at_write_time() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let uid = h.store.table_id("User").unwrap();

    // Unknown reference: rejected before any commit.
    let bogus = BlobHash::of(b"never uploaded");
    let err = h
        .pipeline
        .call(Box::new(move |tx| {
            tx.insert(uid, user(1, &bogus))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("names no stored object"), "{err}");

    // No store attached: rejected with a clear error.
    let schema = Schema::from_tables([&USER]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log2"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let _worker = tokio::spawn(worker.run());
    let uid2 = store.table_id("User").unwrap();
    let hash = BlobHash::of(b"whatever");
    let err = pipeline
        .call(Box::new(move |tx| {
            tx.insert(uid2, user(1, &hash))?;
            Ok(())
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no blob store attached"), "{err}");
}

/// Upload leases (DMX-041): a staged blob survives `reclaim` until GC ages
/// its lease out; a referenced blob's lease is released without exposure.
#[tokio::test(flavor = "multi_thread")]
async fn upload_leases_protect_staged_blobs_until_gc() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let uid = h.store.table_id("User").unwrap();

    // Staged, never referenced: plain reclaim leaves it; gc(0) collects it.
    let orphan = h.blobs.stage(b"orphaned upload").unwrap();
    assert!(h.blobs.reclaim().unwrap().is_empty(), "lease protects it");
    assert_eq!(h.blobs.gc(Duration::ZERO).unwrap(), vec![orphan]);

    // Staged then referenced: gc never touches it (the row protects it).
    let live = h.blobs.stage(b"referenced upload").unwrap();
    {
        h.pipeline
            .call(Box::new(move |tx| {
                tx.insert(uid, user(9, &live))?;
                Ok(())
            }))
            .await
            .unwrap();
    }
    assert!(h.blobs.gc(Duration::ZERO).unwrap().is_empty());
    assert_eq!(h.blobs.refcount(&live), Some(1));
    assert_eq!(h.blobs.get(&live).unwrap().unwrap(), b"referenced upload");
}

/// Restart semantics: `attach_blob_store` rebuilds refcounts from the live
/// rows, so counts survive a process restart exactly (the store's in-memory
/// index is an index over rows, not durable state of its own).
#[tokio::test(flavor = "multi_thread")]
async fn attach_rebuilds_refcounts_from_live_rows() {
    let dir = tempfile::tempdir().unwrap();
    let hash;
    {
        let h = harness(dir.path());
        let uid = h.store.table_id("User").unwrap();
        hash = h.blobs.stage(b"survives restart").unwrap();
        let put = hash;
        h.pipeline
            .call(Box::new(move |tx| {
                tx.insert(uid, user(1, &put))?;
                tx.insert(uid, user(2, &put))?;
                Ok(())
            }))
            .await
            .unwrap();
        assert_eq!(h.blobs.refcount(&hash), Some(2));
    }

    // "Restart": fresh store hand-fed the surviving rows, fresh BlobStore
    // over the same directory (objects load at refcount 0), then attach.
    let schema = Schema::from_tables([&USER]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log3"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let blobs = Arc::new(BlobStore::open(&dir.path().join("blobs")).unwrap());
    assert_eq!(blobs.refcount(&hash), Some(0), "fresh index starts at zero");
    // Attach (rebuild over the still-empty store), then re-insert the rows —
    // counts maintained incrementally from there.
    store.attach_blob_store(Arc::clone(&blobs));
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let _worker = tokio::spawn(worker.run());
    let uid = store.table_id("User").unwrap();
    let put = hash;
    pipeline
        .call(Box::new(move |tx| {
            tx.insert(uid, user(1, &put))?;
            tx.insert(uid, user(2, &put))?;
            Ok(())
        }))
        .await
        .unwrap();
    assert_eq!(blobs.refcount(&hash), Some(2));

    // The rebuild path itself: zero everything, then rebuild from the rows.
    blobs.rebuild_refcounts([hash, hash]);
    assert_eq!(blobs.refcount(&hash), Some(2));
    blobs.rebuild_refcounts([]);
    assert_eq!(blobs.refcount(&hash), Some(0));
}
