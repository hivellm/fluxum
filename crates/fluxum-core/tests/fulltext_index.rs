//! SPEC-019 FTS-021/022 — the full-text inverted index rides the commit
//! merge exactly like the B-tree/spatial indexes: after any insert/update/
//! delete sequence it is bit-identical to a fresh rebuild over the committed
//! rows (STG-007 rule 2), and the post-recovery rebuild reproduces it.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::schema::{
    ColumnSchema, FluxType, FullTextLanguage, IndexSchema, Schema, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};

const SHARD: u32 = 19;

static COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "body",
        ty: FluxType::Str,
    },
];
static ARTICLE: TableSchema = TableSchema {
    name: "Article",
    columns: COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::FullText {
        column: 1,
        language: FullTextLanguage::English,
        stop_words: true,
        stemming: true,
    }],
    visibility: VisibilityRule::PublicAll,
};

fn row(id: u64, body: &str) -> Vec<RowValue> {
    vec![RowValue::U64(id), RowValue::Str(body.to_owned())]
}

struct Harness {
    store: Arc<MemStore>,
    pipeline: TxPipeline,
    table: TableId,
    _worker: tokio::task::JoinHandle<()>,
}

fn harness(dir: &std::path::Path) -> Harness {
    let schema = Schema::from_tables([&ARTICLE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log =
        Arc::new(CommitLog::open(&dir.join("log"), SHARD, 1, CommitLogOptions::default()).unwrap());
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    let table = store.table_id("Article").unwrap();
    Harness {
        store,
        pipeline,
        table,
        _worker: tokio::spawn(worker.run()),
    }
}

/// A deterministic LCG so the "random" sequence is reproducible without an
/// RNG dependency.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, modulo: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.0 >> 33) % modulo
    }
}

const CORPUS: &[&str] = &[
    "the quick brown fox jumps over the lazy dog",
    "a journey of a thousand miles begins with a single step",
    "to be or not to be that is the question",
    "all that glitters is not gold and roses are red",
    "the cats are running and the dogs were barking loudly",
    "berries and classes and quickly walking through the woods",
    "",
    "single",
];

/// FTS-021: a long random insert/update/delete sequence keeps the index
/// bit-identical to a fresh rebuild after every commit.
#[tokio::test(flavor = "multi_thread")]
async fn random_mutations_keep_the_index_rebuild_identical() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;
    let mut rng = Lcg(0x1234_5678_9abc_def0);
    let mut live: Vec<u64> = Vec::new();

    for step in 0..400u64 {
        let choice = rng.next(3);
        match choice {
            // Insert a fresh id.
            0 => {
                let id = step + 1;
                let body = CORPUS[(rng.next(CORPUS.len() as u64)) as usize].to_owned();
                h.pipeline
                    .call(Box::new(move |tx| {
                        tx.insert(table, row(id, &body))?;
                        Ok(())
                    }))
                    .await
                    .unwrap();
                live.push(id);
            }
            // Update an existing id (or no-op if none).
            1 if !live.is_empty() => {
                let id = live[(rng.next(live.len() as u64)) as usize];
                let body = CORPUS[(rng.next(CORPUS.len() as u64)) as usize].to_owned();
                h.pipeline
                    .call(Box::new(move |tx| {
                        tx.upsert(table, row(id, &body))?;
                        Ok(())
                    }))
                    .await
                    .unwrap();
            }
            // Delete an existing id (or no-op if none).
            2 if !live.is_empty() => {
                let idx = (rng.next(live.len() as u64)) as usize;
                let id = live.remove(idx);
                h.pipeline
                    .call(Box::new(move |tx| {
                        tx.delete(table, &[RowValue::U64(id)])?;
                        Ok(())
                    }))
                    .await
                    .unwrap();
            }
            _ => continue,
        }

        // The load-bearing invariant: the maintained index equals a fresh
        // rebuild over the committed rows (STG-007 rule 2, FTS-021).
        h.store
            .snapshot()
            .verify_index_integrity(table)
            .unwrap_or_else(|e| panic!("integrity broke at step {step}: {e}"));
    }
    assert!(!live.is_empty(), "the sequence exercised live rows");
}

/// FTS-022: the post-recovery rebuild path reproduces the index. Marking it
/// rebuilding empties it (and would gate queries with
/// `STORAGE_FULLTEXT_REBUILDING`); rebuilding from rows restores integrity.
#[tokio::test(flavor = "multi_thread")]
async fn rebuild_from_rows_restores_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;

    for (i, body) in CORPUS.iter().enumerate() {
        let id = i as u64 + 1;
        let body = (*body).to_owned();
        h.pipeline
            .call(Box::new(move |tx| {
                tx.insert(table, row(id, &body))?;
                Ok(())
            }))
            .await
            .unwrap();
    }
    h.store.snapshot().verify_index_integrity(table).unwrap();
    assert!(h.store.fulltext_ready());

    // Simulate the recovery gate: mark rebuilding (not ready), then rebuild.
    h.store.mark_fulltext_rebuilding();
    assert!(!h.store.fulltext_ready(), "gated while rebuilding");

    h.store.rebuild_fulltext_indexes().unwrap();
    assert!(h.store.fulltext_ready());
    h.store
        .snapshot()
        .verify_index_integrity(table)
        .expect("rebuild reproduces the maintained index");
}

/// A corpus far larger than a handful of rows is maintained correctly (the
/// index scales functionally; when the pager is wired into the live path the
/// same postings page and evict like the B-tree index — FTS-022).
#[tokio::test(flavor = "multi_thread")]
async fn a_large_corpus_stays_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(dir.path());
    let table = h.table;

    for id in 1..=1_000u64 {
        let body = format!(
            "{} document number {id} with shared terms fox dog running berries",
            CORPUS[(id as usize) % CORPUS.len()]
        );
        h.pipeline
            .call(Box::new(move |tx| {
                tx.insert(table, row(id, &body))?;
                Ok(())
            }))
            .await
            .unwrap();
    }
    h.store
        .snapshot()
        .verify_index_integrity(table)
        .expect("1000-document corpus stays rebuild-identical");
}
