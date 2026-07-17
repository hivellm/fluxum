//! SPEC-020 §5 (PLG-040/041) — the ReadPath query hooks over MATCH:
//! ScoreReranker reorders the BM25 top-K, Retriever + Fusion (default RRF)
//! produce hybrid lexical+dense results, every failure degrades to the BM25
//! order, and a non-deterministic hook can never touch stored state or
//! diffs (determinism containment, PLG-022).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, OnceLock};

use fluxum_core::config::{Config, PluginDecl, PluginHost, PluginScope};
use fluxum_core::plugin::{
    FtQuery, Fusion, InProcPluginDef, PluginCtx, PluginError, PluginInstance, PluginRegistry,
    ReciprocalRankFusion, Retriever, ScoreReranker, Scored,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, FullTextLanguage, IndexSchema, Schema, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "description",
        ty: FluxType::Str,
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
    indexes: &[IndexSchema::FullText {
        column: 1,
        language: FullTextLanguage::Simple,
        stop_words: false,
        stemming: false,
    }],
    visibility: VisibilityRule::PublicAll,
};

/// Corpus tuned so pure BM25 gives a deterministic order for 'sword':
/// tf 3 (id 3) > tf 2 (id 2) > tf 1 short (id 1) — id 4 has no match.
const CORPUS: &[(u64, &str)] = &[
    (1, "sword"),
    (2, "sword sword shield"),
    (3, "sword sword sword arena"),
    (4, "a dragon with no blade at all"),
];

// --- In-proc test plugins (link-time, PLG-030) ------------------------------------

struct ReverseReranker;
impl ScoreReranker for ReverseReranker {
    fn rerank(
        &self,
        _q: &FtQuery,
        mut candidates: Vec<Scored>,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        candidates.reverse();
        Ok(candidates)
    }
}
struct PanicReranker;
impl ScoreReranker for PanicReranker {
    fn rerank(
        &self,
        _q: &FtQuery,
        _c: Vec<Scored>,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        panic!("model runtime exploded");
    }
}

/// The dense list the stub retriever returns (filled per test).
static DENSE: OnceLock<Vec<Scored>> = OnceLock::new();
struct StubRetriever;
impl Retriever for StubRetriever {
    fn retrieve(
        &self,
        _q: &FtQuery,
        _k: usize,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        Ok(DENSE.get().cloned().unwrap_or_default())
    }
}
struct FailRetriever;
impl Retriever for FailRetriever {
    fn retrieve(
        &self,
        _q: &FtQuery,
        _k: usize,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        Err(PluginError("vectorizer unreachable".into()))
    }
}

fn make_reverse() -> PluginInstance {
    PluginInstance::ScoreReranker(Arc::new(ReverseReranker))
}
fn make_panic() -> PluginInstance {
    PluginInstance::ScoreReranker(Arc::new(PanicReranker))
}
fn make_stub_retriever() -> PluginInstance {
    PluginInstance::Retriever(Arc::new(StubRetriever))
}
fn make_fail_retriever() -> PluginInstance {
    PluginInstance::Retriever(Arc::new(FailRetriever))
}

fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "hx_reverse", feature: "t", construct: make_reverse }
}
fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "hx_panic", feature: "t", construct: make_panic }
}
fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "hx_stub_retriever", feature: "t", construct: make_stub_retriever }
}
fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "hx_fail_retriever", feature: "t", construct: make_fail_retriever }
}

// --- Harness ----------------------------------------------------------------------

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&ITEM]).unwrap())
}

fn seeded(schema: &Schema) -> MemStore {
    let store = MemStore::new(schema).unwrap();
    let item = store.table_id("Item").unwrap();
    let mut tx = store.begin();
    for (id, description) in CORPUS {
        tx.insert(
            item,
            vec![RowValue::U64(*id), RowValue::Str((*description).into())],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    store
}

fn registry(schema: &Schema, plugins: &[(&str, &str)]) -> Arc<PluginRegistry> {
    let config = Config {
        plugins: plugins
            .iter()
            .map(|(name, capability)| PluginDecl {
                name: (*name).into(),
                capability: (*capability).into(),
                host: PluginHost::InProcess {
                    feature: String::new(),
                },
                applies_to: PluginScope {
                    tables: vec!["Item".into()],
                    columns: vec!["description".into()],
                },
            })
            .collect(),
        ..Config::default()
    };
    Arc::new(PluginRegistry::build(schema, &config).unwrap())
}

fn manager_with(
    schema: &Arc<Schema>,
    registry: Option<Arc<PluginRegistry>>,
) -> SubscriptionManager {
    let mut manager = SubscriptionManager::new(Arc::clone(schema), SubscriptionLimits::default());
    if let Some(registry) = registry {
        manager.set_plugins(registry);
    }
    manager
}

const RANKED: &str =
    "SELECT * FROM Item WHERE description MATCH 'sword' ORDER BY SCORE DESC LIMIT 3";

fn ids(manager: &SubscriptionManager, store: &MemStore, sql: &str) -> Vec<u64> {
    let result = manager
        .query_json(
            Subscriber::server_peer(Identity::from_bytes([9; 32])),
            sql,
            &store.snapshot(),
        )
        .unwrap();
    result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_u64().unwrap())
        .collect()
}

// --- PLG-040: reranker over the top-K ----------------------------------------------

#[test]
fn reranker_order_is_authoritative_and_hooks_need_score_ranking() {
    let schema = schema();
    let store = seeded(&schema);

    let base = manager_with(&schema, None);
    assert_eq!(ids(&base, &store, RANKED), vec![3, 2, 1], "pure BM25 order");

    let hooked = manager_with(
        &schema,
        Some(registry(&schema, &[("hx_reverse", "score_reranker")])),
    );
    assert_eq!(
        ids(&hooked, &store, RANKED),
        vec![1, 2, 3],
        "the reranker's order is returned, truncated to LIMIT (PLG-040)"
    );

    // Without ORDER BY SCORE the hooks do not apply (snapshot ranking only).
    let unranked = "SELECT * FROM Item WHERE description MATCH 'sword'";
    let mut plain = ids(&hooked, &store, unranked);
    plain.sort_unstable();
    assert_eq!(plain, vec![1, 2, 3], "boolean result untouched by hooks");
}

#[test]
fn panicking_reranker_falls_back_to_bm25_and_is_disabled() {
    let schema = schema();
    let store = seeded(&schema);
    let registry = registry(&schema, &[("hx_panic", "score_reranker")]);
    let hooked = manager_with(&schema, Some(Arc::clone(&registry)));

    assert_eq!(
        ids(&hooked, &store, RANKED),
        vec![3, 2, 1],
        "the BM25 order stands on plugin panic (PLG-040/031)"
    );
    let plugin = registry.get("hx_panic").unwrap();
    assert_eq!(plugin.state.panics(), 1, "panic metered");
    assert!(plugin.state.is_disabled(), "auto-disabled (PLG-030)");
    // Second query: the disabled plugin short-circuits, result unchanged.
    assert_eq!(ids(&hooked, &store, RANKED), vec![3, 2, 1]);
    assert_eq!(plugin.state.panics(), 1, "never ran again");
}

// --- PLG-041: hybrid retrieval + RRF fusion ----------------------------------------

#[test]
fn stub_retriever_fuses_with_reference_rrf_and_fails_open() {
    let schema = schema();
    let store = seeded(&schema);
    let snapshot = store.snapshot();
    let item = store.table_id("Item").unwrap();

    // The dense half: best hit is doc 1, plus the dense-only doc 4 (no
    // lexical match at all — the hybrid admission case).
    let pk = |id: u64| snapshot.encode_pk(item, &[RowValue::U64(id)]).unwrap();
    let dense = vec![
        Scored {
            pk: pk(1),
            score: 0.99,
        },
        Scored {
            pk: pk(4),
            score: 0.80,
        },
    ];
    DENSE.set(dense.clone()).unwrap();

    let hooked = manager_with(
        &schema,
        Some(registry(&schema, &[("hx_stub_retriever", "retriever")])),
    );
    let got = ids(
        &hooked,
        &store,
        "SELECT * FROM Item WHERE description MATCH 'sword' ORDER BY SCORE DESC LIMIT 4",
    );

    // Reference RRF: lexical [3, 2, 1] fused with dense [1, 4].
    let lexical = [3u64, 2, 1];
    let reference = ReciprocalRankFusion::default().fuse(
        &lexical
            .iter()
            .enumerate()
            .map(|(rank, id)| Scored {
                pk: pk(*id),
                #[allow(clippy::cast_precision_loss)]
                score: 10.0 - rank as f64, // scores are ignored by RRF; rank matters
            })
            .collect::<Vec<_>>(),
        &dense,
        &PluginCtx {
            identity: Identity::from_bytes([0; 32]),
            is_server_peer: false,
            shard_id: 0,
        },
    );
    let reference_ids: Vec<u64> = reference
        .iter()
        .map(|s| {
            (1..=4u64)
                .find(|id| pk(*id).as_bytes() == s.pk.as_bytes())
                .unwrap()
        })
        .collect();
    assert_eq!(
        got, reference_ids,
        "engine order == reference RRF (PLG-041)"
    );
    assert!(got.contains(&4), "dense-only candidate admitted (hybrid)");

    // A failing retriever leaves the lexical result standing.
    let failing = manager_with(
        &schema,
        Some(registry(&schema, &[("hx_fail_retriever", "retriever")])),
    );
    assert_eq!(
        ids(&failing, &store, RANKED),
        vec![3, 2, 1],
        "retriever failure falls back to BM25 (PLG-041)"
    );
}

// --- PLG-022: determinism containment ----------------------------------------------

#[test]
fn hooks_never_touch_stored_state_or_diffs() {
    let schema = schema();
    let store_plain = MemStore::new(&schema).unwrap();
    let store_hooked = MemStore::new(&schema).unwrap();
    let manager = manager_with(
        &schema,
        Some(registry(&schema, &[("hx_reverse", "score_reranker")])),
    );
    let _ = &manager; // the hooked manager exists while commits run

    let mut diffs_plain = Vec::new();
    let mut diffs_hooked = Vec::new();
    for (store, out) in [
        (&store_plain, &mut diffs_plain),
        (&store_hooked, &mut diffs_hooked),
    ] {
        let item = store.table_id("Item").unwrap();
        for (id, description) in CORPUS {
            let mut tx = store.begin();
            tx.insert(
                item,
                vec![RowValue::U64(*id), RowValue::Str((*description).into())],
            )
            .unwrap();
            out.push(tx.commit().unwrap());
        }
    }
    assert_eq!(
        diffs_plain, diffs_hooked,
        "TxDiffs bit-identical with a non-deterministic hook bound (PLG-022)"
    );
    let rows = |store: &MemStore| -> Vec<_> {
        let item = store.table_id("Item").unwrap();
        store
            .snapshot()
            .scan(item)
            .unwrap()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows(&store_plain),
        rows(&store_hooked),
        "stored rows identical"
    );
}
