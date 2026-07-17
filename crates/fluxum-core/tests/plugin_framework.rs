//! SPEC-020 plugin framework core (PLG-001/002/003/020/021/030/032/060/061):
//! the closed capability set with placement classes, manifest validation at
//! `PluginRegistry::build`, in-process panic isolation, hot disable, RRF
//! fusion, and secret-free introspection.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::config::{Config, PluginDecl, PluginHost, PluginScope, TransformKey};
use fluxum_core::plugin::{
    Capability, FtQuery, Fusion, InProcPluginDef, Placement, PluginCtx, PluginError,
    PluginInstance, PluginRegistry, PluginState, ReciprocalRankFusion, Scored, ScoreReranker,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::PkBytes;
use fluxum_core::types::Identity;

// --- Fixtures --------------------------------------------------------------------

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
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Schema {
    Schema::from_tables([&ITEM]).unwrap()
}

fn ctx() -> PluginCtx {
    PluginCtx {
        identity: Identity::from_token("t"),
        is_server_peer: false,
        shard_id: 1,
    }
}

fn pk(byte: u8) -> PkBytes {
    PkBytes::from_bytes(vec![byte])
}

fn scored(byte: u8, score: f64) -> Scored {
    Scored {
        pk: pk(byte),
        score,
    }
}

fn query() -> FtQuery {
    FtQuery {
        table: "Item".into(),
        column: "description".into(),
        query: "widget".into(),
        limit: 10,
    }
}

/// A compiled in-process reranker: reverses the candidate order.
struct ReverseReranker;
impl ScoreReranker for ReverseReranker {
    fn rerank(
        &self,
        _query: &FtQuery,
        mut candidates: Vec<Scored>,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        candidates.reverse();
        Ok(candidates)
    }
}

/// A reranker that panics — the PLG-030 isolation case.
struct PanicReranker;
impl ScoreReranker for PanicReranker {
    fn rerank(
        &self,
        _query: &FtQuery,
        _candidates: Vec<Scored>,
        _ctx: &PluginCtx,
    ) -> Result<Vec<Scored>, PluginError> {
        panic!("model exploded");
    }
}

fn make_reverse() -> PluginInstance {
    PluginInstance::ScoreReranker(Arc::new(ReverseReranker))
}
fn make_panicky() -> PluginInstance {
    PluginInstance::ScoreReranker(Arc::new(PanicReranker))
}

// Link-time registration (PLG-030): present in this binary ⇔ "feature on".
fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "reverse_reranker", feature: "plugin-reverse", construct: make_reverse }
}
fluxum_core::schema::inventory::submit! {
    InProcPluginDef { name: "panic_reranker", feature: "plugin-panic", construct: make_panicky }
}

fn decl(name: &str, capability: &str, host: PluginHost) -> PluginDecl {
    PluginDecl {
        name: name.into(),
        capability: capability.into(),
        host,
        applies_to: PluginScope::default(),
    }
}

fn in_proc(name: &str, capability: &str) -> PluginDecl {
    decl(
        name,
        capability,
        PluginHost::InProcess {
            feature: String::new(),
        },
    )
}

fn sidecar(name: &str, capability: &str) -> PluginDecl {
    decl(
        name,
        capability,
        PluginHost::Sidecar {
            endpoint: "127.0.0.1:15810".into(),
            timeout_ms: 40,
        },
    )
}

fn config_with(plugins: Vec<PluginDecl>) -> Config {
    Config {
        plugins,
        ..Config::default()
    }
}

fn build_err(plugins: Vec<PluginDecl>) -> String {
    PluginRegistry::build(&schema(), &config_with(plugins))
        .expect_err("binding must be rejected")
        .to_string()
}

// --- PLG-001/003/020/021: capability set & placement ------------------------------

#[test]
fn capability_set_is_closed_with_fixed_placement() {
    // Every v1 capability parses to itself and back.
    for name in [
        "auth",
        "column_transform",
        "key_provider",
        "visibility",
        "score_reranker",
        "retriever",
        "fusion",
        "stream_sink",
    ] {
        let cap = Capability::parse(name).expect(name);
        assert_eq!(cap.name(), name);
    }
    assert!(Capability::parse("run_anything").is_none(), "closed set");

    // Placement classes (PLG-020) and the sidecar rule (PLG-021).
    assert_eq!(Capability::Auth.placement(), Placement::WritePath);
    assert_eq!(Capability::ScoreReranker.placement(), Placement::ReadPath);
    assert_eq!(Capability::StreamSink.placement(), Placement::OffPath);
    assert!(!Capability::Auth.sidecar_allowed());
    assert!(!Capability::ColumnTransform.sidecar_allowed());
    assert!(!Capability::Visibility.sidecar_allowed());
    // The one WritePath exception: KMS-backed keys, cached off the path.
    assert!(Capability::KeyProvider.sidecar_allowed());
    assert!(Capability::ScoreReranker.sidecar_allowed());
    assert!(Capability::StreamSink.sidecar_allowed());
}

// --- PLG-032: manifest validation ---------------------------------------------

#[test]
fn build_rejects_illegal_bindings_with_descriptive_errors() {
    // Unknown capability (closed set, PLG-003).
    let err = build_err(vec![in_proc("x", "mind_reader")]);
    assert!(err.contains("unknown capability") && err.contains("PLG-003"), "{err}");

    // Sidecar on a WritePath capability (PLG-021).
    let err = build_err(vec![sidecar("x", "column_transform")]);
    assert!(err.contains("WritePath") && err.contains("PLG-021"), "{err}");

    // In-proc plugin not compiled into the binary (PLG-030).
    let err = build_err(vec![in_proc("absent_plugin", "score_reranker")]);
    assert!(err.contains("compiled into this binary"), "{err}");

    // Declared capability disagrees with the compiled instance.
    let err = build_err(vec![in_proc("reverse_reranker", "retriever")]);
    assert!(err.contains("implements") && err.contains("score_reranker"), "{err}");

    // applies_to targets must exist.
    let mut bad_table = sidecar("x", "score_reranker");
    bad_table.applies_to.tables = vec!["Ghost".into()];
    let err = build_err(vec![bad_table]);
    assert!(err.contains("unknown table `Ghost`"), "{err}");

    let mut bad_column = sidecar("x", "score_reranker");
    bad_column.applies_to.tables = vec!["Item".into()];
    bad_column.applies_to.columns = vec!["ghost_col".into()];
    let err = build_err(vec![bad_column]);
    assert!(err.contains("ghost_col"), "{err}");

    let mut orphan_column = sidecar("x", "score_reranker");
    orphan_column.applies_to.columns = vec!["description".into()];
    let err = build_err(vec![orphan_column]);
    assert!(err.contains("requires applies_to.tables"), "{err}");

    // Duplicate names.
    let err = build_err(vec![
        sidecar("dup", "score_reranker"),
        sidecar("dup", "retriever"),
    ]);
    assert!(err.contains("duplicate plugin name"), "{err}");
}

#[test]
fn a_legal_set_builds_and_reports() {
    let mut reranker = in_proc("reverse_reranker", "score_reranker");
    reranker.applies_to.tables = vec!["Item".into()];
    reranker.applies_to.columns = vec!["description".into()];
    let registry = PluginRegistry::build(
        &schema(),
        &config_with(vec![reranker, sidecar("vec_hybrid", "retriever")]),
    )
    .unwrap();

    let bound = registry.get("reverse_reranker").unwrap();
    assert_eq!(bound.capability, Capability::ScoreReranker);
    assert!(bound.instance.is_some(), "in-proc instance constructed");
    let side = registry.get("vec_hybrid").unwrap();
    assert!(
        side.instance.is_none(),
        "sidecar proxy is the phase-5 task; binding validates only"
    );

    let report = registry.report();
    let names: Vec<&str> = report.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"reverse_reranker") && names.contains(&"vec_hybrid"));
    // Adopted seams (PLG-002): the configured auth provider is always there.
    assert!(
        report.iter().any(|p| p.capability == "auth" && p.host == "builtin"),
        "auth seam adopted: {names:?}"
    );
}

// --- PLG-060/061: introspection has no secrets; hot disable ---------------------

#[test]
fn report_lists_key_ids_but_never_secrets() {
    let secret_hex = "aa".repeat(32);
    let mut config = config_with(vec![]);
    config.transforms.keys.push(TransformKey {
        id: "orders_key".into(),
        scheme: fluxum_core::config::KeyScheme::X25519,
        secret: secret_hex.clone(),
        previous: vec![],
    });
    let registry = PluginRegistry::build(&schema(), &config).unwrap();
    let report = registry.report();
    let key_seam = report
        .iter()
        .find(|p| p.capability == "key_provider")
        .expect("key material adopted as a seam");
    let json = serde_json::to_string(&report).unwrap();
    assert!(key_seam.name.contains("config"));
    assert!(json.contains("orders_key"), "key ids are listed");
    assert!(!json.contains(&secret_hex), "key material never leaks (PLG-060)");
}

#[test]
fn hot_disable_flips_health_without_rebuild() {
    let registry =
        PluginRegistry::build(&schema(), &config_with(vec![sidecar("r", "retriever")])).unwrap();
    assert!(registry.set_disabled("r", true), "known plugin");
    assert!(!registry.set_disabled("ghost", true), "unknown plugin");
    let report = registry.report();
    let plugin = report.iter().find(|p| p.name == "r").unwrap();
    assert_eq!(plugin.health, "disabled");
    registry.set_disabled("r", false);
    let report = registry.report();
    assert_eq!(report.iter().find(|p| p.name == "r").unwrap().health, "active");
}

// --- PLG-030: in-process panic isolation ----------------------------------------

#[test]
fn panicking_plugin_is_disabled_and_metered_not_fatal() {
    let registry = PluginRegistry::build(
        &schema(),
        &config_with(vec![in_proc("panic_reranker", "score_reranker")]),
    )
    .unwrap();
    let plugin = registry.get("panic_reranker").unwrap();
    let Some(PluginInstance::ScoreReranker(reranker)) = &plugin.instance else {
        panic!("expected a reranker instance");
    };

    let candidates = vec![scored(1, 0.9), scored(2, 0.5)];
    let err = plugin
        .state
        .guard("panic_reranker", || {
            reranker.rerank(&query(), candidates.clone(), &ctx())
        })
        .expect_err("the panic surfaces as a PluginError, never unwinds");
    assert!(err.0.contains("panicked") && err.0.contains("model exploded"), "{err}");
    assert_eq!(plugin.state.panics(), 1, "fluxum_plugin_panics_total");
    assert!(plugin.state.is_disabled(), "auto-disabled (PLG-030)");

    // Subsequent calls short-circuit while disabled.
    let err = plugin
        .state
        .guard("panic_reranker", || {
            reranker.rerank(&query(), candidates.clone(), &ctx())
        })
        .expect_err("disabled plugin never runs");
    assert!(err.0.contains("disabled"), "{err}");
    assert_eq!(plugin.state.panics(), 1, "no second panic — it never ran");

    // Errors (non-panic) are metered separately.
    let state = PluginState::default();
    let err = state
        .guard::<()>("e", || Err(PluginError("timeout".into())))
        .expect_err("error propagates");
    assert_eq!(err.0, "timeout");
    assert_eq!((state.errors(), state.panics()), (1, 0));
    assert!(!state.is_disabled(), "plain errors do not disable");
}

#[test]
fn healthy_in_proc_plugin_runs_under_guard() {
    let registry = PluginRegistry::build(
        &schema(),
        &config_with(vec![in_proc("reverse_reranker", "score_reranker")]),
    )
    .unwrap();
    let plugin = registry.get("reverse_reranker").unwrap();
    let Some(PluginInstance::ScoreReranker(reranker)) = &plugin.instance else {
        panic!("expected a reranker instance");
    };
    let reordered = plugin
        .state
        .guard("reverse_reranker", || {
            reranker.rerank(&query(), vec![scored(1, 0.9), scored(2, 0.5)], &ctx())
        })
        .unwrap();
    assert_eq!(
        reordered.iter().map(|s| s.pk.as_bytes()[0]).collect::<Vec<_>>(),
        vec![2, 1],
        "plugin output honored on the read path"
    );
}

// --- PLG-041: Reciprocal Rank Fusion reference behavior --------------------------

#[test]
fn rrf_fuses_by_rank_matching_the_reference_formula() {
    let fusion = ReciprocalRankFusion::default();
    // Lexical: A, B, C — Dense: B, D.
    let lexical = vec![scored(b'A', 12.0), scored(b'B', 8.0), scored(b'C', 3.0)];
    let dense = vec![scored(b'B', 0.99), scored(b'D', 0.42)];
    let fused = fusion.fuse(&lexical, &dense, &ctx());

    let order: Vec<u8> = fused.iter().map(|s| s.pk.as_bytes()[0]).collect();
    // Reference RRF (k=60): B = 1/62 + 1/61, A = 1/61, D = 1/62, C = 1/63.
    assert_eq!(order, vec![b'B', b'A', b'D', b'C']);
    let b = 1.0 / 62.0 + 1.0 / 61.0;
    assert!((fused[0].score - b).abs() < 1e-12, "score is the RRF sum");

    // Disabling the dense half degrades to the lexical order (PLG-041).
    let lexical_only = fusion.fuse(&lexical, &[], &ctx());
    let order: Vec<u8> = lexical_only.iter().map(|s| s.pk.as_bytes()[0]).collect();
    assert_eq!(order, vec![b'A', b'B', b'C']);
}

// --- Manifest YAML shape (PLG-032) ----------------------------------------------

#[test]
fn manifest_yaml_parses_the_spec_shape() {
    let yaml = r#"
plugins:
  - name: ft_reranker
    capability: score_reranker
    host: { kind: sidecar, endpoint: "127.0.0.1:15810", timeout_ms: 40 }
    applies_to: { tables: [Item], columns: [description] }
  - name: reverse_reranker
    capability: score_reranker
    host: { kind: in_process, feature: "plugin-reverse" }
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.plugins.len(), 2);
    let registry = PluginRegistry::build(&schema(), &config).unwrap();
    assert_eq!(registry.plugins().len(), 2);
    let side = registry.get("ft_reranker").unwrap();
    assert_eq!(side.tables, vec!["Item"]);
    assert_eq!(side.columns, vec!["description"]);
}
