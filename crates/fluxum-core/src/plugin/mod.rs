//! Plugin capability framework (SPEC-020): the closed capability set with
//! placement classes, the unified [`PluginRegistry`] with PLG-032 manifest
//! validation, the in-process host's panic isolation ([`PluginState`],
//! PLG-030), and the adoption of the existing extension seams
//! (`AuthProvider`, `ColumnTransform`, key material, `visibility(custom)`)
//! as introspectable capabilities (PLG-002) — without touching their APIs
//! or call sites.
//!
//! # The non-distortion contract (PLG-020/021/022)
//!
//! The deterministic single-writer commit path admits only deterministic,
//! bounded, in-process plugins. That rule is *structural* here:
//!
//! - [`Placement::WritePath`] capabilities reject a sidecar host at
//!   [`PluginRegistry::build`] time — the one exception is
//!   [`Capability::KeyProvider`] (an external KMS is legal only because the
//!   runtime caches keys, so the commit path makes no network call).
//! - [`Placement::ReadPath`] capabilities ([`ScoreReranker`], [`Retriever`],
//!   [`Fusion`]) only ever reorder a snapshot result — stored rows, indexes,
//!   and `TxUpdate` diffs are never a function of their output.
//! - [`Placement::OffPath`] ([`StreamSink`]) is fed committed deltas off the
//!   commit path and can only produce external side effects.
//!
//! # Hosting
//!
//! In-process plugins are compiled, feature-gated Rust registered at link
//! time ([`InProcPluginDef`], the DM-040 `inventory` pattern): when the
//! Cargo feature is off, the def simply does not exist in the binary, and a
//! manifest entry naming it fails [`PluginRegistry::build`] with a
//! descriptive error. Sidecar hosting (process-isolated Plugin RPC,
//! PLG-031) is declared and validated here; the generic RPC proxy is the
//! phase-5 `plugin-sidecar-host` task — until it lands, a sidecar binding
//! validates but carries no live instance.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::Serialize;

use crate::config::{Config, PluginDecl, PluginHost};
use crate::error::{FluxumError, Result};
use crate::schema::Schema;
use crate::store::{PkBytes, TxDiff};
use crate::types::Identity;

// ---------------------------------------------------------------------------
// Placement & capabilities (PLG-001/003/020)
// ---------------------------------------------------------------------------

/// Where a capability runs and what it may do (SPEC-020 PLG-020).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    /// Inside the reducer/commit transaction: deterministic + bounded,
    /// in-process only; an error rolls the transaction back.
    WritePath,
    /// Query/`InitialData`/one-off, after base evaluation: may be
    /// non-deterministic; failure falls back to the base result.
    ReadPath,
    /// Asynchronous, fed by the commit log: external side effects only,
    /// never feeds back into state, never stalls commit.
    OffPath,
}

/// The closed v1 capability set (PLG-001/003): every extension point is a
/// reviewed trait with a defined placement — there is no "run any code
/// anywhere" hook, and adding a capability is a spec change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// `AuthProvider` (SPEC-009 AUTH-030) — existing seam, adopted.
    Auth,
    /// `ColumnTransform` (SPEC-017 CT-010) — existing seam, adopted.
    ColumnTransform,
    /// Transform key material (SPEC-017 CT-035/037) — existing seam,
    /// adopted. The one WritePath capability permitted a sidecar (KMS),
    /// because keys are cached off the commit path (PLG-021).
    KeyProvider,
    /// `#[visibility(custom)]` predicate (SUB-032) — deterministic, classed
    /// WritePath-strict because subscription correctness depends on it.
    Visibility,
    /// Full-text re-rank of a `MATCH` top-K ([`ScoreReranker`], PLG-040).
    ScoreReranker,
    /// External candidate retrieval for hybrid fusion ([`Retriever`]).
    Retriever,
    /// Fusion of lexical + retriever lists ([`Fusion`]; default RRF).
    Fusion,
    /// CDC sink fed committed deltas off the commit path ([`StreamSink`]).
    StreamSink,
}

impl Capability {
    /// Parse a manifest `capability:` name (PLG-032).
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "auth" => Self::Auth,
            "column_transform" => Self::ColumnTransform,
            "key_provider" => Self::KeyProvider,
            "visibility" => Self::Visibility,
            "score_reranker" => Self::ScoreReranker,
            "retriever" => Self::Retriever,
            "fusion" => Self::Fusion,
            "stream_sink" => Self::StreamSink,
            _ => return None,
        })
    }

    /// The manifest/introspection name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::ColumnTransform => "column_transform",
            Self::KeyProvider => "key_provider",
            Self::Visibility => "visibility",
            Self::ScoreReranker => "score_reranker",
            Self::Retriever => "retriever",
            Self::Fusion => "fusion",
            Self::StreamSink => "stream_sink",
        }
    }

    /// The capability's placement class (PLG-020).
    pub fn placement(self) -> Placement {
        match self {
            Self::Auth | Self::ColumnTransform | Self::KeyProvider | Self::Visibility => {
                Placement::WritePath
            }
            Self::ScoreReranker | Self::Retriever | Self::Fusion => Placement::ReadPath,
            Self::StreamSink => Placement::OffPath,
        }
    }

    /// Whether a sidecar host may bind this capability (PLG-021): never on
    /// the WritePath — except `KeyProvider`, whose runtime caches keys so
    /// the commit path makes no per-transaction network call.
    pub fn sidecar_allowed(self) -> bool {
        match self.placement() {
            Placement::ReadPath | Placement::OffPath => true,
            Placement::WritePath => self == Self::KeyProvider,
        }
    }
}

// ---------------------------------------------------------------------------
// Invocation context, error, and exchange types
// ---------------------------------------------------------------------------

/// What every plugin invocation receives: the calling posture, never
/// ambient authority (PLG-061 — a plugin runs with no more privilege than
/// configured; RLS bypass requires an explicit server-peer grant).
#[derive(Debug, Clone, Copy)]
pub struct PluginCtx {
    /// The identity the enclosing operation runs under.
    pub identity: Identity,
    /// Whether that identity is a privileged server peer (AUTH-062).
    pub is_server_peer: bool,
    /// The shard the operation runs on.
    pub shard_id: u32,
}

/// A plugin-surfaced failure. ReadPath failures degrade to the base result
/// (PLG-031); WritePath failures roll the transaction back (PLG-030).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginError(pub String);

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PluginError {}

/// One scored candidate row: the primary key plus a relevance score.
#[derive(Debug, Clone, PartialEq)]
pub struct Scored {
    /// The row's encoded primary key.
    pub pk: PkBytes,
    /// The candidate's score (higher = more relevant).
    pub score: f64,
}

/// The v1 query surface handed to ReadPath plugins: which `MATCH` this is.
/// The phase-4 full-text MATCH task binds this to the real query pipeline
/// (SPEC-019 FTS-040); the shape is fixed here so plugin authors compile
/// against a stable contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtQuery {
    /// The queried table's struct name.
    pub table: String,
    /// The `#[fulltext]` column queried.
    pub column: String,
    /// The raw match query text.
    pub query: String,
    /// The requested result limit.
    pub limit: usize,
}

/// A batch of committed deltas delivered to a [`StreamSink`] (PLG-050):
/// at-least-once, in commit order, off the commit path.
#[derive(Debug, Clone)]
pub struct CommitBatch {
    /// The committed diffs, ascending by `tx_id`.
    pub diffs: Vec<TxDiff>,
}

/// A sink's resume point: the last fully processed `tx_id` (PLG-050).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub u64);

// ---------------------------------------------------------------------------
// New capability traits (PLG-001 §2.1 — definitions; sibling tasks bind them)
// ---------------------------------------------------------------------------

/// ReadPath: re-score/re-order the top-K candidates of a `MATCH` query
/// (SPEC-019). Snapshot-only — the returned order affects `InitialData`/
/// one-off results, never `TxUpdate` diffs (PLG-022).
pub trait ScoreReranker: Send + Sync {
    /// Return the candidates reordered (and possibly re-scored). Failure or
    /// timeout falls back to the base BM25 order (PLG-031/040).
    fn rerank(
        &self,
        query: &FtQuery,
        candidates: Vec<Scored>,
        ctx: &PluginCtx,
    ) -> std::result::Result<Vec<Scored>, PluginError>;
}

/// ReadPath: contribute external candidates + scores for hybrid fusion
/// (e.g. the family's Vectorizer). On failure the lexical result stands.
pub trait Retriever: Send + Sync {
    /// The external retriever's top-`k` `(primary key, score)` list.
    fn retrieve(
        &self,
        query: &FtQuery,
        k: usize,
        ctx: &PluginCtx,
    ) -> std::result::Result<Vec<Scored>, PluginError>;
}

/// Fusion of the lexical (BM25) and retriever result lists (PLG-041).
/// The default implementation is [`ReciprocalRankFusion`].
pub trait Fusion: Send + Sync {
    /// Fuse the two ranked lists into one.
    fn fuse(&self, lexical: &[Scored], dense: &[Scored], ctx: &PluginCtx) -> Vec<Scored>;
}

/// OffPath: receive committed deltas from the commit log, off the commit
/// path (CDC, PLG-050). Delivery is at-least-once; a slow sink is buffered
/// then dropped, never allowed to stall commits.
pub trait StreamSink: Send + Sync {
    /// Consume one committed batch (at-least-once).
    fn on_commit(&self, batch: &CommitBatch) -> std::result::Result<(), PluginError>;

    /// The sink's resume point (persisted per sink).
    fn checkpoint(&self) -> Offset;
}

/// PLG-040: how many BM25 candidates feed a bound [`ScoreReranker`]
/// (`rerank_candidate_k`; a manifest override is a follow-up).
pub const RERANK_CANDIDATE_K: usize = 100;

/// The default [`Fusion`]: Reciprocal Rank Fusion — rank-based, so no
/// score-scale normalization between BM25 and a dense retriever is needed
/// (PLG-041). `score(d) = Σ_lists 1 / (k + rank_d)` with the standard
/// `k = 60`; ties break toward the lexical list's order (deterministic).
#[derive(Debug, Clone, Copy)]
pub struct ReciprocalRankFusion {
    /// The RRF dampening constant (standard 60).
    pub k: f64,
}

impl Default for ReciprocalRankFusion {
    fn default() -> Self {
        Self { k: 60.0 }
    }
}

impl Fusion for ReciprocalRankFusion {
    fn fuse(&self, lexical: &[Scored], dense: &[Scored], _ctx: &PluginCtx) -> Vec<Scored> {
        // pk → (rrf score, first-seen order for deterministic ties).
        let mut fused: Vec<(PkBytes, f64, usize)> = Vec::new();
        let add = |list: &[Scored], fused: &mut Vec<(PkBytes, f64, usize)>| {
            for (rank, cand) in list.iter().enumerate() {
                #[allow(clippy::cast_precision_loss)] // ranks are tiny
                let contribution = 1.0 / (self.k + rank as f64 + 1.0);
                if let Some(entry) = fused.iter_mut().find(|(pk, _, _)| *pk == cand.pk) {
                    entry.1 += contribution;
                } else {
                    let order = fused.len();
                    fused.push((cand.pk.clone(), contribution, order));
                }
            }
        };
        add(lexical, &mut fused);
        add(dense, &mut fused);
        fused.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.2.cmp(&b.2))
        });
        fused
            .into_iter()
            .map(|(pk, score, _)| Scored { pk, score })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// In-process host (PLG-030)
// ---------------------------------------------------------------------------

/// A constructed in-process plugin instance, one variant per bindable
/// capability. (The adopted seams — auth, transforms, keys, visibility —
/// keep their existing installation paths and are introspected as
/// [`BuiltinSeam`]s instead.)
#[derive(Clone)]
pub enum PluginInstance {
    /// A [`ScoreReranker`] implementation.
    ScoreReranker(Arc<dyn ScoreReranker>),
    /// A [`Retriever`] implementation.
    Retriever(Arc<dyn Retriever>),
    /// A [`Fusion`] implementation.
    Fusion(Arc<dyn Fusion>),
    /// A [`StreamSink`] implementation.
    StreamSink(Arc<dyn StreamSink>),
}

impl PluginInstance {
    /// The capability this instance implements.
    pub fn capability(&self) -> Capability {
        match self {
            Self::ScoreReranker(_) => Capability::ScoreReranker,
            Self::Retriever(_) => Capability::Retriever,
            Self::Fusion(_) => Capability::Fusion,
            Self::StreamSink(_) => Capability::StreamSink,
        }
    }
}

impl std::fmt::Debug for PluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.capability().name())
    }
}

/// One compiled, feature-gated in-process plugin in the link-time registry
/// (PLG-030, the DM-040 `inventory` pattern). When the plugin crate's Cargo
/// feature is off, this def is absent from the binary — a manifest entry
/// naming it then fails [`PluginRegistry::build`].
pub struct InProcPluginDef {
    /// The plugin name the manifest binds (`plugins[].name`).
    pub name: &'static str,
    /// The Cargo feature that compiles this plugin in (introspection; the
    /// gating itself is the def's presence or absence).
    pub feature: &'static str,
    /// Construct the instance (called once at `build`).
    pub construct: fn() -> PluginInstance,
}

inventory::collect!(InProcPluginDef);

/// Every in-process plugin compiled into this binary (linker order).
pub fn registered_plugins() -> impl Iterator<Item = &'static InProcPluginDef> {
    inventory::iter::<InProcPluginDef>()
}

/// Per-plugin runtime state: the hot-disable flag (PLG-061) and the panic/
/// error meters (PLG-030). Shared by every invocation site via `Arc`.
#[derive(Debug, Default)]
pub struct PluginState {
    disabled: AtomicBool,
    panics: AtomicU64,
    errors: AtomicU64,
}

impl PluginState {
    /// Whether the plugin is currently disabled (hot circuit-break or
    /// panic auto-disable).
    pub fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    /// Hot-disable (or re-enable) without a core restart (PLG-061).
    pub fn set_disabled(&self, disabled: bool) {
        self.disabled.store(disabled, Ordering::Relaxed);
    }

    /// Panics caught so far (`fluxum_plugin_panics_total`).
    pub fn panics(&self) -> u64 {
        self.panics.load(Ordering::Relaxed)
    }

    /// Non-panic errors surfaced so far.
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Run one plugin invocation under `catch_unwind` isolation (PLG-030):
    /// a disabled plugin short-circuits; a panic increments the meter,
    /// auto-disables the plugin, and surfaces as a [`PluginError`] — for a
    /// WritePath plugin the enclosing transaction rolls back (RED-004
    /// pattern), the shard never crashes.
    pub fn guard<R>(
        &self,
        name: &str,
        f: impl FnOnce() -> std::result::Result<R, PluginError>,
    ) -> std::result::Result<R, PluginError> {
        if self.is_disabled() {
            return Err(PluginError(format!("plugin `{name}` is disabled")));
        }
        match std::panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
            Err(payload) => {
                self.panics.fetch_add(1, Ordering::Relaxed);
                self.set_disabled(true);
                Err(PluginError(format!(
                    "plugin `{name}` panicked and was disabled (PLG-030): {}",
                    crate::txn::panic_message(payload.as_ref())
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Bound plugins & the registry (PLG-032 validation, PLG-060 introspection)
// ---------------------------------------------------------------------------

/// How a validated plugin is hosted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundHost {
    /// Compiled into this binary behind `feature`.
    InProcess {
        /// The gating Cargo feature.
        feature: String,
    },
    /// A separate process at `endpoint`, called over Plugin RPC with
    /// `timeout_ms` per ReadPath/OffPath call (PLG-031; proxy lands with
    /// the phase-5 sidecar-host task).
    Sidecar {
        /// The sidecar's RPC endpoint.
        endpoint: String,
        /// Per-call timeout in milliseconds.
        timeout_ms: u64,
    },
}

/// One validated manifest binding.
pub struct BoundPlugin {
    /// The manifest name.
    pub name: String,
    /// The bound capability.
    pub capability: Capability,
    /// The hosting mode.
    pub host: BoundHost,
    /// The tables the plugin applies to (empty = unscoped).
    pub tables: Vec<String>,
    /// The columns the plugin applies to (empty = whole tables).
    pub columns: Vec<String>,
    /// The constructed instance — in-process only; a sidecar binding
    /// carries `None` until the phase-5 proxy task lands.
    pub instance: Option<PluginInstance>,
    /// Runtime disable flag + meters, shared with invocation sites.
    pub state: Arc<PluginState>,
}

/// An adopted built-in seam (PLG-002), introspected alongside manifest
/// plugins with its behavior and call sites unchanged.
#[derive(Debug, Clone, Serialize)]
pub struct BuiltinSeam {
    /// A stable descriptive name (e.g. `auth:token`).
    pub name: String,
    /// The capability the seam implements.
    pub capability: Capability,
    /// Placement class.
    pub placement: Placement,
    /// Human-readable scope/detail — never key material (PLG-060).
    pub detail: String,
}

/// One `GET /plugins` row (PLG-060): name, capability, host, placement,
/// health, scope — never secrets.
#[derive(Debug, Clone, Serialize)]
pub struct PluginInfo {
    /// Plugin (or seam) name.
    pub name: String,
    /// Capability name.
    pub capability: &'static str,
    /// Placement class.
    pub placement: Placement,
    /// Host description: `builtin`, `in_process(feature=…)`, or
    /// `sidecar(endpoint=…)`.
    pub host: String,
    /// `active` | `disabled`.
    pub health: String,
    /// Panics caught (PLG-030 meter).
    pub panics: u64,
    /// Errors surfaced.
    pub errors: u64,
    /// Table scope (empty = unscoped).
    pub tables: Vec<String>,
    /// Column scope (empty = whole tables).
    pub columns: Vec<String>,
    /// Human-readable detail (adopted seams: scope/key ids — never key
    /// material or tokens, PLG-060).
    pub detail: String,
}

/// The unified plugin registry (PLG-001/002): every validated manifest
/// binding plus the adopted built-in seams, built once at server assembly.
#[derive(Default)]
pub struct PluginRegistry {
    plugins: Vec<BoundPlugin>,
    builtins: Vec<BuiltinSeam>,
}

impl std::fmt::Debug for PluginRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRegistry")
            .field(
                "plugins",
                &self.plugins.iter().map(|p| &p.name).collect::<Vec<_>>(),
            )
            .field("builtins", &self.builtins.len())
            .finish()
    }
}

impl PluginRegistry {
    /// An empty registry (no manifest, no adopted seams) — test scaffolding.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build and validate the registry from the manifest (PLG-032): every
    /// binding's capability must exist, the placement must be legal for the
    /// host (PLG-020/021), an in-process name must be compiled into this
    /// binary, and `applies_to` targets must exist in the assembled schema.
    /// Any violation aborts startup with a descriptive error. Adopts the
    /// existing seams from `config`/`schema` for introspection (PLG-002).
    pub fn build(schema: &Schema, config: &Config) -> Result<Self> {
        let mut plugins: Vec<BoundPlugin> = Vec::with_capacity(config.plugins.len());
        for decl in &config.plugins {
            plugins.push(Self::bind(schema, decl)?);
        }
        for (i, plugin) in plugins.iter().enumerate() {
            if plugins[..i].iter().any(|p| p.name == plugin.name) {
                return Err(FluxumError::Config(format!(
                    "plugins: duplicate plugin name `{}` (PLG-032)",
                    plugin.name
                )));
            }
        }
        Ok(Self {
            plugins,
            builtins: adopt_builtins(schema, config),
        })
    }

    /// Validate and construct one manifest binding.
    fn bind(schema: &Schema, decl: &PluginDecl) -> Result<BoundPlugin> {
        let fail =
            |detail: String| FluxumError::Config(format!("plugins: `{}`: {detail}", decl.name));
        if decl.name.is_empty() {
            return Err(FluxumError::Config(
                "plugins: plugin name must be non-empty (PLG-032)".into(),
            ));
        }
        let capability = Capability::parse(&decl.capability).ok_or_else(|| {
            fail(format!(
                "unknown capability `{}` — the set is closed (PLG-003): auth, \
                 column_transform, key_provider, visibility, score_reranker, retriever, \
                 fusion, stream_sink",
                decl.capability
            ))
        })?;

        // PLG-020/021: placement legality for the host.
        let (host, instance) = match &decl.host {
            PluginHost::Sidecar {
                endpoint,
                timeout_ms,
            } => {
                if !capability.sidecar_allowed() {
                    return Err(fail(format!(
                        "capability `{}` is WritePath — a sidecar host would put a \
                         network round-trip on the deterministic commit path (PLG-021); \
                         host it in-process",
                        capability.name()
                    )));
                }
                if endpoint.is_empty() {
                    return Err(fail("sidecar host needs an `endpoint` (PLG-032)".into()));
                }
                (
                    BoundHost::Sidecar {
                        endpoint: endpoint.clone(),
                        timeout_ms: *timeout_ms,
                    },
                    // The generic RPC proxy is the phase-5 sidecar-host task.
                    None,
                )
            }
            PluginHost::InProcess { feature } => {
                let def = registered_plugins()
                    .find(|def| def.name == decl.name)
                    .ok_or_else(|| {
                        fail(format!(
                            "no in-process plugin named `{}` is compiled into this binary — \
                             the crate is missing or its Cargo feature{} is off (PLG-030)",
                            decl.name,
                            if feature.is_empty() {
                                String::new()
                            } else {
                                format!(" `{feature}`")
                            }
                        ))
                    })?;
                let instance = (def.construct)();
                if instance.capability() != capability {
                    return Err(fail(format!(
                        "manifest declares capability `{}` but the compiled plugin \
                         implements `{}` (PLG-032)",
                        capability.name(),
                        instance.capability().name()
                    )));
                }
                (
                    BoundHost::InProcess {
                        feature: def.feature.to_owned(),
                    },
                    Some(instance),
                )
            }
        };

        // applies_to targets must exist (PLG-032).
        for table in &decl.applies_to.tables {
            if schema.table(table).is_none() {
                return Err(fail(format!(
                    "applies_to names unknown table `{table}` (PLG-032)"
                )));
            }
        }
        for column in &decl.applies_to.columns {
            if decl.applies_to.tables.is_empty() {
                return Err(fail(format!(
                    "applies_to.columns (`{column}`) requires applies_to.tables (PLG-032)"
                )));
            }
            let known = decl.applies_to.tables.iter().any(|table| {
                schema
                    .table(table)
                    .is_some_and(|t| t.columns.iter().any(|c| c.name == column))
            });
            if !known {
                return Err(fail(format!(
                    "applies_to names column `{column}` absent from every listed table \
                     (PLG-032)"
                )));
            }
        }

        Ok(BoundPlugin {
            name: decl.name.clone(),
            capability,
            host,
            tables: decl.applies_to.tables.clone(),
            columns: decl.applies_to.columns.clone(),
            instance,
            state: Arc::new(PluginState::default()),
        })
    }

    /// The bound plugin named `name`, if any.
    pub fn get(&self, name: &str) -> Option<&BoundPlugin> {
        self.plugins.iter().find(|p| p.name == name)
    }

    /// Every bound plugin.
    pub fn plugins(&self) -> &[BoundPlugin] {
        &self.plugins
    }

    /// The first active (not disabled) in-process binding of `capability`
    /// whose `applies_to` scope covers `(table, column)` — an empty scope
    /// covers everything. The ReadPath query hooks (PLG-040/041) resolve
    /// their plugin through this; sidecar bindings carry no instance until
    /// the phase-5 proxy lands and are skipped here.
    pub fn readpath_binding(
        &self,
        capability: Capability,
        table: &str,
        column: &str,
    ) -> Option<&BoundPlugin> {
        self.plugins.iter().find(|plugin| {
            plugin.capability == capability
                && plugin.instance.is_some()
                && !plugin.state.is_disabled()
                && (plugin.tables.is_empty() || plugin.tables.iter().any(|t| t == table))
                && (plugin.columns.is_empty() || plugin.columns.iter().any(|c| c == column))
        })
    }

    /// Hot-disable or re-enable a plugin without a core restart (PLG-061).
    /// Returns whether `name` names a bound plugin. Built-in seams cannot be
    /// disabled — they are core subsystems, not optional plugins.
    pub fn set_disabled(&self, name: &str, disabled: bool) -> bool {
        match self.get(name) {
            Some(plugin) => {
                plugin.state.set_disabled(disabled);
                true
            }
            None => false,
        }
    }

    /// The `GET /plugins` report (PLG-060): every adopted seam and manifest
    /// binding — name, capability, host, placement, health, meters, scope.
    /// Never key material, tokens, or other secrets.
    pub fn report(&self) -> Vec<PluginInfo> {
        let mut out: Vec<PluginInfo> = self
            .builtins
            .iter()
            .map(|seam| PluginInfo {
                name: seam.name.clone(),
                capability: seam.capability.name(),
                placement: seam.placement,
                host: "builtin".to_owned(),
                health: "active".to_owned(),
                panics: 0,
                errors: 0,
                tables: Vec::new(),
                columns: Vec::new(),
                detail: seam.detail.clone(),
            })
            .collect();
        out.extend(self.plugins.iter().map(|plugin| PluginInfo {
            name: plugin.name.clone(),
            capability: plugin.capability.name(),
            placement: plugin.capability.placement(),
            host: match &plugin.host {
                BoundHost::InProcess { feature } => format!("in_process(feature={feature})"),
                BoundHost::Sidecar {
                    endpoint,
                    timeout_ms,
                } => format!("sidecar(endpoint={endpoint}, timeout_ms={timeout_ms})"),
            },
            health: if plugin.state.is_disabled() {
                "disabled".to_owned()
            } else {
                "active".to_owned()
            },
            panics: plugin.state.panics(),
            errors: plugin.state.errors(),
            tables: plugin.tables.clone(),
            columns: plugin.columns.clone(),
            detail: String::new(),
        }));
        out
    }
}

/// Adopt the existing extension seams as introspectable capabilities
/// (PLG-002) — read-only reflection over `config`/`schema`; their call
/// sites and behavior are untouched.
fn adopt_builtins(schema: &Schema, config: &Config) -> Vec<BuiltinSeam> {
    let mut seams = Vec::new();
    // AuthProvider (SPEC-009): always present — the configured scheme.
    seams.push(BuiltinSeam {
        name: format!("auth:{:?}", config.auth.provider).to_lowercase(),
        capability: Capability::Auth,
        placement: Placement::WritePath,
        detail: "SPEC-009 AuthProvider (AUTH-030), selected by auth.provider".to_owned(),
    });
    // ColumnTransform pipelines registered against this schema (SPEC-017).
    let transformed: Vec<String> = crate::transform::registered_column_transforms()
        .filter(|def| schema.table(def.table).is_some())
        .map(|def| format!("{}.{}", def.table, def.column))
        .collect();
    if !transformed.is_empty() {
        seams.push(BuiltinSeam {
            name: "column_transforms".to_owned(),
            capability: Capability::ColumnTransform,
            placement: Placement::WritePath,
            detail: format!("SPEC-017 pipelines on: {}", transformed.join(", ")),
        });
    }
    // Key material for the transform executors (CT-035/037) — ids only,
    // never secrets (PLG-060).
    if !config.transforms.keys.is_empty() {
        let ids: Vec<&str> = config
            .transforms
            .keys
            .iter()
            .map(|k| k.id.as_str())
            .collect();
        seams.push(BuiltinSeam {
            name: "key_provider:config".to_owned(),
            capability: Capability::KeyProvider,
            placement: Placement::WritePath,
            detail: format!("config-embedded transform keys: {}", ids.join(", ")),
        });
    }
    // #[visibility(custom)] predicates declared in the schema (SUB-032).
    let custom_visibility: Vec<String> = schema
        .tables()
        .filter_map(|table| match table.visibility {
            crate::schema::VisibilityRule::Custom(name) => Some(format!("{} → {name}", table.name)),
            _ => None,
        })
        .collect();
    if !custom_visibility.is_empty() {
        seams.push(BuiltinSeam {
            name: "visibility:custom".to_owned(),
            capability: Capability::Visibility,
            placement: Placement::WritePath,
            detail: format!("SUB-032 predicates: {}", custom_visibility.join(", ")),
        });
    }
    seams
}
