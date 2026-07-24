//! Subscription manager (SPEC-005 §4–6, T4.2; FR-30/31/34): the per-shard
//! registry of compiled subscription plans and the post-commit fan-out that
//! turns a [`TxDiff`] into per-query [`TableUpdate`] deltas — deduplicated by
//! [`QueryHash`], pruned by value, and encoded once per query.
//!
//! # What this task owns (and what it defers)
//!
//! - **Registry + lifecycle** (SUB-001..006/040/044): `Subscribe`,
//!   `SubscribeSingle`, `Unsubscribe`, disconnect cleanup; server-assigned
//!   `query_id`s; admission caps (429).
//! - **Dedup** (SUB-020): N identical (normalized) queries share exactly one
//!   [`Arc<CompiledPlan>`]; a query's delta is evaluated and encoded **once**,
//!   then every subscriber gets a refcount bump of the shared bytes.
//! - **Pruning** (SUB-023/040): equality plans live in `search_args`
//!   (value-indexed), the rest in `table_watchers`; a commit selects
//!   candidates by its delta-row *values*, never by scanning all plans.
//! - **InitialData** (SUB-013): ordered/limited snapshot, identical to a
//!   direct `CommittedState` query.
//!
//! Deferred to sibling tasks: RLS/`#[visibility]` (T4.3 fills the plan's
//! `rls` slot; here the manager threads the caller identity through but the
//! slot is `None`), and the actual socket delivery + backpressure tiers
//! (T4.4 — this task produces the shared bytes and the subscriber list; the
//! transport consumes them).
//!
//! # Cost model (SUB-022)
//!
//! Per commit the work is `O(P_matched + S_matched)`: only plans selected by
//! the pruning indexes are evaluated, and per subscriber the cost is one
//! enqueue of already-encoded bytes. Plans not selected and clients not
//! subscribed to a matched plan incur zero per-commit work — the manager
//! never iterates all connected clients or all registered plans.

pub mod sendbuffer;

#[cfg(test)]
mod proptest;

pub use sendbuffer::{
    BLOCKED_DROP_AFTER, DropReason, Message, Offered, SubscriberBuffer, SubscriberDropCounter, Tier,
};

mod matview;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluxum_protocol::codes;
use fluxum_protocol::{InitialData, RowList, RowListBuilder, TableUpdate, TxUpdate};

use crate::error::{FluxumError, Result};
use crate::schema::{Schema, TableSchema};
use crate::sql::{AccessPath, CompiledPlan, QueryHash, SpatialConstraint, compile};
use crate::store::committed::Snapshot;
use crate::store::row::{encode_pk_of_row, encode_pk_values, encode_row};
use crate::store::{Row, TableId, TxDiff};
use crate::types::Identity;

/// Who is subscribing (SUB-030/031): the caller's stable identity and
/// whether the auth layer resolved it as a server-to-server peer
/// (`SHA-256("SERVER:" + name)`, AUTH-061). Server peers bypass every
/// `#[visibility]` filter; the manager cannot tell a server identity from
/// its bytes alone, so the transport (which holds the
/// [`crate::auth::Authenticator`]) supplies this flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscriber {
    /// The caller's 256-bit identity (SPEC-009).
    pub identity: Identity,
    /// Whether this identity is a trusted server peer (RLS bypass, SUB-031).
    pub is_server_peer: bool,
    /// The caller's RBAC roles (AUTH-070) — what `#[column_grant(select =
    /// "role")]` resolves against (SPEC-017 CT-040). Empty for role-less
    /// callers; cheap to clone.
    pub roles: Arc<[String]>,
}

impl Subscriber {
    /// A regular client subscriber (RLS applies), no roles.
    pub fn client(identity: Identity) -> Self {
        Self {
            identity,
            is_server_peer: false,
            roles: Arc::from([]),
        }
    }

    /// A client subscriber carrying its auth-layer roles (CT-040).
    pub fn client_with_roles(identity: Identity, roles: impl Into<Arc<[String]>>) -> Self {
        Self {
            identity,
            is_server_peer: false,
            roles: roles.into(),
        }
    }

    /// A server-peer subscriber (RLS + grant bypass, SUB-031/AUTH-062).
    pub fn server_peer(identity: Identity) -> Self {
        Self {
            identity,
            is_server_peer: true,
            roles: Arc::from([]),
        }
    }
}

/// One membership table's live `(member identity, encoded key)` pairs
/// (RV-041).
type MemberSet = HashSet<([u8; 32], Vec<u8>)>;

/// One resolved `member_of` visibility rule (SPEC-022 RV-040).
struct MembershipSpec {
    /// The protected table.
    protected: TableId,
    /// The membership table.
    member_table: TableId,
    /// The join-key column's ordinal in the protected table.
    key_in_protected: u16,
    /// The join-key column's ordinal in the membership table.
    key_in_member: u16,
    /// The membership table's member-identity column ordinal.
    identity_in_member: u16,
}

/// The `search_args` value key: the FluxBIN encoding of one equality value
/// ([`RowValue`] is not `Ord`/`Hash` because of floats, but its byte
/// encoding is). Equality is exact, so any deterministic encoding indexes
/// correctly.
type ValueKey = Vec<u8>;

/// SEC-045 query execution bounds: the ceilings the snapshot evaluator
/// enforces on every `InitialData` / one-off read. Interior-mutable atomics
/// so the server's hot-reload path (OPS-040) can retune a *running* manager
/// through the shared `Arc` without pausing evaluation.
///
/// Every field's zero value disables that bound — the built-in default is
/// fully unbounded (today's behavior); the server config supplies generous
/// production defaults and operators tighten per deployment.
#[derive(Debug, Default)]
pub struct QueryBounds {
    /// Applied to queries that carry no `LIMIT` (`0` = none).
    default_limit: std::sync::atomic::AtomicU32,
    /// Ceiling on any effective `LIMIT` (`0` = unbounded).
    max_limit: std::sync::atomic::AtomicU32,
    /// `true`: a `LIMIT` above `max_limit` is rejected (3030); `false` (the
    /// default): it is clamped to `max_limit`.
    reject_over_max: std::sync::atomic::AtomicBool,
    /// Rows the evaluator may touch per query before aborting (`0` = no
    /// budget).
    row_scan_budget: std::sync::atomic::AtomicU64,
    /// Wall-clock evaluation deadline in milliseconds (`0` = none).
    deadline_ms: std::sync::atomic::AtomicU64,
}

impl QueryBounds {
    /// Publish new bounds (boot and OPS-040 hot reload land here).
    pub fn set(
        &self,
        default_limit: u32,
        max_limit: u32,
        reject_over_max: bool,
        row_scan_budget: u64,
        deadline_ms: u64,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        self.default_limit.store(default_limit, Relaxed);
        self.max_limit.store(max_limit, Relaxed);
        self.reject_over_max.store(reject_over_max, Relaxed);
        self.row_scan_budget.store(row_scan_budget, Relaxed);
        self.deadline_ms.store(deadline_ms, Relaxed);
    }

    fn default_limit(&self) -> u32 {
        self.default_limit
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn max_limit(&self) -> u32 {
        self.max_limit.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn reject_over_max(&self) -> bool {
        self.reject_over_max
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn row_scan_budget(&self) -> u64 {
        self.row_scan_budget
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn deadline_ms(&self) -> u64 {
        self.deadline_ms.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Per-query scan accounting against [`QueryBounds`] (SEC-045): counts every
/// candidate row the evaluator touches, aborts the evaluation when the
/// row-scan budget or the wall-clock deadline is breached. The deadline is
/// polled every [`DEADLINE_POLL_ROWS`] rows (and once at the end), so the
/// hot path pays one `Cell` bump per row, not a clock read.
struct ScanGuard {
    budget: u64,
    deadline: Option<std::time::Instant>,
    scanned: std::cell::Cell<u64>,
    aborted: std::cell::Cell<Option<crate::metrics::QueryAbortReason>>,
}

/// How many admitted rows pass between wall-clock deadline polls.
const DEADLINE_POLL_ROWS: u64 = 256;

impl ScanGuard {
    fn new(bounds: &QueryBounds) -> Self {
        let deadline_ms = bounds.deadline_ms();
        Self {
            budget: bounds.row_scan_budget(),
            deadline: (deadline_ms > 0)
                .then(|| std::time::Instant::now() + std::time::Duration::from_millis(deadline_ms)),
            scanned: std::cell::Cell::new(0),
            aborted: std::cell::Cell::new(None),
        }
    }

    /// Account one candidate row; `false` once the evaluation is aborted —
    /// callers stop scanning (or filter everything out) from then on.
    fn admit(&self) -> bool {
        use crate::metrics::QueryAbortReason;
        if self.aborted.get().is_some() {
            return false;
        }
        QUERY_ROWS_SCANNED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let scanned = self.scanned.get() + 1;
        self.scanned.set(scanned);
        if self.budget > 0 && scanned > self.budget {
            self.aborted.set(Some(QueryAbortReason::ScanBudget));
            return false;
        }
        if scanned.is_multiple_of(DEADLINE_POLL_ROWS)
            && let Some(deadline) = self.deadline
            && std::time::Instant::now() > deadline
        {
            self.aborted.set(Some(QueryAbortReason::Deadline));
            return false;
        }
        true
    }

    /// Whether the evaluation was aborted (loop-break form of [`Self::admit`]).
    fn is_aborted(&self) -> bool {
        self.aborted.get().is_some()
    }

    /// Surface an abort as its typed wire error (3031/3032), counting it;
    /// also the final deadline poll (covers sorts and small-scan tails the
    /// per-row cadence missed).
    fn finish(&self, metrics: Option<&crate::metrics::Metrics>) -> Result<()> {
        use crate::metrics::QueryAbortReason;
        if self.aborted.get().is_none()
            && let Some(deadline) = self.deadline
            && std::time::Instant::now() > deadline
        {
            self.aborted.set(Some(QueryAbortReason::Deadline));
        }
        match self.aborted.get() {
            None => Ok(()),
            Some(reason) => {
                if let Some(metrics) = metrics {
                    metrics.note_query_aborted(reason);
                }
                Err(match reason {
                    QueryAbortReason::ScanBudget => FluxumError::query(
                        codes::SQL_SCAN_BUDGET_EXCEEDED,
                        format!(
                            "query aborted: row-scan budget of {} rows exceeded (SEC-045)",
                            self.budget
                        ),
                    ),
                    _ => FluxumError::query(
                        codes::SQL_DEADLINE_EXCEEDED,
                        "query aborted: execution deadline exceeded (SEC-045)",
                    ),
                })
            }
        }
    }
}

/// Configurable subscription admission caps (SUB-044).
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionLimits {
    /// Max live subscriptions per connection (default 1,000).
    pub max_subscriptions_per_connection: usize,
    /// Max unique `QueryState` entries shard-wide (default 100,000).
    pub max_compiled_plans: usize,
    /// SPEC-021 CS-021: how many committed deltas each query retains for
    /// resumption (default 256). A `Resume` whose `from_offset` predates the
    /// retained window falls back to a full snapshot + cache reset
    /// (CS-022) — this bounds the memory a resumable subscription costs.
    pub resume_window_deltas: usize,
}

impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_subscriptions_per_connection: 1_000,
            max_compiled_plans: 100_000,
            resume_window_deltas: 256,
        }
    }
}

/// One unique query bucket (SUB-040): the shared plan, its subscriber set,
/// and the viewer identity the RLS filter is bound to.
///
/// For a caller-parameterized query (an `owner_only` table, SUB-030) the
/// bucket is per-identity — the dedup key folds the identity in — so
/// `viewer` is `Some(id)` and the plan's `rls` filter runs with it. For a
/// public query, or a server-peer bypass (SUB-031), `viewer` is `None` and
/// no row-level filter is applied; the shared encoding is then correct for
/// every subscriber in the bucket because they all see the same rows.
struct QueryState {
    plan: Arc<CompiledPlan>,
    subscribers: HashSet<u128>,
    viewer: Option<Identity>,
    /// The bucket viewer's RBAC roles (CT-040 grants) — folded into the
    /// effective hash, so differently-privileged viewers never share a
    /// bucket or its encodes.
    roles: Arc<[String]>,
}

/// One query's bounded retained-delta window (SPEC-021 CS-021): the last
/// `resume_window_deltas` committed updates, each tagged with the offset it
/// committed at, so a reconnecting client can replay from where it left off
/// instead of re-downloading the snapshot.
#[derive(Debug, Default)]
struct DeltaWindow {
    /// `(offset, shared update)`, ascending by offset.
    entries: std::collections::VecDeque<(u64, Arc<TableUpdate>)>,
    /// The highest offset evicted from `entries` (0 = nothing evicted yet).
    ///
    /// A resume is serviceable iff `from_offset >= trimmed_before`: the
    /// client has already applied everything up to `from_offset`, so it only
    /// needs offsets *after* it — and every such offset is still retained
    /// exactly when `from_offset` is at or past the last eviction (CS-022).
    trimmed_before: u64,
}

impl DeltaWindow {
    /// Retain one committed delta, evicting the oldest past `cap`.
    fn push(&mut self, offset: u64, update: Arc<TableUpdate>, cap: usize) {
        self.entries.push_back((offset, update));
        while self.entries.len() > cap.max(1) {
            if let Some((evicted, _)) = self.entries.pop_front() {
                self.trimmed_before = evicted;
            }
        }
    }

    /// Whether every delta after `from_offset` is still retained.
    fn can_resume(&self, from_offset: u64) -> bool {
        from_offset >= self.trimmed_before
    }

    /// The retained updates committed strictly after `from_offset`.
    fn since(&self, from_offset: u64) -> Vec<(u64, Arc<TableUpdate>)> {
        self.entries
            .iter()
            .filter(|(offset, _)| *offset > from_offset)
            .cloned()
            .collect()
    }
}

/// The server's answer to a [`Resume`](fluxum_protocol::Resume) (SPEC-021
/// CS-021/CS-022).
#[derive(Debug)]
pub enum Resumed {
    /// The offset was inside the retained window: replay only these deltas
    /// (ascending by offset), then continue with live `TxUpdate`s. Empty
    /// means the client was already up to date.
    Deltas(Vec<(u64, Arc<TableUpdate>)>),
    /// The offset predated the retained window (CS-022): the client must
    /// clear its cache for this query and replay from this snapshot, whose
    /// `cache_reset` flag is set.
    Reset(Box<InitialData>),
}

/// One shared, pre-encoded per-query delta produced by the fan-out
/// (SUB-024): the encoding is done once; each subscriber is a refcount bump
/// of the `Arc`-shared [`TableUpdate`].
#[derive(Debug, Clone)]
pub struct QueryDelta {
    /// The query this delta belongs to.
    pub query_hash: QueryHash,
    /// The shared, once-encoded table update (inserts + deletes).
    pub update: Arc<TableUpdate>,
    /// The fan-out targets, each with the `query_id` THAT connection knows
    /// this query by (SUB-001 — ids are assigned per connection). The
    /// fan-out stamps it on the delivered `TxUpdate` so an SDK can attribute
    /// rows to the subscription that produced them (SDK-044 bookkeeping,
    /// unsubscribe refcounts). `0` for view deltas, which are name-addressed
    /// (RV-011), not query-id-addressed.
    pub subscribers: Vec<(u128, u32)>,
}

impl QueryDelta {
    /// The target connection ids without their per-connection query ids —
    /// what non-transport consumers (and most assertions) care about.
    pub fn connections(&self) -> Vec<u128> {
        self.subscribers.iter().map(|&(conn, _)| conn).collect()
    }
}

/// The result of registering a subscription: the assigned `query_id` and the
/// `InitialData` snapshot the caller sends before any `TxUpdate` (SUB-002).
#[derive(Debug)]
pub struct Subscribed {
    /// Server-assigned per-connection query handle (SUB-001).
    pub query_id: u32,
    /// The initial snapshot (ordered/limited per SUB-013).
    pub initial: InitialData,
}

/// Per-shard subscription registry and fan-out engine (SUB-040).
///
/// Not internally synchronized: the server assembly wraps it in the
/// SUB-041 `tokio::sync::Mutex` and holds it only across evaluation, never
/// across network I/O. `ConnectionId` is carried as its raw `u128`.
pub struct SubscriptionManager {
    schema: Arc<Schema>,
    limits: SubscriptionLimits,
    /// The module's declared schema version (MIG-001), stamped on every
    /// `InitialData` so a generated SDK can check its embedded version
    /// against the wire (SDK-043) — the SAME number `/schema` publishes
    /// (SDK-002); the two diverging would make every client see a mismatch.
    schema_version: u32,
    /// One entry per unique query (SUB-020).
    queries: HashMap<QueryHash, QueryState>,
    /// SPEC-021 CS-020: the highest offset committed on this shard — the
    /// cursor stamped on `InitialData`/`TxUpdate` and echoed by `Resume`.
    /// Interior-mutable so `on_commit(&self)` can advance it.
    last_offset: std::sync::atomic::AtomicU64,
    /// SPEC-021 CS-021: per-query retained delta windows, maintained by
    /// `on_commit(&self)` — hence the mutex (the `members` pattern).
    windows: std::sync::Mutex<HashMap<QueryHash, DeltaWindow>>,
    /// Per-connection: `query_id` → the query it handles (Unsubscribe/cleanup).
    connections: HashMap<u128, HashMap<u32, QueryHash>>,
    /// Value-level pruning index (SUB-023): `(table, column, encoded value)`
    /// → plans. The value key is FluxBIN bytes (see [`ValueKey`]).
    search_args: HashMap<(TableId, u16, ValueKey), HashSet<QueryHash>>,
    /// FTS-042 term→plans pruning: a MATCH plan registers its query terms
    /// (a phrase registers its first term) so only plans whose terms appear
    /// in a delta row's analyzed text are evaluated.
    fts_terms: HashMap<(TableId, String), HashSet<QueryHash>>,
    /// FTS-031 prefix registrations, scanned linearly per delta term
    /// (prefixes are few; exact terms dominate).
    fts_prefixes: HashMap<TableId, Vec<(String, QueryHash)>>,
    /// The analyzers of every `#[fulltext]` column, per table — used to
    /// analyze delta rows during candidate selection.
    fts_analyzers: HashMap<TableId, Vec<(u16, crate::index::Analyzer)>>,
    /// The validated plugin registry (SPEC-020), once installed: the
    /// ReadPath query hooks (PLG-040/041) resolve rerank/retrieve/fusion
    /// bindings through it. `None` = hooks absent, pure BM25.
    plugins: Option<Arc<crate::plugin::PluginRegistry>>,
    /// SPEC-017 §6: per-table column policies (grants + masks), resolved at
    /// construction. Empty for schemas without non-public grants.
    column_policies: HashMap<TableId, crate::transform::mask::TablePolicy>,
    /// The transform engine (SPEC-017 §5), once installed: read surfaces
    /// decrypt `#[encrypted]` columns for authorized callers (CT-031)
    /// before masking projects the rest.
    transforms: Option<Arc<crate::transform::engine::TransformEngine>>,
    /// The materialized-view engine (SPEC-022 RV-010..013) — empty until
    /// [`SubscriptionManager::init_views`] resolves and rebuilds it.
    matviews: matview::MatViewEngine,
    /// RV-040 `member_of` rules resolved against the schema, one per
    /// protected table.
    membership_specs: Vec<MembershipSpec>,
    /// RV-041 membership index, parallel to `membership_specs`: the
    /// `(member identity, encoded key)` pairs currently in each membership
    /// table — an O(1) probe per row instead of a scan. Interior-mutable so
    /// `on_commit(&self)` can maintain it.
    members: std::sync::Mutex<Vec<MemberSet>>,
    /// View subscribers, per view name (RV-011).
    view_subs: HashMap<String, HashSet<u128>>,
    /// The views each connection subscribes to (disconnect cleanup).
    conn_views: HashMap<u128, HashSet<String>>,
    /// Which columns of a table carry a search arg, with a refcount so a
    /// column is probed only while some plan indexes it (drives per-delta
    /// probing without scanning all search args).
    indexed_columns: HashMap<TableId, HashMap<u16, usize>>,
    /// Fallback tier (SUB-040): plans on a table with no usable search arg.
    table_watchers: HashMap<TableId, HashSet<QueryHash>>,
    /// Next per-connection query id (monotonic per connection).
    next_query_id: HashMap<u128, u32>,
    /// SEC-045 query execution bounds, shared with the server's hot-reload
    /// path. Defaults to fully unbounded (today's behavior).
    bounds: Arc<QueryBounds>,
    /// The shard metrics registry, once installed — the SEC-045 abort
    /// counters land here. `None` (embedders, tests) skips counting.
    metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl SubscriptionManager {
    /// Build an empty manager over an assembled schema.
    pub fn new(schema: Arc<Schema>, limits: SubscriptionLimits) -> Self {
        let schema_for_policies = Arc::clone(&schema);
        // FTS-042: pre-resolve the analyzer of every #[fulltext] column so
        // candidate selection can analyze delta rows without schema walks.
        let mut fts_analyzers: HashMap<TableId, Vec<(u16, crate::index::Analyzer)>> =
            HashMap::new();
        for table in schema.tables() {
            for index in table.indexes {
                if let crate::schema::IndexSchema::FullText {
                    column,
                    language,
                    stop_words,
                    stemming,
                } = index
                {
                    fts_analyzers
                        .entry(TableId::of(table.name))
                        .or_default()
                        .push((
                            *column,
                            crate::index::Analyzer {
                                language: match language {
                                    crate::schema::FullTextLanguage::Simple => {
                                        crate::index::Language::Simple
                                    }
                                    crate::schema::FullTextLanguage::English => {
                                        crate::index::Language::English
                                    }
                                },
                                stop_words: *stop_words,
                                stemming: *stemming,
                            },
                        ));
                }
            }
        }
        // RV-040: resolve every `member_of` rule (validated at schema
        // assembly, so the lookups here cannot fail on a legal schema).
        let mut membership_specs = Vec::new();
        for table in schema.tables() {
            let crate::schema::VisibilityRule::MemberOf { table: member, key } = table.visibility
            else {
                continue;
            };
            let Some(member_schema) = schema.table(member) else {
                continue;
            };
            let ordinal_of = |t: &'static crate::schema::TableSchema, name: &str| {
                t.columns
                    .iter()
                    .position(|c| c.name == name)
                    .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
            };
            let identity = member_schema
                .columns
                .iter()
                .position(|c| matches!(c.ty, crate::schema::FluxType::Identity))
                .map(|i| u16::try_from(i).unwrap_or(u16::MAX));
            if let (Some(key_in_protected), Some(key_in_member), Some(identity_in_member)) = (
                ordinal_of(table, key),
                ordinal_of(member_schema, key),
                identity,
            ) {
                membership_specs.push(MembershipSpec {
                    protected: TableId::of(table.name),
                    member_table: TableId::of(member),
                    key_in_protected,
                    key_in_member,
                    identity_in_member,
                });
            }
        }
        let member_sets = vec![HashSet::new(); membership_specs.len()];
        Self {
            schema,
            limits,
            // The same fallback as `/schema` uses: a module that never
            // declared `#[fluxum::schema_version]` is version 1.
            schema_version: crate::migration::declared_schema_version().unwrap_or(1),
            queries: HashMap::new(),
            last_offset: std::sync::atomic::AtomicU64::new(0),
            windows: std::sync::Mutex::new(HashMap::new()),
            connections: HashMap::new(),
            search_args: HashMap::new(),
            fts_terms: HashMap::new(),
            fts_prefixes: HashMap::new(),
            fts_analyzers,
            plugins: None,
            column_policies: crate::transform::mask::resolve_policies(&schema_for_policies),
            transforms: None,
            matviews: matview::MatViewEngine::default(),
            membership_specs,
            members: std::sync::Mutex::new(member_sets),
            view_subs: HashMap::new(),
            conn_views: HashMap::new(),
            indexed_columns: HashMap::new(),
            table_watchers: HashMap::new(),
            next_query_id: HashMap::new(),
            bounds: Arc::new(QueryBounds::default()),
            metrics: None,
        }
    }

    /// Install the shared SEC-045 query bounds (assembly; the same `Arc` is
    /// retuned by the server's OPS-040 hot-reload path).
    pub fn set_query_bounds(&mut self, bounds: Arc<QueryBounds>) {
        self.bounds = bounds;
    }

    /// Install the shard metrics registry so SEC-045 query aborts are
    /// counted (`fluxum_query_aborted_total`). Called at assembly.
    pub fn set_metrics(&mut self, metrics: Arc<crate::metrics::Metrics>) {
        self.metrics = Some(metrics);
    }

    /// The effective `LIMIT` for a plan under the SEC-045 bounds: a query
    /// without one gets `default_limit` (if configured); one above
    /// `max_limit` is clamped — or rejected with a wire-ready 3030 in
    /// `reject` mode.
    fn effective_limit(&self, plan_limit: Option<u32>) -> Result<Option<u32>> {
        let default_limit = self.bounds.default_limit();
        let max_limit = self.bounds.max_limit();
        let limit = plan_limit.or((default_limit > 0).then_some(default_limit));
        match limit {
            Some(n) if max_limit > 0 && n > max_limit => {
                if self.bounds.reject_over_max() && plan_limit.is_some() {
                    if let Some(metrics) = &self.metrics {
                        metrics.note_query_aborted(crate::metrics::QueryAbortReason::Limit);
                    }
                    return Err(FluxumError::query(
                        codes::SQL_LIMIT_REJECTED,
                        format!("LIMIT {n} exceeds the configured maximum {max_limit} (SEC-045)"),
                    ));
                }
                Ok(Some(max_limit))
            }
            other => Ok(other),
        }
    }

    /// One membership entry of `spec` from a membership-table row.
    fn membership_entry(spec: &MembershipSpec, row: &Row) -> Option<([u8; 32], Vec<u8>)> {
        let identity = match row.value(spec.identity_in_member) {
            Some(crate::store::RowValue::Identity(id)) => *id.as_bytes(),
            _ => return None,
        };
        let key_value = row.value(spec.key_in_member)?;
        let mut key = Vec::new();
        crate::index::btree::encode_value(key_value, &mut key);
        Some((identity, key))
    }

    /// RV-040/041: whether `row` of `plan`'s table is visible to `viewer` —
    /// the `rls` closure (owner_only) AND the membership index (member_of).
    /// `viewer = None` (public plan or server-peer bypass) sees everything.
    fn row_visible(&self, plan: &CompiledPlan, row: &Row, viewer: Option<&Identity>) -> bool {
        if !visible(plan, row, viewer) {
            return false;
        }
        let Some(viewer) = viewer else {
            return true;
        };
        let table_id = plan.table_ids[0];
        let Some(index) = self
            .membership_specs
            .iter()
            .position(|spec| spec.protected == table_id)
        else {
            return true;
        };
        let spec = &self.membership_specs[index];
        let Some(key_value) = row.value(spec.key_in_protected) else {
            return false;
        };
        let mut key = Vec::new();
        crate::index::btree::encode_value(key_value, &mut key);
        let members = self
            .members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        members[index].contains(&(*viewer.as_bytes(), key))
    }

    /// Resolve the registered `#[fluxum::view(materialized)]` declarations
    /// and rebuild their state from `snapshot` (SPEC-022 RV-010/013 — the
    /// startup/recovery path). Call at assembly, before serving; without
    /// it, materialized views are inactive.
    pub fn init_views(&mut self, snapshot: &Snapshot) -> Result<()> {
        self.matviews = matview::MatViewEngine::init(&self.schema, snapshot)?;
        // RV-041: rebuild the membership index from the membership tables
        // (the same startup/recovery contract as view state).
        let mut sets = Vec::with_capacity(self.membership_specs.len());
        for spec in &self.membership_specs {
            let mut set = HashSet::new();
            for row in snapshot.scan(spec.member_table)? {
                if let Some(entry) = Self::membership_entry(spec, row) {
                    set.insert(entry);
                }
            }
            sets.push(set);
        }
        *self
            .members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sets;
        Ok(())
    }

    /// RV-013 validation seam: assert every view's incremental state equals
    /// a bit-identical fresh rebuild from `snapshot`.
    pub fn validate_views(&self, snapshot: &Snapshot) -> Result<()> {
        self.matviews.validate_against(snapshot)
    }

    /// Subscribe `connection` to a materialized view (RV-011): returns the
    /// current view rows as `InitialData`; subsequent changes arrive as
    /// `TxUpdate`s through the ordinary fan-out.
    pub fn subscribe_view(&mut self, connection: u128, name: &str) -> Result<InitialData> {
        let Some(update) = self.matviews.snapshot_rows(name)? else {
            return Err(FluxumError::query(
                codes::REDUCER_UNKNOWN_VIEW,
                format!("unknown materialized view `{name}` (RV-011)"),
            ));
        };
        self.view_subs
            .entry(name.to_owned())
            .or_default()
            .insert(connection);
        self.conn_views
            .entry(connection)
            .or_default()
            .insert(name.to_owned());
        Ok(InitialData {
            id: 0,
            schema_version: self.schema_version,
            tx_offset: self.current_offset(),
            cache_reset: false,
            tables: vec![update],
        })
    }

    /// Drop `connection`'s subscription to view `name`. Returns whether it
    /// existed.
    pub fn unsubscribe_view(&mut self, connection: u128, name: &str) -> bool {
        let existed = self
            .view_subs
            .get_mut(name)
            .is_some_and(|subs| subs.remove(&connection));
        if let Some(views) = self.conn_views.get_mut(&connection) {
            views.remove(name);
        }
        existed
    }

    /// Install the validated plugin registry (SPEC-020 PLG-040/041): binds
    /// `score_reranker` / `retriever` / `fusion` plugins into the MATCH
    /// snapshot path. Called at assembly, before serving.
    pub fn set_plugins(&mut self, plugins: Arc<crate::plugin::PluginRegistry>) {
        self.plugins = Some(plugins);
    }

    /// Install the transform engine (SPEC-017 §5/§6): read surfaces decrypt
    /// `#[encrypted]` columns (CT-031) before grant-driven masking projects
    /// unauthorized ones (CT-040/041). Called at assembly, with the same
    /// engine attached to the store.
    pub fn set_transforms(&mut self, engine: Arc<crate::transform::engine::TransformEngine>) {
        self.transforms = Some(engine);
    }

    /// SPEC-017 §6: decrypt-then-mask one row for a viewer. `viewer = None`
    /// (public plan or server-peer bucket) reads raw — still decrypted when
    /// an engine is installed (a public grant means everyone is authorized,
    /// CT-040 default). The masked substitute for an unauthorized column is
    /// computed from the ORIGINAL stored value (the `ciphertext` strategy
    /// exposes the sealed envelope) and the decrypted value (`hash`), never
    /// leaking plaintext (CT-012/041).
    fn project_row(
        &self,
        table_id: TableId,
        schema: &TableSchema,
        row: &Row,
        viewer: Option<&Identity>,
        roles: &[String],
    ) -> Result<Row> {
        let policy = self.column_policies.get(&table_id);
        let engine = self
            .transforms
            .as_ref()
            .filter(|engine| engine.touches(table_id));
        if policy.is_none() && engine.is_none() {
            return Ok(row.clone());
        }
        let mut values = row.values().to_vec();
        if let Some(engine) = engine {
            let pk = encode_pk_of_row(schema, row.values())?;
            engine.on_read_row(table_id, &mut values, pk.as_bytes(), true)?;
        }
        if let (Some(policy), Some(viewer)) = (policy, viewer) {
            for column in &policy.columns {
                if !crate::transform::mask::authorized(column, policy.owner, viewer, roles, row) {
                    let idx = usize::from(column.ordinal);
                    let original = &row.values()[idx];
                    values[idx] =
                        crate::transform::mask::mask_value(column, original, &values[idx]);
                }
            }
        }
        Ok(Row::new(values))
    }

    /// The assembled schema (for HTTP admin `/schema` introspection).
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Number of unique compiled plans currently registered (SUB-044 cap
    /// surface; also the dedup witness).
    pub fn plan_count(&self) -> usize {
        self.queries.len()
    }

    /// Number of live subscriptions on `connection`.
    pub fn subscription_count(&self, connection: u128) -> usize {
        self.connections.get(&connection).map_or(0, HashMap::len)
    }

    /// Register one subscription `sql` for `connection` on behalf of
    /// `subscriber` (SUB-001/002/020/030/044): compile, enforce the
    /// public-table and admission policies, dedup (or reuse) the plan under
    /// its **effective** [`QueryHash`], assign a `query_id`, and return the
    /// `InitialData` snapshot filtered for this subscriber. A compile error
    /// or a subscription to a non-public table is a wire-ready 400/403; a cap
    /// breach is a 429 — none of them register anything.
    ///
    /// For an `owner_only` table the effective hash folds in the caller
    /// identity (or a shared server-peer tag for the RLS bypass), so
    /// different viewers get distinct buckets while identical viewers still
    /// share one plan and one encoding (SUB-020/031).
    pub fn subscribe(
        &mut self,
        connection: u128,
        subscriber: Subscriber,
        sql: &str,
        snapshot: &Snapshot,
    ) -> Result<Subscribed> {
        let plan = compile(&self.schema, sql)?;
        let table_id = plan.table_ids[0];

        // A client may only subscribe to a `public` table (SPEC-001
        // acceptance 9): private/global tables never appear in client
        // messages. Server peers are still bound to this — private tables
        // are server-internal, reached through reducers, not subscriptions.
        let schema = self.table_schema(table_id)?;
        if !schema.access.is_client_visible() {
            return Err(FluxumError::query(
                codes::SUB_TABLE_NOT_PUBLIC,
                format!(
                    "table `{}` is not public and cannot be subscribed",
                    schema.name
                ),
            ));
        }

        // Caller-parameterization (SUB-030/031): an `owner_only` plan is
        // per-viewer unless the caller is a server peer (bypass).
        let (hash, viewer, roles) = self.effective_key(&plan, &subscriber);

        // Admission (SUB-044): reject before any mutation. A brand-new query
        // bucket adds a plan; re-subscribing to an existing one does not.
        let live = self.subscription_count(connection);
        if live >= self.limits.max_subscriptions_per_connection {
            return Err(limit_exceeded("max_subscriptions_per_connection"));
        }
        let new_plan = !self.queries.contains_key(&hash);
        if new_plan && self.queries.len() >= self.limits.max_compiled_plans {
            return Err(limit_exceeded("max_compiled_plans"));
        }

        let (initial, _) = self.initial_data(&plan, viewer.as_ref(), &roles, snapshot)?;

        // Register: shared plan + pruning-index membership on first sighting.
        if let Some(state) = self.queries.get_mut(&hash) {
            state.subscribers.insert(connection);
        } else {
            let plan = Arc::new(plan);
            self.index_plan(hash, &plan);
            let mut subscribers = HashSet::new();
            subscribers.insert(connection);
            self.queries.insert(
                hash,
                QueryState {
                    plan,
                    subscribers,
                    viewer,
                    roles,
                },
            );
        }

        let query_id = self.assign_query_id(connection);
        self.connections
            .entry(connection)
            .or_default()
            .insert(query_id, hash);

        let mut initial = initial;
        for table in &mut initial.tables {
            table.query_id = query_id;
        }
        Ok(Subscribed { query_id, initial })
    }

    /// The effective dedup key and viewer for a subscription (SUB-020/030/
    /// 031). A public (non-caller-parameterized) plan keeps its plaintext
    /// hash and no viewer; an `owner_only` plan folds the caller identity
    /// (client) or a shared server-peer tag (bypass) into the hash.
    fn effective_key(
        &self,
        plan: &CompiledPlan,
        subscriber: &Subscriber,
    ) -> (QueryHash, Option<Identity>, Arc<[String]>) {
        if !plan.caller_scoped {
            return (plan.query_hash, None, Arc::from([]));
        }
        if subscriber.is_server_peer {
            // Server peers bypass RLS and grants, sharing one bucket that
            // sees every matching row raw (SUB-031/AUTH-062).
            let hash = QueryHash(
                crate::simd::global().hash64(b"__fluxum_server_peer__", plan.query_hash.0),
            );
            (hash, None, Arc::from([]))
        } else {
            // CT-040: roles change the projection, so they fold into the
            // bucket key — differently-privileged viewers never share an
            // encode.
            let mut hash =
                crate::simd::global().hash64(subscriber.identity.as_bytes(), plan.query_hash.0);
            for role in subscriber.roles.iter() {
                hash = crate::simd::global().hash64(role.as_bytes(), hash);
            }
            (
                QueryHash(hash),
                Some(subscriber.identity),
                Arc::clone(&subscriber.roles),
            )
        }
    }

    /// Resume subscription `query_id` on `connection` from `from_offset`
    /// (SPEC-021 CS-021/CS-022).
    ///
    /// Returns [`Resumed::Deltas`] — only the committed updates after
    /// `from_offset`, ascending — when the offset is still inside the
    /// query's retained window; the caller ships them as `TxUpdate`s and
    /// live updates continue normally. When the offset predates the window
    /// (its deltas were evicted, CS-022) the answer is [`Resumed::Reset`]: a
    /// full snapshot with `cache_reset` set, which the client applies after
    /// clearing its cache.
    ///
    /// `query_id` is resolved against `connection`'s registered
    /// subscriptions, so this only serves a session that outlived the
    /// transport blip; an unknown `query_id` is `None` and the client must
    /// `Subscribe` afresh.
    pub fn resume(
        &self,
        connection: u128,
        query_id: u32,
        from_offset: u64,
        snapshot: &Snapshot,
    ) -> Result<Option<Resumed>> {
        let Some(hash) = self
            .connections
            .get(&connection)
            .and_then(|handles| handles.get(&query_id))
            .copied()
        else {
            return Ok(None);
        };
        let Some(state) = self.queries.get(&hash) else {
            return Ok(None);
        };

        let resumable = {
            let windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
            match windows.get(&hash) {
                // CS-022: the client's offset fell out of the window.
                Some(window) if !window.can_resume(from_offset) => None,
                Some(window) => Some(window.since(from_offset)),
                // Nothing retained yet: the query has produced no delta since
                // it was registered, so there is nothing to replay.
                None => Some(Vec::new()),
            }
        };

        match resumable {
            Some(deltas) => Ok(Some(Resumed::Deltas(deltas))),
            None => {
                // Rebuild the snapshot for this bucket's viewer and mark it a
                // cache reset so the SDK clears before applying (CS-022).
                let (mut initial, _) =
                    self.initial_data(&state.plan, state.viewer.as_ref(), &state.roles, snapshot)?;
                initial.cache_reset = true;
                initial.tx_offset = self.current_offset();
                for table in &mut initial.tables {
                    table.query_id = query_id;
                }
                Ok(Some(Resumed::Reset(Box::new(initial))))
            }
        }
    }

    /// Drop the subscription `query_id` on `connection` (SUB-004). Returns
    /// whether it existed. Removing the last subscriber of a query evicts
    /// the shared plan and its pruning-index entries.
    pub fn unsubscribe(&mut self, connection: u128, query_id: u32) -> bool {
        let Some(handles) = self.connections.get_mut(&connection) else {
            return false;
        };
        let Some(hash) = handles.remove(&query_id) else {
            return false;
        };
        if handles.is_empty() {
            self.connections.remove(&connection);
            self.next_query_id.remove(&connection);
        }
        self.drop_subscriber(hash, connection);
        true
    }

    /// Drop every subscription of `connection` (SUB-005 disconnect cleanup).
    pub fn disconnect(&mut self, connection: u128) {
        // Materialized-view subscriptions die with the connection (RV-011).
        if let Some(views) = self.conn_views.remove(&connection) {
            for name in views {
                if let Some(subs) = self.view_subs.get_mut(&name) {
                    subs.remove(&connection);
                    if subs.is_empty() {
                        self.view_subs.remove(&name);
                    }
                }
            }
        }
        let Some(handles) = self.connections.remove(&connection) else {
            return;
        };
        self.next_query_id.remove(&connection);
        for hash in handles.into_values() {
            self.drop_subscriber(hash, connection);
        }
    }

    /// The current server-side result of one query for `subscriber`, without
    /// registering a subscription (a one-off read, SUB-025): the same
    /// filtered, RLS-applied `InitialData` a fresh `Subscribe` would return
    /// against `snapshot`. The subscription-correctness property suite uses
    /// this as the ground truth its diff-maintained client caches must
    /// match after every commit.
    pub fn snapshot_result(
        &self,
        subscriber: Subscriber,
        sql: &str,
        snapshot: &Snapshot,
    ) -> Result<InitialData> {
        let plan = compile(&self.schema, sql)?;
        let schema = self.table_schema(plan.table_ids[0])?;
        if !schema.access.is_client_visible() {
            return Err(FluxumError::query(
                codes::SUB_TABLE_NOT_PUBLIC,
                format!(
                    "table `{}` is not public and cannot be subscribed",
                    schema.name
                ),
            ));
        }
        let (_, viewer, roles) = self.effective_key(&plan, &subscriber);
        Ok(self
            .initial_data(&plan, viewer.as_ref(), &roles, snapshot)?
            .0)
    }

    /// Run a one-off read (SUB-025) and return the rows as JSON — the shape
    /// the HTTP admin `POST /query` returns (RPC-050): `{ "table": name,
    /// "columns": [...], "rows": [ { col: value, ... }, ... ] }`. RLS and the
    /// public-table gate apply exactly as for [`Self::snapshot_result`].
    pub fn query_json(
        &self,
        subscriber: Subscriber,
        sql: &str,
        snapshot: &Snapshot,
    ) -> Result<serde_json::Value> {
        let plan = compile(&self.schema, sql)?;
        let table = self.table_schema(plan.table_ids[0])?;
        if !table.access.is_client_visible() {
            return Err(FluxumError::query(
                codes::SUB_TABLE_NOT_PUBLIC,
                format!("table `{}` is not public", table.name),
            ));
        }
        let (_, viewer, roles) = self.effective_key(&plan, &subscriber);
        let (initial, scores) = self.initial_data(&plan, viewer.as_ref(), &roles, snapshot)?;
        let mut columns: Vec<&str> = table.columns.iter().map(|c| c.name).collect();
        // FTS-041: the opt-in `_score` projection on the JSON read surface.
        let with_score = plan.select_score && scores.len() == initial.tables[0].inserts.len();
        if with_score {
            columns.push("_score");
        }
        let mut rows = Vec::new();
        let table_id = plan.table_ids[0];
        // CT-034: the `<field>_verified` projection siblings — re-verify
        // the STORED (sealed) row for every `#[signed]` column.
        let verify_engine = self
            .transforms
            .as_ref()
            .filter(|engine| engine.touches(table_id));
        for (index, bytes) in initial.tables[0].inserts.iter().enumerate() {
            let row = crate::store::row::decode_row(table, bytes)?;
            let mut object = serde_json::Map::new();
            for (column, value) in table.columns.iter().zip(row.values()) {
                object.insert(column.name.to_owned(), row_value_to_json(value));
            }
            if let Some(engine) = verify_engine {
                // PK columns are never transformed (CT-013), so the decoded
                // row's key resolves the original stored row.
                let pk_values: Vec<crate::store::RowValue> = table
                    .primary_key
                    .iter()
                    .map(|&ord| row.values()[usize::from(ord)].clone())
                    .collect();
                if let Some(stored) = snapshot.query_pk(table_id, &pk_values)? {
                    let pk = encode_pk_of_row(table, stored.values())?;
                    for (ordinal, verified) in
                        engine.verify_outcomes(table_id, stored.values(), pk.as_bytes())
                    {
                        let name = table.columns[usize::from(ordinal)].name;
                        object.insert(
                            format!("{name}_verified"),
                            serde_json::Value::Bool(verified),
                        );
                    }
                }
            }
            if with_score {
                object.insert("_score".to_owned(), serde_json::json!(scores[index]));
            }
            rows.push(serde_json::Value::Object(object));
        }
        Ok(serde_json::json!({
            "table": table.name,
            "columns": columns,
            "rows": rows,
        }))
    }

    /// The highest offset committed on this shard (SPEC-021 CS-020) — the
    /// cursor stamped on every `InitialData`/`TxUpdate` and echoed back by
    /// [`Resume`](fluxum_protocol::Resume).
    pub fn current_offset(&self) -> u64 {
        self.last_offset.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Evaluate a commit against the candidate plans and produce one shared,
    /// once-encoded [`QueryDelta`] per matched query (SUB-021..024).
    ///
    /// Only plans selected by the pruning indexes for this commit's delta
    /// rows are evaluated; a query whose matched inserts and deletes are both
    /// empty produces nothing. Ordering: deltas come back sorted by
    /// `QueryHash` for deterministic tests.
    ///
    /// Each produced delta is also retained in its query's bounded resume
    /// window (SPEC-021 CS-021) and the shard's offset advances to this
    /// commit's `tx_id`.
    pub fn on_commit(&self, diff: &TxDiff) -> Result<Vec<QueryDelta>> {
        // CS-020: the offset advances with every commit, whether or not any
        // query matched — it is the shard's cursor, not a per-query counter.
        self.last_offset
            .fetch_max(diff.tx_id, std::sync::atomic::Ordering::Relaxed);
        let candidates = self.candidate_plans(diff);
        let mut deltas = Vec::new();
        for hash in candidates {
            let Some(state) = self.queries.get(&hash) else {
                continue;
            };
            if state.subscribers.is_empty() {
                continue;
            }
            let Some(update) =
                self.evaluate(&state.plan, state.viewer.as_ref(), &state.roles, diff)?
            else {
                continue;
            };
            let mut subscribers: Vec<(u128, u32)> = state
                .subscribers
                .iter()
                .map(|&connection| (connection, self.query_id_of(connection, hash)))
                .collect();
            subscribers.sort_unstable();
            let update = Arc::new(update);
            // CS-021: retain it for resumption, evicting past the bound.
            {
                let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
                windows.entry(hash).or_default().push(
                    diff.tx_id,
                    Arc::clone(&update),
                    self.limits.resume_window_deltas,
                );
            }
            deltas.push(QueryDelta {
                query_hash: hash,
                update,
                subscribers,
            });
        }
        // SPEC-022 RV-010/011: feed the materialized-view engine EVERY
        // commit (state correctness is independent of subscribers) and fan
        // out changed view rows to view subscribers, O(affected groups).
        for (name, update) in self.matviews.on_commit(diff)? {
            let Some(subs) = self.view_subs.get(&name) else {
                continue;
            };
            if subs.is_empty() {
                continue;
            }
            let mut subscribers: Vec<(u128, u32)> = subs.iter().map(|&c| (c, 0)).collect();
            subscribers.sort_unstable();
            deltas.push(QueryDelta {
                query_hash: QueryHash(crate::simd::global().hash64(name.as_bytes(), 0x4D56)),
                update: Arc::new(update),
                subscribers,
            });
        }
        deltas.sort_by_key(|d| d.query_hash);
        // RV-040: apply this commit's membership-table changes AFTER the
        // deltas were evaluated — a membership change flips visibility for
        // LATER commits (joining mid-commit never retro-filters this one).
        {
            let mut members = self
                .members
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (spec, set) in self.membership_specs.iter().zip(members.iter_mut()) {
                let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == spec.member_table)
                else {
                    continue;
                };
                for (_, old) in &table_diff.deletes {
                    if let Some(entry) = Self::membership_entry(spec, old) {
                        set.remove(&entry);
                    }
                }
                for row in &table_diff.inserts {
                    if let Some(entry) = Self::membership_entry(spec, row) {
                        set.insert(entry);
                    }
                }
            }
        }
        Ok(deltas)
    }

    /// Assemble a full [`TxUpdate`] envelope for one query's delta (the
    /// transport wraps this per subscriber; the `tables` bytes are shared).
    /// The reducer metadata (`timestamp`, `reducer_name`, `caller`,
    /// `duration_us`) is stamped by the transport from the commit context —
    /// the manager owns only the row effects.
    #[must_use]
    pub fn tx_update(diff: &TxDiff, delta: &QueryDelta) -> TxUpdate {
        TxUpdate {
            tx_id: diff.tx_id,
            timestamp: 0,
            reducer_name: String::new(),
            caller: [0u8; 32],
            duration_us: 0,
            shard_id: 0,
            // CS-020: the resume cursor. It mirrors `tx_id` today; clients
            // retain this field (not `tx_id`) and echo it in `Resume`.
            tx_offset: diff.tx_id,
            tables: vec![(*delta.update).clone()],
        }
    }

    // --- InitialData (SUB-002/013) ------------------------------------------

    /// Returns the encoded snapshot plus, for a MATCH plan, the BM25 scores
    /// parallel to the encoded rows (consumed by the `_score` projection on
    /// the JSON read surface; empty when not applicable).
    fn initial_data(
        &self,
        plan: &CompiledPlan,
        viewer: Option<&Identity>,
        roles: &[String],
        snapshot: &Snapshot,
    ) -> Result<(InitialData, Vec<f64>)> {
        let table_id = plan.table_ids[0];
        let schema = self.table_schema(table_id)?;
        // SEC-045: the effective LIMIT under the configured bounds (implicit
        // default, clamp-or-reject over the maximum) and the per-query scan
        // accounting the candidate paths below report into.
        let limit = self.effective_limit(plan.limit)?;
        let guard = ScanGuard::new(&self.bounds);

        // Candidate rows: spatial clauses go through the spatial index
        // (SUB-022); a `MATCH` goes through the inverted index (SPEC-019
        // FTS-030 — never a full scan); an `IndexScan` plan goes through its
        // bounded B-tree scans (SPEC-018 QP-010); otherwise a full committed
        // scan. Every path applies RLS for this viewer (SUB-030) — `viewer`
        // is `None` for a public query or a server-peer bypass.
        let keep = |row: &Row| plan.matches(row) && self.row_visible(plan, row, viewer);
        // BM25 scores parallel to `rows`, present only for a MATCH plan
        // (FTS-040/041); dropped unless SCORE ordering/projection needs them.
        let mut scores: Vec<f64> = Vec::new();
        let mut rows: Vec<Row> = match (&plan.fts, &plan.spatial, &plan.access) {
            (Some(fts), None, _) => {
                let mut rows = Vec::new();
                for (row, score) in snapshot.fulltext_match(table_id, fts)? {
                    if !guard.admit() {
                        break;
                    }
                    if keep(&row) {
                        rows.push(row);
                        scores.push(score);
                    }
                }
                rows
            }
            (None, Some(constraint), _) => self
                .spatial_candidates(snapshot, table_id, *constraint)?
                .into_iter()
                .filter(|row| guard.admit() && keep(row))
                .collect(),
            (None, None, AccessPath::IndexScan(scan)) => self.index_scan_rows(
                plan, scan, viewer, snapshot, table_id, schema, &guard, limit,
            )?,
            (_, _, AccessPath::FullScan) => {
                let mut out = Vec::new();
                for row in snapshot.scan(table_id)? {
                    if !guard.admit() {
                        break;
                    }
                    if keep(row) {
                        out.push(row.clone());
                    }
                }
                out
            }
            (Some(_), Some(_), _) => unreachable!("compile rejects MATCH + spatial"),
        };
        // SEC-045: an aborted candidate scan is a typed error, never a
        // silently truncated result.
        guard.finish(self.metrics.as_deref())?;

        // ORDER BY / LIMIT apply to InitialData ONLY (SUB-013). QP-020: an
        // index-served order skips the in-RAM sort. FTS-041: `ORDER BY
        // SCORE` sorts by BM25 (snapshot-only, like every ordering).
        if let Some(descending) = plan.order_by_score {
            QUERY_SORTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut paired: Vec<(Row, f64)> = rows.drain(..).zip(scores.drain(..)).collect();
            paired.sort_by(|a, b| {
                let ord = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
                if descending { ord.reverse() } else { ord }
            });
            // SPEC-020 PLG-040/041: with a score-ranked MATCH, apply the
            // bound ReadPath hooks — retriever+fusion, then reranker over
            // the top-K. Snapshot-only; every failure falls back to the
            // BM25 order (never an error to the caller).
            if let (Some(fts), Some(registry)) = (&plan.fts, &self.plugins) {
                paired = self.apply_read_hooks(
                    registry, fts, schema, paired, viewer, snapshot, table_id, plan,
                )?;
            }
            for (row, score) in paired {
                rows.push(row);
                scores.push(score);
            }
        } else if let Some(order) = plan.order_by
            && !plan.ordered_by_index
        {
            QUERY_SORTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // A column sort on a MATCH plan drops score pairing (scores are
            // only surfaced with SCORE ordering / projection intact).
            scores.clear();
            rows.sort_by(|a, b| {
                let ord = a
                    .value(order.column)
                    .zip(b.value(order.column))
                    .and_then(|(x, y)| crate::sql::cmp_row_values(x, y))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if order.descending { ord.reverse() } else { ord }
            });
        }
        if let Some(limit) = limit {
            rows.truncate(limit as usize);
            scores.truncate(limit as usize);
        }
        // SEC-045: the deadline also covers the sort/rank phase above.
        guard.finish(self.metrics.as_deref())?;

        // SPEC-017 §6: decrypt-then-mask before the wire (CT-031/040/041).
        let rows: Vec<Row> = rows
            .iter()
            .map(|row| self.project_row(table_id, schema, row, viewer, roles))
            .collect::<Result<_>>()?;

        let inserts = encode_full_rows(&rows)?;
        Ok((
            InitialData {
                id: 0,
                schema_version: self.schema_version,
                // CS-020: the snapshot's resume cursor.
                tx_offset: self.current_offset(),
                cache_reset: false,
                tables: vec![TableUpdate {
                    table_id: table_id.as_u32(),
                    table_name: schema.name.to_owned(),
                    query_id: 0,
                    inserts,
                    deletes: RowList::empty(),
                }],
            },
            scores,
        ))
    }

    /// SPEC-020 PLG-040/041: the ReadPath query hooks over a score-ranked
    /// MATCH result (best-first). Order of application: retriever + fusion
    /// (hybrid lexical+dense, PLG-041), then the reranker over the top-K
    /// (PLG-040). Every failure or panic degrades to the list as it stood —
    /// the caller never sees an error from a plugin (the isolation guard
    /// meters and, on panic, disables it). Hooks apply to descending score
    /// order only (best-first ranking is what they reorder).
    #[allow(clippy::too_many_arguments)]
    fn apply_read_hooks(
        &self,
        registry: &crate::plugin::PluginRegistry,
        fts: &crate::index::FtsQuery,
        schema: &TableSchema,
        paired: Vec<(Row, f64)>,
        viewer: Option<&Identity>,
        snapshot: &Snapshot,
        table_id: TableId,
        plan: &CompiledPlan,
    ) -> Result<Vec<(Row, f64)>> {
        use crate::plugin::{
            Capability, FtQuery, Fusion, PluginCtx, PluginInstance, RERANK_CANDIDATE_K,
            ReciprocalRankFusion, Scored,
        };
        if plan.order_by_score != Some(true) {
            return Ok(paired); // hooks reorder best-first rankings only
        }
        let column_name = schema.columns[usize::from(fts.column)].name;
        let query = FtQuery {
            table: schema.name.to_owned(),
            column: column_name.to_owned(),
            query: fts.raw.clone(),
            limit: plan.limit.map_or(0, |n| n as usize),
        };
        let ctx = PluginCtx {
            identity: viewer.copied().unwrap_or(Identity::from_bytes([0u8; 32])),
            is_server_peer: false,
            shard_id: 0,
        };
        let mut paired = paired;

        // PLG-041: hybrid retrieval — external top-K fused with the BM25
        // list (default Reciprocal Rank Fusion). A dense-only candidate is
        // admitted (that is the point of hybrid) but still passes the
        // ordinary filters and RLS; a retriever failure leaves BM25 intact.
        if let Some(binding) =
            registry.readpath_binding(Capability::Retriever, schema.name, column_name)
            && let Some(PluginInstance::Retriever(retriever)) = &binding.instance
            && let Ok(dense) = binding.state.guard(&binding.name, || {
                retriever.retrieve(&query, RERANK_CANDIDATE_K, &ctx)
            })
        {
            let mut by_pk: HashMap<Vec<u8>, Row> = HashMap::new();
            let mut lexical = Vec::with_capacity(paired.len());
            for (row, score) in &paired {
                let pk = encode_pk_of_row(schema, row.values())?;
                lexical.push(Scored {
                    pk: pk.clone(),
                    score: *score,
                });
                by_pk.insert(pk.as_bytes().to_vec(), row.clone());
            }
            let default_fusion = ReciprocalRankFusion::default();
            let fused = if let Some(fusion_binding) =
                registry.readpath_binding(Capability::Fusion, schema.name, column_name)
                && let Some(PluginInstance::Fusion(fusion)) = &fusion_binding.instance
            {
                fusion_binding
                    .state
                    .guard(&fusion_binding.name, || {
                        Ok(fusion.fuse(&lexical, &dense, &ctx))
                    })
                    .unwrap_or_else(|_| default_fusion.fuse(&lexical, &dense, &ctx))
            } else {
                default_fusion.fuse(&lexical, &dense, &ctx)
            };
            let keep = |row: &Row| plan.matches(row) && self.row_visible(plan, row, viewer);
            let mut out = Vec::with_capacity(fused.len());
            for scored in fused {
                if let Some(row) = by_pk.remove(scored.pk.as_bytes()) {
                    out.push((row, scored.score));
                } else if let Some(row) = snapshot.row_by_encoded_pk(table_id, &scored.pk)?
                    && keep(&row)
                {
                    out.push((row, scored.score));
                }
            }
            paired = out;
        }

        // PLG-040: rerank the top-K candidates; the reranker's order is
        // authoritative for those K, the tail keeps the base order. Rows
        // outside the handed candidates are dropped defensively — a
        // reranker reorders, it never injects.
        if let Some(binding) =
            registry.readpath_binding(Capability::ScoreReranker, schema.name, column_name)
            && let Some(PluginInstance::ScoreReranker(reranker)) = &binding.instance
        {
            let k = RERANK_CANDIDATE_K.min(paired.len());
            let mut by_pk: HashMap<Vec<u8>, Row> = HashMap::new();
            let mut candidates = Vec::with_capacity(k);
            for (row, score) in paired.iter().take(k) {
                let pk = encode_pk_of_row(schema, row.values())?;
                candidates.push(Scored {
                    pk: pk.clone(),
                    score: *score,
                });
                by_pk.insert(pk.as_bytes().to_vec(), row.clone());
            }
            if let Ok(reordered) = binding
                .state
                .guard(&binding.name, || reranker.rerank(&query, candidates, &ctx))
            {
                let tail = paired.split_off(k);
                let mut out = Vec::with_capacity(reordered.len() + tail.len());
                for scored in reordered {
                    if let Some(row) = by_pk.remove(scored.pk.as_bytes()) {
                        out.push((row, scored.score));
                    }
                }
                out.extend(tail);
                paired = out;
            }
        }
        Ok(paired)
    }

    // --- Commit evaluation (SUB-021) ----------------------------------------

    /// Matched inserts + deletes for one plan against a commit, encoded once
    /// (SUB-024). `None` when nothing matched. `viewer` applies the RLS
    /// filter (SUB-030) to both inserts and deletes — a delete of a row the
    /// viewer could never see is correctly not delivered.
    fn evaluate(
        &self,
        plan: &CompiledPlan,
        viewer: Option<&Identity>,
        roles: &[String],
        diff: &TxDiff,
    ) -> Result<Option<TableUpdate>> {
        let table_id = plan.table_ids[0];
        let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == table_id) else {
            return Ok(None); // fast path: this plan's table did not change
        };

        // FTS-042: live diffs test the boolean MATCH by re-analyzing the
        // delta row — no re-ranking. RV-040: relational visibility applies
        // to diffs exactly as to initial data.
        let keep = |row: &&Row| {
            plan.matches(row)
                && plan.fts.as_ref().is_none_or(|fts| fts.matches_row(row))
                && self.row_visible(plan, row, viewer)
        };
        let matched_inserts: Vec<&Row> = table_diff.inserts.iter().filter(keep).collect();
        // Deletes are matched by running the SAME predicate + RLS over the
        // deleted rows' pre-commit values (SUB-021) — no per-row
        // subscription bookkeeping is needed.
        let matched_deletes: Vec<&Row> = table_diff
            .deletes
            .iter()
            .map(|(_, old)| old)
            .filter(keep)
            .collect();

        if matched_inserts.is_empty() && matched_deletes.is_empty() {
            return Ok(None);
        }

        let schema = self.table_schema(table_id)?;
        // SPEC-017 §6 (CT-041/042): project each matched insert for THIS
        // viewer, and suppress an update pair whose projected content did
        // not change — a masked-column-only change must not leak that
        // something changed to an unauthorized subscriber.
        let has_policy = self.column_policies.contains_key(&table_id)
            || self
                .transforms
                .as_ref()
                .is_some_and(|engine| engine.touches(table_id));
        let mut suppressed: HashSet<Vec<u8>> = HashSet::new();
        let projected_inserts: Vec<Row> = if has_policy {
            let old_by_pk: HashMap<&[u8], &Row> = table_diff
                .deletes
                .iter()
                .map(|(pk, old)| (pk.as_bytes(), old))
                .collect();
            let mut out = Vec::with_capacity(matched_inserts.len());
            for row in &matched_inserts {
                let projected = self.project_row(table_id, schema, row, viewer, roles)?;
                if viewer.is_some() {
                    let pk = encode_pk_of_row(schema, row.values())?;
                    if let Some(old) = old_by_pk.get(pk.as_bytes()) {
                        let old_projected =
                            self.project_row(table_id, schema, old, viewer, roles)?;
                        if old_projected == projected {
                            suppressed.insert(pk.as_bytes().to_vec());
                            continue;
                        }
                    }
                }
                out.push(projected);
            }
            out
        } else {
            matched_inserts.iter().map(|row| (*row).clone()).collect()
        };
        let matched_deletes: Vec<&Row> = matched_deletes
            .into_iter()
            .filter(|old| {
                encode_pk_of_row(schema, old.values())
                    .map_or(true, |pk| !suppressed.contains(pk.as_bytes()))
            })
            .collect();
        if projected_inserts.is_empty() && matched_deletes.is_empty() {
            return Ok(None);
        }

        // SPEC-023 DMX-061: a CrdtText column of an in-place replacement
        // fans out as the compact tagged op diff, never the whole document.
        let inserts = match crdt_ordinals(schema) {
            ords if ords.is_empty() => encode_full_rows(&projected_inserts)?,
            ords => {
                let refs: Vec<&Row> = projected_inserts.iter().collect();
                let rows = crdt_patch_rows(schema, &ords, &refs, table_diff)?;
                encode_full_rows(&rows)?
            }
        };
        let deletes = encode_pk_rows(schema, &matched_deletes)?;
        Ok(Some(TableUpdate {
            table_id: table_id.as_u32(),
            table_name: schema.name.to_owned(),
            query_id: 0, // per-connection id is applied by the transport
            inserts,
            deletes,
        }))
    }

    // --- Candidate selection (SUB-023/040) ----------------------------------

    /// Unique plans to evaluate for this commit: value-index hits for every
    /// delta row plus the per-table fallback watchers — never a scan over
    /// all registered plans.
    fn candidate_plans(&self, diff: &TxDiff) -> HashSet<QueryHash> {
        let mut candidates = HashSet::new();
        for table in &diff.tables {
            // Fallback tier: every no-search-arg plan on a touched table.
            if let Some(plans) = self.table_watchers.get(&table.table_id) {
                candidates.extend(plans.iter().copied());
            }
            // Value tier: project each delta row's value for every
            // registered (table, column) and select exact matches.
            let rows = table
                .inserts
                .iter()
                .chain(table.deletes.iter().map(|(_, old)| old));
            for row in rows {
                self.select_by_value(table.table_id, row, &mut candidates);
                self.select_by_fts_terms(table.table_id, row, &mut candidates);
            }
        }
        candidates
    }

    /// FTS-042 candidate tier: analyze the delta row's `#[fulltext]` columns
    /// and select the MATCH plans registered under any of its terms (exact
    /// term hits plus the few prefix registrations) — fan-out stays
    /// O(P_matched + S_matched), never O(all MATCH plans).
    fn select_by_fts_terms(&self, table_id: TableId, row: &Row, out: &mut HashSet<QueryHash>) {
        let Some(analyzers) = self.fts_analyzers.get(&table_id) else {
            return;
        };
        let prefixes = self.fts_prefixes.get(&table_id);
        for (column, analyzer) in analyzers {
            let text = match row.value(*column) {
                Some(crate::store::RowValue::Str(s)) => s.clone(),
                Some(crate::store::RowValue::Optional(Some(inner))) => match inner.as_ref() {
                    crate::store::RowValue::Str(s) => s.clone(),
                    _ => continue,
                },
                Some(crate::store::RowValue::List(values)) => {
                    let mut parts = Vec::with_capacity(values.len());
                    for value in values {
                        if let crate::store::RowValue::Str(s) = value {
                            parts.push(s.as_str());
                        }
                    }
                    parts.join(" ")
                }
                _ => continue,
            };
            let mut seen: HashSet<String> = HashSet::new();
            for (term, _) in analyzer.analyze(&text) {
                if !seen.insert(term.clone()) {
                    continue;
                }
                if let Some(plans) = self.fts_terms.get(&(table_id, term.clone())) {
                    out.extend(plans.iter().copied());
                }
                if let Some(prefixes) = prefixes {
                    for (prefix, hash) in prefixes {
                        if term.starts_with(prefix.as_str()) {
                            out.insert(*hash);
                        }
                    }
                }
            }
        }
    }

    /// Add the value-indexed plans whose `(table, column, value)` matches a
    /// projected value of `row` — probing only the columns some plan
    /// actually indexes (O(indexed columns), not O(all search args)).
    fn select_by_value(&self, table_id: TableId, row: &Row, out: &mut HashSet<QueryHash>) {
        let Some(columns) = self.indexed_columns.get(&table_id) else {
            return;
        };
        for &column in columns.keys() {
            if let Some(value) = row.value(column)
                && let Ok(encoded) = encode_row(std::slice::from_ref(value))
                && let Some(plans) = self.search_args.get(&(table_id, column, encoded))
            {
                out.extend(plans.iter().copied());
            }
        }
    }

    // --- Registry internals -------------------------------------------------

    /// The `(table, column, encoded value)` search key of a plan's leading
    /// equality, if it has one.
    fn search_key(plan: &CompiledPlan) -> Option<(TableId, u16, ValueKey)> {
        let table_id = plan.table_ids[0];
        let (column, value) = plan.equalities.first()?;
        let encoded = encode_row(std::slice::from_ref(value)).ok()?;
        Some((table_id, *column, encoded))
    }

    /// Place a plan in exactly one pruning tier (SUB-023/040) under its
    /// **effective** `hash` (the `queries` key, which folds in the viewer for
    /// RLS plans): the value index when it has a top-level single-column
    /// equality, else the per-table fallback.
    fn index_plan(&mut self, hash: QueryHash, plan: &Arc<CompiledPlan>) {
        let table_id = plan.table_ids[0];
        if let Some(key) = Self::search_key(plan) {
            let column = key.1;
            self.search_args.entry(key).or_default().insert(hash);
            *self
                .indexed_columns
                .entry(table_id)
                .or_default()
                .entry(column)
                .or_insert(0) += 1;
        } else if let Some(fts) = &plan.fts {
            // FTS-042: register the MATCH plan under its query terms so a
            // commit only evaluates plans whose terms appear in the delta.
            let (terms, prefixes) = fts.pruning_terms();
            for term in terms {
                self.fts_terms
                    .entry((table_id, term))
                    .or_default()
                    .insert(hash);
            }
            for prefix in prefixes {
                self.fts_prefixes
                    .entry(table_id)
                    .or_default()
                    .push((prefix, hash));
            }
        } else {
            self.table_watchers
                .entry(table_id)
                .or_default()
                .insert(hash);
        }
    }

    /// Remove a plan's pruning-index membership under its effective `hash`
    /// (last-subscriber eviction).
    fn deindex_plan(&mut self, hash: QueryHash, plan: &CompiledPlan) {
        let table_id = plan.table_ids[0];
        if let Some(key) = Self::search_key(plan) {
            let column = key.1;
            if let Some(set) = self.search_args.get_mut(&key) {
                set.remove(&hash);
                if set.is_empty() {
                    self.search_args.remove(&key);
                }
            }
            if let Some(columns) = self.indexed_columns.get_mut(&table_id) {
                if let Some(count) = columns.get_mut(&column) {
                    *count -= 1;
                    if *count == 0 {
                        columns.remove(&column);
                    }
                }
                if columns.is_empty() {
                    self.indexed_columns.remove(&table_id);
                }
            }
        } else if let Some(fts) = &plan.fts {
            let (terms, prefixes) = fts.pruning_terms();
            for term in terms {
                let key = (table_id, term);
                if let Some(set) = self.fts_terms.get_mut(&key) {
                    set.remove(&hash);
                    if set.is_empty() {
                        self.fts_terms.remove(&key);
                    }
                }
            }
            if !prefixes.is_empty()
                && let Some(list) = self.fts_prefixes.get_mut(&table_id)
            {
                list.retain(|(prefix, plan_hash)| {
                    !(*plan_hash == hash && prefixes.contains(prefix))
                });
                if list.is_empty() {
                    self.fts_prefixes.remove(&table_id);
                }
            }
        } else if let Some(set) = self.table_watchers.get_mut(&table_id) {
            set.remove(&hash);
            if set.is_empty() {
                self.table_watchers.remove(&table_id);
            }
        }
    }

    fn drop_subscriber(&mut self, hash: QueryHash, connection: u128) {
        let Some(state) = self.queries.get_mut(&hash) else {
            return;
        };
        state.subscribers.remove(&connection);
        if state.subscribers.is_empty() {
            let plan = Arc::clone(&state.plan);
            self.queries.remove(&hash);
            self.deindex_plan(hash, &plan);
            // The bucket is gone, so its retained resume window is dead
            // weight (SPEC-021 CS-021): free it with the plan.
            self.windows
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&hash);
        }
    }

    /// The `query_id` `connection` holds for `hash` (SUB-001) — the handle
    /// the client knows this query by. A connection has few queries, so the
    /// linear probe over its handle map is cheaper than a maintained reverse
    /// index. `0` when the connection does not hold the query (a race with
    /// unsubscribe; the fan-out delivers nothing special for it).
    fn query_id_of(&self, connection: u128, hash: QueryHash) -> u32 {
        self.connections
            .get(&connection)
            .and_then(|handles| {
                handles
                    .iter()
                    .find_map(|(qid, h)| (*h == hash).then_some(*qid))
            })
            .unwrap_or(0)
    }

    fn assign_query_id(&mut self, connection: u128) -> u32 {
        let next = self.next_query_id.entry(connection).or_insert(1);
        let id = *next;
        *next = next.wrapping_add(1);
        id
    }

    fn table_schema(&self, table_id: TableId) -> Result<&'static TableSchema> {
        self.schema
            .tables()
            .find(|t| TableId::of(t.name) == table_id)
            .ok_or_else(|| {
                FluxumError::Storage(format!(
                    "subscription plan references unknown table id {table_id}"
                ))
            })
    }

    fn spatial_candidates(
        &self,
        snapshot: &Snapshot,
        table_id: TableId,
        constraint: SpatialConstraint,
    ) -> Result<Vec<Row>> {
        match constraint {
            SpatialConstraint::Region(rect) => snapshot.spatial_region(table_id, rect),
            SpatialConstraint::Radius { x, y, r } => snapshot.spatial_radius(table_id, x, y, r),
        }
    }
}

/// Rows touched by snapshot-read candidate selection (SPEC-018 acceptance
/// 2/3: proves range pushdown reads O(bounded range), not O(table)).
/// Process-global, relaxed — a observability counter, never control flow.
pub static QUERY_ROWS_SCANNED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// In-RAM sorts performed for `ORDER BY` (SPEC-018 QP-020: an index-served
/// order must leave this untouched).
pub static QUERY_SORTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Execute an `IndexScan` plan (SPEC-018 QP-010/011/020/021/022): one
/// bounded scan per probe, residual filter + RLS applied per yielded row —
/// and, when the order is index-served with a `LIMIT`, an early stop after
/// the first `n` authorized rows (top-N reads O(n + skipped), and RLS runs
/// *before* counting toward the limit so a page is never short, QP-022).
impl SubscriptionManager {
    #[allow(clippy::too_many_arguments)]
    fn index_scan_rows(
        &self,
        plan: &CompiledPlan,
        scan: &crate::sql::IndexScanPlan,
        viewer: Option<&Identity>,
        snapshot: &Snapshot,
        table_id: TableId,
        schema: &TableSchema,
        guard: &ScanGuard,
        limit: Option<u32>,
    ) -> Result<Vec<Row>> {
        // QP-040/041: the AFTER cursor's bound already seeks to the order
        // value; what remains is skipping rows AT the cursor value up to (and
        // including) the cursor's primary key — the `(value = c AND pk ≤ k)`
        // residue of the keyset predicate (`pk ≥ k` for a DESC walk). The
        // tiebreak compares ENCODED PK bytes, because that is the total order
        // the index actually yields within one key — the cursor is unambiguous
        // exactly because the walk and the comparison share one order.
        let cursor_boundary: Option<crate::store::PkBytes> = plan
            .cursor
            .as_ref()
            .map(|cursor| encode_pk_values(schema, std::slice::from_ref(&cursor.pk_value)))
            .transpose()?;
        let cursor_skip = |row: &Row| -> bool {
            let (Some(cursor), Some(boundary), Some(order)) =
                (&plan.cursor, &cursor_boundary, plan.order_by)
            else {
                return false;
            };
            let Some(value) = row.value(order.column) else {
                return false;
            };
            if crate::sql::cmp_row_values(value, &cursor.order_value)
                != Some(std::cmp::Ordering::Equal)
            {
                return false;
            }
            let Ok(row_pk) = encode_pk_of_row(schema, row.values()) else {
                return false;
            };
            match row_pk.as_bytes().cmp(boundary.as_bytes()) {
                std::cmp::Ordering::Equal => true,
                std::cmp::Ordering::Less => !order.descending,
                std::cmp::Ordering::Greater => order.descending,
            }
        };
        let residual_keep = |row: &Row| {
            guard.admit()
                && !cursor_skip(row)
                && plan.residual.as_ref().is_none_or(|f| f(row))
                && self.row_visible(plan, row, viewer)
        };
        // QP-021: early stop only when the scan order IS the result order.
        // SEC-045: the *effective* limit drives it, so a clamped/default
        // LIMIT stops the walk exactly like an explicit one.
        let early_stop = if plan.ordered_by_index {
            limit.map(|n| n as usize)
        } else {
            None
        };
        let descending = plan.order_by.is_some_and(|o| o.descending) && plan.ordered_by_index;
        let mut rows: Vec<Row> = Vec::new();
        'probes: for probe in &scan.probes {
            let lower = bound_ref(&scan.lower);
            let upper = bound_ref(&scan.upper);
            let iter = snapshot.index_scan(table_id, scan.index_id, probe, lower, upper)?;
            if descending {
                // DESC is served by walking the bounded range in reverse: the
                // range itself stays bounded, so the read cost is unchanged.
                let range: Vec<&Row> = iter.collect();
                for row in range.into_iter().rev() {
                    if residual_keep(row) {
                        rows.push(row.clone());
                        if early_stop.is_some_and(|n| rows.len() >= n) {
                            break 'probes;
                        }
                    } else if guard.is_aborted() {
                        break 'probes;
                    }
                }
            } else {
                for row in iter {
                    if residual_keep(row) {
                        rows.push(row.clone());
                        if early_stop.is_some_and(|n| rows.len() >= n) {
                            break 'probes;
                        }
                    } else if guard.is_aborted() {
                        break 'probes;
                    }
                }
            }
        }
        Ok(rows)
    }
}

/// Borrow a `Bound<RowValue>` as the `Bound<&RowValue>` the scan API takes.
fn bound_ref(
    bound: &std::ops::Bound<crate::store::RowValue>,
) -> std::ops::Bound<&crate::store::RowValue> {
    match bound {
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        std::ops::Bound::Included(v) => std::ops::Bound::Included(v),
        std::ops::Bound::Excluded(v) => std::ops::Bound::Excluded(v),
    }
}

/// The ordinals of `schema`'s `CrdtText` columns (SPEC-023 DMX-060).
fn crdt_ordinals(schema: &TableSchema) -> Vec<u16> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.ty, crate::schema::FluxType::CrdtText))
        .map(|(i, _)| u16::try_from(i).unwrap_or(u16::MAX))
        .collect()
}

/// DMX-061: rewrite each matched insert that replaces a same-PK deleted row
/// so its `CrdtText` columns carry the compact tagged op diff (the ops this
/// commit added) instead of the whole document. Fresh inserts — and any
/// value that fails to decode — keep the full tagged state; the tag byte
/// tells the subscriber which decode applies.
fn crdt_patch_rows(
    schema: &TableSchema,
    ordinals: &[u16],
    inserts: &[&Row],
    table_diff: &crate::store::TableDiff,
) -> Result<Vec<Row>> {
    use crate::crdt::{CrdtText, encode_ops};
    let old_by_pk: HashMap<&[u8], &Row> = table_diff
        .deletes
        .iter()
        .map(|(pk, old)| (pk.as_bytes(), old))
        .collect();
    let mut out = Vec::with_capacity(inserts.len());
    for row in inserts {
        let pk = encode_pk_of_row(schema, row.values())?;
        let Some(old) = old_by_pk.get(pk.as_bytes()) else {
            out.push((*row).clone()); // fresh insert: full state
            continue;
        };
        let mut values = row.values().to_vec();
        for &ordinal in ordinals {
            let idx = usize::from(ordinal);
            let (Some(crate::store::RowValue::Bytes(new_bytes)), Some(old_value)) =
                (values.get(idx), old.values().get(idx))
            else {
                continue;
            };
            let crate::store::RowValue::Bytes(old_bytes) = old_value else {
                continue;
            };
            if let (Ok(new_doc), Ok(old_doc)) = (
                CrdtText::from_bytes(new_bytes),
                CrdtText::from_bytes(old_bytes),
            ) {
                let patch = new_doc.ops_since(&old_doc);
                values[idx] = crate::store::RowValue::Bytes(encode_ops(&patch));
            }
        }
        out.push(Row::new(values));
    }
    Ok(out)
}

/// Encode owned rows as a full-row FluxBIN `RowList` (RPC-041).
fn encode_full_rows(rows: &[Row]) -> Result<RowList> {
    let mut builder = RowListBuilder::new();
    for row in rows {
        builder.push_row(&encode_row(row.values())?);
    }
    Ok(builder.finish())
}

/// Encode rows as a PK-only FluxBIN `RowList` (RPC-042 delete form): only
/// the primary-key field bytes travel for a delete.
fn encode_pk_rows(schema: &TableSchema, rows: &[&Row]) -> Result<RowList> {
    let mut builder = RowListBuilder::new();
    for row in rows {
        let pk = encode_pk_of_row(schema, row.values())?;
        builder.push_row(pk.as_bytes());
    }
    Ok(builder.finish())
}

/// Convert one [`RowValue`] to JSON for the HTTP admin surface (RPC-050).
/// Numbers that overflow an IEEE-754 double (`u64`/`i64` extremes,
/// entity/timestamp micros) are rendered as strings to avoid precision
/// loss; bytes and identities are hex strings.
///
/// Public because the admin console's live diff stream (SPEC-024 DEV-030)
/// renders committed [`TxDiff`] rows with exactly the same JSON currency as
/// `POST /query` — one converter, so the two surfaces cannot drift.
pub fn row_value_to_json(value: &crate::store::RowValue) -> serde_json::Value {
    use crate::store::RowValue as V;
    use serde_json::Value as J;
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    match value {
        V::Bool(b) => J::Bool(*b),
        V::I8(n) => J::from(*n),
        V::I16(n) => J::from(*n),
        V::I32(n) => J::from(*n),
        V::I64(n) => J::from(*n),
        V::U8(n) => J::from(*n),
        V::U16(n) => J::from(*n),
        V::U32(n) => J::from(*n),
        V::U64(n) => J::from(*n),
        V::F32(x) => serde_json::Number::from_f64(f64::from(*x)).map_or(J::Null, J::Number),
        V::F64(x) => serde_json::Number::from_f64(*x).map_or(J::Null, J::Number),
        V::Str(s) => J::String(s.clone()),
        V::Bytes(b) => J::String(hex(b)),
        V::Identity(id) => J::String(id.to_string()),
        V::ConnectionId(c) => J::String(c.as_u128().to_string()),
        V::EntityId(e) => J::String(e.as_u64().to_string()),
        V::Timestamp(t) => J::String(t.as_micros().to_string()),
        V::Decimal(d) => J::String(d.to_string()),
        V::Blob(b) => J::String(b.to_string()),
        V::Optional(None) => J::Null,
        V::Optional(Some(inner)) => row_value_to_json(inner),
        V::List(items) => J::Array(items.iter().map(row_value_to_json).collect()),
        V::Enum { tag, payload } => {
            let mut obj = serde_json::Map::new();
            obj.insert("tag".to_owned(), J::from(*tag));
            obj.insert(
                "payload".to_owned(),
                J::Array(payload.iter().map(row_value_to_json).collect()),
            );
            J::Object(obj)
        }
        V::Struct(fields) => J::Array(fields.iter().map(row_value_to_json).collect()),
    }
}

/// Apply a plan's row-level visibility filter for `viewer` (SUB-030):
/// `true` when there is no viewer (public query or server-peer bypass) or
/// the plan has no `#[visibility]` filter; otherwise the plan's `rls`
/// closure decides.
fn visible(plan: &CompiledPlan, row: &Row, viewer: Option<&Identity>) -> bool {
    match (viewer, plan.rls.as_ref()) {
        (Some(id), Some(rls)) => rls(row, id),
        _ => true,
    }
}

fn limit_exceeded(which: &str) -> FluxumError {
    FluxumError::query(
        codes::SUB_LIMIT_EXCEEDED,
        format!("subscription limit exceeded: {which}"),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::row_value_to_json;
    use crate::store::RowValue;
    use crate::types::{ConnectionId, Decimal, EntityId, Identity, Timestamp};
    use serde_json::{Value as J, json};

    /// RPC-050: every RowValue variant renders to its documented JSON form —
    /// numbers as numbers, precision-risky scalars as strings, bytes as hex.
    #[test]
    fn every_row_value_variant_renders_to_json() {
        assert_eq!(row_value_to_json(&RowValue::Bool(true)), json!(true));
        assert_eq!(row_value_to_json(&RowValue::I8(-1)), json!(-1));
        assert_eq!(row_value_to_json(&RowValue::I16(-300)), json!(-300));
        assert_eq!(row_value_to_json(&RowValue::I32(70_000)), json!(70_000));
        assert_eq!(row_value_to_json(&RowValue::I64(-9)), json!(-9));
        assert_eq!(row_value_to_json(&RowValue::U8(255)), json!(255));
        assert_eq!(row_value_to_json(&RowValue::U16(65_535)), json!(65_535));
        assert_eq!(row_value_to_json(&RowValue::U32(7)), json!(7));
        assert_eq!(row_value_to_json(&RowValue::U64(u64::MAX)), json!(u64::MAX));
        assert_eq!(row_value_to_json(&RowValue::F32(0.5)), json!(0.5));
        assert_eq!(row_value_to_json(&RowValue::F64(1.25)), json!(1.25));
        // Non-finite floats have no JSON number: rendered as null.
        assert_eq!(row_value_to_json(&RowValue::F32(f32::NAN)), J::Null);
        assert_eq!(row_value_to_json(&RowValue::F64(f64::INFINITY)), J::Null);
        assert_eq!(row_value_to_json(&RowValue::Str("hi".into())), json!("hi"));
        assert_eq!(
            row_value_to_json(&RowValue::Bytes(vec![0x00, 0xAB])),
            json!("00ab")
        );
        let id = Identity::from_bytes([9u8; 32]);
        assert_eq!(
            row_value_to_json(&RowValue::Identity(id)),
            json!(id.to_string())
        );
        assert_eq!(
            row_value_to_json(&RowValue::ConnectionId(ConnectionId::new(42))),
            json!("42")
        );
        assert_eq!(
            row_value_to_json(&RowValue::EntityId(EntityId::new(7))),
            json!("7")
        );
        assert_eq!(
            row_value_to_json(&RowValue::Timestamp(Timestamp::from_micros(1000))),
            json!("1000")
        );
        assert_eq!(
            row_value_to_json(&RowValue::Decimal(Decimal::from_parts(150, 2))),
            json!("1.50")
        );
        assert_eq!(row_value_to_json(&RowValue::Optional(None)), J::Null);
        assert_eq!(
            row_value_to_json(&RowValue::Optional(Some(Box::new(RowValue::U32(3))))),
            json!(3)
        );
        assert_eq!(
            row_value_to_json(&RowValue::List(vec![RowValue::I64(1), RowValue::I64(2)])),
            json!([1, 2])
        );
        assert_eq!(
            row_value_to_json(&RowValue::Enum {
                tag: 2,
                payload: vec![RowValue::Bool(false)],
            }),
            json!({ "tag": 2, "payload": [false] })
        );
        assert_eq!(
            row_value_to_json(&RowValue::Struct(vec![
                RowValue::U8(1),
                RowValue::Str("s".into()),
            ])),
            json!([1, "s"])
        );
    }
}
