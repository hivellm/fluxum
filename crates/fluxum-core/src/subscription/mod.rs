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

mod manager;
pub use manager::{QUERY_ROWS_SCANNED, QUERY_SORTS};

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
            let range = snapshot.index_scan(table_id, scan.index_id, probe, lower, upper)?;
            if descending {
                // DESC is served by walking the bounded range in reverse: the
                // range itself stays bounded, so the read cost is unchanged.
                for row in range.into_iter().rev() {
                    if residual_keep(&row) {
                        rows.push(row);
                        if early_stop.is_some_and(|n| rows.len() >= n) {
                            break 'probes;
                        }
                    } else if guard.is_aborted() {
                        break 'probes;
                    }
                }
            } else {
                for row in range {
                    if residual_keep(&row) {
                        rows.push(row);
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
