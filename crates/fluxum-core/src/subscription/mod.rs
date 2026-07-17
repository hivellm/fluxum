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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluxum_protocol::codes;
use fluxum_protocol::{InitialData, RowList, RowListBuilder, TableUpdate, TxUpdate};

use crate::error::{FluxumError, Result};
use crate::schema::{Schema, TableSchema};
use crate::sql::{CompiledPlan, QueryHash, SpatialConstraint, compile};
use crate::store::committed::Snapshot;
use crate::store::row::{encode_pk_of_row, encode_row};
use crate::store::{Row, TableId, TxDiff};
use crate::types::Identity;

/// Who is subscribing (SUB-030/031): the caller's stable identity and
/// whether the auth layer resolved it as a server-to-server peer
/// (`SHA-256("SERVER:" + name)`, AUTH-061). Server peers bypass every
/// `#[visibility]` filter; the manager cannot tell a server identity from
/// its bytes alone, so the transport (which holds the
/// [`crate::auth::Authenticator`]) supplies this flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscriber {
    /// The caller's 256-bit identity (SPEC-009).
    pub identity: Identity,
    /// Whether this identity is a trusted server peer (RLS bypass, SUB-031).
    pub is_server_peer: bool,
}

impl Subscriber {
    /// A regular client subscriber (RLS applies).
    pub fn client(identity: Identity) -> Self {
        Self {
            identity,
            is_server_peer: false,
        }
    }

    /// A server-peer subscriber (RLS bypass, SUB-031).
    pub fn server_peer(identity: Identity) -> Self {
        Self {
            identity,
            is_server_peer: true,
        }
    }
}

/// The `search_args` value key: the FluxBIN encoding of one equality value
/// ([`RowValue`] is not `Ord`/`Hash` because of floats, but its byte
/// encoding is). Equality is exact, so any deterministic encoding indexes
/// correctly.
type ValueKey = Vec<u8>;

/// Configurable subscription admission caps (SUB-044).
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionLimits {
    /// Max live subscriptions per connection (default 1,000).
    pub max_subscriptions_per_connection: usize,
    /// Max unique `QueryState` entries shard-wide (default 100,000).
    pub max_compiled_plans: usize,
}

impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_subscriptions_per_connection: 1_000,
            max_compiled_plans: 100_000,
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
    /// The connections subscribed to this query — the fan-out targets.
    pub subscribers: Vec<u128>,
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
    /// One entry per unique query (SUB-020).
    queries: HashMap<QueryHash, QueryState>,
    /// Per-connection: `query_id` → the query it handles (Unsubscribe/cleanup).
    connections: HashMap<u128, HashMap<u32, QueryHash>>,
    /// Value-level pruning index (SUB-023): `(table, column, encoded value)`
    /// → plans. The value key is FluxBIN bytes (see [`ValueKey`]).
    search_args: HashMap<(TableId, u16, ValueKey), HashSet<QueryHash>>,
    /// Which columns of a table carry a search arg, with a refcount so a
    /// column is probed only while some plan indexes it (drives per-delta
    /// probing without scanning all search args).
    indexed_columns: HashMap<TableId, HashMap<u16, usize>>,
    /// Fallback tier (SUB-040): plans on a table with no usable search arg.
    table_watchers: HashMap<TableId, HashSet<QueryHash>>,
    /// Next per-connection query id (monotonic per connection).
    next_query_id: HashMap<u128, u32>,
}

impl SubscriptionManager {
    /// Build an empty manager over an assembled schema.
    pub fn new(schema: Arc<Schema>, limits: SubscriptionLimits) -> Self {
        Self {
            schema,
            limits,
            queries: HashMap::new(),
            connections: HashMap::new(),
            search_args: HashMap::new(),
            indexed_columns: HashMap::new(),
            table_watchers: HashMap::new(),
            next_query_id: HashMap::new(),
        }
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
        let (hash, viewer) = self.effective_key(&plan, subscriber);

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

        let initial = self.initial_data(&plan, viewer.as_ref(), snapshot)?;

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
        subscriber: Subscriber,
    ) -> (QueryHash, Option<Identity>) {
        if plan.rls.is_none() {
            return (plan.query_hash, None);
        }
        if subscriber.is_server_peer {
            // Server peers bypass RLS and share one bucket that sees all
            // rows matching the predicate (SUB-031).
            let hash = QueryHash(
                crate::simd::global().hash64(b"__fluxum_server_peer__", plan.query_hash.0),
            );
            (hash, None)
        } else {
            let hash = QueryHash(
                crate::simd::global().hash64(subscriber.identity.as_bytes(), plan.query_hash.0),
            );
            (hash, Some(subscriber.identity))
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
        let (_, viewer) = self.effective_key(&plan, subscriber);
        self.initial_data(&plan, viewer.as_ref(), snapshot)
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
        let initial = self.snapshot_result(subscriber, sql, snapshot)?;
        let columns: Vec<&str> = table.columns.iter().map(|c| c.name).collect();
        let mut rows = Vec::new();
        for bytes in initial.tables[0].inserts.iter() {
            let row = crate::store::row::decode_row(table, bytes)?;
            let mut object = serde_json::Map::new();
            for (column, value) in table.columns.iter().zip(row.values()) {
                object.insert(column.name.to_owned(), row_value_to_json(value));
            }
            rows.push(serde_json::Value::Object(object));
        }
        Ok(serde_json::json!({
            "table": table.name,
            "columns": columns,
            "rows": rows,
        }))
    }

    /// Evaluate a commit against the candidate plans and produce one shared,
    /// once-encoded [`QueryDelta`] per matched query (SUB-021..024).
    ///
    /// Only plans selected by the pruning indexes for this commit's delta
    /// rows are evaluated; a query whose matched inserts and deletes are both
    /// empty produces nothing. Ordering: deltas come back sorted by
    /// `QueryHash` for deterministic tests.
    pub fn on_commit(&self, diff: &TxDiff) -> Result<Vec<QueryDelta>> {
        let candidates = self.candidate_plans(diff);
        let mut deltas = Vec::new();
        for hash in candidates {
            let Some(state) = self.queries.get(&hash) else {
                continue;
            };
            if state.subscribers.is_empty() {
                continue;
            }
            let Some(update) = self.evaluate(&state.plan, state.viewer.as_ref(), diff)? else {
                continue;
            };
            let mut subscribers: Vec<u128> = state.subscribers.iter().copied().collect();
            subscribers.sort_unstable();
            deltas.push(QueryDelta {
                query_hash: hash,
                update: Arc::new(update),
                subscribers,
            });
        }
        deltas.sort_by_key(|d| d.query_hash);
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
            tables: vec![(*delta.update).clone()],
        }
    }

    // --- InitialData (SUB-002/013) ------------------------------------------

    fn initial_data(
        &self,
        plan: &CompiledPlan,
        viewer: Option<&Identity>,
        snapshot: &Snapshot,
    ) -> Result<InitialData> {
        let table_id = plan.table_ids[0];
        let schema = self.table_schema(table_id)?;

        // Candidate rows: spatial clauses go through the index (SUB-022);
        // otherwise a full committed scan, filtered by the predicate and the
        // row-level visibility filter for this viewer (SUB-030). `viewer` is
        // `None` for a public query or a server-peer bypass — no RLS.
        let keep = |row: &Row| plan.matches(row) && visible(plan, row, viewer);
        let mut rows: Vec<Row> = match &plan.spatial {
            Some(constraint) => self
                .spatial_candidates(snapshot, table_id, *constraint)?
                .into_iter()
                .filter(|row| keep(row))
                .collect(),
            None => snapshot
                .scan(table_id)?
                .filter(|row| keep(row))
                .cloned()
                .collect(),
        };

        // ORDER BY / LIMIT apply to InitialData ONLY (SUB-013).
        if let Some(order) = plan.order_by {
            rows.sort_by(|a, b| {
                let ord = a
                    .value(order.column)
                    .zip(b.value(order.column))
                    .and_then(|(x, y)| crate::sql::cmp_row_values(x, y))
                    .unwrap_or(std::cmp::Ordering::Equal);
                if order.descending { ord.reverse() } else { ord }
            });
        }
        if let Some(limit) = plan.limit {
            rows.truncate(limit as usize);
        }

        let inserts = encode_full_rows(&rows)?;
        Ok(InitialData {
            id: 0,
            schema_version: 0,
            tables: vec![TableUpdate {
                table_id: table_id.as_u32(),
                table_name: schema.name.to_owned(),
                query_id: 0,
                inserts,
                deletes: RowList::empty(),
            }],
        })
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
        diff: &TxDiff,
    ) -> Result<Option<TableUpdate>> {
        let table_id = plan.table_ids[0];
        let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == table_id) else {
            return Ok(None); // fast path: this plan's table did not change
        };

        let keep = |row: &&Row| plan.matches(row) && visible(plan, row, viewer);
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
        // SPEC-023 DMX-061: a CrdtText column of an in-place replacement
        // fans out as the compact tagged op diff, never the whole document.
        let inserts = match crdt_ordinals(schema) {
            ords if ords.is_empty() => encode_full_row_refs(&matched_inserts)?,
            ords => {
                let rows = crdt_patch_rows(schema, &ords, &matched_inserts, table_diff)?;
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
            }
        }
        candidates
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
        }
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
            if let (Ok(new_doc), Ok(old_doc)) =
                (CrdtText::from_bytes(new_bytes), CrdtText::from_bytes(old_bytes))
            {
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

/// Encode borrowed rows as a full-row FluxBIN `RowList`.
fn encode_full_row_refs(rows: &[&Row]) -> Result<RowList> {
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
fn row_value_to_json(value: &crate::store::RowValue) -> serde_json::Value {
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
