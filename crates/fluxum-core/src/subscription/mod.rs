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

/// One unique query (SUB-040): the shared plan and its subscriber set.
struct QueryState {
    plan: Arc<CompiledPlan>,
    subscribers: HashSet<u128>,
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

    /// Number of unique compiled plans currently registered (SUB-044 cap
    /// surface; also the dedup witness).
    pub fn plan_count(&self) -> usize {
        self.queries.len()
    }

    /// Number of live subscriptions on `connection`.
    pub fn subscription_count(&self, connection: u128) -> usize {
        self.connections.get(&connection).map_or(0, HashMap::len)
    }

    /// Register one subscription `sql` for `connection`, evaluated for
    /// `caller` (SUB-001/002/020/044): compile (or reuse the shared plan by
    /// [`QueryHash`]), enforce the admission caps, assign a `query_id`, and
    /// return the `InitialData` snapshot. A compile error is a wire-ready
    /// 400; a cap breach is a 429 — neither registers anything.
    pub fn subscribe(
        &mut self,
        connection: u128,
        caller: &Identity,
        sql: &str,
        snapshot: &Snapshot,
    ) -> Result<Subscribed> {
        let plan = compile(&self.schema, sql)?;
        let hash = plan.query_hash;

        // Admission (SUB-044): reject before any mutation. A brand-new query
        // adds a plan; re-subscribing to an existing one does not.
        let live = self.subscription_count(connection);
        if live >= self.limits.max_subscriptions_per_connection {
            return Err(limit_exceeded("max_subscriptions_per_connection"));
        }
        let new_plan = !self.queries.contains_key(&hash);
        if new_plan && self.queries.len() >= self.limits.max_compiled_plans {
            return Err(limit_exceeded("max_compiled_plans"));
        }

        let initial = self.initial_data(&plan, caller, snapshot)?;

        // Register: shared plan + pruning-index membership on first sighting.
        let plan = if let Some(state) = self.queries.get_mut(&hash) {
            state.subscribers.insert(connection);
            Arc::clone(&state.plan)
        } else {
            let plan = Arc::new(plan);
            self.index_plan(&plan);
            let mut subscribers = HashSet::new();
            subscribers.insert(connection);
            self.queries.insert(
                hash,
                QueryState {
                    plan: Arc::clone(&plan),
                    subscribers,
                },
            );
            plan
        };

        let query_id = self.assign_query_id(connection);
        self.connections
            .entry(connection)
            .or_default()
            .insert(query_id, hash);

        let mut initial = initial;
        for table in &mut initial.tables {
            table.query_id = query_id;
        }
        let _ = &plan; // plan kept alive by `queries`
        Ok(Subscribed { query_id, initial })
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
            let Some(update) = self.evaluate(&state.plan, diff)? else {
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
        caller: &Identity,
        snapshot: &Snapshot,
    ) -> Result<InitialData> {
        let table_id = plan.table_ids[0];
        let schema = self.table_schema(table_id)?;

        // Candidate rows: spatial clauses go through the index (SUB-022);
        // otherwise a full committed scan, filtered by the predicate and the
        // row-level visibility slot (SUB-030 — `None` until T4.3 fills it).
        let keep = |row: &Row| plan.matches(row) && visible(plan, row, caller);
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
    /// (SUB-024). `None` when nothing matched.
    fn evaluate(&self, plan: &CompiledPlan, diff: &TxDiff) -> Result<Option<TableUpdate>> {
        let table_id = plan.table_ids[0];
        let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == table_id) else {
            return Ok(None); // fast path: this plan's table did not change
        };

        let matched_inserts: Vec<&Row> = table_diff
            .inserts
            .iter()
            .filter(|row| plan.matches(row))
            .collect();
        // Deletes are matched by running the SAME predicate over the deleted
        // rows' pre-commit values (SUB-021) — no per-row subscription
        // bookkeeping is needed.
        let matched_deletes: Vec<&Row> = table_diff
            .deletes
            .iter()
            .map(|(_, old)| old)
            .filter(|row| plan.matches(row))
            .collect();

        if matched_inserts.is_empty() && matched_deletes.is_empty() {
            return Ok(None);
        }

        let schema = self.table_schema(table_id)?;
        let inserts = encode_full_row_refs(&matched_inserts)?;
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

    /// Place a plan in exactly one pruning tier (SUB-023/040): the value
    /// index when it has a top-level single-column equality, else the
    /// per-table fallback.
    fn index_plan(&mut self, plan: &Arc<CompiledPlan>) {
        let hash = plan.query_hash;
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

    /// Remove a plan's pruning-index membership (last-subscriber eviction).
    fn deindex_plan(&mut self, plan: &CompiledPlan) {
        let hash = plan.query_hash;
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
            self.deindex_plan(&plan);
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

/// Apply a plan's row-level visibility slot (SUB-030): `true` when the plan
/// has no `#[visibility]` filter (the T4.2 state — T4.3 compiles the slot).
fn visible(plan: &CompiledPlan, row: &Row, caller: &Identity) -> bool {
    plan.rls.as_ref().is_none_or(|rls| rls(row, caller))
}

fn limit_exceeded(which: &str) -> FluxumError {
    FluxumError::query(
        codes::RATE_LIMITED,
        format!("subscription limit exceeded: {which}"),
    )
}
