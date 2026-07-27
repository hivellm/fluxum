//! [`SubscriptionManager`]'s registration, evaluation and fan-out core
//! (SPEC-005 SUB-001..SUB-044) — split from the parent module to honour
//! the file-size convention; a child module, so the manager's internals
//! stay private to `subscription`.

#[allow(clippy::wildcard_imports)]
use super::*;

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
    pub(super) fn row_visible(
        &self,
        plan: &CompiledPlan,
        row: &Row,
        viewer: Option<&Identity>,
    ) -> bool {
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
                if let Some(entry) = Self::membership_entry(spec, &row) {
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
                    if keep(&row) {
                        out.push(row);
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
