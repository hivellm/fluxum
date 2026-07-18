//! HTTP/JSON admin surface (SPEC-006 §7, RPC-050..RPC-053; FR-44/FR-91): the
//! curl-friendly operator API served on the same `http_port` as the binary
//! `/rpc` transport, but with plain JSON envelopes (never FluxRPC binary).
//!
//! | Method | Path | Purpose |
//! |--------|------|---------|
//! | GET  | `/health`        | lock-free shard status (`< 50 ms`, RPC-053) |
//! | GET  | `/metrics`       | Prometheus text (`fluxum_*`; T5.6 expands it) |
//! | GET  | `/schema`        | tables + reducers + views as JSON (SPEC-011) |
//! | POST | `/reducer/:name` | call a reducer (JSON args → JSON result) |
//! | POST | `/query`         | one-off read-only SQL → JSON rows |
//! | GET  | `/view/:name`    | call a `#[fluxum::view]` → JSON |
//! | POST | `/audit`         | who changed a table/row and when (SPEC-025 OPS-020; server-peer only) |
//! | GET  | `/plugins`       | active plugins + adopted seams (SPEC-020 PLG-060; never secrets) |
//! | POST | `/plugins/:name/disable` | hot circuit-break without restart (PLG-061); `/enable` reverts |
//!
//! Every response uses the RPC-052 envelope
//! (`{ "success": bool, "request_id"?, "payload"|"error" }`); the paths are
//! unversioned. Admin calls run under the server admin identity (RLS bypass,
//! AUTH-062) — this surface is for trusted operators.

use std::fmt::Write as _;
use std::sync::Arc;

use serde_json::{Value, json};

use fluxum_core::FluxumError;
use fluxum_core::reducer::{FluxValue, ReducerCaller};
use fluxum_core::subscription::Subscriber;
use fluxum_core::types::{ConnectionId, Timestamp};

use crate::ShardContext;

/// A ready-to-serialize admin response: HTTP status + JSON body.
pub struct AdminResponse {
    /// HTTP status code.
    pub status: u16,
    /// JSON body (RPC-052 envelope).
    pub body: Value,
}

impl AdminResponse {
    fn ok(request_id: Option<&str>, payload: Value) -> Self {
        Self {
            status: 200,
            body: envelope(true, request_id, Some(payload), None),
        }
    }

    fn err(status: u16, request_id: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            status,
            body: envelope(false, request_id, None, Some(message.into())),
        }
    }
}

/// RPC-052 success/error envelope.
fn envelope(
    success: bool,
    request_id: Option<&str>,
    payload: Option<Value>,
    error: Option<String>,
) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("success".into(), Value::Bool(success));
    if let Some(id) = request_id {
        object.insert("request_id".into(), Value::String(id.to_owned()));
    }
    if let Some(payload) = payload {
        object.insert("payload".into(), payload);
    }
    if let Some(error) = error {
        object.insert("error".into(), Value::String(error));
    }
    Value::Object(object)
}

/// Dispatch one admin route. `method`/`path` are the request line; `body` is
/// the raw request body (JSON, may be empty). Unknown routes → 404.
pub async fn dispatch(
    ctx: &Arc<ShardContext>,
    method: &str,
    path: &str,
    body: &[u8],
) -> AdminResponse {
    match (method, split_path(path).as_slice()) {
        ("GET", ["health"]) => health(ctx),
        ("GET", ["metrics"]) => metrics(ctx).await,
        ("GET", ["schema"]) => schema(ctx).await,
        ("POST", ["reducer", name]) => reducer_call(ctx, name, body).await,
        ("POST", ["query"]) => query(ctx, body).await,
        ("POST", ["query", "explain"]) => query_explain(ctx, body).await,
        ("POST", ["audit"]) => audit(ctx, body).await,
        ("GET", ["view", name]) => view(ctx, name).await,
        ("GET", ["plugins"]) => plugins(ctx),
        ("POST", ["plugins", name, "disable"]) => plugin_set_disabled(ctx, name, true),
        ("POST", ["plugins", name, "enable"]) => plugin_set_disabled(ctx, name, false),
        ("POST", ["drain"]) => drain(ctx),
        ("POST", ["config", "reload"]) => config_reload(ctx),
        _ => AdminResponse::err(404, None, "not found"),
    }
}

// --- GET /plugins (SPEC-020 PLG-060) --------------------------------------------

/// List active plugins and adopted seams: name, capability, host, placement,
/// health, meters, scope — never key material or tokens.
fn plugins(ctx: &Arc<ShardContext>) -> AdminResponse {
    let report = ctx
        .plugins()
        .map(|registry| registry.report())
        .unwrap_or_default();
    match serde_json::to_value(&report) {
        Ok(payload) => AdminResponse::ok(None, json!({ "plugins": payload })),
        Err(e) => AdminResponse::err(500, None, format!("plugin report serialization: {e}")),
    }
}

/// Hot-disable / re-enable a plugin without a core restart (PLG-061).
fn plugin_set_disabled(ctx: &Arc<ShardContext>, name: &str, disabled: bool) -> AdminResponse {
    let Some(registry) = ctx.plugins() else {
        return AdminResponse::err(404, None, "no plugin registry installed");
    };
    if registry.set_disabled(name, disabled) {
        AdminResponse::ok(None, json!({ "plugin": name, "disabled": disabled }))
    } else {
        AdminResponse::err(404, None, format!("unknown plugin `{name}`"))
    }
}

/// Whether a request path is an admin route (vs the binary `/rpc`).
pub fn is_admin_path(path: &str) -> bool {
    matches!(
        split_path(path).as_slice(),
        ["health"]
            | ["metrics"]
            | ["schema"]
            | ["query"]
            | ["query", "explain"]
            | ["audit"]
            | ["reducer", _]
            | ["view", _]
            | ["plugins"]
            | ["plugins", _, "disable" | "enable"]
            | ["drain"]
            | ["config", "reload"]
    )
}

fn split_path(path: &str) -> Vec<&str> {
    path.split('?')
        .next()
        .unwrap_or("")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

// --- GET /health (RPC-053: lock-free, < 50 ms) ---------------------------------

fn health(ctx: &Arc<ShardContext>) -> AdminResponse {
    use fluxum_core::metrics::ShardState;
    let health = ctx.health(); // atomics + channel gauge — no storage lock
    // OBS-060: status + HTTP code derive from the shard's lifecycle state.
    let (status, code) = match health.state {
        ShardState::Ready => ("ok", 200),
        ShardState::Recovering => ("degraded", 503),
        ShardState::Starting | ShardState::ShuttingDown => ("error", 503),
    };
    let mut body = json!({
        "status": status,
        "shards": [
            {
                "id": health.shard_id.to_string(),
                "state": health.state.as_str(),
                "tx_id": health.last_tx_id,
                "queue_depth": health.queue_depth,
            }
        ],
        "connections": ctx.metrics().connections_active(),
        "uptime_s": ctx.uptime_s(),
    });
    // HWA-013: the effective configuration — probe inputs, derived values
    // with their sources, and the per-kernel SIMD selection. Pre-rendered at
    // install, so this stays a clone on the < 50 ms path (OBS-061).
    if let Some(effective) = ctx.effective_config()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("config".into(), effective.clone());
    }
    // OPS-040: the reloadable values actually in force, with each one's
    // source — this is how an operator confirms a reload landed (and, when
    // a value looks unchanged, sees that `env` outranked the file).
    // Re-rendered on publish, so this is a clone here too.
    if let Some(reloadable) = ctx.reloadable_config()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("reloadable".into(), reloadable);
    }
    AdminResponse { status: code, body }
}

// --- GET /metrics (Prometheus text; T5.6 expands the metric set) ----------------

async fn metrics(ctx: &Arc<ShardContext>) -> AdminResponse {
    let health = ctx.health();
    // OBS-012: publish the live queue depth before rendering the gauge.
    ctx.metrics().set_queue_depth(health.queue_depth);
    // OBS-020: refresh the active-subscription gauge from the manager.
    {
        let active = ctx.subscriptions.lock().await.plan_count();
        ctx.metrics()
            .set_subscriptions_active(i64::try_from(active).unwrap_or(i64::MAX));
    }
    // OBS-010..OBS-050: the shard's own counter block (the default database).
    let mut text = ctx.metrics().prometheus(health.last_tx_id);
    // SPEC-025 OPS-051: the same series per named namespace, each carrying a
    // `namespace` label so a tenant's load is attributable. Only the series
    // lines are appended — the HELP/TYPE headers were already emitted above,
    // and repeating them for the same metric name is invalid exposition.
    let tenants = ctx.namespaces();
    for ns in &tenants {
        let block = ns
            .metrics()
            .prometheus_in_namespace(ns.name(), ns.last_tx_id());
        for line in block
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            text.push_str(line);
            text.push('\n');
        }
    }
    // SPEC-025 OPS-061: per-tenant quota usage and ceilings, so an operator
    // sees headroom and can alert *before* a tenant starts being refused. An
    // unset ceiling reports 0 ("no limit") next to usage, which is always
    // meaningful.
    if !tenants.is_empty() {
        text.push_str(
            "# HELP fluxum_tenant_memory_bytes Estimated in-memory footprint per tenant.\n\
             # TYPE fluxum_tenant_memory_bytes gauge\n",
        );
        for ns in &tenants {
            let _ = writeln!(
                text,
                "fluxum_tenant_memory_bytes{{namespace=\"{}\"}} {}",
                ns.name(),
                ns.memory_bytes()
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_storage_bytes Durable commit-log bytes per tenant.\n\
             # TYPE fluxum_tenant_storage_bytes gauge\n",
        );
        for ns in &tenants {
            let _ = writeln!(
                text,
                "fluxum_tenant_storage_bytes{{namespace=\"{}\"}} {}",
                ns.name(),
                ns.storage_bytes()
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_subscriptions_active Live subscription plans per tenant.\n\
             # TYPE fluxum_tenant_subscriptions_active gauge\n",
        );
        for ns in &tenants {
            let live = ns.subscriptions().lock().await.plan_count();
            let _ = writeln!(
                text,
                "fluxum_tenant_subscriptions_active{{namespace=\"{}\"}} {live}",
                ns.name(),
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_quota_bytes Configured ceiling per tenant (0 = unlimited).\n\
             # TYPE fluxum_tenant_quota_bytes gauge\n",
        );
        for ns in &tenants {
            let q = *ns.quotas().quotas();
            let _ = writeln!(
                text,
                "fluxum_tenant_quota_bytes{{namespace=\"{}\", quota=\"memory\"}} {}",
                ns.name(),
                q.max_memory_bytes.unwrap_or(0),
            );
            let _ = writeln!(
                text,
                "fluxum_tenant_quota_bytes{{namespace=\"{}\", quota=\"storage\"}} {}",
                ns.name(),
                q.max_storage_bytes.unwrap_or(0),
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_quota_exceeded_total Times a tenant hit a quota (OPS-060).\n\
             # TYPE fluxum_tenant_quota_exceeded_total counter\n",
        );
        for ns in &tenants {
            for quota in crate::quota::Quota::ALL {
                let _ = writeln!(
                    text,
                    "fluxum_tenant_quota_exceeded_total{{namespace=\"{}\", quota=\"{}\"}} {}",
                    ns.name(),
                    quota.as_str(),
                    ns.quotas().exceeded(quota),
                );
            }
        }
    }
    // OBS-030/031: per-table row counts + an estimated MemStore footprint.
    // Lock-free snapshot; the byte figure is a schema-width estimate (the
    // spec's `memstore_bytes` is explicitly an estimate, not exact bytes).
    {
        let shard = health.shard_id;
        let snapshot = ctx.store().snapshot();
        let mut rows_block = String::from(
            "# HELP fluxum_table_rows Committed rows per table.\n\
             # TYPE fluxum_table_rows gauge\n",
        );
        let mut estimated_bytes: u64 = 0;
        for table in ctx.store().table_schemas() {
            let table_id = fluxum_core::store::TableId::of(table.name);
            let rows = snapshot.row_count(table_id).unwrap_or(0);
            let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX);
            let _ = writeln!(
                rows_block,
                "fluxum_table_rows{{shard=\"{shard}\",table=\"{}\"}} {rows_u64}",
                table.name,
            );
            // ~24 bytes per column (tag + inline scalar / small heap) — a
            // coarse gauge for RAM-pressure alerting (OBS-031).
            let width = u64::try_from(table.columns.len()).unwrap_or(0) * 24;
            estimated_bytes = estimated_bytes.saturating_add(rows_u64.saturating_mul(width));
        }
        text.push_str(&rows_block);
        let _ = writeln!(
            text,
            "# HELP fluxum_memstore_bytes Estimated in-memory CommittedState size.\n\
             # TYPE fluxum_memstore_bytes gauge\n\
             fluxum_memstore_bytes{{shard=\"{shard}\"}} {estimated_bytes}",
        );
    }
    // SPEC-017 CT-014/034: transform read-error and signature-verify meters.
    if let Some(engine) = ctx.store().transform_engine() {
        text.push_str(&format!(
            "# HELP fluxum_transform_read_errors_total Read-path transform errors (CT-014).\n\
             # TYPE fluxum_transform_read_errors_total counter\n\
             fluxum_transform_read_errors_total {}\n\
             # HELP fluxum_signature_verify_failures_total Signature verifications that failed (CT-034).\n\
             # TYPE fluxum_signature_verify_failures_total counter\n\
             fluxum_signature_verify_failures_total {}\n",
            engine.read_errors(),
            engine.verify_failures(),
        ));
    }
    // SPEC-020 PLG-030: per-plugin panic/error meters.
    if let Some(registry) = ctx.plugins() {
        let bound = registry.plugins();
        if !bound.is_empty() {
            text.push_str(
                "# HELP fluxum_plugin_panics_total Panics caught per plugin (PLG-030).\n\
                 # TYPE fluxum_plugin_panics_total counter\n",
            );
            for plugin in bound {
                text.push_str(&format!(
                    "fluxum_plugin_panics_total{{plugin=\"{}\"}} {}\n",
                    plugin.name,
                    plugin.state.panics()
                ));
            }
            text.push_str(
                "# HELP fluxum_plugin_errors_total Non-panic plugin errors (PLG-031).\n\
                 # TYPE fluxum_plugin_errors_total counter\n",
            );
            for plugin in bound {
                text.push_str(&format!(
                    "fluxum_plugin_errors_total{{plugin=\"{}\"}} {}\n",
                    plugin.name,
                    plugin.state.errors()
                ));
            }
            // PLG-031: the sidecar breakdown. Emitted only when a sidecar is
            // bound, but then for every reason label — an alert on
            // `rate(...{reason="timeout"})` must not go stale-for-lack-of-series
            // on the run where the sidecar is healthy.
            let sidecars: Vec<_> = bound.iter().filter(|p| p.sidecar.is_some()).collect();
            if !sidecars.is_empty() {
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_errors_total Sidecar Plugin RPC failures \
                     by reason (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_errors_total counter\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    for (reason, count) in stats.by_reason() {
                        text.push_str(&format!(
                            "fluxum_plugin_sidecar_errors_total{{plugin=\"{}\", reason=\"{reason}\"}} {count}\n",
                            plugin.name,
                        ));
                    }
                }
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_calls_total Sidecar Plugin RPC calls \
                     attempted (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_calls_total counter\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    text.push_str(&format!(
                        "fluxum_plugin_sidecar_calls_total{{plugin=\"{}\"}} {}\n",
                        plugin.name,
                        stats.calls()
                    ));
                }
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_breaker_open Whether the sidecar circuit \
                     breaker is currently open (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_breaker_open gauge\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    let open =
                        u8::from(stats.breaker_state() == fluxum_core::plugin::BreakerState::Open);
                    text.push_str(&format!(
                        "fluxum_plugin_sidecar_breaker_open{{plugin=\"{}\"}} {open}\n",
                        plugin.name,
                    ));
                }
            }
        }
    }
    AdminResponse {
        status: 200,
        body: Value::String(text), // the caller serves it as text/plain
    }
}

// --- POST /config/reload (SPEC-025 OPS-040/041) ---------------------------------

/// Re-read the config file + environment and hot-apply the reloadable keys
/// (OPS-040) — the same operation SIGHUP triggers, for platforms and
/// orchestrators where signalling the process is awkward.
///
/// A rejected reload (OPS-041: some frozen key changed) answers `400` with
/// the offending keys named, and the running config is untouched — retrying
/// after fixing the file is safe, and so is ignoring the failure.
///
/// Deliberately allowed while draining: a reload admits no new work and
/// changes no state a drain is waiting on, and raising the log level to
/// debug a slow drain is exactly when an operator needs it most.
fn config_reload(ctx: &Arc<ShardContext>) -> AdminResponse {
    match ctx.reload_config() {
        Ok(changed) => AdminResponse::ok(
            None,
            json!({
                "reloaded": true,
                "changed": changed,
                "reloadable": ctx.reloadable_config(),
            }),
        ),
        Err(e) => AdminResponse::err(400, None, e),
    }
}

// --- POST /drain (SPEC-025 OPS-030) ---------------------------------------------

/// Put the shard into the drain state: stop admitting new work while
/// in-flight transactions finish (`fluxum drain` / a deploy's pre-stop
/// hook).
///
/// This *enters* drain and returns immediately, rather than blocking until
/// the process exits: the caller is an operator or an orchestrator's
/// pre-stop hook that wants a prompt ack, then polls `/health` (which now
/// reports `shutting_down`) to watch the shard quiesce. The quiesce +
/// checkpoint + exit sequence belongs to whoever owns the process — see
/// [`crate::drain`].
fn drain(ctx: &Arc<ShardContext>) -> AdminResponse {
    ctx.begin_drain();
    let health = ctx.health();
    AdminResponse::ok(
        None,
        json!({
            "draining": true,
            "shard": health.shard_id,
            "state": health.state.as_str(),
            "queue_depth": health.queue_depth,
            "last_tx_id": health.last_tx_id,
        }),
    )
}

// --- GET /schema (SPEC-011: tables + reducers + views) -------------------------

/// The `/schema` document's **shape** version (SPEC-011 FR-81), reported as
/// `document_version`.
///
/// `1` is the **module API freeze** (DAG T6.1): from here the `#[fluxum::*]`
/// surface and this document may only change *additively* — new keys, new
/// optional fields — so a generator built against v1 keeps working. A change
/// that removes or repurposes a key is a breaking change and must bump this.
///
/// Distinct from the document's `schema_version`, which is the *module's*
/// declared schema version (MIG-001) and moves when the application migrates
/// (SDK-002).
pub const SCHEMA_DOCUMENT_VERSION: u32 = 1;

/// A table's row-visibility rule as `/schema` JSON (SUB-030..032). The shape
/// is a tagged object rather than a bare string so a rule that carries a
/// column or predicate name stays machine-readable.
fn visibility_json(table: &fluxum_core::schema::TableSchema) -> Value {
    use fluxum_core::schema::VisibilityRule;
    match table.visibility {
        VisibilityRule::PublicAll => json!({ "kind": "public_all" }),
        VisibilityRule::OwnerOnly { owner } => json!({
            "kind": "owner_only",
            "column": table.columns[usize::from(owner)].name,
        }),
        VisibilityRule::ShardLocal => json!({ "kind": "shard_local" }),
        VisibilityRule::Custom(name) => json!({ "kind": "custom", "predicate": name }),
        VisibilityRule::MemberOf { table, key } => json!({
            "kind": "member_of",
            "table": table,
            "key": key,
        }),
    }
}

/// A table's indexes as `/schema` JSON: the access paths a generator can
/// document and the query planner can serve (SPEC-008/018/019).
fn index_json(table: &fluxum_core::schema::TableSchema) -> Value {
    use fluxum_core::schema::IndexSchema;
    let column = |ordinal: &u16| table.columns[usize::from(*ordinal)].name;
    Value::Array(
        table
            .indexes
            .iter()
            .map(|index| match index {
                IndexSchema::BTree { columns } => json!({
                    "kind": "btree",
                    "columns": columns.iter().map(column).collect::<Vec<_>>(),
                }),
                IndexSchema::Spatial { kind, columns } => json!({
                    // `quadtree` | `rtree` (SPEC-008) — the spatial flavour a
                    // generator documents, not a bare "spatial".
                    "kind": format!("{kind:?}").to_lowercase(),
                    "columns": columns.iter().map(column).collect::<Vec<_>>(),
                }),
                IndexSchema::FullText {
                    column: col,
                    language,
                    stop_words,
                    stemming,
                } => json!({
                    "kind": "fulltext",
                    "columns": [column(col)],
                    "language": format!("{language:?}").to_lowercase(),
                    "stop_words": stop_words,
                    "stemming": stemming,
                }),
            })
            .collect::<Vec<_>>(),
    )
}

async fn schema(ctx: &Arc<ShardContext>) -> AdminResponse {
    let manager = ctx.subscriptions.lock().await;
    let tables: Vec<Value> = manager
        .schema()
        .tables()
        .map(|table| {
            let columns: Vec<Value> = table
                .columns
                .iter()
                .map(|c| {
                    let mut column = json!({ "name": c.name, "type": format!("{:?}", c.ty) });
                    // SPEC-017 CT-050/CT-052: the column's transform pipeline
                    // (descriptors — key names only, never key material).
                    if let Some(transforms) =
                        fluxum_core::transform::column_transforms(table.name, c.name)
                    {
                        column["transforms"] =
                            Value::Array(transforms.iter().map(transform_json).collect());
                    }
                    column
                })
                .collect();
            let mut entry = json!({
                "name": table.name,
                "columns": columns,
                "primary_key": table.primary_key,
                "access": format!("{:?}", table.access),
                // SPEC-001: the rest of the table contract a generator needs
                // to emit typed accessors — the auto-inc column (by name, so
                // a generator never has to resolve an ordinal), the unique
                // constraints, the row-visibility rule, and the partition key.
                "auto_inc": table
                    .auto_inc
                    .map(|ordinal| Value::String(table.columns[usize::from(ordinal)].name.to_owned()))
                    .unwrap_or(Value::Null),
                "unique": table
                    .unique
                    .iter()
                    .map(|cols| {
                        Value::Array(
                            cols.iter()
                                .map(|c| Value::String(table.columns[usize::from(*c)].name.to_owned()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>(),
                "visibility": visibility_json(table),
                "partition_by": table
                    .partition_by
                    .map(|ordinal| Value::String(table.columns[usize::from(ordinal)].name.to_owned()))
                    .unwrap_or(Value::Null),
                "indexes": index_json(table),
            });
            // SPEC-019 FTS-050: expose each full-text index — column,
            // analyzer id/config, BM25 params. Never corpus content.
            let fulltext: Vec<Value> = table
                .indexes
                .iter()
                .filter_map(|index| match index {
                    fluxum_core::schema::IndexSchema::FullText {
                        column,
                        language,
                        stop_words,
                        stemming,
                    } => Some(json!({
                        "column": table.columns[usize::from(*column)].name,
                        "language": format!("{language:?}").to_lowercase(),
                        "stop_words": stop_words,
                        "stemming": stemming,
                        "bm25": {
                            "k1": fluxum_core::index::BM25_K1,
                            "b": fluxum_core::index::BM25_B,
                        },
                    })),
                    _ => None,
                })
                .collect();
            if !fulltext.is_empty() {
                entry["fulltext"] = Value::Array(fulltext);
            }
            entry
        })
        .collect();
    // Reducers as full descriptors (SDK-001): name, declared params and
    // return type, whether a client may call it, and its admission rate.
    // Sorted by name so the document is byte-stable across builds — linker
    // order is not, and the freeze gate compares bytes.
    let mut reducer_names: Vec<&str> = ctx.engine.registry().names().collect();
    reducer_names.sort_unstable();
    let reducers: Vec<Value> = reducer_names
        .iter()
        .map(|name| {
            let declaration = ctx.engine.registry().declaration(name);
            let signature = fluxum_core::reducer::signature_of(name);
            json!({
                "name": name,
                // A hand-written ReducerDef registers no signature; it
                // reports no params rather than being absent from the list.
                "params": signature
                    .map(|s| {
                        s.params
                            .iter()
                            .map(|p| json!({ "name": p.name, "type": p.ty }))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                "return_type": signature.map_or("", |s| s.returns),
                "client_callable": declaration.is_none_or(|(callable, _)| callable),
                "max_rate_per_sec": declaration.map_or(0, |(_, rate)| rate),
            })
        })
        .collect();
    let mut views: Vec<&str> = ctx.views.names().collect();
    views.sort_unstable();
    AdminResponse::ok(
        None,
        json!({
            // SPEC-011 SDK-002: the *module's* declared schema version
            // (MIG-001) — what a generated SDK embeds and checks against
            // `InitialData.schema_version` at runtime (SDK-043). It moves
            // when the application's schema migrates.
            "schema_version": fluxum_core::migration::declared_schema_version().unwrap_or(1),
            // The version of this *document's shape*, distinct from the
            // module's. Frozen at 1 by T6.1: the format may only change
            // additively, and a breaking change bumps this.
            "document_version": SCHEMA_DOCUMENT_VERSION,
            "tables": tables,
            "reducers": reducers,
            "views": views,
            // `#[fluxum::procedure]` is not implemented yet; the key is
            // present and empty so a generator can rely on its existence
            // rather than branching on absence once it lands.
            "procedures": Value::Array(Vec::new()),
            // SPEC-018 QP-031: the query surface SDK codegen documents —
            // the extended operator set plus keyset pagination (no OFFSET).
            "query": {
                "operators": ["=", "IN", "BETWEEN", "<", ">", "<=", ">=", "MATCH"],
                "pagination": "keyset: ORDER BY <indexed col> [, <pk>] LIMIT n AFTER (value, pk)",
                // SPEC-019 FTS-052: the full-text surface SDKs render.
                "match": "col MATCH 'term \"a phrase\" prefix*' [ORDER BY SCORE DESC] [SELECT *, SCORE]",
            },
        }),
    )
}

/// One transform descriptor as `/schema` JSON (CT-052): the stable `kind` tag
/// plus its parameters. Key **names** only — never key material.
fn transform_json(descriptor: &fluxum_core::transform::TransformDescriptor) -> Value {
    use fluxum_core::transform::{
        CaseFold, GrantScope, SignedBy, StringForm, TransformDescriptor as D,
    };
    let kind = descriptor.kind();
    match descriptor {
        D::NormalizeMoney { scale, currency } => {
            json!({ "kind": kind, "scale": scale, "currency": currency })
        }
        D::NormalizeDatetime => json!({ "kind": kind }),
        D::NormalizeString { form, case, trim } => json!({
            "kind": kind,
            "form": match form { StringForm::Nfc => "nfc", StringForm::Nfkc => "nfkc" },
            "case": match case {
                CaseFold::None => "none",
                CaseFold::Fold => "fold",
                CaseFold::Lower => "lower",
            },
            "trim": trim,
        }),
        D::Encrypted { key, .. } => json!({ "kind": kind, "scheme": "ecies", "key": key }),
        D::Signed { by, .. } => json!({
            "kind": kind,
            "scheme": "ed25519",
            "by": match by {
                SignedBy::Server => json!("server"),
                SignedBy::IdentityColumn(ord) => json!({ "column": ord }),
            },
        }),
        D::Masked { strategy } => {
            json!({ "kind": kind, "strategy": format!("{strategy:?}").to_lowercase() })
        }
        D::Grant { select } => json!({
            "kind": kind,
            "select": match select {
                GrantScope::Public => json!("public"),
                GrantScope::Owner => json!("owner"),
                GrantScope::ServerPeer => json!("server_peer"),
                GrantScope::Role(role) => json!({ "role": role }),
            },
        }),
    }
}

// --- POST /reducer/:name -------------------------------------------------------

async fn reducer_call(ctx: &Arc<ShardContext>, name: &str, body: &[u8]) -> AdminResponse {
    let (request_id, payload) = match parse_request(body) {
        Ok(pair) => pair,
        Err(e) => return AdminResponse::err(400, None, e),
    };
    // SPEC-025 OPS-030: a reducer call is new work, and the admin surface
    // reaches the engine directly rather than through the session router —
    // so it needs the drain refusal of its own, or `POST /reducer/:name`
    // would keep writing to a shard that is on its way out.
    if ctx.is_draining() {
        return AdminResponse::err(
            503,
            request_id.as_deref(),
            "shard draining for restart; retry",
        );
    }
    // The payload is the reducer's argument array (RPC-051).
    let args = match &payload {
        Value::Null => Vec::new(),
        Value::Array(items) => match items.iter().map(json_to_flux).collect::<Option<Vec<_>>>() {
            Some(args) => args,
            None => {
                return AdminResponse::err(
                    400,
                    request_id.as_deref(),
                    "arguments contain a value outside the FluxValue universe",
                );
            }
        },
        _ => {
            return AdminResponse::err(
                400,
                request_id.as_deref(),
                "payload must be an argument array",
            );
        }
    };

    let caller = ReducerCaller {
        identity: ctx.admin_identity,
        connection_id: ConnectionId::new(0),
        timestamp: Timestamp::now(),
        shard_id: ctx.shard_id,
    };
    match ctx.engine.call(caller, name, args).await {
        Ok(receipt) => {
            ctx.publish_commit(receipt.diff);
            AdminResponse::ok(request_id.as_deref(), json!({ "committed": true }))
        }
        Err(FluxumError::Reducer(message)) => {
            // A business error (RED-060) is a well-formed failure envelope.
            AdminResponse::err(400, request_id.as_deref(), message)
        }
        Err(e) => AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string()),
    }
}

// --- POST /query (one-off read-only SQL) ---------------------------------------

async fn query(ctx: &Arc<ShardContext>, body: &[u8]) -> AdminResponse {
    let (request_id, payload) = match parse_request(body) {
        Ok(pair) => pair,
        Err(e) => return AdminResponse::err(400, None, e),
    };
    let sql = match payload.get("sql").and_then(Value::as_str) {
        Some(sql) => sql.to_owned(),
        None => {
            return AdminResponse::err(400, request_id.as_deref(), "payload.sql (string) required");
        }
    };
    let subscriber = Subscriber::server_peer(ctx.admin_identity); // admin bypasses RLS
    // SPEC-022 RV-021: `AS OF` resolves a historical snapshot.
    let snapshot = match fluxum_core::sql::as_of_point(&sql) {
        Ok(Some(point)) => match ctx.store().snapshot_as_of(point) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                return AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string());
            }
        },
        Ok(None) => ctx.store().snapshot(),
        Err(e) => return AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string()),
    };
    let manager = ctx.subscriptions.lock().await;
    match manager.query_json(subscriber, &sql, &snapshot) {
        Ok(result) => AdminResponse::ok(request_id.as_deref(), result),
        Err(e) => AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string()),
    }
}

// --- POST /query/explain (SPEC-018 QP-051) ---------------------------------------

/// Compile the query and describe its access path — chosen index, probes,
/// bounds, residual conditions, index-served order — without executing it.
async fn query_explain(ctx: &Arc<ShardContext>, body: &[u8]) -> AdminResponse {
    let (request_id, payload) = match parse_request(body) {
        Ok(pair) => pair,
        Err(e) => return AdminResponse::err(400, None, e),
    };
    let sql = match payload.get("sql").and_then(Value::as_str) {
        Some(sql) => sql.to_owned(),
        None => {
            return AdminResponse::err(400, request_id.as_deref(), "payload.sql (string) required");
        }
    };
    let manager = ctx.subscriptions.lock().await;
    match fluxum_core::sql::explain(manager.schema(), &sql) {
        Ok(report) => AdminResponse::ok(request_id.as_deref(), report),
        Err(e) => AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string()),
    }
}

// --- POST /audit (SPEC-025 OPS-020/021) ----------------------------------------

/// Trace who changed a table/row and when, from the commit log (OPS-020).
///
/// Access control (OPS-021): the request must carry a `token` that
/// authenticates to a **server peer** (AUTH-062); a plain client identity is
/// refused `403`. The result is metadata only — `tx_id`, `timestamp`,
/// `caller`, `reducer_name`, `inserted`, `deleted` — never column values, so
/// a masked or field-encrypted column cannot leak plaintext through it.
///
/// Body: `{ token, table, pk?: [pk-col values], tx_from?, tx_to?, time_from?,
/// time_to?, limit? }`.
async fn audit(ctx: &Arc<ShardContext>, body: &[u8]) -> AdminResponse {
    use fluxum_core::commitlog::AuditQuery;

    let (request_id, payload) = match parse_request(body) {
        Ok(pair) => pair,
        Err(e) => return AdminResponse::err(400, None, e),
    };
    let rid = request_id.as_deref();

    // OPS-021: server-peer credential required.
    let Some(token) = payload.get("token").and_then(Value::as_str) else {
        return AdminResponse::err(401, rid, "payload.token (server-peer credential) required");
    };
    match ctx.authenticator.authenticate(token.as_bytes()) {
        Ok(outcome) if outcome.server_peer.is_some() => {}
        Ok(_) => {
            return AdminResponse::err(
                403,
                rid,
                "audit is restricted to server-peer identities (OPS-021)",
            );
        }
        Err(_) => return AdminResponse::err(403, rid, "invalid audit credential"),
    }

    let Some(table_name) = payload.get("table").and_then(Value::as_str) else {
        return AdminResponse::err(400, rid, "payload.table (string) required");
    };
    let Some(table_id) = ctx.store().table_id(table_name) else {
        return AdminResponse::err(404, rid, format!("unknown table `{table_name}`"));
    };
    let Some(schema) = ctx.store().table_schema(table_id) else {
        return AdminResponse::err(404, rid, format!("unknown table `{table_name}`"));
    };

    // Optional row key: PK column values in declaration order, encoded to the
    // table's stable key currency for matching.
    let pk = match payload.get("pk") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => match encode_pk_values(ctx, table_id, schema, values) {
            Ok(pk) => Some(pk),
            Err(e) => return AdminResponse::err(400, rid, e),
        },
        Some(_) => {
            return AdminResponse::err(400, rid, "payload.pk must be an array of key values");
        }
    };

    let query = AuditQuery {
        table: table_id,
        pk,
        tx_from: payload.get("tx_from").and_then(Value::as_u64),
        tx_to: payload.get("tx_to").and_then(Value::as_u64),
        time_from: payload.get("time_from").and_then(Value::as_i64),
        time_to: payload.get("time_to").and_then(Value::as_i64),
        limit: payload
            .get("limit")
            .and_then(Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0),
    };

    match ctx.engine.pipeline().log().audit(schema, &query) {
        Ok(entries) => {
            let rows: Vec<Value> = entries
                .iter()
                .map(|e| {
                    json!({
                        "tx_id": e.tx_id,
                        "timestamp": e.timestamp,
                        "caller": e.caller.to_string(),
                        "reducer_name": e.reducer_name,
                        "inserted": e.inserted,
                        "deleted": e.deleted,
                    })
                })
                .collect();
            AdminResponse::ok(rid, json!({ "count": rows.len(), "entries": rows }))
        }
        Err(e) => AdminResponse::err(status_of(&e), rid, e.to_string()),
    }
}

/// Coerce a JSON array of primary-key values (declaration order) into the
/// table's encoded key. Errors on arity mismatch or an unsupported PK column
/// type.
fn encode_pk_values(
    ctx: &Arc<ShardContext>,
    table_id: fluxum_core::store::TableId,
    schema: &fluxum_core::schema::TableSchema,
    values: &[Value],
) -> Result<fluxum_core::store::PkBytes, String> {
    use fluxum_core::schema::FluxType;
    use fluxum_core::store::RowValue;

    if values.len() != schema.primary_key.len() {
        return Err(format!(
            "pk has {} value(s) but table `{}` has a {}-column primary key",
            values.len(),
            schema.name,
            schema.primary_key.len()
        ));
    }
    let mut pk_values = Vec::with_capacity(values.len());
    for (ordinal, value) in schema.primary_key.iter().zip(values) {
        let column = &schema.columns[usize::from(*ordinal)];
        let bad = || {
            format!(
                "pk value for column `{}` is not a valid {:?}",
                column.name, column.ty
            )
        };
        let rv = match column.ty {
            FluxType::Bool => RowValue::Bool(value.as_bool().ok_or_else(bad)?),
            FluxType::I8 => RowValue::I8(
                int(value)
                    .and_then(|n| i8::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::I16 => RowValue::I16(
                int(value)
                    .and_then(|n| i16::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::I32 => RowValue::I32(
                int(value)
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::I64 => RowValue::I64(int(value).ok_or_else(bad)?),
            FluxType::U8 => RowValue::U8(
                uint(value)
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::U16 => RowValue::U16(
                uint(value)
                    .and_then(|n| u16::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::U32 => RowValue::U32(
                uint(value)
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(bad)?,
            ),
            FluxType::U64 => RowValue::U64(uint(value).ok_or_else(bad)?),
            FluxType::Str => RowValue::Str(value.as_str().ok_or_else(bad)?.to_owned()),
            other => {
                return Err(format!(
                    "audit row-key matching does not support a `{other:?}` primary-key column; \
                     filter by tx/time range instead"
                ));
            }
        };
        pk_values.push(rv);
    }
    ctx.store()
        .snapshot()
        .encode_pk(table_id, &pk_values)
        .map_err(|e| e.to_string())
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64()
}

fn uint(v: &Value) -> Option<u64> {
    v.as_u64()
}

// --- GET /view/:name -----------------------------------------------------------

async fn view(ctx: &Arc<ShardContext>, name: &str) -> AdminResponse {
    if !ctx.views.contains(name) {
        return AdminResponse::err(404, None, format!("unknown view `{name}`"));
    }
    let snapshot = ctx.store().snapshot();
    match ctx.views.dispatch(name, &snapshot, ctx.shard_id, &[]) {
        Ok(result) => AdminResponse::ok(None, result),
        Err(e) => AdminResponse::err(status_of(&e), None, e.to_string()),
    }
}

// --- helpers -------------------------------------------------------------------

/// Parse an RPC-051 request envelope; a bare (non-enveloped) JSON body is
/// accepted too, with its whole value taken as the payload.
fn parse_request(body: &[u8]) -> Result<(Option<String>, Value), String> {
    if body.is_empty() {
        return Ok((None, Value::Null));
    }
    let value: Value = serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    match value {
        Value::Object(mut map) if map.contains_key("payload") => {
            let request_id = map
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let payload = map.remove("payload").unwrap_or(Value::Null);
            Ok((request_id, payload))
        }
        other => Ok((None, other)),
    }
}

/// Convert a JSON value to a [`FluxValue`] reducer argument; `None` for a
/// value outside the RPC-010 universe (e.g. an object).
fn json_to_flux(value: &Value) -> Option<FluxValue> {
    match value {
        Value::Null => Some(FluxValue::Null),
        Value::Bool(b) => Some(FluxValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(FluxValue::I64(i))
            } else {
                n.as_f64().map(FluxValue::F64)
            }
        }
        Value::String(s) => Some(FluxValue::Str(s.clone())),
        Value::Array(items) => items
            .iter()
            .map(json_to_flux)
            .collect::<Option<Vec<_>>>()
            .map(FluxValue::Array),
        Value::Object(_) => None,
    }
}

/// The HTTP status for a [`FluxumError`], derived from its SPEC-028 catalog
/// entry (§7): total via [`FluxumError::to_wire`].
fn status_of(e: &FluxumError) -> u16 {
    fluxum_protocol::codes::entry(e.to_wire().code).map_or(500, |entry| entry.http_status)
}
