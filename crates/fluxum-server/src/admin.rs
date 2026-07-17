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
//! | GET  | `/plugins`       | active plugins + adopted seams (SPEC-020 PLG-060; never secrets) |
//! | POST | `/plugins/:name/disable` | hot circuit-break without restart (PLG-061); `/enable` reverts |
//!
//! Every response uses the RPC-052 envelope
//! (`{ "success": bool, "request_id"?, "payload"|"error" }`); the paths are
//! unversioned. Admin calls run under the server admin identity (RLS bypass,
//! AUTH-062) — this surface is for trusted operators.

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
        ("GET", ["metrics"]) => metrics(ctx),
        ("GET", ["schema"]) => schema(ctx).await,
        ("POST", ["reducer", name]) => reducer_call(ctx, name, body).await,
        ("POST", ["query"]) => query(ctx, body).await,
        ("GET", ["view", name]) => view(ctx, name).await,
        ("GET", ["plugins"]) => plugins(ctx),
        ("POST", ["plugins", name, "disable"]) => plugin_set_disabled(ctx, name, true),
        ("POST", ["plugins", name, "enable"]) => plugin_set_disabled(ctx, name, false),
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
        AdminResponse::ok(
            None,
            json!({ "plugin": name, "disabled": disabled }),
        )
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
            | ["reducer", _]
            | ["view", _]
            | ["plugins"]
            | ["plugins", _, "disable" | "enable"]
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
    let health = ctx.health(); // atomics only — no storage lock
    // Envelope-free per the spec's `/health` shape, plus the per-shard detail.
    AdminResponse {
        status: 200,
        body: json!({
            "status": "ok",
            "shards": 1,
            "shard": {
                "id": health.shard_id,
                "state": "ready",
                "last_tx_id": health.last_tx_id,
            }
        }),
    }
}

// --- GET /metrics (Prometheus text; T5.6 expands the metric set) ----------------

fn metrics(ctx: &Arc<ShardContext>) -> AdminResponse {
    let health = ctx.health();
    let text = format!(
        "# HELP fluxum_shard_last_tx_id Last committed transaction id per shard.\n\
         # TYPE fluxum_shard_last_tx_id gauge\n\
         fluxum_shard_last_tx_id{{shard=\"{shard}\"}} {tx}\n\
         # HELP fluxum_up Whether the shard is serving.\n\
         # TYPE fluxum_up gauge\n\
         fluxum_up{{shard=\"{shard}\"}} 1\n",
        shard = health.shard_id,
        tx = health.last_tx_id,
    );
    let mut text = text;
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
        }
    }
    AdminResponse {
        status: 200,
        body: Value::String(text), // the caller serves it as text/plain
    }
}

// --- GET /schema (SPEC-011: tables + reducers + views) -------------------------

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
            json!({
                "name": table.name,
                "columns": columns,
                "primary_key": table.primary_key,
                "access": format!("{:?}", table.access),
            })
        })
        .collect();
    let mut reducers: Vec<&str> = ctx.engine.registry().names().collect();
    reducers.sort_unstable();
    let mut views: Vec<&str> = ctx.views.names().collect();
    views.sort_unstable();
    AdminResponse::ok(
        None,
        json!({ "tables": tables, "reducers": reducers, "views": views }),
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
    let snapshot = ctx.store().snapshot();
    let manager = ctx.subscriptions.lock().await;
    match manager.query_json(subscriber, &sql, &snapshot) {
        Ok(result) => AdminResponse::ok(request_id.as_deref(), result),
        Err(e) => AdminResponse::err(status_of(&e), request_id.as_deref(), e.to_string()),
    }
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
