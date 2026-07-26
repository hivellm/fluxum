//! Read-only PostgreSQL wire-protocol endpoint (SPEC-027, PGW-0xx): let
//! standard SQL/BI tools (psql, Grafana, Metabase, Superset) run point-in-time
//! reads over Fluxum's existing compiled-query surface, with **no** bespoke
//! connector — and no write path.
//!
//! # What it is, and is not
//!
//! Every `SELECT` is compiled and executed by the *same* engine the admin
//! `POST /query` uses ([`SubscriptionManager::query_json`]): the index-aware
//! planner (SPEC-018), RLS (SPEC-005), and column masking (SPEC-017) all apply,
//! keyed on the connection's authenticated identity. Writes never exist here —
//! `INSERT`/`UPDATE`/`DELETE`/DDL are rejected (PGW-002); mutation stays in
//! reducers. Transaction-control and session `SET`s are accepted as harmless
//! no-ops so tools connect, but they enable nothing. Schema discovery
//! (`information_schema.tables`/`.columns`, PGW-003) is reflected from the live
//! [`Schema`]. `AS OF` (SPEC-022) rides through for point-in-time BI reads
//! (PGW-005).
//!
//! # Security posture (PGW-004)
//!
//! Disabled by default and gated behind auth: the connection password IS a
//! Fluxum token, resolved by the [`Authenticator`] into the per-connection
//! identity. The wire is plaintext — SSLRequest is declined (`N`) — so the
//! default bind is loopback; a remote deployment fronts it with a TLS proxy.
//! Both `PGW-002` read-only enforcement and `PGW-004` identity/RLS are covered
//! by [`crate::pgwire`]'s wire tests.

pub mod proto;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;

use fluxum_core::schema::FluxType;
use fluxum_core::subscription::Subscriber;

use crate::ShardContext;
use proto::{FieldDesc, Frontend, Out};

/// Tuning knobs for the pgwire listener.
#[derive(Debug, Clone)]
pub struct PgOptions {
    /// Idle read timeout; a connection silent this long is closed.
    pub idle_timeout: Option<Duration>,
    /// Shared socket hardening (keepalive, buffers) — the same knobs the
    /// FluxRPC transports use.
    pub socket: crate::sock::SocketOptions,
}

impl Default for PgOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Some(Duration::from_secs(300)),
            socket: crate::sock::SocketOptions::default(),
        }
    }
}

/// A bound pgwire listener.
pub struct PgServer {
    /// The bound address (the resolved port when configured as `:0`).
    pub local_addr: SocketAddr,
    shutdown: Arc<Notify>,
}

impl PgServer {
    /// Stop accepting and end the accept loop (established connections drain).
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// Bind and start the pgwire listener. Returns once bound; the accept loop
/// runs in a spawned task.
///
/// # Errors
/// The bind failing.
pub async fn serve(
    ctx: Arc<ShardContext>,
    addr: impl tokio::net::ToSocketAddrs,
    options: PgOptions,
) -> io::Result<PgServer> {
    let listener = crate::sock::bind(addr, options.socket).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(Notify::new());
    let accept_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = accept_shutdown.notified() => break,
                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(e) => {
                            tracing::debug!(target: "fluxum::pgwire", error = %e, "accept failed");
                            continue;
                        }
                    };
                    // OPS-030: admit nobody new while draining; the client
                    // reconnects onto the restarted process.
                    if ctx.is_draining() {
                        drop(stream);
                        continue;
                    }
                    let _ = stream.set_nodelay(true);
                    crate::sock::apply_keepalive(&stream, options.socket.tcp_keepalive);
                    let conn_ctx = Arc::clone(&ctx);
                    tokio::spawn(async move {
                        if let Err(e) = drive(conn_ctx, stream, options.idle_timeout).await {
                            tracing::debug!(target: "fluxum::pgwire", ip = %peer.ip(), error = %e,
                                "pgwire connection ended");
                        }
                    });
                }
            }
        }
    });
    Ok(PgServer {
        local_addr,
        shutdown,
    })
}

/// Drive one connection: startup handshake → auth → the message loop.
async fn drive<S>(ctx: Arc<ShardContext>, mut stream: S, idle: Option<Duration>) -> io::Result<()>
where
    S: tokio::io::AsyncRead + AsyncWrite + Unpin,
{
    // 1. Startup: decline SSL/GSS (plaintext), drop cancel requests, accept 3.0.
    let _params = loop {
        match proto::read_startup(&mut stream).await? {
            Frontend::SslRequest | Frontend::GssRequest => {
                stream.write_all(b"N").await?; // no encryption; client may retry plaintext
                stream.flush().await?;
            }
            Frontend::CancelRequest { .. } => return Ok(()), // nothing to cancel
            Frontend::Startup { params } => break params,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected startup message {other:?}"),
                ));
            }
        }
    };

    // 2. Cleartext password = a Fluxum token → the per-connection identity.
    let mut out = Out::new();
    out.auth_cleartext();
    write(&mut stream, out).await?;
    let token = match read_msg(&mut stream, idle).await? {
        Frontend::Password(mut bytes) => {
            // The password is NUL-terminated on the wire; drop the trailing NUL.
            if bytes.last() == Some(&0) {
                bytes.pop();
            }
            bytes
        }
        Frontend::Terminate => return Ok(()),
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected a password message, got {other:?}"),
            ));
        }
    };
    let subscriber = match ctx.authenticator.authenticate(&token) {
        Ok(outcome) => {
            if outcome.bypass_rls {
                Subscriber::server_peer(outcome.identity)
            } else {
                Subscriber::client_with_roles(outcome.identity, outcome.roles)
            }
        }
        Err(_) => {
            let mut out = Out::new();
            out.error(
                "FATAL",
                "28P01",
                "authentication failed: password must be a valid Fluxum token",
            );
            write(&mut stream, out).await?;
            return Ok(());
        }
    };

    // 3. Authentication succeeded — announce readiness.
    let mut out = Out::new();
    out.auth_ok()
        .param_status("server_version", "14.0 (Fluxum read-only)")
        .param_status("server_encoding", "UTF8")
        .param_status("client_encoding", "UTF8")
        .param_status("DateStyle", "ISO, MDY")
        .param_status("TimeZone", "UTC")
        .param_status("integer_datetimes", "on")
        .param_status("standard_conforming_strings", "on")
        .backend_key(0, 0)
        .ready_for_query(b'I');
    write(&mut stream, out).await?;

    // 4. The query loop. Extended-protocol prepared statements/portals are held
    //    per connection; this endpoint supports only zero-parameter statements.
    let mut prepared: HashMap<String, String> = HashMap::new();
    let mut portals: HashMap<String, String> = HashMap::new();
    loop {
        let msg = match read_msg(&mut stream, idle).await {
            Ok(msg) => msg,
            // A clean client hangup (EOF) ends the loop without an error.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        match msg {
            Frontend::Terminate => return Ok(()),
            Frontend::Query(sql) => {
                let mut out = Out::new();
                emit_query(&ctx, &subscriber, &sql, &mut out).await;
                out.ready_for_query(b'I');
                write(&mut stream, out).await?;
            }
            Frontend::Parse { name, sql, .. } => {
                prepared.insert(name, sql);
                let mut out = Out::new();
                out.parse_complete();
                write(&mut stream, out).await?;
            }
            Frontend::Bind {
                portal,
                statement,
                param_count,
            } => {
                let mut out = Out::new();
                if param_count != 0 {
                    out.error(
                        "ERROR",
                        "0A000",
                        "bind parameters are not supported by the read-only pgwire endpoint",
                    );
                } else if let Some(sql) = prepared.get(&statement) {
                    portals.insert(portal, sql.clone());
                    out.bind_complete();
                } else {
                    out.error(
                        "ERROR",
                        "26000",
                        &format!("prepared statement `{statement}` does not exist"),
                    );
                }
                write(&mut stream, out).await?;
            }
            Frontend::Describe { kind, name } => {
                let sql = if kind == b'S' {
                    prepared.get(&name)
                } else {
                    portals.get(&name)
                };
                let mut out = Out::new();
                match sql {
                    Some(sql) => {
                        if kind == b'S' {
                            out.parameter_description();
                        }
                        match resolve_columns(&ctx, sql).await {
                            Some(fields) => {
                                out.row_description(&fields);
                            }
                            None => {
                                out.no_data();
                            }
                        }
                    }
                    None => {
                        out.error("ERROR", "26000", "no such prepared statement or portal");
                    }
                }
                write(&mut stream, out).await?;
            }
            Frontend::Execute { portal, .. } => {
                let mut out = Out::new();
                match portals.get(&portal).cloned() {
                    Some(sql) => emit_execute(&ctx, &subscriber, &sql, &mut out).await,
                    None => {
                        out.error(
                            "ERROR",
                            "34000",
                            &format!("portal `{portal}` does not exist"),
                        );
                    }
                }
                write(&mut stream, out).await?;
            }
            Frontend::Close { name, .. } => {
                prepared.remove(&name);
                portals.remove(&name);
                let mut out = Out::new();
                out.close_complete();
                write(&mut stream, out).await?;
            }
            Frontend::Sync => {
                let mut out = Out::new();
                out.ready_for_query(b'I');
                write(&mut stream, out).await?;
            }
            Frontend::Flush => {} // responses are already flushed per write
            Frontend::Unknown(tag) => {
                let mut out = Out::new();
                out.error(
                    "ERROR",
                    "08P01",
                    &format!("unsupported protocol message `{}`", tag as char),
                )
                .ready_for_query(b'I');
                write(&mut stream, out).await?;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected message in query loop: {other:?}"),
                ));
            }
        }
    }
}

/// The classification of an incoming statement (read-only surface).
enum Stmt {
    /// Empty/whitespace-only.
    Empty,
    /// A read: pass to the compiled-query engine or the catalog reflector.
    Read,
    /// A catalog/introspection query answered from the schema (PGW-003).
    Catalog(Catalog),
    /// A harmless session/transaction no-op accepted with this command tag.
    NoOp(&'static str),
    /// A rejected write/DDL — the SQLSTATE + message to return (PGW-002).
    Reject(&'static str),
}

/// The recognized catalog/introspection shapes.
enum Catalog {
    /// `information_schema.tables`.
    Tables,
    /// `information_schema.columns`.
    Columns,
    /// `SELECT version()`.
    Version,
    /// `SELECT current_schema()` / `current_schema`.
    CurrentSchema,
    /// `SHOW <name>` — a single-setting row.
    Show(String),
}

/// Classify a statement by its leading keyword and any catalog markers.
fn classify(sql: &str) -> Stmt {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Stmt::Empty;
    }
    let lower = trimmed.to_ascii_lowercase();
    // Catalog/introspection first — these never compile as Fluxum SELECTs.
    if lower.contains("information_schema.tables") {
        return Stmt::Catalog(Catalog::Tables);
    }
    if lower.contains("information_schema.columns") {
        return Stmt::Catalog(Catalog::Columns);
    }
    if lower.contains("version()") {
        return Stmt::Catalog(Catalog::Version);
    }
    if lower.contains("current_schema") {
        return Stmt::Catalog(Catalog::CurrentSchema);
    }
    let verb: String = lower
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    match verb.as_str() {
        "show" => Stmt::Catalog(Catalog::Show(
            trimmed[verb.len()..]
                .trim()
                .trim_end_matches(';')
                .to_owned(),
        )),
        "set" => Stmt::NoOp("SET"),
        "reset" => Stmt::NoOp("RESET"),
        "begin" | "start" => Stmt::NoOp("BEGIN"),
        "commit" | "end" => Stmt::NoOp("COMMIT"),
        "rollback" | "abort" => Stmt::NoOp("ROLLBACK"),
        "discard" => Stmt::NoOp("DISCARD ALL"),
        "insert" | "update" | "delete" | "merge" | "upsert" | "truncate" | "copy" => {
            Stmt::Reject("this endpoint is read-only (PGW-002); writes go through reducers")
        }
        "create" | "drop" | "alter" | "grant" | "revoke" | "comment" | "reindex" | "vacuum"
        | "analyze" | "cluster" | "lock" | "call" | "do" => {
            Stmt::Reject("this endpoint is read-only (PGW-002): DDL and procedures are not allowed")
        }
        _ => Stmt::Read,
    }
}

/// Emit a full simple-query reply (RowDescription + DataRows + CommandComplete,
/// or a no-op/error) into `out`. The caller appends `ReadyForQuery`.
async fn emit_query(ctx: &Arc<ShardContext>, subscriber: &Subscriber, sql: &str, out: &mut Out) {
    match run(ctx, subscriber, sql).await {
        Ok(QueryReply::Rows { fields, rows, tag }) => {
            out.row_description(&fields);
            for row in &rows {
                out.data_row(row);
            }
            out.command_complete(&tag);
        }
        Ok(QueryReply::Empty) => {
            out.empty_query();
        }
        Ok(QueryReply::NoOp(tag)) => {
            out.command_complete(tag);
        }
        Err(reply) => {
            out.error(&reply.severity, &reply.code, &reply.message);
        }
    }
}

/// Emit an extended-protocol `Execute` reply (DataRows + CommandComplete, no
/// RowDescription — the client already got it from `Describe`).
async fn emit_execute(ctx: &Arc<ShardContext>, subscriber: &Subscriber, sql: &str, out: &mut Out) {
    match run(ctx, subscriber, sql).await {
        Ok(QueryReply::Rows { rows, tag, .. }) => {
            for row in &rows {
                out.data_row(row);
            }
            out.command_complete(&tag);
        }
        Ok(QueryReply::Empty) => {
            out.empty_query();
        }
        Ok(QueryReply::NoOp(tag)) => {
            out.command_complete(tag);
        }
        Err(reply) => {
            out.error(&reply.severity, &reply.code, &reply.message);
        }
    }
}

/// A successful query reply.
enum QueryReply {
    /// A result set.
    Rows {
        /// Column descriptors.
        fields: Vec<FieldDesc>,
        /// Rows of text-format cells (`None` = SQL NULL).
        rows: Vec<Vec<Option<Vec<u8>>>>,
        /// The `CommandComplete` tag (e.g. `SELECT 5`).
        tag: String,
    },
    /// An empty query string.
    Empty,
    /// A no-op accepted with this tag.
    NoOp(&'static str),
}

/// A PG error reply.
struct ErrReply {
    severity: String,
    code: String,
    message: String,
}

impl ErrReply {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            severity: "ERROR".into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Run a statement: catalog, no-op, read, or rejection.
async fn run(
    ctx: &Arc<ShardContext>,
    subscriber: &Subscriber,
    sql: &str,
) -> Result<QueryReply, ErrReply> {
    match classify(sql) {
        Stmt::Empty => Ok(QueryReply::Empty),
        Stmt::NoOp(tag) => Ok(QueryReply::NoOp(tag)),
        Stmt::Reject(msg) => Err(ErrReply::new("25006", msg)),
        Stmt::Catalog(cat) => catalog(ctx, cat).await,
        Stmt::Read => read(ctx, subscriber, sql).await,
    }
}

/// Execute a `SELECT` through the shared compiled-query engine (SPEC-018),
/// honoring RLS + masking for `subscriber` (PGW-001/004), and translate the
/// JSON result to PG rows.
async fn read(
    ctx: &Arc<ShardContext>,
    subscriber: &Subscriber,
    sql: &str,
) -> Result<QueryReply, ErrReply> {
    // SPEC-022 RV-021: `AS OF` resolves a historical snapshot (PGW-005).
    let snapshot = match fluxum_core::sql::as_of_point(sql) {
        Ok(Some(point)) => ctx
            .store()
            .snapshot_as_of(point)
            .map_err(|e| flux_err(&e))?,
        Ok(None) => ctx.store().snapshot(),
        Err(e) => return Err(flux_err(&e)),
    };
    let manager = ctx.subscriptions.lock().await;
    let result = manager
        .query_json(subscriber.clone(), sql, &snapshot)
        .map_err(|e| flux_err(&e))?;
    let table_name = result.get("table").and_then(|v| v.as_str()).unwrap_or("");
    let table = manager.schema().table(table_name);
    let columns: Vec<String> = result
        .get("columns")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let fields: Vec<FieldDesc> = columns
        .iter()
        .map(|name| {
            let (type_oid, type_size) = column_type(table, name);
            FieldDesc {
                name: name.clone(),
                type_oid,
                type_size,
            }
        })
        .collect();
    let mut rows = Vec::new();
    if let Some(json_rows) = result.get("rows").and_then(|v| v.as_array()) {
        for row in json_rows {
            let cells: Vec<Option<Vec<u8>>> = columns
                .iter()
                .map(|c| row.get(c).and_then(json_to_text))
                .collect();
            rows.push(cells);
        }
    }
    let tag = format!("SELECT {}", rows.len());
    Ok(QueryReply::Rows { fields, rows, tag })
}

/// Resolve just the column descriptors of a statement (extended `Describe`),
/// compiling but never executing. `None` for a no-op/empty statement (NoData).
async fn resolve_columns(ctx: &Arc<ShardContext>, sql: &str) -> Option<Vec<FieldDesc>> {
    match classify(sql) {
        Stmt::Empty | Stmt::NoOp(_) | Stmt::Reject(_) => None,
        Stmt::Catalog(cat) => Some(catalog_fields(&cat)),
        Stmt::Read => {
            let manager = ctx.subscriptions.lock().await;
            let plan = fluxum_core::sql::compile(manager.schema(), sql).ok()?;
            let target = plan.table_ids.first().copied()?;
            let table = manager
                .schema()
                .tables()
                .find(|t| fluxum_core::store::TableId::of(t.name) == target)?;
            let mut fields: Vec<FieldDesc> = table
                .columns
                .iter()
                .map(|c| {
                    let (type_oid, type_size) = flux_oid(&c.ty);
                    FieldDesc {
                        name: c.name.to_owned(),
                        type_oid,
                        type_size,
                    }
                })
                .collect();
            if plan.select_score {
                fields.push(FieldDesc {
                    name: "_score".into(),
                    type_oid: proto::OID_FLOAT8,
                    type_size: 8,
                });
            }
            Some(fields)
        }
    }
}

/// Map a result column to its PG type. Columns present in the table use their
/// `FluxType`; synthetic projections (`_score`, `<col>_verified`) and unknowns
/// fall back to sensible defaults.
fn column_type(table: Option<&'static fluxum_core::schema::TableSchema>, name: &str) -> (i32, i16) {
    if name == "_score" {
        return (proto::OID_FLOAT8, 8);
    }
    if name.ends_with("_verified") {
        return (proto::OID_BOOL, 1);
    }
    table
        .and_then(|t| t.columns.iter().find(|c| c.name == name))
        .map_or((proto::OID_TEXT, -1), |c| flux_oid(&c.ty))
}

/// `FluxType` → `(pg OID, type size)`. Types whose JSON text representation
/// already parses under the chosen OID are mapped honestly; the rest fall back
/// to `text` (the JSON scalar renders as a readable string).
fn flux_oid(ty: &FluxType) -> (i32, i16) {
    match ty {
        FluxType::Bool => (proto::OID_BOOL, 1),
        FluxType::I8 | FluxType::I16 => (proto::OID_INT2, 2),
        FluxType::I32 | FluxType::U8 | FluxType::U16 => (proto::OID_INT4, 4),
        // i64/u32/entity ids/timestamps render as an integer (micros for a
        // timestamp) whose text parses as int8.
        FluxType::I64 | FluxType::U32 | FluxType::EntityId | FluxType::Timestamp => {
            (proto::OID_INT8, 8)
        }
        FluxType::U64 => (proto::OID_NUMERIC, -1),
        FluxType::F32 => (proto::OID_FLOAT4, 4),
        FluxType::F64 => (proto::OID_FLOAT8, 8),
        FluxType::Decimal => (proto::OID_NUMERIC, -1),
        FluxType::Option(inner) => flux_oid(inner),
        // Everything else (Str, Bytes as hex, Identity/ConnectionId/Blob as
        // strings, List/Enum/Struct/CrdtText as JSON text) is text.
        _ => (proto::OID_TEXT, -1),
    }
}

/// Render a JSON scalar to PG text-format bytes. `null` → SQL NULL; a bool
/// becomes `t`/`f`; numbers/strings pass through; arrays/objects serialize to
/// compact JSON.
fn json_to_text(v: &serde_json::Value) -> Option<Vec<u8>> {
    use serde_json::Value;
    match v {
        Value::Null => None,
        Value::Bool(true) => Some(b"t".to_vec()),
        Value::Bool(false) => Some(b"f".to_vec()),
        Value::Number(n) => Some(n.to_string().into_bytes()),
        Value::String(s) => Some(s.clone().into_bytes()),
        other => Some(other.to_string().into_bytes()),
    }
}

// --- catalog / information_schema (PGW-003) ---------------------------------------

/// The `RowDescription` fields for a catalog result.
fn catalog_fields(cat: &Catalog) -> Vec<FieldDesc> {
    let text = |name: &str| FieldDesc {
        name: name.to_owned(),
        type_oid: proto::OID_TEXT,
        type_size: -1,
    };
    match cat {
        Catalog::Tables => vec![
            text("table_catalog"),
            text("table_schema"),
            text("table_name"),
            text("table_type"),
        ],
        Catalog::Columns => vec![
            text("table_catalog"),
            text("table_schema"),
            text("table_name"),
            text("column_name"),
            FieldDesc {
                name: "ordinal_position".into(),
                type_oid: proto::OID_INT4,
                type_size: 4,
            },
            text("data_type"),
            text("is_nullable"),
            text("udt_name"),
        ],
        Catalog::Version => vec![text("version")],
        Catalog::CurrentSchema => vec![text("current_schema")],
        Catalog::Show(name) => vec![text(if name.is_empty() { "setting" } else { name })],
    }
}

/// Answer a catalog query from the live schema (PGW-003).
async fn catalog(ctx: &Arc<ShardContext>, cat: Catalog) -> Result<QueryReply, ErrReply> {
    let fields = catalog_fields(&cat);
    let cell = |s: &str| Some(s.as_bytes().to_vec());
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    match cat {
        Catalog::Version => rows.push(vec![cell(&format!(
            "PostgreSQL 14.0 (Fluxum {} read-only)",
            env!("CARGO_PKG_VERSION")
        ))]),
        Catalog::CurrentSchema => rows.push(vec![cell("public")]),
        Catalog::Show(name) => rows.push(vec![cell(show_value(&name))]),
        Catalog::Tables => {
            let manager = ctx.subscriptions.lock().await;
            for table in manager.schema().tables() {
                if !table.access.is_client_visible() {
                    continue;
                }
                rows.push(vec![
                    cell("fluxum"),
                    cell("public"),
                    cell(table.name),
                    cell("BASE TABLE"),
                ]);
            }
            // Views are relations for discovery, even though they carry no
            // column schema (they are JSON-returning functions).
            for name in ctx.views.names() {
                rows.push(vec![
                    cell("fluxum"),
                    cell("public"),
                    cell(name),
                    cell("VIEW"),
                ]);
            }
        }
        Catalog::Columns => {
            let manager = ctx.subscriptions.lock().await;
            for table in manager.schema().tables() {
                if !table.access.is_client_visible() {
                    continue;
                }
                for (i, col) in table.columns.iter().enumerate() {
                    rows.push(vec![
                        cell("fluxum"),
                        cell("public"),
                        cell(table.name),
                        cell(col.name),
                        Some((i + 1).to_string().into_bytes()),
                        cell(pg_type_name(&col.ty)),
                        cell(if matches!(col.ty, FluxType::Option(_)) {
                            "YES"
                        } else {
                            "NO"
                        }),
                        cell(pg_type_name(&col.ty)),
                    ]);
                }
            }
        }
    }
    let tag = format!("SELECT {}", rows.len());
    Ok(QueryReply::Rows { fields, rows, tag })
}

/// Best-effort value for `SHOW <name>` — enough for tools' connect probes.
fn show_value(name: &str) -> &'static str {
    match name.to_ascii_lowercase().as_str() {
        "server_version" => "14.0",
        "transaction_isolation" | "default_transaction_isolation" => "read committed",
        "transaction_read_only" | "default_transaction_read_only" => "on",
        "client_encoding" | "server_encoding" => "UTF8",
        "standard_conforming_strings" => "on",
        "timezone" => "UTC",
        _ => "",
    }
}

/// A readable `information_schema` type name for a `FluxType`.
fn pg_type_name(ty: &FluxType) -> &'static str {
    match ty {
        FluxType::Bool => "boolean",
        FluxType::I8 | FluxType::I16 => "smallint",
        FluxType::I32 | FluxType::U8 | FluxType::U16 => "integer",
        FluxType::I64 | FluxType::U32 | FluxType::EntityId | FluxType::Timestamp => "bigint",
        FluxType::U64 | FluxType::Decimal => "numeric",
        FluxType::F32 => "real",
        FluxType::F64 => "double precision",
        FluxType::Option(inner) => pg_type_name(inner),
        _ => "text",
    }
}

// --- small IO helpers -------------------------------------------------------------

/// Map a `FluxumError` to a PG error reply, reusing its wire code as the
/// message and a best-effort SQLSTATE.
fn flux_err(e: &fluxum_core::FluxumError) -> ErrReply {
    // Compile errors surface as syntax errors; a missing table as undefined
    // table; everything else as a generic data exception.
    let msg = e.to_string();
    let code = if msg.contains("not public") || msg.contains("unknown table") {
        "42P01" // undefined_table
    } else if msg.contains("SQL") || msg.contains("parse") || msg.contains("expected") {
        "42601" // syntax_error
    } else {
        "22000" // data_exception
    };
    ErrReply::new(code, msg)
}

/// Read one message with the optional idle timeout applied.
async fn read_msg<S>(stream: &mut S, idle: Option<Duration>) -> io::Result<Frontend>
where
    S: tokio::io::AsyncRead + Unpin,
{
    match idle {
        Some(d) => tokio::time::timeout(d, proto::read_message(stream))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pgwire idle timeout"))?,
        None => proto::read_message(stream).await,
    }
}

/// Flush a built response to the client.
async fn write<S>(stream: &mut S, out: Out) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let bytes = out.into_bytes();
    if !bytes.is_empty() {
        stream.write_all(&bytes).await?;
        stream.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use fluxum_core::schema::{ColumnSchema, TableAccess, TableSchema, VisibilityRule};

    static I64_TY: FluxType = FluxType::I64;
    static COLS: &[ColumnSchema] = &[ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    }];
    static TBL: TableSchema = TableSchema {
        name: "T",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    #[test]
    fn classify_routes_reads_catalog_noops_and_rejections() {
        assert!(matches!(classify("   "), Stmt::Empty));
        assert!(matches!(classify("SELECT * FROM Item"), Stmt::Read));
        assert!(matches!(classify("with x as (..) select 1"), Stmt::Read));
        // Catalog markers win regardless of leading verb.
        assert!(matches!(
            classify("SELECT table_name FROM information_schema.tables"),
            Stmt::Catalog(Catalog::Tables)
        ));
        assert!(matches!(
            classify("select * from information_schema.columns"),
            Stmt::Catalog(Catalog::Columns)
        ));
        assert!(matches!(
            classify("SELECT version()"),
            Stmt::Catalog(Catalog::Version)
        ));
        assert!(matches!(
            classify("select current_schema()"),
            Stmt::Catalog(Catalog::CurrentSchema)
        ));
        assert!(matches!(
            classify("SHOW server_version;"),
            Stmt::Catalog(Catalog::Show(s)) if s == "server_version"
        ));
        // No-ops (accepted, enable nothing).
        for (sql, tag) in [
            ("SET x = 1", "SET"),
            ("reset all", "RESET"),
            ("BEGIN", "BEGIN"),
            ("start transaction", "BEGIN"),
            ("commit", "COMMIT"),
            ("END", "COMMIT"),
            ("rollback", "ROLLBACK"),
            ("abort", "ROLLBACK"),
            ("discard all", "DISCARD ALL"),
        ] {
            assert!(matches!(classify(sql), Stmt::NoOp(t) if t == tag), "{sql}");
        }
        // Rejections (read-only).
        for sql in [
            "INSERT INTO Item VALUES (1)",
            "UPDATE Item SET x=1",
            "DELETE FROM Item",
            "TRUNCATE Item",
            "CREATE TABLE t (id int)",
            "DROP TABLE Item",
            "ALTER TABLE Item ADD c int",
            "GRANT ALL",
        ] {
            assert!(matches!(classify(sql), Stmt::Reject(_)), "{sql}");
        }
    }

    #[test]
    fn flux_type_maps_to_honest_oids() {
        assert_eq!(flux_oid(&FluxType::Bool), (proto::OID_BOOL, 1));
        assert_eq!(flux_oid(&FluxType::I16), (proto::OID_INT2, 2));
        assert_eq!(flux_oid(&FluxType::I32), (proto::OID_INT4, 4));
        assert_eq!(flux_oid(&FluxType::U16), (proto::OID_INT4, 4));
        assert_eq!(flux_oid(&FluxType::I64), (proto::OID_INT8, 8));
        assert_eq!(flux_oid(&FluxType::Timestamp), (proto::OID_INT8, 8));
        assert_eq!(flux_oid(&FluxType::EntityId), (proto::OID_INT8, 8));
        assert_eq!(flux_oid(&FluxType::U64), (proto::OID_NUMERIC, -1));
        assert_eq!(flux_oid(&FluxType::Decimal), (proto::OID_NUMERIC, -1));
        assert_eq!(flux_oid(&FluxType::F32), (proto::OID_FLOAT4, 4));
        assert_eq!(flux_oid(&FluxType::F64), (proto::OID_FLOAT8, 8));
        assert_eq!(flux_oid(&FluxType::Str), (proto::OID_TEXT, -1));
        assert_eq!(flux_oid(&FluxType::Bytes), (proto::OID_TEXT, -1));
        // Option unwraps to the inner type's OID.
        assert_eq!(flux_oid(&FluxType::Option(&I64_TY)), (proto::OID_INT8, 8));
    }

    #[test]
    fn column_type_handles_synthetic_and_schema_columns() {
        assert_eq!(column_type(None, "_score"), (proto::OID_FLOAT8, 8));
        assert_eq!(column_type(None, "name_verified"), (proto::OID_BOOL, 1));
        assert_eq!(column_type(None, "whatever"), (proto::OID_TEXT, -1));
        assert_eq!(column_type(Some(&TBL), "id"), (proto::OID_NUMERIC, -1));
        // A column absent from the table falls back to text.
        assert_eq!(column_type(Some(&TBL), "ghost"), (proto::OID_TEXT, -1));
    }

    #[test]
    fn json_scalars_render_to_pg_text() {
        use serde_json::json;
        assert_eq!(json_to_text(&json!(null)), None);
        assert_eq!(json_to_text(&json!(true)), Some(b"t".to_vec()));
        assert_eq!(json_to_text(&json!(false)), Some(b"f".to_vec()));
        assert_eq!(json_to_text(&json!(42)), Some(b"42".to_vec()));
        assert_eq!(json_to_text(&json!("hi")), Some(b"hi".to_vec()));
        assert_eq!(json_to_text(&json!([1, 2])), Some(b"[1,2]".to_vec()));
    }

    #[test]
    fn catalog_fields_and_type_names() {
        assert_eq!(catalog_fields(&Catalog::Tables).len(), 4);
        assert_eq!(catalog_fields(&Catalog::Columns).len(), 8);
        assert_eq!(catalog_fields(&Catalog::Version).len(), 1);
        assert_eq!(catalog_fields(&Catalog::CurrentSchema).len(), 1);
        assert_eq!(catalog_fields(&Catalog::Show(String::new())).len(), 1);
        assert_eq!(pg_type_name(&FluxType::Bool), "boolean");
        assert_eq!(pg_type_name(&FluxType::I64), "bigint");
        assert_eq!(pg_type_name(&FluxType::U64), "numeric");
        assert_eq!(pg_type_name(&FluxType::F64), "double precision");
        assert_eq!(pg_type_name(&FluxType::Option(&I64_TY)), "bigint");
        assert_eq!(pg_type_name(&FluxType::Str), "text");
    }

    #[test]
    fn show_values_cover_the_connect_probes() {
        assert_eq!(show_value("server_version"), "14.0");
        assert_eq!(show_value("transaction_isolation"), "read committed");
        assert_eq!(show_value("transaction_read_only"), "on");
        assert_eq!(show_value("TimeZone"), "UTC");
        assert_eq!(show_value("nonsense"), "");
    }

    #[test]
    fn flux_error_maps_to_a_sqlstate() {
        use fluxum_core::FluxumError;
        assert_eq!(
            flux_err(&FluxumError::Storage("unknown table X".into())).code,
            "42P01"
        );
        assert_eq!(
            flux_err(&FluxumError::Storage("table X is not public".into())).code,
            "42P01"
        );
        assert_eq!(
            flux_err(&FluxumError::Storage("parse error near '('".into())).code,
            "42601"
        );
        assert_eq!(
            flux_err(&FluxumError::Storage("expected a table name".into())).code,
            "42601"
        );
        assert_eq!(
            flux_err(&FluxumError::Storage("a random failure".into())).code,
            "22000"
        );
    }
}
