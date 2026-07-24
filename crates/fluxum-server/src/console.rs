//! The built-in admin web console (SPEC-024 DEV-030..032; FR-137): a
//! self-contained single-page UI served on the HTTP admin port, so a
//! single-binary deployment gets a data browser, a read-only query panel, a
//! live diff viewer, the reducer log stream, and the `/metrics` + `/schema`
//! views with nothing extra to deploy.
//!
//! | Route | Purpose |
//! |-------|---------|
//! | `GET /console`       | the console shell — one HTML file, all CSS/JS inline |
//! | `GET /console/state` | gate posture + auth verdict the UI boots from |
//! | `GET /console/watch` | committed-diff NDJSON stream (`?table=` filters) |
//!
//! The shell is static and data-free (it is a login screen until the gate
//! passes), so it is served under the SEC-054 *network* guard only. Every
//! data-bearing console route enforces [`crate::admin::check_console_access`]:
//! the SEC-054 guard first, then — outside the `development` profile — a
//! server-peer operator credential even from loopback (DEV-031). The data
//! tabs call the existing admin endpoints (`/schema`, `/query`, `/metrics`,
//! `/logs`), which keep their own SPEC-026 SEC-054 posture.
//!
//! Lock discipline (DEV-031): the watch stream reads the commit broadcast and
//! the static table catalog only — no storage locks, no subscription-manager
//! mutex — so an open (even stuck) console can never violate the RPC-053
//! `/health` latency budget.
//!
//! The stream serves the *default* database's commits; per-namespace watch
//! rides the same transport once a UI need appears (the broadcast already
//! exists per namespace, SPEC-025 OPS-050).

use serde_json::{Value, json};

use fluxum_core::store::{MemStore, Row, TxDiff};
use fluxum_core::subscription::row_value_to_json;
use fluxum_core::txn::CommitMeta;

/// The console shell: one self-contained HTML document (inline CSS + JS, no
/// external requests — the page's CSP forbids any non-self origin).
pub const CONSOLE_HTML: &str = include_str!("console.html");

/// A console route (under `GET /console`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// `GET /console` — serve the shell.
    Shell,
    /// `GET /console/state` — the boot document.
    State,
    /// `GET /console/watch[?table=..]` — the live diff stream, with the
    /// parsed table filter (`None` = all tables).
    Watch(Option<String>),
    /// Anything else under `/console/` — 404.
    NotFound,
}

/// Whether `path` belongs to the console (routes it, query string and all).
pub fn is_console_path(path: &str) -> bool {
    let bare = path.split('?').next().unwrap_or("");
    bare == "/console" || bare == "/console/" || bare.starts_with("/console/")
}

/// Resolve a console path to its [`Route`].
pub fn route(path: &str) -> Route {
    let (bare, query) = match path.split_once('?') {
        Some((bare, query)) => (bare, Some(query)),
        None => (path, None),
    };
    match bare {
        "/console" | "/console/" => Route::Shell,
        "/console/state" => Route::State,
        "/console/watch" => Route::Watch(table_filter(query)),
        _ => Route::NotFound,
    }
}

/// The `table=` filter from a watch query string. Table names are plain
/// identifiers (SPEC-001), so no percent-decoding is involved; an empty
/// value means no filter.
fn table_filter(query: Option<&str>) -> Option<String> {
    query?
        .split('&')
        .find_map(|pair| pair.strip_prefix("table="))
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

/// The `GET /console/state` payload: what the UI needs to decide whether to
/// show the login screen. `console_open` is the DEV-031 gate posture
/// (`true` only under the `development` profile); `authed` is whether the
/// presented credential authenticates to a server peer.
pub fn state_json(console_open: bool, authed: bool) -> Value {
    json!({
        "console_open": console_open,
        "authed": authed,
    })
}

/// Render one committed transaction as a watch-stream NDJSON line, or `None`
/// when no table survives the filter (nothing to send). Rows use the same
/// JSON currency as `POST /query` ([`row_value_to_json`]), so the browser
/// sees one value shape everywhere.
///
/// Reads only the static table catalog ([`MemStore::table_schema`]) — never
/// a storage lock (DEV-031).
pub fn render_commit(
    store: &MemStore,
    diff: &TxDiff,
    meta: &CommitMeta,
    table_filter: Option<&str>,
) -> Option<String> {
    let mut tables = Vec::new();
    for table in &diff.tables {
        // A table absent from the catalog (dropped since the commit) has no
        // column names to render — skip it rather than guess.
        let Some(schema) = store.table_schema(table.table_id) else {
            continue;
        };
        if table_filter.is_some_and(|want| want != schema.name) {
            continue;
        }
        let row_json = |row: &Row| -> Value {
            let mut object = serde_json::Map::new();
            for (column, value) in schema.columns.iter().zip(row.values()) {
                object.insert(column.name.to_owned(), row_value_to_json(value));
            }
            Value::Object(object)
        };
        let inserts: Vec<Value> = table.inserts.iter().map(row_json).collect();
        let deletes: Vec<Value> = table
            .deletes
            .iter()
            .map(|(_pk, row)| row_json(row))
            .collect();
        if inserts.is_empty() && deletes.is_empty() {
            continue;
        }
        tables.push(json!({
            "table": schema.name,
            "inserts": inserts,
            "deletes": deletes,
        }));
    }
    if tables.is_empty() {
        return None;
    }
    // Provenance rides every event (RPC-033 parity): the empty reducer name
    // of an anonymous/internal commit is rendered as `null`.
    let line = json!({
        "tx_id": diff.tx_id,
        "reducer": (!meta.reducer_name.is_empty()).then_some(meta.reducer_name.as_str()),
        "caller": meta.caller.to_string(),
        "tables": tables,
    });
    Some(line.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn routes_resolve() {
        assert_eq!(route("/console"), Route::Shell);
        assert_eq!(route("/console/"), Route::Shell);
        assert_eq!(route("/console/state"), Route::State);
        assert_eq!(route("/console/watch"), Route::Watch(None));
        assert_eq!(
            route("/console/watch?table=Chat"),
            Route::Watch(Some("Chat".to_owned()))
        );
        assert_eq!(
            route("/console/watch?follow=1&table=Chat"),
            Route::Watch(Some("Chat".to_owned()))
        );
        assert_eq!(route("/console/watch?table="), Route::Watch(None));
        assert_eq!(route("/console/nope"), Route::NotFound);
        assert!(is_console_path("/console"));
        assert!(is_console_path("/console/watch?table=Chat"));
        assert!(!is_console_path("/consoles"));
        assert!(!is_console_path("/health"));
    }

    #[test]
    fn the_shell_is_self_contained() {
        // No external fetch surface: everything inline, and the page must
        // never reference another origin (the CSP also enforces this at
        // runtime; this pins it at build time).
        assert!(CONSOLE_HTML.contains("Content-Security-Policy"));
        for forbidden in ["http://", "https://", "//cdn", "@import"] {
            assert!(
                !CONSOLE_HTML.contains(forbidden),
                "console.html must be self-contained; found `{forbidden}`"
            );
        }
    }
}
