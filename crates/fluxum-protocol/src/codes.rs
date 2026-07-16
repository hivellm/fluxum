//! The stable machine-readable error catalog (SPEC-028 ERR).
//!
//! Every client-visible error is one [`CatalogEntry`] in [`CATALOG`] — the
//! **single source** all emission paths, the generated `docs/errors.md`
//! reference, and future SDK enums derive from. Codes are `u16` values in
//! per-subsystem ranges; codes and names are never reused, renumbered, or
//! renamed once released (retiring an error retires its number permanently).
//!
//! | Range | Prefix | Subsystem |
//! |---|---|---|
//! | 1000–1999 | `PROTO_` | protocol / framing |
//! | 2000–2999 | `AUTH_` | authentication / authorization |
//! | 3000–3999 | `SQL_` / `TXN_` | SQL, constraints, transactions |
//! | 4000–4999 | `SCHEMA_` | schema / migration / transform |
//! | 5000–5999 | `REDUCER_` | reducers / scheduling |
//! | 6000–6999 | `SUB_` | subscriptions |
//! | 7000–7999 | `STORAGE_` | storage / durability / tiering |
//! | 8000–8999 | `CLUSTER_` | sharding / replication |
//! | 9000–9999 | `SYS_` | system / limits |
//!
//! Entries in the SQL range carry a PostgreSQL-compatible five-character
//! SQLSTATE; every entry declares default severity, retryability, the exact
//! `details` keys its emissions may attach, and the HTTP status the
//! Streamable HTTP transport derives (SPEC-028 §7).

/// How the connection behaves after the error frame (SPEC-028 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// The connection stays open; the request failed.
    Error,
    /// The server closes the connection immediately after this frame.
    Fatal,
}

/// One catalog entry (SPEC-028 §6): the registry row every emission,
/// document, and SDK enum derives from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogEntry {
    /// Stable wire code (never reused or renumbered).
    pub code: u16,
    /// Stable canonical `SCREAMING_SNAKE` name (never renamed).
    pub name: &'static str,
    /// Default severity.
    pub severity: Severity,
    /// Default retryability (SPEC-028 §4).
    pub retryable: bool,
    /// PostgreSQL-compatible SQLSTATE — SQL range (3xxx) only.
    pub sqlstate: Option<&'static str>,
    /// The exact `details` keys emissions of this code may attach.
    pub details_keys: &'static [&'static str],
    /// Human-readable message template (docs; wire messages may specialize).
    pub message_template: &'static str,
    /// HTTP status the Streamable HTTP transport derives (SPEC-028 §7).
    pub http_status: u16,
}

// --- 1xxx PROTO_ ------------------------------------------------------------

/// 1000 — malformed frame, envelope, or message body (RPC-001).
pub const PROTO_MALFORMED: u16 = 1000;
/// 1003 — frame exceeds `max_frame_bytes` (RPC-061).
pub const PROTO_FRAME_TOO_LARGE: u16 = 1003;
/// 1004 — idle connection closed (RPC-060).
pub const PROTO_IDLE_TIMEOUT: u16 = 1004;
/// 1005 — unknown or expired Streamable-HTTP session (RPC-007).
pub const PROTO_SESSION_EXPIRED: u16 = 1005;

// --- 2xxx AUTH_ -------------------------------------------------------------

/// 2000 — message before a successful `Authenticate` (RPC-020).
pub const AUTH_REQUIRED: u16 = 2000;
/// 2001 — token validation failed (AUTH-020/021).
pub const AUTH_FAILED: u16 = 2001;

// --- 3xxx SQL_ / TXN_ -------------------------------------------------------

/// 3000 — SQL lexical or syntactic error.
pub const SQL_MALFORMED: u16 = 3000;
/// 3001 — query names a table that does not exist.
pub const SQL_UNKNOWN_TABLE: u16 = 3001;
/// 3002 — query names a column that does not exist.
pub const SQL_UNKNOWN_COLUMN: u16 = 3002;
/// 3003 — syntactically valid but unsupported construct (SUB-011 subset).
pub const SQL_UNSUPPORTED: u16 = 3003;
/// 3004 — literal does not inhabit the column type.
pub const SQL_TYPE_MISMATCH: u16 = 3004;
/// 3010 — spatial predicate on a table without a spatial index (SPX-022).
pub const SQL_NO_SPATIAL_INDEX: u16 = 3010;
/// 3100 — `#[unique]` constraint violation (TXN-041).
pub const SQL_UNIQUE_VIOLATION: u16 = 3100;
/// 3200 — transaction conflict (reserved; single-writer has none today).
pub const TXN_CONFLICT: u16 = 3200;

// --- 4xxx SCHEMA_ -----------------------------------------------------------

/// 4000 — invalid assembled schema or transform declaration (DM-040/CT-051).
pub const SCHEMA_INVALID: u16 = 4000;

// --- 5xxx REDUCER_ ----------------------------------------------------------

/// 5000 — unknown reducer name (RED-006).
pub const REDUCER_UNKNOWN: u16 = 5000;
/// 5001 — the reducer body returned `Err(message)` (RED-060).
pub const REDUCER_USER_ERROR: u16 = 5001;
/// 5002 — the reducer body panicked; transaction rolled back (RED-061).
pub const REDUCER_PANIC: u16 = 5002;
/// 5003 — argument count or type mismatch (RED-001).
pub const REDUCER_BAD_ARGS: u16 = 5003;
/// 5004 — client call to a schedule-only reducer (RED-025).
pub const REDUCER_SCHEDULE_ONLY: u16 = 5004;
/// 5005 — per-(Identity, reducer) token bucket exhausted (RED-050).
pub const REDUCER_RATE_LIMITED: u16 = 5005;
/// 5006 — unknown `#[fluxum::view]` name (RED-030).
pub const REDUCER_UNKNOWN_VIEW: u16 = 5006;

// --- 6xxx SUB_ --------------------------------------------------------------

/// 6000 — subscription admission cap exceeded (SUB-044).
pub const SUB_LIMIT_EXCEEDED: u16 = 6000;
/// 6001 — subscription to a non-public table (SUB-005).
pub const SUB_TABLE_NOT_PUBLIC: u16 = 6001;

// --- 7xxx STORAGE_ ----------------------------------------------------------

/// 7000 — internal storage failure (commit log, pages, checkpoints).
pub const STORAGE_INTERNAL: u16 = 7000;
/// 7001 — cold page failed CRC verification on fault-in (TIER-062).
pub const STORAGE_PAGE_CORRUPT: u16 = 7001;
/// 7002 — buffer pool has no evictable frame (TIER-003).
pub const STORAGE_BUFFER_POOL_EXHAUSTED: u16 = 7002;
/// 7003 — spatial index still rebuilding after recovery (SPX-023).
pub const STORAGE_SPATIAL_REBUILDING: u16 = 7003;
/// 7004 — full-text index still rebuilding after recovery (FTS-022).
pub const STORAGE_FULLTEXT_REBUILDING: u16 = 7004;

// --- 8xxx CLUSTER_ ----------------------------------------------------------

/// 8000 — shard unavailable (writer queue full, TXN-011).
pub const CLUSTER_SHARD_UNAVAILABLE: u16 = 8000;
/// 8001 — entity mid-handoff between shards (SPEC-007; reserved).
pub const CLUSTER_ENTITY_HANDOFF: u16 = 8001;

// --- 9xxx SYS_ --------------------------------------------------------------

/// 9000 — unexpected internal error.
pub const SYS_INTERNAL: u16 = 9000;
/// 9001 — shard-wide admission cap exceeded (RED-052).
pub const SYS_OVERLOADED: u16 = 9001;

/// The catalog (SPEC-028 §6): one row per released error, sorted by code.
pub const CATALOG: &[CatalogEntry] = &[
    CatalogEntry {
        code: PROTO_MALFORMED,
        name: "PROTO_MALFORMED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "malformed frame or message body",
        http_status: 400,
    },
    CatalogEntry {
        code: PROTO_FRAME_TOO_LARGE,
        name: "PROTO_FRAME_TOO_LARGE",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["declared_len", "max_frame_bytes"],
        message_template: "frame exceeds the configured maximum size",
        http_status: 413,
    },
    CatalogEntry {
        code: PROTO_IDLE_TIMEOUT,
        name: "PROTO_IDLE_TIMEOUT",
        severity: Severity::Fatal,
        retryable: true,
        sqlstate: None,
        details_keys: &[],
        message_template: "connection idle beyond the configured timeout",
        http_status: 408,
    },
    CatalogEntry {
        code: PROTO_SESSION_EXPIRED,
        name: "PROTO_SESSION_EXPIRED",
        severity: Severity::Fatal,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "unknown or expired session token",
        http_status: 404,
    },
    CatalogEntry {
        code: AUTH_REQUIRED,
        name: "AUTH_REQUIRED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "authenticate before sending this message",
        http_status: 401,
    },
    CatalogEntry {
        code: AUTH_FAILED,
        name: "AUTH_FAILED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "token validation failed",
        http_status: 401,
    },
    CatalogEntry {
        code: SQL_MALFORMED,
        name: "SQL_MALFORMED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("42601"),
        details_keys: &[],
        message_template: "SQL lexical or syntactic error",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_UNKNOWN_TABLE,
        name: "SQL_UNKNOWN_TABLE",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("42P01"),
        details_keys: &["table"],
        message_template: "query names a table that does not exist",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_UNKNOWN_COLUMN,
        name: "SQL_UNKNOWN_COLUMN",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("42703"),
        details_keys: &["table", "column"],
        message_template: "query names a column that does not exist",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_UNSUPPORTED,
        name: "SQL_UNSUPPORTED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("0A000"),
        details_keys: &[],
        message_template: "construct outside the supported SQL subset",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_TYPE_MISMATCH,
        name: "SQL_TYPE_MISMATCH",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("42804"),
        details_keys: &[],
        message_template: "literal does not inhabit the column type",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_NO_SPATIAL_INDEX,
        name: "SQL_NO_SPATIAL_INDEX",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("0A000"),
        details_keys: &["table"],
        message_template: "spatial predicate on a table without a spatial index",
        http_status: 400,
    },
    CatalogEntry {
        code: SQL_UNIQUE_VIOLATION,
        name: "SQL_UNIQUE_VIOLATION",
        severity: Severity::Error,
        retryable: false,
        sqlstate: Some("23505"),
        details_keys: &["table", "constraint"],
        message_template: "unique constraint violation",
        http_status: 400,
    },
    CatalogEntry {
        code: TXN_CONFLICT,
        name: "TXN_CONFLICT",
        severity: Severity::Error,
        retryable: true,
        sqlstate: Some("40001"),
        details_keys: &[],
        message_template: "transaction conflict; safe to retry",
        http_status: 409,
    },
    CatalogEntry {
        code: SCHEMA_INVALID,
        name: "SCHEMA_INVALID",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "invalid schema or transform declaration",
        http_status: 400,
    },
    CatalogEntry {
        code: REDUCER_UNKNOWN,
        name: "REDUCER_UNKNOWN",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["reducer"],
        message_template: "unknown reducer name",
        http_status: 404,
    },
    CatalogEntry {
        code: REDUCER_USER_ERROR,
        name: "REDUCER_USER_ERROR",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["app_code"],
        message_template: "the reducer rejected the call",
        http_status: 400,
    },
    CatalogEntry {
        code: REDUCER_PANIC,
        name: "REDUCER_PANIC",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["reducer"],
        message_template: "the reducer panicked; the transaction was rolled back",
        http_status: 500,
    },
    CatalogEntry {
        code: REDUCER_BAD_ARGS,
        name: "REDUCER_BAD_ARGS",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["reducer"],
        message_template: "argument count or type mismatch",
        http_status: 400,
    },
    CatalogEntry {
        code: REDUCER_SCHEDULE_ONLY,
        name: "REDUCER_SCHEDULE_ONLY",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["reducer"],
        message_template: "reducer is schedule-only and not client-callable",
        http_status: 403,
    },
    CatalogEntry {
        code: REDUCER_RATE_LIMITED,
        name: "REDUCER_RATE_LIMITED",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &["reducer"],
        message_template: "per-caller rate limit exceeded",
        http_status: 429,
    },
    CatalogEntry {
        code: REDUCER_UNKNOWN_VIEW,
        name: "REDUCER_UNKNOWN_VIEW",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["view"],
        message_template: "unknown view name",
        http_status: 404,
    },
    CatalogEntry {
        code: SUB_LIMIT_EXCEEDED,
        name: "SUB_LIMIT_EXCEEDED",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["limit"],
        message_template: "subscription admission cap exceeded",
        http_status: 429,
    },
    CatalogEntry {
        code: SUB_TABLE_NOT_PUBLIC,
        name: "SUB_TABLE_NOT_PUBLIC",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["table"],
        message_template: "table is not visible to client subscriptions",
        http_status: 403,
    },
    CatalogEntry {
        code: STORAGE_INTERNAL,
        name: "STORAGE_INTERNAL",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "internal storage failure",
        http_status: 500,
    },
    CatalogEntry {
        code: STORAGE_PAGE_CORRUPT,
        name: "STORAGE_PAGE_CORRUPT",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &["shard_id", "table_id", "page_id"],
        message_template: "cold page failed integrity verification",
        http_status: 500,
    },
    CatalogEntry {
        code: STORAGE_BUFFER_POOL_EXHAUSTED,
        name: "STORAGE_BUFFER_POOL_EXHAUSTED",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &["capacity"],
        message_template: "buffer pool has no evictable frame; retry shortly",
        http_status: 503,
    },
    CatalogEntry {
        code: STORAGE_SPATIAL_REBUILDING,
        name: "STORAGE_SPATIAL_REBUILDING",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &["table"],
        message_template: "spatial index is rebuilding after recovery; retry shortly",
        http_status: 503,
    },
    CatalogEntry {
        code: STORAGE_FULLTEXT_REBUILDING,
        name: "STORAGE_FULLTEXT_REBUILDING",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &["table"],
        message_template: "full-text index is rebuilding after recovery; retry shortly",
        http_status: 503,
    },
    CatalogEntry {
        code: CLUSTER_SHARD_UNAVAILABLE,
        name: "CLUSTER_SHARD_UNAVAILABLE",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &["shard_id"],
        message_template: "shard temporarily unavailable; retry shortly",
        http_status: 503,
    },
    CatalogEntry {
        code: CLUSTER_ENTITY_HANDOFF,
        name: "CLUSTER_ENTITY_HANDOFF",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &[],
        message_template: "entity is mid-handoff between shards; retry shortly",
        http_status: 503,
    },
    CatalogEntry {
        code: SYS_INTERNAL,
        name: "SYS_INTERNAL",
        severity: Severity::Error,
        retryable: false,
        sqlstate: None,
        details_keys: &[],
        message_template: "unexpected internal error",
        http_status: 500,
    },
    CatalogEntry {
        code: SYS_OVERLOADED,
        name: "SYS_OVERLOADED",
        severity: Severity::Error,
        retryable: true,
        sqlstate: None,
        details_keys: &[],
        message_template: "shard-wide admission cap exceeded; retry shortly",
        http_status: 503,
    },
];

/// The catalog entry of `code`, if released.
pub fn entry(code: u16) -> Option<&'static CatalogEntry> {
    CATALOG.iter().find(|e| e.code == code)
}

/// The subsystem range `(low, high, prefix)` a code belongs to.
pub const fn subsystem(code: u16) -> Option<(u16, u16, &'static str)> {
    match code {
        1000..=1999 => Some((1000, 1999, "PROTO_")),
        2000..=2999 => Some((2000, 2999, "AUTH_")),
        3000..=3999 => Some((3000, 3999, "SQL_/TXN_")),
        4000..=4999 => Some((4000, 4999, "SCHEMA_")),
        5000..=5999 => Some((5000, 5999, "REDUCER_")),
        6000..=6999 => Some((6000, 6999, "SUB_")),
        7000..=7999 => Some((7000, 7999, "STORAGE_")),
        8000..=8999 => Some((8000, 8999, "CLUSTER_")),
        9000..=9999 => Some((9000, 9999, "SYS_")),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// SPEC-028 §2: unique codes, unique names, every code inside its
    /// subsystem range, names matching the range prefix, sorted by code.
    #[test]
    fn catalog_is_unique_ranged_and_sorted() {
        let mut codes = HashSet::new();
        let mut names = HashSet::new();
        let mut last = 0u16;
        for e in CATALOG {
            assert!(codes.insert(e.code), "duplicate code {}", e.code);
            assert!(names.insert(e.name), "duplicate name {}", e.name);
            assert!(e.code > last, "catalog not sorted at {}", e.code);
            last = e.code;
            let (low, high, prefix) = subsystem(e.code)
                .unwrap_or_else(|| panic!("{} outside every subsystem range", e.code));
            assert!(e.code >= low && e.code <= high);
            let prefix_ok = prefix
                .split('/')
                .any(|p| e.name.starts_with(p.trim_end_matches('_')));
            assert!(prefix_ok, "{} does not match prefix {prefix}", e.name);
            assert!(!e.message_template.is_empty());
        }
    }

    /// SPEC-028 §5: SQL-range entries carry a SQLSTATE; others never do.
    #[test]
    fn sqlstate_only_in_the_sql_range() {
        for e in CATALOG {
            let in_sql_range = (3000..=3999).contains(&e.code);
            assert_eq!(
                e.sqlstate.is_some(),
                in_sql_range,
                "{} sqlstate presence mismatch",
                e.name
            );
            if let Some(state) = e.sqlstate {
                assert_eq!(state.len(), 5, "{} SQLSTATE must be 5 chars", e.name);
            }
        }
    }

    /// Spec-pinned codes never move (SPEC-028 §2 stability rule).
    #[test]
    fn spec_pinned_codes_are_stable() {
        let pinned = [
            (1003, "PROTO_FRAME_TOO_LARGE"),
            (1004, "PROTO_IDLE_TIMEOUT"),
            (1005, "PROTO_SESSION_EXPIRED"),
            (2000, "AUTH_REQUIRED"),
            (3001, "SQL_UNKNOWN_TABLE"),
            (3100, "SQL_UNIQUE_VIOLATION"),
            (3200, "TXN_CONFLICT"),
            (5001, "REDUCER_USER_ERROR"),
            (5002, "REDUCER_PANIC"),
            (5005, "REDUCER_RATE_LIMITED"),
            (7002, "STORAGE_BUFFER_POOL_EXHAUSTED"),
            (8001, "CLUSTER_ENTITY_HANDOFF"),
        ];
        for (code, name) in pinned {
            let e = entry(code).unwrap_or_else(|| panic!("pinned code {code} missing"));
            assert_eq!(e.name, name);
        }
        // Retry semantics pinned by SPEC-028 §4 scenarios.
        assert!(entry(REDUCER_RATE_LIMITED).unwrap().retryable);
        assert!(entry(CLUSTER_ENTITY_HANDOFF).unwrap().retryable);
        assert!(entry(STORAGE_BUFFER_POOL_EXHAUSTED).unwrap().retryable);
        // HTTP derivations pinned by SPEC-028 §7 scenarios.
        assert_eq!(entry(PROTO_SESSION_EXPIRED).unwrap().http_status, 404);
        assert_eq!(entry(PROTO_FRAME_TOO_LARGE).unwrap().http_status, 413);
        assert_eq!(entry(SYS_INTERNAL).unwrap().http_status, 500);
    }

    #[test]
    fn entry_lookup_misses_unreleased_codes() {
        assert!(entry(1).is_none());
        assert!(entry(400).is_none(), "HTTP-era codes are retired");
        assert!(entry(9999).is_none());
    }
}

/// Render the error-reference document (`docs/errors.md`) from the catalog
/// (SPEC-028 §6): exactly one section per entry — code, name, message
/// template, details keys, retry semantics, SQLSTATE, and HTTP status. The
/// golden test below keeps the committed file in sync.
pub fn render_errors_md() -> String {
    let mut out = String::from(
        "# Error reference\n\n\
         Generated from the SPEC-028 catalog (`fluxum-protocol/src/codes.rs`) — do not edit by\n\
         hand. Regenerate with `FLUXUM_REGEN_DOCS=1 cargo test -p fluxum-protocol --lib`.\n",
    );
    for entry in CATALOG {
        use std::fmt::Write;
        let _ = write!(
            out,
            "\n## {} `{}`\n\n{}\n\n\
             - severity: `{}` · retryable: `{}` · HTTP {}\n",
            entry.code,
            entry.name,
            entry.message_template,
            match entry.severity {
                Severity::Error => "error",
                Severity::Fatal => "fatal",
            },
            entry.retryable,
            entry.http_status,
        );
        if let Some(state) = entry.sqlstate {
            let _ = writeln!(out, "- SQLSTATE: `{state}`");
        }
        if !entry.details_keys.is_empty() {
            let _ = writeln!(out, "- details keys: `{}`", entry.details_keys.join("`, `"));
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod docs_tests {
    use super::*;

    /// SPEC-028 §6: `docs/errors.md` is generated from the registry and
    /// committed; this golden test keeps them in sync (set
    /// `FLUXUM_REGEN_DOCS=1` to rewrite).
    #[test]
    fn errors_md_matches_the_catalog() {
        let rendered = render_errors_md();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/errors.md");
        if std::env::var_os("FLUXUM_REGEN_DOCS").is_some() {
            std::fs::write(&path, &rendered).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&path)
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert_eq!(
            committed, rendered,
            "docs/errors.md is out of sync with the catalog: run \
             FLUXUM_REGEN_DOCS=1 cargo test -p fluxum-protocol --lib"
        );
        // One section per entry, exactly (SPEC-028 scenario).
        assert_eq!(
            rendered.matches("\n## ").count(),
            CATALOG.len(),
            "one section per catalog entry"
        );
    }
}
