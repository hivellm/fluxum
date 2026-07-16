//! Shared error type for the whole workspace.
//!
//! Every fallible fluxum API returns [`Result<T>`] with [`FluxumError`], one
//! variant per subsystem so callers can match on the failure domain without
//! string inspection.

/// Workspace-wide result alias.
pub type Result<T> = std::result::Result<T, FluxumError>;

/// The one error type shared by every fluxum crate.
///
/// Variants map to subsystems; construction helpers keep call sites terse.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FluxumError {
    /// Invalid or inconsistent configuration (file, env override, or derived).
    #[error("config error: {0}")]
    Config(String),

    /// Configuration file could not be parsed as YAML.
    #[error("config parse error: {0}")]
    ConfigParse(#[from] serde_yaml::Error),

    /// Underlying I/O failure (file system, sockets).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Storage engine failure (commit log, pages, checkpoints).
    #[error("storage error: {0}")]
    Storage(String),

    /// The buffer pool has no evictable frame — every frame is pinned or in
    /// flight. The faulting operation fails (and its transaction rolls back
    /// per SPEC-002 STG-006) rather than allocate past the memory budget
    /// (SPEC-015 TIER-003).
    #[error(
        "buffer pool exhausted: no evictable frame among {capacity} \
         (all pinned or faulting); the operation must roll back (TIER-003)"
    )]
    BufferPoolExhausted {
        /// Pool capacity in frames.
        capacity: usize,
    },

    /// A cold-tier page failed its CRC32C integrity check on fault-in and
    /// was not served (SPEC-015 TIER-021/TIER-032/TIER-062).
    #[error(
        "page corrupt: shard {shard_id}, table {table_id:#010x}, page \
         {page_id} failed CRC32C verification on fault-in (TIER-062)"
    )]
    PageCorrupt {
        /// Owning shard.
        shard_id: u32,
        /// Owning table (STG-050 stable id).
        table_id: u32,
        /// Page id within the (shard, table) page file.
        page_id: u64,
    },

    /// Wire-protocol failure (framing, encoding, unexpected message).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Authentication or identity failure.
    #[error("auth error: {0}")]
    Auth(String),

    /// Invalid assembled schema — duplicate table names or a declaration the
    /// proc macro could not reject at compile time (SPEC-001 DM-040).
    #[error("schema error: {0}")]
    Schema(String),

    /// A reducer body returned `Err(message)` (SPEC-004 RED-060): the
    /// transaction was fully rolled back and the message travels verbatim to
    /// the caller as a `ReducerResult` outcome with code 5001
    /// `REDUCER_USER_ERROR` — unlike [`FluxumError::Query`], which maps to a
    /// wire `Error` frame.
    #[error("reducer error: {0}")]
    Reducer(String),

    /// A reducer body panicked (RED-061): the transaction was fully rolled
    /// back; the caller receives a `ReducerResult` outcome with code 5002
    /// `REDUCER_PANIC` (SPEC-028 — a panic is never a user error).
    #[error("reducer panicked: {0}")]
    ReducerPanic(String),

    /// Request rejected with a stable catalog code (SPEC-028): the server
    /// layer projects `code`/`message` into the structured wire `Error`
    /// payload via [`FluxumError::to_wire`].
    #[error("query error {code}: {message}")]
    Query {
        /// Stable SPEC-028 catalog code (`fluxum_protocol::codes`).
        code: u16,
        /// Human-readable message, sent verbatim to the client.
        message: String,
        /// Safe-retry delay estimate, when the rejecting subsystem has one
        /// (e.g. the RED-050 token bucket's refill estimate).
        retry_after_ms: Option<u32>,
    },

    /// Hardware probe / derivation failure that must abort boot
    /// (e.g. SPEC-016 HWA-015 memory shortfall).
    #[error("hardware error: {0}")]
    Hardware(String),
}

impl FluxumError {
    /// Build a [`FluxumError::Config`] from anything displayable.
    pub fn config(msg: impl std::fmt::Display) -> Self {
        Self::Config(msg.to_string())
    }

    /// Build a [`FluxumError::Query`] carrying a stable catalog code.
    pub fn query(code: u16, message: impl std::fmt::Display) -> Self {
        Self::Query {
            code,
            message: message.to_string(),
            retry_after_ms: None,
        }
    }

    /// [`FluxumError::query`] with a safe-retry delay estimate attached.
    pub fn query_retryable(
        code: u16,
        message: impl std::fmt::Display,
        retry_after_ms: Option<u32>,
    ) -> Self {
        Self::Query {
            code,
            message: message.to_string(),
            retry_after_ms,
        }
    }

    /// The wire error code of a [`FluxumError::Query`], if this is one.
    pub fn query_code(&self) -> Option<u16> {
        match self {
            Self::Query { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Build a [`FluxumError::Hardware`] from anything displayable.
    pub fn hardware(msg: impl std::fmt::Display) -> Self {
        Self::Hardware(msg.to_string())
    }

    /// Project this error onto the SPEC-028 wire catalog — **total**: every
    /// variant maps to a released code (the exhaustive match makes adding a
    /// variant without a mapping a compile error).
    pub fn to_wire(&self) -> WireError {
        use fluxum_protocol::{FluxValue, codes};
        let plain = |code: u16| WireError {
            code,
            message: self.to_string(),
            retry_after_ms: None,
            details: Vec::new(),
        };
        match self {
            Self::Config(_) | Self::ConfigParse(_) | Self::Io(_) | Self::Hardware(_) => {
                plain(codes::SYS_INTERNAL)
            }
            Self::Storage(_) => plain(codes::STORAGE_INTERNAL),
            Self::BufferPoolExhausted { capacity } => WireError {
                code: codes::STORAGE_BUFFER_POOL_EXHAUSTED,
                message: self.to_string(),
                retry_after_ms: None,
                details: vec![(
                    "capacity".to_owned(),
                    FluxValue::I64(i64::try_from(*capacity).unwrap_or(i64::MAX)),
                )],
            },
            Self::PageCorrupt {
                shard_id,
                table_id,
                page_id,
            } => WireError {
                code: codes::STORAGE_PAGE_CORRUPT,
                message: self.to_string(),
                retry_after_ms: None,
                details: vec![
                    ("shard_id".to_owned(), FluxValue::I64(i64::from(*shard_id))),
                    ("table_id".to_owned(), FluxValue::I64(i64::from(*table_id))),
                    (
                        "page_id".to_owned(),
                        FluxValue::I64(i64::try_from(*page_id).unwrap_or(i64::MAX)),
                    ),
                ],
            },
            Self::Protocol(_) => plain(codes::PROTO_MALFORMED),
            Self::Auth(_) => plain(codes::AUTH_FAILED),
            Self::Schema(_) => plain(codes::SCHEMA_INVALID),
            // RED-060: the reducer's own message, verbatim, under the stable
            // user-error code (SPEC-028; system-caused failures use their own
            // codes upstream of this mapping).
            Self::Reducer(message) => WireError {
                code: codes::REDUCER_USER_ERROR,
                message: message.clone(),
                retry_after_ms: None,
                details: Vec::new(),
            },
            Self::ReducerPanic(message) => WireError {
                code: codes::REDUCER_PANIC,
                message: message.clone(),
                retry_after_ms: None,
                details: Vec::new(),
            },
            Self::Query {
                code,
                message,
                retry_after_ms,
            } => WireError {
                code: *code,
                message: message.clone(),
                retry_after_ms: *retry_after_ms,
                details: Vec::new(),
            },
        }
    }
}

/// The wire-facing projection of a [`FluxumError`] (SPEC-028 §6): what the
/// transport turns into a structured `Error` frame via
/// `ErrorMessage::from_catalog`.
#[derive(Debug, Clone)]
pub struct WireError {
    /// Stable catalog code.
    pub code: u16,
    /// Human-readable message.
    pub message: String,
    /// Safe-retry delay estimate, if any.
    pub retry_after_ms: Option<u32>,
    /// Structured details (keys per the catalog entry).
    pub details: Vec<(String, fluxum_protocol::FluxValue)>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_subsystem_prefix() {
        assert_eq!(
            FluxumError::config("bad port").to_string(),
            "config error: bad port"
        );
        assert_eq!(
            FluxumError::Auth("nope".into()).to_string(),
            "auth error: nope"
        );
    }

    #[test]
    fn query_code_is_none_for_non_query_errors() {
        assert_eq!(FluxumError::query(404, "nope").query_code(), Some(404));
        assert_eq!(FluxumError::Storage("disk".into()).query_code(), None);
    }

    #[test]
    fn io_error_converts() {
        let e: FluxumError = std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
        assert!(matches!(e, FluxumError::Io(_)));
    }

    /// SPEC-028 §6 registry adherence: every `FluxumError` variant projects
    /// onto a **released** catalog entry — no emission path can produce an
    /// uncataloged code (the `to_wire` match is exhaustive, so a new variant
    /// without a mapping fails compilation; this test pins the codes).
    #[test]
    fn every_variant_maps_onto_the_catalog() {
        use fluxum_protocol::codes;
        let yaml_err = serde_yaml::from_str::<u32>("[not an int").unwrap_err();
        let cases: Vec<(FluxumError, u16)> = vec![
            (FluxumError::config("x"), codes::SYS_INTERNAL),
            (FluxumError::ConfigParse(yaml_err), codes::SYS_INTERNAL),
            (std::io::Error::other("io").into(), codes::SYS_INTERNAL),
            (FluxumError::hardware("probe"), codes::SYS_INTERNAL),
            (FluxumError::Storage("disk".into()), codes::STORAGE_INTERNAL),
            (
                FluxumError::BufferPoolExhausted { capacity: 64 },
                codes::STORAGE_BUFFER_POOL_EXHAUSTED,
            ),
            (
                FluxumError::PageCorrupt {
                    shard_id: 1,
                    table_id: 2,
                    page_id: 3,
                },
                codes::STORAGE_PAGE_CORRUPT,
            ),
            (FluxumError::Protocol("bad".into()), codes::PROTO_MALFORMED),
            (FluxumError::Auth("no".into()), codes::AUTH_FAILED),
            (FluxumError::Schema("dup".into()), codes::SCHEMA_INVALID),
            (
                FluxumError::Reducer("saldo insuficiente".into()),
                codes::REDUCER_USER_ERROR,
            ),
            (
                FluxumError::ReducerPanic("boom".into()),
                codes::REDUCER_PANIC,
            ),
            (
                FluxumError::query(codes::SQL_UNKNOWN_TABLE, "no such table"),
                codes::SQL_UNKNOWN_TABLE,
            ),
        ];
        for (error, expected) in cases {
            let wire = error.to_wire();
            assert_eq!(wire.code, expected, "{error}");
            let entry = codes::entry(wire.code)
                .unwrap_or_else(|| panic!("{error} maps to uncataloged code {}", wire.code));
            // Emitted details keys stay within the documented set.
            for (key, _) in &wire.details {
                assert!(
                    entry.details_keys.contains(&key.as_str()),
                    "{error}: undocumented details key `{key}`"
                );
            }
        }
    }

    /// SPEC-028 scenarios pinned at the core boundary.
    #[test]
    fn wire_projection_carries_structured_data() {
        use fluxum_protocol::{FluxValue, codes};
        // Buffer-pool exhaustion advertises its capacity (scenario).
        let wire = FluxumError::BufferPoolExhausted { capacity: 64 }.to_wire();
        assert_eq!(
            wire.details,
            vec![("capacity".to_owned(), FluxValue::I64(64))]
        );
        // A user error's message travels verbatim (RED-060 scenario).
        let wire = FluxumError::Reducer("saldo insuficiente".into()).to_wire();
        assert_eq!(wire.message, "saldo insuficiente");
        // The retry estimate survives the projection.
        let wire =
            FluxumError::query_retryable(codes::REDUCER_RATE_LIMITED, "slow down", Some(200))
                .to_wire();
        assert_eq!(wire.retry_after_ms, Some(200));
    }
}
