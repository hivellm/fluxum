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
    /// the caller as `ReducerResult { outcome: Err(message) }` — unlike
    /// [`FluxumError::Query`], which maps to a wire `Error` frame.
    #[error("reducer error: {0}")]
    Reducer(String),

    /// Query rejected with a wire error code (SPEC-006 RPC-034 registry) —
    /// e.g. SPEC-008's `400 table 'X' has no spatial index` (SPX-022) and
    /// `503 spatial index not ready` (SPX-023). The server layer forwards
    /// `code`/`message` verbatim as the wire `Error` payload.
    #[error("query error {code}: {message}")]
    Query {
        /// HTTP-compatible wire error code (`fluxum_protocol::codes`).
        code: u16,
        /// Human-readable message, sent verbatim to the client.
        message: String,
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

    /// Build a [`FluxumError::Query`] carrying a wire error code.
    pub fn query(code: u16, message: impl std::fmt::Display) -> Self {
        Self::Query {
            code,
            message: message.to_string(),
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
}

#[cfg(test)]
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
}
