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

    /// Wire-protocol failure (framing, encoding, unexpected message).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Authentication or identity failure.
    #[error("auth error: {0}")]
    Auth(String),

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
    fn io_error_converts() {
        let e: FluxumError = std::io::Error::new(std::io::ErrorKind::NotFound, "gone").into();
        assert!(matches!(e, FluxumError::Io(_)));
    }
}
