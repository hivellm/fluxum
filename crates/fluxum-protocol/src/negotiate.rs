//! Per-connection wire-option negotiation (SPEC-006 RPC-008 / RPC-035).
//!
//! `Authenticate` carries two option strings — `compression` and
//! `tx_updates` — and the server echoes what it will actually do in the
//! `AuthResult` tail. This module is the shared vocabulary: the parse is the
//! validation (an unrecognized value is a 400 by RPC-020), and the parsed
//! options are `Copy` so a session can pin them for its lifetime.

use std::fmt;

/// Server→client frame compression (RPC-008), as negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireCompression {
    /// No tag byte; framing is exactly RPC-001.
    #[default]
    None,
    /// One raw-DEFLATE stream per ordered server→client byte stream, each
    /// compressed frame a sync-flushed chunk (the context-carryover delta
    /// layer). The negotiation token is `"gzip"` for family familiarity; the
    /// bytes are RFC 1951.
    Gzip,
}

impl WireCompression {
    /// The token echoed in `AuthResult.compression` (RPC-030).
    #[must_use]
    pub const fn echo(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
        }
    }
}

/// Commit-broadcast form (RPC-035), as negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdateForm {
    /// Enriched `TxUpdate` (RPC-033) — the default.
    #[default]
    Full,
    /// `TxUpdateLight`: provenance stripped, cursor kept (RPC-035).
    Light,
}

impl UpdateForm {
    /// The token echoed in `AuthResult.tx_updates` (RPC-030).
    #[must_use]
    pub const fn echo(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Light => "light",
        }
    }
}

/// A connection's negotiated wire options, fixed at the first successful
/// `Authenticate` for the connection's lifetime (RPC-008/RPC-035).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WireOptions {
    /// Server→client frame compression (RPC-008).
    pub compression: WireCompression,
    /// Commit-broadcast form (RPC-035).
    pub update_form: UpdateForm,
}

/// Why a negotiation string was refused. Every variant is a client error
/// (RPC-020: reject with a 400-class `Error`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiateError {
    /// Not a token this protocol version defines.
    UnknownCompression(String),
    /// A defined token this build does not implement: `brotli` is reserved
    /// (RPC-008) and rejected rather than silently degraded.
    UnsupportedCompression(String),
    /// Not a token this protocol version defines.
    UnknownTxUpdates(String),
    /// `delta` is specified (RPC-036) but not yet implemented; rejected so
    /// the client falls back explicitly.
    UnsupportedTxUpdates(String),
}

impl fmt::Display for NegotiateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCompression(v) => {
                write!(
                    f,
                    "unknown compression {v:?} (RPC-008: none | gzip | brotli)"
                )
            }
            Self::UnsupportedCompression(v) => {
                write!(
                    f,
                    "compression {v:?} is not available in this build (RPC-008)"
                )
            }
            Self::UnknownTxUpdates(v) => {
                write!(f, "unknown tx_updates {v:?} (RPC-035: full | light)")
            }
            Self::UnsupportedTxUpdates(v) => {
                write!(f, "tx_updates {v:?} is not implemented yet (RPC-036)")
            }
        }
    }
}

impl std::error::Error for NegotiateError {}

/// Parse an `Authenticate.compression` / `?compression=` token (RPC-008).
/// `None` means the default.
pub fn parse_compression(token: Option<&str>) -> Result<WireCompression, NegotiateError> {
    match token {
        None | Some("none") => Ok(WireCompression::None),
        Some("gzip") => Ok(WireCompression::Gzip),
        // Reserved by RPC-008; a build without the codec refuses loudly so
        // the client falls back, instead of waiting for tags that never come.
        Some("brotli") => Err(NegotiateError::UnsupportedCompression("brotli".into())),
        Some(other) => Err(NegotiateError::UnknownCompression(other.into())),
    }
}

/// Parse an `Authenticate.tx_updates` / `?tx_updates=` token (RPC-035).
/// `None` means the default.
pub fn parse_update_form(token: Option<&str>) -> Result<UpdateForm, NegotiateError> {
    match token {
        None | Some("full") => Ok(UpdateForm::Full),
        Some("light") => Ok(UpdateForm::Light),
        Some("delta") => Err(NegotiateError::UnsupportedTxUpdates("delta".into())),
        Some(other) => Err(NegotiateError::UnknownTxUpdates(other.into())),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse_from_absence() {
        assert_eq!(parse_compression(None).unwrap(), WireCompression::None);
        assert_eq!(parse_update_form(None).unwrap(), UpdateForm::Full);
    }

    #[test]
    fn explicit_tokens_parse() {
        assert_eq!(
            parse_compression(Some("none")).unwrap(),
            WireCompression::None
        );
        assert_eq!(
            parse_compression(Some("gzip")).unwrap(),
            WireCompression::Gzip
        );
        assert_eq!(parse_update_form(Some("full")).unwrap(), UpdateForm::Full);
        assert_eq!(parse_update_form(Some("light")).unwrap(), UpdateForm::Light);
    }

    #[test]
    fn brotli_is_refused_as_unsupported_not_unknown() {
        assert_eq!(
            parse_compression(Some("brotli")),
            Err(NegotiateError::UnsupportedCompression("brotli".into()))
        );
    }

    #[test]
    fn delta_is_refused_as_unsupported_not_unknown() {
        assert_eq!(
            parse_update_form(Some("delta")),
            Err(NegotiateError::UnsupportedTxUpdates("delta".into()))
        );
    }

    #[test]
    fn garbage_is_unknown() {
        assert!(matches!(
            parse_compression(Some("zstd")),
            Err(NegotiateError::UnknownCompression(_))
        ));
        assert!(matches!(
            parse_update_form(Some("lite")),
            Err(NegotiateError::UnknownTxUpdates(_))
        ));
    }

    #[test]
    fn echoes_are_the_wire_tokens() {
        assert_eq!(WireCompression::None.echo(), "none");
        assert_eq!(WireCompression::Gzip.echo(), "gzip");
        assert_eq!(UpdateForm::Full.echo(), "full");
        assert_eq!(UpdateForm::Light.echo(), "light");
    }

    #[test]
    fn errors_render_the_offending_token() {
        let e = parse_compression(Some("zstd")).unwrap_err();
        assert!(e.to_string().contains("zstd"));
        let e = parse_update_form(Some("delta")).unwrap_err();
        assert!(e.to_string().contains("delta"));
    }
}
