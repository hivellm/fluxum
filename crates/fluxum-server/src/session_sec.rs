//! Session-token security for the Streamable HTTP transport (SPEC-026
//! SEC-050..053). The pure, socket-free pieces: minting an unpredictable
//! token, deriving the at-rest lookup id, and the resolved policy knobs.
//!
//! # Threat model
//!
//! On a directly exposed port the `Fluxum-Session` token IS the bearer
//! credential for every post-auth request — steal it and you are the victim
//! until it expires. So:
//!
//! - The token is **CSPRNG** output (≥128 bits), independent of the caller's
//!   identity: unpredictable regardless of what else leaks. The former
//!   `SHA-256(identity ++ counter)` scheme rested entirely on identity
//!   secrecy and a walkable counter (SEC-050).
//! - Only the token's **hash** is stored server-side (the map key), so a
//!   disclosure of the session map — a log, a core dump — yields no usable
//!   token. Lookup hashes the presented token first, so nothing
//!   secret-dependent is compared in the clear (SEC-050).
//! - A presented token the server never minted hashes to an id that simply
//!   misses: it can never be **adopted** as a session (SEC-050 anti-fixation).

use std::time::Duration;

use sha2::{Digest, Sha256};

use fluxum_core::config::SessionConfig;

/// Raw token width: 128 bits of CSPRNG entropy (SEC-050).
const TOKEN_BYTES: usize = 16;

/// A freshly minted token: the raw value handed to the client in the
/// `Fluxum-Session` header, and the hex lookup id (`SHA-256(raw)`) stored
/// server-side. The raw value is never persisted.
pub struct MintedToken {
    /// The header value the client presents on every later request.
    pub raw: String,
    /// The at-rest lookup id — what the session map is keyed by.
    pub id: String,
}

/// Mint a new CSPRNG session token (SEC-050).
#[must_use]
pub fn mint() -> MintedToken {
    let bytes = fluxum_core::crypto::random_bytes::<TOKEN_BYTES>();
    let raw = hex(&bytes);
    MintedToken {
        id: token_id(&raw),
        raw,
    }
}

/// The at-rest lookup id for a presented token: `hex(SHA-256(raw))`. A token
/// the server never minted hashes to an id that is simply absent — the basis
/// of both hashed-at-rest storage and anti-fixation (SEC-050).
#[must_use]
pub fn token_id(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The resolved session-security policy (SEC-051/052), in native units.
#[derive(Debug, Clone, Copy)]
pub struct SessionPolicy {
    /// Bind a session to its authenticating client IP (SEC-051).
    pub bind_client_ip: bool,
    /// Rotate the token this often (`None` = no interval rotation; a re-auth
    /// still rotates) (SEC-052).
    pub rotate_interval: Option<Duration>,
    /// Grace window a just-rotated token is still honored for (SEC-052).
    pub rotate_grace: Duration,
    /// Absolute session lifetime on top of the idle expiry (`None` = none)
    /// (SEC-052).
    pub absolute_lifetime: Option<Duration>,
}

impl SessionPolicy {
    /// Resolve from config: a `0` disables the corresponding knob.
    pub fn from_config(cfg: &SessionConfig) -> Self {
        Self {
            bind_client_ip: cfg.bind_client_ip,
            rotate_interval: (cfg.rotate_interval_secs != 0)
                .then(|| Duration::from_secs(cfg.rotate_interval_secs)),
            rotate_grace: Duration::from_secs(cfg.rotate_grace_secs),
            absolute_lifetime: (cfg.absolute_lifetime_secs != 0)
                .then(|| Duration::from_secs(cfg.absolute_lifetime_secs)),
        }
    }
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self::from_config(&SessionConfig::default())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn minted_tokens_are_unpredictable_and_distinct() {
        let a = mint();
        let b = mint();
        // 128 bits of entropy → 32 hex chars raw, 64-char id.
        assert_eq!(a.raw.len(), TOKEN_BYTES * 2);
        assert_eq!(a.id.len(), 64);
        assert_ne!(a.raw, b.raw, "two mints collide with ~zero probability");
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn the_id_is_the_hash_of_the_raw_token_not_the_token() {
        let m = mint();
        assert_eq!(token_id(&m.raw), m.id);
        // The stored id is not the token — a map disclosure yields no token.
        assert_ne!(m.raw, m.id);
        // A token the server never minted just hashes to some absent id.
        assert_ne!(token_id("attacker-supplied"), m.id);
    }

    #[test]
    fn policy_resolves_zeroes_to_disabled() {
        let p = SessionPolicy::from_config(&SessionConfig::default());
        assert!(!p.bind_client_ip);
        assert!(p.rotate_interval.is_none());
        assert!(p.absolute_lifetime.is_none());

        let p = SessionPolicy::from_config(&SessionConfig {
            bind_client_ip: true,
            rotate_interval_secs: 300,
            rotate_grace_secs: 30,
            absolute_lifetime_secs: 86400,
        });
        assert!(p.bind_client_ip);
        assert_eq!(p.rotate_interval, Some(Duration::from_secs(300)));
        assert_eq!(p.rotate_grace, Duration::from_secs(30));
        assert_eq!(p.absolute_lifetime, Some(Duration::from_secs(86400)));
    }
}
