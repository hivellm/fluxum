//! Core identifier newtypes shared across the workspace
//! (SPEC-001 §type catalogue, SPEC-009 §identity).
//!
//! | Type | Repr | Semantics |
//! |---|---|---|
//! | [`Identity`] | `[u8; 32]` | SHA-256 of the canonical auth token; stable across sessions |
//! | [`ConnectionId`] | `u128` | Ephemeral per-connection id, never persisted |
//! | [`EntityId`] | `u64` | Generic row/entity identifier |
//! | [`Timestamp`] | `i64` | Microseconds since the Unix epoch |

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/// A stable 256-bit caller identity (SPEC-009 AUTH-001).
///
/// Derived deterministically from the canonical token form, so the same
/// principal always maps to the same `Identity` across sessions, reconnects,
/// server restarts, and (for JWT) token rotation.
///
/// Displays and serializes as 64 lowercase hex characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identity([u8; 32]);

impl Identity {
    /// Wrap raw identity bytes (already derived elsewhere, e.g. replicated).
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 identity bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Derive an identity from opaque token bytes: `SHA-256(token)`.
    ///
    /// Used by the `token` and `none` auth providers (SPEC-009 AUTH-001):
    /// the canonical form of an opaque token is its raw bytes.
    pub fn from_token(token: impl AsRef<[u8]>) -> Self {
        Self(Sha256::digest(token.as_ref()).into())
    }

    /// Derive an identity from validated JWT claims:
    /// `SHA-256("{iss}|{sub}")` (SPEC-009 AUTH-001).
    ///
    /// Claims-based derivation makes the identity survive token refresh,
    /// rotation, and re-signing — any token carrying the same `(iss, sub)`
    /// maps to the same `Identity`.
    pub fn from_claims(iss: &str, sub: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(iss.as_bytes());
        hasher.update(b"|");
        hasher.update(sub.as_bytes());
        Self(hasher.finalize().into())
    }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Identity({self})")
    }
}

impl FromStr for Identity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(format!(
                "identity must be 64 hex characters, got {}",
                s.len()
            ));
        }
        let mut bytes = [0u8; 32];
        let (pairs, _) = s.as_bytes().as_chunks::<2>();
        for (i, pair) in pairs.iter().enumerate() {
            let hex = std::str::from_utf8(pair).map_err(|_| "identity is not ASCII hex")?;
            bytes[i] =
                u8::from_str_radix(hex, 16).map_err(|_| format!("invalid hex byte '{hex}'"))?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for Identity {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// ConnectionId
// ---------------------------------------------------------------------------

/// An ephemeral 128-bit connection identifier (SPEC-009 AUTH-010).
///
/// Assigned by the server at connection establishment; never persisted, and a
/// new value is issued on every reconnect. Displays as 32 lowercase hex chars.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ConnectionId(u128);

impl ConnectionId {
    /// Wrap a raw 128-bit connection id.
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// The raw 128-bit value.
    pub const fn as_u128(&self) -> u128 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

// ---------------------------------------------------------------------------
// EntityId
// ---------------------------------------------------------------------------

/// Generic 64-bit row/entity identifier (SPEC-001 type catalogue).
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize,
)]
pub struct EntityId(u64);

impl EntityId {
    /// Wrap a raw entity id.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw 64-bit value.
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// Microseconds since the Unix epoch, signed (SPEC-001 type catalogue).
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize,
)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Construct from raw microseconds since the Unix epoch.
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    /// Raw microseconds since the Unix epoch.
    pub const fn as_micros(&self) -> i64 {
        self.0
    }

    /// The current wall-clock time.
    ///
    /// Saturates at `i64::MAX` µs (~294,000 years) rather than panicking.
    pub fn now() -> Self {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => Self(i64::try_from(d.as_micros()).unwrap_or(i64::MAX)),
            // Clock set before the epoch: negative offset.
            Err(e) => Self(i64::try_from(e.duration().as_micros()).map_or(i64::MIN, |v| -v)),
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn identity_from_token_is_deterministic() {
        let a = Identity::from_token("my-secret-token");
        let b = Identity::from_token("my-secret-token");
        let c = Identity::from_token("other-token");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn identity_from_token_matches_sha256() {
        // SHA-256("") — well-known vector.
        assert_eq!(
            Identity::from_token(b"").to_string(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn identity_from_claims_stable_across_token_rotation() {
        // Two different signed tokens carrying the same (iss, sub) claims must
        // map to the same Identity (SPEC-009 AUTH-001/AUTH-002)…
        let before_rotation = Identity::from_claims("https://auth.example.com", "user-42");
        let after_rotation = Identity::from_claims("https://auth.example.com", "user-42");
        assert_eq!(before_rotation, after_rotation);

        // …while hashing the raw token bytes would differ per rotation.
        let raw_a = Identity::from_token("jwt-bytes-signature-1");
        let raw_b = Identity::from_token("jwt-bytes-signature-2");
        assert_ne!(raw_a, raw_b);

        // Claims derivation equals SHA-256("{iss}|{sub}").
        let manual = Identity::from_token("https://auth.example.com|user-42");
        assert_eq!(before_rotation, manual);
    }

    #[test]
    fn identity_from_claims_distinguishes_principals() {
        assert_ne!(
            Identity::from_claims("iss-a", "sub"),
            Identity::from_claims("iss-b", "sub")
        );
        assert_ne!(
            Identity::from_claims("iss", "sub-a"),
            Identity::from_claims("iss", "sub-b")
        );
    }

    #[test]
    fn identity_display_roundtrips_through_fromstr() {
        let id = Identity::from_token("roundtrip");
        let parsed: Identity = id.to_string().parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(id.to_string().len(), 64);
    }

    #[test]
    fn identity_fromstr_rejects_bad_input() {
        assert!(Identity::from_str("abc").is_err());
        assert!(Identity::from_str(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn identity_serde_roundtrip_as_hex_string() {
        let id = Identity::from_token("serde");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        let back: Identity = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn connection_id_displays_as_padded_hex() {
        assert_eq!(
            ConnectionId::new(0xdead_beef).to_string(),
            "000000000000000000000000deadbeef"
        );
        assert_eq!(ConnectionId::new(7).as_u128(), 7);
    }

    #[test]
    fn entity_id_and_timestamp_accessors() {
        assert_eq!(EntityId::new(99).as_u64(), 99);
        assert_eq!(EntityId::new(99).to_string(), "99");
        assert_eq!(Timestamp::from_micros(-5).as_micros(), -5);
        assert!(Timestamp::now().as_micros() > 1_600_000_000_000_000); // after 2020
    }
}
