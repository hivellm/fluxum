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

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

/// Exact fixed-point decimal: the value is `unscaled × 10^-scale`
/// (SPEC-017 CT-020). The analogue of PostgreSQL `numeric(p, s)` — exact, no
/// binary-float rounding.
///
/// The representation is **self-describing**: each value carries its own
/// `scale`, so a `Decimal` column accepts any scale (a `#[normalize(money,
/// scale)]` transform, added later, canonicalises to a fixed scale on write).
///
/// `PartialEq`/`Eq`/`Hash`/`Ord` are **structural** over `(unscaled, scale)`,
/// which keeps "equal `RowValue` ⟺ equal FluxBIN bytes" — the invariant the
/// store and index-integrity checks rely on (STG-007). Numeric comparison that
/// treats `1.50` and `1.5` as equal is [`Decimal::value_cmp`], used by the
/// query layer (`ORDER BY`), not by storage equality.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Decimal {
    unscaled: i128,
    scale: u8,
}

impl Decimal {
    /// Construct from an unscaled integer and a scale: value `unscaled ×
    /// 10^-scale` (e.g. `from_parts(150, 2)` == `1.50`).
    pub const fn from_parts(unscaled: i128, scale: u8) -> Self {
        Self { unscaled, scale }
    }

    /// An integer value (`scale == 0`).
    pub const fn from_integer(value: i128) -> Self {
        Self {
            unscaled: value,
            scale: 0,
        }
    }

    /// The unscaled integer coefficient.
    pub const fn unscaled(&self) -> i128 {
        self.unscaled
    }

    /// The number of fractional decimal digits.
    pub const fn scale(&self) -> u8 {
        self.scale
    }

    /// Compare two decimals by **numeric value**, so `1.50` and `1.5` are
    /// equal and `1.5 < 2.0` regardless of scale (SPEC-017 CT-020). Exact
    /// where the scale alignment fits `i128`; for the rare extreme-scale
    /// overflow it falls back to a sign/magnitude decision.
    pub fn value_cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.scale == other.scale {
            return self.unscaled.cmp(&other.unscaled);
        }
        let max_scale = self.scale.max(other.scale);
        let lift = |v: &Self| -> Option<i128> {
            pow10_i128(u32::from(max_scale - v.scale)).and_then(|p| v.unscaled.checked_mul(p))
        };
        match (lift(self), lift(other)) {
            (Some(a), Some(b)) => a.cmp(&b),
            // A lift overflowed `i128`: that side's magnitude dominates, so its
            // sign decides the ordering (the overflowing coefficient is never 0).
            (None, Some(_)) => sign_ordering(self.unscaled),
            (Some(_), None) => sign_ordering(other.unscaled).reverse(),
            (None, None) => self.unscaled.cmp(&other.unscaled),
        }
    }
}

/// `Greater`/`Less`/`Equal` from the sign of `v`.
fn sign_ordering(v: i128) -> std::cmp::Ordering {
    v.cmp(&0)
}

/// `10^n` as `i128`, or `None` on overflow.
fn pow10_i128(n: u32) -> Option<i128> {
    10i128.checked_pow(n)
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = usize::from(self.scale);
        // Digit-string arithmetic avoids any 10^scale overflow for large scales.
        let digits = self.unscaled.unsigned_abs().to_string();
        let sign = if self.unscaled < 0 { "-" } else { "" };
        if scale == 0 {
            return write!(f, "{sign}{digits}");
        }
        if digits.len() > scale {
            let point = digits.len() - scale;
            write!(f, "{sign}{}.{}", &digits[..point], &digits[point..])
        } else {
            let zeros = "0".repeat(scale - digits.len());
            write!(f, "{sign}0.{zeros}{digits}")
        }
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

    #[test]
    fn decimal_accessors_and_parts() {
        let d = Decimal::from_parts(150, 2);
        assert_eq!(d.unscaled(), 150);
        assert_eq!(d.scale(), 2);
        assert_eq!(Decimal::from_integer(42), Decimal::from_parts(42, 0));
    }

    #[test]
    fn decimal_display_places_the_point() {
        assert_eq!(Decimal::from_parts(150, 2).to_string(), "1.50");
        assert_eq!(Decimal::from_parts(1234, 0).to_string(), "1234");
        assert_eq!(Decimal::from_parts(-5, 3).to_string(), "-0.005");
        assert_eq!(Decimal::from_parts(-12345, 2).to_string(), "-123.45");
        assert_eq!(Decimal::from_parts(0, 4).to_string(), "0.0000");
        assert_eq!(Decimal::from_parts(7, 1).to_string(), "0.7");
    }

    #[test]
    fn decimal_structural_eq_distinguishes_scale_but_value_cmp_does_not() {
        use std::cmp::Ordering;
        let a = Decimal::from_parts(150, 2); // 1.50
        let b = Decimal::from_parts(15, 1); // 1.5
        assert_ne!(a, b); // structural: distinct bytes
        assert_eq!(a.value_cmp(&b), Ordering::Equal); // numeric: equal value

        let c = Decimal::from_parts(2, 0); // 2.0
        assert_eq!(a.value_cmp(&c), Ordering::Less);
        assert_eq!(c.value_cmp(&a), Ordering::Greater);
        // negative vs positive across scales
        let n = Decimal::from_parts(-1, 3); // -0.001
        assert_eq!(n.value_cmp(&a), Ordering::Less);
    }
}
