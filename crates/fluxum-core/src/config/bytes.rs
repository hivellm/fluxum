//! Byte-size values (`"2GiB"`, `"512MiB"`, `1048576`) and the
//! `auto | <value>` config wrapper used by adaptive keys (SPEC-016 HWA-010).

use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------------------
// ByteSize
// ---------------------------------------------------------------------------

/// A byte count that deserializes from either an integer (`1048576`) or a
/// human-readable string with a unit suffix (`"512MiB"`, `"2GiB"`, `"1 kb"`).
///
/// Binary suffixes (`KiB`/`MiB`/`GiB`/`TiB`, powers of 1024) and decimal
/// suffixes (`KB`/`MB`/`GB`/`TB`, powers of 1000) are accepted
/// case-insensitively. Serializes as the raw integer byte count.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// The raw number of bytes.
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Parse a byte-size string: optional-whitespace-separated integer + unit.
pub fn parse_byte_size(input: &str) -> Result<u64, String> {
    let trimmed = input.trim();
    let digits_end = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(digits_end);
    if digits.is_empty() {
        return Err(format!("invalid byte size '{input}': missing number"));
    }
    let value: u64 = digits
        .parse()
        .map_err(|e| format!("invalid byte size '{input}': {e}"))?;
    let multiplier: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1000,
        "mb" => 1000 * 1000,
        "gb" => 1000 * 1000 * 1000,
        "tb" => 1000 * 1000 * 1000 * 1000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        other => {
            return Err(format!(
                "invalid byte size '{input}': unknown unit '{other}'"
            ));
        }
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte size '{input}' overflows u64"))
}

impl fmt::Display for ByteSize {
    /// Human-readable: exact binary multiples render with their suffix,
    /// everything else as a plain byte count.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: [(u64, &str); 4] = [
            (1 << 40, "TiB"),
            (1 << 30, "GiB"),
            (1 << 20, "MiB"),
            (1 << 10, "KiB"),
        ];
        for (factor, suffix) in UNITS {
            if self.0 >= factor && self.0.is_multiple_of(factor) {
                return write!(f, "{}{suffix}", self.0 / factor);
            }
        }
        write!(f, "{}", self.0)
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(u64),
            Str(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Int(n) => Ok(ByteSize(n)),
            Raw::Str(s) => parse_byte_size(&s).map(ByteSize).map_err(D::Error::custom),
        }
    }
}

// ---------------------------------------------------------------------------
// AutoOr
// ---------------------------------------------------------------------------

/// A config value that is either the literal string `auto` (derive from the
/// hardware probe, SPEC-016) or an explicit, always-winning value (HWA-010).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum AutoOr<T> {
    /// Derive the value from the [`crate::hw::HardwareProfile`].
    #[default]
    Auto,
    /// Operator-pinned value; always wins over the derivation.
    Value(T),
}

impl<T> AutoOr<T> {
    /// `true` when the value is `auto`.
    pub const fn is_auto(&self) -> bool {
        matches!(self, AutoOr::Auto)
    }

    /// The explicit value, if any.
    pub fn explicit(&self) -> Option<&T> {
        match self {
            AutoOr::Auto => None,
            AutoOr::Value(v) => Some(v),
        }
    }
}

impl<T: fmt::Display> fmt::Display for AutoOr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AutoOr::Auto => write!(f, "auto"),
            AutoOr::Value(v) => v.fmt(f),
        }
    }
}

impl<T: Serialize> Serialize for AutoOr<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            AutoOr::Auto => serializer.serialize_str("auto"),
            AutoOr::Value(v) => v.serialize(serializer),
        }
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for AutoOr<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        if value.as_str() == Some("auto") {
            return Ok(AutoOr::Auto);
        }
        T::deserialize(value)
            .map(AutoOr::Value)
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_bytes_and_binary_units() {
        assert_eq!(parse_byte_size("0").unwrap(), 0);
        assert_eq!(parse_byte_size("1048576").unwrap(), 1 << 20);
        assert_eq!(parse_byte_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_byte_size("512MiB").unwrap(), 512 << 20);
        assert_eq!(parse_byte_size("2GiB").unwrap(), 2 << 30);
        assert_eq!(parse_byte_size("1TiB").unwrap(), 1 << 40);
    }

    #[test]
    fn parses_decimal_units_case_insensitively_with_spaces() {
        assert_eq!(parse_byte_size("2kb").unwrap(), 2000);
        assert_eq!(parse_byte_size("3 MB").unwrap(), 3_000_000);
        assert_eq!(parse_byte_size(" 1 gib ").unwrap(), 1 << 30);
        assert_eq!(parse_byte_size("5B").unwrap(), 5);
    }

    #[test]
    fn rejects_garbage_and_overflow() {
        assert!(parse_byte_size("GiB").is_err());
        assert!(parse_byte_size("12XiB").is_err());
        assert!(parse_byte_size("-1").is_err());
        assert!(parse_byte_size("1.5GiB").is_err());
        assert!(parse_byte_size("99999999999999999999TiB").is_err());
        assert!(parse_byte_size("").is_err());
    }

    #[test]
    fn bytesize_deserializes_from_int_or_string() {
        let from_int: ByteSize = serde_yaml::from_str("134217728").unwrap();
        let from_str: ByteSize = serde_yaml::from_str("\"128MiB\"").unwrap();
        assert_eq!(from_int, from_str);
        assert!(serde_yaml::from_str::<ByteSize>("\"bogus\"").is_err());
    }

    #[test]
    fn bytesize_display_uses_binary_suffixes() {
        assert_eq!(ByteSize(2 << 30).to_string(), "2GiB");
        assert_eq!(ByteSize(64 * 1024).to_string(), "64KiB");
        assert_eq!(ByteSize(1000).to_string(), "1000");
    }

    #[test]
    fn auto_or_roundtrips() {
        let auto: AutoOr<ByteSize> = serde_yaml::from_str("auto").unwrap();
        assert!(auto.is_auto());
        let explicit: AutoOr<ByteSize> = serde_yaml::from_str("\"2GiB\"").unwrap();
        assert_eq!(explicit.explicit().map(ByteSize::as_u64), Some(2 << 30));
        let n: AutoOr<u32> = serde_yaml::from_str("8").unwrap();
        assert_eq!(n, AutoOr::Value(8));
        assert_eq!(
            serde_yaml::to_string(&AutoOr::<u32>::Auto).unwrap().trim(),
            "auto"
        );
        assert_eq!(
            serde_yaml::to_string(&AutoOr::Value(8u32)).unwrap().trim(),
            "8"
        );
    }
}
