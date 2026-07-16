//! Deterministic column-value normalizers (SPEC-017 CT-021/022).
//!
//! Pure functions: the same input always yields the same stored value, so they
//! are safe on the replay/DST path. `#[normalize(money, scale = N)]` and
//! `#[normalize(datetime)]` apply these on the write path so the stored,
//! indexed, and replicated value is already canonical.

use unicode_normalization::UnicodeNormalization;

use super::{CaseFold, StringForm};
use crate::error::{FluxumError, Result};
use crate::types::{Decimal, Timestamp};

fn money_err(input: &str, reason: &str) -> FluxumError {
    FluxumError::Storage(format!("money value `{input}`: {reason} (SPEC-017 CT-021)"))
}

/// Normalize a decimal-string money value to an exact [`Decimal`] at `scale`
/// fractional digits (CT-021). Accepts an optional sign, an integer part, and
/// an optional fractional part (`"12.50"`, `"-3"`, `".005"`, `"12.500"`).
///
/// Rejects **precision loss**: a value with more than `scale` *significant*
/// fractional digits is an error, never silently truncated (trailing zeros
/// beyond `scale` are fine). Leading/trailing ASCII whitespace is ignored.
pub fn money_from_str(input: &str, scale: u8) -> Result<Decimal> {
    let s = input.trim();
    let (neg, rest) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if rest.is_empty() {
        return Err(money_err(input, "no digits"));
    }
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (if i.is_empty() { "0" } else { i }, f),
        None => (rest, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(money_err(input, "non-digit character"));
    }

    let target = usize::from(scale);
    // Precision guard: any fractional digit beyond `scale` must be zero.
    if frac_part.len() > target && frac_part.as_bytes()[target..].iter().any(|&b| b != b'0') {
        return Err(money_err(
            input,
            &format!("more than {scale} fractional digit(s) — precision loss"),
        ));
    }

    // Build the unscaled coefficient: integer digits followed by exactly
    // `scale` fractional digits (right-padded with zeros, or truncated — the
    // truncated tail is all zeros by the guard above).
    let mut frac = frac_part.to_string();
    if frac.len() < target {
        frac.push_str(&"0".repeat(target - frac.len()));
    } else {
        frac.truncate(target);
    }
    let magnitude: i128 = format!("{int_part}{frac}")
        .parse()
        .map_err(|_| money_err(input, "value exceeds the i128 coefficient range"))?;
    let unscaled = if neg { -magnitude } else { magnitude };
    Ok(Decimal::from_parts(unscaled, scale))
}

/// Normalize a minor-unit integer (e.g. cents when `scale == 2`) already at
/// `scale` into a [`Decimal`] (CT-021) — a total, lossless conversion.
pub fn money_from_minor_units(minor: i128, scale: u8) -> Decimal {
    Decimal::from_parts(minor, scale)
}

/// Canonicalize a timestamp to UTC microseconds (CT-022).
///
/// A [`Timestamp`] is stored as `i64` microseconds since the Unix epoch, which
/// is already the canonical UTC instant, so this is the identity
/// canonicalization. It exists so the `#[normalize(datetime)]` attribute has a
/// uniform CT-022 entry point; parsing timezone-bearing string inputs
/// (`assume_tz`) needs a timezone dependency and lands with the string
/// normalizer.
pub const fn datetime_utc(ts: Timestamp) -> Timestamp {
    ts
}

/// Canonicalize a string column value (CT-023): optional trim, Unicode
/// normalization to `form`, then case handling — so equality, `#[unique]`,
/// and index keys operate on one canonical spelling (the `citext` analog).
///
/// Deterministic and pure (Unicode tables are compiled in, DST-safe).
/// `CaseFold::Fold` and `CaseFold::Lower` both use Rust's full Unicode
/// lowercase mapping in this version — the same choice PostgreSQL's `citext`
/// makes (`lower()`), so `ß` stays `ß` rather than folding to `ss`; the
/// distinction is kept in the descriptor so a future full case-fold can light
/// up without a schema change.
pub fn normalize_string(input: &str, form: StringForm, case: CaseFold, trim: bool) -> String {
    let s = if trim { input.trim() } else { input };
    let normalized: String = match form {
        StringForm::Nfc => s.nfc().collect(),
        StringForm::Nfkc => s.nfkc().collect(),
    };
    match case {
        CaseFold::None => normalized,
        CaseFold::Fold | CaseFold::Lower => normalized.to_lowercase(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn dec(unscaled: i128, scale: u8) -> Decimal {
        Decimal::from_parts(unscaled, scale)
    }

    #[test]
    fn money_parses_and_pads_to_scale() {
        assert_eq!(money_from_str("12.50", 2).unwrap(), dec(1250, 2));
        assert_eq!(money_from_str("12.5", 2).unwrap(), dec(1250, 2)); // right-pad
        assert_eq!(money_from_str("12", 2).unwrap(), dec(1200, 2)); // no fraction
        assert_eq!(money_from_str("  7 ", 2).unwrap(), dec(700, 2)); // trimmed
        assert_eq!(money_from_str(".5", 2).unwrap(), dec(50, 2)); // empty int part
        assert_eq!(money_from_str("+3.25", 2).unwrap(), dec(325, 2)); // explicit plus
        assert_eq!(money_from_str("0", 0).unwrap(), dec(0, 0)); // scale 0
    }

    #[test]
    fn money_handles_sign_and_trailing_zeros_beyond_scale() {
        assert_eq!(money_from_str("-3.05", 2).unwrap(), dec(-305, 2));
        // Trailing zeros past `scale` are not precision loss.
        assert_eq!(money_from_str("12.500", 2).unwrap(), dec(1250, 2));
        assert_eq!(money_from_str("-0.00", 2).unwrap(), dec(0, 2));
    }

    #[test]
    fn money_rejects_precision_loss_and_garbage() {
        assert!(money_from_str("12.005", 2).is_err()); // 3rd fractional digit is significant
        assert!(money_from_str("1.5", 0).is_err()); // any fraction at scale 0
        assert!(money_from_str("", 2).is_err());
        assert!(money_from_str("-", 2).is_err());
        assert!(money_from_str("1.2.3", 2).is_err());
        assert!(money_from_str("12a", 2).is_err());
        assert!(money_from_str(&"9".repeat(40), 0).is_err()); // i128 overflow
    }

    #[test]
    fn money_normalized_values_compare_equal_across_scales() {
        // The normalizer yields exact fixed-point, so numeric equality holds.
        let a = money_from_str("1.5", 1).unwrap();
        let b = money_from_str("1.50", 2).unwrap();
        assert_eq!(a.value_cmp(&b), std::cmp::Ordering::Equal);
        assert_eq!(money_from_minor_units(1250, 2), dec(1250, 2));
    }

    #[test]
    fn datetime_is_canonical_utc_micros() {
        let ts = Timestamp::from_micros(1_700_000_000_000_000);
        assert_eq!(datetime_utc(ts), ts);
        assert_eq!(datetime_utc(ts).as_micros(), 1_700_000_000_000_000);
    }

    #[test]
    fn string_normalizes_composed_and_decomposed_to_one_form() {
        // "Café" spelled precomposed (é) vs decomposed (e + U+0301).
        let composed = "Caf\u{e9}";
        let decomposed = "Cafe\u{301}";
        assert_ne!(composed, decomposed);
        let a = normalize_string(composed, StringForm::Nfc, CaseFold::None, false);
        let b = normalize_string(decomposed, StringForm::Nfc, CaseFold::None, false);
        assert_eq!(a, b, "NFC unifies the two spellings");
        assert_eq!(a, composed);
    }

    #[test]
    fn string_nfkc_folds_compatibility_forms() {
        // The "ﬁ" ligature decomposes to "fi" under NFKC but not NFC.
        assert_eq!(
            normalize_string("ﬁle", StringForm::Nfkc, CaseFold::None, false),
            "file"
        );
        assert_eq!(
            normalize_string("ﬁle", StringForm::Nfc, CaseFold::None, false),
            "ﬁle"
        );
    }

    #[test]
    fn string_case_and_trim_apply_after_normalization() {
        assert_eq!(
            normalize_string("  HeLLo  ", StringForm::Nfc, CaseFold::Fold, true),
            "hello"
        );
        assert_eq!(
            normalize_string("İstanbul", StringForm::Nfc, CaseFold::Lower, false),
            "i\u{307}stanbul",
            "multi-char lowercase mappings are honored"
        );
        // No trim requested: whitespace preserved.
        assert_eq!(
            normalize_string(" x ", StringForm::Nfc, CaseFold::None, false),
            " x "
        );
    }
}
