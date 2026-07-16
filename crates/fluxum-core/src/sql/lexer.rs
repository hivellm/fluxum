//! Tokenizer for the SPEC-005 SQL subset (SUB-010..SUB-012, T4.1).
//!
//! Deliberately closed: the only characters that can ever appear in a valid
//! subscription query are tokenized, and everything else — statement
//! separators, comments, quotes styles used for injection pivots, operators
//! outside the subset — is rejected here with a wire-ready 400 before any
//! parsing happens. The lexer never panics on any input (the injection
//! corpus pins that).

use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};

/// Hard cap on query text length: subscription queries are short; anything
/// larger is hostile or a client bug (guards allocation, SUB-012 spirit).
pub(crate) const MAX_QUERY_BYTES: usize = 8 * 1024;

/// One lexical token. Keywords arrive as [`Token::Word`] and are classified
/// case-insensitively by the parser (identifiers keep their exact case —
/// table and column names are case-sensitive Rust declarations).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    /// Identifier or keyword: `[A-Za-z_][A-Za-z0-9_]*`.
    Word(String),
    /// Integer literal (optionally negative).
    Int(i64),
    /// Float literal (optionally negative).
    Float(f64),
    /// Single-quoted string literal; `''` escapes a quote.
    Str(String),
    /// `*`
    Star,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `,`
    Comma,
    /// `=`
    Eq,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Word(w) => write!(f, "{w}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(x) => write!(f, "{x}"),
            Self::Str(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Self::Star => write!(f, "*"),
            Self::LParen => write!(f, "("),
            Self::RParen => write!(f, ")"),
            Self::Comma => write!(f, ","),
            Self::Eq => write!(f, "="),
        }
    }
}

pub(crate) fn unsupported(detail: impl std::fmt::Display) -> FluxumError {
    FluxumError::query(
        codes::SQL_UNSUPPORTED,
        format!("unsupported query syntax: {detail}"),
    )
}

/// Tokenize `sql`, rejecting every character outside the subset.
pub(crate) fn tokenize(sql: &str) -> Result<Vec<Token>> {
    if sql.len() > MAX_QUERY_BYTES {
        return Err(unsupported(format!(
            "query text of {} bytes exceeds the {MAX_QUERY_BYTES}-byte limit",
            sql.len()
        )));
    }
    let mut tokens = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            b'\'' => {
                let (token, next) = lex_string(sql, i)?;
                tokens.push(token);
                i = next;
            }
            b'-' => {
                // A comment (`--`) is an injection pivot; a lone minus is
                // only valid introducing a numeric literal.
                if bytes.get(i + 1) == Some(&b'-') {
                    return Err(unsupported("SQL comments (`--`) are not allowed"));
                }
                if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    let (token, next) = lex_number(sql, i)?;
                    tokens.push(token);
                    i = next;
                } else {
                    return Err(unsupported("operator `-` outside a numeric literal"));
                }
            }
            b'0'..=b'9' => {
                let (token, next) = lex_number(sql, i)?;
                tokens.push(token);
                i = next;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(Token::Word(sql[start..i].to_owned()));
            }
            // Named rejections for the classic injection/probe characters
            // give the clearest diagnostics; everything else falls through
            // to a generic unexpected-character error.
            b';' => return Err(unsupported("statement separator `;` is not allowed")),
            b'"' | b'`' => {
                return Err(unsupported(
                    "quoted identifiers are not allowed (use the declared table/column name)",
                ));
            }
            b'/' | b'#' => return Err(unsupported("SQL comments are not allowed")),
            b'<' | b'>' | b'!' => {
                return Err(unsupported(format!(
                    "comparison operator `{}` (the subset supports =, IN, BETWEEN)",
                    char::from(c)
                )));
            }
            other => {
                return Err(unsupported(format!(
                    "unexpected character {:?}",
                    char::from(other)
                )));
            }
        }
    }
    Ok(tokens)
}

/// Lex a single-quoted string starting at `start` (which is the quote).
fn lex_string(sql: &str, start: usize) -> Result<(Token, usize)> {
    let bytes = sql.as_bytes();
    let mut value = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                value.push('\'');
                i += 2;
                continue;
            }
            return Ok((Token::Str(value), i + 1));
        }
        // Advance by whole UTF-8 characters, not bytes.
        let ch = sql[i..].chars().next().unwrap_or('\u{FFFD}');
        value.push(ch);
        i += ch.len_utf8();
    }
    Err(unsupported("unterminated string literal"))
}

/// Lex an integer or float literal starting at `start` (`-` allowed).
fn lex_number(sql: &str, start: usize) -> Result<(Token, usize)> {
    let bytes = sql.as_bytes();
    let mut i = start;
    if bytes[i] == b'-' {
        i += 1;
    }
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let mut is_float = false;
    if i < bytes.len() && bytes[i] == b'.' {
        if !bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            return Err(unsupported("malformed numeric literal"));
        }
        is_float = true;
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    // `1e5`, `0x2A`, `1abc` — reject a literal glued to letters.
    if i < bytes.len() && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
        return Err(unsupported(format!(
            "malformed numeric literal `{}`",
            &sql[start..=i.min(sql.len() - 1)]
        )));
    }
    let text = &sql[start..i];
    if is_float {
        let value: f64 = text
            .parse()
            .map_err(|_| unsupported(format!("malformed numeric literal `{text}`")))?;
        if !value.is_finite() {
            return Err(unsupported(format!("non-finite numeric literal `{text}`")));
        }
        Ok((Token::Float(value), i))
    } else {
        let value: i64 = text
            .parse()
            .map_err(|_| unsupported(format!("integer literal `{text}` out of range")))?;
        Ok((Token::Int(value), i))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn tokens_display_in_their_source_form() {
        assert_eq!(Token::Word("Sensor".into()).to_string(), "Sensor");
        assert_eq!(Token::Int(-3).to_string(), "-3");
        assert_eq!(Token::Float(1.5).to_string(), "1.5");
        // String display re-escapes embedded quotes.
        assert_eq!(Token::Str("o'brien".into()).to_string(), "'o''brien'");
        assert_eq!(Token::Star.to_string(), "*");
        assert_eq!(Token::LParen.to_string(), "(");
        assert_eq!(Token::RParen.to_string(), ")");
        assert_eq!(Token::Comma.to_string(), ",");
        assert_eq!(Token::Eq.to_string(), "=");
    }

    fn reject(sql: &str) -> String {
        tokenize(sql).unwrap_err().to_string()
    }

    #[test]
    fn lone_minus_and_malformed_numerics_are_rejected() {
        // `-` not introducing a numeric literal.
        let err = reject("SELECT * FROM T WHERE a = - 1");
        assert!(err.contains("operator `-`"), "{err}");
        // A trailing decimal point with no fraction digits.
        let err = reject("SELECT * FROM T WHERE a = 1.");
        assert!(err.contains("malformed numeric literal"), "{err}");
    }

    #[test]
    fn overflowing_float_literals_are_rejected_as_non_finite() {
        // 400 digits parse as f64 infinity — rejected, never stored.
        let huge = format!("SELECT * FROM T WHERE a = {}.0", "9".repeat(400));
        let err = reject(&huge);
        assert!(err.contains("non-finite numeric literal"), "{err}");
    }
}
