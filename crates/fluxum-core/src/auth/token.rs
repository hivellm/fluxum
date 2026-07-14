//! Built-in `token` provider: HMAC-SHA256 signed opaque tokens
//! (SPEC-009 AUTH-031).
//!
//! Token wire format: `<payload> "." <hex(HMAC-SHA256(secret, payload))>`.
//! The payload is opaque application data (e.g. a user id); the signature
//! proves the token was minted by a holder of the shared secret.
//!
//! `canonical_token` is the raw token bytes, so `Identity = SHA-256(token)`
//! (AUTH-001): opaque tokens are long-lived by definition — the token value
//! itself is the stable identifier, and refresh returns the same token
//! (AUTH-022).

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{AuthClaims, AuthProvider};

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256 signed opaque-token provider (`auth.provider: token`).
pub struct TokenProvider {
    secret: Vec<u8>,
}

impl TokenProvider {
    /// Create a provider from the shared signing secret (`auth.secret`).
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    /// Mint a signed token for a payload: `payload . hex(hmac)`.
    ///
    /// Exposed for tooling and tests; production tokens are typically minted
    /// once by an operator and distributed as long-lived API keys.
    pub fn mint(&self, payload: &[u8]) -> std::result::Result<Vec<u8>, String> {
        let mut mac = self.mac()?;
        mac.update(payload);
        let sig = hex_encode(&mac.finalize().into_bytes());
        let mut token = Vec::with_capacity(payload.len() + 1 + sig.len());
        token.extend_from_slice(payload);
        token.push(b'.');
        token.extend_from_slice(sig.as_bytes());
        Ok(token)
    }

    fn mac(&self) -> std::result::Result<HmacSha256, String> {
        HmacSha256::new_from_slice(&self.secret).map_err(|e| format!("invalid HMAC key: {e}"))
    }
}

impl AuthProvider for TokenProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        let dot = token
            .iter()
            .rposition(|&b| b == b'.')
            .ok_or("malformed token: missing '.' signature separator")?;
        let (payload, sig_hex) = (&token[..dot], &token[dot + 1..]);
        let sig = hex_decode(sig_hex).ok_or("malformed token: signature is not hex")?;
        let mut mac = self.mac()?;
        mac.update(payload);
        mac.verify_slice(&sig)
            .map_err(|_| "invalid token signature")?;
        Ok(AuthClaims {
            canonical_token: token.to_vec(),
            display_name: None,
            roles: Vec::new(),
            expires_at: None,
        })
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        // Non-expiring scheme: validate, then return the identical token
        // (AUTH-022) — identity is trivially stable across refresh.
        self.authenticate(token)?;
        Ok(token.to_vec())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn hex_decode(hex: &[u8]) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let (pairs, _) = hex.as_chunks::<2>();
    pairs
        .iter()
        .map(|pair| Some((hex_val(pair[0])? << 4) | hex_val(pair[1])?))
        .collect()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::Identity;

    fn provider() -> TokenProvider {
        TokenProvider::new(b"shared-secret".as_slice())
    }

    #[test]
    fn minted_token_authenticates_and_identity_is_stable_across_reconnect() {
        let p = provider();
        let token = p.mint(b"device-7").unwrap();

        // Two authentications (a reconnect) yield byte-identical identities.
        let first = p.authenticate(&token).unwrap();
        let second = p.authenticate(&token).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.identity(), second.identity());
        assert_eq!(first.identity(), Identity::from_token(&token));
        assert_eq!(first.canonical_token, token);
        assert!(first.expires_at.is_none());
    }

    #[test]
    fn distinct_payloads_yield_distinct_identities() {
        let p = provider();
        let a = p.mint(b"user-a").unwrap();
        let b = p.mint(b"user-b").unwrap();
        assert_ne!(
            p.authenticate(&a).unwrap().identity(),
            p.authenticate(&b).unwrap().identity()
        );
    }

    #[test]
    fn tampered_or_malformed_tokens_are_rejected() {
        let p = provider();
        let mut token = p.mint(b"user-a").unwrap();

        // Flip a payload byte: signature no longer matches.
        token[0] ^= 0xff;
        assert!(p.authenticate(&token).is_err());

        // Wrong secret.
        let other = TokenProvider::new(b"other-secret".as_slice());
        let valid = p.mint(b"user-a").unwrap();
        assert!(other.authenticate(&valid).is_err());

        // Structural failures.
        assert!(p.authenticate(b"no-separator").is_err());
        assert!(p.authenticate(b"payload.nothex!!").is_err());
        assert!(p.authenticate(b"payload.abc").is_err()); // odd-length hex
        assert!(p.authenticate(b"").is_err());
    }

    #[test]
    fn refresh_returns_the_identical_token() {
        let p = provider();
        let token = p.mint(b"user-a").unwrap();
        assert_eq!(p.refresh(&token).unwrap(), token);
        assert!(p.refresh(b"garbage").is_err());
    }

    #[test]
    fn hex_roundtrip() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0x1a]), "00ff1a");
        assert_eq!(hex_decode(b"00ff1a").unwrap(), vec![0x00, 0xff, 0x1a]);
        assert_eq!(hex_decode(b"00FF1A").unwrap(), vec![0x00, 0xff, 0x1a]);
        assert!(hex_decode(b"0g").is_none());
        assert!(hex_decode(b"0").is_none());
    }
}
