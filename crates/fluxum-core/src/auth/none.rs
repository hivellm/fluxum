//! Built-in `none` provider: dev-mode, accept-anything authentication
//! (SPEC-009 AUTH-031/AUTH-040).
//!
//! Any token bytes are accepted and `Identity = SHA-256(token)`. Startup
//! rejects this provider on non-loopback listen addresses
//! ([`super::enforce_loopback_guard`]) so it can never be exposed on a
//! public interface.

use super::{AuthClaims, AuthProvider};

/// Dev-mode provider (`auth.provider: none`): loopback-only, no validation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoneProvider;

impl AuthProvider for NoneProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        // AUTH-040: accept any token bytes; canonical form = raw bytes.
        Ok(AuthClaims {
            canonical_token: token.to_vec(),
            display_name: None,
            roles: Vec::new(),
            expires_at: None,
        })
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        // Non-expiring: the same token is returned (AUTH-022).
        Ok(token.to_vec())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::Identity;

    #[test]
    fn accepts_any_token_with_sha256_identity() {
        let p = NoneProvider;
        for token in [b"dev-user".as_slice(), b"", &[0xff, 0x00, 0x7f]] {
            let claims = p.authenticate(token).unwrap();
            assert_eq!(claims.canonical_token, token);
            assert_eq!(claims.identity(), Identity::from_token(token));
            assert!(claims.expires_at.is_none());
            assert!(claims.roles.is_empty());
        }
    }

    #[test]
    fn identity_is_stable_across_reconnect_and_refresh() {
        let p = NoneProvider;
        let first = p.authenticate(b"dev-user").unwrap().identity();
        let second = p.authenticate(b"dev-user").unwrap().identity();
        assert_eq!(first, second);

        let refreshed = p.refresh(b"dev-user").unwrap();
        assert_eq!(refreshed, b"dev-user");
        assert_eq!(p.authenticate(&refreshed).unwrap().identity(), first);

        assert_ne!(first, p.authenticate(b"other-user").unwrap().identity());
    }
}
