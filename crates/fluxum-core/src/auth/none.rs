//! Built-in `none` provider: dev-mode, accept-anything authentication
//! (SPEC-009 AUTH-031/AUTH-040).
//!
//! Any token bytes are accepted and `Identity = SHA-256(token)`. Startup
//! rejects this provider on non-loopback listen addresses
//! ([`super::enforce_loopback_guard`]) so it can never be exposed on a
//! public interface.

use std::collections::HashSet;
use std::sync::Mutex;

use super::{AuthClaims, AuthProvider};
use crate::types::Identity;

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

/// The permissive provider (`none`) bounded by a distinct-identity cap
/// (SPEC-009 SEC-062, OWASP F-020). `none` accepts any token as its own
/// identity, so without a bound a local caller could mint unbounded distinct
/// identities. This wraps [`NoneProvider`] and caps how many *distinct*
/// identities it will admit: a never-seen identity past the cap is refused,
/// while an already-admitted one keeps working. `none` is loopback-only
/// (AUTH-040), so this is defense-in-depth for dev mode.
#[derive(Debug)]
pub struct BoundedNoneProvider {
    inner: NoneProvider,
    cap: usize,
    seen: Mutex<HashSet<Identity>>,
}

impl BoundedNoneProvider {
    /// A permissive provider admitting at most `cap` distinct identities
    /// (`0` = unbounded).
    pub fn new(cap: u32) -> Self {
        Self {
            inner: NoneProvider,
            cap: cap as usize,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// The number of distinct identities admitted so far (test introspection).
    #[cfg(test)]
    pub fn admitted(&self) -> usize {
        self.seen.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl AuthProvider for BoundedNoneProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        let claims = self.inner.authenticate(token)?;
        if self.cap == 0 {
            return Ok(claims); // unbounded
        }
        let identity = claims.identity();
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if seen.contains(&identity) {
            return Ok(claims); // an already-admitted identity always works
        }
        if seen.len() >= self.cap {
            return Err(format!(
                "permissive-auth distinct-identity cap ({}) reached (SEC-062); refusing a new \
                 identity. Raise auth.max_permissive_identities or use a real auth provider",
                self.cap
            ));
        }
        seen.insert(identity);
        Ok(claims)
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        self.inner.refresh(token)
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
    fn bounded_permissive_caps_distinct_identities() {
        let p = BoundedNoneProvider::new(2);
        // Two distinct identities are admitted.
        p.authenticate(b"alice").unwrap();
        p.authenticate(b"bob").unwrap();
        assert_eq!(p.admitted(), 2);
        // A third *new* identity is refused (SEC-062).
        let err = p.authenticate(b"carol").unwrap_err();
        assert!(err.contains("cap"), "{err}");
        // But an already-admitted identity keeps working.
        p.authenticate(b"alice").unwrap();
        assert_eq!(p.admitted(), 2);
    }

    #[test]
    fn a_zero_cap_is_unbounded() {
        let p = BoundedNoneProvider::new(0);
        for i in 0..1000u32 {
            p.authenticate(format!("user-{i}").as_bytes()).unwrap();
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
