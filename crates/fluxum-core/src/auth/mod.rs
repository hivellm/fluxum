//! Pluggable authentication and identity derivation (SPEC-009).
//!
//! - [`AuthProvider`] — the object-safe trait every auth scheme implements
//!   (AUTH-030); installed as `Arc<dyn AuthProvider>` and replaceable by
//!   application code (AUTH-032).
//! - Built-in providers (AUTH-031): [`TokenProvider`] (HMAC-SHA256 signed
//!   opaque tokens), [`JwtProvider`] (HS256 JWT, claims-based identity), and
//!   [`NoneProvider`] (dev mode, loopback-only per AUTH-040).
//! - [`ServerPeerRegistry`] — trusted server-to-server peers with identities
//!   in the reserved `SHA-256("SERVER:" + name)` namespace (AUTH-060/061).
//! - [`Authenticator`] — combines the provider and the peer registry into the
//!   single entry point the connection layer calls for `Authenticate`
//!   messages, producing an [`AuthOutcome`] (identity + refreshed token +
//!   privilege flags, AUTH-021/022/062).

mod jwt;
mod none;
mod token;

pub use jwt::JwtProvider;
pub use none::NoneProvider;
pub use token::TokenProvider;

use std::net::IpAddr;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::config::{AuthConfig, AuthProvider as AuthProviderKind, Config, ServerPeer};
use crate::error::{FluxumError, Result};
use crate::types::{Identity, Timestamp};

/// Reserved canonical-token namespace for server identities (AUTH-060).
///
/// `ServerIdentity = SHA-256("SERVER:" + name)`; client canonical tokens are
/// never allowed to start with this prefix, so a user can never forge a
/// server identity by presenting crafted token bytes or claims.
pub const SERVER_NAMESPACE_PREFIX: &[u8] = b"SERVER:";

/// The documented startup error for a non-loopback `none` provider (AUTH-040).
pub const LOOPBACK_GUARD_ERROR: &str =
    "auth.provider=none is only permitted when the listen address is 127.0.0.1 or ::1";

// ---------------------------------------------------------------------------
// AuthProvider trait + claims
// ---------------------------------------------------------------------------

/// Pluggable, object-safe authentication (SPEC-009 AUTH-030).
///
/// Installed as `Arc<dyn AuthProvider>`; the built-in providers are selected
/// from `auth.provider` in the configuration ([`provider_from_config`]) and a
/// custom implementation can be registered at startup instead (AUTH-032).
pub trait AuthProvider: Send + Sync {
    /// Validate a token and return its claims, or an error reason.
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String>;

    /// Return a refreshed token, or the same token for non-expiring schemes.
    ///
    /// Invariant (AUTH-002/AUTH-022): the refreshed token MUST map to the
    /// same [`AuthClaims::canonical_token`] — token refresh never changes
    /// the caller's [`Identity`].
    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String>;
}

/// Validated claims returned by an [`AuthProvider`] (SPEC-009 AUTH-030).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthClaims {
    /// Used for Identity derivation; MUST be stable across refreshes
    /// (AUTH-001/AUTH-002).
    pub canonical_token: Vec<u8>,
    /// Optional human-readable name supplied by the provider.
    pub display_name: Option<String>,
    /// Roles for RBAC gating (AUTH-070; empty when unsupported).
    pub roles: Vec<String>,
    /// µs since Unix epoch; `None` = no expiry.
    pub expires_at: Option<Timestamp>,
}

impl AuthClaims {
    /// Derive the caller identity: `SHA-256(canonical_token)` (AUTH-001).
    pub fn identity(&self) -> Identity {
        Identity::from_token(&self.canonical_token)
    }
}

// ---------------------------------------------------------------------------
// Server-to-server identity
// ---------------------------------------------------------------------------

/// Derive a server-peer identity: `SHA-256("SERVER:" + name)` (AUTH-060).
pub fn server_identity(name: &str) -> Identity {
    let mut hasher = Sha256::new();
    hasher.update(SERVER_NAMESPACE_PREFIX);
    hasher.update(name.as_bytes());
    Identity::from_bytes(hasher.finalize().into())
}

/// One configured server peer, resolved from `auth.server_peers`.
#[derive(Clone, Debug)]
struct PeerEntry {
    name: String,
    /// SHA-256 of the shared token; compared digest-to-digest so raw token
    /// bytes are neither retained nor compared byte-by-byte.
    token_digest: [u8; 32],
    identity: Identity,
}

/// Registry of trusted server peers from `config.yml` (AUTH-061).
#[derive(Clone, Debug, Default)]
pub struct ServerPeerRegistry {
    peers: Vec<PeerEntry>,
}

impl ServerPeerRegistry {
    /// An empty registry (no server peers configured).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build the registry from the `auth.server_peers` config section.
    ///
    /// Rejects empty names/tokens and duplicate names or tokens: each token
    /// must resolve to exactly one peer identity.
    pub fn from_config(peers: &[ServerPeer]) -> Result<Self> {
        let mut entries: Vec<PeerEntry> = Vec::with_capacity(peers.len());
        for peer in peers {
            if peer.name.is_empty() {
                return Err(FluxumError::config(
                    "auth.server_peers: peer name must be non-empty",
                ));
            }
            if peer.token.expose_str().is_empty() {
                return Err(FluxumError::config(format!(
                    "auth.server_peers: peer '{}' has an empty token",
                    peer.name
                )));
            }
            let token_digest: [u8; 32] = Sha256::digest(peer.token.expose_str().as_bytes()).into();
            if entries.iter().any(|e| e.name == peer.name) {
                return Err(FluxumError::config(format!(
                    "auth.server_peers: duplicate peer name '{}'",
                    peer.name
                )));
            }
            if entries.iter().any(|e| e.token_digest == token_digest) {
                return Err(FluxumError::config(format!(
                    "auth.server_peers: peer '{}' reuses another peer's token",
                    peer.name
                )));
            }
            entries.push(PeerEntry {
                name: peer.name.clone(),
                token_digest,
                identity: server_identity(&peer.name),
            });
        }
        Ok(Self { peers: entries })
    }

    /// Resolve a presented token to `(peer_name, ServerIdentity)`, if it
    /// matches a configured server peer (AUTH-061).
    pub fn lookup_token(&self, token: &[u8]) -> Option<(&str, Identity)> {
        let digest: [u8; 32] = Sha256::digest(token).into();
        self.peers
            .iter()
            .find(|e| e.token_digest == digest)
            .map(|e| (e.name.as_str(), e.identity))
    }

    /// Whether an identity belongs to a configured server peer (AUTH-063).
    pub fn is_server_identity(&self, identity: &Identity) -> bool {
        self.peers.iter().any(|e| e.identity == *identity)
    }

    /// Number of configured peers.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether no peers are configured.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Config wiring
// ---------------------------------------------------------------------------

/// Instantiate the configured built-in provider (AUTH-031).
pub fn provider_from_config(auth: &AuthConfig) -> Result<Arc<dyn AuthProvider>> {
    match auth.provider {
        AuthProviderKind::Token => Ok(Arc::new(TokenProvider::new(required_secret(auth)?))),
        AuthProviderKind::Jwt => Ok(Arc::new(JwtProvider::new(required_secret(auth)?))),
        AuthProviderKind::None => Ok(Arc::new(NoneProvider)),
    }
}

fn required_secret(auth: &AuthConfig) -> Result<&[u8]> {
    match auth.secret.as_ref() {
        Some(secret) if !secret.expose_str().is_empty() => Ok(secret.expose_str().as_bytes()),
        _ => Err(FluxumError::config(format!(
            "auth.secret: required for auth.provider '{:?}'",
            auth.provider
        ))),
    }
}

/// Enforce the dev-mode loopback guard (AUTH-040): `auth.provider: none`
/// is rejected unless the listen address is a loopback address.
pub fn enforce_loopback_guard(auth: &AuthConfig, listen_host: &str) -> Result<()> {
    if auth.provider == AuthProviderKind::None && !is_loopback_host(listen_host) {
        return Err(FluxumError::config(LOOPBACK_GUARD_ERROR));
    }
    Ok(())
}

/// Whether `host` names a loopback address (or `localhost`). Also
/// `0.0.0.0`/`::` are NOT loopback — they bind every interface, so they are a
/// public bind for the SEC-059 plaintext guard.
pub fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

// ---------------------------------------------------------------------------
// Authenticator
// ---------------------------------------------------------------------------

/// The result of a successful `Authenticate` (feeds `AuthResult`, AUTH-021).
#[derive(Clone, Debug)]
pub struct AuthOutcome {
    /// The caller's stable identity (AUTH-001/AUTH-060).
    pub identity: Identity,
    /// Refreshed token to return in `AuthResult.token` (AUTH-022); identical
    /// to the input for non-expiring schemes and server peers.
    pub refreshed_token: Vec<u8>,
    /// Optional display name from the provider (peer name for server peers).
    pub display_name: Option<String>,
    /// Roles for RBAC gating (AUTH-070).
    pub roles: Vec<String>,
    /// Token expiry, if the scheme expires.
    pub expires_at: Option<Timestamp>,
    /// `Some(name)` when authenticated as a configured server peer.
    pub server_peer: Option<String>,
    /// Server peers bypass all `#[visibility]` RLS filters (AUTH-062).
    pub bypass_rls: bool,
}

/// Combines the active [`AuthProvider`] with the [`ServerPeerRegistry`]:
/// the single authentication entry point for the connection layer.
pub struct Authenticator {
    provider: Arc<dyn AuthProvider>,
    peers: ServerPeerRegistry,
}

impl Authenticator {
    /// Build from the resolved server configuration: enforces the loopback
    /// guard (AUTH-040), instantiates the configured provider (AUTH-031),
    /// and loads the server-peer registry (AUTH-061).
    pub fn from_config(config: &Config) -> Result<Self> {
        enforce_loopback_guard(&config.auth, &config.server.tcp_host)?;
        Ok(Self {
            provider: provider_from_config(&config.auth)?,
            peers: ServerPeerRegistry::from_config(&config.auth.server_peers)?,
        })
    }

    /// Install a custom provider (AUTH-032), e.g. via `fluxum::ServerBuilder`.
    pub fn with_provider(provider: Arc<dyn AuthProvider>, peers: ServerPeerRegistry) -> Self {
        Self { provider, peers }
    }

    /// The server-peer registry (for `ctx.is_server_identity()`, AUTH-063).
    pub fn peers(&self) -> &ServerPeerRegistry {
        &self.peers
    }

    /// Authenticate a presented token (AUTH-021).
    ///
    /// Order: server-peer tokens are recognised first (AUTH-061) and receive
    /// the privileged `SHA-256("SERVER:" + name)` identity with the RLS
    /// bypass flag set (AUTH-062); otherwise the provider validates the token
    /// and the identity is `SHA-256(canonical_token)` (AUTH-001). Canonical
    /// tokens in the reserved `SERVER:` namespace are rejected so client
    /// tokens can never collide with a server identity (AUTH-060).
    pub fn authenticate(&self, token: &[u8]) -> Result<AuthOutcome> {
        if let Some((name, identity)) = self.peers.lookup_token(token) {
            return Ok(AuthOutcome {
                identity,
                refreshed_token: token.to_vec(),
                display_name: Some(name.to_owned()),
                roles: Vec::new(),
                expires_at: None,
                server_peer: Some(name.to_owned()),
                bypass_rls: true,
            });
        }

        let claims = self.provider.authenticate(token).map_err(auth_failed)?;
        if claims.canonical_token.starts_with(SERVER_NAMESPACE_PREFIX) {
            return Err(auth_failed(
                "canonical token uses the reserved SERVER identity namespace",
            ));
        }
        let refreshed_token = self.provider.refresh(token).map_err(auth_failed)?;
        Ok(AuthOutcome {
            identity: claims.identity(),
            refreshed_token,
            display_name: claims.display_name,
            roles: claims.roles,
            expires_at: claims.expires_at,
            server_peer: None,
            bypass_rls: false,
        })
    }

    /// Refresh a token without re-running the full flow (AUTH-022).
    ///
    /// Server-peer tokens are long-lived shared secrets and refresh to
    /// themselves; provider tokens delegate to [`AuthProvider::refresh`].
    pub fn refresh(&self, token: &[u8]) -> Result<Vec<u8>> {
        if self.peers.lookup_token(token).is_some() {
            return Ok(token.to_vec());
        }
        self.provider.refresh(token).map_err(auth_failed)
    }
}

/// Wrap a provider reason into the wire error shape (AUTH-021).
fn auth_failed(reason: impl std::fmt::Display) -> FluxumError {
    FluxumError::Auth(format!("authentication failed: {reason}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn peer(name: &str, token: &str) -> ServerPeer {
        ServerPeer {
            name: name.to_owned(),
            token: token.to_owned().into(),
        }
    }

    fn auth_config(provider: AuthProviderKind, secret: Option<&str>) -> AuthConfig {
        AuthConfig {
            provider,
            secret: secret.map(|s| s.to_owned().into()),
            server_peers: Vec::new(),
        }
    }

    // -- provider matrix (task 1.6) ------------------------------------------

    #[test]
    fn provider_from_config_selects_the_configured_kind() {
        // token: only correctly signed tokens pass.
        let provider =
            provider_from_config(&auth_config(AuthProviderKind::Token, Some("s3cret"))).unwrap();
        let minted = TokenProvider::new(b"s3cret".as_slice())
            .mint(b"user-1")
            .unwrap();
        assert!(provider.authenticate(&minted).is_ok());
        assert!(provider.authenticate(b"not-signed").is_err());

        // jwt: arbitrary bytes are rejected (full jwt matrix in jwt.rs tests).
        let provider =
            provider_from_config(&auth_config(AuthProviderKind::Jwt, Some("s3cret"))).unwrap();
        assert!(provider.authenticate(b"not-a-jwt").is_err());

        // none: everything is accepted.
        let provider = provider_from_config(&auth_config(AuthProviderKind::None, None)).unwrap();
        assert!(provider.authenticate(b"anything").is_ok());
    }

    #[test]
    fn provider_from_config_requires_secret_for_token_and_jwt() {
        for kind in [AuthProviderKind::Token, AuthProviderKind::Jwt] {
            for secret in [None, Some("")] {
                let err = provider_from_config(&auth_config(kind, secret))
                    .err()
                    .unwrap();
                assert!(err.to_string().contains("auth.secret"), "{err}");
            }
        }
        assert!(provider_from_config(&auth_config(AuthProviderKind::None, None)).is_ok());
    }

    // -- loopback guard (AUTH-040, task 1.5) ---------------------------------

    #[test]
    fn none_provider_requires_loopback_listen_address() {
        let none = auth_config(AuthProviderKind::None, None);
        for host in ["127.0.0.1", "::1", "127.0.0.53", "localhost"] {
            assert!(enforce_loopback_guard(&none, host).is_ok(), "{host}");
        }
        for host in ["0.0.0.0", "::", "10.1.2.3", "192.168.0.1", "example.com"] {
            let err = enforce_loopback_guard(&none, host).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("config error: {LOOPBACK_GUARD_ERROR}")
            );
        }
    }

    #[test]
    fn loopback_guard_ignores_authenticating_providers() {
        let token = auth_config(AuthProviderKind::Token, Some("s"));
        assert!(enforce_loopback_guard(&token, "0.0.0.0").is_ok());
        let jwt = auth_config(AuthProviderKind::Jwt, Some("s"));
        assert!(enforce_loopback_guard(&jwt, "0.0.0.0").is_ok());
    }

    #[test]
    fn authenticator_from_config_applies_the_guard() {
        let mut config = Config {
            auth: auth_config(AuthProviderKind::None, None),
            ..Config::default()
        };
        config.server.tcp_host = "127.0.0.1".to_owned();
        assert!(Authenticator::from_config(&config).is_ok());

        config.server.tcp_host = "0.0.0.0".to_owned();
        let err = Authenticator::from_config(&config).err().unwrap();
        assert!(err.to_string().contains(LOOPBACK_GUARD_ERROR), "{err}");
    }

    // -- server identity namespace (AUTH-060/061/062) ------------------------

    #[test]
    fn server_identity_matches_the_spec_formula() {
        assert_eq!(
            server_identity("ingestion_service"),
            Identity::from_token(b"SERVER:ingestion_service")
        );
        assert_ne!(server_identity("a"), server_identity("b"));
    }

    #[test]
    fn server_peer_authentication_yields_privileged_identity() {
        let mut config = Config {
            auth: auth_config(AuthProviderKind::None, None),
            ..Config::default()
        };
        config.auth.server_peers = vec![peer("ingestion_service", "peer-secret-1")];
        let auth = Authenticator::from_config(&config).unwrap();

        let outcome = auth.authenticate(b"peer-secret-1").unwrap();
        assert_eq!(outcome.identity, server_identity("ingestion_service"));
        assert_eq!(outcome.server_peer.as_deref(), Some("ingestion_service"));
        assert!(outcome.bypass_rls);
        assert_eq!(outcome.refreshed_token, b"peer-secret-1");
        assert!(auth.peers().is_server_identity(&outcome.identity));

        // A normal client authentication is unprivileged.
        let client = auth.authenticate(b"any-user-token").unwrap();
        assert!(client.server_peer.is_none());
        assert!(!client.bypass_rls);
        assert!(!auth.peers().is_server_identity(&client.identity));
    }

    #[test]
    fn client_tokens_cannot_forge_server_identities() {
        // Even with the permissive `none` provider, canonical tokens in the
        // reserved namespace are rejected — SHA-256("SERVER:x") is unreachable
        // from client input (task 1.6: server namespace non-collision).
        let registry = ServerPeerRegistry::from_config(&[peer("x", "real-token")]).unwrap();
        let auth = Authenticator::with_provider(Arc::new(NoneProvider), registry);
        let err = auth.authenticate(b"SERVER:x").unwrap_err();
        assert!(err.to_string().contains("reserved SERVER"), "{err}");

        // The real peer token still works.
        assert_eq!(
            auth.authenticate(b"real-token").unwrap().identity,
            server_identity("x")
        );
    }

    #[test]
    fn registry_rejects_invalid_and_duplicate_peers() {
        assert!(ServerPeerRegistry::from_config(&[peer("", "t")]).is_err());
        assert!(ServerPeerRegistry::from_config(&[peer("a", "")]).is_err());
        assert!(ServerPeerRegistry::from_config(&[peer("a", "t1"), peer("a", "t2")]).is_err());
        assert!(ServerPeerRegistry::from_config(&[peer("a", "t"), peer("b", "t")]).is_err());

        let registry =
            ServerPeerRegistry::from_config(&[peer("a", "t1"), peer("b", "t2")]).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(registry.lookup_token(b"t2").map(|(n, _)| n), Some("b"));
        assert!(registry.lookup_token(b"t3").is_none());
        assert!(ServerPeerRegistry::empty().is_empty());
    }

    // -- custom provider pluggability (AUTH-032) ------------------------------

    struct UpperProvider;

    impl AuthProvider for UpperProvider {
        fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
            // Custom stable derivation: uppercased token bytes.
            Ok(AuthClaims {
                canonical_token: token.to_ascii_uppercase(),
                display_name: Some("custom".to_owned()),
                roles: vec!["admin".to_owned()],
                expires_at: None,
            })
        }

        fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
            Ok(token.to_vec())
        }
    }

    #[test]
    fn custom_provider_drives_identity_derivation() {
        let auth =
            Authenticator::with_provider(Arc::new(UpperProvider), ServerPeerRegistry::empty());
        let a = auth.authenticate(b"user-1").unwrap();
        let b = auth.authenticate(b"USER-1").unwrap();
        // Both tokens share the custom canonical form → same identity.
        assert_eq!(a.identity, b.identity);
        assert_eq!(a.identity, Identity::from_token(b"USER-1"));
        assert_eq!(a.roles, vec!["admin".to_owned()]);
        assert_eq!(a.display_name.as_deref(), Some("custom"));
    }

    // -- refresh semantics (AUTH-022) -----------------------------------------

    #[test]
    fn refresh_returns_same_token_for_peers_and_non_expiring_schemes() {
        let registry = ServerPeerRegistry::from_config(&[peer("svc", "svc-token")]).unwrap();
        let auth = Authenticator::with_provider(Arc::new(NoneProvider), registry);
        assert_eq!(auth.refresh(b"svc-token").unwrap(), b"svc-token");
        assert_eq!(auth.refresh(b"user-token").unwrap(), b"user-token");
    }

    #[test]
    fn provider_failures_surface_as_retryable_auth_errors() {
        let config = Config {
            auth: auth_config(AuthProviderKind::Token, Some("s3cret")),
            ..Config::default()
        };
        let auth = Authenticator::from_config(&config).unwrap();
        let err = auth.authenticate(b"garbage").unwrap_err();
        assert!(matches!(err, FluxumError::Auth(_)));
        assert!(err.to_string().contains("authentication failed:"), "{err}");

        // The same Authenticator then accepts a valid token (AUTH-021: the
        // connection stays open, the client may retry).
        let minted = TokenProvider::new(b"s3cret".as_slice()).mint(b"u").unwrap();
        assert!(auth.authenticate(&minted).is_ok());
    }
}
