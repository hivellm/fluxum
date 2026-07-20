//! Built-in `jwt` provider: HS256 JWT validation via `jsonwebtoken`
//! (SPEC-009 AUTH-031).
//!
//! Identity is claims-based (AUTH-001): `canonical_token = "{iss}|{sub}"`,
//! so `Identity = SHA-256(iss || "|" || sub)`. Any number of distinct tokens
//! carrying the same `(iss, sub)` — rotated, re-signed, refreshed, or with a
//! different expiry — map to the same identity by construction (AUTH-002).
//! `iss` and `sub` must be non-empty for the derivation to be accepted.

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use super::{AuthClaims, AuthProvider};
use crate::types::Timestamp;

/// Default lifetime of a refreshed JWT, in seconds.
pub const DEFAULT_REFRESH_TTL_SECS: u64 = 3600;

/// Registered + private claims Fluxum understands.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    #[serde(default)]
    iss: String,
    #[serde(default)]
    sub: String,
    /// Seconds since Unix epoch (required; validated by `jsonwebtoken`).
    exp: u64,
    /// Optional display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// Optional roles for RBAC (AUTH-070).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    roles: Vec<String>,
}

/// JWT provider (`auth.provider: jwt`). Symmetric HS256 holds an
/// [`EncodingKey`] and can mint (and refresh) tokens; an asymmetric
/// verify-only provider (SEC-061, F-019) holds only the public
/// [`DecodingKey`], so a database compromise cannot forge tokens.
pub struct JwtProvider {
    /// `None` for a verify-only asymmetric provider (cannot mint).
    encoding: Option<EncodingKey>,
    decoding: DecodingKey,
    algorithm: Algorithm,
    validation: Validation,
    refresh_ttl_secs: u64,
}

impl JwtProvider {
    /// Create a symmetric HS256 provider from the shared secret
    /// (`auth.secret`). The DB holds the minting secret.
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        let secret = secret.as_ref();
        Self {
            encoding: Some(EncodingKey::from_secret(secret)),
            decoding: DecodingKey::from_secret(secret),
            algorithm: Algorithm::HS256,
            // `exp` required and validated (default 60s leeway).
            validation: Validation::new(Algorithm::HS256),
            refresh_ttl_secs: DEFAULT_REFRESH_TTL_SECS,
        }
    }

    /// Create an asymmetric **verify-only** provider from a PEM public key
    /// (SEC-061): the DB holds no token-minting material. `refresh` returns
    /// the presented token unchanged (a new token must come from the external
    /// issuer). `algorithm` selects RS256, ES256, or Ed25519.
    ///
    /// # Errors
    /// The PEM is not a valid public key for `algorithm`.
    pub fn verify_only(algorithm: Algorithm, public_key_pem: &[u8]) -> Result<Self, String> {
        let decoding = match algorithm {
            Algorithm::RS256 => DecodingKey::from_rsa_pem(public_key_pem),
            Algorithm::ES256 => DecodingKey::from_ec_pem(public_key_pem),
            Algorithm::EdDSA => DecodingKey::from_ed_pem(public_key_pem),
            other => return Err(format!("unsupported asymmetric JWT algorithm {other:?}")),
        }
        .map_err(|e| format!("jwt public key: {e}"))?;
        Ok(Self {
            encoding: None,
            decoding,
            algorithm,
            validation: Validation::new(algorithm),
            refresh_ttl_secs: DEFAULT_REFRESH_TTL_SECS,
        })
    }

    /// Override the lifetime granted to refreshed tokens (AUTH-022).
    pub fn with_refresh_ttl(mut self, seconds: u64) -> Self {
        self.refresh_ttl_secs = seconds;
        self
    }

    /// Issue a token for `(iss, sub)` expiring `ttl_secs` from now.
    ///
    /// Exposed for tooling and tests; production tokens normally come from
    /// the application's own issuer sharing the HS256 secret.
    pub fn issue(
        &self,
        iss: &str,
        sub: &str,
        ttl_secs: u64,
    ) -> std::result::Result<Vec<u8>, String> {
        self.encode(&Claims {
            iss: iss.to_owned(),
            sub: sub.to_owned(),
            exp: now_secs().saturating_add(ttl_secs),
            name: None,
            roles: Vec::new(),
        })
    }

    fn encode(&self, claims: &Claims) -> std::result::Result<Vec<u8>, String> {
        let encoding = self.encoding.as_ref().ok_or(
            "this JWT provider is verify-only (asymmetric); it holds no signing key and cannot \
             mint tokens (SEC-061)",
        )?;
        jsonwebtoken::encode(&Header::new(self.algorithm), claims, encoding)
            .map(String::into_bytes)
            .map_err(|e| format!("jwt signing failed: {e}"))
    }

    fn decode(&self, token: &[u8]) -> std::result::Result<Claims, String> {
        let token = std::str::from_utf8(token).map_err(|_| "jwt is not valid UTF-8")?;
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| format!("jwt validation failed: {e}"))
    }
}

impl AuthProvider for JwtProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        let claims = self.decode(token)?;
        if claims.iss.is_empty() || claims.sub.is_empty() {
            return Err("jwt must carry non-empty 'iss' and 'sub' claims".to_owned());
        }
        // Stable claims-based canonical form (AUTH-001): rotation-proof.
        let canonical_token = format!("{}|{}", claims.iss, claims.sub).into_bytes();
        let expires_at = i64::try_from(claims.exp.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        Ok(AuthClaims {
            canonical_token,
            display_name: claims.name,
            roles: claims.roles,
            expires_at: Some(Timestamp::from_micros(expires_at)),
        })
    }

    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        // Verify-only (asymmetric): the DB cannot re-sign, so the presented
        // token is returned unchanged — the identity is invariant and a fresh
        // token comes from the external issuer (AUTH-022, SEC-061).
        if self.encoding.is_none() {
            return Ok(token.to_vec());
        }
        // Symmetric: a new JWT with extended expiry and unchanged (iss, sub) —
        // the refreshed bytes differ but the identity is invariant (AUTH-022).
        let mut claims = self.decode(token)?;
        claims.exp = now_secs().saturating_add(self.refresh_ttl_secs);
        self.encode(&claims)
    }
}

fn now_secs() -> u64 {
    u64::try_from(Timestamp::now().as_micros() / 1_000_000).unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::types::Identity;

    const SECRET: &[u8] = b"jwt-test-secret";

    // A throwaway EC P-256 keypair for the SEC-061 verify-only test.
    const EC_PRIV_PK8: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgoPsDIASu9SvjLyxj\n\
AsyKHdbzkTogSMbEYfuFlIpvhLqhRANCAARvo2K4Oet72HQdW8AFHo0T8hYgjhGz\n\
8+zU7XZyYTjZkMaDyieORmEIQSOvAdhycgbq/TfZyPJe3WUNDNuAAqzN\n\
-----END PRIVATE KEY-----\n";
    const EC_PUB: &str = "-----BEGIN PUBLIC KEY-----\n\
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEb6NiuDnre9h0HVvABR6NE/IWII4R\n\
s/Ps1O12cmE42ZDGg8onjkZhCEEjrwHYcnIG6v032cjyXt1lDQzbgAKszQ==\n\
-----END PUBLIC KEY-----\n";

    fn provider() -> JwtProvider {
        JwtProvider::new(SECRET)
    }

    /// Sign an arbitrary claims object with the test secret.
    fn sign(claims: &serde_json::Value) -> Vec<u8> {
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
        .into_bytes()
    }

    #[test]
    fn asymmetric_verify_only_validates_but_cannot_mint() {
        // SEC-061 (F-019): the verify-only provider holds only the public key.
        let p = JwtProvider::verify_only(Algorithm::ES256, EC_PUB.as_bytes()).unwrap();

        // A token signed by the matching *private* key (the external issuer)
        // verifies and yields the claims-based identity.
        let claims = serde_json::json!({
            "iss": "https://issuer.example",
            "sub": "user-7",
            "exp": now_secs() + 600,
        });
        let signing = EncodingKey::from_ec_pem(EC_PRIV_PK8.as_bytes()).unwrap();
        let token = jsonwebtoken::encode(&Header::new(Algorithm::ES256), &claims, &signing)
            .unwrap()
            .into_bytes();
        let out = p.authenticate(&token).unwrap();
        assert_eq!(
            out.identity(),
            Identity::from_claims("https://issuer.example", "user-7")
        );

        // The DB cannot mint: no signing key.
        let mint = p.issue("https://issuer.example", "user-7", 600);
        assert!(mint.unwrap_err().contains("verify-only"));

        // Refresh returns the presented token unchanged (identity invariant).
        assert_eq!(p.refresh(&token).unwrap(), token);

        // A token signed with the wrong (HS256) key is rejected.
        let forged = sign(&claims);
        assert!(p.authenticate(&forged).is_err());
    }

    #[test]
    fn valid_token_yields_claims_based_identity() {
        let p = provider();
        let token = p.issue("https://auth.example.com", "user-42", 600).unwrap();
        let claims = p.authenticate(&token).unwrap();
        assert_eq!(claims.canonical_token, b"https://auth.example.com|user-42");
        assert_eq!(
            claims.identity(),
            Identity::from_claims("https://auth.example.com", "user-42")
        );
        assert!(claims.expires_at.unwrap().as_micros() > Timestamp::now().as_micros());
    }

    #[test]
    fn identity_is_stable_across_token_rotation() {
        // Two distinct tokens (different exp → different bytes/signature)
        // carrying the same (iss, sub) map to the SAME identity (AUTH-001/002).
        let p = provider();
        let token_a = p.issue("iss", "user-1", 100).unwrap();
        let token_b = p.issue("iss", "user-1", 5000).unwrap();
        assert_ne!(token_a, token_b);
        assert_eq!(
            p.authenticate(&token_a).unwrap().identity(),
            p.authenticate(&token_b).unwrap().identity()
        );

        // A re-signing with a different key still yields the same identity
        // once validated by a provider holding that key (claims unchanged).
        let other = JwtProvider::new(b"rotated-signing-key");
        let token_c = other.issue("iss", "user-1", 100).unwrap();
        assert_eq!(
            other.authenticate(&token_c).unwrap().identity(),
            p.authenticate(&token_a).unwrap().identity()
        );

        // Distinct principals get distinct identities.
        let token_d = p.issue("iss", "user-2", 100).unwrap();
        assert_ne!(
            p.authenticate(&token_a).unwrap().identity(),
            p.authenticate(&token_d).unwrap().identity()
        );
    }

    #[test]
    fn refresh_extends_expiry_without_changing_identity() {
        let p = provider().with_refresh_ttl(7200);
        let original = p.issue("iss", "user-1", 60).unwrap();
        let refreshed = p.refresh(&original).unwrap();
        assert_ne!(original, refreshed, "refresh mints a new token");

        let before = p.authenticate(&original).unwrap();
        let after = p.authenticate(&refreshed).unwrap();
        assert_eq!(before.identity(), after.identity(), "AUTH-022 invariant");
        assert!(after.expires_at.unwrap() > before.expires_at.unwrap());
    }

    #[test]
    fn invalid_tokens_are_rejected() {
        let p = provider();

        // Garbage bytes / not a JWT.
        assert!(p.authenticate(b"not-a-jwt").is_err());
        assert!(p.authenticate(&[0xff, 0xfe]).is_err());

        // Wrong signing key.
        let forged = JwtProvider::new(b"attacker-key")
            .issue("iss", "sub", 600)
            .unwrap();
        assert!(p.authenticate(&forged).is_err());

        // Expired (beyond the 60s default leeway).
        let expired = sign(&serde_json::json!({
            "iss": "iss", "sub": "sub", "exp": now_secs() - 3600,
        }));
        let err = p.authenticate(&expired).unwrap_err();
        assert!(err.contains("jwt validation failed"), "{err}");
        assert!(
            p.refresh(&expired).is_err(),
            "expired tokens cannot refresh"
        );

        // Missing exp claim.
        let no_exp = sign(&serde_json::json!({ "iss": "iss", "sub": "sub" }));
        assert!(p.authenticate(&no_exp).is_err());
    }

    #[test]
    fn empty_iss_or_sub_is_rejected() {
        let p = provider();
        for claims in [
            serde_json::json!({ "sub": "user", "exp": now_secs() + 600 }),
            serde_json::json!({ "iss": "iss", "exp": now_secs() + 600 }),
            serde_json::json!({ "iss": "", "sub": "user", "exp": now_secs() + 600 }),
            serde_json::json!({ "iss": "iss", "sub": "", "exp": now_secs() + 600 }),
        ] {
            let err = p.authenticate(&sign(&claims)).unwrap_err();
            assert!(err.contains("iss"), "{err}");
        }
    }

    #[test]
    fn optional_name_and_roles_claims_flow_through() {
        let p = provider();
        let token = sign(&serde_json::json!({
            "iss": "iss", "sub": "user", "exp": now_secs() + 600,
            "name": "Ada", "roles": ["admin", "auditor"],
        }));
        let claims = p.authenticate(&token).unwrap();
        assert_eq!(claims.display_name.as_deref(), Some("Ada"));
        assert_eq!(claims.roles, vec!["admin".to_owned(), "auditor".to_owned()]);

        // Roles/name survive refresh.
        let refreshed = p.refresh(&token).unwrap();
        let claims = p.authenticate(&refreshed).unwrap();
        assert_eq!(claims.display_name.as_deref(), Some("Ada"));
        assert_eq!(claims.roles.len(), 2);
    }
}
