//! Field-level cryptographic transforms (SPEC-017 §5, CT-030..036).
//!
//! [`ecies_seal`]/[`ecies_open`] implement the `#[encrypted(ecies, key = …)]`
//! executor: **ECIES over X25519** — an ephemeral X25519 key agreement, an
//! HKDF-SHA-256 key derivation, and an XChaCha20-Poly1305 AEAD seal. The
//! ciphertext is a self-describing envelope stored as `Bytes`, so the plaintext
//! never reaches the commit log, cold pages, checkpoints, the replication
//! stream, or any index — only the recipient key holder can recover it.
//!
//! # Sealed envelope (freeze surface, versioned)
//!
//! ```text
//! | 0  | 1  | version (0x01)                                   |
//! | 1  | 1  | scheme  (0x01 = ECIES-X25519-HKDF-XChaCha20P1305) |
//! | 2  | 32 | ephemeral X25519 public key                       |
//! | 34 | 24 | XChaCha20 nonce                                   |
//! | 58 | .. | ciphertext ++ 16-byte Poly1305 tag                |
//! ```
//!
//! # Associated data (CT-032)
//!
//! The AEAD associated data binds the ciphertext to its `(table, column,
//! primary_key)` position, so a validly-encrypted value cannot be relocated to
//! another row or column without failing authentication. The ephemeral public
//! key and the version/scheme bytes are authenticated too (they are the AAD
//! prefix), so the envelope header cannot be tampered.
//!
//! # Rotation (CT-036)
//!
//! [`ecies_open`] tries the active recipient secret first, then each retired
//! (`previous`) secret; a value sealed under an old key still decrypts, and is
//! re-sealed under the active key the next time its row is written.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};
use zeroize::Zeroize;

use crate::error::{FluxumError, Result};

/// Ed25519 signature width.
pub const SIGNATURE_LEN: usize = 64;

/// A named Ed25519 signing key for `#[signed(ed25519, by = server)]`
/// (SPEC-017 CT-033/035). Holds the server's signing key; the verifying
/// (public) key is derived. Signing-key bytes are zeroized on drop.
pub struct SignKey {
    id: String,
    signing: SigningKey,
}

impl SignKey {
    /// Build from a label and the 32-byte Ed25519 secret.
    pub fn new(id: impl Into<String>, secret: [u8; 32]) -> Self {
        Self {
            id: id.into(),
            signing: SigningKey::from_bytes(&secret),
        }
    }

    /// Parse from a label and 64 hex characters.
    pub fn from_hex(id: impl Into<String>, hex: &str) -> Result<Self> {
        Ok(Self::new(id, parse_secret(hex)?))
    }

    /// The key's label.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Sign `msg` (CT-033). Returns the 64-byte signature.
    pub fn sign(&self, msg: &[u8]) -> [u8; SIGNATURE_LEN] {
        self.signing.sign(msg).to_bytes()
    }

    /// Verify `signature` over `msg` against this key's public key (CT-034).
    pub fn verify(&self, msg: &[u8], signature: &[u8; SIGNATURE_LEN]) -> bool {
        let vk: VerifyingKey = self.signing.verifying_key();
        ed25519_dalek::Signature::try_from(&signature[..])
            .map(|sig| vk.verify(msg, &sig).is_ok())
            .unwrap_or(false)
    }
}

impl std::fmt::Debug for SignKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignKey").field("id", &self.id).finish()
    }
}

/// Verify a signature against an arbitrary Ed25519 public key — the
/// `#[signed(by = <Identity column>)]` path, where the [`crate::types::Identity`]
/// is itself the signer's public key (SPEC-009). Returns `false` on any
/// malformed input rather than erroring (CT-034 never drops the row).
pub fn ed25519_verify_with(public: &[u8; 32], msg: &[u8], signature: &[u8]) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(public) else {
        return false;
    };
    let Ok(sig) = ed25519_dalek::Signature::try_from(signature) else {
        return false;
    };
    vk.verify(msg, &sig).is_ok()
}

/// Envelope format version.
const ENVELOPE_VERSION: u8 = 1;
/// Scheme tag: ECIES over X25519 + HKDF-SHA-256 + XChaCha20-Poly1305.
const SCHEME_ECIES_X25519: u8 = 1;
/// X25519 public-key / nonce widths.
const PUBKEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;
/// Header length before the ciphertext: version + scheme + ephemeral pk + nonce.
const HEADER_LEN: usize = 2 + PUBKEY_LEN + NONCE_LEN;
/// HKDF info string domain-separating this KDF use.
const HKDF_INFO: &[u8] = b"fluxum-ecies-x25519-v1";

/// A named X25519 recipient key (CT-035): the static secret every write seals
/// **to**, plus retired secrets reads still accept (CT-036 rotation). The
/// secret material is zeroized on drop.
pub struct EciesKey {
    id: String,
    active: StaticSecret,
    previous: Vec<StaticSecret>,
}

impl EciesKey {
    /// Build a key from its label, the active 32-byte X25519 secret, and any
    /// retired secrets (rotation read keys).
    pub fn new(id: impl Into<String>, active: [u8; 32], previous: Vec<[u8; 32]>) -> Self {
        Self {
            id: id.into(),
            active: StaticSecret::from(active),
            previous: previous.into_iter().map(StaticSecret::from).collect(),
        }
    }

    /// Parse a key from a label and 64-hex-char active secret plus retired
    /// secrets (CT-035 config surface).
    pub fn from_hex(
        id: impl Into<String>,
        active_hex: &str,
        previous_hex: &[String],
    ) -> Result<Self> {
        let active = parse_secret(active_hex)?;
        let previous = previous_hex
            .iter()
            .map(|h| parse_secret(h))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(id, active, previous))
    }

    /// The key's label.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The active recipient public key (writes seal to this).
    fn active_public(&self) -> PublicKey {
        PublicKey::from(&self.active)
    }
}

impl std::fmt::Debug for EciesKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render secret material.
        f.debug_struct("EciesKey").field("id", &self.id).finish()
    }
}

fn parse_secret(hex: &str) -> Result<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err(FluxumError::Config(format!(
            "x25519 key must be 64 hex characters (32 bytes), got {}",
            hex.len()
        )));
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let s = std::str::from_utf8(chunk)
            .map_err(|_| FluxumError::Config("x25519 key is not valid hex".into()))?;
        bytes[i] = u8::from_str_radix(s, 16)
            .map_err(|_| FluxumError::Config("x25519 key is not valid hex".into()))?;
    }
    Ok(bytes)
}

/// Derive the 32-byte AEAD key from an X25519 shared secret (HKDF-SHA-256).
fn derive_key(shared: &[u8; 32]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, shared);
    let mut key = [0u8; 32];
    // HKDF-Expand of 32 bytes never fails.
    let _ = hk.expand(HKDF_INFO, &mut key);
    key
}

/// Seal `plaintext` to `key`'s active recipient public key (CT-030), binding
/// `aad`. Returns the self-describing envelope.
pub fn ecies_seal(key: &EciesKey, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let ephemeral = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_pub = PublicKey::from(&ephemeral);
    let shared = ephemeral.diffie_hellman(&key.active_public());
    let mut aead_key = derive_key(shared.as_bytes());

    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.push(ENVELOPE_VERSION);
    header.push(SCHEME_ECIES_X25519);
    header.extend_from_slice(ephemeral_pub.as_bytes());
    header.extend_from_slice(nonce.as_slice());

    // AAD authenticates the header (version, scheme, ephemeral pk, nonce) plus
    // the caller's position binding.
    let mut full_aad = header.clone();
    full_aad.extend_from_slice(aad);

    let cipher = XChaCha20Poly1305::new((&aead_key).into());
    aead_key.zeroize();
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &full_aad,
            },
        )
        .map_err(|_| FluxumError::Storage("field encryption failed".into()))?;

    let mut envelope = header;
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Open an envelope produced by [`ecies_seal`], trying the active recipient
/// secret then each retired one (CT-036). Binds `aad`. Returns the plaintext
/// and whether the active key opened it (`false` ⇒ re-seal on the next write).
pub fn ecies_open(key: &EciesKey, envelope: &[u8], aad: &[u8]) -> Result<(Vec<u8>, bool)> {
    if envelope.len() < HEADER_LEN {
        return Err(FluxumError::Storage(format!(
            "encrypted field envelope of {} bytes is shorter than the {HEADER_LEN}-byte header",
            envelope.len()
        )));
    }
    let (header, ciphertext) = envelope.split_at(HEADER_LEN);
    if header[0] != ENVELOPE_VERSION || header[1] != SCHEME_ECIES_X25519 {
        return Err(FluxumError::Storage(format!(
            "unknown encrypted field envelope version/scheme ({}, {})",
            header[0], header[1]
        )));
    }
    let mut ephemeral_pub = [0u8; PUBKEY_LEN];
    ephemeral_pub.copy_from_slice(&header[2..2 + PUBKEY_LEN]);
    let ephemeral_pub = PublicKey::from(ephemeral_pub);
    let nonce = XNonce::from_slice(&header[2 + PUBKEY_LEN..HEADER_LEN]);

    let mut full_aad = header.to_vec();
    full_aad.extend_from_slice(aad);

    let try_secret = |secret: &StaticSecret| -> Option<Vec<u8>> {
        let shared = secret.diffie_hellman(&ephemeral_pub);
        let mut aead_key = derive_key(shared.as_bytes());
        let cipher = XChaCha20Poly1305::new((&aead_key).into());
        aead_key.zeroize();
        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad: &full_aad,
                },
            )
            .ok()
    };

    if let Some(plain) = try_secret(&key.active) {
        return Ok((plain, true));
    }
    for secret in &key.previous {
        if let Some(plain) = try_secret(secret) {
            return Ok((plain, false));
        }
    }
    Err(FluxumError::Storage(
        "encrypted field decryption failed: no configured key authenticates this value \
         (wrong key, tampering, or relocation) — refusing to serve (CT-032)"
            .into(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn key(id: &str, seed: u8, previous: &[u8]) -> EciesKey {
        EciesKey::new(id, [seed; 32], previous.iter().map(|s| [*s; 32]).collect())
    }

    #[test]
    fn seal_open_round_trips_and_hides_plaintext() {
        let k = key("votes", 1, &[]);
        let plain = b"candidate-A";
        let env = ecies_seal(&k, plain, b"aad").unwrap();
        assert!(
            !env.windows(plain.len()).any(|w| w == plain),
            "plaintext must not appear in the envelope"
        );
        assert!(env.len() > HEADER_LEN + 16, "header + ct + tag");
        let (out, active) = ecies_open(&k, &env, b"aad").unwrap();
        assert_eq!(out, plain);
        assert!(active);
    }

    #[test]
    fn wrong_aad_position_fails_authentication() {
        let k = key("votes", 1, &[]);
        let env = ecies_seal(&k, b"secret", b"table:Vote|col:choice|pk:7").unwrap();
        // Relocated to another row (different pk in the AAD): rejected (CT-032).
        let err = ecies_open(&k, &env, b"table:Vote|col:choice|pk:8").unwrap_err();
        assert!(err.to_string().contains("no configured key"), "{err}");
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let k = key("votes", 1, &[]);
        let mut env = ecies_seal(&k, b"secret", b"aad").unwrap();
        let last = env.len() - 1;
        env[last] ^= 0x01;
        assert!(ecies_open(&k, &env, b"aad").is_err());
        // A tampered header (ephemeral pk) also fails.
        let mut env2 = ecies_seal(&k, b"secret", b"aad").unwrap();
        env2[3] ^= 0x01;
        assert!(ecies_open(&k, &env2, b"aad").is_err());
    }

    #[test]
    fn a_wrong_key_never_yields_plaintext() {
        let writer = key("votes", 1, &[]);
        let env = ecies_seal(&writer, b"secret", b"aad").unwrap();
        let other = key("votes", 9, &[8]);
        assert!(ecies_open(&other, &env, b"aad").is_err());
    }

    #[test]
    fn a_retired_key_opens_and_signals_reseal() {
        // Sealed under seed-1 (later retired), read under a ring whose active
        // is seed-2 and previous is seed-1.
        let old = key("votes", 1, &[]);
        let env = ecies_seal(&old, b"legacy", b"aad").unwrap();
        let rotated = key("votes", 2, &[1]);
        let (plain, active) = ecies_open(&rotated, &env, b"aad").unwrap();
        assert_eq!(plain, b"legacy");
        assert!(!active, "opened under a previous key ⇒ needs reseal");
    }

    #[test]
    fn short_and_malformed_envelopes_are_rejected() {
        let k = key("votes", 1, &[]);
        assert!(ecies_open(&k, &[0u8; 10], b"aad").is_err());
        let mut env = ecies_seal(&k, b"x", b"aad").unwrap();
        env[0] = 0xFF; // bad version
        assert!(ecies_open(&k, &env, b"aad").is_err());
    }

    #[test]
    fn hex_keys_parse_and_reject_bad_input() {
        let k = EciesKey::from_hex("votes", &"ab".repeat(32), &[]).unwrap();
        assert_eq!(k.id(), "votes");
        assert!(EciesKey::from_hex("votes", "abcd", &[]).is_err());
        assert!(EciesKey::from_hex("votes", &"zz".repeat(32), &[]).is_err());
        assert!(
            !format!("{k:?}").contains("ab"),
            "Debug never leaks material"
        );
    }

    #[test]
    fn ed25519_sign_verify_round_trips_and_rejects_tampering() {
        let key = SignKey::new("server", [3u8; 32]);
        let msg = b"table:Vote|col:choice|pk:1|field";
        let sig = key.sign(msg);
        assert!(key.verify(msg, &sig), "a valid signature verifies");
        // A tampered message fails.
        assert!(!key.verify(b"different message", &sig));
        // A tampered signature fails.
        let mut bad = sig;
        bad[0] ^= 0x01;
        assert!(!key.verify(msg, &bad));
        // Debug never leaks the signing key.
        assert!(!format!("{key:?}").contains("signing"));
    }

    #[test]
    fn ed25519_verify_with_public_key_binds_the_signer() {
        // The `by = <Identity column>` path: verify against the signer's
        // public key (an Identity IS an Ed25519 public key, SPEC-009).
        let signer = SignKey::new("alice", [7u8; 32]);
        let public = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .to_bytes();
        let msg = b"signed field";
        let sig = signer.sign(msg);
        assert!(super::ed25519_verify_with(&public, msg, &sig));
        // A different public key rejects.
        let other = ed25519_dalek::SigningKey::from_bytes(&[8u8; 32])
            .verifying_key()
            .to_bytes();
        assert!(!super::ed25519_verify_with(&other, msg, &sig));
        // Malformed signature length rejects without panicking.
        assert!(!super::ed25519_verify_with(&public, msg, &[0u8; 10]));
    }

    #[test]
    fn ed25519_hex_keys_parse() {
        let k = SignKey::from_hex("server", &"ab".repeat(32)).unwrap();
        assert_eq!(k.id(), "server");
        assert!(SignKey::from_hex("server", "abcd").is_err());
    }
}
