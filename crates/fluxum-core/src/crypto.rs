//! At-rest encryption keyring (SPEC-026 SEC-010/011/012) — the shared AEAD
//! primitive behind cold-tier page encryption and checkpoint/backup artifact
//! encryption.
//!
//! # Cipher
//!
//! XChaCha20-Poly1305 (192-bit nonce, 128-bit Poly1305 tag). The large nonce
//! is generated at random per seal, so re-encrypting the *same* page under
//! the *same* key after an edit never risks a two-time-pad: copy-on-write
//! page rewrites are the norm, and a counter/derived nonce would be unsound
//! there. Random 192-bit nonces are exactly the XChaCha use case.
//!
//! # Sealed envelope (freeze surface, versioned with the page/artifact format)
//!
//! ```text
//! | 0  | 24 | nonce         | XChaCha20 nonce (random per seal)          |
//! | 24 | .. | ciphertext    | AEAD ciphertext ++ 16-byte Poly1305 tag    |
//! ```
//!
//! No key id is stored: [`Keyring::open`] tries the active key, then each
//! `previous` key in turn, and the Poly1305 tag authenticates the match
//! (SEC-012 lazy rotation — a page written under an old key still reads,
//! and is re-sealed under the active key the next time it spills). A wrong
//! or absent key is an authentication failure, never silent garbage
//! (SEC-011).
//!
//! # Associated data
//!
//! Callers bind context (page identity, codec bits, artifact kind) into the
//! AEAD associated data, so a validly-encrypted page or artifact cannot be
//! replayed in a different position without failing authentication.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use zeroize::Zeroize;

use crate::error::{FluxumError, Result};

/// Nonce width (XChaCha20-Poly1305): 24 bytes.
pub const NONCE_LEN: usize = 24;
/// At-rest key width: 256 bits.
pub const KEY_LEN: usize = 32;

/// One named 256-bit at-rest key. The bytes are zeroized on drop (SEC-010).
#[derive(Clone)]
pub struct AtRestKey {
    id: String,
    bytes: [u8; KEY_LEN],
}

impl AtRestKey {
    /// Build a key from a label and raw 32 bytes.
    pub fn new(id: impl Into<String>, bytes: [u8; KEY_LEN]) -> Self {
        Self {
            id: id.into(),
            bytes,
        }
    }

    /// Parse a key from a label and 64 lowercase/uppercase hex characters.
    pub fn from_hex(id: impl Into<String>, hex: &str) -> Result<Self> {
        let hex = hex.trim();
        if hex.len() != KEY_LEN * 2 {
            return Err(FluxumError::Config(format!(
                "at-rest key must be {} hex characters ({KEY_LEN} bytes), got {}",
                KEY_LEN * 2,
                hex.len()
            )));
        }
        let mut bytes = [0u8; KEY_LEN];
        for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
            let s = std::str::from_utf8(chunk)
                .map_err(|_| FluxumError::Config("at-rest key is not valid hex".into()))?;
            bytes[i] = u8::from_str_radix(s, 16)
                .map_err(|_| FluxumError::Config("at-rest key is not valid hex".into()))?;
        }
        Ok(Self {
            id: id.into(),
            bytes,
        })
    }

    /// The key's label (for diagnostics / rotation reporting).
    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.bytes).into())
    }
}

impl Drop for AtRestKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

impl std::fmt::Debug for AtRestKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render key material.
        f.debug_struct("AtRestKey").field("id", &self.id).finish()
    }
}

/// An ordered set of at-rest keys: one **active** key every seal uses, plus
/// zero or more **previous** keys that [`Keyring::open`] still accepts for
/// reads during lazy rotation (SEC-012).
#[derive(Debug, Clone)]
pub struct Keyring {
    active: AtRestKey,
    previous: Vec<AtRestKey>,
}

impl Keyring {
    /// Build a keyring from its active key and the ordered previous keys.
    pub fn new(active: AtRestKey, previous: Vec<AtRestKey>) -> Self {
        Self { active, previous }
    }

    /// The active key's label — the one fresh writes seal under.
    pub fn active_id(&self) -> &str {
        self.active.id()
    }

    /// Seal `plaintext` under the active key, binding `aad`. Returns the
    /// [`NONCE_LEN`]-prefixed envelope.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = self
            .active
            .cipher()
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| FluxumError::Storage("at-rest seal failed".into()))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Open a sealed envelope, trying the active key then every previous key
    /// (SEC-012). Returns whether the active key opened it (`false` ⇒ the
    /// page/artifact should be re-sealed under the active key) alongside the
    /// plaintext. A key that no ring member matches is an authentication
    /// failure (SEC-011).
    pub fn open(&self, sealed: &[u8], aad: &[u8]) -> Result<(Vec<u8>, bool)> {
        let Some((nonce, ciphertext)) = sealed.split_at_checked(NONCE_LEN) else {
            return Err(FluxumError::Storage(format!(
                "sealed payload of {} bytes is shorter than the {NONCE_LEN}-byte nonce",
                sealed.len()
            )));
        };
        let nonce = XNonce::from_slice(nonce);
        if let Ok(plaintext) = self.active.cipher().decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        ) {
            return Ok((plaintext, true));
        }
        for key in &self.previous {
            if let Ok(plaintext) = key.cipher().decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            ) {
                return Ok((plaintext, false));
            }
        }
        Err(FluxumError::Storage(
            "at-rest decryption failed: no configured key authenticates this payload \
             (wrong key, or the data is corrupt) — refusing to serve (SEC-011)"
                .into(),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn key(id: &str, seed: u8) -> AtRestKey {
        AtRestKey::new(id, [seed; KEY_LEN])
    }

    #[test]
    fn seal_open_round_trips_under_the_active_key() {
        let ring = Keyring::new(key("k1", 1), vec![]);
        let sealed = ring.seal(b"secret page bytes", b"aad-context").unwrap();
        assert!(sealed.len() > NONCE_LEN + 16, "nonce + ciphertext + tag");
        let (plain, active) = ring.open(&sealed, b"aad-context").unwrap();
        assert_eq!(plain, b"secret page bytes");
        assert!(active, "active key opened it");
    }

    #[test]
    fn wrong_associated_data_fails_authentication() {
        let ring = Keyring::new(key("k1", 1), vec![]);
        let sealed = ring.seal(b"payload", b"page-7").unwrap();
        let err = ring.open(&sealed, b"page-8").unwrap_err();
        assert!(err.to_string().contains("no configured key"), "{err}");
    }

    #[test]
    fn previous_key_reads_and_signals_reseal() {
        // A page sealed under k1 becomes a "previous" key after rotation to k2.
        let old = Keyring::new(key("k1", 1), vec![]);
        let sealed = old.seal(b"rotated payload", b"aad").unwrap();

        let rotated = Keyring::new(key("k2", 2), vec![key("k1", 1)]);
        let (plain, active) = rotated.open(&sealed, b"aad").unwrap();
        assert_eq!(plain, b"rotated payload");
        assert!(!active, "opened under a previous key ⇒ needs reseal");

        // Re-sealed under the active key, it now opens as active.
        let resealed = rotated.seal(&plain, b"aad").unwrap();
        assert!(rotated.open(&resealed, b"aad").unwrap().1);
    }

    #[test]
    fn an_unknown_key_never_yields_garbage() {
        let writer = Keyring::new(key("k1", 1), vec![]);
        let sealed = writer.seal(b"data", b"aad").unwrap();
        let other = Keyring::new(key("kX", 9), vec![key("kY", 8)]);
        assert!(other.open(&sealed, b"aad").is_err());
    }

    #[test]
    fn short_envelope_is_rejected() {
        let ring = Keyring::new(key("k1", 1), vec![]);
        let err = ring.open(&[0u8; 10], b"aad").unwrap_err();
        assert!(err.to_string().contains("shorter than"), "{err}");
    }

    #[test]
    fn hex_keys_parse_and_reject_bad_input() {
        let k = AtRestKey::from_hex("k", &"ab".repeat(KEY_LEN)).unwrap();
        assert_eq!(k.id(), "k");
        assert!(AtRestKey::from_hex("k", "abcd").is_err(), "too short");
        assert!(
            AtRestKey::from_hex("k", &"zz".repeat(KEY_LEN)).is_err(),
            "non-hex"
        );
        // Debug never leaks key bytes.
        assert!(!format!("{k:?}").contains("ab"));
    }
}
