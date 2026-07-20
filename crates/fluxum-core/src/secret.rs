//! Redacting, zeroizing secret wrapper for config material (SPEC-026
//! SEC-058; OWASP A02 F-006).
//!
//! Every secret in the configuration — `auth.secret`, `server_peers[].token`,
//! `encryption.keys[].key_hex`, `transforms.keys[].secret`, the sidecar token —
//! is a [`Secret<T>`] rather than a bare `String`. That buys three things a
//! plain field cannot:
//!
//! - **`Debug` redaction.** A stray `{:?}` of the config (a panic, a trace, an
//!   error context) prints `Secret([redacted])`, never the bytes.
//! - **`Serialize` redaction.** Any serialization of the config — the reload
//!   diff, an `/health` render, an operator dumping it — emits
//!   `"[redacted]"`, so a serialized config can never leak minting material.
//!   (Deserialization reads the real value; the wrapper is one-way.)
//! - **Zeroize on drop.** The plaintext is wiped from memory when the config
//!   value is dropped, shrinking the window a core dump can capture it in.
//!
//! The plaintext is reachable only through the explicit
//! [`Secret::expose_secret`] at the point of use, so every read of a secret is
//! grep-able.
//!
//! **Serialization is lossy by design** (it emits the redaction, not the
//! value): the config is loaded from its source once and never rebuilt by
//! serializing-then-deserializing a `Secret`. The hot-reload path re-reads the
//! *file*, so a changed secret is applied by restart, not by diffing a
//! serialized secret (secrets are non-reloadable regardless).

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroize;

/// The text emitted in place of a secret by `Debug`/`Serialize`.
const REDACTED: &str = "[redacted]";

/// A configuration secret: redacts in `Debug`/`Serialize`, zeroizes on drop,
/// and yields its plaintext only through [`Secret::expose_secret`].
#[derive(Clone)]
pub struct Secret<T: Zeroize>(T);

impl<T: Zeroize> Secret<T> {
    /// Wrap a plaintext secret.
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    /// The plaintext, at the point of use. Every call is an explicit,
    /// grep-able acknowledgement that a secret is being read.
    pub fn expose_secret(&self) -> &T {
        &self.0
    }
}

impl<T: Zeroize> Drop for Secret<T> {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl<T: Zeroize> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secret({REDACTED})")
    }
}

impl<T: Zeroize> Serialize for Secret<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Never the value — a serialized config must not carry secrets.
        serializer.serialize_str(REDACTED)
    }
}

impl<'de, T: Zeroize + Deserialize<'de>> Deserialize<'de> for Secret<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Secret::new)
    }
}

impl<T: Zeroize> From<T> for Secret<T> {
    fn from(inner: T) -> Self {
        Self::new(inner)
    }
}

impl Secret<String> {
    /// Convenience for the common `Secret<String>` case: the plaintext as
    /// `&str`.
    pub fn expose_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for Secret<String> {
    fn from(s: &str) -> Self {
        Self::new(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn debug_redacts_the_value() {
        let s = Secret::new("hunter2".to_owned());
        let shown = format!("{s:?}");
        assert!(
            !shown.contains("hunter2"),
            "debug leaks the secret: {shown}"
        );
        assert_eq!(shown, "Secret([redacted])");
    }

    #[test]
    fn serialize_redacts_the_value() {
        let s = Secret::new("s3cret".to_owned());
        let json = serde_json::to_string(&s).unwrap();
        assert!(
            !json.contains("s3cret"),
            "serialize leaks the secret: {json}"
        );
        assert_eq!(json, "\"[redacted]\"");

        // Inside a struct the field redacts too.
        #[derive(Serialize)]
        struct Wrap {
            token: Secret<String>,
        }
        let w = Wrap {
            token: Secret::new("peertoken".to_owned()),
        };
        let yaml = serde_yaml::to_string(&w).unwrap();
        assert!(!yaml.contains("peertoken"), "struct field leaks: {yaml}");
    }

    #[test]
    fn deserialize_reads_the_real_value_and_expose_returns_it() {
        let s: Secret<String> = serde_yaml::from_str("real-secret").unwrap();
        assert_eq!(s.expose_str(), "real-secret");
        assert_eq!(s.expose_secret(), "real-secret");
    }

    #[test]
    fn a_vec_of_secrets_round_trips_from_yaml_and_redacts() {
        let v: Vec<Secret<String>> = serde_yaml::from_str("- a\n- b\n").unwrap();
        assert_eq!(v[0].expose_str(), "a");
        assert_eq!(v[1].expose_str(), "b");
        let out = serde_yaml::to_string(&v).unwrap();
        assert!(!out.contains('a') || out.contains("redacted"));
        assert!(out.contains("[redacted]"));
    }
}
