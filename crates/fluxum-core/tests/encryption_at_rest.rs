//! SPEC-014 SEC-010/011/012 — at-rest encryption: the page codec seal/open
//! stages (encrypt after compression, decrypt after CRC), lazy key rotation
//! via a retained read key, hard failure on a wrong/absent key, checkpoint
//! artifact encryption, and the config keyring surface. The live spill→fault
//! cycle through the pager is covered by an in-crate test in the pager module.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::config::{EncryptionConfig, EncryptionKey};
use fluxum_core::crypto::{AtRestKey, Keyring};
use fluxum_core::store::pager::codec::{
    ARTIFACT_ENC_MAGIC, PageCodec, compress_artifact, decompress_artifact, encode_for_storage,
    open_image,
};
use fluxum_core::store::pager::format::{FLAG_INDEX, PageHeader, decode_page, encode_page};

const SHARD: u32 = 5;
const PAGE: usize = 8192;

fn ring(active: &str, seed: u8, previous: &[(&str, u8)]) -> Arc<Keyring> {
    Arc::new(Keyring::new(
        AtRestKey::new(active, [seed; 32]),
        previous
            .iter()
            .map(|(id, s)| AtRestKey::new(*id, [*s; 32]))
            .collect(),
    ))
}

fn compressible(len: usize) -> Vec<u8> {
    b"the quick brown fox jumps over the lazy dog -- "
        .iter()
        .copied()
        .cycle()
        .take(len)
        .collect()
}

/// A compressible page whose payload embeds a recognizable plaintext marker.
fn marked_page(table_id: u32, page_id: u64) -> (Vec<u8>, &'static [u8]) {
    let marker: &[u8] = b"TOP-SECRET-PLAINTEXT-MARKER";
    let mut payload = compressible(4000);
    payload[100..100 + marker.len()].copy_from_slice(marker);
    let image = encode_page(&PageHeader::new(page_id, table_id, 7, FLAG_INDEX), &payload).unwrap();
    (image, marker)
}

/// SEC-010/011: the stored page carries [`PageHeader::is_encrypted`], never
/// exposes the plaintext marker, verifies its CRC on decode, and `open_image`
/// rebuilds the exact original pool image after decryption.
#[test]
fn page_seals_after_compression_and_opens_after_crc() {
    let keyring = ring("active", 1, &[]);
    let (image, marker) = marked_page(42, 1);

    let stored = encode_for_storage(&image, PageCodec::Lz4, 1024, SHARD, Some(keyring.as_ref()))
        .unwrap()
        .expect("encryption always produces a stored image");
    assert!(
        !stored.windows(marker.len()).any(|w| w == marker),
        "plaintext marker leaked into the stored page"
    );

    // decode_page performs the mandatory CRC32C verification (SEC-011 gate).
    let (header, payload) = decode_page(&stored, SHARD, 42, 1).unwrap();
    assert!(header.is_encrypted(), "FLAG_ENCRYPTED set");
    assert_ne!(header.codec(), 0, "compressed under the ciphertext");

    let rebuilt = open_image(&header, payload, PAGE, SHARD, Some(keyring.as_ref())).unwrap();
    assert_eq!(rebuilt, image, "decrypt+decompress round-trip diverged");
}

/// SEC-012: a page sealed under `k1` still opens after the active key rotates
/// to `k2`, as long as `k1` is retained as a read key.
#[test]
fn a_retired_key_still_opens_after_rotation() {
    let (image, _) = marked_page(9, 3);
    let stored = encode_for_storage(
        &image,
        PageCodec::Lz4,
        1024,
        SHARD,
        Some(ring("k1", 1, &[]).as_ref()),
    )
    .unwrap()
    .unwrap();
    let (header, payload) = decode_page(&stored, SHARD, 9, 3).unwrap();

    let rotated = ring("k2", 2, &[("k1", 1)]);
    let rebuilt = open_image(&header, payload, PAGE, SHARD, Some(rotated.as_ref())).unwrap();
    assert_eq!(rebuilt, image, "retired-key read failed");
}

/// SEC-011: the wrong key (and the missing key) fail `open_image` rather than
/// producing garbage; the AAD binds the page position.
#[test]
fn a_wrong_or_missing_key_never_yields_garbage() {
    let (image, _) = marked_page(3, 2);
    let stored = encode_for_storage(
        &image,
        PageCodec::Lz4,
        1024,
        SHARD,
        Some(ring("real", 7, &[]).as_ref()),
    )
    .unwrap()
    .unwrap();
    let (header, payload) = decode_page(&stored, SHARD, 3, 2).unwrap();

    let wrong = ring("other", 9, &[]);
    let err = open_image(&header, payload, PAGE, SHARD, Some(wrong.as_ref())).unwrap_err();
    assert!(err.to_string().contains("no configured key"), "{err}");

    let err = open_image(&header, payload, PAGE, SHARD, None).unwrap_err();
    assert!(err.to_string().contains("no keyring"), "{err}");
}

/// The AAD binds the page's identity: an encrypted page decoded at another
/// (shard, table, page) position fails authentication (no replay).
#[test]
fn page_identity_is_authenticated() {
    let (image, _) = marked_page(9, 3);
    let keyring = ring("k", 1, &[]);
    let stored = encode_for_storage(&image, PageCodec::Lz4, 1024, SHARD, Some(keyring.as_ref()))
        .unwrap()
        .unwrap();
    // Decode honestly (position matches the header), then attempt to open
    // against a different shard id — the AAD no longer matches.
    let (header, payload) = decode_page(&stored, SHARD, 9, 3).unwrap();
    let err = open_image(&header, payload, PAGE, SHARD + 1, Some(keyring.as_ref())).unwrap_err();
    assert!(err.to_string().contains("no configured key"), "{err}");
}

/// Without a keyring, `encode_for_storage` keeps the existing behavior:
/// compressible pages compress (no encryption flag), incompressible pages may
/// store verbatim.
#[test]
fn without_a_keyring_pages_are_not_encrypted() {
    let (image, _) = marked_page(1, 1);
    let stored = encode_for_storage(&image, PageCodec::Lz4, 1024, SHARD, None)
        .unwrap()
        .expect("compressible image compresses");
    let (header, payload) = decode_page(&stored, SHARD, 1, 1).unwrap();
    assert!(!header.is_encrypted());
    let rebuilt = open_image(&header, payload, PAGE, SHARD, None).unwrap();
    assert_eq!(rebuilt, image);
}

/// SEC-010/011: artifact envelopes encrypt after compression and round-trip
/// exactly; a wrong/absent key is a hard error; plaintext artifacts pass
/// through (self-describing framing).
#[test]
fn artifacts_encrypt_and_round_trip_with_key_and_reject_without() {
    let keyring = ring("k1", 4, &[]);
    let body = compressible(10_000);

    let sealed = compress_artifact(&body, 3, Some(keyring.as_ref())).unwrap();
    assert!(
        sealed.starts_with(&ARTIFACT_ENC_MAGIC),
        "enc envelope magic"
    );
    assert!(
        !sealed.windows(16).any(|w| w == &body[..16]),
        "plaintext must not appear verbatim"
    );

    let out = decompress_artifact(&sealed, Some(keyring.as_ref())).unwrap();
    assert_eq!(out.as_ref(), body.as_slice());

    let err = decompress_artifact(&sealed, None).unwrap_err();
    assert!(
        err.to_string().contains("no keyring is configured"),
        "{err}"
    );

    let wrong = ring("kX", 8, &[]);
    let err = decompress_artifact(&sealed, Some(wrong.as_ref())).unwrap_err();
    assert!(err.to_string().contains("no configured key"), "{err}");

    // A retired key reads an artifact sealed under it (SEC-012).
    let rotated = ring("k2", 5, &[("k1", 4)]);
    assert_eq!(
        decompress_artifact(&sealed, Some(rotated.as_ref()))
            .unwrap()
            .as_ref(),
        body.as_slice()
    );

    // A plaintext (unencrypted) artifact still reads with a keyring present.
    let plain = compress_artifact(&body, 3, None).unwrap();
    assert_eq!(
        decompress_artifact(&plain, Some(keyring.as_ref()))
            .unwrap()
            .as_ref(),
        body.as_slice()
    );
}

/// SEC-010: the config keyring surface builds a ring only when enabled and
/// rejects enabling with no / mismatched key material.
#[test]
fn config_keyring_validates_key_material() {
    assert!(EncryptionConfig::default().keyring().unwrap().is_none());

    let good = EncryptionConfig {
        enabled: true,
        active_key_id: "k2".into(),
        keys: vec![
            EncryptionKey {
                id: "k1".into(),
                key_hex: "ab".repeat(32).into(),
            },
            EncryptionKey {
                id: "k2".into(),
                key_hex: "cd".repeat(32).into(),
            },
        ],
    };
    let built = good.keyring().unwrap().expect("enabled ⇒ Some");
    assert_eq!(built.active_id(), "k2");

    let empty = EncryptionConfig {
        enabled: true,
        active_key_id: "k1".into(),
        keys: vec![],
    };
    assert!(empty.keyring().is_err());

    let dangling = EncryptionConfig {
        enabled: true,
        active_key_id: "ghost".into(),
        keys: vec![EncryptionKey {
            id: "k1".into(),
            key_hex: "ab".repeat(32).into(),
        }],
    };
    assert!(dangling.keyring().is_err());
}
