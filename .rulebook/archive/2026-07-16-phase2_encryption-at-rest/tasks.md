## 1. Implementation
- [x] 1.1 Keyring config surface: `storage.encryption { enabled, active_key_id, keys[{id, key_hex}] }`; `EncryptionConfig::keyring()` builds the runtime ring (active + `previous` read keys) or `None` when disabled (SEC-010; crates/fluxum-core/src/config/mod.rs)
- [x] 1.2 XChaCha20-Poly1305 AEAD encrypt stage in `PageCodec::encode_for_storage`, after compression, keyed per page — AAD binds (shard, table, page id, flags); new `FLAG_ENCRYPTED` page-header bit; random 192-bit nonce per seal (CoW page rewrites make a derived nonce unsound) (SEC-010; crates/fluxum-core/src/store/pager/codec.rs, format.rs)
- [x] 1.3 Matching decrypt stage in `open_image`, run only after `decode_page`'s mandatory CRC32C verification succeeds — the CRC covers the ciphertext, so integrity is checked before decryption (SEC-011; crates/fluxum-core/src/store/pager/codec.rs, mod.rs spill/fault)
- [x] 1.4 Checkpoint/backup artifact encryption: `compress_artifact` seals after zstd behind a self-describing `FLXENC01` magic; `decompress_artifact` decrypts first; threaded through `CheckpointRepo::with_keyring` and `decode_manifest` (SEC-010; crates/fluxum-core/src/store/pager/codec.rs, checkpoint/repo.rs, checkpoint/manifest.rs)
- [x] 1.5 Integrity before decrypt for artifacts: checkpoint objects are content-addressed (their hash verifies the ciphertext before decrypt); the AEAD Poly1305 tag authenticates manifests — a wrong/absent key is a hard authentication failure, never garbage (SEC-011; crates/fluxum-core/src/checkpoint/repo.rs)
- [x] 1.6 Lazy key rotation: every spill re-seals under the active key (`Keyring::seal`), so a page rewrite re-encrypts under the active key; reads accept any `previous` key (`Keyring::open` tries active then each previous; Poly1305 tag authenticates the match) (SEC-012; crates/fluxum-core/src/crypto.rs)
- [x] 1.7 Wiring + guardrails: `Pager::open_with_keyring` and `CheckpointRepo::with_keyring` install the ring; `EncryptionConfig::keyring()` rejects enabling with no keys / an `active_key_id` that names none; key bytes zeroize on drop and never render via Debug (SEC-010/011; crates/fluxum-core/src/config/mod.rs, store/pager/mod.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
