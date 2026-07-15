## 1. Implementation
- [ ] 1.1 Add an `AtRestKey` / keyring config surface (enable flag, active key + `previous` read keys, config or KMS reference) (SEC-010; crates/fluxum-core/src/config)
- [ ] 1.2 Add an XChaCha20-Poly1305 AEAD encrypt stage to the page path after compression in `PageCodec`, keyed per page (page id in AEAD associated data) (SEC-010; crates/fluxum-core/src/store/pager/codec.rs)
- [ ] 1.3 Add the matching decrypt stage on fault-in that runs only after CRC32C verification succeeds (SEC-011; crates/fluxum-core/src/store/pager/codec.rs)
- [ ] 1.4 Encrypt checkpoint/backup artifacts in `compress_artifact`/writer and decrypt in the recover path, keeping self-describing framing (SEC-010; crates/fluxum-core/src/checkpoint)
- [ ] 1.5 Verify content hash / integrity before decrypting artifacts and abort startup on a key mismatch instead of serving garbage (SEC-011; crates/fluxum-core/src/checkpoint/recover.rs)
- [ ] 1.6 Implement lazy key rotation: rewrite re-encrypts under the active key while reads accept any `previous` key (SEC-012; crates/fluxum-core/src/store/pager)
- [ ] 1.7 Wire the enable flag + keyring through pager/checkpoint construction and reject enabling with no key material (SEC-010, SEC-011; crates/fluxum-core/src/config)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
