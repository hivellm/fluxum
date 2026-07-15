## 1. Implementation
- [ ] 1.1 Keyring: parse config.yml transforms.keys (x25519/ed25519), FLUXUM_* env injection, derive pubkeys; ServerBuilder::build() aborts on missing/scheme-mismatched key referenced by an attribute (CT-035)
- [ ] 1.2 ECIES encrypt/decrypt over X25519: ephemeral key agreement + HKDF-SHA-256 + XChaCha20-Poly1305 AEAD; self-describing envelope version‖scheme‖ephemeral_pubkey‖nonce‖ciphertext‖tag stored as Bytes (CT-030)
- [ ] 1.3 AEAD associated data binds ciphertext to (table, column, primary_key); relocation/tamper fails decryption (CT-032)
- [ ] 1.4 Ed25519 sign on write over (table, column, pk, field_bytes); store field_bytes‖signature; by=server or by=<Identity field> (CT-033)
- [ ] 1.5 Read-side verify + strip; expose <field>_verified sibling in row projection; failed verify sets false without dropping the row (CT-034)
- [ ] 1.6 Wire ColumnTransform on_write into the tx write path (store/tx.rs) so stored rows carry transformed bytes; on_write error rolls back the transaction (CT-011, CT-014)
- [ ] 1.7 Read-path on_read hook honoring an authorized flag; default authorized = server-peer only until phase4 provides column-grant resolution (CT-012, CT-031)
- [ ] 1.8 Key rotation: previous keys decrypt legacy envelopes while current key encrypts new writes (CT-036)
- [ ] 1.9 Metrics fluxum_transform_read_errors_total, fluxum_signature_verify_failures_total (CT-014, CT-034)
- [ ] 1.10 Verification: persisted-bytes scan proves no plaintext in commit log / cold pages / checkpoints for an encrypted column; authorized read returns exact plaintext; tampered ciphertext/signature rejected

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
