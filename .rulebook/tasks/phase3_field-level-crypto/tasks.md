## 1. Implementation
- [x] 1.1 Keyring: `config.transforms.keys` (id, scheme x25519/ed25519, secret hex, previous[]); `TransformsConfig::ecies_keys()` builds the X25519 recipient keyring; `TransformEngine::build` aborts when an `#[encrypted]` attribute names a missing/non-X25519 key (CT-035) (crates/fluxum-core/src/config/mod.rs, transform/engine.rs)
- [x] 1.2 ECIES over X25519: ephemeral key agreement + HKDF-SHA-256 + XChaCha20-Poly1305 AEAD; self-describing envelope version‖scheme‖ephemeral_pubkey‖nonce‖ciphertext‖tag stored as Bytes (CT-030; crates/fluxum-core/src/transform/crypto.rs)
- [x] 1.3 AEAD associated data binds ciphertext to (table_id, column ordinal, primary_key); envelope header (version/scheme/ephemeral pk/nonce) authenticated too; relocation/tamper fails decryption (CT-032; crates/fluxum-core/src/transform/{crypto.rs,engine.rs})
- [ ] 1.4 Ed25519 sign on write over (table, column, pk, field_bytes); store field_bytes‖signature; by=server or by=<Identity field> (CT-033) — FOLLOW-UP SLICE
- [ ] 1.5 Read-side verify + strip; expose <field>_verified sibling in row projection; failed verify sets false without dropping the row (CT-034) — FOLLOW-UP SLICE
- [x] 1.6 `TransformEngine::on_write_row` wired into the tx write path (store/memstore.rs) after validation + pk derivation, before storage — stored rows carry ciphertext, so the commit log / cold pages / checkpoints never see plaintext (CT-011, CT-014; crates/fluxum-core/src/store/memstore.rs, transform/engine.rs)
- [x] 1.7 `on_read_row` decryption wired at the reducer TxHandle boundary (insert/upsert/query_pk/scan family/delete_where), gated by an authorized flag; reducers run as server peers (AUTH-062) → authorized; client-facing reads keep ciphertext until phase-4 column grants (CT-012, CT-031; crates/fluxum-core/src/reducer/mod.rs)
- [x] 1.8 Key rotation: `ecies_open` tries the active secret then each `previous` secret; a value sealed under a retired key still decrypts while new writes seal under the active key (CT-036; crates/fluxum-core/src/transform/crypto.rs)
- [ ] 1.9 Metrics fluxum_transform_read_errors_total, fluxum_signature_verify_failures_total (CT-014, CT-034) — FOLLOW-UP SLICE
- [x] 1.10 Verification (encryption): persisted-row scan proves no plaintext in the committed row for an encrypted column; authorized reducer read returns exact plaintext; tampered ciphertext and cross-pk relocation rejected; rotation reads legacy + writes new; build aborts on a missing key (crates/fluxum-core/tests/field_crypto.rs, crates/fluxum-macros/tests/field_crypto_build.rs, ECIES unit tests) — signature verification proof rides the follow-up

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
