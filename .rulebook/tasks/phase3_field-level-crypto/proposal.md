# Proposal: phase3_field-level-crypto

## Why
Sensitive fields (e.g. votes) need native, modern cryptographic protection applied automatically at the storage boundary, not hand-rolled in every reducer. Fluxum has no encryption anywhere today (SECURITY.md scopes confidentiality to transport TLS only). This task implements the server-side crypto transforms — the pgcrypto model, modernized to elliptic curves — so an #[encrypted]/#[signed] column stores ciphertext/signed bytes and never leaks plaintext into the commit log, cold pages, checkpoints, replication stream, or indexes.

## What Changes
Implement the crypto family of column transforms defined in SPEC-017 §5: ECIES over X25519 (ephemeral key agreement + HKDF-SHA-256 + XChaCha20-Poly1305 AEAD) for #[encrypted], and Ed25519 for #[signed] with a <field>_verified sibling on read. Add the named Keyring backed by config.yml transforms.keys with FLUXUM_* injection, scheme validation at ServerBuilder::build(), and key rotation (previous keys). Wire the ColumnTransform on_write hook into the transaction write path (store/tx.rs) and the on_read hook (gated by an authorized flag supplied by phase4). AEAD associated data binds ciphertext to (table, column, primary_key). Read-path masking/authorization resolution itself is phase4 — this task supplies the crypto primitives and the write/read hook points, defaulting authorized=server-peer-only until phase4 lands.

## Impact
- Governing spec: SPEC-017 §5 (cryptographic transforms) + §3 (pipeline hooks) — docs/specs/SPEC-017-column-transforms.md
- Related specs: SPEC-009 (AUTH-062 server-peer bypass, AUTH-070 roles), SPEC-003 (rollback on on_write error), SPEC-002/SPEC-015 (no plaintext in log/pages/checkpoints)
- New PRD requirements: FR-91 (field-level security — crypto half)
- Affected code: crates/fluxum-core/src/store/tx.rs (write hook), new crypto module (ecies/ed25519/keyring), crates/fluxum-core/src/config (transforms.keys), crates/fluxum-server (metrics)
- Depends on: phase1_column-transforms-type-surface (attribute surface, ColumnTransform trait, ColumnSchema); T3.x transaction/reducer runtime (write path)
- Breaking change: NO (additive; only affects columns that opt in)
- User benefit: native elliptic-curve encryption + signatures for sensitive fields, keys managed server-side, zero plaintext at rest
