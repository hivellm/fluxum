# Proposal: phase2_encryption-at-rest

## Why
SPEC-017 (column transforms) gives field-granularity crypto, but nothing encrypts the whole store at rest. Cold pages spill through `PageCodec` in `crates/fluxum-core/src/store/pager/codec.rs` (compression enum `None`/`Lz4`/`Zstd`, `compress_image`/`decompress_image`) and checkpoints/backups are written as zstd artifacts (`compress_artifact`/`decompress_artifact`) — all land on disk as plaintext. A stolen data directory today exposes every row in cold pages, checkpoints, and backups. Whole-store at-rest encryption under a single managed key is a common compliance requirement (encryption of the entire data volume, not just tagged columns) and is the encryption seam the codec was built to host.

## What Changes
When at-rest encryption is enabled, cold pages, checkpoints, and backups are encrypted with an AEAD (XChaCha20-Poly1305) under a key from config/KMS, added as a stage inside the `PageCodec` (after compression, before the write) and the checkpoint/backup artifact writers. Existing page/segment integrity (CRC32C / content hash) is verified before decryption so corruption is caught first and a key mismatch aborts startup rather than serving garbage. Key rotation re-encrypts lazily on page rewrite, with a set of `previous` keys accepted for read during the rotation window. This is distinct from SPEC-017 column-level crypto: it protects the entire on-disk footprint under one key, not selected fields.

## Impact
- Governing spec: docs/specs/SPEC-026-security-hardening.md
- Related specs: docs/specs/SPEC-015 (tiered storage / page codec), docs/specs/SPEC-017-column-transforms.md (distinct field-level crypto)
- New PRD requirements: FR-145
- Requirements covered: SEC-010, SEC-011, SEC-012
- Affected code: crates/fluxum-core/src/store/pager/codec.rs (AEAD encrypt/decrypt stage in the page + artifact paths), crates/fluxum-core/src/checkpoint (encrypting checkpoint/backup writers + recover path), crates/fluxum-core/src/config (key material / KMS reference, enable flag), crates/fluxum-core/src/store/pager (fault-in verify-then-decrypt ordering)
- Depends on: phase2 tiered storage + checkpoints (archived)
- Breaking change: NO
- User benefit: A copied data directory is opaque without the key — cold pages, checkpoints, and backups are unreadable, satisfying encryption-at-rest compliance without per-column configuration.
