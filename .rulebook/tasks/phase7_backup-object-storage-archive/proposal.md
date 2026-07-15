# Proposal: phase7_backup-object-storage-archive

## Why
The planned `fluxum backup` (phase7_backup-pitr) writes only to the local filesystem: its hot backup is the latest checkpoint (crates/fluxum-core/src/checkpoint/repo.rs, manifest.rs) plus archived commit-log segments (crates/fluxum-core/src/commitlog/segment.rs), and PITR replays those local segments (crates/fluxum-core/src/commitlog/replay.rs). Production operators want offsite durability: a node whose disk is lost keeps zero backups, and there is no way to stream nightly checkpoints/segments to shared object storage or to range-read one archived segment during a targeted restore. SPEC-025 OPS-010/011 close that gap by making S3-compatible object storage a first-class backup source/destination with content-hash integrity and incremental, non-stalling archival.

## What Changes
Add an object-store target so `fluxum backup` can push checkpoints and archived log segments to an S3-compatible endpoint and restore/PITR from it. Archived segments are written with seekable-zstd framing so PITR can HTTP range-read only the byte window covering the target tx_id/timestamp instead of downloading a whole segment. Every uploaded artifact carries a content hash recorded in the backup manifest and re-verified on download (OPS-011). A scheduled incremental archiver uploads only new/changed checkpoint pages and freshly sealed segments, running off the checkpoint worker so it never stalls writers. Local filesystem backup remains the default; the object-store target is additive and selected by config/CLI flag.

## Impact
- Governing spec: SPEC-025 §2 Object-storage backup & archive (OPS-010, OPS-011) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-014 (replication & backup design), SPEC-013 (checkpoints), SPEC-002 (commit-log segments), SPEC-015 (tiering / seekable-zstd framing)
- New PRD requirements: FR-139 (object-storage backup)
- Requirements covered: OPS-010, OPS-011
- Affected code: crates/fluxum-core/src/checkpoint/ (archive/upload path — repo.rs, manifest.rs, worker.rs, recover.rs), crates/fluxum-core/src/commitlog/segment.rs (seekable-zstd sealed segments), crates/fluxum-core/src/commitlog/replay.rs (range-read PITR), crates/fluxum-cli/src (backup subcommand target flags), a new object-store client module
- Depends on: phase7_backup-pitr (local backup/PITR + segment archival)
- Breaking change: NO (additive target; local backup unchanged, manifest gains hash/target fields)
- User benefit: offsite, integrity-verified backups with range-read PITR — a lost disk no longer means lost recoverability
