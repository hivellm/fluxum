# Proposal: phase7_backup-pitr

## Why
Operational trust requires hot backups that never stall writers and the ability to rewind to any point in time from archived log segments.

## What Changes
Implement fluxum backup create/restore/verify (hot, zstd-compressed, no writer stall) and PITR to a timestamp or tx_id from archived segments.

## Impact
- DAG task: T7.3
- Affected specs: SPEC-014 (replication and backup)
- PRD requirements: FR-103, FR-104
- Affected code: crates/fluxum-cli (backup commands), crates/fluxum-server (archive/restore paths)
- Depends on: G6
- Breaking change: NO
- User benefit: online backups and point-in-time recovery without downtime
