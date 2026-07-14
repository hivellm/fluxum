# Proposal: phase2_page-compression

## Why
Cold pages and checkpoints dominate disk footprint; LZ4 for latency-sensitive page I/O and zstd for checkpoints/backups cut storage cost roughly 3x with negligible CPU impact.

## What Changes
Implement page compression: LZ4 per cold page (above a threshold), zstd for checkpoints and backups, plus a compression-ratio benchmark.

## Impact
- DAG task: T2.9
- Affected specs: SPEC-015 (tiered storage)
- PRD requirements: FR-19
- Affected code: crates/fluxum-server (storage/tier, storage/checkpoint)
- Depends on: T2.8 (phase2_paged-cold-tier-buffer-pool)
- Breaking change: NO
- User benefit: roughly 3x smaller cold data and checkpoints without hot-path cost
