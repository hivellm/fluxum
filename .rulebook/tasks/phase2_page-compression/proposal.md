# Proposal: phase2_page-compression

## Why
Compression multiplies effective disk capacity and cuts checkpoint/backup sizes; a 3x ratio target is part of the tiered-storage value proposition.

## What Changes
Implement LZ4 cold-page compression (threshold-gated, zstd optional), zstd checkpoint/backup compression, bit-identical roundtrip property tests, and the published ratio benchmark.

## Impact
- DAG task: T2.9
- Affected specs: SPEC-015 (TIER-040..044)
- PRD requirements: FR-19
- Affected code: crates/fluxum-core (pager/checkpoint compression)
- Depends on: T2.8
- Breaking change: NO
- User benefit: at least 3x smaller cold data, checkpoints, and backups
