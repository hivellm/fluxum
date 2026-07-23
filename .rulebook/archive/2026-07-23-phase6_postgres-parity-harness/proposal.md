# Proposal: phase6_postgres-parity-harness

## Why
The PostgreSQL parity claim (NFR-11) must be proven, not asserted; the harness is permanent infrastructure whose comparative report ships as a release artifact from 0.1.0 onward.

## What Changes
Build the fluxum-bench parity harness: an identical application on app-server + PostgreSQL (and a SQLite variant), equal hardware, honest durability configs on both sides, plus the comparative report generator.

## Impact
- DAG task: T6.3
- Affected specs: SPEC-013 (testing and conformance)
- PRD requirements: NFR-11
- Affected code: fluxum-bench (new bench crate/workspace member), CI release pipeline
- Depends on: G5 (baseline app can start any time after G1)
- Breaking change: NO
- User benefit: honest, reproducible performance comparison against the incumbent stack
