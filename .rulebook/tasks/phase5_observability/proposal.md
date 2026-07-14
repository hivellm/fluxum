# Proposal: phase5_observability

## Why
A realtime database is operated by its metrics; without the fluxum_* catalogue, structured logs, and slow-reducer warnings, production issues are invisible.

## What Changes
Implement observability: all P0 fluxum_* Prometheus metrics from the SPEC-012 catalogue, structured JSON logs, and slow-reducer warnings.

## Impact
- DAG task: T5.6
- Affected specs: SPEC-012 (observability)
- PRD requirements: FR-90, FR-92, FR-93
- Affected code: crates/fluxum-server (metrics/logging), /metrics endpoint
- Depends on: T5.3 (phase5_http-admin-api), T5.4 (phase5_shardcoord-shardhost)
- Breaking change: NO
- User benefit: production-grade visibility into throughput, latency, fan-out, and misbehaving reducers
