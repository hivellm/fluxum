# Proposal: phase7_replica-sets-failover

## Why
Replication without automatic failover is only half of HA; consensus election plus read/fan-out offload turns replicas into both a safety net and extra serving capacity.

## What Changes
Implement replica sets: consensus-based primary election (OQ-8), automatic failover, SDK reconnect/resubscribe, and replicas serving reads plus subscription fan-out.

## Impact
- DAG task: T7.2
- Affected specs: SPEC-014 (replication and backup)
- PRD requirements: FR-101, FR-102, FR-105
- Affected code: crates/fluxum-server (replication/consensus), SDK reconnect paths
- Depends on: T7.1 (phase7_replication-streaming)
- Breaking change: NO
- User benefit: automatic failover with zero committed-tx loss in semi-sync mode, plus read scaling
