# Proposal: phase7_replication-streaming

## Why
High availability starts with replicas that can join cold and stay caught up; streaming the frozen commit-log format keeps replication mechanically simple and PITR-compatible.

## What Changes
Implement replication log streaming: full sync via checkpoint transfer, partial sync from a log offset, async + semi-sync quorum modes, and epoch fencing.

## Impact
- DAG task: T7.1
- Affected specs: SPEC-014 (replication and backup); stream format = log format frozen at G5
- PRD requirements: FR-100
- Affected code: crates/fluxum-server (replication module)
- Depends on: G6
- Breaking change: NO
- User benefit: replicas converge from nothing or from an offset, with a durability mode dial
