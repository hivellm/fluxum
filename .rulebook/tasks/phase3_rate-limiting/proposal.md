# Proposal: phase3_rate-limiting

## Why
Public reducers must survive abusive or buggy clients; per-identity token buckets reject floods before they consume transaction resources.

## What Changes
Implement max_rate = "N/s" token-bucket rate limiting per (Identity, reducer), rejecting before TxState creation with a 429-style error.

## Impact
- DAG task: T3.5
- Affected specs: SPEC-004 (reducers)
- PRD requirements: FR-24
- Affected code: crates/fluxum-server (reducer engine), crates/fluxum-macros (max_rate attr)
- Depends on: T3.3 (phase3_reducer-engine-lifecycle)
- Breaking change: NO
- User benefit: built-in flood protection per client identity, no gateway required
