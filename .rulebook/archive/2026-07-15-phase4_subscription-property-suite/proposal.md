# Proposal: phase4_subscription-property-suite

## Why
Subscription correctness (client cache identical to server state) is the product promise; only adversarial property testing over random workloads can defend it.

## What Changes
Build the subscription correctness property suite: 10,000 random mutations across random subscriptions, asserting every client cache is equivalent to server state.

## Impact
- DAG task: T4.5
- Affected specs: SPEC-013 (testing and conformance)
- PRD requirements: NFR-10
- Affected code: crates/fluxum-server tests, CI workflows
- Depends on: T4.3 (phase4_visibility-rls), T4.4 (phase4_backpressure)
- Breaking change: NO
- User benefit: guaranteed cache coherence for every subscribed client
