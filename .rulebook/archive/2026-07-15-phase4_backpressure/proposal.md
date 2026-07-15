# Proposal: phase4_backpressure

## Why
One slow consumer must never stall the commit path or other subscribers; a tiered send buffer with an explicit drop policy makes overload behavior predictable and observable.

## What Changes
Implement the 3-tier per-client send buffer (Normal / Pressured / Full): non-blocking checks, drop policy, and the fluxum_subscriber_drops_total metric.

## Impact
- DAG task: T4.4
- Affected specs: SPEC-005 (subscriptions)
- PRD requirements: FR-33
- Affected code: crates/fluxum-server (subscription/transport buffer)
- Depends on: T4.2 (phase4_subscription-manager-fanout)
- Breaking change: NO
- User benefit: slow clients degrade individually and visibly instead of degrading everyone
