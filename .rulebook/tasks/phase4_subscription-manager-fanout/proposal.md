# Proposal: phase4_subscription-manager-fanout

## Why
Fan-out cost must scale with matching plans, never with client count; query-hash dedup and value-level plan pruning are what keep realtime updates cheap at scale.

## What Changes
Implement SubscriptionManager: register/unsubscribe plans, post-commit fan-out with query-hash dedup (shared query = one evaluation + one encoding for all subscribers) and value-level plan pruning (plans indexed by equality-filter values); ORDER BY/LIMIT on InitialData only.

## Impact
- DAG task: T4.2
- Affected specs: SPEC-005 (subscriptions)
- PRD requirements: FR-30, FR-31, FR-34
- Affected code: crates/fluxum-server (subscription module)
- Depends on: T4.1 (phase4_sql-subscription-compiler)
- Breaking change: NO
- User benefit: TxUpdate diffs delivered to thousands of clients at O(matching plans) cost
