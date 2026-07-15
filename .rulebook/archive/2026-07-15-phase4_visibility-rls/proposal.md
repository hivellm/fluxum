# Proposal: phase4_visibility-rls

## Why
Multi-tenant realtime data leaks by default without row-level security; owner-only visibility enforced at fan-out is the containment boundary.

## What Changes
Implement #[visibility(owner_only(field))] row-level security applied per subscriber identity during fan-out and initial data, with server-peer bypass.

## Impact
- DAG task: T4.3
- Affected specs: SPEC-005 (subscriptions)
- PRD requirements: FR-32, FR-72
- Affected code: crates/fluxum-server (subscription module), crates/fluxum-macros (#[visibility])
- Depends on: T4.2 (phase4_subscription-manager-fanout)
- Breaking change: NO
- User benefit: per-user data isolation enforced by the database, not by application discipline
