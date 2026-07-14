# Proposal: phase3_reducer-engine-lifecycle

## Why
Reducers are the only write path; dispatch, lifecycle hooks, and panic isolation determine whether a buggy reducer can ever take a shard down (it must not).

## What Changes
Implement the reducer engine: #[fluxum::reducer] dispatch, on_init/on_connect/on_disconnect lifecycle hooks, and catch_unwind panic isolation (panic means rollback; the shard never dies).

## Impact
- DAG task: T3.3
- Affected specs: SPEC-004 (reducers)
- PRD requirements: FR-20, FR-23, FR-25
- Affected code: crates/fluxum-server (reducer engine), crates/fluxum-macros (#[reducer])
- Depends on: T3.2 (phase3_reducer-context-txhandle)
- Breaking change: NO
- User benefit: server-side logic with crash isolation; one bad reducer cannot kill the database
