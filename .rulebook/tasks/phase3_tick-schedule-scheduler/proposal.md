# Proposal: phase3_tick-schedule-scheduler

## Why
Periodic and deferred server-side work (aggregation, expiry, simulation steps) needs a drift-free fixed-timestep clock and durable one-shot/recurring scheduling.

## What Changes
Implement the #[fluxum::tick(rate)] fixed-timestep clock (absolute targets, missed-tick log, 3x-period drift reset) and #[fluxum::schedule] one-shot/recurring reducers via the __schedule__ table.

## Impact
- DAG task: T3.4
- Affected specs: SPEC-004 (reducers)
- PRD requirements: FR-21, FR-22
- Affected code: crates/fluxum-server (scheduler), crates/fluxum-macros (#[tick], #[schedule])
- Depends on: T3.3 (phase3_reducer-engine-lifecycle)
- Breaking change: NO
- User benefit: reliable periodic and deferred logic without external cron infrastructure
