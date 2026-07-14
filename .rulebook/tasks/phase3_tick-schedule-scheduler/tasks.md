## 1. Implementation
- [ ] 1.1 Implement `#[fluxum::tick(rate = N)]` fixed-timestep scheduler with absolute-clock targets; decide OQ-4 (writer thread vs dedicated scheduler task feeding the reducer queue) (FR-21)
- [ ] 1.2 Drift semantics: stall of 1-3 periods re-fires immediately with no warning; stall > 3 periods logs exactly one warning and resets the clock with no catch-up burst; a tick never runs concurrently with itself on a shard (RED-020)
- [ ] 1.3 Implement `#[fluxum::schedule]` one-shot and recurring deferred reducers persisted in `__schedule__` (FR-22): rollback-safe at-least-once - scheduling inside a rolled-back tx never fires; execution removes its row in the same transaction (RED-021..RED-023)
- [ ] 1.4 Restart rescan: pending `__schedule__` rows re-enqueued exactly once after kill -9; past-due entries fire once immediately with no backfill of missed occurrences
- [ ] 1.5 Recurring anti-drift: intended-time rebase (1 s schedule with 300 ms handler fires at t+1s, t+2s, t+3s); stalled handler rebases to present without catch-up burst (RED-024)
- [ ] 1.6 Scheduled execution context: server identity + ConnectionId(0); schedule-only reducers reject client ReducerCalls with 403 unless client_callable = true (RED-025)
- [ ] 1.7 Verification (DAG exit test): tick-drift timing tests (60 Hz over 10 s = 600 +/- 1 executions, no cumulative drift); self-rescheduling example runs 10+ consecutive cycles
- [ ] 1.8 Gate G3 input: tick-drift suite green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
