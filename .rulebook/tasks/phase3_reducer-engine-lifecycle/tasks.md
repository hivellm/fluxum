## 1. Implementation
- [ ] 1.1 Implement `#[fluxum::reducer]` dispatch with the link-time reducer registry: duplicate names abort startup; unknown reducer returns an error without starting a transaction (FR-20, RED-006)
- [ ] 1.2 Implement lifecycle hooks: `#[fluxum::on_init]` (exactly once on fresh shard), `on_shard_start` (after recovery, before first call), `#[fluxum::on_connect]` / `#[fluxum::on_disconnect]` driving presence end to end (FR-23, RED-010..RED-013, UC-1)
- [ ] 1.3 Implement `catch_unwind` panic isolation: panic = rollback + error to caller, no CommitLog entry, no subscription events, shard never dies (FR-25, TXN-022)
- [ ] 1.4 Implement `#[fluxum::view]` read-only functions over `ReadOnlyTxHandle` (no write methods - compile-fail test) for the admin API `GET /view/:name` (FR-26 P1 half; RED-030/RED-031); `#[fluxum::procedure]` endpoint half is P2 post-launch
- [ ] 1.5 Panic-isolation soak: a panicking reducer called 10,000 times keeps returning internal-error results while interleaved healthy calls succeed; memory stable (RED-061)
- [ ] 1.6 Verification (DAG exit test): panic-injection tests
- [ ] 1.7 Gate G3 input (with T3.4/T3.5/T3.6 suites)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
