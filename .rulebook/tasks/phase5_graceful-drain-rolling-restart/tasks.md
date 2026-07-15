## 1. Implementation
- [ ] 1.1 Add a drain state to the server: entering drain flips accept loops to refuse new connections and new subscriptions while keeping existing ones serviced (OPS-030; crates/fluxum-server/src/lib.rs, tcp.rs, http.rs)
- [ ] 1.2 Refuse new reducer calls during drain with a retryable error code (not a hard drop) so clients can retry against the restarted process (OPS-030/OPS-031; crates/fluxum-server/src/session.rs)
- [ ] 1.3 Quiesce: wait for in-flight transactions to commit before proceeding, tracking outstanding tx count (OPS-030; crates/fluxum-server/src/lib.rs)
- [ ] 1.4 Trigger a final checkpoint at end of drain so restart replays little or nothing (OPS-030; crates/fluxum-core/src/checkpoint/worker.rs)
- [ ] 1.5 Bounded deadline: exit cleanly within a configured drain timeout; stragglers past the deadline are force-closed and logged (OPS-030; crates/fluxum-server/src/lib.rs over the existing Arc<Notify> shutdown handle)
- [ ] 1.6 Trigger drain from SIGTERM (main) and from a `fluxum drain` command / admin endpoint (OPS-030; crates/fluxum-server/src/main.rs, crates/fluxum-cli/src)
- [ ] 1.7 Verification: a client with an in-flight reducer call during drain sees that call commit, a call started mid-drain gets the retryable signal, the process checkpoints and exits within the deadline, and restart replay is near-empty

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
