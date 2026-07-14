## 1. Implementation
- [ ] 1.1 Implement the 3-tier per-client send buffer (Normal / Pressured / Full) with non-blocking checks on the fan-out path (FR-33, SUB-041)
- [ ] 1.2 Drop policy: a client blocked past the threshold (5 s) is disconnected; WARN logged; `fluxum_subscriber_drops_total` incremented (SUB-042)
- [ ] 1.3 Verification (DAG exit test): slow-consumer stress test - 1,000 subscribers with one blocked socket: the other 999 receive TxUpdate with p99 < 5 ms (NFR-04) and commit throughput unaffected while the socket stays blocked
- [ ] 1.4 Gate G4 input: slow-consumer stress green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
