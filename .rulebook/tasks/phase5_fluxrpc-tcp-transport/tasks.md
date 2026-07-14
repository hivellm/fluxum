## 1. Implementation
- [ ] 1.1 Implement the FluxRPC TCP listener (:15801): frame parser + session state machine (FR-40, FR-42)
- [ ] 1.2 Message routing for Authenticate / ReducerCall / Subscribe / SubscribeSingle / Unsubscribe / OneOffQuery; any non-Authenticate message pre-auth returns 401 "unauthenticated" with the connection kept open (AUTH-020)
- [ ] 1.3 Multiplexing by per-message id: pipelined concurrent calls answered out of order, every response echoing the correct id (SPEC-006 acceptance 4)
- [ ] 1.4 Idle connection timeout (408 then close) and max frame size enforcement (default 16 MB, configurable, 413) (FR-45)
- [ ] 1.5 Reconnect resync: after forced disconnect a client re-authenticates, re-subscribes, gets fresh InitialData, detects missed updates via the tx_id gap (SPEC-006 acceptance 14)
- [ ] 1.6 Loopback RTT benchmark: FluxRPC over loopback TCP p99 < 0.5 ms (NFR-05, TST-062)
- [ ] 1.7 Verification (DAG exit test): loopback integration tests

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
