## 1. Implementation
- [ ] 1.1 Implement consensus-based primary election per the OQ-8 resolution in SPEC-014 (election, term/epoch bump, fencing integration)
- [ ] 1.2 Implement failure detection and automatic failover promoting an up-to-date replica
- [ ] 1.3 Implement SDK reconnect/resubscribe against the new primary (sessions re-authenticate, subscriptions resume)
- [ ] 1.4 Serve reads and subscription fan-out from replicas (read offload with staleness semantics per spec)
- [ ] 1.5 Verification (DAG exit test): failover drill with zero committed-tx loss in semi-sync mode; fan-out offload verified
- [ ] 1.6 Gate G7 input: PRD section 12.2 all green - failover + PITR + 5 SDKs + 1B-row soak + parity report v2 (release 0.2.0)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
