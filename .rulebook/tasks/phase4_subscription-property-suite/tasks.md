## 1. Implementation
- [ ] 1.1 Build the random-mutation generator: inserts/updates/deletes across random tables (including owner_only tables) with a population of clients holding random subscriptions (TST-030..)
- [ ] 1.2 Build the client-cache model: initialized from InitialData, maintained solely by applying TxUpdate diffs
- [ ] 1.3 Full run: 10,000 random mutations - every client cache equals the server-side query result for its subscriptions after every commit; required accuracy 100% (NFR-10, SUB acceptance 8)
- [ ] 1.4 Wire into CI as the G4 gate suite (full run per TST-034); scenario/fixture set feeds the shared SDK conformance corpus (phase6_sdk-conformance-corpus)
- [ ] 1.5 Verification (DAG exit test): suite green in CI
- [ ] 1.6 Gate G4: subscription correctness + backpressure green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
