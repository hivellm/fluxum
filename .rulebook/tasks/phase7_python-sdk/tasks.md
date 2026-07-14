## 1. Implementation
- [ ] 1.1 Implement the FluxBIN/FluxRPC codec and frame layer in Python
- [ ] 1.2 Implement the asyncio client: connect (TCP + streamable HTTP), authenticate, reducer calls, OneOffQuery
- [ ] 1.3 Implement subscriptions with a typed local cache applying InitialData + TxUpdate diffs and async iterators/callbacks for changes
- [ ] 1.4 Implement fluxum generate --lang python emitting typed bindings from the /schema JSON; package for PyPI
- [ ] 1.5 Verification (DAG exit test): shared conformance corpus green in Python CI
- [ ] 1.6 Gate G7 input: PRD section 12.2 all green - failover + PITR + 5 SDKs + 1B-row soak + parity report v2 (release 0.2.0)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
