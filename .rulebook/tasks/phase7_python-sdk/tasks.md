## 1. Implementation
- [ ] 1.1 Implement `fluxum generate --lang python` bindings + the asyncio-first runtime over FluxRPC (FR-83, SDK-061)
- [ ] 1.2 Fully type-hinted public API shipping `py.typed`; connection/subscription lifecycle via async context managers
- [ ] 1.3 Client cache + reconnect/resubscribe/reconcile semantics per SDK-04x (shared behavior rules)
- [ ] 1.4 Python CI job running the shared conformance corpus (phase6_sdk-conformance-corpus)
- [ ] 1.5 Verification (DAG exit test): corpus green in Python CI
- [ ] 1.6 Gate G7 input (five-SDK conformance, SDK-064)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
