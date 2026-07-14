## 1. Implementation
- [ ] 1.1 Implement the `fluxum-sdk` client crate under sdks/rust reusing `fluxum-protocol` wire types directly (FR-84); symbol/type audit confirms all wire types come from fluxum-protocol (SPEC-011 acceptance 3)
- [ ] 1.2 TCP (:15801) and Streamable HTTP transports; auth, reducer calls, subscriptions with typed callbacks
- [ ] 1.3 Local client cache + reconnect/resubscribe/reconcile behavior consistent with SDK-04x rules
- [ ] 1.4 `fluxum generate --lang rust` bindings for module-specific types
- [ ] 1.5 Verification (DAG exit test): client conformance subset green (shared corpus, phase6_sdk-conformance-corpus)
- [ ] 1.6 Gate G6 input

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
