## 1. Implementation
- [ ] 1.1 Implement the `fluxum-sdk` client crate under sdks/rust reusing `fluxum-protocol` wire types directly (FR-84); symbol/type audit confirms all wire types come from fluxum-protocol (SPEC-011 acceptance 3)
- [ ] 1.2 TCP (:15801) and Streamable HTTP transports; auth, reducer calls, subscriptions with typed callbacks
- [ ] 1.3 Local client cache + reconnect/resubscribe/reconcile behavior consistent with SDK-04x rules
- [ ] 1.3b Wire `fluxum_sdk::ResumeTracker` into the real connection (follow-up split from phase5_resumable-subscriptions-delta-resync 1.7, SPEC-021 CS-020/CS-021/CS-022): feed it every `InitialData`/`TxUpdate`, send its `resume_message()` on reconnect instead of re-subscribing, and honour a `cache_reset` snapshot by clearing the query's cached rows before applying. The tracker and its unit tests already exist — this is the socket/cache wiring that could not land before this crate had a client.
- [ ] 1.4 `fluxum generate --lang rust` bindings for module-specific types
- [ ] 1.5 Verification (DAG exit test): client conformance subset green (shared corpus, phase6_sdk-conformance-corpus)
- [ ] 1.6 Gate G6 input

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
