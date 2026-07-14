## 1. Implementation
- [ ] 1.1 Implement `fluxum generate --lang typescript`: typed table interfaces + reducer wrappers from schema.json; compiles under tsc --strict with zero manual stubs (FR-82, SDK-021)
- [ ] 1.2 Codegen determinism + offline mode: regenerating twice is byte-identical; generation from an exported schema.json equals URL-based generation; every file carries the generated-do-not-edit header with schema_version (SPEC-011 acceptance 10/11)
- [ ] 1.3 Browser-native runtime: binary FluxRPC over Streamable HTTP (POST /rpc + GET /rpc via fetch ReadableStream), FluxBIN decoded via ArrayBuffer/DataView - never JSON on the hot path (SDK-080/SDK-084); `fluxum://` in a browser fails fast with an actionable error (SDK-082)
- [ ] 1.4 Node.js support: TCP and Streamable HTTP transports
- [ ] 1.5 Refcounted local client cache with FluxBIN-byte row identity and mutate-then-callback ordering: delete+insert of the same PK in one TxUpdate = one update callback; overlapping subscriptions = one cached row with refcount, byte-identical pairs fire nothing (SDK-044/SDK-045)
- [ ] 1.6 Auto-reconnect with exponential backoff: re-auth, resubscribe, cache reconciliation emitting only net-difference callbacks; bounded inbound queues with transport backpressure, no silent drops (SDK-046/SDK-047)
- [ ] 1.7 Schema-mismatch drill: client built against version N vs server N+1 refreshes schema and reconnects, surfacing a typed SchemaMismatch error, never mistyped rows (SPEC-011 acceptance 9)
- [ ] 1.8 Packaging: plain ESM/CJS JavaScript + .d.ts typings, zero runtime dependencies, runtime <= 50 KB min+gzip (SDK-083); vanilla-JS `<script type="module">` smoke test - no build step, connect, subscribe, receive a typed TxUpdate (SDK-081)
- [ ] 1.9 Verification (DAG exit test): shared conformance corpus green in Node AND headless Chromium; vanilla-JS smoke test green
- [ ] 1.10 Gate G6 input (via T6.5 demo app)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
