## 1. Implementation
- [x] 1.1 Implement `fluxum generate --lang typescript`: typed table interfaces + reducer wrappers from schema.json; compiles under tsc --strict with zero manual stubs (FR-82, SDK-021) — crates/fluxum-cli/src/generate/{mod,typescript}.rs. Emits types.ts (row interface + `<Table>Key` tuple per table), reducers.ts (a wrapper per client-callable reducer; schedule-only ones are omitted since the server answers clients 403), client.ts (the small `ReducerCaller` interface the wrappers call through, so the output typechecks before any runtime exists), index.ts. 64-bit ints map to `bigint` not `number` (u64 pk precision loss = silent row collisions); an unmodelled type is `unknown`, never `any`. Verified by a REAL compiler: tests/typescript_compiles.rs runs `tsc --noEmit` with strict + noUnusedLocals + noUncheckedIndexedAccess + exactOptionalPropertyTypes and fails on any diagnostic (it already caught an unused-import bug); confirmed the gate fails on an injected type error
- [x] 1.2 Codegen determinism + offline mode: regenerating twice is byte-identical; generation from an exported schema.json equals URL-based generation; every file carries the generated-do-not-edit header with schema_version (SPEC-011 acceptance 10/11) — generation is a pure function of the document and the file map is a BTreeMap; `--schema` accepts a URL or a file and both funnel through the same canonicalization, so offline == online (pinned by a test using a deliberately differently-ordered file). Banner carries both schema_version and document_version
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

## Progress log

This task is being delivered in **complete units** rather than one sweep: it spans a generator, a
browser runtime, a Node transport, a client cache, reconnect logic, and npm packaging under a size
budget — too much to land in one stretch without leaving something half-built.

- **Unit 1 (done, committed)** — 1.1 + 1.2, the generator. Self-contained and gated by a real `tsc`.
- **Unit 2 (next)** — 1.3 + 1.4, the runtime: FluxBIN decode over ArrayBuffer/DataView, Streamable
  HTTP transport for the browser, TCP for Node.
- **Unit 3** — 1.5 + 1.6, the refcounted cache and reconnect/reconciliation.
- **Unit 4** — 1.7 + 1.8 + 1.9, schema-mismatch drill, packaging/size budget, conformance runs.

Note on 1.9: it requires the shared conformance corpus, which is its own task
(`phase6_sdk-conformance-corpus`) whose acceptance is in turn "green against the first reference
client (TypeScript)". The two are mutually referential; the corpus task is the natural place for
that half, and 1.9 here closes once it lands.
