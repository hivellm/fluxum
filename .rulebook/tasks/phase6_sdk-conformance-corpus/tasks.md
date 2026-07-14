## 1. Implementation
- [ ] 1.1 Define the shared, language-agnostic SDK conformance corpus (SPEC-013 TST-052): fixture set (demo schema, seeded data, scripted server interactions) + a runner protocol every SDK implements
- [ ] 1.2 Scenarios: connect/authenticate, subscribe + InitialData, reducer calls, TxUpdate diff application, client-cache equality against server state, unsubscribe, reconnect/resync via tx_id gap, error mapping (401/408/413/429/503), rate-limit behavior
- [ ] 1.3 Server-side harness: boot the demo module server, drive the scripted scenarios, expose fixtures for all 5 SDK CI jobs (reuses the T4.5 property-suite generator/fixtures where applicable)
- [ ] 1.4 Wire into CI as a reusable job consumed by TypeScript (T6.2, Node + Chromium), Rust (T6.4 subset), and later Python/Go/C# (T7.4-T7.6) - the single corpus is the SDK gate for G7 (SDK-064)
- [ ] 1.5 Verification: corpus runs green against the first reference client (TypeScript)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
