## 1. Implementation

- [ ] 1.1 Write docs/specs/SPEC-028-error-catalog.md (ranges, seed catalog table, payload, retry/severity semantics, SQLSTATE + HTTP mappings, stability rules) and amend SPEC-006 RPC-034 + SPEC-004 RED-060
- [ ] 1.2 fluxum-protocol: replace codes.rs with the catalog registry (code, name, severity, retryable, sqlstate, details keys) + uniqueness/range tests
- [ ] 1.3 fluxum-protocol: extend ErrorMessage with name/severity/retryable/retry_after_ms/sqlstate/details; update envelope + fluxbin golden tests
- [ ] 1.4 fluxum-core: map every FluxumError variant to a catalog code; migrate FluxumError::Query and all query(...) call sites off HTTP codes
- [ ] 1.5 fluxum-core: wrap reducer Err(String) as REDUCER_USER_ERROR with optional app_code passthrough (RED-060 amendment)
- [ ] 1.6 fluxum-server: emit catalog errors at every Error-frame site (auth, framing, idle, rate-limit, session) + derive HTTP status for Streamable HTTP
- [ ] 1.7 Registry-adherence test: no emission path produces a code absent from the registry; retryable/severity match registry defaults
- [ ] 1.8 Docs generator: emit docs/errors.md reference from the registry (one section per code: name, message template, details keys, retryability)

## 2. Tail (docs + tests — check or waive with tailWaiver)

- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass; full suite + clippy green, coverage >90% on touched crates
