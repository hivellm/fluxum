## 1. Implementation

- [x] 1.1 Wrote docs/specs/SPEC-028-error-catalog.md (ranges, payload, retry/severity semantics, SQLSTATE + HTTP mappings, stability rules) and amended SPEC-006 RPC-034 (structured payload + catalog table) and SPEC-004 RED-060 (structured outcome, panic=5002); indexed in docs/specs/README.md
- [x] 1.2 fluxum-protocol: codes.rs rewritten as the catalog registry — CatalogEntry {code, name, severity, retryable, sqlstate, details_keys, message_template, http_status}, 31 released entries across the 9 subsystem ranges + uniqueness/range/sort/prefix tests, SQLSTATE-only-in-3xxx test, spec-pinned-code stability test, HTTP-era codes retired (entry(400) == None)
- [x] 1.3 fluxum-protocol: ErrorMessage extended with name/severity/retryable/retry_after_ms/sqlstate/details; `from_catalog` is the sanctioned constructor (registry-populated; uncataloged code degrades to SYS_INTERNAL with a debug assert); envelope + roundtrip-prop tests regenerated over catalog codes
- [x] 1.4 fluxum-core: `FluxumError::to_wire()` total mapping (exhaustive match — a new variant without a mapping fails compilation); Query gained retry_after_ms (+ query_retryable helper); all ~40 emission sites across sql/subscription/reducer/txn/index/store/auth migrated to subsystem-correct catalog codes
- [x] 1.5 fluxum-core: reducer Err(String) → structured ReducerResult outcome [5001 REDUCER_USER_ERROR, app_code, message verbatim]; panics are the new FluxumError::ReducerPanic variant → 5002 REDUCER_PANIC (never 5001); the wire ReducerError carries app_code (attach-API rides the module surface later — the field exists and round-trips)
- [x] 1.6 fluxum-server: session/tcp/http emit via from_catalog (from_error projects to_wire + retry_after); Streamable HTTP derives status from entry.http_status (session expiry keeps 404, frame-too-large 413, etc.); admin status_of derives from the catalog; rate limiter advertises retry_after_ms = refill estimate (RED-050)
- [x] 1.7 Registry adherence: every FluxumError variant pins to a released code with details keys checked against the registry (error.rs tests); from_catalog structurally prevents uncataloged emission; loopback suites assert catalog codes end-to-end
- [x] 1.8 Docs generator: `render_errors_md()` + golden test keeping docs/errors.md in sync (FLUXUM_REGEN_DOCS=1 regenerates; one section per entry asserted)

## 2. Tail (docs + tests — check or waive with tailWaiver)

- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
